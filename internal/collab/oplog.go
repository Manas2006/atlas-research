package collab

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// Log is the durability boundary for one document. Append must not return
// until every record survives a crash, because the server only acknowledges
// an edit after Append succeeds. The file-backed log below is the default; a
// replicated log can stand in for it without changes elsewhere.
type Log interface {
	Append(records [][]byte) error
	Close() error
}

// fileLog stores one record per line as "<crc32 hex> <json>". Stripping the
// first nine bytes of every line yields plain JSON Lines for analysis.
type fileLog struct {
	mu   sync.Mutex
	file *os.File
	size int64 // bytes known to be durable
}

const crcWidth = 8

// errCorruptLog marks damage that is not a torn final record. A torn tail is
// the expected result of a crash mid-append and is repaired; anything else
// means acknowledged history is unreadable, so opening fails instead.
var errCorruptLog = errors.New("operation log is corrupt")

// openFileLog opens or creates a log and returns every valid record in it.
func openFileLog(path string) (*fileLog, [][]byte, error) {
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	// O_APPEND makes every write land at the true end of the file even if
	// something else has written to it since this handle was opened.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, nil, err
	}
	if created {
		// Make the new directory entry durable before anything depends on it.
		if err := syncDir(filepath.Dir(path)); err != nil {
			file.Close()
			return nil, nil, err
		}
	}
	records, validBytes, err := readRecords(file)
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	if size != validBytes {
		// Drop the torn tail so the next append starts on a clean line.
		if err := file.Truncate(validBytes); err != nil {
			file.Close()
			return nil, nil, err
		}
	}
	// What was just read may include records whose fsync failed or never
	// ran before the previous owner of this file went away. They are about
	// to be served to clients as committed, so make them so first.
	if err := file.Sync(); err != nil {
		file.Close()
		return nil, nil, err
	}
	return &fileLog{file: file, size: validBytes}, records, nil
}

// readRecords returns the valid records and the byte offset where they end.
// A final line that is incomplete or fails its checksum is treated as torn.
// A bad line followed by more data is corruption.
func readRecords(file *os.File) ([][]byte, int64, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, err
	}
	reader := bufio.NewReaderSize(file, 1<<16)
	var records [][]byte
	var offset int64
	for {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			// Bytes without a trailing newline were never fully written.
			return records, offset, nil
		}
		if err != nil {
			return nil, 0, err
		}
		payload, ok := checkLine(line[:len(line)-1])
		if !ok {
			if _, peekErr := reader.Peek(1); errors.Is(peekErr, io.EOF) {
				return records, offset, nil
			}
			return nil, 0, fmt.Errorf("%w at byte %d", errCorruptLog, offset)
		}
		records = append(records, payload)
		offset += int64(len(line))
	}
}

func checkLine(line []byte) ([]byte, bool) {
	if len(line) < crcWidth+2 || line[crcWidth] != ' ' {
		return nil, false
	}
	want, err := strconv.ParseUint(string(line[:crcWidth]), 16, 32)
	if err != nil {
		return nil, false
	}
	payload := line[crcWidth+1:]
	if crc32.ChecksumIEEE(payload) != uint32(want) {
		return nil, false
	}
	return append([]byte(nil), payload...), true
}

// Append writes the records with one write and one fsync, which is what lets
// the document actor commit a whole batch of edits for the price of one.
func (l *fileLog) Append(records [][]byte) error {
	if len(records) == 0 {
		return nil
	}
	var buffer bytes.Buffer
	for _, payload := range records {
		if bytes.IndexByte(payload, '\n') >= 0 {
			return errors.New("log record must not contain a newline")
		}
		fmt.Fprintf(&buffer, "%08x ", crc32.ChecksumIEEE(payload))
		buffer.Write(payload)
		buffer.WriteByte('\n')
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err := l.file.Write(buffer.Bytes())
	if err == nil {
		err = l.file.Sync()
	}
	if err != nil {
		// Nothing in this batch was acknowledged. Take it back out so a
		// later reader of the file cannot mistake it for committed history.
		if l.file.Truncate(l.size) == nil {
			_ = l.file.Sync()
		}
		return err
	}
	l.size += int64(buffer.Len())
	return nil
}

func (l *fileLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}

func syncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
}

// memoryLog keeps records in memory. It backs the convergence simulator,
// where thousands of runs would otherwise spend their time in fsync.
type memoryLog struct {
	mu      sync.Mutex
	records [][]byte
	failing bool
}

func (l *memoryLog) Append(records [][]byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failing {
		return errors.New("injected log failure")
	}
	for _, payload := range records {
		l.records = append(l.records, append([]byte(nil), payload...))
	}
	return nil
}

func (l *memoryLog) Close() error { return nil }

func (l *memoryLog) snapshot() [][]byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([][]byte(nil), l.records...)
}
