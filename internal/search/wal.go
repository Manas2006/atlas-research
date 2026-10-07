package search

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
)

type walRecord struct {
	Document Document `json:"document"`
	Checksum uint32   `json:"checksum"`
}

type WAL struct {
	mu   sync.Mutex
	file *os.File
}

func OpenWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	return &WAL{file: file}, nil
}

// Append does not return until the record is fsynced.
func (w *WAL) Append(doc Document) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	payload, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	record, err := json.Marshal(walRecord{Document: doc, Checksum: crc32.ChecksumIEEE(payload)})
	if err != nil {
		return err
	}
	if _, err := w.file.Write(append(record, '\n')); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) Replay(apply func(Document) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	scanner := bufio.NewScanner(w.file)
	buffer := make([]byte, 64*1024)
	scanner.Buffer(buffer, 16*1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		var record walRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return fmt.Errorf("decode WAL line %d: %w", line, err)
		}
		payload, _ := json.Marshal(record.Document)
		if crc32.ChecksumIEEE(payload) != record.Checksum {
			return fmt.Errorf("WAL checksum mismatch at line %d", line)
		}
		if err := apply(record.Document); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func (w *WAL) Close() error { return w.file.Close() }
