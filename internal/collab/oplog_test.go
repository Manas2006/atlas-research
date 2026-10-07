package collab

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func appendAll(t *testing.T, log *fileLog, records ...string) {
	t.Helper()
	payloads := make([][]byte, len(records))
	for index, record := range records {
		payloads[index] = []byte(record)
	}
	if err := log.Append(payloads); err != nil {
		t.Fatal(err)
	}
}

func recordStrings(records [][]byte) []string {
	out := make([]string, len(records))
	for index, record := range records {
		out[index] = string(record)
	}
	return out
}

func TestFileLogRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "doc.oplog")
	log, records, err := openFileLog(path)
	if err != nil || len(records) != 0 {
		t.Fatalf("fresh log: %v %v", records, err)
	}
	appendAll(t, log, `{"k":"meta"}`, `{"k":"op","rev":1}`)
	appendAll(t, log, `{"k":"op","rev":2}`)
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	log, records, err = openFileLog(path)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	if got := recordStrings(records); len(got) != 3 || got[2] != `{"k":"op","rev":2}` {
		t.Fatalf("unexpected records %v", got)
	}
	if err := log.Append([][]byte{[]byte("two\nlines")}); err == nil {
		t.Fatal("a record containing a newline must be refused")
	}
}

// A crash in the middle of an append leaves a partial last line. Opening the
// log must drop it, keep everything before it, and leave the file ready for
// the next append.
func TestFileLogRepairsTornTail(t *testing.T) {
	for name, tail := range map[string]string{
		"partial line":       `0badc0de {"k":"op","re`,
		"complete, bad sum":  "00000000 {\"k\":\"op\",\"rev\":3}\n",
		"garbage after sync": "\x00\x00\x00\x00\x00\x00",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "doc.oplog")
			log, _, err := openFileLog(path)
			if err != nil {
				t.Fatal(err)
			}
			appendAll(t, log, `{"k":"op","rev":1}`, `{"k":"op","rev":2}`)
			log.Close()
			file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			file.WriteString(tail)
			file.Close()

			log, records, err := openFileLog(path)
			if err != nil {
				t.Fatalf("a torn tail must not prevent opening: %v", err)
			}
			if len(records) != 2 {
				t.Fatalf("expected the two durable records, got %v", recordStrings(records))
			}
			appendAll(t, log, `{"k":"op","rev":3}`)
			log.Close()
			_, records, err = openFileLog(path)
			if err != nil || len(records) != 3 || string(records[2]) != `{"k":"op","rev":3}` {
				t.Fatalf("append after repair: %v %v", recordStrings(records), err)
			}
		})
	}
}

// Damage that is followed by more records is not a torn tail. Dropping it
// would silently lose acknowledged edits, so opening must fail instead.
func TestFileLogRefusesMidFileCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "doc.oplog")
	log, _, err := openFileLog(path)
	if err != nil {
		t.Fatal(err)
	}
	appendAll(t, log, `{"k":"op","rev":1}`, `{"k":"op","rev":2}`, `{"k":"op","rev":3}`)
	log.Close()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip one byte inside the second record.
	content[len(content)/2] ^= 0x01
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openFileLog(path); !errors.Is(err, errCorruptLog) {
		t.Fatalf("expected corruption to be reported, got %v", err)
	}
}
