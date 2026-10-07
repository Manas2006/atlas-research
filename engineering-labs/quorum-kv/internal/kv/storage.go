package kv

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type metadata struct {
	Term        uint64 `json:"term"`
	VotedFor    string `json:"voted_for,omitempty"`
	CommitIndex uint64 `json:"commit_index"`
}

type diskSnapshot struct {
	LastApplied uint64           `json:"last_applied"`
	Data        map[string]Value `json:"data"`
}

type LogStore struct {
	mu       sync.Mutex
	dir      string
	logPath  string
	metaPath string
}

func OpenLogStore(dir string) (*LogStore, metadata, []Entry, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, metadata{}, nil, err
	}
	store := &LogStore{dir: dir, logPath: filepath.Join(dir, "raft.wal"), metaPath: filepath.Join(dir, "raft.json")}
	var meta metadata
	if payload, err := os.ReadFile(store.metaPath); err == nil {
		if err := json.Unmarshal(payload, &meta); err != nil {
			return nil, metadata{}, nil, fmt.Errorf("decode raft metadata: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return nil, metadata{}, nil, err
	}
	entries, err := store.readEntries()
	return store, meta, entries, err
}

func (s *LogStore) readEntries() ([]Entry, error) {
	file, err := os.Open(s.logPath)
	if os.IsNotExist(err) {
		return []Entry{{Index: 0, Term: 0}}, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	entries := []Entry{{Index: 0, Term: 0}}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry Entry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, fmt.Errorf("decode raft WAL: %w", err)
		}
		if entry.Index != uint64(len(entries)) {
			return nil, fmt.Errorf("non-contiguous raft WAL index %d", entry.Index)
		}
		entries = append(entries, entry)
	}
	return entries, scanner.Err()
}

func (s *LogStore) Append(entries ...Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	file, err := os.OpenFile(s.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	for _, entry := range entries {
		if err := encoder.Encode(entry); err != nil {
			file.Close()
			return err
		}
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

func (s *LogStore) Rewrite(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	temporary := s.logPath + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	for _, entry := range entries {
		if entry.Index == 0 {
			continue
		}
		if err := encoder.Encode(entry); err != nil {
			file.Close()
			return err
		}
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(temporary, s.logPath)
}

func (s *LogStore) SaveMetadata(meta metadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	temporary := s.metaPath + ".tmp"
	if err := os.WriteFile(temporary, payload, 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, s.metaPath)
}

func (s *LogStore) SaveSnapshot(lastApplied uint64, machine *StateMachine) error {
	payload, err := json.Marshal(diskSnapshot{LastApplied: lastApplied, Data: machine.copyData()})
	if err != nil {
		return err
	}
	path := filepath.Join(s.dir, "snapshot.json")
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, payload, 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func (s *LogStore) LoadSnapshot(machine *StateMachine) (uint64, error) {
	payload, err := os.ReadFile(filepath.Join(s.dir, "snapshot.json"))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var snapshot diskSnapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return 0, err
	}
	machine.replaceData(snapshot.Data)
	return snapshot.LastApplied, nil
}
