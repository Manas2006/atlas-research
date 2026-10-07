package video

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
)

var safeID = regexp.MustCompile(`^[a-zA-Z0-9_-]{8,128}$`)

type ObjectStore struct {
	root  string
	locks sync.Map
}

func NewObjectStore(root string) (*ObjectStore, error) {
	if err := os.MkdirAll(filepath.Join(root, "uploads"), 0o700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(root, "outputs"), 0o700); err != nil {
		return nil, err
	}
	return &ObjectStore{root: root}, nil
}

func (s *ObjectStore) Root() string { return s.root }

func (s *ObjectStore) PutChunk(id string, offset int64, source io.Reader) (int64, error) {
	if !safeID.MatchString(id) {
		return 0, errors.New("invalid upload id")
	}
	lockValue, _ := s.locks.LoadOrStore(id, &sync.Mutex{})
	lock := lockValue.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()
	path := filepath.Join(s.root, "uploads", id+".part")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() != offset {
		return info.Size(), fmt.Errorf("offset mismatch: current offset is %d", info.Size())
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return offset, err
	}
	written, err := io.Copy(file, io.LimitReader(source, 64<<20))
	if err != nil {
		return offset, err
	}
	if err := file.Sync(); err != nil {
		return offset, err
	}
	return offset + written, nil
}

func (s *ObjectStore) Finalize(id string) (string, error) {
	if !safeID.MatchString(id) {
		return "", errors.New("invalid upload id")
	}
	lockValue, _ := s.locks.LoadOrStore(id, &sync.Mutex{})
	lock := lockValue.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()
	source := filepath.Join(s.root, "uploads", id+".part")
	destinationDir := filepath.Join(s.root, "uploads", id)
	if err := os.MkdirAll(destinationDir, 0o700); err != nil {
		return "", err
	}
	destination := filepath.Join(destinationDir, "source")
	if err := os.Rename(source, destination); err != nil {
		if os.IsNotExist(err) {
			if _, statErr := os.Stat(destination); statErr == nil {
				return destination, nil
			}
		}
		return "", err
	}
	return destination, nil
}

func (s *ObjectStore) OutputPath(id string) string { return filepath.Join(s.root, "outputs", id) }
