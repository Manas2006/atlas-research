//go:build unix

package collab

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockDir takes an exclusive advisory lock on a directory for the life of
// the process. Two processes appending to the same operation logs would each
// acknowledge edits the other then overwrites, so the second one is refused.
func lockDir(dir string) (release func(), err error) {
	file, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%s is in use by another Atlas process", dir)
		}
		return nil, err
	}
	return func() { file.Close() }, nil
}
