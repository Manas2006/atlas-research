//go:build !unix

package collab

// lockDir is a no-op where flock is unavailable. Run only one Atlas process
// per data directory on such systems.
func lockDir(string) (release func(), err error) { return func() {}, nil }
