package video

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

type JobState string

const (
	Queued    JobState = "queued"
	Leased    JobState = "leased"
	Completed JobState = "completed"
	Dead      JobState = "dead"
)

var (
	ErrNoJob       = errors.New("no job available")
	ErrLeaseOwner  = errors.New("worker does not own active lease")
	ErrJobNotFound = errors.New("job not found")
	ErrQueueClosed = errors.New("queue is closed")
)

// errCorruptJournal marks damage that is not a torn final record. A torn
// tail is the expected result of a crash mid-append and is repaired; anything
// else means acknowledged changes are unreadable, so opening fails instead.
var errCorruptJournal = errors.New("queue journal is corrupt")

type Job struct {
	ID             string    `json:"id"`
	Name           string    `json:"name,omitempty"`
	SizeBytes      int64     `json:"size_bytes,omitempty"`
	IdempotencyKey string    `json:"idempotency_key"`
	InputPath      string    `json:"input_path"`
	OutputPath     string    `json:"output_path"`
	State          JobState  `json:"state"`
	Attempts       int       `json:"attempts"`
	MaxAttempts    int       `json:"max_attempts"`
	LeaseOwner     string    `json:"lease_owner,omitempty"`
	LeaseUntil     time.Time `json:"lease_until,omitempty"`
	NextAttempt    time.Time `json:"next_attempt,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// A queue is durable as a snapshot plus a journal. The snapshot, at the
// queue's path, holds every job and idempotency key in the format earlier
// releases wrote. The journal, at the same path plus ".wal", holds one
// fsynced record per change made since: the changed job's full state and a
// sequence number. Opening a queue loads the snapshot and replays the newer
// journal records over it.
//
// A change therefore costs one append instead of a rewrite of every job.
// Once the journal holds as many records as the queue holds jobs (and at
// least journalCompactMin), the next change writes a new snapshot instead
// and empties the journal. That bounds recovery work and keeps the amortized
// cost of a change independent of the number of jobs.
type queueFile struct {
	Jobs            map[string]Job    `json:"jobs"`
	IdempotencyKeys map[string]string `json:"idempotency_keys"`
	// JournalSeq is the last change this snapshot reflects. Recovery skips
	// journal records up to it, which the journal still holds if the process
	// stopped between writing the snapshot and emptying the journal. Every
	// snapshot this release writes has a JournalSeq of at least 1.
	JournalSeq uint64 `json:"journal_seq,omitempty"`
}

type journalRecord struct {
	Seq uint64 `json:"seq"`
	ID  string `json:"id"`
	Job Job    `json:"job"`
	// IdempotencyKey is set only by Create, the one change that maps a key.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// journalCompactMin is the fewest journal records that trigger a new
// snapshot, so a small queue is not rewritten on every change.
const journalCompactMin = 1024

type Queue struct {
	mu              sync.Mutex
	path            string
	jobs            map[string]Job
	idempotencyKeys map[string]string
	index           *leaseIndex
	journal         *os.File
	journalSize     int64  // bytes of the journal known to be durable
	journalRecords  int    // records in the journal, replayed or skipped
	seq             uint64 // sequence number of the last change
	compactMin      int    // see journalCompactMin; tests lower it
	// snapshotDue means the next change must write a full snapshot instead
	// of appending: memory may hold a change the journal lacks, or the
	// journal may end in bytes that are not a clean record.
	snapshotDue bool
	now         func() time.Time
}

func journalPath(path string) string { return path + ".wal" }

func OpenQueue(path string) (*Queue, error) {
	queue := &Queue{path: path, jobs: make(map[string]Job), idempotencyKeys: make(map[string]string), index: newLeaseIndex(), compactMin: journalCompactMin, now: time.Now}
	payload, err := os.ReadFile(path)
	switch {
	case os.IsNotExist(err):
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	default:
		var stored queueFile
		if err := json.Unmarshal(payload, &stored); err != nil {
			return nil, fmt.Errorf("decode queue: %w", err)
		}
		if stored.Jobs != nil {
			queue.jobs = stored.Jobs
		}
		if stored.IdempotencyKeys != nil {
			queue.idempotencyKeys = stored.IdempotencyKeys
		}
		queue.seq = stored.JournalSeq
	}
	// This release writes no journal record before a snapshot exists. With
	// no snapshot, or one from a release that kept no journal, any journal
	// present predates the snapshot and must not be replayed over it. It is
	// discarded, and the first change writes a snapshot the journal follows.
	queue.snapshotDue = queue.seq == 0
	if err := queue.openJournal(!queue.snapshotDue); err != nil {
		return nil, err
	}
	for id, job := range queue.jobs {
		queue.index.update(id, job)
	}
	return queue, nil
}

// Close releases the journal. Every acknowledged change is already durable.
// The queue must not be used afterwards.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.journal == nil {
		return nil
	}
	err := q.journal.Close()
	q.journal = nil
	return err
}

func (q *Queue) Create(job Job) (Job, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if existingID := q.idempotencyKeys[job.IdempotencyKey]; existingID != "" {
		return q.jobs[existingID], false, nil
	}
	if job.ID == "" || job.IdempotencyKey == "" || job.InputPath == "" || job.OutputPath == "" {
		return Job{}, false, errors.New("id, idempotency key, input, and output are required")
	}
	if job.MaxAttempts <= 0 {
		job.MaxAttempts = 5
	}
	if q.journal == nil {
		return Job{}, false, ErrQueueClosed
	}
	now := q.now().UTC()
	job.State, job.CreatedAt, job.UpdatedAt = Queued, now, now
	q.idempotencyKeys[job.IdempotencyKey] = job.ID
	if err := q.commitLocked(job.ID, job, job.IdempotencyKey); err != nil {
		delete(q.jobs, job.ID)
		delete(q.idempotencyKeys, job.IdempotencyKey)
		q.index.remove(job.ID)
		return Job{}, false, err
	}
	return job, true, nil
}

func (q *Queue) Get(id string) (Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	job, ok := q.jobs[id]
	return job, ok
}

func (q *Queue) List() []Job {
	q.mu.Lock()
	defer q.mu.Unlock()
	jobs := make([]Job, 0, len(q.jobs))
	for _, job := range q.jobs {
		jobs = append(jobs, job)
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].CreatedAt.After(jobs[j].CreatedAt) })
	return jobs
}

func (q *Queue) Lease(owner string, duration time.Duration) (Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now().UTC()
	// The oldest job by CreatedAt among those queued past their next attempt
	// or leased past their lease expiry.
	id, ok := q.index.next(now)
	if !ok {
		return Job{}, ErrNoJob
	}
	job := q.jobs[id]
	job.State = Leased
	job.Attempts++
	job.LeaseOwner = owner
	job.LeaseUntil = now.Add(duration)
	job.UpdatedAt = now
	if err := q.commitLocked(id, job, ""); err != nil {
		return Job{}, err
	}
	return job, nil
}

func (q *Queue) Heartbeat(id, owner string, duration time.Duration) (Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	job, ok := q.jobs[id]
	if !ok {
		return Job{}, ErrJobNotFound
	}
	now := q.now().UTC()
	if job.State != Leased || job.LeaseOwner != owner || !job.LeaseUntil.After(now) {
		return Job{}, ErrLeaseOwner
	}
	job.LeaseUntil, job.UpdatedAt = now.Add(duration), now
	return job, q.commitLocked(id, job, "")
}

func (q *Queue) Complete(id, owner string) (Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	job, err := q.ownedLocked(id, owner)
	if err != nil {
		return Job{}, err
	}
	job.State = Completed
	job.LeaseOwner, job.LastError = "", ""
	job.LeaseUntil = time.Time{}
	job.UpdatedAt = q.now().UTC()
	return job, q.commitLocked(id, job, "")
}

func (q *Queue) Fail(id, owner, message string, baseDelay time.Duration) (Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	job, err := q.ownedLocked(id, owner)
	if err != nil {
		return Job{}, err
	}
	now := q.now().UTC()
	job.LeaseOwner, job.LeaseUntil, job.UpdatedAt = "", time.Time{}, now
	job.LastError = message
	if job.Attempts >= job.MaxAttempts {
		job.State = Dead
	} else {
		job.State = Queued
		shift := min(job.Attempts-1, 10)
		job.NextAttempt = now.Add(baseDelay * time.Duration(1<<shift))
	}
	return job, q.commitLocked(id, job, "")
}

func (q *Queue) ReplayDead(id string) (Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	job, ok := q.jobs[id]
	if !ok {
		return Job{}, ErrJobNotFound
	}
	if job.State != Dead {
		return Job{}, errors.New("job is not dead-lettered")
	}
	job.State, job.Attempts, job.LastError = Queued, 0, ""
	job.NextAttempt, job.UpdatedAt = time.Time{}, q.now().UTC()
	return job, q.commitLocked(id, job, "")
}

func (q *Queue) ownedLocked(id, owner string) (Job, error) {
	job, ok := q.jobs[id]
	if !ok {
		return Job{}, ErrJobNotFound
	}
	if job.State != Leased || job.LeaseOwner != owner || !job.LeaseUntil.After(q.now().UTC()) {
		return Job{}, ErrLeaseOwner
	}
	return job, nil
}

// commitLocked applies a job's new state to memory and the lease index, then
// makes it durable: one journal append, or a full snapshot when the journal
// is due to be compacted or an earlier change failed to reach it. As before
// the journal existed, a change whose write fails stays in memory and is
// written with the next change. idempotencyKey is set only by Create.
func (q *Queue) commitLocked(id string, job Job, idempotencyKey string) error {
	if q.journal == nil {
		return ErrQueueClosed
	}
	q.jobs[id] = job
	q.index.update(id, job)
	q.seq++
	if q.snapshotDue || q.journalRecords >= max(q.compactMin, len(q.jobs)) {
		return q.snapshotLocked()
	}
	return q.appendLocked(journalRecord{Seq: q.seq, ID: id, Job: job, IdempotencyKey: idempotencyKey})
}

func (q *Queue) appendLocked(record journalRecord) error {
	payload, err := json.Marshal(record)
	if err != nil {
		q.snapshotDue = true
		return err
	}
	line := make([]byte, 0, crcWidth+len(payload)+2)
	line = fmt.Appendf(line, "%08x ", crc32.ChecksumIEEE(payload))
	line = append(line, payload...)
	line = append(line, '\n')
	_, err = q.journal.Write(line)
	if err == nil {
		err = q.journal.Sync()
	}
	if err != nil {
		// The caller is told this change failed, so take the record back out
		// where recovery could mistake it for a committed one. Memory still
		// holds the change, so the next change writes everything.
		if q.journal.Truncate(q.journalSize) == nil {
			_ = q.journal.Sync()
		}
		q.snapshotDue = true
		return err
	}
	q.journalSize += int64(len(line))
	q.journalRecords++
	return nil
}

// snapshotLocked writes every job to a new snapshot and empties the journal.
func (q *Queue) snapshotLocked() error {
	// Until a snapshot is durable and the journal is empty again, every
	// change keeps writing snapshots.
	q.snapshotDue = true
	payload, err := json.MarshalIndent(queueFile{Jobs: q.jobs, IdempotencyKeys: q.idempotencyKeys, JournalSeq: q.seq}, "", "  ")
	if err != nil {
		return err
	}
	temporary := q.path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(payload); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, q.path); err != nil {
		return err
	}
	// The change is now as durable as a snapshot has always made it.
	// Recovery skips every journal record the snapshot reflects, so emptying
	// the journal only reclaims space. That waits until the rename itself is
	// durable, or a crash could lose both files' changes. If any step fails,
	// the journal is kept and the next change writes a snapshot again.
	if syncDir(filepath.Dir(q.path)) == nil && q.journal.Truncate(0) == nil && q.journal.Sync() == nil {
		q.journalSize, q.journalRecords, q.snapshotDue = 0, 0, false
	}
	return nil
}

// openJournal opens the journal. With replay it applies the records the
// snapshot does not already reflect and drops a torn final record left by a
// crash; without it the contents are stale and are discarded.
func (q *Queue) openJournal(replay bool) error {
	path := journalPath(q.path)
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	// O_APPEND makes every write land at the true end of the file.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	if created {
		// Make the new directory entry durable before anything depends on it.
		if err := syncDir(filepath.Dir(path)); err != nil {
			file.Close()
			return err
		}
	}
	var valid int64
	if replay {
		valid, err = q.replayJournal(file)
		if err != nil {
			file.Close()
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return err
	}
	if size != valid {
		// Drop the torn tail, or the stale journal, so the next append
		// starts on a clean line.
		if err := file.Truncate(valid); err != nil {
			file.Close()
			return err
		}
	}
	// The records just replayed may never have been fsynced by the process
	// that wrote them. They are about to be treated as committed, so make
	// them so first.
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	q.journal, q.journalSize = file, valid
	return nil
}

// replayJournal applies, in file order, every journal record newer than the
// snapshot and returns the byte offset where the valid records end. Each
// line is "<crc32 hex> <json>". A final line that is incomplete or fails its
// checksum is treated as torn. A bad line followed by more data, or a line
// that passes its checksum but does not decode, is corruption.
func (q *Queue) replayJournal(file *os.File) (int64, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	snapshotSeq := q.seq
	reader := bufio.NewReaderSize(file, 1<<16)
	var offset int64
	for {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			// Bytes without a trailing newline were never fully written.
			return offset, nil
		}
		if err != nil {
			return 0, err
		}
		payload, ok := checkJournalLine(line[:len(line)-1])
		if !ok {
			if _, peekErr := reader.Peek(1); errors.Is(peekErr, io.EOF) {
				return offset, nil
			}
			return 0, fmt.Errorf("%w at byte %d", errCorruptJournal, offset)
		}
		var record journalRecord
		if err := json.Unmarshal(payload, &record); err != nil || record.ID == "" {
			return 0, fmt.Errorf("%w: undecodable record at byte %d", errCorruptJournal, offset)
		}
		// A record at or below the snapshot's sequence number was written
		// before the snapshot, which already reflects it.
		if record.Seq > snapshotSeq {
			q.jobs[record.ID] = record.Job
			if record.IdempotencyKey != "" {
				q.idempotencyKeys[record.IdempotencyKey] = record.ID
			}
			q.seq = max(q.seq, record.Seq)
		}
		q.journalRecords++
		offset += int64(len(line))
	}
}

const crcWidth = 8

func checkJournalLine(line []byte) ([]byte, bool) {
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
	return payload, true
}

func syncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
}
