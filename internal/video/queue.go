package video

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
)

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

type queueFile struct {
	Jobs            map[string]Job    `json:"jobs"`
	IdempotencyKeys map[string]string `json:"idempotency_keys"`
}

type Queue struct {
	mu              sync.Mutex
	path            string
	jobs            map[string]Job
	idempotencyKeys map[string]string
	now             func() time.Time
}

func OpenQueue(path string) (*Queue, error) {
	queue := &Queue{path: path, jobs: make(map[string]Job), idempotencyKeys: make(map[string]string), now: time.Now}
	payload, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, err
		}
		return queue, nil
	}
	if err != nil {
		return nil, err
	}
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
	return queue, nil
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
	now := q.now().UTC()
	job.State, job.CreatedAt, job.UpdatedAt = Queued, now, now
	q.jobs[job.ID] = job
	q.idempotencyKeys[job.IdempotencyKey] = job.ID
	if err := q.persistLocked(); err != nil {
		delete(q.jobs, job.ID)
		delete(q.idempotencyKeys, job.IdempotencyKey)
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
	ids := make([]string, 0, len(q.jobs))
	for id := range q.jobs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return q.jobs[ids[i]].CreatedAt.Before(q.jobs[ids[j]].CreatedAt) })
	for _, id := range ids {
		job := q.jobs[id]
		eligible := job.State == Queued && !job.NextAttempt.After(now)
		if job.State == Leased && !job.LeaseUntil.After(now) {
			eligible = true
		}
		if !eligible {
			continue
		}
		job.State = Leased
		job.Attempts++
		job.LeaseOwner = owner
		job.LeaseUntil = now.Add(duration)
		job.UpdatedAt = now
		q.jobs[id] = job
		if err := q.persistLocked(); err != nil {
			return Job{}, err
		}
		return job, nil
	}
	return Job{}, ErrNoJob
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
	q.jobs[id] = job
	return job, q.persistLocked()
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
	q.jobs[id] = job
	return job, q.persistLocked()
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
	q.jobs[id] = job
	return job, q.persistLocked()
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
	q.jobs[id] = job
	return job, q.persistLocked()
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

func (q *Queue) persistLocked() error {
	payload, err := json.MarshalIndent(queueFile{Jobs: q.jobs, IdempotencyKeys: q.idempotencyKeys}, "", "  ")
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
	return os.Rename(temporary, q.path)
}
