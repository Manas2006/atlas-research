package video

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestLeaseExpiryRetryAndDeadLetter(t *testing.T) {
	queue, err := OpenQueue(filepath.Join(t.TempDir(), "jobs.json"))
	if err != nil { t.Fatal(err) }
	now := time.Unix(100, 0).UTC()
	queue.now = func() time.Time { return now }
	created, wasCreated, err := queue.Create(Job{ID: "job-0001", IdempotencyKey: "source-1", InputPath: "/in", OutputPath: "/out", MaxAttempts: 2})
	if err != nil || !wasCreated || created.State != Queued { t.Fatalf("create: %#v %v", created, err) }
	leased, err := queue.Lease("worker-a", 10*time.Second)
	if err != nil || leased.Attempts != 1 { t.Fatalf("lease: %#v %v", leased, err) }
	now = now.Add(11 * time.Second)
	leased, err = queue.Lease("worker-b", 10*time.Second)
	if err != nil || leased.LeaseOwner != "worker-b" || leased.Attempts != 2 { t.Fatalf("re-lease: %#v %v", leased, err) }
	dead, err := queue.Fail(leased.ID, "worker-b", "ffmpeg exited", time.Second)
	if err != nil || dead.State != Dead { t.Fatalf("dead letter: %#v %v", dead, err) }
	if _, err := queue.Lease("worker-c", time.Second); !errors.Is(err, ErrNoJob) { t.Fatalf("dead job was leased: %v", err) }
}

func TestIdempotentCreateAndPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.json")
	queue, _ := OpenQueue(path)
	first, _, err := queue.Create(Job{ID: "job-0001", IdempotencyKey: "same-upload", InputPath: "/a", OutputPath: "/b"})
	if err != nil { t.Fatal(err) }
	second, created, err := queue.Create(Job{ID: "job-0002", IdempotencyKey: "same-upload", InputPath: "/x", OutputPath: "/y"})
	if err != nil || created || first.ID != second.ID { t.Fatalf("idempotency failed: %#v %#v", first, second) }
	reopened, err := OpenQueue(path)
	if err != nil { t.Fatal(err) }
	if job, ok := reopened.Get(first.ID); !ok || job.InputPath != "/a" { t.Fatalf("persistence failed: %#v", job) }
}

func BenchmarkLeaseFromTenThousandJobs(b *testing.B) {
	for iteration := 0; iteration < b.N; iteration++ {
		queue, _ := OpenQueue(filepath.Join(b.TempDir(), "jobs.json"))
		for index := 0; index < 10_000; index++ {
			queue.jobs[fmt.Sprintf("job-%08d", index)] = Job{
				ID: fmt.Sprintf("job-%08d", index), State: Queued, CreatedAt: time.Unix(int64(index), 0),
			}
		}
		if _, err := queue.Lease("benchmark-worker", time.Minute); err != nil { b.Fatal(err) }
	}
}
