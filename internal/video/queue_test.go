package video

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLeaseExpiryRetryAndDeadLetter(t *testing.T) {
	queue, err := OpenQueue(filepath.Join(t.TempDir(), "jobs.json"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0).UTC()
	queue.now = func() time.Time { return now }
	created, wasCreated, err := queue.Create(Job{ID: "job-0001", IdempotencyKey: "source-1", InputPath: "/in", OutputPath: "/out", MaxAttempts: 2})
	if err != nil || !wasCreated || created.State != Queued {
		t.Fatalf("create: %#v %v", created, err)
	}
	leased, err := queue.Lease("worker-a", 10*time.Second)
	if err != nil || leased.Attempts != 1 {
		t.Fatalf("lease: %#v %v", leased, err)
	}
	now = now.Add(11 * time.Second)
	leased, err = queue.Lease("worker-b", 10*time.Second)
	if err != nil || leased.LeaseOwner != "worker-b" || leased.Attempts != 2 {
		t.Fatalf("re-lease: %#v %v", leased, err)
	}
	dead, err := queue.Fail(leased.ID, "worker-b", "ffmpeg exited", time.Second)
	if err != nil || dead.State != Dead {
		t.Fatalf("dead letter: %#v %v", dead, err)
	}
	if _, err := queue.Lease("worker-c", time.Second); !errors.Is(err, ErrNoJob) {
		t.Fatalf("dead job was leased: %v", err)
	}
}

func TestIdempotentCreateAndPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.json")
	queue, _ := OpenQueue(path)
	first, _, err := queue.Create(Job{ID: "job-0001", IdempotencyKey: "same-upload", InputPath: "/a", OutputPath: "/b"})
	if err != nil {
		t.Fatal(err)
	}
	second, created, err := queue.Create(Job{ID: "job-0002", IdempotencyKey: "same-upload", InputPath: "/x", OutputPath: "/y"})
	if err != nil || created || first.ID != second.ID {
		t.Fatalf("idempotency failed: %#v %#v", first, second)
	}
	reopened, err := OpenQueue(path)
	if err != nil {
		t.Fatal(err)
	}
	if job, ok := reopened.Get(first.ID); !ok || job.InputPath != "/a" {
		t.Fatalf("persistence failed: %#v", job)
	}
}

// testClock is a fake clock shared by a queue and every reopened copy of it.
type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time             { return c.now }
func (c *testClock) Advance(step time.Duration) { c.now = c.now.Add(step) }

func openTestQueue(t testing.TB, path string, clock *testClock) *Queue {
	t.Helper()
	queue, err := OpenQueue(path)
	if err != nil {
		t.Fatal(err)
	}
	queue.now = clock.Now
	t.Cleanup(func() { queue.Close() })
	return queue
}

func mustCreate(t *testing.T, queue *Queue, id string) Job {
	t.Helper()
	job, created, err := queue.Create(Job{ID: id, IdempotencyKey: "key-" + id, InputPath: "/in/" + id, OutputPath: "/out/" + id, MaxAttempts: 3})
	if err != nil || !created {
		t.Fatalf("create %s: %#v %v", id, job, err)
	}
	return job
}

func mustLease(t *testing.T, queue *Queue, owner string, duration time.Duration, want string) Job {
	t.Helper()
	job, err := queue.Lease(owner, duration)
	if err != nil || job.ID != want {
		t.Fatalf("lease by %s: got %q (%v), want %q", owner, job.ID, err, want)
	}
	return job
}

func mustBeEmpty(t *testing.T, queue *Queue) {
	t.Helper()
	if job, err := queue.Lease("probe", time.Second); !errors.Is(err, ErrNoJob) {
		t.Fatalf("expected no leasable job, got %q (%v)", job.ID, err)
	}
}

func TestLeaseOrder(t *testing.T) {
	clock := &testClock{now: time.Unix(1_000, 0).UTC()}
	queue := openTestQueue(t, filepath.Join(t.TempDir(), "jobs.json"), clock)
	// IDs sort opposite to creation order, so only CreatedAt can explain the
	// order below. "job-b" and "job-a" share a CreatedAt; the ID breaks it.
	mustCreate(t, queue, "job-z")
	clock.Advance(time.Second)
	mustCreate(t, queue, "job-y")
	clock.Advance(time.Second)
	mustCreate(t, queue, "job-b")
	mustCreate(t, queue, "job-a")
	clock.Advance(time.Second)
	mustCreate(t, queue, "job-x")

	for _, want := range []string{"job-z", "job-y", "job-a", "job-b", "job-x"} {
		mustLease(t, queue, "worker", time.Minute, want)
	}
	mustBeEmpty(t, queue)

	// A failed job waits out its backoff while newer work is leased, then
	// goes ahead of that work again because it is older.
	if _, err := queue.Fail("job-z", "worker", "transient", 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Fail("job-x", "worker", "transient", time.Second); err != nil {
		t.Fatal(err)
	}
	mustBeEmpty(t, queue)
	clock.Advance(time.Second)
	mustLease(t, queue, "worker", time.Minute, "job-x")
	if _, err := queue.Fail("job-x", "worker", "transient", time.Second); err != nil {
		t.Fatal(err)
	}
	clock.Advance(9 * time.Second)
	mustLease(t, queue, "worker", time.Minute, "job-z")
	mustLease(t, queue, "worker", time.Minute, "job-x")
	mustBeEmpty(t, queue)
}

func TestLeaseExpiryRequeuesAndHeartbeatExtends(t *testing.T) {
	clock := &testClock{now: time.Unix(1_000, 0).UTC()}
	queue := openTestQueue(t, filepath.Join(t.TempDir(), "jobs.json"), clock)
	mustCreate(t, queue, "job-old")
	clock.Advance(time.Second)
	mustCreate(t, queue, "job-new")

	mustLease(t, queue, "worker-a", 10*time.Second, "job-old")
	clock.Advance(5 * time.Second)
	beat, err := queue.Heartbeat("job-old", "worker-a", 10*time.Second)
	if err != nil || !beat.LeaseUntil.Equal(clock.Now().Add(10*time.Second)) {
		t.Fatalf("heartbeat: %#v %v", beat, err)
	}
	if _, err := queue.Heartbeat("job-old", "worker-b", 10*time.Second); !errors.Is(err, ErrLeaseOwner) {
		t.Fatalf("a non-owner extended the lease: %v", err)
	}

	// Past the original expiry but inside the extension, only the newer job
	// can be leased.
	clock.Advance(6 * time.Second)
	mustLease(t, queue, "worker-b", time.Minute, "job-new")
	mustBeEmpty(t, queue)

	// The lease ends exactly at LeaseUntil. The expired job is leasable
	// again, and its old owner can no longer act on it.
	clock.Advance(4*time.Second - time.Nanosecond)
	mustBeEmpty(t, queue)
	clock.Advance(time.Nanosecond)
	if _, err := queue.Heartbeat("job-old", "worker-a", 10*time.Second); !errors.Is(err, ErrLeaseOwner) {
		t.Fatalf("an expired lease was extended: %v", err)
	}
	released := mustLease(t, queue, "worker-c", 10*time.Second, "job-old")
	if released.Attempts != 2 || released.LeaseOwner != "worker-c" {
		t.Fatalf("expired lease was not reassigned: %#v", released)
	}
	if _, err := queue.Complete("job-old", "worker-a"); !errors.Is(err, ErrLeaseOwner) {
		t.Fatalf("a previous owner completed the job: %v", err)
	}
	if done, err := queue.Complete("job-old", "worker-c"); err != nil || done.State != Completed {
		t.Fatalf("complete: %#v %v", done, err)
	}
	clock.Advance(time.Hour)
	mustLease(t, queue, "worker-d", time.Minute, "job-new")
	mustBeEmpty(t, queue)
}

func TestRetryBackoffTimingAndReplay(t *testing.T) {
	clock := &testClock{now: time.Unix(1_000, 0).UTC()}
	queue := openTestQueue(t, filepath.Join(t.TempDir(), "jobs.json"), clock)
	mustCreate(t, queue, "job-0001")
	for attempt, delay := range []time.Duration{2 * time.Second, 4 * time.Second} {
		mustLease(t, queue, "worker", time.Minute, "job-0001")
		failed, err := queue.Fail("job-0001", "worker", "ffmpeg exited", 2*time.Second)
		if err != nil || failed.State != Queued || !failed.NextAttempt.Equal(clock.Now().Add(delay)) || failed.Attempts != attempt+1 {
			t.Fatalf("attempt %d: %#v %v", attempt+1, failed, err)
		}
		clock.Advance(delay - time.Nanosecond)
		mustBeEmpty(t, queue)
		clock.Advance(time.Nanosecond)
	}
	mustLease(t, queue, "worker", time.Minute, "job-0001")
	dead, err := queue.Fail("job-0001", "worker", "ffmpeg exited", 2*time.Second)
	if err != nil || dead.State != Dead || dead.Attempts != 3 {
		t.Fatalf("dead letter: %#v %v", dead, err)
	}
	clock.Advance(time.Hour)
	mustBeEmpty(t, queue)
	if _, err := queue.ReplayDead("job-0001"); err != nil {
		t.Fatal(err)
	}
	if replayed := mustLease(t, queue, "worker", time.Minute, "job-0001"); replayed.Attempts != 1 {
		t.Fatalf("replay did not reset attempts: %#v", replayed)
	}
}

// snapshotOf returns everything a queue would serve, for comparison.
func snapshotOf(t *testing.T, queue *Queue) string {
	t.Helper()
	queue.mu.Lock()
	defer queue.mu.Unlock()
	payload, err := json.Marshal(queueFile{Jobs: queue.jobs, IdempotencyKeys: queue.idempotencyKeys})
	if err != nil {
		t.Fatal(err)
	}
	return string(payload)
}

func TestRecoveryAfterReopeningQueue(t *testing.T) {
	for _, compactMin := range []int{journalCompactMin, 3} {
		t.Run(fmt.Sprintf("compact-after-%d", compactMin), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "jobs.json")
			clock := &testClock{now: time.Unix(1_000, 0).UTC()}
			queue := openTestQueue(t, path, clock)
			queue.compactMin = compactMin
			for _, id := range []string{"job-done", "job-dead", "job-retry", "job-leased", "job-queued", "job-later"} {
				mustCreate(t, queue, id)
				clock.Advance(time.Second)
			}
			mustLease(t, queue, "worker", time.Minute, "job-done")
			if _, err := queue.Complete("job-done", "worker"); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				mustLease(t, queue, "worker", time.Minute, "job-dead")
				if _, err := queue.Fail("job-dead", "worker", "bad input", 0); err != nil {
					t.Fatal(err)
				}
			}
			mustLease(t, queue, "worker", time.Minute, "job-retry")
			if _, err := queue.Fail("job-retry", "worker", "transient", 30*time.Second); err != nil {
				t.Fatal(err)
			}
			mustLease(t, queue, "worker-a", 20*time.Second, "job-leased")
			if _, err := queue.Heartbeat("job-leased", "worker-a", 40*time.Second); err != nil {
				t.Fatal(err)
			}
			before, listed := snapshotOf(t, queue), queue.List()
			if err := queue.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := queue.Lease("worker", time.Second); !errors.Is(err, ErrQueueClosed) {
				t.Fatalf("closed queue accepted a change: %v", err)
			}

			reopened := openTestQueue(t, path, clock)
			if after := snapshotOf(t, reopened); after != before {
				t.Fatalf("state changed across reopen:\nbefore %s\nafter  %s", before, after)
			}
			if !reflect.DeepEqual(jobIDs(reopened.List()), jobIDs(listed)) {
				t.Fatalf("listing changed across reopen: %v, want %v", jobIDs(reopened.List()), jobIDs(listed))
			}
			if again, created, err := reopened.Create(Job{ID: "job-other", IdempotencyKey: "key-job-queued", InputPath: "/x", OutputPath: "/y"}); err != nil || created || again.ID != "job-queued" {
				t.Fatalf("idempotency key lost across reopen: %#v %v %v", again, created, err)
			}

			// The rebuilt index serves the same order and timing: the
			// unexpired lease and the backoff are both still honored.
			mustLease(t, reopened, "worker-b", time.Minute, "job-queued")
			mustLease(t, reopened, "worker-b", time.Minute, "job-later")
			mustBeEmpty(t, reopened)
			clock.Advance(40 * time.Second)
			mustLease(t, reopened, "worker-b", time.Minute, "job-retry")
			mustLease(t, reopened, "worker-b", time.Minute, "job-leased")
			mustBeEmpty(t, reopened)
			if _, err := reopened.Complete("job-leased", "worker-a"); !errors.Is(err, ErrLeaseOwner) {
				t.Fatalf("a lease that expired across the restart was honored: %v", err)
			}
		})
	}
}

func jobIDs(jobs []Job) []string {
	ids := make([]string, len(jobs))
	for index, job := range jobs {
		ids[index] = job.ID
	}
	return ids
}

func TestRecoveryDropsTornJournalTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.json")
	clock := &testClock{now: time.Unix(1_000, 0).UTC()}
	queue := openTestQueue(t, path, clock)
	mustCreate(t, queue, "job-0001")
	mustLease(t, queue, "worker", time.Minute, "job-0001")
	before := snapshotOf(t, queue)
	queue.Close()

	// A crash in the middle of an append leaves part of a record behind.
	journal, err := os.OpenFile(journalPath(path), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.WriteString(`0badf00d {"seq":99,"id":"job-0001","job":{"state":"comp`); err != nil {
		t.Fatal(err)
	}
	journal.Close()

	reopened := openTestQueue(t, path, clock)
	if after := snapshotOf(t, reopened); after != before {
		t.Fatalf("torn record was applied:\nbefore %s\nafter  %s", before, after)
	}
	if _, err := reopened.Complete("job-0001", "worker"); err != nil {
		t.Fatal(err)
	}
	reopened.Close()
	if again := openTestQueue(t, path, clock); !strings.Contains(snapshotOf(t, again), `"state":"completed"`) {
		t.Fatalf("change after a repaired tail was lost: %s", snapshotOf(t, again))
	}
}

func TestRecoveryRejectsCorruptJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.json")
	clock := &testClock{now: time.Unix(1_000, 0).UTC()}
	queue := openTestQueue(t, path, clock)
	mustCreate(t, queue, "job-0001")
	mustLease(t, queue, "worker", time.Minute, "job-0001")
	if _, err := queue.Heartbeat("job-0001", "worker", time.Minute); err != nil {
		t.Fatal(err)
	}
	queue.Close()

	payload, err := os.ReadFile(journalPath(path))
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(payload), "\n"); lines != 2 {
		t.Fatalf("expected two journal records, found %d", lines)
	}
	payload[crcWidth+10] ^= 0x01 // damage the first record, not the last
	if err := os.WriteFile(journalPath(path), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenQueue(path); !errors.Is(err, errCorruptJournal) {
		t.Fatalf("opened a queue whose acknowledged history is damaged: %v", err)
	}
}

func TestRecoverySkipsRecordsTheSnapshotReflects(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.json")
	clock := &testClock{now: time.Unix(1_000, 0).UTC()}
	queue := openTestQueue(t, path, clock)
	queue.compactMin = 1
	mustCreate(t, queue, "job-0001") // the first change writes a snapshot
	mustLease(t, queue, "worker", time.Minute, "job-0001")
	stale, err := os.ReadFile(journalPath(path))
	if err != nil || len(stale) == 0 {
		t.Fatalf("the lease was not journaled: %v", err)
	}
	// With one journal record per job, the next change writes a snapshot
	// and empties the journal.
	if _, err := queue.Complete("job-0001", "worker"); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(journalPath(path)); err != nil || info.Size() != 0 {
		t.Fatalf("journal was not emptied by the snapshot: %v %v", info, err)
	}
	before := snapshotOf(t, queue)
	queue.Close()

	// A crash between the snapshot and emptying the journal leaves the old
	// records beside a snapshot that already includes them.
	if err := os.WriteFile(journalPath(path), stale, 0o600); err != nil {
		t.Fatal(err)
	}
	reopened := openTestQueue(t, path, clock)
	if after := snapshotOf(t, reopened); after != before {
		t.Fatalf("old journal records were replayed over a newer snapshot:\nbefore %s\nafter  %s", before, after)
	}
	mustBeEmpty(t, reopened)
}

func TestOpenSnapshotFromReleaseWithoutJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.json")
	created := time.Unix(1_000, 0).UTC()
	legacy := map[string]any{
		"jobs": map[string]Job{
			"job-0001": {ID: "job-0001", IdempotencyKey: "upload:job-0001", InputPath: "/in", OutputPath: "/out", State: Queued, MaxAttempts: 5, CreatedAt: created, UpdatedAt: created},
		},
		"idempotency_keys": map[string]string{"upload:job-0001": "job-0001"},
	}
	payload, _ := json.MarshalIndent(legacy, "", "  ")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	// A journal left from before a downgrade is older than this snapshot.
	staleJob := Job{ID: "job-0001", State: Completed}
	stale, _ := json.Marshal(journalRecord{Seq: 7, ID: "job-0001", Job: staleJob})
	line := fmt.Sprintf("%08x %s\n", crc32.ChecksumIEEE(stale), stale)
	if err := os.WriteFile(journalPath(path), []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}

	clock := &testClock{now: created.Add(time.Minute)}
	queue := openTestQueue(t, path, clock)
	leased := mustLease(t, queue, "worker", time.Minute, "job-0001")
	queue.Close()

	var stored queueFile
	payload, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(payload, &stored) != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if stored.JournalSeq == 0 || stored.Jobs["job-0001"].LeaseOwner != "worker" {
		t.Fatalf("first change did not write a journal-aware snapshot: %s", payload)
	}
	reopened := openTestQueue(t, path, clock)
	if job, _ := reopened.Get("job-0001"); job.State != Leased || job.Attempts != leased.Attempts {
		t.Fatalf("stale journal was replayed: %#v", job)
	}
}

// TestLeaseMatchesScan drives a queue through random operations, clock
// changes (including the clock stepping backwards), and restarts, and
// checks every lease against the full scan that Lease used to perform.
func TestLeaseMatchesScan(t *testing.T) {
	random := rand.New(rand.NewSource(20261007))
	path := filepath.Join(t.TempDir(), "jobs.json")
	clock := &testClock{now: time.Unix(1_000, 0).UTC()}
	queue := openTestQueue(t, path, clock)
	queue.compactMin = 16
	owners := []string{"worker-a", "worker-b", "worker-c"}
	created := 0
	for step := 0; step < 1500; step++ {
		switch roll := random.Intn(100); {
		case roll < 15:
			mustCreate(t, queue, fmt.Sprintf("job-%04d", created))
			created++
			if random.Intn(3) == 0 {
				clock.Advance(time.Duration(random.Intn(3)) * time.Second)
			}
		case roll < 45:
			want, ok := scanForLease(queue, clock.Now())
			got, err := queue.Lease(owners[random.Intn(len(owners))], time.Duration(1+random.Intn(20))*time.Second)
			if !ok {
				if !errors.Is(err, ErrNoJob) {
					t.Fatalf("step %d: leased %q (%v), scan found nothing", step, got.ID, err)
				}
				continue
			}
			if err != nil || got.ID != want {
				t.Fatalf("step %d: leased %q (%v), scan chose %q", step, got.ID, err, want)
			}
		case roll < 75:
			id, owner, ok := randomLeased(queue, random)
			if !ok {
				continue
			}
			switch random.Intn(3) {
			case 0:
				_, _ = queue.Heartbeat(id, owner, time.Duration(1+random.Intn(20))*time.Second)
			case 1:
				_, _ = queue.Complete(id, owner)
			default:
				_, _ = queue.Fail(id, owner, "injected", time.Duration(random.Intn(4))*time.Second)
			}
		case roll < 78:
			for _, job := range queue.List() {
				if job.State == Dead {
					if _, err := queue.ReplayDead(job.ID); err != nil {
						t.Fatal(err)
					}
					break
				}
			}
		case roll < 95:
			clock.Advance(time.Duration(random.Intn(8000)) * time.Millisecond)
		case roll < 97:
			clock.Advance(-time.Duration(random.Intn(5000)) * time.Millisecond)
		default:
			before := snapshotOf(t, queue)
			if err := queue.Close(); err != nil {
				t.Fatal(err)
			}
			queue = openTestQueue(t, path, clock)
			queue.compactMin = 16
			if after := snapshotOf(t, queue); after != before {
				t.Fatalf("step %d: state changed across reopen", step)
			}
		}
	}
}

// scanForLease is the job selection Lease made before it had an index: the
// oldest leasable job by CreatedAt. The ID breaks ties, as the index does.
func scanForLease(queue *Queue, now time.Time) (string, bool) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	var best Job
	found := false
	for _, job := range queue.jobs {
		eligible := job.State == Queued && !job.NextAttempt.After(now) || job.State == Leased && !job.LeaseUntil.After(now)
		if !eligible {
			continue
		}
		if !found || job.CreatedAt.Before(best.CreatedAt) || job.CreatedAt.Equal(best.CreatedAt) && job.ID < best.ID {
			best, found = job, true
		}
	}
	return best.ID, found
}

func randomLeased(queue *Queue, random *rand.Rand) (string, string, bool) {
	var leased []Job
	for _, job := range queue.List() {
		if job.State == Leased {
			leased = append(leased, job)
		}
	}
	if len(leased) == 0 {
		return "", "", false
	}
	job := leased[random.Intn(len(leased))]
	return job.ID, job.LeaseOwner, true
}

// writeSnapshot stores a queue of size queued jobs the way OpenQueue reads
// it, so the benchmarks run unchanged against any queue implementation.
func writeSnapshot(b *testing.B, path string, size int) {
	b.Helper()
	jobs := make(map[string]Job, size)
	keys := make(map[string]string, size)
	for index := 0; index < size; index++ {
		id := fmt.Sprintf("job-%08d", index)
		created := time.Unix(int64(index), 0).UTC()
		jobs[id] = Job{ID: id, IdempotencyKey: "upload:" + id, InputPath: "/in/" + id, OutputPath: "/out/" + id, State: Queued, MaxAttempts: 5, CreatedAt: created, UpdatedAt: created}
		keys["upload:"+id] = id
	}
	// journal_seq marks the snapshot as current, so the timed leases do not
	// include the one-off snapshot an upgraded queue writes on its first
	// change. A queue without the journal ignores the field.
	payload, err := json.Marshal(map[string]any{"jobs": jobs, "idempotency_keys": keys, "journal_seq": 1})
	if err != nil {
		b.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		b.Fatal(err)
	}
}

// benchmarkLease measures one durable lease from a queue that holds size
// queued jobs. The clock advances 1 ms per lease and each lease lasts size/2
// ms, so after the first size/2 leases half the jobs are leased and half are
// ready, and every lease also returns one expired lease to the ready set.
// Run with -benchtime=50000x or more to include that steady state and the
// periodic snapshot rewrites in the average.
func benchmarkLease(b *testing.B, size int) {
	path := filepath.Join(b.TempDir(), "jobs.json")
	writeSnapshot(b, path, size)
	queue, err := OpenQueue(path)
	if err != nil {
		b.Fatal(err)
	}
	defer queue.Close()
	clock := time.Unix(int64(size), 0).UTC()
	queue.now = func() time.Time { return clock }
	lease := time.Duration(size/2) * time.Millisecond
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		clock = clock.Add(time.Millisecond)
		if _, err := queue.Lease("benchmark-worker", lease); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLeaseFromTenThousandJobs(b *testing.B)     { benchmarkLease(b, 10_000) }
func BenchmarkLeaseFromHundredThousandJobs(b *testing.B) { benchmarkLease(b, 100_000) }
