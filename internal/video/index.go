package video

import (
	"container/heap"
	"time"
)

// leaseIndex finds the job Lease should hand out without scanning the queue.
//
// A job is leasable when it is queued and its next attempt time has passed,
// or when it is leased and its lease has expired. Every queued or leased job
// has exactly one entry, held in one of two heaps:
//
//   - waiting holds jobs that become leasable at a known time (the next
//     attempt for a queued job, the lease expiry for a leased one), earliest
//     first.
//   - ready holds jobs whose time has passed, in lease order: oldest
//     CreatedAt first, then ID, so jobs created in the same instant still
//     have a fixed order.
//
// Completed and dead jobs can never be leased again without another state
// change, so they have no entry. Every operation is O(log n), apart from a
// lookup that first moves k newly due entries to ready, O(k log n), which is
// at most once per entry per state change.
type leaseIndex struct {
	entries map[string]*indexEntry
	waiting jobHeap
	ready   jobHeap
	// now is the latest time passed to next. An entry already due then
	// goes straight to ready, which keeps new jobs out of the waiting heap.
	now time.Time
}

type indexEntry struct {
	id        string
	createdAt time.Time
	due       time.Time // when the job becomes leasable
	ready     bool      // which heap holds the entry
	position  int       // the entry's slot in that heap
}

func newLeaseIndex() *leaseIndex {
	return &leaseIndex{
		entries: make(map[string]*indexEntry),
		waiting: jobHeap{less: dueBefore},
		ready:   jobHeap{less: leaseOrder},
	}
}

// leasableAt reports when a job becomes leasable, and false for a job that
// never will in its current state.
func leasableAt(job Job) (time.Time, bool) {
	switch job.State {
	case Queued:
		return job.NextAttempt, true
	case Leased:
		return job.LeaseUntil, true
	}
	return time.Time{}, false
}

// update files the job stored under id according to its current state. It
// must run after every change to a job.
func (x *leaseIndex) update(id string, job Job) {
	due, leasable := leasableAt(job)
	entry := x.entries[id]
	if entry != nil {
		x.detach(entry)
	}
	if !leasable {
		delete(x.entries, id)
		return
	}
	if entry == nil {
		entry = &indexEntry{id: id}
		x.entries[id] = entry
	}
	entry.createdAt, entry.due = job.CreatedAt, due
	entry.ready = !due.After(x.now)
	if entry.ready {
		heap.Push(&x.ready, entry)
	} else {
		heap.Push(&x.waiting, entry)
	}
}

// remove forgets a job that no longer exists.
func (x *leaseIndex) remove(id string) {
	if entry := x.entries[id]; entry != nil {
		x.detach(entry)
		delete(x.entries, id)
	}
}

func (x *leaseIndex) detach(entry *indexEntry) {
	if entry.ready {
		heap.Remove(&x.ready, entry.position)
	} else {
		heap.Remove(&x.waiting, entry.position)
	}
}

// next returns the ID of the first job in lease order among those leasable
// at now. It does not change the job; the caller leases it and calls update.
func (x *leaseIndex) next(now time.Time) (string, bool) {
	x.now = now
	for x.waiting.Len() > 0 && !x.waiting.entries[0].due.After(now) {
		entry := heap.Pop(&x.waiting).(*indexEntry)
		entry.ready = true
		heap.Push(&x.ready, entry)
	}
	for x.ready.Len() > 0 {
		entry := x.ready.entries[0]
		if !entry.due.After(now) {
			return entry.id, true
		}
		// The clock went backwards after this entry became ready, so the
		// job is not leasable yet. It waits again for its time. Entries
		// below it in ready that are not due either cannot be returned
		// before they reach the top, where this check catches them.
		heap.Pop(&x.ready)
		entry.ready = false
		heap.Push(&x.waiting, entry)
	}
	return "", false
}

func leaseOrder(a, b *indexEntry) bool {
	if !a.createdAt.Equal(b.createdAt) {
		return a.createdAt.Before(b.createdAt)
	}
	return a.id < b.id
}

func dueBefore(a, b *indexEntry) bool {
	if !a.due.Equal(b.due) {
		return a.due.Before(b.due)
	}
	return a.id < b.id
}

// jobHeap implements heap.Interface. Each entry tracks its own position so
// it can be removed in O(log n) when its job changes.
type jobHeap struct {
	entries []*indexEntry
	less    func(a, b *indexEntry) bool
}

func (h *jobHeap) Len() int           { return len(h.entries) }
func (h *jobHeap) Less(i, j int) bool { return h.less(h.entries[i], h.entries[j]) }

func (h *jobHeap) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
	h.entries[i].position = i
	h.entries[j].position = j
}

func (h *jobHeap) Push(value any) {
	entry := value.(*indexEntry)
	entry.position = len(h.entries)
	h.entries = append(h.entries, entry)
}

func (h *jobHeap) Pop() any {
	last := len(h.entries) - 1
	entry := h.entries[last]
	h.entries[last] = nil
	h.entries = h.entries[:last]
	entry.position = -1
	return entry
}
