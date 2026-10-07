package search

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// replica is a fake search node that answers with a single result naming
// itself, optionally after a stall.
type replica struct {
	name      string
	stall     time.Duration
	status    int
	requests  atomic.Int32
	cancelled chan struct{}
	server    *httptest.Server
}

func newReplica(t *testing.T, name string, stall time.Duration, status int) *replica {
	t.Helper()
	rep := &replica{name: name, stall: stall, status: status, cancelled: make(chan struct{}, 16)}
	rep.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rep.requests.Add(1)
		if rep.stall > 0 {
			select {
			case <-time.After(rep.stall):
			case <-r.Context().Done():
				rep.cancelled <- struct{}{}
				return
			}
		}
		if rep.status != http.StatusOK {
			writeJSON(w, rep.status, map[string]string{"error": "unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, QueryResponse{Results: []Result{{ID: rep.name, Score: 1}}})
	}))
	t.Cleanup(rep.server.Close)
	return rep
}

// deadURL returns the address of a server that has already shut down, so
// connections to it are refused immediately.
func deadURL(t *testing.T) string {
	t.Helper()
	server := httptest.NewServer(http.NotFoundHandler())
	address := server.URL
	server.Close()
	return address
}

type searchReply struct {
	Results      []Result `json:"results"`
	FailedShards int      `json:"failed_shards"`
	Error        string   `json:"error"`
}

func coordinatorSearch(t *testing.T, coordinator *Coordinator) (searchReply, int, time.Duration) {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/v1/search?q=hedge&limit=5", nil)
	recorder := httptest.NewRecorder()
	started := time.Now()
	coordinator.Handler().ServeHTTP(recorder, request)
	elapsed := time.Since(started)
	var reply searchReply
	if err := json.NewDecoder(recorder.Body).Decode(&reply); err != nil {
		t.Fatalf("decode coordinator reply: %v", err)
	}
	return reply, recorder.Code, elapsed
}

func expectWinner(t *testing.T, reply searchReply, code int, want string) {
	t.Helper()
	if code != http.StatusOK || reply.FailedShards != 0 || len(reply.Results) != 1 || reply.Results[0].ID != want {
		t.Fatalf("status %d reply %+v, want one result from %s", code, reply, want)
	}
}

func TestCoordinatorHedgesStalledReplica(t *testing.T) {
	const hedgeDelay = 50 * time.Millisecond
	stalled := newReplica(t, "stalled", 2*time.Second, http.StatusOK)
	healthy := newReplica(t, "healthy", 0, http.StatusOK)
	coordinator := NewCoordinator([]ReplicaSet{{Name: "a", Replicas: []string{stalled.server.URL, healthy.server.URL}}})
	if coordinator.HedgeDelay != hedgeDelay {
		t.Fatalf("default hedge delay %s, want %s", coordinator.HedgeDelay, hedgeDelay)
	}

	for round := 0; round < 3; round++ {
		reply, code, elapsed := coordinatorSearch(t, coordinator)
		expectWinner(t, reply, code, "healthy")
		// The healthy replica is only asked after the hedge delay, and it
		// answers at once, so the query takes about one hedge delay rather
		// than the 2s stall.
		if elapsed < hedgeDelay || elapsed > hedgeDelay+400*time.Millisecond {
			t.Fatalf("round %d: query took %s, want close to the %s hedge delay", round, elapsed, hedgeDelay)
		}
		t.Logf("round %d: %s", round, elapsed)
		select {
		case <-stalled.cancelled:
		case <-time.After(time.Second):
			t.Fatalf("round %d: the losing request to the stalled replica was not cancelled", round)
		}
	}
	if got := stalled.requests.Load(); got != 3 {
		t.Fatalf("stalled replica saw %d requests, want 3", got)
	}
}

func TestCoordinatorFailsOverDeadReplicaImmediately(t *testing.T) {
	// A long hedge delay shows that failover does not wait for it.
	const hedgeDelay = 5 * time.Second
	for _, tc := range []struct {
		name  string
		first func(t *testing.T) string
	}{
		{"connection refused", deadURL},
		{"server error", func(t *testing.T) string {
			return newReplica(t, "broken", 0, http.StatusInternalServerError).server.URL
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			healthy := newReplica(t, "healthy", 0, http.StatusOK)
			coordinator := NewCoordinator([]ReplicaSet{{Name: "a", Replicas: []string{tc.first(t), healthy.server.URL}}})
			coordinator.HedgeDelay = hedgeDelay
			reply, code, elapsed := coordinatorSearch(t, coordinator)
			expectWinner(t, reply, code, "healthy")
			if elapsed > 500*time.Millisecond {
				t.Fatalf("failover took %s; it should not wait for the %s hedge delay", elapsed, hedgeDelay)
			}
			t.Logf("failover in %s", elapsed)
		})
	}
}

func TestCoordinatorDoesNotHedgeFastReplica(t *testing.T) {
	primary := newReplica(t, "primary", 0, http.StatusOK)
	secondary := newReplica(t, "secondary", 0, http.StatusOK)
	coordinator := NewCoordinator([]ReplicaSet{{Name: "a", Replicas: []string{primary.server.URL, secondary.server.URL}}})
	coordinator.HedgeDelay = time.Second
	for round := 0; round < 5; round++ {
		reply, code, _ := coordinatorSearch(t, coordinator)
		expectWinner(t, reply, code, "primary")
	}
	if got := secondary.requests.Load(); got != 0 {
		t.Fatalf("secondary replica saw %d requests without any need to hedge", got)
	}
}

func TestCoordinatorReportsShardWhenAllReplicasFail(t *testing.T) {
	broken := newReplica(t, "broken", 0, http.StatusServiceUnavailable)
	healthy := newReplica(t, "healthy", 0, http.StatusOK)
	coordinator := NewCoordinator([]ReplicaSet{
		{Name: "down", Replicas: []string{deadURL(t), broken.server.URL}},
		{Name: "up", Replicas: []string{healthy.server.URL}},
	})
	reply, code, elapsed := coordinatorSearch(t, coordinator)
	if code != http.StatusOK || reply.FailedShards != 1 || len(reply.Results) != 1 || reply.Results[0].ID != "healthy" {
		t.Fatalf("status %d reply %+v, want healthy results with one failed shard", code, reply)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("query took %s although every failure was immediate", elapsed)
	}

	coordinator.Shards = coordinator.Shards[:1]
	reply, code, _ = coordinatorSearch(t, coordinator)
	if code != http.StatusServiceUnavailable || reply.Error == "" {
		t.Fatalf("status %d reply %+v, want 503 when every shard is down", code, reply)
	}
}

func TestCoordinatorHedgingDisabled(t *testing.T) {
	slow := newReplica(t, "slow", 150*time.Millisecond, http.StatusOK)
	other := newReplica(t, "other", 0, http.StatusOK)
	coordinator := NewCoordinator([]ReplicaSet{{Name: "a", Replicas: []string{slow.server.URL, other.server.URL}}})
	coordinator.HedgeDelay = -1
	reply, code, _ := coordinatorSearch(t, coordinator)
	expectWinner(t, reply, code, "slow")
	if got := other.requests.Load(); got != 0 {
		t.Fatalf("hedging is disabled but the second replica saw %d requests", got)
	}
}
