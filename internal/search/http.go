package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type QueryResponse struct {
	Results []Result `json:"results"`
}

type Node struct {
	Index *Index
	WAL   *WAL
}

func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "documents": n.Index.Len()})
	})
	mux.HandleFunc("POST /v1/documents", n.handleDocument)
	mux.HandleFunc("GET /v1/search", n.handleSearch)
	return mux
}

func (n *Node) handleDocument(w http.ResponseWriter, r *http.Request) {
	var doc Document
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&doc); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if strings.TrimSpace(doc.ID) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id is required"})
		return
	}
	if err := n.WAL.Append(doc); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "durability failure"})
		return
	}
	n.Index.Upsert(doc)
	writeJSON(w, http.StatusCreated, map[string]string{"id": doc.ID})
}

func (n *Node) handleSearch(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 100 {
		limit = 10
	}
	writeJSON(w, http.StatusOK, QueryResponse{Results: n.Index.Search(r.URL.Query().Get("q"), limit)})
}

type ReplicaSet struct {
	Name     string
	Replicas []string
}

// DefaultHedgeDelay is how long a search waits on one replica of a shard
// before also asking the next replica.
const DefaultHedgeDelay = 50 * time.Millisecond

type Coordinator struct {
	Shards []ReplicaSet
	Client *http.Client
	// HedgeDelay bounds how long a search waits for a successful response
	// from a shard replica before it also queries the next replica. Zero
	// means DefaultHedgeDelay. A negative value disables hedging, so the next
	// replica is only queried after the current one fails.
	HedgeDelay time.Duration
}

func NewCoordinator(shards []ReplicaSet) *Coordinator {
	return &Coordinator{Shards: shards, Client: &http.Client{Timeout: 2 * time.Second}, HedgeDelay: DefaultHedgeDelay}
}

func (c *Coordinator) hedgeDelay() time.Duration {
	if c.HedgeDelay == 0 {
		return DefaultHedgeDelay
	}
	return c.HedgeDelay
}

// ShardFor applies rendezvous hashing, which minimizes movement when shards
// are added or removed without requiring a centralized hash ring.
func (c *Coordinator) ShardFor(key string) (ReplicaSet, error) {
	if len(c.Shards) == 0 {
		return ReplicaSet{}, errors.New("no shards configured")
	}
	selected := c.Shards[0]
	var best uint64
	for _, shard := range c.Shards {
		hash := fnv.New64a()
		_, _ = hash.Write([]byte(key + "\x00" + shard.Name))
		if score := hash.Sum64(); score >= best {
			best, selected = score, shard
		}
	}
	return selected, nil
}

func (c *Coordinator) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "shards": len(c.Shards)})
	})
	mux.HandleFunc("POST /v1/documents", c.handleDocument)
	mux.HandleFunc("GET /v1/search", c.handleSearch)
	return mux
}

func (c *Coordinator) handleDocument(w http.ResponseWriter, r *http.Request) {
	var doc Document
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&doc); err != nil || doc.ID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "valid document id and JSON body required"})
		return
	}
	shard, err := c.ShardFor(doc.ID)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	payload, _ := json.Marshal(doc)
	type outcome struct{ err error }
	outcomes := make(chan outcome, len(shard.Replicas))
	for _, replica := range shard.Replicas {
		go func(baseURL string) {
			req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, strings.TrimRight(baseURL, "/")+"/v1/documents", strings.NewReader(string(payload)))
			if err == nil {
				req.Header.Set("Content-Type", "application/json")
				var response *http.Response
				response, err = c.Client.Do(req)
				if err == nil {
					defer response.Body.Close()
					if response.StatusCode/100 != 2 {
						err = fmt.Errorf("replica returned %s", response.Status)
					}
				}
			}
			outcomes <- outcome{err: err}
		}(replica)
	}
	successes := 0
	for range shard.Replicas {
		if (<-outcomes).err == nil {
			successes++
		}
	}
	quorum := len(shard.Replicas)/2 + 1
	if successes < quorum {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "replica quorum unavailable", "acknowledged": successes, "required": quorum})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"id": doc.ID, "shard": shard.Name, "replicas": successes})
}

func (c *Coordinator) handleSearch(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 100 {
		limit = 10
	}
	type shardResult struct {
		results []Result
		err     error
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2500*time.Millisecond)
	defer cancel()
	path := "/v1/search?q=" + url.QueryEscape(r.URL.Query().Get("q")) + "&limit=" + strconv.Itoa(limit)
	responses := make(chan shardResult, len(c.Shards))
	for _, shard := range c.Shards {
		go func(set ReplicaSet) {
			results, err := c.searchShard(ctx, set, path)
			responses <- shardResult{results: results, err: err}
		}(shard)
	}

	all := make([]Result, 0, len(c.Shards)*limit)
	failed := 0
	for range c.Shards {
		response := <-responses
		if response.err != nil {
			failed++
			continue
		}
		all = append(all, response.results...)
	}
	if failed == len(c.Shards) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "all shards unavailable"})
		return
	}
	sort.Slice(all, func(a, b int) bool { return all[a].Score > all[b].Score })
	if len(all) > limit {
		all = all[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": all, "failed_shards": failed})
}

// searchShard sends a search to one replica set with hedging. It queries the
// first replica, and each time the hedge delay passes without a successful
// response it also queries the next replica. A replica that fails is replaced
// by the next one at once, without waiting for the delay. The first successful
// response wins and the requests still in flight are cancelled. If every
// replica fails, the last error is returned.
func (c *Coordinator) searchShard(ctx context.Context, set ReplicaSet, path string) ([]Result, error) {
	if len(set.Replicas) == 0 {
		return nil, fmt.Errorf("shard %s has no replicas", set.Name)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type attempt struct {
		results []Result
		err     error
	}
	// Buffered so a losing request can always report and exit after the
	// winner has returned.
	attempts := make(chan attempt, len(set.Replicas))
	next, pending := 0, 0
	launch := func() {
		replica := set.Replicas[next]
		next++
		pending++
		go func() {
			results, err := c.searchReplica(ctx, replica, path)
			attempts <- attempt{results: results, err: err}
		}()
	}

	// A fresh timer per hedge keeps a stale tick from an earlier timer from
	// firing a hedge early.
	delay := c.hedgeDelay()
	var timer *time.Timer
	var hedge <-chan time.Time // nil, so never ready, while no hedge is due
	schedule := func() {
		if timer != nil {
			timer.Stop()
		}
		hedge = nil
		if delay >= 0 && next < len(set.Replicas) {
			timer = time.NewTimer(delay)
			hedge = timer.C
		}
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	launch()
	schedule()
	var lastErr error
	for pending > 0 {
		select {
		case outcome := <-attempts:
			pending--
			if outcome.err == nil {
				return outcome.results, nil
			}
			lastErr = outcome.err
			if next < len(set.Replicas) {
				launch()
				schedule()
			}
		case <-hedge:
			launch()
			schedule()
		case <-ctx.Done():
			// The query deadline passed or the client went away. The
			// requests in flight share ctx, so they are already cancelled.
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			return nil, lastErr
		}
	}
	return nil, lastErr
}

func (c *Coordinator) searchReplica(ctx context.Context, replica, path string) ([]Result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(replica, "/")+path, nil)
	if err != nil {
		return nil, err
	}
	response, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return nil, fmt.Errorf("replica %s returned %s", replica, response.Status)
	}
	var body QueryResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("replica %s: decode search response: %w", replica, err)
	}
	return body.Results, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
