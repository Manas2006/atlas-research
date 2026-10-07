package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type HTTPTransport struct {
	URLs   map[string]string
	Client *http.Client
}

func NewHTTPTransport(urls map[string]string) *HTTPTransport {
	return &HTTPTransport{URLs: urls, Client: &http.Client{Timeout: 300 * time.Millisecond}}
}

func (t *HTTPTransport) RequestVote(ctx context.Context, peer string, request RequestVoteRequest) (RequestVoteResponse, error) {
	var response RequestVoteResponse
	err := t.post(ctx, peer, "/raft/request-vote", request, &response)
	return response, err
}

func (t *HTTPTransport) AppendEntries(ctx context.Context, peer string, request AppendEntriesRequest) (AppendEntriesResponse, error) {
	var response AppendEntriesResponse
	err := t.post(ctx, peer, "/raft/append-entries", request, &response)
	return response, err
}

func (t *HTTPTransport) post(ctx context.Context, peer, path string, input, output any) error {
	baseURL, ok := t.URLs[peer]
	if !ok {
		return errors.New("unknown peer")
	}
	payload, _ := json.Marshal(input)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := t.Client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return errors.New(response.Status)
	}
	return json.NewDecoder(response.Body).Decode(output)
}

func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/request-vote", func(w http.ResponseWriter, r *http.Request) {
		var request RequestVoteRequest
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		writeKVJSON(w, http.StatusOK, n.HandleRequestVote(request))
	})
	mux.HandleFunc("POST /raft/append-entries", func(w http.ResponseWriter, r *http.Request) {
		var request AppendEntriesRequest
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		writeKVJSON(w, http.StatusOK, n.HandleAppendEntries(request))
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		role, term, leader, commit := n.Status()
		writeKVJSON(w, http.StatusOK, map[string]any{"id": n.id, "role": role, "term": term, "leader": leader, "commit_index": commit})
	})
	mux.HandleFunc("PUT /v1/kv/{key}", n.handlePut)
	mux.HandleFunc("GET /v1/kv/{key}", n.handleGet)
	mux.HandleFunc("DELETE /v1/kv/{key}", n.handleDelete)
	mux.HandleFunc("GET /v1/range", n.handleRange)
	mux.HandleFunc("POST /v1/admin/snapshot", func(w http.ResponseWriter, _ *http.Request) {
		if err := n.Snapshot(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeKVJSON(w, http.StatusOK, map[string]string{"status": "created"})
	})
	return mux
}

type putRequest struct {
	Value      string `json:"value"`
	TTLSeconds int64  `json:"ttl_seconds,omitempty"`
}

func (n *Node) handlePut(w http.ResponseWriter, r *http.Request) {
	var input putRequest
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&input) != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	command := Command{Operation: Put, Key: r.PathValue("key"), Value: input.Value}
	if input.TTLSeconds > 0 {
		command.ExpiresAt = time.Now().Add(time.Duration(input.TTLSeconds) * time.Second).UnixNano()
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	index, err := n.Propose(ctx, command)
	if err != nil {
		n.writeLeaderError(w, err)
		return
	}
	writeKVJSON(w, http.StatusOK, map[string]any{"index": index})
}

func (n *Node) handleDelete(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	index, err := n.Propose(ctx, Command{Operation: Delete, Key: r.PathValue("key")})
	if err != nil {
		n.writeLeaderError(w, err)
		return
	}
	writeKVJSON(w, http.StatusOK, map[string]any{"index": index})
}

func (n *Node) handleGet(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
	defer cancel()
	value, ok, err := n.LinearizableGet(ctx, r.PathValue("key"))
	if err != nil {
		n.writeLeaderError(w, err)
		return
	}
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeKVJSON(w, http.StatusOK, map[string]string{"key": r.PathValue("key"), "value": value})
}

func (n *Node) handleRange(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	items, err := n.LinearizableRange(r.Context(), r.URL.Query().Get("start"), r.URL.Query().Get("end"), r.URL.Query().Get("prefix"), limit)
	if err != nil {
		n.writeLeaderError(w, err)
		return
	}
	writeKVJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (n *Node) writeLeaderError(w http.ResponseWriter, err error) {
	_, _, leader, _ := n.Status()
	status := http.StatusServiceUnavailable
	if errors.Is(err, ErrNotLeader) {
		status = http.StatusTemporaryRedirect
	}
	if leaderURL, ok := n.transport.(*HTTPTransport); ok && leader != "" && leaderURL.URLs[leader] != "" {
		w.Header().Set("Location", leaderURL.URLs[leader])
	}
	writeKVJSON(w, status, map[string]string{"error": err.Error(), "leader": leader})
}

func writeKVJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
