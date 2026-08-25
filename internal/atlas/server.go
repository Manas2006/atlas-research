package atlas

import (
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Manas2006/distributed-systems-portfolio/internal/search"
	"github.com/Manas2006/distributed-systems-portfolio/internal/tsdb"
	"github.com/Manas2006/distributed-systems-portfolio/internal/video"
)

//go:embed ui/*
var interfaceFiles embed.FS

type Server struct {
	catalog     *Catalog
	index       *search.Index
	wal         *search.WAL
	metrics     *tsdb.Store
	video       http.Handler
	started     time.Time
	requests    atomic.Uint64
	searchMu    sync.Mutex
	searchTimes []time.Duration
}

type SearchResult struct {
	Entry Entry   `json:"entry"`
	Score float64 `json:"score"`
}

func Open(dataDir string) (*Server, error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, err
	}
	catalog, err := OpenCatalog(filepath.Join(dataDir, "catalog.json"))
	if err != nil {
		return nil, err
	}
	wal, err := search.OpenWAL(filepath.Join(dataDir, "knowledge.wal"))
	if err != nil {
		return nil, err
	}
	index := search.NewIndex()
	for _, entry := range catalog.Entries() {
		index.Upsert(search.Document{ID: entry.ID, Title: entry.Title, Body: entry.Body + " " + strings.Join(entry.Tags, " ")})
	}
	if err := wal.Replay(func(document search.Document) error { index.Upsert(document); return nil }); err != nil {
		wal.Close()
		return nil, err
	}
	metrics, err := tsdb.OpenStore(filepath.Join(dataDir, "metrics"), 1024)
	if err != nil {
		wal.Close()
		return nil, err
	}
	queue, err := video.OpenQueue(filepath.Join(dataDir, "video", "jobs.json"))
	if err != nil {
		metrics.Close()
		wal.Close()
		return nil, err
	}
	objects, err := video.NewObjectStore(filepath.Join(dataDir, "video", "objects"))
	if err != nil {
		metrics.Close()
		wal.Close()
		return nil, err
	}
	return &Server{catalog: catalog, index: index, wal: wal, metrics: metrics, video: (&video.API{Queue: queue, Store: objects}).Handler(), started: time.Now().UTC()}, nil
}

func (s *Server) Close() error {
	if err := s.metrics.Close(); err != nil {
		_ = s.wal.Close()
		return err
	}
	return s.wal.Close()
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", s.health)
	mux.HandleFunc("GET /api/entries", s.entries)
	mux.HandleFunc("POST /api/entries", s.entries)
	mux.HandleFunc("GET /api/search", s.search)
	mux.HandleFunc("GET /api/runs", s.runs)
	mux.HandleFunc("POST /api/runs", s.runs)
	mux.HandleFunc("PUT /api/runs/{id}", s.updateRun)
	mux.HandleFunc("GET /api/signals", s.signals)
	mux.Handle("/api/metrics/", http.StripPrefix("/api/metrics", (&tsdb.API{Store: s.metrics}).Handler()))
	mux.Handle("/api/video/", http.StripPrefix("/api/video", s.video))
	assets, _ := fs.Sub(interfaceFiles, "ui")
	mux.Handle("/", http.FileServerFS(assets))
	return s.cors(s.observe(mux))
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	entries, runs := s.catalog.Counts()
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "mode": "connected", "uptime_seconds": int64(time.Since(s.started).Seconds()), "documents": s.index.Len(), "entries": entries, "runs": runs, "metric_series": s.metrics.SeriesCount()})
}

func (s *Server) entries(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, map[string]any{"entries": s.catalog.Entries()})
		return
	}
	var entry Entry
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&entry); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "valid JSON body required"})
		return
	}
	saved, err := s.catalog.SaveEntry(entry)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	document := search.Document{ID: saved.ID, Title: saved.Title, Body: saved.Body + " " + strings.Join(saved.Tags, " ")}
	if err := s.wal.Append(document); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "entry persisted but indexing WAL failed; restart will rebuild the index"})
		return
	}
	s.index.Upsert(document)
	writeJSON(w, http.StatusCreated, map[string]any{"entry": saved})
}

func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit < 1 || limit > 100 {
		limit = 20
	}
	if query == "" {
		writeJSON(w, http.StatusOK, map[string]any{"results": []SearchResult{}})
		return
	}
	started := time.Now()
	ranked := s.index.Search(query, limit)
	elapsed := time.Since(started)
	s.searchMu.Lock()
	s.searchTimes = append(s.searchTimes, elapsed)
	if len(s.searchTimes) > 512 {
		s.searchTimes = append([]time.Duration(nil), s.searchTimes[len(s.searchTimes)-512:]...)
	}
	s.searchMu.Unlock()
	results := make([]SearchResult, 0, len(ranked))
	for _, result := range ranked {
		if entry, ok := s.catalog.Entry(result.ID); ok {
			results = append(results, SearchResult{Entry: entry, Score: result.Score})
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results, "took_microseconds": elapsed.Microseconds()})
}

func (s *Server) runs(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, map[string]any{"runs": s.catalog.Runs()})
		return
	}
	var run Run
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&run); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "valid JSON body required"})
		return
	}
	saved, err := s.catalog.SaveRun(run)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"run": saved})
}

func (s *Server) updateRun(w http.ResponseWriter, r *http.Request) {
	var run Run
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&run); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "valid JSON body required"})
		return
	}
	run.ID = r.PathValue("id")
	saved, err := s.catalog.SaveRun(run)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run": saved})
}

func (s *Server) signals(w http.ResponseWriter, _ *http.Request) {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	s.searchMu.Lock()
	times := append([]time.Duration(nil), s.searchTimes...)
	s.searchMu.Unlock()
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	var p95 time.Duration
	if len(times) > 0 {
		p95 = times[(len(times)-1)*95/100]
	}
	writeJSON(w, http.StatusOK, map[string]any{"requests": s.requests.Load(), "goroutines": runtime.NumGoroutine(), "heap_bytes": memory.HeapAlloc, "search_p95_microseconds": p95.Microseconds(), "search_samples": len(times), "metric_series": s.metrics.SeriesCount(), "uptime_seconds": int64(time.Since(s.started).Seconds())})
}

func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests.Add(1)
		next.ServeHTTP(w, r)
	})
}

func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Idempotency-Key, Upload-Offset, Access-Control-Request-Private-Network")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, OPTIONS")
			w.Header().Set("Access-Control-Allow-Private-Network", "true")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
