package atlas

import (
	"embed"
	"encoding/json"
	"io/fs"
	"mime"
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

	"github.com/Manas2006/atlas-research/internal/collab"
	"github.com/Manas2006/atlas-research/internal/experiments"
	"github.com/Manas2006/atlas-research/internal/search"
	"github.com/Manas2006/atlas-research/internal/tsdb"
	"github.com/Manas2006/atlas-research/internal/video"
)

//go:embed ui/*
var interfaceFiles embed.FS

func init() {
	// Go's built-in table has no entry for web fonts, and a minimal container
	// has no system table to fall back on.
	_ = mime.AddExtensionType(".woff2", "font/woff2")
}

type Server struct {
	catalog     *Catalog
	docs        *collab.Store
	origins     []string
	index       *search.Index
	wal         *search.WAL
	metrics     *tsdb.Store
	impact      *experiments.Engine
	video       http.Handler
	queue       *video.Queue
	started     time.Time
	requests    atomic.Uint64
	searchMu    sync.Mutex
	searchTimes []time.Duration
	trafficMu   sync.Mutex
	traffic     [12]requestBucket
}

type requestBucket struct {
	Minute int64  `json:"minute"`
	Count  uint64 `json:"count"`
}

type SearchResult struct {
	Entry Entry   `json:"entry"`
	Score float64 `json:"score"`
}

// Options configures optional parts of the runtime.
type Options struct {
	// Writer enables the model-backed Writer agent in live docs when set.
	Writer *collab.LLMConfig
	// AllowedOrigins limits which web pages may call the API from a
	// browser, for example https://manas2006.github.io. Pages served by this
	// process are always allowed. Empty keeps the original behaviour of
	// allowing any page, which suits a runtime that only listens on
	// localhost and is wrong for one a whole lab can reach.
	AllowedOrigins []string
}

func Open(dataDir string) (*Server, error) { return OpenWithOptions(dataDir, Options{}) }

func OpenWithOptions(dataDir string, options Options) (*Server, error) {
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
	impact, err := experiments.Open(filepath.Join(dataDir, "impact", "events.wal"))
	if err != nil {
		metrics.Close()
		wal.Close()
		return nil, err
	}
	queue, err := video.OpenQueue(filepath.Join(dataDir, "video", "jobs.json"))
	if err != nil {
		impact.Close()
		metrics.Close()
		wal.Close()
		return nil, err
	}
	objects, err := video.NewObjectStore(filepath.Join(dataDir, "video", "objects"))
	if err != nil {
		queue.Close()
		impact.Close()
		metrics.Close()
		wal.Close()
		return nil, err
	}
	server := &Server{catalog: catalog, index: index, wal: wal, metrics: metrics, impact: impact, queue: queue, video: (&video.API{Queue: queue, Store: objects}).Handler(), started: time.Now().UTC()}
	for _, origin := range options.AllowedOrigins {
		if origin = strings.TrimRight(strings.TrimSpace(origin), "/"); origin != "" {
			server.origins = append(server.origins, origin)
		}
	}

	// Live docs keep their own durable operation logs. Their text is indexed
	// here in memory and rebuilt from those logs on every start, so it never
	// needs a second copy in the knowledge WAL.
	collabOptions := collab.Options{Search: server.agentSearch, OnChange: server.indexDoc}
	if options.Writer != nil {
		writer, err := collab.NewWriter(*options.Writer)
		if err != nil {
			queue.Close()
			impact.Close()
			metrics.Close()
			wal.Close()
			return nil, err
		}
		collabOptions.Agents = append(collabOptions.Agents, writer)
	}
	docs, err := collab.OpenStore(filepath.Join(dataDir, "docs"), collabOptions)
	if err != nil {
		queue.Close()
		impact.Close()
		metrics.Close()
		wal.Close()
		return nil, err
	}
	server.docs = docs
	docs.Each(server.indexDoc)
	return server, nil
}

const docPrefix = "doc:"

func (s *Server) indexDoc(info collab.DocInfo, text string) {
	s.index.Upsert(search.Document{ID: docPrefix + info.ID, Title: info.Title, Body: text})
}

// snippet flattens text to one line of at most limit runes.
func snippet(text string, limit int) string {
	text = strings.Join(strings.Fields(text), " ")
	if runes := []rune(text); len(runes) > limit {
		return strings.TrimSpace(string(runes[:limit])) + "..."
	}
	return text
}

// lookup resolves a search hit to something displayable, whether it is a
// catalog entry or a live doc.
func (s *Server) lookup(id string) (Entry, bool) {
	// Docs are checked first so a catalog entry cannot pose as one by
	// borrowing its id.
	if docID, isDoc := strings.CutPrefix(id, docPrefix); isDoc {
		summary, ok := s.docs.Summary(docID)
		if !ok {
			return Entry{}, false
		}
		return Entry{ID: id, Title: summary.Title, Body: snippet(summary.Snippet, 280), Type: "Doc", Tags: []string{}, CreatedAt: summary.CreatedAt, UpdatedAt: summary.UpdatedAt}, true
	}
	return s.catalog.Entry(id)
}

// agentSearch is the read-only view of Atlas that agents in live docs get.
func (s *Server) agentSearch(query string, limit int) []collab.Hit {
	hits := make([]collab.Hit, 0, limit)
	for _, result := range s.index.Search(query, limit) {
		if entry, ok := s.lookup(result.ID); ok {
			hits = append(hits, collab.Hit{ID: entry.ID, Title: entry.Title, Type: entry.Type, Snippet: snippet(entry.Body, 160), Score: result.Score})
		}
	}
	return hits
}

func (s *Server) Close() error {
	// Docs first: each session flushes the edits it has already accepted.
	_ = s.docs.Close()
	// Every acknowledged media job change is already durable.
	_ = s.queue.Close()
	if err := s.impact.Close(); err != nil {
		_ = s.metrics.Close()
		_ = s.wal.Close()
		return err
	}
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
	mux.HandleFunc("GET /api/impact-experiments", s.impactExperiments)
	mux.HandleFunc("POST /api/impact-experiments", s.impactExperiments)
	mux.HandleFunc("GET /api/impact-experiments/{id}", s.impactExperiment)
	mux.HandleFunc("POST /api/impact-experiments/{id}/start", s.startImpactExperiment)
	mux.HandleFunc("POST /api/impact-experiments/{id}/stop", s.stopImpactExperiment)
	mux.HandleFunc("POST /api/impact-experiments/{id}/assignments", s.assignImpactSubject)
	mux.HandleFunc("POST /api/impact-experiments/{id}/events", s.ingestImpactEvents)
	mux.HandleFunc("POST /api/impact-experiments/{id}/demo", s.runImpactDemo)
	mux.HandleFunc("GET /api/signals", s.signals)
	mux.Handle("/api/metrics/", http.StripPrefix("/api/metrics", (&tsdb.API{Store: s.metrics}).Handler()))
	mux.Handle("/api/video/", http.StripPrefix("/api/video", s.video))
	docs := s.docs.Handler()
	mux.Handle("/api/docs", docs)
	mux.Handle("/api/docs/", docs)
	mux.Handle("/api/agents", docs)
	assets, _ := fs.Sub(interfaceFiles, "ui")
	mux.Handle("/", http.FileServerFS(assets))
	return s.cors(s.observe(mux))
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	entries, runs := s.catalog.Counts()
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "mode": "connected", "uptime_seconds": int64(time.Since(s.started).Seconds()), "documents": s.index.Len(), "entries": entries, "runs": runs, "impact_experiments": s.impact.Count(), "docs": s.docs.Count(), "metric_series": s.metrics.SeriesCount(), "media_jobs": len(s.queue.List())})
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
		if entry, ok := s.lookup(result.ID); ok {
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
	minute := time.Now().Unix() / 60
	history := make([]requestBucket, 0, len(s.traffic))
	s.trafficMu.Lock()
	for offset := int64(len(s.traffic) - 1); offset >= 0; offset-- {
		current := minute - offset
		bucket := s.traffic[current%int64(len(s.traffic))]
		if bucket.Minute != current {
			bucket = requestBucket{Minute: current}
		}
		history = append(history, bucket)
	}
	s.trafficMu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"requests": s.requests.Load(), "request_history": history, "goroutines": runtime.NumGoroutine(), "heap_bytes": memory.HeapAlloc, "search_p95_microseconds": p95.Microseconds(), "search_samples": len(times), "metric_series": s.metrics.SeriesCount(), "uptime_seconds": int64(time.Since(s.started).Seconds())})
}

func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests.Add(1)
		minute := time.Now().Unix() / 60
		s.trafficMu.Lock()
		bucket := &s.traffic[minute%int64(len(s.traffic))]
		if bucket.Minute != minute {
			*bucket = requestBucket{Minute: minute}
		}
		bucket.Count++
		s.trafficMu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && !s.originAllowed(origin, r.Host) {
			// This also covers WebSocket upgrades, which browsers send
			// with an Origin header but do not subject to CORS.
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Idempotency-Key, Upload-Offset, X-Upload-Name, Access-Control-Request-Private-Network")
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

func (s *Server) originAllowed(origin, host string) bool {
	if len(s.origins) == 0 {
		return true
	}
	if _, sameHost, found := strings.Cut(origin, "://"); found && sameHost == host {
		return true
	}
	for _, allowed := range s.origins {
		if origin == allowed {
			return true
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
