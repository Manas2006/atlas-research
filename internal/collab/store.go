package collab

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Options configures a Store. Everything is optional.
type Options struct {
	// Search lets agents look things up in Atlas.
	Search SearchFunc
	// OnChange is called shortly after a document's text or title changes,
	// so the caller can keep a search index current.
	OnChange func(DocInfo, string)
	// Agents are offered in addition to the built-in Librarian.
	Agents []Agent
	// AgentRunsPerHour caps agent runs per document. Zero means 30.
	AgentRunsPerHour int
	// AgentConcurrency caps simultaneous runs per document. Zero means 2.
	AgentConcurrency int
}

// Store holds every collaborative document under one directory, one
// operation log per document.
type Store struct {
	dir  string
	opts Options
	now  func() time.Time

	mu       sync.Mutex
	sessions map[string]*session
	damaged  []DamagedDoc
	closed   bool
	unlock   func()

	agents     map[string]Agent
	agentOrder []string
	budget     *budget
	runCounter atomic.Int64
}

// DamagedDoc is a document whose log could not be replayed. Its file is left
// exactly as it was found.
type DamagedDoc struct {
	ID    string `json:"id"`
	Error string `json:"error"`
}

const logSuffix = ".oplog"

var docIDPattern = regexp.MustCompile(`^[a-z0-9]{8,32}$`)

// OpenStore loads every document in dir by replaying its log.
func OpenStore(dir string, opts Options) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if opts.AgentRunsPerHour <= 0 {
		opts.AgentRunsPerHour = 30
	}
	if opts.AgentConcurrency <= 0 {
		opts.AgentConcurrency = 2
	}
	unlock, err := lockDir(dir)
	if err != nil {
		return nil, err
	}
	st := &Store{
		dir: dir, opts: opts, now: func() time.Time { return time.Now().UTC() },
		sessions: make(map[string]*session), agents: make(map[string]Agent),
		budget: newBudget(opts.AgentRunsPerHour, opts.AgentConcurrency), unlock: unlock,
	}
	for _, agent := range append([]Agent{Librarian{}}, opts.Agents...) {
		id := agent.Info().ID
		if _, exists := st.agents[id]; exists {
			st.Close()
			return nil, fmt.Errorf("agent %q is registered twice", id)
		}
		st.agents[id] = agent
		st.agentOrder = append(st.agentOrder, id)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		st.Close()
		return nil, err
	}
	for _, entry := range entries {
		id, isLog := strings.CutSuffix(entry.Name(), logSuffix)
		if entry.IsDir() || !isLog || !docIDPattern.MatchString(id) {
			continue
		}
		s, err := st.openSession(id)
		if err != nil {
			// One unreadable document must not take every other one down
			// with it. Leave its file alone and say so loudly.
			log.Printf("collab: document %s is damaged and was not opened: %v", id, err)
			st.damaged = append(st.damaged, DamagedDoc{ID: id, Error: err.Error()})
			continue
		}
		if s.state.createdAt.IsZero() {
			// A log with no records is a creation that never completed.
			s.stop()
			continue
		}
		st.sessions[id] = s
	}
	return st, nil
}

// Damaged lists documents that could not be opened.
func (st *Store) Damaged() []DamagedDoc { return append([]DamagedDoc{}, st.damaged...) }

func (st *Store) openSession(id string) (*session, error) {
	log, payloads, err := openFileLog(filepath.Join(st.dir, id+logSuffix))
	if err != nil {
		return nil, err
	}
	state, err := replay(id, payloads)
	if err != nil {
		log.Close()
		return nil, err
	}
	s := newSession(state, log)
	s.now = st.now
	s.onChange = st.opts.OnChange
	s.looping = true
	go s.loop()
	return s, nil
}

// session returns the live session for a document. If a session died because
// its log failed, it is rebuilt from what the log actually holds.
func (st *Store) session(id string) (*session, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	s, ok := st.sessions[id]
	if !ok || st.closed {
		return nil, ErrNotFound
	}
	if s.isDead() {
		<-s.done
		_ = s.log.Close()
		reopened, err := st.openSession(id)
		if err != nil {
			return nil, err
		}
		st.sessions[id] = reopened
		s = reopened
	}
	return s, nil
}

func newDocID() (string, error) {
	value := make([]byte, 8)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

// Templates seed a new document. The factoid template follows the lab's
// weekly format: one section per member under a dated heading.
var templates = map[string]func(now time.Time) string{
	"blank": func(time.Time) string { return "" },
	"factoid": func(now time.Time) string {
		return "# Factoids, " + now.Format("Jan 2, 2006") + "\n\n" +
			"One section per person. Type @librarian and a topic on its own line, then press Enter, to pull related work from Atlas.\n\n" +
			"## Your name\n" +
			"Paper or talk: \n" +
			"Link: \n" +
			"Problem: \n" +
			"Method: \n" +
			"Results: \n" +
			"What interested me: \n" +
			"What could be better: \n"
	},
	"experiment": func(now time.Time) string {
		return "# Experiment notes\n\n" +
			"Started " + now.Format("Jan 2, 2006") + "\n\n" +
			"## Hypothesis\n\n## Setup\n\n## Results\n\n## Next steps\n"
	},
}

// Create makes a new document and returns once it is durable.
func (st *Store) Create(title, template string, by Author) (DocInfo, error) {
	title = cleanText(title, 120)
	if title == "" {
		return DocInfo{}, errors.New("title is required")
	}
	if template == "" {
		template = "blank"
	}
	seed, ok := templates[template]
	if !ok {
		return DocInfo{}, fmt.Errorf("unknown template %q", template)
	}
	id, err := newDocID()
	if err != nil {
		return DocInfo{}, err
	}
	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		return DocInfo{}, ErrNotFound
	}
	s, err := st.openSession(id)
	if err != nil {
		st.mu.Unlock()
		return DocInfo{}, err
	}
	st.sessions[id] = s
	st.mu.Unlock()

	info, err := ask(s, func(b *batch) (DocInfo, error) {
		now := s.now()
		creator := Author{ID: cleanText(by.ID, 64), Name: cleanText(by.Name, maxNameRunes), Kind: KindHuman}
		meta := record{Kind: recMeta, Time: now, Title: title}
		if creator.Name != "" {
			meta.By = &creator
		}
		if _, err := s.commit(b, meta); err != nil {
			return DocInfo{}, err
		}
		if text := seed(now); text != "" {
			author := Author{ID: "template:" + template, Name: "Template", Kind: KindSystem}
			op := NewOp().InsertString(text)
			if _, err := s.commit(b, record{Kind: recOp, Time: now, Rev: 1, Op: op, Author: &author}); err != nil {
				return DocInfo{}, err
			}
		}
		return s.info(), nil
	})
	if err != nil {
		// Nothing durable names this document, so do not leave an empty,
		// untitled one behind.
		st.mu.Lock()
		delete(st.sessions, id)
		st.mu.Unlock()
		s.stop()
		_ = os.Remove(filepath.Join(st.dir, id+logSuffix))
		return DocInfo{}, err
	}
	return info, nil
}

// List returns every document, most recently edited first. It reads each
// document's published summary, so it answers at once even while a document
// is busy.
func (st *Store) List() []DocInfo {
	st.mu.Lock()
	docs := make([]DocInfo, 0, len(st.sessions))
	for _, s := range st.sessions {
		if summary := s.published.Load(); summary != nil && !summary.CreatedAt.IsZero() {
			docs = append(docs, summary.DocInfo)
		}
	}
	st.mu.Unlock()
	sort.Slice(docs, func(i, j int) bool {
		if docs[i].UpdatedAt.Equal(docs[j].UpdatedAt) {
			return docs[i].ID < docs[j].ID
		}
		return docs[i].UpdatedAt.After(docs[j].UpdatedAt)
	})
	return docs
}

// Snapshot is a document's current text.
type Snapshot struct {
	Doc         DocInfo       `json:"doc"`
	Text        string        `json:"text"`
	Suggestions []*Suggestion `json:"suggestions"`
}

// Read returns a document's current state.
func (st *Store) Read(id string) (Snapshot, error) {
	s, err := st.session(id)
	if err != nil {
		return Snapshot{}, err
	}
	return ask(s, func(*batch) (Snapshot, error) {
		return Snapshot{Doc: s.info(), Text: decodeText(s.state.text), Suggestions: s.state.openSuggestions()}, nil
	})
}

// Summary returns a document's published summary without waiting on it.
func (st *Store) Summary(id string) (Summary, bool) {
	st.mu.Lock()
	s, ok := st.sessions[id]
	st.mu.Unlock()
	if !ok {
		return Summary{}, false
	}
	summary := s.published.Load()
	if summary == nil || summary.CreatedAt.IsZero() {
		return Summary{}, false
	}
	return *summary, true
}

// Each calls fn with every document's summary and text. Atlas uses it to
// build the search index at startup.
func (st *Store) Each(fn func(DocInfo, string)) {
	for _, info := range st.List() {
		if snapshot, err := st.Read(info.ID); err == nil {
			fn(snapshot.Doc, snapshot.Text)
		}
	}
}

// Count is the number of documents.
func (st *Store) Count() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.sessions)
}

// Agents lists the agents this store offers, built-ins first.
func (st *Store) Agents() []AgentInfo {
	infos := make([]AgentInfo, 0, len(st.agentOrder))
	for _, id := range st.agentOrder {
		infos = append(infos, st.agents[id].Info())
	}
	return infos
}

// AuthorShare is how much of the current text one author wrote.
type AuthorShare struct {
	Author Author `json:"author"`
	Units  int    `json:"units"`
}

// AgentStats counts what became of one agent's suggestions.
type AgentStats struct {
	Agent    Author `json:"agent"`
	Proposed int    `json:"proposed"`
	Accepted int    `json:"accepted"`
	Rejected int    `json:"rejected"`
	Stale    int    `json:"stale"`
	Pending  int    `json:"pending"`
}

// Provenance answers who wrote what. Spans cover the document in order and
// index into Authors.
type Provenance struct {
	Doc     DocInfo        `json:"doc"`
	Units   int            `json:"units"`
	ByKind  map[string]int `json:"by_kind"`
	Shares  []AuthorShare  `json:"shares"`
	Authors []Author       `json:"authors"`
	Spans   []span         `json:"spans"`
	Agents  []AgentStats   `json:"agents"`
}

// Provenance reports the authorship of a document's current text and the
// accept and reject record of each agent that has proposed edits to it.
func (st *Store) Provenance(id string) (Provenance, error) {
	s, err := st.session(id)
	if err != nil {
		return Provenance{}, err
	}
	return ask(s, func(*batch) (Provenance, error) {
		state := s.state
		// Empty lists are sent as [] and never null, so clients can iterate.
		result := Provenance{
			Doc: s.info(), Units: len(state.text), ByKind: map[string]int{},
			Shares: []AuthorShare{}, Authors: append([]Author{}, state.authors...), Spans: state.spans(), Agents: []AgentStats{},
		}
		units := make([]int, len(state.authors))
		for _, author := range state.attr {
			units[author]++
		}
		for index, count := range units {
			if count == 0 {
				continue
			}
			result.Shares = append(result.Shares, AuthorShare{Author: state.authors[index], Units: count})
			result.ByKind[state.authors[index].Kind] += count
		}
		sort.SliceStable(result.Shares, func(i, j int) bool { return result.Shares[i].Units > result.Shares[j].Units })
		stats := map[Author]*AgentStats{}
		for _, id := range state.order {
			suggestion := state.suggestions[id]
			entry, ok := stats[suggestion.Agent]
			if !ok {
				entry = &AgentStats{Agent: suggestion.Agent}
				stats[suggestion.Agent] = entry
			}
			entry.Proposed++
			switch suggestion.Status {
			case StatusAccepted:
				entry.Accepted++
			case StatusRejected:
				entry.Rejected++
			case StatusStale, StatusDismissed:
				entry.Stale++
			default:
				entry.Pending++
			}
		}
		for _, entry := range stats {
			result.Agents = append(result.Agents, *entry)
		}
		sort.Slice(result.Agents, func(i, j int) bool { return result.Agents[i].Agent.ID < result.Agents[j].Agent.ID })
		return result, nil
	})
}

// Close stops every session after it has flushed what it already accepted.
func (st *Store) Close() error {
	st.mu.Lock()
	st.closed = true
	sessions := make([]*session, 0, len(st.sessions))
	for _, s := range st.sessions {
		sessions = append(sessions, s)
	}
	st.mu.Unlock()
	for _, s := range sessions {
		s.stop()
	}
	if st.unlock != nil {
		st.unlock()
		st.unlock = nil
	}
	return nil
}

// Handler serves the documents API. Atlas mounts it at /api/docs and
// /api/agents.
func (st *Store) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/agents", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"agents": st.Agents()})
	})
	mux.HandleFunc("GET /api/docs", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"docs": st.List(), "agents": st.Agents(), "templates": templateNames(), "damaged": st.Damaged()})
	})
	mux.HandleFunc("POST /api/docs", st.handleCreate)
	mux.HandleFunc("GET /api/docs/{id}", func(w http.ResponseWriter, r *http.Request) {
		snapshot, err := st.Read(r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, snapshot)
	})
	mux.HandleFunc("GET /api/docs/{id}/provenance", func(w http.ResponseWriter, r *http.Request) {
		provenance, err := st.Provenance(r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, provenance)
	})
	mux.HandleFunc("GET /api/docs/{id}/log", st.handleLog)
	mux.HandleFunc("POST /api/docs/{id}/invoke", st.handleInvoke)
	mux.HandleFunc("GET /api/docs/{id}/ws", st.handleSocket)
	return mux
}

func templateNames() []string {
	names := make([]string, 0, len(templates))
	for name := range templates {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (st *Store) handleCreate(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Title    string `json:"title"`
		Template string `json:"template"`
		User     Author `json:"user"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&input); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "valid JSON body required"})
		return
	}
	info, err := st.Create(input.Title, input.Template, input.User)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if st.opts.OnChange != nil {
		if snapshot, err := st.Read(info.ID); err == nil {
			st.opts.OnChange(snapshot.Doc, snapshot.Text)
		}
	}
	writeJSON(w, http.StatusCreated, map[string]any{"doc": info})
}

func (st *Store) handleInvoke(w http.ResponseWriter, r *http.Request) {
	var request InvokeRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "valid JSON body required"})
		return
	}
	result, err := st.Invoke(r.Context(), r.PathValue("id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleLog streams a document's history as JSON Lines: one record per edit,
// suggestion, and decision, each with its author. This is the dataset for
// studying how people and agents write together.
func (st *Store) handleLog(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := st.session(id); err != nil {
		writeError(w, err)
		return
	}
	file, err := os.Open(filepath.Join(st.dir, id+logSuffix))
	if err != nil {
		writeError(w, err)
		return
	}
	defer file.Close()
	records, _, err := readRecords(file)
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Content-Disposition", "attachment; filename="+strconv.Quote(id+".jsonl"))
	w.Header().Set("Cache-Control", "no-store")
	for _, payload := range records {
		if _, err := w.Write(append(payload, '\n')); err != nil {
			return
		}
	}
}

// maxClientMessage must fit the largest legal operation: an insert that
// fills a whole document, with every unit escaped in JSON.
const maxClientMessage = 8 << 20

func (st *Store) handleSocket(w http.ResponseWriter, r *http.Request) {
	s, err := st.session(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	socket, err := upgradeWebSocket(w, r)
	if err != nil {
		return
	}
	serveSocket(s, socket)
}

// serveSocket pumps one WebSocket: a writer goroutine drains the outbound
// queue while this goroutine reads messages and hands them to the actor.
func serveSocket(s *session, socket *wsConn) {
	c := newConn(func() { go socket.Close() })
	// Deferred so the editor leaves presence however this function ends,
	// including a panic recovered further up by net/http.
	defer func() {
		c.kill()
		s.do(func(b *batch) { s.leave(b, c) })
	}()
	go func() {
		ping := time.NewTicker(wsPingInterval)
		defer ping.Stop()
		for {
			select {
			case <-c.done:
				return
			case message := <-c.out:
				if message == nil || socket.WriteText(message) != nil {
					c.kill()
					return
				}
			case <-ping.C:
				if socket.WritePing() != nil {
					c.kill()
					return
				}
			}
		}
	}()
	for {
		payload, err := socket.ReadText(maxClientMessage)
		if err != nil {
			break
		}
		var message clientMsg
		if err := json.Unmarshal(payload, &message); err != nil {
			s.do(func(b *batch) { s.reject(b, c, "message is not valid JSON: "+err.Error()) })
			// Give the writer a moment to deliver the error before closing.
			select {
			case <-c.done:
			case <-time.After(time.Second):
			}
			break
		}
		if !s.do(func(b *batch) { s.handle(b, c, message) }) {
			break
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	var status int
	switch {
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrUnknownAgent):
		status = http.StatusNotFound
	case errors.Is(err, ErrBudget), errors.Is(err, ErrAgentBusy):
		status = http.StatusTooManyRequests
	case errors.Is(err, errSessionClosed):
		status = http.StatusServiceUnavailable
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
	default:
		// Anything else an agent or a bad range produced is the caller's to fix.
		status = http.StatusBadRequest
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
