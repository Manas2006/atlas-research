package collab

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// An agent is a collaborator, not a privileged writer. It reads a document at
// some revision, takes however long it takes, and comes back with a proposal.
// By then the document has usually moved on, which is the same problem a slow
// human client poses, so the same machinery handles it: the agent's target
// range is carried forward through every operation committed since its read.
// Convergence alone is not enough for an agent, though. If someone rewrote the
// text it was working on, the proposal no longer means what the agent meant,
// so the server checks whether that text was touched and, if it was, has the
// agent read again rather than posting something out of date.

// AgentInfo describes an agent to the console.
type AgentInfo struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Kind        string `json:"kind"` // "retrieval" or "llm"
	Model       string `json:"model,omitempty"`
	Description string `json:"description"`
}

// Hit is one search result from the Atlas index.
type Hit struct {
	ID      string  `json:"id"`
	Title   string  `json:"title"`
	Type    string  `json:"type"`
	Snippet string  `json:"snippet"`
	Score   float64 `json:"score"`
}

// SearchFunc is the read-only window agents get into Atlas.
type SearchFunc func(query string, limit int) []Hit

// AgentRequest is everything an agent may look at for one run.
type AgentRequest struct {
	DocID       string
	DocTitle    string
	Before      string // document text ahead of the selection
	Selection   string
	After       string
	Instruction string
	// Command is set when the selection is the line that summoned the agent
	// ("@librarian ..."). The answer then replaces that line.
	Command   bool
	InvokedBy Author
	Search    SearchFunc
}

// AgentResult is what an agent proposes. An empty Text with a Note means the
// agent has nothing to suggest and says why.
type AgentResult struct {
	Text      string
	Placement string
	Note      string
}

// Agent is implemented by anything that can propose an edit.
type Agent interface {
	Info() AgentInfo
	Run(ctx context.Context, request AgentRequest) (AgentResult, error)
}

// InvokeRequest asks an agent to work on a range of a document. Rev is the
// revision the range was measured against.
type InvokeRequest struct {
	Agent       string `json:"agent"`
	Rev         int    `json:"rev"`
	Start       int    `json:"start"`
	End         int    `json:"end"`
	Instruction string `json:"instruction"`
	Command     bool   `json:"command"`
	User        Author `json:"user"`
}

// InvokeResult reports how a run ended.
type InvokeResult struct {
	Suggestion *Suggestion `json:"suggestion,omitempty"`
	Note       string      `json:"note,omitempty"`
	Attempts   int         `json:"attempts"`
}

const (
	// maxAgentAttempts is how many times an agent reads before its proposal
	// is posted regardless. A second stale result is shown as outdated
	// instead of looping against people who are actively typing.
	maxAgentAttempts = 2
	agentTimeout     = 90 * time.Second
	maxInstruction   = 2000
	maxSuggestion    = 40_000
)

var (
	ErrNotFound     = errors.New("not found")
	ErrBudget       = errors.New("agent budget for this document is used up; try again later")
	ErrAgentBusy    = errors.New("too many agents are already working in this document")
	ErrUnknownAgent = errors.New("unknown agent")
)

// budget caps what agents may cost per document: a rolling hourly limit on
// runs and a limit on how many run at once.
type budget struct {
	mu         sync.Mutex
	perHour    int
	concurrent int
	started    map[string][]time.Time
	running    map[string]int
}

func newBudget(perHour, concurrent int) *budget {
	return &budget{perHour: perHour, concurrent: concurrent, started: make(map[string][]time.Time), running: make(map[string]int)}
}

func (b *budget) acquire(doc string, now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	recent := b.started[doc][:0]
	for _, at := range b.started[doc] {
		if now.Sub(at) < time.Hour {
			recent = append(recent, at)
		}
	}
	b.started[doc] = recent
	if b.running[doc] >= b.concurrent {
		return ErrAgentBusy
	}
	if len(recent) >= b.perHour {
		return ErrBudget
	}
	b.started[doc] = append(recent, now)
	b.running[doc]++
	return nil
}

// release ends a run. A run that never got an answer out of its agent, for
// example because the request named a range that does not exist, is not
// counted against the hourly limit.
func (b *budget) release(doc string, charge bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.running[doc]--
	if started := b.started[doc]; !charge && len(started) > 0 {
		b.started[doc] = started[:len(started)-1]
	}
}

// agentRead is the snapshot handed to an agent.
type agentRead struct {
	rev    int
	start  int
	end    int
	title  string
	before string
	text   string
	after  string
}

// beginRead maps the requested range to the current revision, snapshots the
// text, and shows the agent in presence at that range.
func (s *session) beginRead(b *batch, runID string, agent Author, invokedBy string, rev, start, end int) (agentRead, error) {
	start, end, _, err := s.state.mapRangeFrom(rev, start, end)
	if err != nil {
		return agentRead{}, err
	}
	if insidePair(s.state.text, start) || insidePair(s.state.text, end) {
		return agentRead{}, errors.New("range splits a character")
	}
	s.runs[runID] = &agentRun{id: runID, agent: agent, invokedBy: invokedBy, start: start, end: end}
	s.broadcast(b, nil, map[string]any{"t": "presence", "clients": s.presenceList()})
	text := s.state.text
	return agentRead{
		rev: s.state.rev, start: start, end: end, title: s.state.title,
		before: decodeText(text[:start]), text: decodeText(text[start:end]), after: decodeText(text[end:]),
	}, nil
}

func (s *session) endRun(b *batch, runID string) {
	if _, running := s.runs[runID]; !running {
		return
	}
	delete(s.runs, runID)
	s.broadcast(b, nil, map[string]any{"t": "presence", "clients": s.presenceList()})
}

// proposal is the outcome of offering an agent's result to the document.
type proposal struct {
	suggestion *Suggestion
	retry      bool
	rev        int
	start      int
	end        int
}

// propose posts an agent's result as a suggestion, unless the text it read
// has been edited and it still has an attempt left, in which case it reports
// where that text is now so the agent can read it again.
func (s *session) propose(b *batch, read agentRead, draft Suggestion, attempt int) (proposal, error) {
	start, end, touched, err := s.state.mapRangeFrom(read.rev, read.start, read.end)
	if err != nil {
		return proposal{}, err
	}
	if touched && attempt < maxAgentAttempts {
		return proposal{retry: true, rev: s.state.rev, start: start, end: end}, nil
	}
	s.nextSug++
	draft.ID = fmt.Sprintf("s%d", s.nextSug)
	draft.BaseRev, draft.Rev = read.rev, s.state.rev
	draft.Start, draft.End = start, end
	draft.Original = read.text
	draft.Attempts = attempt
	draft.CreatedAt = s.now()
	draft.Status = StatusPending
	if touched {
		draft.Status = StatusStale
	}
	changed, err := s.commit(b, record{Kind: recSuggest, Time: draft.CreatedAt, Suggestion: &draft})
	if err != nil {
		return proposal{}, err
	}
	posted := *changed[0]
	s.broadcast(b, nil, map[string]any{"t": "sug", "s": &posted})
	return proposal{suggestion: &posted}, nil
}

// Invoke runs an agent against a range of a document and posts what it comes
// back with as a tracked suggestion. It blocks until the run ends.
func (st *Store) Invoke(ctx context.Context, docID string, request InvokeRequest) (InvokeResult, error) {
	agent, ok := st.agents[request.Agent]
	if !ok {
		return InvokeResult{}, ErrUnknownAgent
	}
	s, err := st.session(docID)
	if err != nil {
		return InvokeResult{}, err
	}
	invoker := Author{ID: cleanText(request.User.ID, 64), Name: cleanText(request.User.Name, maxNameRunes), Kind: KindHuman}
	if invoker.Name == "" {
		return InvokeResult{}, errors.New("invoke needs the name of the person asking")
	}
	instruction := strings.TrimSpace(request.Instruction)
	if len(instruction) > maxInstruction {
		return InvokeResult{}, errors.New("instruction is too long")
	}
	if err := st.budget.acquire(docID, st.now()); err != nil {
		return InvokeResult{}, err
	}
	answered := false
	defer func() { st.budget.release(docID, answered) }()

	ctx, cancel := context.WithTimeout(ctx, agentTimeout)
	defer cancel()
	info := agent.Info()
	author := Author{ID: info.ID, Name: info.Name, Kind: KindAgent, Model: info.Model}
	runID := fmt.Sprintf("agent:%s:%d", info.ID, st.runCounter.Add(1))
	defer ask(s, func(b *batch) (struct{}, error) { s.endRun(b, runID); return struct{}{}, nil })

	rev, start, end := request.Rev, request.Start, request.End
	for attempt := 1; ; attempt++ {
		read, err := ask(s, func(b *batch) (agentRead, error) {
			return s.beginRead(b, runID, author, invoker.Name, rev, start, end)
		})
		if err != nil {
			return InvokeResult{}, err
		}
		result, err := agent.Run(ctx, AgentRequest{
			DocID: docID, DocTitle: read.title, Before: read.before, Selection: read.text, After: read.after,
			Instruction: instruction, Command: request.Command, InvokedBy: invoker, Search: st.opts.Search,
		})
		if err != nil {
			return InvokeResult{}, fmt.Errorf("%s: %w", info.Name, err)
		}
		answered = true
		// A textarea drops carriage returns, which would leave browsers
		// counting positions differently from the server.
		result.Text = normalizeNewlines(result.Text)
		placement := result.Placement
		if placement != PlaceAfter || request.Command {
			placement = PlaceReplace
		}
		if len(result.Text) > maxSuggestion {
			return InvokeResult{}, fmt.Errorf("%s returned more text than a suggestion may hold", info.Name)
		}
		nothingToDo := result.Text == "" && (placement == PlaceAfter || read.start == read.end)
		unchanged := placement == PlaceReplace && result.Text == read.text
		if nothingToDo || unchanged {
			note := result.Note
			if note == "" {
				note = info.Name + " had nothing to suggest."
			}
			return InvokeResult{Note: note, Attempts: attempt}, nil
		}
		draft := Suggestion{Agent: author, InvokedBy: invoker, Instruction: instruction, Command: request.Command, Placement: placement, Text: result.Text, Note: result.Note}
		outcome, err := ask(s, func(b *batch) (proposal, error) { return s.propose(b, read, draft, attempt) })
		if err != nil {
			return InvokeResult{}, err
		}
		if !outcome.retry {
			return InvokeResult{Suggestion: outcome.suggestion, Note: result.Note, Attempts: attempt}, nil
		}
		rev, start, end = outcome.rev, outcome.start, outcome.end
	}
}

// Librarian is a retrieval agent. It looks up what the lab already has in
// Atlas that relates to the selected text and proposes a short reading list.
// It is deterministic and needs no model, so it works on any install.
type Librarian struct{}

func (Librarian) Info() AgentInfo {
	return AgentInfo{ID: "librarian", Name: "Librarian", Kind: "retrieval", Description: "Finds papers, notes, and docs already in Atlas that relate to the selection."}
}

const librarianHits = 5

func (Librarian) Run(_ context.Context, request AgentRequest) (AgentResult, error) {
	query := strings.TrimSpace(request.Instruction)
	if query == "" && !request.Command {
		query = strings.TrimSpace(request.Selection)
	}
	if query == "" {
		// Fall back to the line the caret is on.
		line := request.Before
		if cut := strings.LastIndexByte(line, '\n'); cut >= 0 {
			line = line[cut+1:]
		}
		rest := request.After
		if cut := strings.IndexByte(rest, '\n'); cut >= 0 {
			rest = rest[:cut]
		}
		query = strings.TrimSpace(line + rest)
	}
	if query == "" {
		return AgentResult{}, errors.New("select some text or say what to look for")
	}
	if request.Search == nil {
		return AgentResult{}, errors.New("search is not available")
	}
	var lines []string
	for _, hit := range request.Search(query, librarianHits+1) {
		if hit.ID == "doc:"+request.DocID || len(lines) == librarianHits {
			continue
		}
		line := "- " + hit.Title
		if hit.Type != "" {
			line += " (" + hit.Type + ")"
		}
		if hit.Snippet != "" {
			line += ": " + hit.Snippet
		}
		lines = append(lines, line)
	}
	if len(lines) == 0 {
		return AgentResult{Note: "Nothing related is indexed in Atlas yet."}, nil
	}
	list := "Related in Atlas:\n" + strings.Join(lines, "\n")
	if request.Command {
		return AgentResult{Text: list, Placement: PlaceReplace}, nil
	}
	return AgentResult{Text: "\n\n" + list, Placement: PlaceAfter}, nil
}
