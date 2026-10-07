package collab

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Author identifies who produced an edit. Agents carry the model that wrote
// the text so the log records provenance down to the character.
type Author struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Kind  string `json:"kind"`
	Model string `json:"model,omitempty"`
}

const (
	KindHuman  = "human"
	KindAgent  = "agent"
	KindSystem = "system"
)

// Suggestion statuses. Only a pending suggestion can be accepted.
const (
	StatusPending   = "pending"
	StatusStale     = "stale"
	StatusAccepted  = "accepted"
	StatusRejected  = "rejected"
	StatusDismissed = "dismissed"
)

// Placement says how an accepted suggestion changes the document.
const (
	PlaceReplace = "replace" // replace the anchored range with the text
	PlaceAfter   = "after"   // keep the range and insert the text after it
)

// Suggestion is an edit an agent proposes instead of making. Start and End
// anchor it to the text the agent read; the server moves them as the document
// changes and marks the suggestion stale once that text is edited.
type Suggestion struct {
	ID          string     `json:"id"`
	Agent       Author     `json:"agent"`
	InvokedBy   Author     `json:"invoked_by"`
	Instruction string     `json:"instruction,omitempty"`
	Command     bool       `json:"command,omitempty"` // summoned by an @agent line
	BaseRev     int        `json:"base_rev"`
	Rev         int        `json:"rev"`
	Start       int        `json:"start"`
	End         int        `json:"end"`
	Placement   string     `json:"placement"`
	Original    string     `json:"original"`
	Text        string     `json:"text"`
	Note        string     `json:"note,omitempty"`
	Status      string     `json:"status"`
	Attempts    int        `json:"attempts"`
	CreatedAt   time.Time  `json:"created_at"`
	ResolvedBy  *Author    `json:"resolved_by,omitempty"`
	ResolvedAt  *time.Time `json:"resolved_at,omitempty"`
}

// Record kinds in the operation log.
const (
	recMeta    = "meta"
	recOp      = "op"
	recSuggest = "sug"
	recResolve = "res"
)

// record is one line of a document's log. The log is the only durable state:
// replaying it from the start rebuilds the text, the authorship of every
// character, and the fate of every suggestion.
type record struct {
	Kind string    `json:"k"`
	Time time.Time `json:"ts"`

	// meta
	Title string `json:"title,omitempty"`

	// op
	Rev    int     `json:"rev,omitempty"`
	Op     *Op     `json:"op,omitempty"`
	Author *Author `json:"a,omitempty"`
	// Client and OpID together identify an operation its sender may resend.
	Client string `json:"cid,omitempty"`
	OpID   string `json:"id,omitempty"`
	// Accepts names the suggestion this operation applies, and By the person
	// who accepted it. By is also who rejected or dismissed in a res record.
	Accepts string  `json:"accepts,omitempty"`
	By      *Author `json:"by,omitempty"`

	// sug
	Suggestion *Suggestion `json:"s,omitempty"`

	// res
	SuggestionID string `json:"sid,omitempty"`
	Action       string `json:"action,omitempty"`
}

// committed is an operation that has a revision number.
type committed struct {
	op     *Op
	author int
	client string
	id     string
}

// opKey scopes an operation id to the client that chose it. Ids are picked
// by clients, so without the scope one client could collide with, or
// deliberately pre-empt, another client's ids.
func opKey(client, id string) string { return client + "\x00" + id }

// docState is a document as rebuilt from its log. Every change goes through
// apply, both live and during replay, so a restart cannot disagree with what
// connected clients saw.
type docState struct {
	id        string
	title     string
	createdAt time.Time
	updatedAt time.Time

	text    []uint16
	attr    []uint32 // attr[i] indexes authors for the unit text[i]
	authors []Author
	lookup  map[Author]int

	rev     int
	history []committed    // history[i] produced revision i+1
	opIDs   map[string]int // opKey -> revision, for deduplication

	suggestions map[string]*Suggestion
	order       []string
}

func newDocState(id string) *docState {
	return &docState{id: id, lookup: make(map[Author]int), opIDs: make(map[string]int), suggestions: make(map[string]*Suggestion)}
}

func (d *docState) authorIndex(author Author) int {
	if index, ok := d.lookup[author]; ok {
		return index
	}
	d.authors = append(d.authors, author)
	d.lookup[author] = len(d.authors) - 1
	return len(d.authors) - 1
}

// apply folds one record into the state and returns the suggestions whose
// status or anchor it changed. It is deterministic: the same records in the
// same order always give the same state.
func (d *docState) apply(r record) ([]*Suggestion, error) {
	switch r.Kind {
	case recMeta:
		if d.createdAt.IsZero() {
			d.createdAt = r.Time
		}
		d.title = r.Title
		d.updatedAt = r.Time
		return nil, nil
	case recOp:
		return d.applyOp(r)
	case recSuggest:
		if r.Suggestion == nil || r.Suggestion.ID == "" {
			return nil, errors.New("suggestion record has no suggestion")
		}
		if _, exists := d.suggestions[r.Suggestion.ID]; exists {
			return nil, fmt.Errorf("duplicate suggestion %s", r.Suggestion.ID)
		}
		suggestion := *r.Suggestion
		if suggestion.Start < 0 || suggestion.End < suggestion.Start || suggestion.End > len(d.text) {
			return nil, fmt.Errorf("suggestion %s is anchored outside the document", suggestion.ID)
		}
		d.suggestions[suggestion.ID] = &suggestion
		d.order = append(d.order, suggestion.ID)
		return []*Suggestion{&suggestion}, nil
	case recResolve:
		suggestion, ok := d.suggestions[r.SuggestionID]
		if !ok {
			return nil, fmt.Errorf("unknown suggestion %s", r.SuggestionID)
		}
		switch r.Action {
		case StatusRejected, StatusDismissed:
			suggestion.Status = r.Action
		default:
			return nil, fmt.Errorf("unknown suggestion action %q", r.Action)
		}
		suggestion.ResolvedBy = r.By
		resolved := r.Time
		suggestion.ResolvedAt = &resolved
		return []*Suggestion{suggestion}, nil
	default:
		return nil, fmt.Errorf("unknown record kind %q", r.Kind)
	}
}

func (d *docState) applyOp(r record) ([]*Suggestion, error) {
	if r.Op == nil || r.Author == nil {
		return nil, errors.New("operation record is incomplete")
	}
	if r.Rev != d.rev+1 {
		return nil, fmt.Errorf("operation has revision %d, expected %d", r.Rev, d.rev+1)
	}
	var accepted *Suggestion
	if r.Accepts != "" {
		found, ok := d.suggestions[r.Accepts]
		if !ok {
			return nil, fmt.Errorf("operation accepts unknown suggestion %s", r.Accepts)
		}
		accepted = found
	}
	// Nothing above changed the state, and nothing below can fail, so a
	// rejected record leaves the document exactly as it was.
	text, err := r.Op.Apply(d.text)
	if err != nil {
		return nil, err
	}
	author := d.authorIndex(*r.Author)
	d.attr = applyAttribution(d.attr, r.Op, uint32(author))
	d.text = text
	d.rev = r.Rev
	d.updatedAt = r.Time
	d.history = append(d.history, committed{op: r.Op, author: author, client: r.Client, id: r.OpID})
	if r.OpID != "" {
		d.opIDs[opKey(r.Client, r.OpID)] = r.Rev
	}

	var changed []*Suggestion
	if accepted != nil {
		accepted.Status = StatusAccepted
		accepted.ResolvedBy = r.By
		resolved := r.Time
		accepted.ResolvedAt = &resolved
		changed = append(changed, accepted)
	}
	// Move the anchors of open suggestions. One whose text was edited can no
	// longer be applied safely, so it becomes stale. Clients move anchors the
	// same way on their own, so only a status change needs announcing.
	for _, id := range d.order {
		suggestion := d.suggestions[id]
		if suggestion.Status != StatusPending && suggestion.Status != StatusStale {
			continue
		}
		start, end, touched := r.Op.MapRange(suggestion.Start, suggestion.End)
		suggestion.Start, suggestion.End = start, end
		if touched && suggestion.Status == StatusPending {
			suggestion.Status = StatusStale
			changed = append(changed, suggestion)
		}
	}
	return changed, nil
}

// applyAttribution carries per-unit authorship through an operation: kept
// text keeps its author, inserted text belongs to the operation's author.
func applyAttribution(attr []uint32, op *Op, author uint32) []uint32 {
	out := make([]uint32, 0, op.targetLen)
	position := 0
	for _, c := range op.comps {
		switch {
		case c.isRetain():
			out = append(out, attr[position:position+c.n]...)
			position += c.n
		case c.isInsert():
			for range c.ins {
				out = append(out, author)
			}
		default:
			position -= c.n
		}
	}
	return out
}

// replay rebuilds a document from its log records.
func replay(id string, payloads [][]byte) (*docState, error) {
	state := newDocState(id)
	for index, payload := range payloads {
		var r record
		if err := json.Unmarshal(payload, &r); err != nil {
			return nil, fmt.Errorf("document %s record %d: %w", id, index, err)
		}
		if _, err := state.apply(r); err != nil {
			return nil, fmt.Errorf("document %s record %d: %w", id, index, err)
		}
	}
	return state, nil
}

// lengthAt is the document length at a revision, read from the operation
// that follows it.
func (d *docState) lengthAt(rev int) int {
	if rev < d.rev {
		return d.history[rev].op.baseLen
	}
	return len(d.text)
}

// catchUp transforms an operation made at baseRev across everything committed
// since, so it can be applied to the current document. The incoming operation
// is always the first argument to Transform; see Transform for why.
func (d *docState) catchUp(op *Op, baseRev int) (*Op, error) {
	if baseRev < 0 || baseRev > d.rev {
		return nil, fmt.Errorf("revision %d is outside 0..%d", baseRev, d.rev)
	}
	if op.baseLen != d.lengthAt(baseRev) {
		return nil, fmt.Errorf("%w at revision %d", errBaseLength, baseRev)
	}
	work := 0
	for _, earlier := range d.history[baseRev:] {
		// Transforming is linear in the size of both operations. Bound the
		// total so one very stale, very fragmented operation cannot occupy
		// the document's only goroutine for minutes.
		if work += len(op.comps) + len(earlier.op.comps); work > maxTransformWork {
			return nil, errTooStale
		}
		transformed, _, err := Transform(op, earlier.op)
		if err != nil {
			return nil, err
		}
		op = transformed
	}
	return op, nil
}

// maxTransformWork is the most component steps one incoming operation may
// cost, which keeps the worst case to a fraction of a second. An ordinary
// edit can be hundreds of thousands of revisions behind and stay under it; a
// buffer of two thousand fragments can still be a couple of thousand behind.
const maxTransformWork = 5_000_000

var errTooStale = errors.New("operation is too far behind the document to merge")

// mapRangeFrom moves a range read at baseRev to the current revision and
// reports whether any operation in between edited its content.
func (d *docState) mapRangeFrom(baseRev, start, end int) (int, int, bool, error) {
	if baseRev < 0 || baseRev > d.rev {
		return 0, 0, false, fmt.Errorf("revision %d is outside 0..%d", baseRev, d.rev)
	}
	if start < 0 || end < start || end > d.lengthAt(baseRev) {
		return 0, 0, false, fmt.Errorf("range [%d,%d) is outside the document at revision %d", start, end, baseRev)
	}
	touched := false
	for _, later := range d.history[baseRev:] {
		var hit bool
		start, end, hit = later.op.MapRange(start, end)
		touched = touched || hit
	}
	return start, end, touched, nil
}

// span is a run of consecutive units written by one author.
type span struct {
	Start  int `json:"start"`
	Length int `json:"length"`
	Author int `json:"author"`
}

func (d *docState) spans() []span {
	runs := []span{}
	for index, author := range d.attr {
		if last := len(runs) - 1; last >= 0 && runs[last].Author == int(author) {
			runs[last].Length++
			continue
		}
		runs = append(runs, span{Start: index, Length: 1, Author: int(author)})
	}
	return runs
}

func (d *docState) openSuggestions() []*Suggestion {
	open := make([]*Suggestion, 0)
	for _, id := range d.order {
		if suggestion := d.suggestions[id]; suggestion.Status == StatusPending || suggestion.Status == StatusStale {
			copied := *suggestion
			open = append(open, &copied)
		}
	}
	return open
}
