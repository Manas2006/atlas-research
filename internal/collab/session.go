package collab

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const (
	maxDocUnits  = 1_000_000 // largest document, in UTF-16 units
	maxBatch     = 128       // most events committed under one fsync
	sendQueue    = 512       // messages buffered per connection before it is dropped
	paletteSize  = 8         // presence colors the console knows how to draw
	maxNameRunes = 40
)

var errSessionClosed = errors.New("document session is closed")

// conn is one connected editor. Only the document actor touches its fields,
// apart from the out queue, which the connection's writer drains.
type conn struct {
	id      string
	client  string
	user    Author
	color   int
	joined  bool
	dropped bool // rejected; anything else it sent is ignored
	pos     int
	anchor  int
	hasCur  bool
	out     chan []byte
	done    chan struct{}
	once    sync.Once
	closeFn func()
}

func newConn(closeFn func()) *conn {
	return &conn{out: make(chan []byte, sendQueue), done: make(chan struct{}), closeFn: closeFn}
}

// kill drops the connection. A client that falls behind is cut loose rather
// than allowed to stall everyone else; it reconnects and catches up.
func (c *conn) kill() {
	c.once.Do(func() {
		close(c.done)
		if c.closeFn != nil {
			c.closeFn()
		}
	})
}

func (c *conn) enqueue(message []byte) {
	select {
	case <-c.done:
	case c.out <- message:
	default:
		c.kill()
	}
}

// Wire messages. Every message has a "t" field naming its type.

type clientMsg struct {
	T      string  `json:"t"`
	Client string  `json:"client,omitempty"`
	User   *Author `json:"user,omitempty"`
	Rev    *int    `json:"rev,omitempty"`
	Epoch  string  `json:"epoch,omitempty"`
	ID     string  `json:"id,omitempty"`
	Op     *Op     `json:"op,omitempty"`
	Pos    *int    `json:"pos,omitempty"`
	Anchor *int    `json:"anchor,omitempty"`
	SID    string  `json:"sid,omitempty"`
	Action string  `json:"action,omitempty"`
	Title  string  `json:"title,omitempty"`
}

// wireOp is a committed operation as clients see it. Conn names the live
// connection that made it, which lets other editors move that caret.
type wireOp struct {
	Rev    int    `json:"rev"`
	Op     *Op    `json:"op"`
	ID     string `json:"id,omitempty"`
	Conn   string `json:"c,omitempty"`
	Author Author `json:"a"`
}

type presence struct {
	Conn      string `json:"c"`
	Name      string `json:"name"`
	Kind      string `json:"kind"`
	Color     int    `json:"color"`
	Pos       *int   `json:"pos,omitempty"`
	Anchor    *int   `json:"anchor,omitempty"`
	Model     string `json:"model,omitempty"`
	InvokedBy string `json:"invoked_by,omitempty"`
}

type initMsg struct {
	T           string        `json:"t"`
	Doc         DocInfo       `json:"doc"`
	Rev         int           `json:"rev"`
	Epoch       string        `json:"epoch"`
	Text        *string       `json:"text,omitempty"`
	Ops         []wireOp      `json:"ops,omitempty"`
	Reset       bool          `json:"reset,omitempty"`
	You         string        `json:"you"`
	Color       int           `json:"color"`
	Clients     []presence    `json:"clients"`
	Suggestions []*Suggestion `json:"suggestions"`
}

// DocInfo is the summary of a document shown in lists.
type DocInfo struct {
	ID              string    `json:"id"`
	Title           string    `json:"title"`
	Rev             int       `json:"rev"`
	Length          int       `json:"length"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	Editors         int       `json:"editors"`
	OpenSuggestions int       `json:"open_suggestions"`
}

// agentRun is an agent that is currently reading or writing in a document.
// It shows up in presence like any other collaborator.
type agentRun struct {
	id        string
	agent     Author
	invokedBy string
	start     int
	end       int
}

// batch collects what one pass of the actor wants to make durable and what it
// wants to tell clients afterwards. Nothing in after runs until the records
// are on disk, so no client ever sees an edit the log could still lose.
type batch struct {
	payloads [][]byte
	after    []func()
	changed  bool
}

// session owns one document. All state changes run on a single goroutine
// (the actor), which gives every operation a place in one total order.
type session struct {
	state *docState
	log   Log
	now   func() time.Time

	inbox   chan func(*batch)
	dead    chan struct{}
	deadErr error
	once    sync.Once
	// looping is set by whoever starts the actor; done closes when it exits.
	looping bool
	done    chan struct{}
	// published is the latest durable summary. Lists and search read it
	// without queueing behind whatever the actor is busy with.
	published atomic.Pointer[Summary]

	conns    map[*conn]struct{}
	runs     map[string]*agentRun
	nextConn int
	nextSug  int
	nextRun  int

	onChange func(DocInfo, string)
	dirty    bool
}

func newSession(state *docState, log Log) *session {
	s := &session{
		state: state, log: log, now: func() time.Time { return time.Now().UTC() },
		inbox: make(chan func(*batch), 256), dead: make(chan struct{}), done: make(chan struct{}),
		conns: make(map[*conn]struct{}), runs: make(map[string]*agentRun),
	}
	// Continue numbering after anything already in the log.
	s.nextSug = len(state.order)
	s.publish()
	return s
}

// Summary is what a list or a search result needs to show for a document.
type Summary struct {
	DocInfo
	Snippet string `json:"snippet"`
}

const snippetUnits = 600

func (s *session) publish() {
	head := s.state.text
	if len(head) > snippetUnits {
		head = head[:snippetUnits]
		if isHighSurrogate(head[len(head)-1]) {
			head = head[:len(head)-1]
		}
	}
	s.published.Store(&Summary{DocInfo: s.info(), Snippet: decodeText(head)})
}

// loop is the actor. It drains whatever is queued, applies it all, makes it
// durable with one fsync, and only then releases the replies.
func (s *session) loop() {
	defer close(s.done)
	for {
		select {
		case <-s.dead:
			return
		case first := <-s.inbox:
			pending := []func(*batch){first}
		drain:
			for len(pending) < maxBatch {
				select {
				case next := <-s.inbox:
					pending = append(pending, next)
				default:
					break drain
				}
			}
			s.run(pending...)
		}
	}
}

// run executes events as one batch. The simulator calls it directly to drive
// a session without goroutines.
func (s *session) run(events ...func(*batch)) {
	if s.isDead() {
		return
	}
	b := &batch{}
	for _, event := range events {
		event(b)
	}
	if len(b.payloads) > 0 {
		if err := s.log.Append(b.payloads); err != nil {
			// Memory is now ahead of the log. Drop everything; the next
			// open replays the log, which is the truth.
			s.fail(fmt.Errorf("append to operation log: %w", err))
			return
		}
	}
	s.publish()
	for _, deliver := range b.after {
		deliver()
	}
	if b.changed {
		s.scheduleIndex()
	}
}

func (s *session) isDead() bool {
	select {
	case <-s.dead:
		return true
	default:
		return false
	}
}

// do queues an event for the actor. It reports false once the session is gone.
func (s *session) do(event func(*batch)) bool {
	select {
	case <-s.dead:
		return false
	default:
	}
	select {
	case s.inbox <- event:
		return true
	case <-s.dead:
		return false
	}
}

// ask runs fn on the actor and waits for its result, which is released only
// after anything fn committed is durable.
func ask[T any](s *session, fn func(*batch) (T, error)) (T, error) {
	type outcome struct {
		value T
		err   error
	}
	reply := make(chan outcome, 1)
	var zero T
	if !s.do(func(b *batch) {
		value, err := fn(b)
		b.after = append(b.after, func() { reply <- outcome{value, err} })
	}) {
		return zero, errSessionClosed
	}
	select {
	case result := <-reply:
		return result.value, result.err
	case <-s.dead:
	}
	// The session ended, but possibly in the very batch that carried this
	// request, in which case the work was committed and the reply is on its
	// way. Let the actor finish before deciding it was lost.
	if s.looping {
		<-s.done
	}
	select {
	case result := <-reply:
		return result.value, result.err
	default:
		return zero, errSessionClosed
	}
}

func (s *session) fail(err error) {
	s.once.Do(func() {
		s.deadErr = err
		close(s.dead)
	})
	for c := range s.conns {
		c.kill()
	}
}

// stop ends the session cleanly. The request queues behind everything
// already accepted, and the log is closed only after the actor has written
// its last batch and exited.
func (s *session) stop() {
	s.do(func(*batch) { s.fail(errSessionClosed) })
	if s.looping {
		<-s.done
	}
	_ = s.log.Close()
}

func (s *session) scheduleIndex() {
	if s.onChange == nil || s.dirty {
		return
	}
	s.dirty = true
	time.AfterFunc(indexDelay, func() {
		s.do(func(b *batch) {
			s.dirty = false
			// Hand over the text only after this batch is durable, so the
			// index never holds words the log could still lose.
			info, text := s.info(), decodeText(s.state.text)
			b.after = append(b.after, func() { s.onChange(info, text) })
		})
	})
}

// indexDelay bounds how stale the search index can be for a live document.
const indexDelay = 1500 * time.Millisecond

func (s *session) info() DocInfo {
	open := 0
	for _, suggestion := range s.state.suggestions {
		if suggestion.Status == StatusPending || suggestion.Status == StatusStale {
			open++
		}
	}
	return DocInfo{
		ID: s.state.id, Title: s.state.title, Rev: s.state.rev, Length: len(s.state.text),
		CreatedAt: s.state.createdAt, UpdatedAt: s.state.updatedAt, Editors: len(s.conns), OpenSuggestions: open,
	}
}

// commit applies a record to memory and queues it for the log. The record is
// encoded immediately so later changes to anything it points at cannot leak
// into what gets written.
func (s *session) commit(b *batch, r record) ([]*Suggestion, error) {
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	changed, err := s.state.apply(r)
	if err != nil {
		return nil, err
	}
	b.payloads = append(b.payloads, payload)
	b.changed = true
	return changed, nil
}

func encode(message any) []byte {
	payload, err := json.Marshal(message)
	if err != nil {
		panic(fmt.Sprintf("collab: cannot encode %T: %v", message, err))
	}
	return payload
}

func (s *session) send(b *batch, c *conn, message any) {
	payload := encode(message)
	b.after = append(b.after, func() { c.enqueue(payload) })
}

// broadcast sends to every joined connection except skip. Recipients are
// fixed now, so someone who joins later in the batch does not get a message
// about a state their init already includes.
func (s *session) broadcast(b *batch, skip *conn, message any) {
	payload := encode(message)
	recipients := make([]*conn, 0, len(s.conns))
	for c := range s.conns {
		if c != skip {
			recipients = append(recipients, c)
		}
	}
	if len(recipients) == 0 {
		return
	}
	b.after = append(b.after, func() {
		for _, c := range recipients {
			c.enqueue(payload)
		}
	})
}

// reject tells a client its message was unusable and drops the connection.
// The client reconnects and resynchronizes from its last known revision.
func (s *session) reject(b *batch, c *conn, reason string) {
	payload := encode(map[string]string{"t": "error", "message": reason})
	c.dropped = true
	s.leave(b, c)
	b.after = append(b.after, func() {
		c.enqueue(payload)
		c.enqueue(nil) // nil asks the writer to close after flushing
	})
}

func (s *session) handle(b *batch, c *conn, m clientMsg) {
	if c.dropped {
		return
	}
	if m.T == "hello" {
		s.handleHello(b, c, m)
		return
	}
	if !c.joined {
		s.reject(b, c, "hello required before any other message")
		return
	}
	switch m.T {
	case "ping":
		// An application-level heartbeat. Browsers answer protocol pings on
		// their own but never show them to scripts, so a client cannot use
		// those to notice a connection that died silently.
		s.send(b, c, map[string]string{"t": "pong"})
	case "op":
		s.handleOp(b, c, m)
	case "cursor":
		s.handleCursor(b, c, m)
	case "resolve":
		s.handleResolve(b, c, m)
	case "title":
		s.handleTitle(b, c, m)
	default:
		s.reject(b, c, fmt.Sprintf("unknown message type %q", m.T))
	}
}

func cleanText(value string, maxRunes int) string {
	value = strings.Join(strings.Fields(value), " ")
	if utf8.RuneCountInString(value) > maxRunes {
		value = string([]rune(value)[:maxRunes])
	}
	return value
}

func (s *session) pickColor() int {
	used := make(map[int]bool, len(s.conns))
	for c := range s.conns {
		used[c.color] = true
	}
	for color := 0; color < paletteSize; color++ {
		if !used[color] {
			return color
		}
	}
	return s.nextConn % paletteSize
}

func (s *session) handleHello(b *batch, c *conn, m clientMsg) {
	if c.joined {
		s.reject(b, c, "hello sent twice")
		return
	}
	if m.User == nil || cleanText(m.User.Name, maxNameRunes) == "" || cleanText(m.Client, 64) == "" {
		s.reject(b, c, "hello needs a client id and a user name")
		return
	}
	s.nextConn++
	c.id = fmt.Sprintf("c%d", s.nextConn)
	c.client = cleanText(m.Client, 64)
	c.user = Author{ID: cleanText(m.User.ID, 64), Name: cleanText(m.User.Name, maxNameRunes), Kind: KindHuman}
	if c.user.ID == "" {
		c.user.ID = c.client
	}
	c.color = s.pickColor()

	init := initMsg{T: "init", Rev: s.state.rev, Epoch: s.epoch(), You: c.id, Color: c.color, Suggestions: s.state.openSuggestions()}
	// A returning client can only be caught up if its revision refers to
	// this document's history and not to an earlier one with the same id.
	sameHistory := m.Epoch == "" || m.Epoch == init.Epoch
	if m.Rev != nil && sameHistory && *m.Rev >= 0 && *m.Rev <= s.state.rev {
		// The client already has the document at an older revision. Send
		// only what it missed so it can rebase edits it made while offline.
		init.Ops = make([]wireOp, 0, s.state.rev-*m.Rev)
		for index := *m.Rev; index < s.state.rev; index++ {
			entry := s.state.history[index]
			missed := wireOp{Rev: index + 1, Op: entry.op, Author: s.state.authors[entry.author]}
			if entry.client == c.client {
				// Only the client that made an operation is told its id,
				// so it can recognise a commit whose acknowledgement it
				// never received. Nobody else has any use for it.
				missed.ID = entry.id
			}
			init.Ops = append(init.Ops, missed)
		}
	} else {
		text := decodeText(s.state.text)
		init.Text = &text
		init.Reset = m.Rev != nil
	}
	c.joined = true
	s.conns[c] = struct{}{}
	init.Doc = s.info()
	init.Clients = s.presenceList()
	s.send(b, c, init)
	s.broadcast(b, c, map[string]any{"t": "presence", "clients": init.Clients})
}

// epoch identifies this document's history. It changes if a document is
// ever recreated under the same id, which makes old revisions meaningless.
func (s *session) epoch() string {
	return strconv.FormatInt(s.state.createdAt.UnixNano(), 36)
}

func (s *session) presenceList() []presence {
	list := make([]presence, 0, len(s.conns)+len(s.runs))
	for c := range s.conns {
		entry := presence{Conn: c.id, Name: c.user.Name, Kind: KindHuman, Color: c.color}
		if c.hasCur {
			pos, anchor := c.pos, c.anchor
			entry.Pos, entry.Anchor = &pos, &anchor
		}
		list = append(list, entry)
	}
	for _, run := range s.runs {
		start, end := run.start, run.end
		list = append(list, presence{Conn: run.id, Name: run.agent.Name, Kind: KindAgent, Color: -1, Pos: &end, Anchor: &start, Model: run.agent.Model, InvokedBy: run.invokedBy})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Conn < list[j].Conn })
	return list
}

func (s *session) leave(b *batch, c *conn) {
	if _, joined := s.conns[c]; !joined {
		return
	}
	delete(s.conns, c)
	s.broadcast(b, nil, map[string]any{"t": "presence", "clients": s.presenceList()})
}

func (s *session) handleOp(b *batch, c *conn, m clientMsg) {
	if m.Op == nil || m.Rev == nil || m.ID == "" || len(m.ID) > 128 {
		s.reject(b, c, "operation needs rev, id, and op")
		return
	}
	if _, duplicate := s.state.opIDs[opKey(c.client, m.ID)]; duplicate {
		// A resend of something already committed, typically after a
		// reconnect that lost the acknowledgement. The client learns the
		// revision from the catch-up or broadcast that carried the original.
		return
	}
	op, err := s.state.catchUp(m.Op, *m.Rev)
	if err != nil {
		s.reject(b, c, err.Error())
		return
	}
	if op.targetLen > maxDocUnits {
		s.reject(b, c, "document would exceed the size limit")
		return
	}
	if err := op.checkText(s.state.text); err != nil {
		s.reject(b, c, err.Error())
		return
	}
	author := c.user
	changed, err := s.commit(b, record{Kind: recOp, Time: s.now(), Rev: s.state.rev + 1, Op: op, Author: &author, Client: c.client, OpID: m.ID})
	if err != nil {
		s.reject(b, c, err.Error())
		return
	}
	s.afterOp(b, c, op, m.ID, author, changed)
}

// afterOp moves carets and agent ranges through a committed operation and
// tells everyone about it. origin is nil for operations no connection sent.
func (s *session) afterOp(b *batch, origin *conn, op *Op, id string, author Author, changed []*Suggestion) {
	for c := range s.conns {
		if c == origin || !c.hasCur {
			continue
		}
		c.pos, c.anchor = op.MapPos(c.pos, false), op.MapPos(c.anchor, false)
	}
	for _, run := range s.runs {
		run.start, run.end, _ = op.MapRange(run.start, run.end)
	}
	type opMsg struct {
		T string `json:"t"`
		wireOp
	}
	message := wireOp{Rev: s.state.rev, Op: op, Author: author}
	if origin != nil {
		if position, ok := op.EditPos(); ok {
			origin.pos, origin.anchor, origin.hasCur = position, position, true
		}
		message.Conn = origin.id
		s.send(b, origin, map[string]any{"t": "ack", "rev": s.state.rev, "id": id})
		// The same client may already be back on a newer connection while
		// this operation arrived on its old one. That connection gets the
		// id, which it will recognise as its own pending operation.
		echo := message
		echo.ID = id
		payload := encode(opMsg{"op", echo})
		for c := range s.conns {
			if c != origin && c.client == origin.client {
				same := c
				b.after = append(b.after, func() { same.enqueue(payload) })
			}
		}
	}
	payload := encode(opMsg{"op", message})
	for c := range s.conns {
		if c != origin && (origin == nil || c.client != origin.client) {
			other := c
			b.after = append(b.after, func() { other.enqueue(payload) })
		}
	}
	for _, suggestion := range changed {
		copied := *suggestion
		s.broadcast(b, nil, map[string]any{"t": "sug", "s": &copied})
	}
}

func (s *session) handleCursor(b *batch, c *conn, m clientMsg) {
	if m.Rev == nil || m.Pos == nil {
		return
	}
	pos, anchor := *m.Pos, *m.Pos
	if m.Anchor != nil {
		anchor = *m.Anchor
	}
	rev := *m.Rev
	if rev < 0 || rev > s.state.rev {
		return
	}
	if length := s.state.lengthAt(rev); pos < 0 || pos > length || anchor < 0 || anchor > length {
		return
	}
	for _, later := range s.state.history[rev:] {
		pos, anchor = later.op.MapPos(pos, false), later.op.MapPos(anchor, false)
	}
	c.pos, c.anchor, c.hasCur = pos, anchor, true
	s.broadcast(b, c, map[string]any{"t": "cursor", "c": c.id, "pos": pos, "anchor": anchor})
}

func (s *session) handleResolve(b *batch, c *conn, m clientMsg) {
	suggestion, ok := s.state.suggestions[m.SID]
	if !ok {
		return
	}
	by := c.user
	switch m.Action {
	case "accept":
		if suggestion.Status != StatusPending {
			// Someone else resolved it, or its text changed. Send the
			// current state so this client stops offering to accept it.
			copied := *suggestion
			s.send(b, c, map[string]any{"t": "sug", "s": &copied})
			return
		}
		start, end := suggestion.Start, suggestion.End
		if suggestion.Placement == PlaceAfter {
			start = end
		}
		op := replaceOp(len(s.state.text), start, end, encodeText(suggestion.Text))
		if op.targetLen > maxDocUnits {
			return
		}
		agent := suggestion.Agent
		changed, err := s.commit(b, record{Kind: recOp, Time: s.now(), Rev: s.state.rev + 1, Op: op, Author: &agent, Accepts: suggestion.ID, By: &by})
		if err != nil {
			return
		}
		s.afterOp(b, nil, op, "", agent, changed)
	case "reject", "dismiss":
		if suggestion.Status != StatusPending && suggestion.Status != StatusStale {
			return
		}
		action := StatusRejected
		if suggestion.Status == StatusStale {
			action = StatusDismissed
		}
		changed, err := s.commit(b, record{Kind: recResolve, Time: s.now(), SuggestionID: suggestion.ID, Action: action, By: &by})
		if err != nil {
			return
		}
		for _, resolved := range changed {
			copied := *resolved
			s.broadcast(b, nil, map[string]any{"t": "sug", "s": &copied})
		}
	}
}

func (s *session) handleTitle(b *batch, c *conn, m clientMsg) {
	title := cleanText(m.Title, 120)
	if title == "" || title == s.state.title {
		return
	}
	if _, err := s.commit(b, record{Kind: recMeta, Time: s.now(), Title: title}); err != nil {
		return
	}
	s.broadcast(b, nil, map[string]any{"t": "title", "title": title})
}
