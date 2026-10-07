package collab

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// peer is a test editor attached straight to a session's actor, skipping the
// WebSocket so tests can inspect every message.
type peer struct {
	t    *testing.T
	s    *session
	conn *conn
	name string
	rev  int
	seq  int
}

func attach(t *testing.T, s *session, name string) (*peer, initMsg) {
	t.Helper()
	p := &peer{t: t, s: s, conn: newConn(nil), name: name}
	p.say(clientMsg{T: "hello", Client: name, User: &Author{ID: name, Name: name}})
	var init initMsg
	p.expect("init", &init)
	p.rev = init.Rev
	return p, init
}

func (p *peer) say(m clientMsg) {
	p.t.Helper()
	if !p.s.do(func(b *batch) { p.s.handle(b, p.conn, m) }) {
		p.t.Fatal("session is closed")
	}
}

// next returns the next message of the wanted type, skipping others.
func (p *peer) expect(kind string, into any) {
	p.t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case payload := <-p.conn.out:
			var envelope struct {
				T string `json:"t"`
			}
			if err := json.Unmarshal(payload, &envelope); err != nil {
				p.t.Fatal(err)
			}
			if envelope.T != kind {
				continue
			}
			if into != nil {
				if err := json.Unmarshal(payload, into); err != nil {
					p.t.Fatal(err)
				}
			}
			return
		case <-deadline:
			p.t.Fatalf("%s: timed out waiting for %q", p.name, kind)
		}
	}
}

// edit sends an operation built against the peer's current revision and
// waits for the acknowledgement.
func (p *peer) edit(op *Op) {
	p.t.Helper()
	p.seq++
	rev := p.rev
	p.say(clientMsg{T: "op", Rev: &rev, ID: p.name + ":" + string(rune('a'+p.seq)), Op: op})
	var ack struct {
		Rev int `json:"rev"`
	}
	p.expect("ack", &ack)
	p.rev = ack.Rev
}

func openTestStore(t *testing.T, opts Options) *Store {
	t.Helper()
	store, err := OpenStore(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func newDoc(t *testing.T, store *Store, text string) (string, *session) {
	t.Helper()
	info, err := store.Create("Test doc", "blank", Author{ID: "u1", Name: "Manas"})
	if err != nil {
		t.Fatal(err)
	}
	s, err := store.session(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if text != "" {
		p, _ := attach(t, s, "seed")
		p.edit(NewOp().InsertString(text))
		p.s.do(func(b *batch) { p.s.leave(b, p.conn) })
	}
	return info.ID, s
}

func docText(t *testing.T, store *Store, id string) string {
	t.Helper()
	snapshot, err := store.Read(id)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot.Text
}

func TestConcurrentEditsFromStaleRevisions(t *testing.T) {
	store := openTestStore(t, Options{})
	id, s := newDoc(t, store, "hello world")
	alice, _ := attach(t, s, "alice")
	bob, _ := attach(t, s, "bob")

	// Both edit revision 1 without having seen each other.
	alice.edit(NewOp().Retain(5).InsertString(",").Retain(6))
	bob.edit(NewOp().Retain(11).InsertString("!"))
	if got := docText(t, store, id); got != "hello, world!" {
		t.Fatalf("unexpected merge %q", got)
	}
	// Alice was told about Bob's edit in a form that fits her document.
	var remote wireOp
	alice.expect("op", &remote)
	if got := mustJSON(remote.Op); got != `[12,"!"]` || remote.Author.Name != "bob" {
		t.Fatalf("unexpected broadcast %s by %s", got, remote.Author.Name)
	}
}

func TestDuplicateOperationIsAppliedOnce(t *testing.T) {
	store := openTestStore(t, Options{})
	id, s := newDoc(t, store, "abc")
	p, _ := attach(t, s, "p")
	rev := p.rev
	op := NewOp().Retain(3).InsertString("d")
	p.say(clientMsg{T: "op", Rev: &rev, ID: "p:resend", Op: op})
	p.expect("ack", nil)
	// The same operation arrives again, as it does when a client reconnects
	// without having seen the acknowledgement.
	p.say(clientMsg{T: "op", Rev: &rev, ID: "p:resend", Op: op})
	if got := docText(t, store, id); got != "abcd" {
		t.Fatalf("duplicate was applied: %q", got)
	}
}

func TestReconnectCatchesUpFromKnownRevision(t *testing.T) {
	store := openTestStore(t, Options{})
	_, s := newDoc(t, store, "one")
	writer, _ := attach(t, s, "writer")
	reader, first := attach(t, s, "reader")
	if first.Text == nil || *first.Text != "one" {
		t.Fatalf("a new client should get the full text, got %+v", first)
	}
	writer.edit(NewOp().Retain(3).InsertString(" two"))
	writer.edit(NewOp().Retain(7).InsertString(" three"))

	// The reader comes back on a new connection knowing only revision 1.
	returning := &peer{t: t, s: s, conn: newConn(nil), name: "reader"}
	known := first.Rev
	returning.say(clientMsg{T: "hello", Client: "reader", User: &Author{ID: "reader", Name: "reader"}, Rev: &known, Epoch: first.Epoch})
	var init initMsg
	returning.expect("init", &init)
	if init.Text != nil || len(init.Ops) != 2 || init.Rev != known+2 {
		t.Fatalf("expected a two-operation catch-up, got text=%v ops=%d rev=%d", init.Text != nil, len(init.Ops), init.Rev)
	}
	_ = reader

	// A client whose revision belongs to some other history gets a reset.
	stranger := &peer{t: t, s: s, conn: newConn(nil), name: "stranger"}
	stranger.say(clientMsg{T: "hello", Client: "stranger", User: &Author{ID: "x", Name: "x"}, Rev: &known, Epoch: "another-history"})
	stranger.expect("init", &init)
	if init.Text == nil || !init.Reset {
		t.Fatal("a client from another history must be reset to the full text")
	}
}

func TestMalformedOperationDropsOnlyThatClient(t *testing.T) {
	store := openTestStore(t, Options{})
	id, s := newDoc(t, store, "abc")
	good, _ := attach(t, s, "good")
	bad, _ := attach(t, s, "bad")
	rev := bad.rev
	bad.say(clientMsg{T: "op", Rev: &rev, ID: "bad:1", Op: NewOp().Retain(99).InsertString("x")})
	bad.expect("error", nil)
	// The rejected connection is ignored from here on.
	bad.say(clientMsg{T: "op", Rev: &rev, ID: "bad:2", Op: NewOp().Retain(3).InsertString("x")})
	good.edit(NewOp().Retain(3).InsertString("d"))
	if got := docText(t, store, id); got != "abcd" {
		t.Fatalf("unexpected text %q", got)
	}
	future := 999
	late, _ := attach(t, s, "late")
	late.say(clientMsg{T: "op", Rev: &future, ID: "late:1", Op: NewOp().Retain(4)})
	late.expect("error", nil)
}

func TestRestartRebuildsTextAuthorshipAndSuggestions(t *testing.T) {
	dir := t.TempDir()
	agent := &scriptedAgent{reply: func(AgentRequest) (AgentResult, error) { return AgentResult{Text: "WORLD"}, nil }}
	store, err := OpenStore(dir, Options{Agents: []Agent{agent}})
	if err != nil {
		t.Fatal(err)
	}
	info, err := store.Create("Lab notes", "factoid", Author{ID: "u1", Name: "Manas"})
	if err != nil {
		t.Fatal(err)
	}
	s, _ := store.session(info.ID)
	p, init := attach(t, s, "manas")
	length := len(encodeText(*init.Text))
	p.edit(NewOp().Retain(length).InsertString("hello world"))
	result, err := store.Invoke(context.Background(), info.ID, InvokeRequest{Agent: "scripted", Rev: p.rev, Start: length + 6, End: length + 11, User: Author{ID: "u1", Name: "Manas"}})
	if err != nil || result.Suggestion == nil {
		t.Fatalf("invoke: %+v %v", result, err)
	}
	p.say(clientMsg{T: "resolve", SID: result.Suggestion.ID, Action: "accept"})
	p.expect("op", nil)
	before, err := store.Provenance(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	textBefore := docText(t, store, info.ID)
	if !strings.HasSuffix(textBefore, "hello WORLD") {
		t.Fatalf("accept did not apply: %q", textBefore)
	}
	store.Close()

	reopened, err := OpenStore(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := docText(t, reopened, info.ID); got != textBefore {
		t.Fatalf("text changed across restart:\n%q\n%q", textBefore, got)
	}
	after, err := reopened.Provenance(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if mustJSON(before.Spans) != mustJSON(after.Spans) || mustJSON(before.Agents) != mustJSON(after.Agents) || mustJSON(before.ByKind) != mustJSON(after.ByKind) {
		t.Fatalf("provenance changed across restart:\n%s\n%s", mustJSON(before), mustJSON(after))
	}
	if after.ByKind[KindAgent] != 5 || after.ByKind[KindHuman] != 6 || after.ByKind[KindSystem] != length {
		t.Fatalf("unexpected authorship split %v", after.ByKind)
	}
	if len(after.Agents) != 1 || after.Agents[0].Accepted != 1 || after.Agents[0].Agent.Model != "test-model" {
		t.Fatalf("unexpected agent record %+v", after.Agents)
	}
	if docs := reopened.List(); len(docs) != 1 || docs[0].Title != "Lab notes" || docs[0].Rev != before.Doc.Rev {
		t.Fatalf("unexpected listing %+v", docs)
	}
}

// When the log stops accepting writes, the session must not acknowledge the
// edit, and what clients read afterwards must be what is actually on disk.
func TestLogFailureNeverAcknowledgesAndRecoversFromDisk(t *testing.T) {
	store := openTestStore(t, Options{})
	id, s := newDoc(t, store, "durable")
	p, _ := attach(t, s, "p")
	if err := s.log.(*fileLog).file.Close(); err != nil {
		t.Fatal(err)
	}
	rev := p.rev
	p.say(clientMsg{T: "op", Rev: &rev, ID: "p:lost", Op: NewOp().Retain(7).InsertString(" and lost")})
	select {
	case <-s.dead:
	case <-time.After(5 * time.Second):
		t.Fatal("session survived a failed append")
	}
	select {
	case payload := <-p.conn.out:
		t.Fatalf("client was told something about an edit that never reached disk: %s", payload)
	default:
	}
	if got := docText(t, store, id); got != "durable" {
		t.Fatalf("store served %q, the log holds %q", got, "durable")
	}
	// The reopened session accepts edits again.
	fresh, err := store.session(id)
	if err != nil {
		t.Fatal(err)
	}
	q, _ := attach(t, fresh, "q")
	q.edit(NewOp().Retain(7).InsertString("!"))
	if got := docText(t, store, id); got != "durable!" {
		t.Fatalf("unexpected text after recovery %q", got)
	}
}

func TestPresenceAndCursors(t *testing.T) {
	store := openTestStore(t, Options{})
	_, s := newDoc(t, store, "hello world")
	alice, _ := attach(t, s, "alice")
	bob, init := attach(t, s, "bob")
	if len(init.Clients) != 2 {
		t.Fatalf("bob should see two editors, got %+v", init.Clients)
	}
	// Alice reports a caret measured before she has seen Bob's insert.
	staleRev := alice.rev
	bob.edit(NewOp().InsertString(">> ").Retain(11))
	position := 5
	alice.say(clientMsg{T: "cursor", Rev: &staleRev, Pos: &position})
	var cursor struct {
		Conn string `json:"c"`
		Pos  int    `json:"pos"`
	}
	bob.expect("cursor", &cursor)
	if cursor.Pos != 8 {
		t.Fatalf("caret should be carried across the insert to 8, got %d", cursor.Pos)
	}
	alice.s.do(func(b *batch) { alice.s.leave(b, alice.conn) })
	var update struct {
		Clients []presence `json:"clients"`
	}
	bob.expect("presence", &update)
	if len(update.Clients) != 1 || update.Clients[0].Name != "bob" {
		t.Fatalf("unexpected presence after leave: %+v", update.Clients)
	}
}

// scriptedAgent runs whatever the test tells it to.
type scriptedAgent struct {
	mu    sync.Mutex
	calls int
	reply func(AgentRequest) (AgentResult, error)
}

func (a *scriptedAgent) Info() AgentInfo {
	return AgentInfo{ID: "scripted", Name: "Scripted", Kind: "llm", Model: "test-model", Description: "test agent"}
}

func (a *scriptedAgent) Run(_ context.Context, request AgentRequest) (AgentResult, error) {
	a.mu.Lock()
	a.calls++
	a.mu.Unlock()
	return a.reply(request)
}

func (a *scriptedAgent) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

func TestAgentRereadsWhenItsTextChangedUnderneath(t *testing.T) {
	var s *session
	var typist *peer
	agent := &scriptedAgent{}
	store := openTestStore(t, Options{Agents: []Agent{agent}})
	id, s := newDoc(t, store, "The method is slow. Results follow.")
	typist, _ = attach(t, s, "typist")

	// While the agent thinks about "slow" on its first read, a person
	// rewrites that word. The second read sees the new word.
	agent.reply = func(request AgentRequest) (AgentResult, error) {
		if agent.count() == 1 {
			if request.Selection != "slow" {
				t.Errorf("first read saw %q", request.Selection)
			}
			typist.edit(NewOp().Retain(14).InsertString("fast").Delete(4).Retain(17))
		}
		return AgentResult{Text: strings.ToUpper(request.Selection)}, nil
	}
	result, err := store.Invoke(context.Background(), id, InvokeRequest{Agent: "scripted", Rev: typist.rev, Start: 14, End: 18, Instruction: "shout", User: Author{ID: "u", Name: "Typist"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempts != 2 || result.Suggestion.Status != StatusPending || result.Suggestion.Text != "FAST" || result.Suggestion.Original != "fast" {
		t.Fatalf("expected a fresh second attempt, got %+v", result.Suggestion)
	}
	typist.say(clientMsg{T: "resolve", SID: result.Suggestion.ID, Action: "accept"})
	typist.expect("op", nil)
	if got := docText(t, store, id); got != "The method is FAST. Results follow." {
		t.Fatalf("unexpected text %q", got)
	}
}

func TestAgentSuggestionGoesStaleInsteadOfLooping(t *testing.T) {
	agent := &scriptedAgent{}
	store := openTestStore(t, Options{Agents: []Agent{agent}})
	id, s := newDoc(t, store, "alpha beta gamma")
	typist, _ := attach(t, s, "typist")
	// Someone edits the target during every read.
	agent.reply = func(request AgentRequest) (AgentResult, error) {
		length := len(encodeText(docText(t, store, id)))
		typist.edit(NewOp().Retain(7).InsertString("x").Retain(length - 7))
		return AgentResult{Text: "BETA"}, nil
	}
	result, err := store.Invoke(context.Background(), id, InvokeRequest{Agent: "scripted", Rev: typist.rev, Start: 6, End: 10, User: Author{ID: "u", Name: "Typist"}})
	if err != nil {
		t.Fatal(err)
	}
	if agent.count() != maxAgentAttempts || result.Suggestion.Status != StatusStale {
		t.Fatalf("expected %d attempts then a stale suggestion, got %d attempts and %+v", maxAgentAttempts, agent.count(), result.Suggestion)
	}
	// A stale suggestion cannot be accepted, only dismissed.
	before := docText(t, store, id)
	typist.say(clientMsg{T: "resolve", SID: result.Suggestion.ID, Action: "accept"})
	var current struct {
		S Suggestion `json:"s"`
	}
	typist.expect("sug", &current)
	if current.S.Status != StatusStale || docText(t, store, id) != before {
		t.Fatal("a stale suggestion was applied")
	}
	typist.say(clientMsg{T: "resolve", SID: result.Suggestion.ID, Action: "dismiss"})
	for current.S.Status != StatusDismissed {
		typist.expect("sug", &current)
	}
}

func TestPendingSuggestionGoesStaleWhenItsTextIsEditedLater(t *testing.T) {
	agent := &scriptedAgent{reply: func(AgentRequest) (AgentResult, error) { return AgentResult{Text: "BETA"}, nil }}
	store := openTestStore(t, Options{Agents: []Agent{agent}})
	id, s := newDoc(t, store, "alpha beta gamma")
	p, _ := attach(t, s, "p")
	result, err := store.Invoke(context.Background(), id, InvokeRequest{Agent: "scripted", Rev: p.rev, Start: 6, End: 10, User: Author{ID: "u", Name: "P"}})
	if err != nil || result.Suggestion.Status != StatusPending {
		t.Fatalf("invoke: %+v %v", result, err)
	}
	// Typing in front of the range moves it but leaves it valid.
	p.edit(NewOp().InsertString(">> ").Retain(16))
	snapshot, _ := store.Read(id)
	if moved := snapshot.Suggestions[0]; moved.Status != StatusPending || moved.Start != 9 || moved.End != 13 {
		t.Fatalf("suggestion should have moved to [9,13), got %+v", moved)
	}
	// Typing inside the range invalidates it.
	p.edit(NewOp().Retain(11).InsertString("!").Retain(8))
	var update struct {
		S Suggestion `json:"s"`
	}
	for update.S.Status != StatusStale {
		p.expect("sug", &update)
	}
}

func TestAgentBudget(t *testing.T) {
	release := make(chan struct{})
	agent := &scriptedAgent{reply: func(AgentRequest) (AgentResult, error) {
		<-release
		return AgentResult{Note: "nothing"}, nil
	}}
	store := openTestStore(t, Options{Agents: []Agent{agent}, AgentRunsPerHour: 3, AgentConcurrency: 1})
	id, _ := newDoc(t, store, "text")
	invoke := func() error {
		_, err := store.Invoke(context.Background(), id, InvokeRequest{Agent: "scripted", Rev: 1, User: Author{ID: "u", Name: "U"}})
		return err
	}
	first := make(chan error, 1)
	go func() { first <- invoke() }()
	for agent.count() == 0 {
		time.Sleep(time.Millisecond)
	}
	if err := invoke(); !errors.Is(err, ErrAgentBusy) {
		t.Fatalf("expected the concurrency cap, got %v", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := invoke(); err != nil {
		t.Fatal(err)
	}
	if err := invoke(); err != nil {
		t.Fatal(err)
	}
	if err := invoke(); !errors.Is(err, ErrBudget) {
		t.Fatalf("expected the hourly cap after three runs, got %v", err)
	}
	if _, err := store.Invoke(context.Background(), id, InvokeRequest{Agent: "nobody", User: Author{Name: "U"}}); !errors.Is(err, ErrUnknownAgent) {
		t.Fatalf("expected an unknown agent error, got %v", err)
	}
}

func TestAgentShowsUpInPresenceWhileWorking(t *testing.T) {
	var watcher *peer
	seen := make(chan presence, 1)
	agent := &scriptedAgent{}
	agent.reply = func(AgentRequest) (AgentResult, error) {
		var update struct {
			Clients []presence `json:"clients"`
		}
		watcher.expect("presence", &update)
		for _, entry := range update.Clients {
			if entry.Kind == KindAgent {
				seen <- entry
			}
		}
		return AgentResult{Text: "x"}, nil
	}
	store := openTestStore(t, Options{Agents: []Agent{agent}})
	id, s := newDoc(t, store, "alpha beta")
	watcher, _ = attach(t, s, "watcher")
	if _, err := store.Invoke(context.Background(), id, InvokeRequest{Agent: "scripted", Rev: watcher.rev, Start: 6, End: 10, User: Author{ID: "u", Name: "Priya"}}); err != nil {
		t.Fatal(err)
	}
	entry := <-seen
	if entry.Name != "Scripted" || entry.InvokedBy != "Priya" || *entry.Anchor != 6 || *entry.Pos != 10 || entry.Model != "test-model" {
		t.Fatalf("unexpected agent presence %+v", entry)
	}
	var update struct {
		Clients []presence `json:"clients"`
	}
	watcher.expect("presence", &update)
	if len(update.Clients) != 1 {
		t.Fatalf("agent should leave presence when done, got %+v", update.Clients)
	}
}

// BenchmarkReplay measures how fast a document reopens from its log. Every
// operation copies the text, so the cost grows with both history and size;
// see docs/components/live-docs.md for what that means in practice.
func BenchmarkReplay(b *testing.B) {
	const keystrokes = 20_000
	author := Author{ID: "u", Name: "u", Kind: KindHuman}
	now := time.Unix(1_700_000_000, 0).UTC()
	payloads := [][]byte{[]byte(mustJSON(record{Kind: recMeta, Time: now, Title: "bench"}))}
	payloads = append(payloads, []byte(mustJSON(record{Kind: recOp, Time: now, Rev: 1, Op: NewOp().InsertString(strings.Repeat("lorem ipsum ", 1000)), Author: &author})))
	for index := 0; index < keystrokes; index++ {
		op := NewOp().Retain(12_000 + index).InsertString("k")
		payloads = append(payloads, []byte(mustJSON(record{Kind: recOp, Time: now, Rev: index + 2, Op: op, Author: &author, Client: "c", OpID: strconv.Itoa(index)})))
	}
	b.ResetTimer()
	for round := 0; round < b.N; round++ {
		if _, err := replay("benchdoc00001", payloads); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(keystrokes), "ops/replay")
}
