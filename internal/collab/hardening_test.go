package collab

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These tests pin down behaviour that an adversarial review of the first
// version found missing. Each one failed before its fix.

func TestFrameLengthNearMaxIsRefusedAndTheEditorLeaves(t *testing.T) {
	store, server, id := socketServer(t)
	path := "/api/docs/" + id + "/ws"
	watcher := dialSocket(t, server, path)
	watcher.sendJSON(hello("watcher"))
	watcher.readMessage("init")
	broken := dialSocket(t, server, path)
	broken.sendJSON(hello("broken"))
	broken.readMessage("init")
	watcher.readMessage("presence")

	// A one-byte fragment followed by a continuation claiming 2^64-1 bytes.
	// Adding the two lengths wraps to zero, which once slipped past the
	// size check and panicked the handler before it could clean up.
	broken.writeFrame(false, opText, []byte("{"), true)
	frame := binary.BigEndian.AppendUint64([]byte{0x80 | opContinuation, 0x80 | 127}, ^uint64(0))
	if _, err := broken.conn.Write(append(frame, 1, 2, 3, 4)); err != nil {
		t.Fatal(err)
	}
	broken.expectClose(closeTooLarge)
	if update := watcher.readMessage("presence"); len(update["clients"].([]any)) != 1 {
		t.Fatalf("the broken connection is still listed: %v", update)
	}
	if snapshot, _ := store.Read(id); snapshot.Doc.Editors != 1 {
		t.Fatalf("expected one editor, got %d", snapshot.Doc.Editors)
	}
}

func TestSecondProcessOnTheSameDirectoryIsRefused(t *testing.T) {
	dir := t.TempDir()
	first, err := OpenStore(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if second, err := OpenStore(dir, Options{}); err == nil {
		second.Close()
		t.Fatal("two stores opened the same directory; each would overwrite what the other acknowledged")
	} else if !strings.Contains(err.Error(), "in use") {
		t.Fatalf("unexpected error %v", err)
	}
	first.Close()
	reopened, err := OpenStore(dir, Options{})
	if err != nil {
		t.Fatalf("the lock was not released on close: %v", err)
	}
	reopened.Close()
}

func TestDamagedDocumentIsSetAsideAndTheRestOpen(t *testing.T) {
	dir := t.TempDir()
	store, _ := OpenStore(dir, Options{})
	good, _ := store.Create("Good", "factoid", Author{ID: "u", Name: "U"})
	bad, _ := store.Create("Bad", "factoid", Author{ID: "u", Name: "U"})
	store.Close()

	path := filepath.Join(dir, bad.ID+logSuffix)
	before, _ := os.ReadFile(path)
	damaged := append([]byte(nil), before...)
	damaged[12] ^= 0xff
	os.WriteFile(path, damaged, 0o600)
	// A creation that crashed before its first record leaves an empty log.
	os.WriteFile(filepath.Join(dir, "0123456789abcdef"+logSuffix), nil, 0o600)

	store, err := OpenStore(dir, Options{})
	if err != nil {
		t.Fatalf("one damaged log kept every document from opening: %v", err)
	}
	defer store.Close()
	if docs := store.List(); len(docs) != 1 || docs[0].ID != good.ID {
		t.Fatalf("expected only the good document, got %+v", docs)
	}
	if set := store.Damaged(); len(set) != 1 || set[0].ID != bad.ID || !strings.Contains(set[0].Error, "corrupt") {
		t.Fatalf("the damaged document was not reported: %+v", set)
	}
	if after, _ := os.ReadFile(path); string(after) != string(damaged) {
		t.Fatal("the damaged log was modified; it must be left for a person to inspect")
	}
}

func TestOperationIDsAreScopedToTheirClient(t *testing.T) {
	store := openTestStore(t, Options{})
	id, s := newDoc(t, store, "")
	alice, _ := attach(t, s, "alice")
	bob, _ := attach(t, s, "bob")
	zero := 0
	// Two clients happen to pick, or one deliberately copies, the same id.
	alice.say(clientMsg{T: "op", Rev: &zero, ID: "same:1", Op: NewOp().InsertString("AAA")})
	bob.say(clientMsg{T: "op", Rev: &zero, ID: "same:1", Op: NewOp().InsertString("BBB")})
	// Bob never learns Alice's ids, live or when catching up, so he cannot
	// claim them. Alice does get her own back.
	var seen wireOp
	alice.expect("ack", nil)
	bob.expect("op", &seen)
	bob.expect("ack", nil)
	if got := docText(t, store, id); got != "BBBAAA" {
		t.Fatalf("one of the two edits was dropped as a duplicate: %q", got)
	}
	if seen.ID != "" {
		t.Fatalf("bob was told alice's operation id %q", seen.ID)
	}
	catchUp := func(name string) []wireOp {
		p := &peer{t: t, s: s, conn: newConn(nil), name: name}
		p.say(clientMsg{T: "hello", Client: name, User: &Author{ID: name, Name: name}, Rev: &zero})
		var init initMsg
		p.expect("init", &init)
		return init.Ops
	}
	for _, op := range catchUp("bob") {
		if op.Author.Name == "alice" && op.ID != "" {
			t.Fatalf("catch-up told bob alice's operation id %q", op.ID)
		}
	}
	own := 0
	for _, op := range catchUp("alice") {
		if op.ID == "same:1" && op.Author.Name == "alice" {
			own++
		}
	}
	if own != 1 {
		t.Fatalf("alice should see her own operation id once in catch-up, saw it %d times", own)
	}
}

// A client that reconnects while its operation is still arriving on the old
// connection must hear about the commit on the new one, with the id.
func TestOwnOperationEchoReachesTheNewConnection(t *testing.T) {
	store := openTestStore(t, Options{})
	_, s := newDoc(t, store, "")
	old, _ := attach(t, s, "tab")
	fresh, _ := attach(t, s, "tab")
	other, _ := attach(t, s, "someone-else")
	zero := 0
	old.say(clientMsg{T: "op", Rev: &zero, ID: "tab:1", Op: NewOp().InsertString("x")})
	var echo, plain wireOp
	fresh.expect("op", &echo)
	other.expect("op", &plain)
	if echo.ID != "tab:1" || plain.ID != "" {
		t.Fatalf("echo id %q to the same client, %q to another", echo.ID, plain.ID)
	}
}

func TestOperationsThatWouldCorruptTextAreRejected(t *testing.T) {
	store := openTestStore(t, Options{})
	id, s := newDoc(t, store, "a\U0001F600b")
	for name, op := range map[string]*Op{
		"delete half a pair":       NewOp().Retain(1).Delete(1).Retain(2),
		"delete the other half":    NewOp().Retain(2).Delete(1).Retain(1),
		"insert inside a pair":     NewOp().Retain(2).InsertString("x").Retain(2),
		"insert a carriage return": NewOp().Retain(4).InsertString("line\r\n"),
	} {
		p, _ := attach(t, s, "client-"+name)
		rev := p.rev
		p.say(clientMsg{T: "op", Rev: &rev, ID: "x", Op: op})
		p.expect("error", nil)
	}
	if got := docText(t, store, id); got != "a\U0001F600b" {
		t.Fatalf("text changed: %q", got)
	}
	p, _ := attach(t, s, "fine")
	p.edit(NewOp().Retain(1).Delete(2).Retain(1))
	if got := docText(t, store, id); got != "ab" {
		t.Fatalf("deleting a whole pair should work: %q", got)
	}

	var op Op
	for _, invalid := range []string{`[-2147483648,"x"]`, `[4294967296]`, `[-9007199254740993]`} {
		if err := json.Unmarshal([]byte(invalid), &op); err == nil {
			t.Fatalf("%s should be rejected, decoded as %s", invalid, mustJSON(op))
		}
	}
}

// One operation that is both very stale and very fragmented used to occupy a
// document's actor for minutes. It is now refused in bounded time.
func TestVeryStaleFragmentedOperationIsRefusedQuickly(t *testing.T) {
	s := newSession(newDocState("costdoc000001"), &memoryLog{})
	s.run(func(b *batch) {
		s.commit(b, record{Kind: recMeta, Time: s.now(), Title: "Cost"})
		seed := Author{ID: "seed", Name: "seed", Kind: KindSystem}
		s.commit(b, record{Kind: recOp, Time: s.now(), Rev: 1, Op: NewOp().InsertString(strings.Repeat("a", 30_000)), Author: &seed})
	})
	typist := newConn(nil)
	s.run(func(b *batch) {
		s.handle(b, typist, clientMsg{T: "hello", Client: "typist", User: &Author{Name: "typist"}})
	})
	for index := 0; index < 2000; index++ {
		rev := s.state.rev
		op := NewOp().Retain(len(s.state.text)).InsertString("k")
		s.run(func(b *batch) { s.handle(b, typist, clientMsg{T: "op", Rev: &rev, ID: fmt.Sprint(index), Op: op}) })
		for len(typist.out) > 0 {
			<-typist.out
		}
	}
	fragmented := NewOp()
	for index := 0; index < maxOpComponents/2; index++ {
		fragmented.Retain(1).Delete(1)
	}
	fragmented.Retain(30_000 - maxOpComponents)
	lurker := newConn(nil)
	one := 1
	started := time.Now()
	s.run(
		func(b *batch) {
			s.handle(b, lurker, clientMsg{T: "hello", Client: "lurker", User: &Author{Name: "lurker"}})
		},
		func(b *batch) { s.handle(b, lurker, clientMsg{T: "op", Rev: &one, ID: "stale", Op: fragmented}) },
	)
	if elapsed := time.Since(started); elapsed > 30*time.Second {
		t.Fatalf("the actor was busy for %v", elapsed)
	}
	<-lurker.out // init
	if rejection := string(<-lurker.out); !strings.Contains(rejection, "too far behind") {
		t.Fatalf("expected a refusal, got %s", rejection)
	}
	if s.state.rev != 2001 {
		t.Fatalf("the stale operation was committed at revision %d", s.state.rev)
	}

	// An ordinary edit from just as far back still merges.
	patient := newConn(nil)
	s.run(
		func(b *batch) {
			s.handle(b, patient, clientMsg{T: "hello", Client: "patient", User: &Author{Name: "patient"}})
		},
		func(b *batch) {
			s.handle(b, patient, clientMsg{T: "op", Rev: &one, ID: "late", Op: NewOp().InsertString("!").Retain(30_000)})
		},
	)
	if s.state.rev != 2002 || s.state.text[0] != '!' {
		t.Fatal("an ordinary stale edit should still be merged")
	}
}

func TestCloseWritesEverythingAlreadyQueued(t *testing.T) {
	for round := 0; round < 60; round++ {
		dir := t.TempDir()
		store, _ := OpenStore(dir, Options{})
		info, _ := store.Create("Doc", "blank", Author{ID: "u", Name: "U"})
		s, _ := store.session(info.ID)
		p, _ := attach(t, s, "p")
		s.do(func(*batch) { time.Sleep(2 * time.Millisecond) })
		for index := 0; index < 20; index++ {
			rev := index
			p.say(clientMsg{T: "op", Rev: &rev, ID: fmt.Sprint(index), Op: NewOp().Retain(index * 10).InsertString(strings.Repeat("x", 10))})
		}
		store.Close()
		reopened, err := OpenStore(dir, Options{})
		if err != nil {
			t.Fatal(err)
		}
		got := docText(t, reopened, info.ID)
		reopened.Close()
		if len(got) != 200 {
			t.Fatalf("round %d: close dropped queued edits, %d of 200 characters survived", round, len(got))
		}
	}
}

func TestAskReturnsTheResultOfWorkCommittedInTheFinalBatch(t *testing.T) {
	for round := 0; round < 100; round++ {
		log := &memoryLog{}
		s := newSession(newDocState("askdoc0000001"), log)
		s.looping = true
		go s.loop()
		// Hold the actor inside one batch so the request and the stop
		// are certain to be queued together behind it.
		gate, entered := make(chan struct{}), make(chan struct{})
		s.do(func(*batch) { close(entered); <-gate })
		<-entered
		result := make(chan error, 1)
		go func() {
			_, err := ask(s, func(b *batch) (struct{}, error) {
				_, err := s.commit(b, record{Kind: recMeta, Time: time.Now().UTC(), Title: "created"})
				return struct{}{}, err
			})
			result <- err
		}()
		for len(s.inbox) < 1 {
			time.Sleep(50 * time.Microsecond)
		}
		stopped := make(chan struct{})
		go func() { s.stop(); close(stopped) }()
		for len(s.inbox) < 2 {
			time.Sleep(50 * time.Microsecond)
		}
		close(gate)
		err := <-result
		<-stopped
		if err != nil && len(log.snapshot()) == 1 {
			t.Fatalf("round %d: caller was told %v although its record is in the log", round, err)
		}
	}
}

func TestListAnswersWhileADocumentIsBusy(t *testing.T) {
	store := openTestStore(t, Options{})
	id, s := newDoc(t, store, "busy text")
	gate := make(chan struct{})
	defer close(gate)
	s.do(func(*batch) { <-gate })
	answered := make(chan []DocInfo, 1)
	go func() { answered <- store.List() }()
	select {
	case docs := <-answered:
		if len(docs) != 1 || docs[0].ID != id || docs[0].Length != 9 {
			t.Fatalf("unexpected listing %+v", docs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("listing waited on a busy document")
	}
	if summary, ok := store.Summary(id); !ok || summary.Snippet != "busy text" {
		t.Fatalf("unexpected summary %+v", summary)
	}
}

func TestRefusedInvokeDoesNotSpendTheBudget(t *testing.T) {
	agent := &scriptedAgent{reply: func(AgentRequest) (AgentResult, error) { return AgentResult{Text: "x"}, nil }}
	store := openTestStore(t, Options{Agents: []Agent{agent}, AgentRunsPerHour: 2})
	id, _ := newDoc(t, store, "text")
	for attempt := 0; attempt < 5; attempt++ {
		if _, err := store.Invoke(context.Background(), id, InvokeRequest{Agent: "scripted", Rev: 1, Start: 900, End: 901, User: Author{Name: "U"}}); err == nil {
			t.Fatal("a range outside the document should be refused")
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := store.Invoke(context.Background(), id, InvokeRequest{Agent: "scripted", Rev: 1, Start: 0, End: 4, User: Author{Name: "U"}}); err != nil {
			t.Fatalf("refused requests used up the budget: %v", err)
		}
	}
	if _, err := store.Invoke(context.Background(), id, InvokeRequest{Agent: "scripted", Rev: 1, Start: 0, End: 4, User: Author{Name: "U"}}); !errors.Is(err, ErrBudget) {
		t.Fatalf("expected the budget to apply to real runs, got %v", err)
	}
}

func TestAgentTextIsNormalizedAndRangesRespectCharacters(t *testing.T) {
	agent := &scriptedAgent{reply: func(AgentRequest) (AgentResult, error) { return AgentResult{Text: "one\r\ntwo\rthree"}, nil }}
	store := openTestStore(t, Options{Agents: []Agent{agent}})
	id, s := newDoc(t, store, "x\U0001F600y")
	p, _ := attach(t, s, "p")
	if _, err := store.Invoke(context.Background(), id, InvokeRequest{Agent: "scripted", Rev: p.rev, Start: 2, End: 3, User: Author{Name: "U"}}); err == nil {
		t.Fatal("a range that starts inside a surrogate pair should be refused")
	}
	result, err := store.Invoke(context.Background(), id, InvokeRequest{Agent: "scripted", Rev: p.rev, Start: 0, End: 1, Command: true, User: Author{Name: "U"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Suggestion.Text != "one\ntwo\nthree" || !result.Suggestion.Command {
		t.Fatalf("unexpected suggestion %+v", result.Suggestion)
	}
}

func TestFailedAppendIsRolledBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "doc.oplog")
	log, _, err := openFileLog(path)
	if err != nil {
		t.Fatal(err)
	}
	appendAll(t, log, `{"k":"op","rev":1}`)
	// Swap in a handle that cannot be synced or written, as after a device
	// error, and check the failed batch is not left behind for a later
	// reader to mistake for committed history.
	good := log.file
	readOnly, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	log.file = readOnly
	if err := log.Append([][]byte{[]byte(`{"k":"op","rev":2}`)}); err == nil {
		t.Fatal("append through a read-only handle should fail")
	}
	readOnly.Close()
	log.file = good
	appendAll(t, log, `{"k":"op","rev":2,"retry":true}`)
	log.Close()
	_, records, err := openFileLog(path)
	if err != nil || len(records) != 2 || !strings.Contains(string(records[1]), "retry") {
		t.Fatalf("unexpected log after a failed append: %v %v", recordStrings(records), err)
	}
}
