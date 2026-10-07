package collab

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// The simulator drives one real session and several model clients through a
// seeded random schedule: edits, slow and reordered delivery, dropped
// connections, resends, late messages from dead connections, agent proposals
// that arrive after the text moved, and accept and reject decisions. Every
// run must end with all replicas holding the same text, and with a replay of
// the log reproducing the server's state exactly. A failing seed reproduces
// the same schedule every time.

type simPending struct {
	id string
	op *Op
}

// simClient follows the same rules as the browser client in
// internal/atlas/ui/collab.js: at most one operation in flight, later edits
// composed into a buffer, and incoming operations transformed across both.
type simClient struct {
	t           *testing.T
	name        string
	rng         *rand.Rand
	text        []uint16
	rev         int
	loaded      bool
	ready       bool // init received on the current connection
	outstanding *simPending
	buffer      *Op
	seq         int
	conn        *conn
	toServer    []clientMsg
}

func (c *simClient) send(m clientMsg) {
	if c.conn != nil {
		c.toServer = append(c.toServer, m)
	}
}

func (c *simClient) sendOutstanding() {
	rev := c.rev
	c.send(clientMsg{T: "op", Rev: &rev, ID: c.outstanding.id, Op: c.outstanding.op})
}

func (c *simClient) edit() {
	if !c.loaded {
		return
	}
	op := randomOp(c.rng, c.text)
	if op.IsNoop() {
		return
	}
	c.text = mustApply(c.t, c.text, op)
	if c.outstanding == nil {
		c.seq++
		c.outstanding = &simPending{id: fmt.Sprintf("%s:%d", c.name, c.seq), op: op}
		if c.ready {
			c.sendOutstanding()
		}
		return
	}
	if c.buffer == nil {
		c.buffer = op
		return
	}
	composed, err := Compose(c.buffer, op)
	if err != nil {
		c.t.Fatalf("%s compose: %v", c.name, err)
	}
	c.buffer = composed
}

func (c *simClient) acknowledge(rev int) {
	if rev != c.rev+1 {
		c.t.Fatalf("%s acknowledged at revision %d while at %d", c.name, rev, c.rev)
	}
	c.rev = rev
	c.outstanding = nil
	if c.buffer != nil {
		c.seq++
		c.outstanding = &simPending{id: fmt.Sprintf("%s:%d", c.name, c.seq), op: c.buffer}
		c.buffer = nil
		c.sendOutstanding()
	}
}

func (c *simClient) serverOp(op wireOp) {
	if c.outstanding != nil && op.ID == c.outstanding.id {
		// Our own operation coming back, which is how a client learns of a
		// commit whose acknowledgement was lost.
		c.acknowledge(op.Rev)
		return
	}
	if op.Rev != c.rev+1 {
		c.t.Fatalf("%s received revision %d while at %d", c.name, op.Rev, c.rev)
	}
	incoming := op.Op
	var err error
	if c.outstanding != nil {
		if c.outstanding.op, incoming, err = Transform(c.outstanding.op, incoming); err != nil {
			c.t.Fatalf("%s transform outstanding: %v", c.name, err)
		}
	}
	if c.buffer != nil {
		if c.buffer, incoming, err = Transform(c.buffer, incoming); err != nil {
			c.t.Fatalf("%s transform buffer: %v", c.name, err)
		}
	}
	c.text = mustApply(c.t, c.text, incoming)
	c.rev = op.Rev
}

func (c *simClient) receive(payload []byte) {
	var envelope struct {
		T string `json:"t"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		c.t.Fatal(err)
	}
	switch envelope.T {
	case "init":
		var init initMsg
		if err := json.Unmarshal(payload, &init); err != nil {
			c.t.Fatal(err)
		}
		if init.Text != nil {
			if c.loaded {
				c.t.Fatalf("%s was reset instead of caught up", c.name)
			}
			c.text, c.rev, c.loaded = encodeText(*init.Text), init.Rev, true
		} else {
			for _, op := range init.Ops {
				c.serverOp(op)
			}
			if c.rev != init.Rev {
				c.t.Fatalf("%s caught up to %d, server said %d", c.name, c.rev, init.Rev)
			}
		}
		c.ready = true
		if c.outstanding != nil {
			c.sendOutstanding()
		}
	case "op":
		var op wireOp
		if err := json.Unmarshal(payload, &op); err != nil {
			c.t.Fatal(err)
		}
		c.serverOp(op)
	case "ack":
		var ack struct {
			Rev int    `json:"rev"`
			ID  string `json:"id"`
		}
		if err := json.Unmarshal(payload, &ack); err != nil {
			c.t.Fatal(err)
		}
		if c.outstanding == nil || c.outstanding.id != ack.ID {
			c.t.Fatalf("%s got an acknowledgement for %s it was not waiting on", c.name, ack.ID)
		}
		c.acknowledge(ack.Rev)
	case "error":
		c.t.Fatalf("%s was rejected: %s", c.name, payload)
	}
}

type simulation struct {
	t       *testing.T
	rng     *rand.Rand
	session *session
	log     *memoryLog
	clients []*simClient
	// zombies are messages from connections the client already gave up on.
	// A real server can still read them off the old socket afterwards.
	zombies []struct {
		conn *conn
		msg  clientMsg
	}
	clock time.Time
	// reads are agent snapshots waiting to be proposed against a later text.
	reads []agentRead
}

func newSimulation(t *testing.T, seed int64, clients int) *simulation {
	rng := rand.New(rand.NewSource(seed))
	log := &memoryLog{}
	sim := &simulation{t: t, rng: rng, log: log, clock: time.Unix(1_700_000_000, 0).UTC()}
	sim.session = newSession(newDocState("simdoc0001"), log)
	sim.session.now = func() time.Time {
		sim.clock = sim.clock.Add(time.Millisecond)
		return sim.clock
	}
	sim.session.run(func(b *batch) {
		if _, err := sim.session.commit(b, record{Kind: recMeta, Time: sim.session.now(), Title: "Simulation"}); err != nil {
			t.Fatal(err)
		}
		seedAuthor := Author{ID: "template:sim", Name: "Template", Kind: KindSystem}
		seedOp := NewOp().InsertString(randomText(rng, 20))
		if seedOp.IsNoop() {
			return
		}
		if _, err := sim.session.commit(b, record{Kind: recOp, Time: sim.session.now(), Rev: 1, Op: seedOp, Author: &seedAuthor}); err != nil {
			t.Fatal(err)
		}
	})
	for index := 0; index < clients; index++ {
		client := &simClient{t: t, name: fmt.Sprintf("u%d", index), rng: rng}
		sim.clients = append(sim.clients, client)
		sim.connect(client)
	}
	return sim
}

func (sim *simulation) connect(c *simClient) {
	c.conn = newConn(nil)
	c.ready = false
	c.toServer = nil
	hello := clientMsg{T: "hello", Client: c.name, User: &Author{ID: c.name, Name: c.name}}
	if c.loaded {
		rev := c.rev
		hello.Rev = &rev
	}
	c.send(hello)
}

func (sim *simulation) disconnect(c *simClient) {
	if c.conn == nil {
		return
	}
	old := c.conn
	// Some of what the client sent may still reach the server later.
	for _, m := range c.toServer {
		if sim.rng.Intn(2) == 0 {
			sim.zombies = append(sim.zombies, struct {
				conn *conn
				msg  clientMsg
			}{old, m})
		} else {
			break // a later message never arrives without the earlier ones
		}
	}
	c.conn, c.toServer, c.ready = nil, nil, false
	old.kill()
	// The server learns of the drop only after it has read whatever was
	// still in flight on that socket, so the leave always comes last.
	sim.zombies = append(sim.zombies, struct {
		conn *conn
		msg  clientMsg
	}{old, clientMsg{T: "__leave"}})
}

func (sim *simulation) deliverToServer(c *simClient) bool {
	if c.conn == nil || len(c.toServer) == 0 {
		return false
	}
	m := c.toServer[0]
	c.toServer = c.toServer[1:]
	connection := c.conn
	sim.session.run(func(b *batch) { sim.session.handle(b, connection, m) })
	return true
}

func (sim *simulation) deliverZombie() bool {
	if len(sim.zombies) == 0 {
		return false
	}
	// Messages on one socket stay in order, so take the oldest for a
	// randomly chosen dead connection.
	pick := sim.zombies[sim.rng.Intn(len(sim.zombies))].conn
	for index, zombie := range sim.zombies {
		if zombie.conn != pick {
			continue
		}
		sim.zombies = append(sim.zombies[:index], sim.zombies[index+1:]...)
		sim.session.run(func(b *batch) {
			if zombie.msg.T == "__leave" {
				sim.session.leave(b, zombie.conn)
				return
			}
			sim.session.handle(b, zombie.conn, zombie.msg)
		})
		return true
	}
	return false
}

func (sim *simulation) deliverToClient(c *simClient) bool {
	if c.conn == nil {
		return false
	}
	select {
	case payload := <-c.conn.out:
		c.receive(payload)
		return true
	default:
		return false
	}
}

// agentRead snapshots a random range the way Invoke does.
func (sim *simulation) agentBegin() {
	state := sim.session.state
	start := sim.rng.Intn(len(state.text) + 1)
	end := start + sim.rng.Intn(len(state.text)-start+1)
	// Stay on character boundaries, as a browser selection would.
	for start > 0 && start < len(state.text) && state.text[start] >= 0xdc00 && state.text[start] <= 0xdfff {
		start--
	}
	for end < len(state.text) && state.text[end] >= 0xdc00 && state.text[end] <= 0xdfff {
		end++
	}
	if end < start {
		end = start
	}
	sim.reads = append(sim.reads, agentRead{rev: state.rev, start: start, end: end, text: decodeText(state.text[start:end])})
}

func (sim *simulation) agentPropose() {
	if len(sim.reads) == 0 {
		return
	}
	index := sim.rng.Intn(len(sim.reads))
	read := sim.reads[index]
	sim.reads = append(sim.reads[:index], sim.reads[index+1:]...)
	placement := PlaceReplace
	if sim.rng.Intn(3) == 0 {
		placement = PlaceAfter
	}
	draft := Suggestion{
		Agent: Author{ID: "simagent", Name: "Sim Agent", Kind: KindAgent, Model: "sim-1"}, InvokedBy: Author{ID: "u0", Name: "u0", Kind: KindHuman},
		Placement: placement, Text: randomText(sim.rng, 5) + "!",
	}
	sim.session.run(func(b *batch) {
		outcome, err := sim.session.propose(b, read, draft, 1+sim.rng.Intn(maxAgentAttempts))
		if err != nil {
			sim.t.Fatalf("propose: %v", err)
		}
		if outcome.retry {
			// The agent reads again from where its text is now.
			text := sim.session.state.text
			sim.reads = append(sim.reads, agentRead{rev: outcome.rev, start: outcome.start, end: outcome.end, text: decodeText(text[outcome.start:outcome.end])})
			return
		}
		// A pending suggestion must still point at exactly what was read.
		if posted := outcome.suggestion; posted.Status == StatusPending {
			current := decodeText(sim.session.state.text[posted.Start:posted.End])
			if current != read.text {
				sim.t.Fatalf("pending suggestion %s drifted: read %q, now anchors %q", posted.ID, read.text, current)
			}
		}
	})
}

func (sim *simulation) resolveSomething(c *simClient) {
	open := sim.session.state.openSuggestions()
	if len(open) == 0 || c.conn == nil || !c.ready {
		return
	}
	target := open[sim.rng.Intn(len(open))]
	action := []string{"accept", "reject", "dismiss"}[sim.rng.Intn(3)]
	c.send(clientMsg{T: "resolve", SID: target.ID, Action: action})
}

func (sim *simulation) step() {
	c := sim.clients[sim.rng.Intn(len(sim.clients))]
	if c.conn != nil {
		select {
		case <-c.conn.done:
			// The server dropped this connection, for example for being slow.
			sim.disconnect(c)
		default:
		}
	}
	switch roll := sim.rng.Intn(100); {
	case roll < 30:
		c.edit()
	case roll < 55:
		sim.deliverToServer(c)
	case roll < 82:
		sim.deliverToClient(c)
	case roll < 86:
		sim.deliverZombie()
	case roll < 89:
		sim.disconnect(c)
	case roll < 93:
		if c.conn == nil {
			sim.connect(c)
		}
	case roll < 95:
		sim.agentBegin()
	case roll < 98:
		sim.agentPropose()
	default:
		sim.resolveSomething(c)
	}
}

// settle reconnects everyone and delivers every message until nothing moves.
func (sim *simulation) settle() {
	for len(sim.zombies) > 0 {
		sim.deliverZombie()
	}
	for _, c := range sim.clients {
		if c.conn != nil {
			select {
			case <-c.conn.done:
				sim.disconnect(c)
			default:
			}
		}
	}
	for len(sim.zombies) > 0 {
		sim.deliverZombie()
	}
	for _, c := range sim.clients {
		if c.conn == nil {
			sim.connect(c)
		}
	}
	for progress := true; progress; {
		progress = false
		for _, c := range sim.clients {
			for sim.deliverToServer(c) {
				progress = true
			}
		}
		for _, c := range sim.clients {
			for sim.deliverToClient(c) {
				progress = true
			}
		}
	}
}

func (sim *simulation) check(seed int64) {
	t := sim.t
	state := sim.session.state
	want := decodeText(state.text)
	for _, c := range sim.clients {
		if c.outstanding != nil || c.buffer != nil {
			t.Fatalf("seed %d: %s still has unacknowledged edits after settling", seed, c.name)
		}
		if got := decodeText(c.text); got != want {
			t.Fatalf("seed %d: %s diverged at revision %d\n client: %q\n server: %q", seed, c.name, c.rev, got, want)
		}
		if c.rev != state.rev {
			t.Fatalf("seed %d: %s is at revision %d, server at %d", seed, c.name, c.rev, state.rev)
		}
	}
	if len(state.attr) != len(state.text) {
		t.Fatalf("seed %d: attribution covers %d units of %d", seed, len(state.attr), len(state.text))
	}
	for _, suggestion := range state.openSuggestions() {
		if suggestion.Start < 0 || suggestion.End < suggestion.Start || suggestion.End > len(state.text) {
			t.Fatalf("seed %d: suggestion %s anchored outside the text", seed, suggestion.ID)
		}
		if suggestion.Status == StatusPending && decodeText(state.text[suggestion.Start:suggestion.End]) != suggestion.Original {
			t.Fatalf("seed %d: pending suggestion %s no longer anchors the text it was written for", seed, suggestion.ID)
		}
	}

	// Recovery: replaying the log must rebuild the same document.
	rebuilt, err := replay(state.id, sim.log.snapshot())
	if err != nil {
		t.Fatalf("seed %d: replay failed: %v", seed, err)
	}
	if decodeText(rebuilt.text) != want || rebuilt.rev != state.rev {
		t.Fatalf("seed %d: replay gave revision %d %q, live state is revision %d %q", seed, rebuilt.rev, decodeText(rebuilt.text), state.rev, want)
	}
	if !reflect.DeepEqual(rebuilt.attr, state.attr) || !reflect.DeepEqual(rebuilt.authors, state.authors) {
		t.Fatalf("seed %d: replay disagrees on authorship", seed)
	}
	for id, live := range state.suggestions {
		recovered, ok := rebuilt.suggestions[id]
		if !ok || recovered.Status != live.Status || recovered.Start != live.Start || recovered.End != live.End {
			t.Fatalf("seed %d: replay disagrees on suggestion %s: %+v vs %+v", seed, id, recovered, live)
		}
	}
}

func runSimulation(t *testing.T, seed int64, clients, steps int) *simulation {
	sim := newSimulation(t, seed, clients)
	for step := 0; step < steps; step++ {
		sim.step()
	}
	sim.settle()
	sim.check(seed)
	return sim
}

func TestSimulatedSessionsConverge(t *testing.T) {
	seeds := 1500
	if testing.Short() {
		seeds = 150
	}
	// ATLAS_SIM_SEEDS=50000 go test ./internal/collab -run Simulated
	if override, err := strconv.Atoi(os.Getenv("ATLAS_SIM_SEEDS")); err == nil && override > 0 {
		seeds = override
	}
	edits, suggestions, stale := 0, 0, 0
	for seed := int64(1); seed <= int64(seeds); seed++ {
		sim := runSimulation(t, seed, 2+int(seed%4), 300+int(seed%5)*100)
		edits += sim.session.state.rev
		for _, suggestion := range sim.session.state.suggestions {
			suggestions++
			if suggestion.Status == StatusStale || suggestion.Status == StatusDismissed {
				stale++
			}
		}
	}
	t.Logf("%d schedules, %d committed operations, %d suggestions (%d went stale), zero divergent replicas", seeds, edits, suggestions, stale)
}

func FuzzSession(f *testing.F) {
	f.Add(int64(7), uint8(3))
	f.Add(int64(99), uint8(5))
	f.Fuzz(func(t *testing.T, seed int64, clients uint8) {
		runSimulation(t, seed, 2+int(clients%5), 400)
	})
}
