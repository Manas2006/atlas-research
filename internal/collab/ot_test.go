package collab

import (
	"encoding/json"
	"flag"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

var updateVectors = flag.Bool("update", false, "rewrite testdata/ot_vectors.json from the Go implementation")

// alphabet mixes ASCII, a newline, a two-byte character, and an emoji that
// needs a surrogate pair, so index arithmetic is exercised in UTF-16 units.
var alphabet = []string{"a", "b", "c", "d", " ", "\n", "é", "😀"}

func randomText(rng *rand.Rand, maxParts int) string {
	text := ""
	for range rng.Intn(maxParts + 1) {
		text += alphabet[rng.Intn(len(alphabet))]
	}
	return text
}

// randomOp builds a valid operation for doc that never splits a surrogate
// pair, mirroring what a browser editor can produce.
func randomOp(rng *rand.Rand, doc []uint16) *Op {
	op := NewOp()
	position := 0
	for position < len(doc) {
		step := 1 + rng.Intn(4)
		if position+step > len(doc) {
			step = len(doc) - position
		}
		// Keep both halves of a surrogate pair in the same component.
		if end := position + step; end < len(doc) && doc[end] >= 0xdc00 && doc[end] <= 0xdfff {
			step++
		}
		switch rng.Intn(5) {
		case 0:
			op.Delete(step)
		case 1:
			op.InsertString(randomText(rng, 3))
			op.Retain(step)
		default:
			op.Retain(step)
		}
		position += step
	}
	if rng.Intn(3) == 0 {
		op.InsertString(randomText(rng, 3))
	}
	return op
}

func mustApply(t *testing.T, doc []uint16, op *Op) []uint16 {
	t.Helper()
	out, err := op.Apply(doc)
	if err != nil {
		t.Fatalf("apply %s to %q: %v", mustJSON(op), decodeText(doc), err)
	}
	return out
}

func mustJSON(value any) string {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(payload)
}

func TestApplyBasics(t *testing.T) {
	doc := encodeText("hello world")
	op := NewOp().Retain(6).InsertString("big ").Delete(5).InsertString("atlas")
	if got := decodeText(mustApply(t, doc, op)); got != "hello big atlas" {
		t.Fatalf("unexpected document %q", got)
	}
	if op.BaseLen() != 11 || op.TargetLen() != 15 {
		t.Fatalf("unexpected lengths %d -> %d", op.BaseLen(), op.TargetLen())
	}
	if _, err := op.Apply(encodeText("short")); err == nil {
		t.Fatal("expected a length mismatch to fail")
	}
}

func TestBuilderNormalizes(t *testing.T) {
	op := NewOp().Retain(2).Retain(3).Delete(1).InsertString("x").Delete(2).InsertString("y")
	if got := mustJSON(op); got != `[5,"xy",-3]` {
		t.Fatalf("unexpected canonical form %s", got)
	}
}

func TestOpJSONRoundTripAndValidation(t *testing.T) {
	var op Op
	if err := json.Unmarshal([]byte(`[2,2,"é😀",-1,-1,4]`), &op); err != nil {
		t.Fatal(err)
	}
	if got := mustJSON(op); got != `[4,"é😀",-2,4]` {
		t.Fatalf("unexpected round trip %s", got)
	}
	if op.BaseLen() != 10 || op.TargetLen() != 11 {
		t.Fatalf("emoji must count as two units: %d -> %d", op.BaseLen(), op.TargetLen())
	}
	for _, invalid := range []string{`{}`, `[0]`, `[""]`, `[1.5]`, `[true]`, `[null]`, `[[1]]`, `"abc"`, `[99999999999]`} {
		if err := json.Unmarshal([]byte(invalid), &op); err == nil {
			t.Fatalf("expected %s to be rejected", invalid)
		}
	}
}

// TestTransformConverges checks the property every replica relies on: two
// concurrent operations reach the same document in either order.
func TestTransformConverges(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for round := 0; round < 20_000; round++ {
		doc := encodeText(randomText(rng, 12))
		a, b := randomOp(rng, doc), randomOp(rng, doc)
		aPrime, bPrime, err := Transform(a, b)
		if err != nil {
			t.Fatal(err)
		}
		viaA := mustApply(t, mustApply(t, doc, a), bPrime)
		viaB := mustApply(t, mustApply(t, doc, b), aPrime)
		if decodeText(viaA) != decodeText(viaB) {
			t.Fatalf("round %d diverged: doc=%q a=%s b=%s -> %q vs %q", round, decodeText(doc), mustJSON(a), mustJSON(b), decodeText(viaA), decodeText(viaB))
		}
	}
}

func TestComposeMatchesSequentialApply(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for round := 0; round < 20_000; round++ {
		doc := encodeText(randomText(rng, 12))
		a := randomOp(rng, doc)
		middle := mustApply(t, doc, a)
		b := randomOp(rng, middle)
		composed, err := Compose(a, b)
		if err != nil {
			t.Fatal(err)
		}
		want := decodeText(mustApply(t, middle, b))
		if got := decodeText(mustApply(t, doc, composed)); got != want {
			t.Fatalf("round %d: compose gave %q, sequential apply gave %q", round, got, want)
		}
	}
}

// TestTransformAgainstComposed covers what a client does while it waits for
// an acknowledgement: it holds one sent operation and one composed buffer and
// must transform an incoming server operation across both.
func TestTransformAgainstComposed(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for round := 0; round < 10_000; round++ {
		doc := encodeText(randomText(rng, 12))
		outstanding := randomOp(rng, doc)
		buffer := randomOp(rng, mustApply(t, doc, outstanding))
		server := randomOp(rng, doc)

		outstandingPrime, serverPrime, err := Transform(outstanding, server)
		if err != nil {
			t.Fatal(err)
		}
		bufferPrime, serverSecond, err := Transform(buffer, serverPrime)
		if err != nil {
			t.Fatal(err)
		}
		local := mustApply(t, mustApply(t, mustApply(t, doc, outstanding), buffer), serverSecond)
		remote := mustApply(t, mustApply(t, mustApply(t, doc, server), outstandingPrime), bufferPrime)
		if decodeText(local) != decodeText(remote) {
			t.Fatalf("round %d diverged: %q vs %q", round, decodeText(local), decodeText(remote))
		}
	}
}

func TestMapPosAndRange(t *testing.T) {
	// "hello world" -> "hello, big world" by inserting at 5 and at 6.
	op := NewOp().Retain(5).InsertString(",").Retain(1).InsertString("big ").Retain(5)
	if got := op.MapPos(5, false); got != 5 {
		t.Fatalf("position before an insert should stay, got %d", got)
	}
	if got := op.MapPos(5, true); got != 6 {
		t.Fatalf("position after an insert should move, got %d", got)
	}
	if got := op.MapPos(11, false); got != 16 {
		t.Fatalf("end position should shift by both inserts, got %d", got)
	}

	// Range covers "world" at [6, 11). The insert at 6 sits on its edge.
	start, end, touched := op.MapRange(6, 11)
	if start != 11 || end != 16 || touched {
		t.Fatalf("edge insert must not touch the range: [%d,%d) touched=%v", start, end, touched)
	}
	inside := NewOp().Retain(8).InsertString("X").Retain(3)
	if _, _, touched := inside.MapRange(6, 11); !touched {
		t.Fatal("an insert inside the range must touch it")
	}
	overlap := NewOp().Retain(4).Delete(3).Retain(4)
	start, end, touched = overlap.MapRange(6, 11)
	if start != 4 || end != 8 || !touched {
		t.Fatalf("overlapping delete: [%d,%d) touched=%v", start, end, touched)
	}
	before := NewOp().Delete(3).Retain(8)
	start, end, touched = before.MapRange(6, 11)
	if start != 3 || end != 8 || touched {
		t.Fatalf("delete before the range: [%d,%d) touched=%v", start, end, touched)
	}
	// Empty range at 6: only a delete that spans the point touches it.
	if _, _, touched := NewOp().Retain(5).Delete(2).Retain(4).MapRange(6, 6); !touched {
		t.Fatal("a delete across an empty range must touch it")
	}
	if _, _, touched := NewOp().Retain(6).Delete(2).Retain(3).MapRange(6, 6); touched {
		t.Fatal("a delete that starts at an empty range must not touch it")
	}
}

// TestMapRangeTracksContent checks that an untouched range still holds the
// same text after any operation.
func TestMapRangeTracksContent(t *testing.T) {
	rng := rand.New(rand.NewSource(4))
	for round := 0; round < 20_000; round++ {
		doc := encodeText(randomText(rng, 14))
		op := randomOp(rng, doc)
		start := rng.Intn(len(doc) + 1)
		end := start + rng.Intn(len(doc)-start+1)
		after := mustApply(t, doc, op)
		mappedStart, mappedEnd, touched := op.MapRange(start, end)
		if mappedStart < 0 || mappedEnd < mappedStart || mappedEnd > len(after) {
			t.Fatalf("round %d: range [%d,%d) mapped out of bounds to [%d,%d) of %d", round, start, end, mappedStart, mappedEnd, len(after))
		}
		if !touched && decodeText(doc[start:end]) != decodeText(after[mappedStart:mappedEnd]) {
			t.Fatalf("round %d: untouched range changed from %q to %q (op %s)", round, decodeText(doc[start:end]), decodeText(after[mappedStart:mappedEnd]), mustJSON(op))
		}
	}
}

// Replacing a word is a delete plus an insert at the same position. The
// range must end up around the new word so an agent can read it again.
func TestMapRangeFollowsAReplacedWord(t *testing.T) {
	// "The method is slow." -> "The method is fast." with the range on "slow".
	replace := NewOp().Retain(14).InsertString("fast").Delete(4).Retain(1)
	start, end, touched := replace.MapRange(14, 18)
	if start != 14 || end != 18 || !touched {
		t.Fatalf("range should cover the replacement: [%d,%d) touched=%v", start, end, touched)
	}
	longer := NewOp().Retain(14).InsertString("much faster").Delete(4).Retain(1)
	if start, end, _ := longer.MapRange(14, 18); start != 14 || end != 25 {
		t.Fatalf("range should grow with the replacement: [%d,%d)", start, end)
	}
}

func TestEditPos(t *testing.T) {
	if position, ok := NewOp().Retain(3).InsertString("ab").Retain(2).EditPos(); !ok || position != 5 {
		t.Fatalf("caret should follow the insert, got %d %v", position, ok)
	}
	if position, ok := NewOp().Retain(3).Delete(2).EditPos(); !ok || position != 3 {
		t.Fatalf("caret should sit at the delete, got %d %v", position, ok)
	}
	if _, ok := NewOp().Retain(5).EditPos(); ok {
		t.Fatal("a retain-only operation has no edit position")
	}
}

// vector is one shared test case. The JavaScript client runs the same file
// (internal/atlas/uitests/ot.test.js) and must produce identical results.
type vector struct {
	Doc       string `json:"doc"`
	A         *Op    `json:"a"`
	B         *Op    `json:"b"`
	APrime    *Op    `json:"a_prime"`
	BPrime    *Op    `json:"b_prime"`
	Merged    string `json:"merged"`
	C         *Op    `json:"c"`
	Composed  *Op    `json:"composed"`
	AfterAC   string `json:"after_ac"`
	Pos       int    `json:"pos"`
	PosBefore int    `json:"pos_before"`
	PosAfter  int    `json:"pos_after"`
	Range     [2]int `json:"range"`
	Mapped    [2]int `json:"mapped"`
	Touched   bool   `json:"touched"`
}

func buildVectors() []vector {
	rng := rand.New(rand.NewSource(20260907))
	vectors := make([]vector, 0, 200)
	for len(vectors) < 200 {
		doc := encodeText(randomText(rng, 10))
		a, b := randomOp(rng, doc), randomOp(rng, doc)
		aPrime, bPrime, _ := Transform(a, b)
		afterA, _ := a.Apply(doc)
		merged, _ := bPrime.Apply(afterA)
		c := randomOp(rng, afterA)
		composed, _ := Compose(a, c)
		afterAC, _ := c.Apply(afterA)
		position := rng.Intn(len(doc) + 1)
		rangeStart := rng.Intn(len(doc) + 1)
		rangeEnd := rangeStart + rng.Intn(len(doc)-rangeStart+1)
		mappedStart, mappedEnd, touched := a.MapRange(rangeStart, rangeEnd)
		vectors = append(vectors, vector{
			Doc: decodeText(doc), A: a, B: b, APrime: aPrime, BPrime: bPrime, Merged: decodeText(merged),
			C: c, Composed: composed, AfterAC: decodeText(afterAC),
			Pos: position, PosBefore: a.MapPos(position, false), PosAfter: a.MapPos(position, true),
			Range: [2]int{rangeStart, rangeEnd}, Mapped: [2]int{mappedStart, mappedEnd}, Touched: touched,
		})
	}
	return vectors
}

func TestSharedVectorsAreCurrent(t *testing.T) {
	path := filepath.Join("testdata", "ot_vectors.json")
	// One vector per line keeps the file small and its diffs readable.
	want := []byte("[\n")
	for index, v := range buildVectors() {
		if index > 0 {
			want = append(want, ",\n"...)
		}
		want = append(want, mustJSON(v)...)
	}
	want = append(want, "\n]\n"...)
	if *updateVectors {
		if err := os.WriteFile(path, want, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read shared vectors (run go test ./internal/collab -update to create them): %v", err)
	}
	if string(got) != string(want) {
		t.Fatal("testdata/ot_vectors.json is stale; run go test ./internal/collab -update")
	}
}

func FuzzTransform(f *testing.F) {
	f.Add(int64(1))
	f.Add(int64(42))
	f.Fuzz(func(t *testing.T, seed int64) {
		rng := rand.New(rand.NewSource(seed))
		doc := encodeText(randomText(rng, 16))
		a, b := randomOp(rng, doc), randomOp(rng, doc)
		aPrime, bPrime, err := Transform(a, b)
		if err != nil {
			t.Fatal(err)
		}
		if decodeText(mustApply(t, mustApply(t, doc, a), bPrime)) != decodeText(mustApply(t, mustApply(t, doc, b), aPrime)) {
			t.Fatalf("seed %d diverged", seed)
		}
	})
}
