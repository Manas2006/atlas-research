package search

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"
	"testing"
)

// searcher is the surface shared by Index and the test-only refIndex oracle.
type searcher interface {
	Upsert(Document)
	Search(string, int) []Result
	Len() int
}

// slotsForTest returns the number of internal slots, live and tombstoned.
// It only drops when a compaction runs.
func (i *Index) slotsForTest() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return len(i.docs)
}

// sameResults reports the first difference between two result lists. Scores
// are compared bit for bit, and a nil result is distinct from an empty one
// because the HTTP layer encodes them differently ("null" versus "[]").
func sameResults(got, want []Result) error {
	if (got == nil) != (want == nil) {
		return fmt.Errorf("nil mismatch: got nil=%v want nil=%v", got == nil, want == nil)
	}
	if len(got) != len(want) {
		return fmt.Errorf("length mismatch: got %d want %d\n got=%v\nwant=%v", len(got), len(want), got, want)
	}
	for k := range want {
		if got[k].ID != want[k].ID || got[k].Title != want[k].Title {
			return fmt.Errorf("rank %d: got %q/%q want %q/%q\n got=%v\nwant=%v", k, got[k].ID, got[k].Title, want[k].ID, want[k].Title, got, want)
		}
		if math.Float64bits(got[k].Score) != math.Float64bits(want[k].Score) {
			return fmt.Errorf("rank %d (%s): score bits differ: got %v (%#x) want %v (%#x)", k, want[k].ID, got[k].Score, math.Float64bits(got[k].Score), want[k].Score, math.Float64bits(want[k].Score))
		}
	}
	return nil
}

// randomCorpus produces documents and queries from a small, skewed vocabulary
// so that score ties, repeated terms, single-character tokens, punctuation,
// case folding, empty and long documents and re-upserted IDs all show up.
type randomCorpus struct {
	rng   *rand.Rand
	vocab []string
	ids   []string
	zipf  *rand.Zipf
}

func newRandomCorpus(seed int64) *randomCorpus {
	rng := rand.New(rand.NewSource(seed))
	vocabSize := 3 + rng.Intn(80)
	vocab := make([]string, vocabSize)
	for k := range vocab {
		switch rng.Intn(10) {
		case 0:
			vocab[k] = string(rune('a' + rng.Intn(26))) // too short to be indexed
		case 1:
			vocab[k] = fmt.Sprintf("Term%d", k) // case folding
		case 2:
			vocab[k] = fmt.Sprintf("t%dé", k) // non-ASCII letter
		default:
			vocab[k] = fmt.Sprintf("t%d", k)
		}
	}
	idPool := 1 + rng.Intn(400)
	ids := make([]string, idPool)
	for k := range ids {
		ids[k] = fmt.Sprintf("d%d", rng.Intn(idPool*3))
	}
	s := 1.01 + rng.Float64()*1.5
	return &randomCorpus{
		rng:   rng,
		vocab: vocab,
		ids:   ids,
		zipf:  rand.NewZipf(rng, s, 1, uint64(vocabSize-1)),
	}
}

func (c *randomCorpus) word() string {
	if c.rng.Intn(4) == 0 {
		return c.vocab[c.rng.Intn(len(c.vocab))]
	}
	return c.vocab[c.zipf.Uint64()]
}

var separators = []string{" ", " ", " ", ", ", "-", "  ", ".", "\t", "/"}

func (c *randomCorpus) text(maxWords int) string {
	words := c.rng.Intn(maxWords + 1)
	var b strings.Builder
	for w := 0; w < words; w++ {
		if w > 0 {
			b.WriteString(separators[c.rng.Intn(len(separators))])
		}
		word := c.word()
		if c.rng.Intn(50) == 0 {
			// A burst of one word gives a large term frequency.
			word = strings.Repeat(word+" ", 1+c.rng.Intn(60))
		}
		b.WriteString(word)
	}
	return b.String()
}

func (c *randomCorpus) document() Document {
	maxWords := 30
	switch c.rng.Intn(8) {
	case 0:
		maxWords = 0
	case 1:
		maxWords = 3
	case 2:
		maxWords = 300
	}
	return Document{
		ID:    c.ids[c.rng.Intn(len(c.ids))],
		Title: c.text(4),
		Body:  c.text(maxWords),
	}
}

// query mixes known terms, unknown terms and repeats of earlier terms in a
// different case, joined by assorted separators.
func (c *randomCorpus) query() string {
	words := c.rng.Intn(7)
	if c.rng.Intn(12) == 0 {
		words = 17 + c.rng.Intn(40) // long enough to skip pruning
	}
	parts := make([]string, 0, words)
	for w := words; w > 0; w-- {
		switch c.rng.Intn(10) {
		case 0:
			parts = append(parts, fmt.Sprintf("unknown%d", c.rng.Intn(5)))
		case 1:
			if len(parts) > 0 {
				parts = append(parts, strings.ToUpper(parts[c.rng.Intn(len(parts))]))
			}
		default:
			parts = append(parts, c.word())
		}
	}
	return strings.Join(parts, separators[c.rng.Intn(len(separators))])
}

var testLimits = []int{-3, 0, 1, 2, 3, 5, 7, 10, 25, 100, 1 << 20}

func (c *randomCorpus) limit() int { return testLimits[c.rng.Intn(len(testLimits))] }

func checkSame(t *testing.T, label string, got, want searcher, queries []string, limits []int) {
	t.Helper()
	if got.Len() != want.Len() {
		t.Fatalf("%s: Len %d want %d", label, got.Len(), want.Len())
	}
	for k, query := range queries {
		if err := sameResults(got.Search(query, limits[k]), want.Search(query, limits[k])); err != nil {
			t.Fatalf("%s: query %q limit %d: %v", label, query, limits[k], err)
		}
	}
}

// upsertCountingCompactions applies doc and reports whether the index
// compacted while doing so.
func upsertCountingCompactions(index *Index, doc Document) bool {
	before := index.slotsForTest()
	index.Upsert(doc)
	return index.slotsForTest() <= before
}

func TestSearchMatchesReference(t *testing.T) {
	corpora := 150
	if testing.Short() {
		corpora = 60
	}
	compactions := 0
	for seed := int64(1); seed <= int64(corpora); seed++ {
		c := newRandomCorpus(seed)
		ref := newRefIndex()
		index := NewIndex()
		ops := c.rng.Intn(600)
		for op := 0; op <= ops; op++ {
			if op > 0 {
				doc := c.document()
				ref.Upsert(doc)
				if upsertCountingCompactions(index, doc) {
					compactions++
				}
			}
			if op%25 != 0 && op != ops {
				continue
			}
			queries := make([]string, 30)
			limits := make([]int, len(queries))
			for k := range queries {
				queries[k] = c.query()
				limits[k] = c.limit()
			}
			label := fmt.Sprintf("seed %d op %d", seed, op)
			checkSame(t, label, index, ref, queries, limits)

			if c.rng.Intn(6) == 0 {
				// A snapshot written by the original code must load into
				// the new index, a snapshot written by the new index must
				// load into the original reader, and the new writer must
				// round-trip, including further updates after the restore.
				var encoded bytes.Buffer
				if err := ref.WriteSnapshot(&encoded); err != nil {
					t.Fatal(err)
				}
				restored, err := ReadSnapshot(&encoded)
				if err != nil {
					t.Fatalf("%s: read original snapshot: %v", label, err)
				}
				checkSame(t, label+" (original snapshot)", restored, ref, queries, limits)

				encoded.Reset()
				if err := index.WriteSnapshot(&encoded); err != nil {
					t.Fatal(err)
				}
				snapshot := encoded.Bytes()
				oldReader, err := refReadSnapshot(bytes.NewReader(snapshot))
				if err != nil {
					t.Fatalf("%s: original reader rejected new snapshot: %v", label, err)
				}
				checkSame(t, label+" (new snapshot, original reader)", oldReader, ref, queries, limits)
				if index, err = ReadSnapshot(bytes.NewReader(snapshot)); err != nil {
					t.Fatalf("%s: read new snapshot: %v", label, err)
				}
				checkSame(t, label+" (new snapshot)", index, ref, queries, limits)
			}
		}
	}
	if compactions == 0 {
		t.Fatalf("no compaction happened; the test does not cover tombstone cleanup")
	}
	t.Logf("%d corpora, %d compactions", corpora, compactions)
}

// TestSearchMatchesReferenceRepeatedUpdates hammers a few IDs so posting lists
// carry many tombstones between compactions.
func TestSearchMatchesReferenceRepeatedUpdates(t *testing.T) {
	c := newRandomCorpus(99)
	c.ids = []string{"x", "y", "z", "w"}
	ref := newRefIndex()
	index := NewIndex()
	for round := 0; round < 3000; round++ {
		doc := c.document()
		ref.Upsert(doc)
		index.Upsert(doc)
		for k := 0; k < 3; k++ {
			query, limit := c.query(), c.limit()
			if err := sameResults(index.Search(query, limit), ref.Search(query, limit)); err != nil {
				t.Fatalf("round %d query %q limit %d: %v", round, query, limit, err)
			}
		}
	}
	if index.Len() != ref.Len() {
		t.Fatalf("Len %d want %d", index.Len(), ref.Len())
	}
	if slots := index.slotsForTest(); slots > 100 {
		t.Fatalf("%d internal slots for %d documents: tombstones are not being compacted", slots, index.Len())
	}
}

// TestSearchMatchesReferenceLargeCorpus runs the 100K-document Zipf workload
// against both implementations, rewrites 30% of the documents in place so a
// compaction happens, and checks every result list is identical. It is gated
// with the benchmark because it takes a while.
func TestSearchMatchesReferenceLargeCorpus(t *testing.T) {
	if os.Getenv("ATLAS_BENCH") != "1" {
		t.Skip("set ATLAS_BENCH=1 to compare against the reference on the 100K-document corpus")
	}
	workload := newZipfWorkload()
	ref := newRefIndex()
	index := NewIndex()
	workload.generate(func(doc Document) {
		ref.Upsert(doc)
		index.Upsert(doc)
	})
	queries := workload.queries(400, benchQuerySeed)
	limits := make([]int, len(queries))
	for k := range limits {
		limits[k] = []int{10, 1, 3, 100, 1000}[k%5]
	}
	checkSame(t, "fresh", index, ref, queries, limits)

	source := workload.source(7)
	compacted := false
	for n := 0; n < zipfDocs*3/10; n++ {
		doc := source.document(zipfDocID(source.rng.Intn(zipfDocs)))
		ref.Upsert(doc)
		if upsertCountingCompactions(index, doc) {
			compacted = true
		}
		if n == zipfDocs/5 {
			checkSame(t, "with tombstones", index, ref, queries[:100], limits[:100])
		}
	}
	if !compacted {
		t.Fatalf("expected a compaction during the update phase")
	}
	checkSame(t, "after updates", index, ref, queries, limits)
}
