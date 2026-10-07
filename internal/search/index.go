package search

import (
	"math"
	"sort"
	"strings"
	"sync"
	"unicode"
)

// Document is the unit stored and ranked by the search engine.
type Document struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Body  string `json:"body"`
}

type Result struct {
	ID    string  `json:"id"`
	Title string  `json:"title"`
	Score float64 `json:"score"`
}

// Index is a concurrency-safe inverted index.
//
// Every stored document version gets a dense internal slot number, assigned
// in increasing order, and each posting list is a slice of (slot, tf) pairs
// sorted by slot. Replacing a document tombstones its old slot: document
// frequencies, the live count and the token total are corrected right away,
// while the stale postings stay in place (skipped by queries) until enough
// tombstones accumulate to compact and renumber every slot.
type Index struct {
	mu      sync.RWMutex
	slots   map[string]uint32 // external ID to its live slot
	docs    []Document        // by slot; tombstoned slots hold the zero Document
	lengths []uint32          // token count by slot
	live    []bool            // false once a slot has been replaced
	terms   map[string]*termPostings
	tokens  int64 // total token count of live documents
	dead    int   // tombstoned slots awaiting compaction
}

// posting records that a term occurs tf times in the document at slot doc.
// A document whose token count overflows uint32 would need a string of more
// than 8 GiB, so 32 bits is enough for both fields.
type posting struct {
	doc uint32
	tf  uint32
}

type termPostings struct {
	postings []posting // ascending slot order, possibly including tombstones
	df       int       // live documents containing the term
	impacts  []impact  // bounds every posting; see addImpact
}

// impact is a (tf, length) pair. A BM25 contribution grows with tf and shrinks
// with document length, so a set of pairs that dominates every posting of a
// term gives an upper bound on that term's contribution to any document.
type impact struct{ tf, length uint32 }

const (
	// maxImpacts caps the per-term frontier used for score upper bounds.
	maxImpacts = 8
	// maxPrunedTerms is the largest number of distinct query terms that use
	// document-at-a-time MaxScore evaluation. Longer queries are scored term
	// at a time into a dense accumulator, which avoids per-document work that
	// grows with the number of terms.
	maxPrunedTerms = 16
	// Compaction runs once at least compactMinDead tombstones exist and they
	// amount to a quarter of the live documents, which keeps posting lists at
	// most about 25% stale and amortizes the rebuild over many updates.
	compactMinDead   = 64
	compactDeadRatio = 4
	// noDoc marks an exhausted cursor. Compaction keeps the slot count close
	// to the live count, so real slots never reach it.
	noDoc = math.MaxUint32
)

func NewIndex() *Index {
	return &Index{
		slots: make(map[string]uint32),
		terms: make(map[string]*termPostings),
	}
}

func tokenize(value string) []string {
	return strings.FieldsFunc(strings.ToLower(value), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}

func frequencies(doc Document) (map[string]int, int) {
	terms := tokenize(doc.Title + " " + doc.Body)
	freq := make(map[string]int, len(terms))
	for _, term := range terms {
		if len(term) > 1 {
			freq[term]++
		}
	}
	return freq, len(terms)
}

func (i *Index) Upsert(doc Document) {
	freq, length := frequencies(doc)

	i.mu.Lock()
	defer i.mu.Unlock()

	if slot, ok := i.slots[doc.ID]; ok {
		i.retire(slot)
	}
	slot := uint32(len(i.docs))
	i.docs = append(i.docs, doc)
	i.lengths = append(i.lengths, uint32(length))
	i.live = append(i.live, true)
	i.slots[doc.ID] = slot
	i.tokens += int64(length)
	for term, count := range freq {
		entry := i.terms[term]
		if entry == nil {
			// Clone so the dictionary does not pin the whole document text
			// that the token was sliced from.
			entry = &termPostings{}
			i.terms[strings.Clone(term)] = entry
		}
		entry.postings = append(entry.postings, posting{doc: slot, tf: uint32(count)})
		entry.df++
		entry.addImpact(uint32(count), uint32(length))
	}
	if i.dead >= compactMinDead && i.dead*compactDeadRatio >= len(i.slots) {
		i.compact()
	}
}

// retire tombstones a replaced slot. Terms left with no live documents are
// dropped together with their stale postings.
func (i *Index) retire(slot uint32) {
	old := i.docs[slot]
	oldTerms, _ := frequencies(old)
	for term := range oldTerms {
		entry := i.terms[term]
		if entry == nil {
			continue
		}
		entry.df--
		if entry.df <= 0 {
			delete(i.terms, term)
		}
	}
	i.tokens -= int64(i.lengths[slot])
	i.live[slot] = false
	i.docs[slot] = Document{}
	delete(i.slots, old.ID)
	i.dead++
}

// compact drops tombstoned postings and renumbers live slots densely. Slot
// order is preserved, so posting lists stay sorted.
func (i *Index) compact() {
	remap := make([]uint32, len(i.docs))
	next := uint32(0)
	for slot, alive := range i.live {
		if alive {
			remap[slot] = next
			next++
		} else {
			remap[slot] = noDoc
		}
	}
	for _, entry := range i.terms {
		kept := entry.postings[:0]
		entry.impacts = entry.impacts[:0]
		for _, p := range entry.postings {
			if to := remap[p.doc]; to != noDoc {
				kept = append(kept, posting{doc: to, tf: p.tf})
				entry.addImpact(p.tf, i.lengths[p.doc])
			}
		}
		if cap(kept) > 2*len(kept)+8 {
			kept = append([]posting(nil), kept...)
		}
		entry.postings = kept
	}
	for slot, to := range remap {
		if to == noDoc {
			continue
		}
		// to <= slot, so moving in place never overwrites unread entries.
		i.docs[to] = i.docs[slot]
		i.lengths[to] = i.lengths[slot]
		i.live[to] = true
		i.slots[i.docs[to].ID] = to
	}
	clear(i.docs[next:])
	i.docs = i.docs[:next]
	i.lengths = i.lengths[:next]
	i.live = i.live[:next]
	i.dead = 0
}

// addImpact keeps t.impacts a Pareto frontier of the (tf, length) pairs seen:
// sorted by ascending length with strictly ascending tf, so no point has both
// a smaller tf and a larger length than another. Past maxImpacts points, two
// neighbours are merged into (larger tf, smaller length), which still
// dominates both, so the bound stays valid but gets looser.
func (t *termPostings) addImpact(tf, length uint32) {
	f := t.impacts
	end := 0 // first point longer than length
	for end < len(f) && f[end].length <= length {
		end++
	}
	if end > 0 && f[end-1].tf >= tf {
		return // dominated
	}
	start := end // first point at least as long as length
	for start > 0 && f[start-1].length == length {
		start--
	}
	for end < len(f) && f[end].tf <= tf {
		end++
	}
	// Replace the points the new one dominates, f[start:end], with it.
	if start == end {
		f = append(f, impact{})
		copy(f[start+1:], f[start:])
	} else {
		f = append(f[:start+1], f[end:]...)
	}
	f[start] = impact{tf: tf, length: length}
	if len(f) > maxImpacts {
		merge := 0
		for k := 1; k+1 < len(f); k++ {
			if f[k+1].tf-f[k].tf < f[merge+1].tf-f[merge].tf {
				merge = k
			}
		}
		f[merge].tf = f[merge+1].tf
		f = append(f[:merge+1], f[merge+2:]...)
	}
	t.impacts = f
}

// bound returns an upper bound on the term's BM25 contribution to any
// document, using the same formula as scoring.
func (t *termPostings) bound(idf, avgLength float64) float64 {
	best := 0.0
	for _, p := range t.impacts {
		tf := float64(p.tf)
		length := float64(p.length)
		denominator := tf + 1.2*(1-0.75+0.75*length/avgLength)
		if contribution := idf * (tf * 2.2 / denominator); contribution > best {
			best = contribution
		}
	}
	return best
}

func (i *Index) Len() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return len(i.slots)
}

// cursor walks one query term's posting list in slot order.
type cursor struct {
	postings []posting
	pos      int
	idf      float64
	bound    float64
}

func (c *cursor) doc() uint32 {
	if c.pos < len(c.postings) {
		return c.postings[c.pos].doc
	}
	return noDoc
}

// seek advances to the first posting at or after target by galloping.
func (c *cursor) seek(target uint32) {
	p := c.postings
	lo := c.pos
	if lo >= len(p) || p[lo].doc >= target {
		return
	}
	// Invariant: p[lo].doc < target, and hi is len(p) or p[hi].doc >= target.
	step := 1
	hi := lo + 1
	for hi < len(p) && p[hi].doc < target {
		lo = hi
		step <<= 1
		hi = lo + step
	}
	if hi > len(p) {
		hi = len(p)
	}
	for lo+1 < hi {
		mid := int(uint(lo+hi) >> 1)
		if p[mid].doc < target {
			lo = mid
		} else {
			hi = mid
		}
	}
	c.pos = hi
}

// estimate is the cursor's contribution for pruning decisions only.
func (c *cursor) estimate(length, avgLength float64) float64 {
	tf := float64(c.postings[c.pos].tf)
	return c.idf * (tf * 2.2 / (tf + 1.2*(1-0.75+0.75*length/avgLength)))
}

// hit is a scored candidate. The final order is descending score, then
// ascending ID.
type hit struct {
	score float64
	slot  uint32
	id    string
}

// ranksBelow reports whether a sorts after b in the result order.
func ranksBelow(a, b hit) bool {
	if a.score != b.score {
		return a.score < b.score
	}
	return a.id > b.id
}

// topK keeps the best limit hits in a heap whose root is the hit that ranks
// lowest, so the root's score is the k-th best score once the heap is full.
type topK struct {
	limit int
	hits  []hit
}

func (t *topK) full() bool { return len(t.hits) >= t.limit }

// offer adds a candidate if it beats the current k-th hit.
func (t *topK) offer(h hit) bool {
	if len(t.hits) < t.limit {
		t.hits = append(t.hits, h)
		for k := len(t.hits) - 1; k > 0; {
			parent := (k - 1) / 2
			if !ranksBelow(t.hits[k], t.hits[parent]) {
				break
			}
			t.hits[k], t.hits[parent] = t.hits[parent], t.hits[k]
			k = parent
		}
		return true
	}
	if !ranksBelow(t.hits[0], h) {
		return false
	}
	t.hits[0] = h
	for k, n := 0, len(t.hits); ; {
		low := k
		if left := 2*k + 1; left < n && ranksBelow(t.hits[left], t.hits[low]) {
			low = left
		}
		if right := 2*k + 2; right < n && ranksBelow(t.hits[right], t.hits[low]) {
			low = right
		}
		if low == k {
			return true
		}
		t.hits[k], t.hits[low] = t.hits[low], t.hits[k]
		k = low
	}
}

// Search uses Okapi BM25 with the conventional k1=1.2 and b=0.75 values and
// returns the limit best documents by descending score, ties broken by
// ascending ID.
//
// Queries are evaluated document at a time with MaxScore dynamic pruning.
// Each term gets an upper bound on its contribution. Once the top-k heap is
// full, the terms whose bounds add up to strictly less than the k-th score
// (with a small slack for rounding) stop generating candidates and are only
// probed for documents found through the other terms, and a candidate is
// dropped as soon as its partial score plus the remaining bounds falls
// strictly below the k-th score. A document that ties the k-th score is
// therefore never skipped. Every surviving candidate is scored by summing
// its term contributions in query order with the same floating-point
// expression as before, so results are identical to scoring every match.
func (i *Index) Search(query string, limit int) []Result {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if limit <= 0 || len(i.slots) == 0 {
		return nil
	}

	n := float64(len(i.slots))
	avgLength := float64(i.tokens) / n
	tokens := tokenize(query)
	cursors := make([]cursor, 0, len(tokens))
	seenTerms := make(map[string]struct{}, len(tokens))
	for _, term := range tokens {
		if _, duplicate := seenTerms[term]; duplicate {
			continue
		}
		seenTerms[term] = struct{}{}
		entry := i.terms[term]
		if entry == nil {
			continue
		}
		df := float64(entry.df)
		idf := math.Log(1 + (n-df+0.5)/(df+0.5))
		cursors = append(cursors, cursor{postings: entry.postings, idf: idf, bound: entry.bound(idf, avgLength)})
	}
	top := topK{limit: limit}
	if len(cursors) > maxPrunedTerms {
		i.scoreExhaustive(cursors, avgLength, &top)
	} else if len(cursors) > 0 {
		i.scorePruned(cursors, avgLength, &top)
	}

	hits := top.hits
	sort.Slice(hits, func(a, b int) bool { return ranksBelow(hits[b], hits[a]) })
	results := make([]Result, len(hits))
	for k, h := range hits {
		results[k] = Result{ID: h.id, Title: i.docs[h.slot].Title, Score: h.score}
	}
	return results
}

// scorePruned is the MaxScore evaluation described on Search. The cursors are
// in query order.
func (i *Index) scorePruned(cursors []cursor, avgLength float64, top *topK) {
	// order lists the cursors by ascending bound; prefix[j] bounds what a
	// document can collect from cursors order[0..j]. Cursors order[:essential]
	// are non-essential: a document found only in them cannot reach the top k.
	order := make([]int, len(cursors))
	for k := range order {
		order[k] = k
	}
	sort.Slice(order, func(a, b int) bool { return cursors[order[a]].bound < cursors[order[b]].bound })
	prefix := make([]float64, len(order))
	sum := 0.0
	for j, k := range order {
		sum += cursors[k].bound
		prefix[j] = sum
	}
	// Bounds and partial sums are rounded and added in a different order than
	// exact scores, so each comparison inflates them by far more than the
	// worst-case accumulated rounding error.
	slack := 1 + float64(len(cursors)+16)*1e-12
	threshold := math.Inf(-1)
	essential := 0

	for essential < len(order) {
		d := uint32(noDoc)
		for _, k := range order[essential:] {
			if doc := cursors[k].doc(); doc < d {
				d = doc
			}
		}
		if d == noDoc {
			return
		}
		if i.live[d] {
			length := float64(i.lengths[d])
			if competitive(cursors, order[:essential], order[essential:], prefix, d, length, avgLength, threshold, slack) {
				score := exactScore(cursors, d, length, avgLength)
				if !top.full() || score >= threshold {
					if top.offer(hit{score: score, slot: d, id: i.docs[d].ID}) && top.full() {
						threshold = top.hits[0].score
						for essential < len(order) && prefix[essential]*slack < threshold {
							essential++
						}
					}
				}
			}
		}
		for _, k := range order[essential:] {
			if c := &cursors[k]; c.doc() == d {
				c.pos++
			}
		}
	}
}

// competitive reports whether document d could still enter the top k. It adds
// the estimated contributions of the essential cursors positioned on d to the
// bounds of the non-essential ones, then probes the non-essential cursors from
// the largest bound down, giving up only when the total is strictly below the
// threshold. When it returns true every non-essential cursor has been moved
// to d or past it, ready for exactScore.
func competitive(cursors []cursor, nonEssential, essential []int, prefix []float64, d uint32, length, avgLength, threshold, slack float64) bool {
	if len(nonEssential) == 0 {
		return true
	}
	partial := 0.0
	for _, k := range essential {
		if c := &cursors[k]; c.doc() == d {
			partial += c.estimate(length, avgLength)
		}
	}
	for j := len(nonEssential) - 1; j >= 0; j-- {
		if (partial+prefix[j])*slack < threshold {
			return false
		}
		c := &cursors[nonEssential[j]]
		c.seek(d)
		if c.doc() == d {
			partial += c.estimate(length, avgLength)
		}
	}
	return true
}

// exactScore sums the contributions of the cursors positioned on d in query
// order. The arithmetic must stay textually identical to the original
// map-based Search: Go may fuse "a + x*y" into one FMA instruction on some
// architectures, and only the same expression shape is guaranteed to round
// the same way.
func exactScore(cursors []cursor, d uint32, length, avgLength float64) float64 {
	score := 0.0
	for k := range cursors {
		c := &cursors[k]
		if c.pos >= len(c.postings) || c.postings[c.pos].doc != d {
			continue
		}
		tf := float64(c.postings[c.pos].tf)
		denominator := tf + 1.2*(1-0.75+0.75*length/avgLength)
		score += c.idf * (tf * 2.2 / denominator)
	}
	return score
}

// scoreExhaustive scores long queries term at a time into a dense
// accumulator. Each document still receives its contributions in query order.
func (i *Index) scoreExhaustive(cursors []cursor, avgLength float64, top *topK) {
	scores := make([]float64, len(i.docs))
	matched := make([]bool, len(i.docs))
	var touched []uint32
	for k := range cursors {
		idf := cursors[k].idf
		for _, p := range cursors[k].postings {
			if !i.live[p.doc] {
				continue
			}
			if !matched[p.doc] {
				matched[p.doc] = true
				touched = append(touched, p.doc)
			}
			tf := float64(p.tf)
			length := float64(i.lengths[p.doc])
			denominator := tf + 1.2*(1-0.75+0.75*length/avgLength)
			scores[p.doc] += idf * (tf * 2.2 / denominator)
		}
	}
	for _, d := range touched {
		if !top.full() || scores[d] >= top.hits[0].score {
			top.offer(hit{score: scores[d], slot: d, id: i.docs[d].ID})
		}
	}
}
