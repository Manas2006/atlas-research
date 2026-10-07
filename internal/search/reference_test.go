package search

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"unicode"
)

// refIndex is a verbatim, test-only copy of the original map-based Index
// (Upsert, Len, Search and the version 1 snapshot writer and reader). It is
// the oracle that the optimized Index must match exactly: same IDs, same
// order and bit-identical scores. Do not "fix" or optimize this file.
type refIndex struct {
	docs     map[string]Document
	lengths  map[string]int
	postings map[string]map[string]int
	tokens   int64
}

func newRefIndex() *refIndex {
	return &refIndex{
		docs:     make(map[string]Document),
		lengths:  make(map[string]int),
		postings: make(map[string]map[string]int),
	}
}

func refTokenize(value string) []string {
	return strings.FieldsFunc(strings.ToLower(value), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}

func refFrequencies(doc Document) (map[string]int, int) {
	terms := refTokenize(doc.Title + " " + doc.Body)
	freq := make(map[string]int, len(terms))
	for _, term := range terms {
		if len(term) > 1 {
			freq[term]++
		}
	}
	return freq, len(terms)
}

func (i *refIndex) Upsert(doc Document) {
	if old, ok := i.docs[doc.ID]; ok {
		oldTerms, _ := refFrequencies(old)
		for term := range oldTerms {
			delete(i.postings[term], doc.ID)
			if len(i.postings[term]) == 0 {
				delete(i.postings, term)
			}
		}
		i.tokens -= int64(i.lengths[doc.ID])
	}

	freq, length := refFrequencies(doc)
	for term, count := range freq {
		if i.postings[term] == nil {
			i.postings[term] = make(map[string]int)
		}
		i.postings[term][doc.ID] = count
	}
	i.docs[doc.ID] = doc
	i.lengths[doc.ID] = length
	i.tokens += int64(length)
}

func (i *refIndex) Len() int { return len(i.docs) }

// Search is the original Okapi BM25 implementation (k1=1.2, b=0.75).
func (i *refIndex) Search(query string, limit int) []Result {
	if limit <= 0 || len(i.docs) == 0 {
		return nil
	}

	n := float64(len(i.docs))
	avgLength := float64(i.tokens) / n
	scores := make(map[string]float64)
	seenTerms := make(map[string]struct{})
	for _, term := range refTokenize(query) {
		if _, duplicate := seenTerms[term]; duplicate {
			continue
		}
		seenTerms[term] = struct{}{}
		posting := i.postings[term]
		df := float64(len(posting))
		if df == 0 {
			continue
		}
		idf := math.Log(1 + (n-df+0.5)/(df+0.5))
		for id, frequency := range posting {
			tf := float64(frequency)
			length := float64(i.lengths[id])
			denominator := tf + 1.2*(1-0.75+0.75*length/avgLength)
			scores[id] += idf * (tf * 2.2 / denominator)
		}
	}

	results := make([]Result, 0, len(scores))
	for id, score := range scores {
		results = append(results, Result{ID: id, Title: i.docs[id].Title, Score: score})
	}
	sort.Slice(results, func(a, b int) bool {
		if results[a].Score == results[b].Score {
			return results[a].ID < results[b].ID
		}
		return results[a].Score > results[b].Score
	})
	if len(results) > limit {
		results = results[:limit]
	}
	return results
}

// WriteSnapshot is the original version 1 snapshot writer, kept so tests can
// prove that snapshots produced by the old code still load.
func (i *refIndex) WriteSnapshot(destination io.Writer) error {
	ids := make([]string, 0, len(i.docs))
	for id := range i.docs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	ordinals := make(map[string]int, len(ids))
	snapshot := diskSnapshot{
		Version:  1,
		Docs:     make([]Document, len(ids)),
		Lengths:  make([]int, len(ids)),
		Postings: make(map[string][]byte, len(i.postings)),
	}
	for ordinal, id := range ids {
		ordinals[id] = ordinal
		snapshot.Docs[ordinal] = i.docs[id]
		snapshot.Lengths[ordinal] = i.lengths[id]
	}

	var scratch [binary.MaxVarintLen64]byte
	for term, posting := range i.postings {
		postingIDs := make([]string, 0, len(posting))
		for id := range posting {
			postingIDs = append(postingIDs, id)
		}
		sort.Slice(postingIDs, func(a, b int) bool {
			return ordinals[postingIDs[a]] < ordinals[postingIDs[b]]
		})
		encoded := make([]byte, 0, len(postingIDs)*2)
		previous := -1
		for _, id := range postingIDs {
			ordinal := ordinals[id]
			gap := ordinal - previous
			n := binary.PutUvarint(scratch[:], uint64(gap))
			encoded = append(encoded, scratch[:n]...)
			n = binary.PutUvarint(scratch[:], uint64(posting[id]))
			encoded = append(encoded, scratch[:n]...)
			previous = ordinal
		}
		snapshot.Postings[term] = encoded
	}

	compressed := gzip.NewWriter(destination)
	if err := gob.NewEncoder(compressed).Encode(snapshot); err != nil {
		_ = compressed.Close()
		return err
	}
	return compressed.Close()
}

// refReadSnapshot is the original snapshot reader, used to prove that
// snapshots written by the new Index still load in the old code.
func refReadSnapshot(source io.Reader) (*refIndex, error) {
	compressed, err := gzip.NewReader(source)
	if err != nil {
		return nil, err
	}
	defer compressed.Close()
	var snapshot diskSnapshot
	if err := gob.NewDecoder(compressed).Decode(&snapshot); err != nil {
		return nil, err
	}
	if snapshot.Version != 1 {
		return nil, fmt.Errorf("unsupported snapshot version %d", snapshot.Version)
	}
	if len(snapshot.Docs) != len(snapshot.Lengths) {
		return nil, fmt.Errorf("corrupt snapshot: document length table mismatch")
	}

	index := newRefIndex()
	for ordinal, doc := range snapshot.Docs {
		index.docs[doc.ID] = doc
		index.lengths[doc.ID] = snapshot.Lengths[ordinal]
		index.tokens += int64(snapshot.Lengths[ordinal])
	}
	for term, encoded := range snapshot.Postings {
		posting := make(map[string]int)
		previous := -1
		for len(encoded) > 0 {
			gap, n := binary.Uvarint(encoded)
			if n <= 0 {
				return nil, fmt.Errorf("corrupt posting for %q", term)
			}
			encoded = encoded[n:]
			frequency, n := binary.Uvarint(encoded)
			if n <= 0 {
				return nil, fmt.Errorf("corrupt frequency for %q", term)
			}
			encoded = encoded[n:]
			ordinal := previous + int(gap)
			if ordinal < 0 || ordinal >= len(snapshot.Docs) {
				return nil, fmt.Errorf("invalid document ordinal %d", ordinal)
			}
			posting[snapshot.Docs[ordinal].ID] = int(frequency)
			previous = ordinal
		}
		index.postings[term] = posting
	}
	return index, nil
}
