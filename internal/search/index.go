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

// Index is a concurrency-safe inverted index. Documents are retained so an
// update can remove the prior posting frequencies without leaving tombstones.
type Index struct {
	mu       sync.RWMutex
	docs     map[string]Document
	lengths  map[string]int
	postings map[string]map[string]int
	tokens   int64
}

func NewIndex() *Index {
	return &Index{
		docs:     make(map[string]Document),
		lengths:  make(map[string]int),
		postings: make(map[string]map[string]int),
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
	i.mu.Lock()
	defer i.mu.Unlock()

	if old, ok := i.docs[doc.ID]; ok {
		oldTerms, _ := frequencies(old)
		for term := range oldTerms {
			delete(i.postings[term], doc.ID)
			if len(i.postings[term]) == 0 {
				delete(i.postings, term)
			}
		}
		i.tokens -= int64(i.lengths[doc.ID])
	}

	freq, length := frequencies(doc)
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

func (i *Index) Len() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return len(i.docs)
}

// Search uses Okapi BM25 with the conventional k1=1.2 and b=0.75 values.
func (i *Index) Search(query string, limit int) []Result {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if limit <= 0 || len(i.docs) == 0 {
		return nil
	}

	n := float64(len(i.docs))
	avgLength := float64(i.tokens) / n
	scores := make(map[string]float64)
	seenTerms := make(map[string]struct{})
	for _, term := range tokenize(query) {
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
