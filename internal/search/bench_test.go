package search

import (
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The synthetic workload: 100K documents of exactly 200 tokens drawn from a
// Zipf(s=1.1) distribution over a 200K-word vocabulary. Queries pair one
// mid-frequency term (rank 101 to 1000) with one rare term (rank 10,001 to
// 100,000), and every fourth query also carries a top-10 common term.
const (
	zipfDocs        = 100_000
	zipfVocab       = 200_000
	zipfDocTokens   = 200
	zipfTitleTokens = 6
	zipfExponent    = 1.1
	benchCorpusSeed = 20261007
	benchQuerySeed  = 4242
)

type zipfWorkload struct {
	vocab []string // vocab[k] is the word of Zipf rank k+1
}

func newZipfWorkload() *zipfWorkload {
	vocab := make([]string, zipfVocab)
	for k := range vocab {
		vocab[k] = "w" + strconv.Itoa(k)
	}
	return &zipfWorkload{vocab: vocab}
}

type zipfSource struct {
	workload *zipfWorkload
	rng      *rand.Rand
	zipf     *rand.Zipf
	words    []string
}

func (w *zipfWorkload) source(seed int64) *zipfSource {
	rng := rand.New(rand.NewSource(seed))
	return &zipfSource{
		workload: w,
		rng:      rng,
		zipf:     rand.NewZipf(rng, zipfExponent, 1, zipfVocab-1),
		words:    make([]string, zipfDocTokens),
	}
}

func zipfDocID(n int) string { return "doc" + strconv.Itoa(1_000_000 + n)[1:] }

func (s *zipfSource) document(id string) Document {
	for k := range s.words {
		s.words[k] = s.workload.vocab[s.zipf.Uint64()]
	}
	return Document{
		ID:    id,
		Title: strings.Join(s.words[:zipfTitleTokens], " "),
		Body:  strings.Join(s.words[zipfTitleTokens:], " "),
	}
}

// generate calls fn with every corpus document, always in the same order.
func (w *zipfWorkload) generate(fn func(Document)) {
	source := w.source(benchCorpusSeed)
	for n := 0; n < zipfDocs; n++ {
		fn(source.document(zipfDocID(n)))
	}
}

func (w *zipfWorkload) queries(count int, seed int64) []string {
	rng := rand.New(rand.NewSource(seed))
	queries := make([]string, count)
	for k := range queries {
		terms := []string{
			w.vocab[100+rng.Intn(900)],       // mid frequency
			w.vocab[10_000+rng.Intn(90_000)], // rare
		}
		if k%4 == 0 {
			terms = append(terms, w.vocab[rng.Intn(10)]) // top-10 common
		}
		rng.Shuffle(len(terms), func(a, b int) { terms[a], terms[b] = terms[b], terms[a] })
		queries[k] = strings.Join(terms, " ")
	}
	return queries
}

func heapAlloc() uint64 {
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

// TestZipfBenchmark reports index build rate, retained heap per document and
// query latency percentiles. Run it with:
//
//	ATLAS_BENCH=1 go test -run TestZipfBenchmark -v ./internal/search/
func TestZipfBenchmark(t *testing.T) {
	if os.Getenv("ATLAS_BENCH") != "1" {
		t.Skip("set ATLAS_BENCH=1 to run the 100K-document benchmark")
	}
	workload := newZipfWorkload()
	queries := workload.queries(400, benchQuerySeed)

	before := heapAlloc()
	docs := make([]Document, 0, zipfDocs)
	textBytes := 0
	workload.generate(func(doc Document) {
		docs = append(docs, doc)
		textBytes += len(doc.ID) + len(doc.Title) + len(doc.Body)
	})
	index := NewIndex()
	start := time.Now()
	for _, doc := range docs {
		index.Upsert(doc)
	}
	build := time.Since(start)
	docs = nil
	// The index still references every document's text, so the delta covers
	// the stored documents plus all index structures.
	retained := heapAlloc() - before

	for _, query := range queries[:40] {
		_ = index.Search(query, 10) // warm up
	}
	latencies := make([]time.Duration, len(queries))
	var total time.Duration
	results := 0
	for k, query := range queries {
		started := time.Now()
		got := index.Search(query, 10)
		latencies[k] = time.Since(started)
		total += latencies[k]
		results += len(got)
	}
	sort.Slice(latencies, func(a, b int) bool { return latencies[a] < latencies[b] })
	percentile := func(p float64) time.Duration {
		rank := int(p*float64(len(latencies))+0.999999) - 1
		return latencies[rank]
	}
	ms := func(d time.Duration) string {
		return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 3, 64) + " ms"
	}

	t.Logf("CPU: %d logical CPUs, GOMAXPROCS=%d, %s/%s, %s", runtime.NumCPU(), runtime.GOMAXPROCS(0), runtime.GOOS, runtime.GOARCH, runtime.Version())
	t.Logf("documents: %d (Len=%d), %d tokens each", zipfDocs, index.Len(), zipfDocTokens)
	t.Logf("build: %s total, %.0f docs/s", build.Round(time.Millisecond), float64(zipfDocs)/build.Seconds())
	t.Logf("heap: %.1f MB retained, %.0f bytes/doc (of which %.0f bytes/doc is the stored ID, title and body text)", float64(retained)/(1<<20), float64(retained)/zipfDocs, float64(textBytes)/zipfDocs)
	t.Logf("queries: %d, mean %s, p50 %s, p95 %s, p99 %s, max %s (avg %.1f results)",
		len(latencies), ms(total/time.Duration(len(latencies))), ms(percentile(0.50)), ms(percentile(0.95)), ms(percentile(0.99)), ms(latencies[len(latencies)-1]), float64(results)/float64(len(latencies)))
	runtime.KeepAlive(index)
}
