package search

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestBM25AndUpdate(t *testing.T) {
	index := NewIndex()
	index.Upsert(Document{ID: "1", Title: "Distributed systems", Body: "replication consensus storage"})
	index.Upsert(Document{ID: "2", Title: "Cooking", Body: "tomato soup"})
	results := index.Search("consensus storage", 10)
	if len(results) != 1 || results[0].ID != "1" {
		t.Fatalf("unexpected results: %#v", results)
	}
	index.Upsert(Document{ID: "1", Title: "Cooking", Body: "bread"})
	if results := index.Search("consensus", 10); len(results) != 0 {
		t.Fatalf("stale posting survived update: %#v", results)
	}
}

func TestCompressedSnapshotRoundTrip(t *testing.T) {
	index := NewIndex()
	for _, doc := range []Document{
		{ID: "10", Title: "Alpha", Body: "distributed search search"},
		{ID: "20", Title: "Beta", Body: "search indexing"},
	} {
		index.Upsert(doc)
	}
	var encoded bytes.Buffer
	if err := index.WriteSnapshot(&encoded); err != nil {
		t.Fatal(err)
	}
	restored, err := ReadSnapshot(&encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got := restored.Search("distributed search", 2); len(got) != 2 || got[0].ID != "10" {
		t.Fatalf("unexpected restored result: %#v", got)
	}
}

func TestWALRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "documents.wal")
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		if err := wal.Append(Document{ID: id, Body: "recovery"}); err != nil {
			t.Fatal(err)
		}
	}
	index := NewIndex()
	if err := wal.Replay(func(doc Document) error { index.Upsert(doc); return nil }); err != nil {
		t.Fatal(err)
	}
	if index.Len() != 2 {
		t.Fatalf("recovered %d documents", index.Len())
	}
	_ = wal.Close()
}

func BenchmarkSearch(b *testing.B) {
	index := NewIndex()
	for n := 0; n < 10_000; n++ {
		index.Upsert(Document{ID: string(rune(n + 1)), Title: "distributed search", Body: "index replication recovery ranking"})
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = index.Search("replication ranking", 10)
	}
}
