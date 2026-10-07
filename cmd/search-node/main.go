package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/Manas2006/atlas-research/internal/search"
)

func main() {
	address := flag.String("listen", ":8081", "HTTP listen address")
	dataDir := flag.String("data", "data/search", "durable data directory")
	flag.Parse()
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatal(err)
	}
	wal, err := search.OpenWAL(filepath.Join(*dataDir, "documents.wal"))
	if err != nil {
		log.Fatal(err)
	}
	defer wal.Close()
	index := search.NewIndex()
	if err := wal.Replay(func(doc search.Document) error { index.Upsert(doc); return nil }); err != nil {
		log.Fatal(err)
	}
	log.Printf("search node listening on %s with %d recovered documents", *address, index.Len())
	log.Fatal(http.ListenAndServe(*address, (&search.Node{Index: index, WAL: wal}).Handler()))
}
