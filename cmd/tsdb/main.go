package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/Manas2006/atlas-research/internal/tsdb"
)

func main() {
	address := flag.String("listen", ":5050", "HTTP listen address")
	dataDir := flag.String("data", "data/tsdb", "durable data directory")
	blockSize := flag.Int("block-size", 4096, "samples per immutable block")
	flag.Parse()
	store, err := tsdb.OpenStore(*dataDir, *blockSize)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	log.Printf("time-series database listening on %s", *address)
	log.Fatal(http.ListenAndServe(*address, (&tsdb.API{Store: store}).Handler()))
}
