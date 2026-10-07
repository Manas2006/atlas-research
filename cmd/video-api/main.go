package main

import (
	"flag"
	"log"
	"net/http"
	"path/filepath"

	"github.com/Manas2006/atlas-research/internal/video"
)

func main() {
	address := flag.String("listen", ":6060", "HTTP listen address")
	dataDir := flag.String("data", "data/video", "durable data directory")
	flag.Parse()
	queue, err := video.OpenQueue(filepath.Join(*dataDir, "jobs.json"))
	if err != nil {
		log.Fatal(err)
	}
	store, err := video.NewObjectStore(filepath.Join(*dataDir, "objects"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("video API listening on %s", *address)
	log.Fatal(http.ListenAndServe(*address, (&video.API{Queue: queue, Store: store}).Handler()))
}
