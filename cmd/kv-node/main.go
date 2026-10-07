package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/Manas2006/atlas-research/internal/kv"
)

func main() {
	id := flag.String("id", "n1", "stable node id")
	address := flag.String("listen", ":9001", "HTTP listen address")
	dataDir := flag.String("data", "data/kv-n1", "durable data directory")
	cluster := flag.String("cluster", "n1=http://127.0.0.1:9001,n2=http://127.0.0.1:9002,n3=http://127.0.0.1:9003", "comma-separated id=url map")
	flag.Parse()
	urls := make(map[string]string)
	for _, member := range strings.Split(*cluster, ",") {
		parts := strings.SplitN(member, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("invalid cluster member %q", member)
		}
		urls[parts[0]] = parts[1]
	}
	var peers []string
	for member := range urls {
		if member != *id {
			peers = append(peers, member)
		}
	}
	node, err := kv.NewNode(*id, peers, kv.NewHTTPTransport(urls), *dataDir)
	if err != nil {
		log.Fatal(err)
	}
	node.Start()
	defer node.Stop()
	log.Printf("kv node %s listening on %s", *id, *address)
	log.Fatal(http.ListenAndServe(*address, node.Handler()))
}
