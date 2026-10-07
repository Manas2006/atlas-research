package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/Manas2006/atlas-research/internal/search"
)

func main() {
	address := flag.String("listen", ":8080", "HTTP listen address")
	config := flag.String("shards", "a=http://127.0.0.1:8081|http://127.0.0.1:8082", "comma-separated name=url|url replica sets")
	flag.Parse()
	var shards []search.ReplicaSet
	for _, item := range strings.Split(*config, ",") {
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("invalid shard %q", item)
		}
		shards = append(shards, search.ReplicaSet{Name: parts[0], Replicas: strings.Split(parts[1], "|")})
	}
	log.Printf("search coordinator listening on %s with %d shards", *address, len(shards))
	log.Fatal(http.ListenAndServe(*address, search.NewCoordinator(shards).Handler()))
}
