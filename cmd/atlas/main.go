package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/Manas2006/distributed-systems-portfolio/internal/atlas"
)

func main() {
	address := flag.String("listen", ":8088", "HTTP listen address")
	dataDir := flag.String("data", "data/atlas", "durable Atlas data directory")
	flag.Parse()

	app, err := atlas.Open(*dataDir)
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	server := &http.Server{Addr: *address, Handler: app.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("Atlas Research Console listening on http://localhost%s", *address)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
