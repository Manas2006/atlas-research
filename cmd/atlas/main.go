package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Manas2006/distributed-systems-portfolio/internal/atlas"
	"github.com/Manas2006/distributed-systems-portfolio/internal/collab"
)

func main() {
	address := flag.String("listen", ":8088", "HTTP listen address")
	dataDir := flag.String("data", "data/atlas", "durable Atlas data directory")
	llmURL := flag.String("llm-url", os.Getenv("ATLAS_LLM_URL"), "base URL of an OpenAI-compatible API for the Writer agent, for example http://localhost:8000/v1")
	llmModel := flag.String("llm-model", os.Getenv("ATLAS_LLM_MODEL"), "model name the Writer agent requests")
	origins := flag.String("origins", os.Getenv("ATLAS_ORIGINS"), "comma-separated web origins allowed to call the API from a browser; empty allows any")
	flag.Parse()

	var options atlas.Options
	if *origins != "" {
		options.AllowedOrigins = strings.Split(*origins, ",")
	}
	if *llmURL != "" || *llmModel != "" {
		// The key comes from the environment so it never shows up in a
		// process listing or shell history.
		options.Writer = &collab.LLMConfig{BaseURL: *llmURL, Model: *llmModel, APIKey: os.Getenv("ATLAS_LLM_API_KEY")}
	}
	app, err := atlas.OpenWithOptions(*dataDir, options)
	if err != nil {
		log.Fatal(err)
	}
	if options.Writer != nil {
		log.Printf("Writer agent enabled with model %s at %s", *llmModel, *llmURL)
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
