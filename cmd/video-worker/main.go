package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Manas2006/atlas-research/internal/video"
)

func main() {
	id := flag.String("id", "worker-1", "worker id")
	api := flag.String("api", "http://127.0.0.1:6060", "queue API base URL")
	ffmpeg := flag.String("ffmpeg", "ffmpeg", "ffmpeg executable")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	worker := video.Worker{ID: *id, Queue: video.NewQueueClient(*api), Transcoder: video.Transcoder{FFmpeg: *ffmpeg}}
	if err := worker.Run(ctx); err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}
