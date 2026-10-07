package video

import (
	"context"
	"errors"
	"time"
)

type Worker struct {
	ID         string
	Queue      *QueueClient
	Transcoder Transcoder
	Lease      time.Duration
}

func (w *Worker) Run(ctx context.Context) error {
	if w.Lease == 0 {
		w.Lease = 30 * time.Second
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		job, err := w.Queue.Lease(ctx, w.ID, w.Lease)
		if errors.Is(err, ErrNoJob) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
				continue
			}
		}
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
				continue
			}
		}
		w.process(ctx, job)
	}
}

func (w *Worker) process(parent context.Context, job Job) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(w.Lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if w.Queue.Heartbeat(ctx, job.ID, w.ID, w.Lease) != nil {
					cancel()
					return
				}
			}
		}
	}()
	err := w.Transcoder.Transcode(ctx, job)
	close(done)
	if err != nil {
		_ = w.Queue.Fail(parent, job.ID, w.ID, err)
		return
	}
	_ = w.Queue.Complete(parent, job.ID, w.ID)
}
