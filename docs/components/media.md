# StreamForge

StreamForge is a durable video-processing platform built in Go around FFmpeg.
The API accepts chunked resumable uploads, atomically finalizes objects, creates
idempotent jobs, and leases work to independently scalable workers. Workers
produce 360p, 720p, and 1080p HLS variants plus a master manifest.

## Failure behavior

- Every upload chunk is fsynced before the server advances `Upload-Offset`.
- Completing the same upload or repeating an idempotency key returns one job.
- A worker must heartbeat its lease; a terminated worker's job becomes eligible
  after lease expiry.
- Failures use capped exponential backoff and become dead letters after five
  attempts. An operator can explicitly replay a dead job.
- Renditions write to a temporary directory and atomically rename only after all
  FFmpeg processes succeed, so retries never publish partial manifests.

## Run

```bash
go run ./cmd/video-api -listen :6060 -data data/video
go run ./cmd/video-worker -id worker-1 -api http://localhost:6060

UPLOAD=$(curl -s -X POST localhost:6060/v1/uploads | jq -r .upload_id)
curl -X PATCH "localhost:6060/v1/uploads/$UPLOAD" \
  -H 'Upload-Offset: 0' --data-binary @sample.mp4
curl -X POST "localhost:6060/v1/uploads/$UPLOAD/complete"
curl "localhost:6060/v1/jobs/$UPLOAD"
```

## Test

```bash
go test -race ./internal/video
```

The queue tests advance a fake clock to verify lease expiry, reassignment,
idempotent job creation, persistence, capped retries, and dead-letter behavior.
The Kubernetes manifest provides a worker HPA; a production deployment should
scale on queue depth rather than CPU once a metrics adapter is available.

