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
- Leasing does not scan the queue. An in-memory index keeps jobs that are
  waiting for a retry time or a lease expiry in one min-heap and jobs that are
  ready in another, ordered oldest `created_at` first (job ID breaks ties), so
  a lease costs O(log n). The index is rebuilt from durable state on start.
- Every job change is one fsynced, checksummed append to `jobs.json.wal`.
  When the journal holds as many records as there are jobs (at least 1,024),
  the next change rewrites the `jobs.json` snapshot and empties the journal.
  Recovery loads the snapshot, replays newer journal records, and drops a torn
  final record left by a crash.
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

The queue tests advance a fake clock to verify lease order, lease expiry,
reassignment, heartbeats, idempotent job creation, capped retries, dead-letter
behavior, and recovery after reopening, including a torn or stale journal. A
randomized test checks every lease against a full scan of the queue.

```bash
go test -run '^$' -bench Lease -benchmem ./internal/video
```

The Kubernetes manifest provides a worker HPA; a production deployment should
scale on queue depth rather than CPU once a metrics adapter is available.

