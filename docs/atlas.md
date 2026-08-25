# Atlas operation guide

## Runtime modes

The console has two useful modes:

1. **Browser mode** keeps knowledge entries, experiment records, and simulated
   media jobs in local storage. It works directly from GitHub Pages without an
   account or backend.
2. **Connected mode** sends records to the Go runtime. Entries are durably
   stored, appended to the search WAL, and indexed with BM25. Signals come from
   the running process, metrics use Chronos, and media uploads use StreamForge.

Use the Runtime control at the bottom of the sidebar to change modes. Connection
settings stay on the current device.

## Local data

`make atlas` writes beneath `data/atlas` by default:

| Path | Contents |
| --- | --- |
| `catalog.json` | Knowledge metadata and experiment records |
| `knowledge.wal` | Replayable search documents |
| `metrics/` | Chronos WAL and immutable metric blocks |
| `video/jobs.json` | Durable media queue and idempotency keys |
| `video/objects/` | Uploaded objects and HLS outputs |

The Docker setup mounts the same layout at `/data` in a named volume.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/health` | Runtime health and durable object counts |
| `GET`, `POST` | `/api/entries` | List or index knowledge entries |
| `GET` | `/api/search?q=...` | Ranked BM25 retrieval |
| `GET`, `POST` | `/api/runs` | List or create experiment records |
| `PUT` | `/api/runs/{id}` | Replace an experiment record |
| `GET` | `/api/signals` | Measured process and search signals |
| any | `/api/metrics/*` | Chronos time-series API |
| any | `/api/video/*` | StreamForge upload and queue API |

Example:

```bash
curl -X POST http://localhost:8088/api/entries \
  -H 'Content-Type: application/json' \
  -d '{"title":"Evaluation notes","body":"Findings from run 23","type":"Run log","tags":["evaluation"]}'

curl 'http://localhost:8088/api/search?q=evaluation+findings'
```

## Extending the distributed topology

Use the standalone binaries when testing multiple processes and failures. Atlas
Search supports shard fan-out and replica fallback, Quorum KV provides Raft
replication, Pulse Analytics runs against Kafka, and the media worker can scale
horizontally because queue ownership is protected by leases.
