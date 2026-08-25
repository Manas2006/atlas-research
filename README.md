# Atlas Research Console

[![CI](https://github.com/Manas2006/distributed-systems-portfolio/actions/workflows/ci.yml/badge.svg)](https://github.com/Manas2006/distributed-systems-portfolio/actions/workflows/ci.yml)
[![Deploy console](https://github.com/Manas2006/distributed-systems-portfolio/actions/workflows/pages.yml/badge.svg)](https://github.com/Manas2006/distributed-systems-portfolio/actions/workflows/pages.yml)

Atlas is a self-hosted research operations workspace backed by a set of focused
distributed systems. It gives you one place to search papers and technical
notes, register experiments, inspect live runtime signals, and prepare research
demo videos. The implementations stay small enough to understand, test, and
extend while preserving the important storage and failure semantics.

**[Open the interactive console](https://manas2006.github.io/distributed-systems-portfolio/)**
or use the [hosted console](https://atlas-research-console.manasp123.chatgpt.site).

The public interface starts in private browser mode, so entries stay on the
current device. Connect it to a locally running Atlas service for durable files,
real BM25 search, runtime measurements, resumable uploads, and media jobs.

## What Atlas is useful for

- Index paper summaries, architecture notes, and experiment findings, then
  retrieve them together with ranked full-text search.
- Keep model, dataset, status, score, parameters, and notes attached to each
  experiment run.
- Inspect measured search latency, request volume, goroutines, heap use, uptime,
  and stored metric series.
- Upload large demo recordings in resumable chunks and queue multi-resolution
  HLS transcoding through lease-based workers.
- Study and extend the underlying search, storage, streaming, and recovery
  mechanisms without hiding them behind a managed service.

## Start it locally

The fastest complete setup uses Docker and includes an FFmpeg media worker:

```bash
docker compose up --build
```

Open [http://localhost:8088](http://localhost:8088). Data survives restarts in
the `atlas-data` volume.

For the Go console without Docker:

```bash
make atlas
```

Open the Runtime control in the sidebar to switch between browser mode and a
local API. The console served by the Go process connects automatically because
it shares the same origin.

## Architecture

| Component | Product responsibility | Core implementation |
| --- | --- | --- |
| Atlas Console | Unified browser and local interface | Responsive HTML, CSS, JavaScript, local persistence, connected API mode |
| [Atlas Search](search-engine/README.md) | Knowledge retrieval | Go inverted index, BM25, sharding, replica failover, WAL recovery |
| [Quorum KV](kv-store/README.md) | Replicated metadata foundation | Raft, linearizable writes, snapshots, TTLs, range scans |
| [Pulse Analytics](event-analytics/README.md) | Streaming research events | Java, Kafka, idempotency, event-time windows, late data, backpressure |
| [StreamForge](video-platform/README.md) | Demo media processing | Resumable uploads, durable leases, retries, dead letters, FFmpeg HLS |
| [Chronos TSDB](time-series-db/README.md) | Metrics and experiment signals | WAL ingestion, delta compression, immutable blocks, range queries, retention |

The single-process Atlas runtime composes the knowledge catalog, search index,
time-series store, and media queue behind one API. The standalone binaries remain
available for distributed failure and scale tests.

## Verify and benchmark

```bash
make test
make search-demo
make kv-demo
make analytics-demo
make video-demo
make tsdb-demo
```

Benchmarks emit machine-readable results. Record hardware, dataset size, commit
SHA, and command before publishing any performance number. See
[the measurement protocol](docs/benchmarking.md) and the
[Atlas operation guide](docs/atlas.md).

## Reliability contract

- Knowledge and metrics recover from write-ahead logs.
- Catalog state is fsynced to a temporary file before atomic replacement.
- Search fan-out uses deadlines and tolerates unavailable replicas.
- Replicated writes require quorum before acknowledgement.
- Media work uses idempotency keys, expiring leases, retries, and dead letters.
- CI runs Go tests with the race detector, Go vet, Java tests, and console syntax
  validation on every change.

Licensed under the [MIT License](LICENSE).
