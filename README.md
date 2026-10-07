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
- Write live docs with the rest of the lab: everyone types in the same page
  at once, and agents propose tracked suggestions that a person accepts or
  rejects.
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

Open the Runtime control under the navigation to switch between browser mode and a
local API. The console served by the Go process connects automatically because
it shares the same origin.

## Architecture

| Component | Product responsibility | Core implementation |
| --- | --- | --- |
| Atlas Console | Unified browser and local interface | Responsive HTML, CSS, JavaScript, local persistence, connected API mode |
| [Live docs](docs/live-docs.md) | Shared writing with people and agents | Operational transformation in Go and JavaScript, per-document operation log with group commit, WebSocket sync, tracked agent suggestions |
| [Atlas Search](search-engine/README.md) | Knowledge retrieval | Go inverted index, BM25, sharding, replica failover, WAL recovery |
| [Quorum KV](kv-store/README.md) | Replicated metadata foundation | Raft, linearizable writes, snapshots, TTLs, range scans |
| [Pulse Analytics](event-analytics/README.md) | Streaming research events | Java, Kafka, idempotency, event-time windows, late data, backpressure |
| [StreamForge](video-platform/README.md) | Demo media processing | Resumable uploads, durable leases, retries, dead letters, FFmpeg HLS |
| [Chronos TSDB](time-series-db/README.md) | Metrics and experiment signals | WAL ingestion, delta compression, immutable blocks, range queries, retention |

The single-process Atlas runtime composes the knowledge catalog, live docs,
search index, time-series store, and media queue behind one API. The standalone binaries remain
available for distributed failure and scale tests.

## Live docs

Open **Live docs** in a connected console, start a doc, and send its address
to someone else on the same runtime. Both of you can type at once. Select
some text and ask the Librarian for related work already in Atlas, or type
`@librarian topic` on its own line and press Enter. What an agent returns is
a suggestion: nothing in the doc changes until someone accepts it, and a
suggestion whose text was edited in the meantime is marked outdated instead
of being applied to words it was not written for.

To add a model-backed Writer agent, point Atlas at any OpenAI-compatible
endpoint, such as a vLLM server:

```bash
ATLAS_LLM_API_KEY=... go run ./cmd/atlas -llm-url http://localhost:8000/v1 -llm-model your-model
```

Live docs need a runtime that everyone can reach; browser mode has no one to
share with. Atlas has no accounts, so run a shared runtime on a network you
trust and pass `-origins` with the addresses the console is served from. The
[design note](docs/live-docs.md) covers the sync protocol, durability, how
agents are kept from overwriting people, and what is not built yet.

## Verify and benchmark

```bash
make test
make collab-demo
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
- A live doc edit is acknowledged only after its record is fsynced; a resend
  after a lost acknowledgement is applied once; one damaged doc log is set
  aside without stopping the rest.
- Media work uses idempotency keys, expiring leases, retries, and dead letters.
- CI runs Go tests with the race detector, Go vet, Java tests, console syntax
  validation, the shared Go and JavaScript transform vectors, and an end-to-end
  live docs run with dropped connections and a killed server on every change.

Licensed under the [MIT License](LICENSE).
