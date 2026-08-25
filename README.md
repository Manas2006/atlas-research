# Distributed Systems Portfolio

[![CI](https://github.com/Manas2006/distributed-systems-portfolio/actions/workflows/ci.yml/badge.svg)](https://github.com/Manas2006/distributed-systems-portfolio/actions/workflows/ci.yml)

Five production-minded systems projects focused on indexing, storage,
streaming, concurrency, and failure recovery. Each service exposes a small API,
persists critical state, includes failure-oriented tests, and has explicit
benchmark instructions. The repository favors understandable implementations
of systems concepts over framework-heavy demos.

| Project | Stack | Systems concepts |
| --- | --- | --- |
| [Atlas Search](search-engine/README.md) | Go | compressed inverted indexes, BM25, sharding, replica failover, WAL recovery |
| [Quorum KV](kv-store/README.md) | Go | Raft replication, linearizable writes, snapshots, TTLs, range scans |
| [Pulse Analytics](event-analytics/README.md) | Java, Kafka | partitioning, idempotency, event-time windows, late data, backpressure |
| [StreamForge](video-platform/README.md) | Go, FFmpeg | resumable uploads, durable leases, retries, dead letters, HLS transcoding |
| [Chronos TSDB](time-series-db/README.md) | Go | WAL ingestion, delta compression, immutable blocks, range queries, retention |

## Quick start

Requirements: Go 1.23+, Java 17+, Maven 3.9+, Docker, and FFmpeg 6+.

```bash
make test
make search-demo
make kv-demo
make analytics-demo
make video-demo
make tsdb-demo
```

Every benchmark prints machine-readable JSON containing throughput and latency.
Record hardware, dataset size, commit SHA, and command whenever publishing a
number. This keeps README and resume claims reproducible.

## Engineering principles

- Acknowledged writes survive process restart or are rejected before success.
- Retries are safe through idempotency keys or immutable log indexes.
- Network fan-out is bounded by deadlines and cancellation.
- Every queue and cache has a capacity policy.
- Health endpoints distinguish process liveness from service readiness.
- Failure behavior is tested, not merely described.

See [docs/benchmarking.md](docs/benchmarking.md) for the measurement protocol
and [docs/portfolio.md](docs/portfolio.md) for suggested resume bullets after
running the benchmarks.
