# Chronos TSDB

Chronos is a compact monitoring database for labeled time-series data. The Go
service fsyncs incoming samples to a WAL, keeps a mutable in-memory head, and
flushes full heads into immutable gzip blocks. Inside each block, timestamps use
delta-of-delta varints and floating-point values use XOR compression.

## Capabilities

- Canonical label sorting and stable 64-bit series fingerprints
- Batched writes and half-open time-range queries
- Last-write-wins deduplication for identical timestamps
- WAL replay across process restart
- Immutable, atomically published compressed blocks
- Explicit flush and retention endpoints
- Codec, recovery, and range-query tests plus a compression benchmark

## Run

```bash
go run ./cmd/tsdb -listen :5050 -data data/tsdb
NOW=$(date +%s000)
curl -X POST localhost:5050/v1/write -H 'content-type: application/json' -d "[
  {\"series\":{\"name\":\"cpu_usage\",\"labels\":{\"host\":\"api-1\"}},
   \"samples\":[{\"timestamp\":$NOW,\"value\":72.5}]}
]"
curl "localhost:5050/v1/query?name=cpu_usage&label.host=api-1&start=0"
```

## Benchmark

```bash
go test -race ./internal/tsdb
go test -bench BenchmarkCodec -benchmem ./internal/tsdb
```

Production next steps would add a block index, background compaction, histogram
types, Prometheus remote-write compatibility, checksummed blocks, and replication.

