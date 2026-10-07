# Resume evidence checklist

Replace every placeholder only with a result reproduced from the benchmark
protocol. Link the repository and a short architecture note from the resume.

## Atlas Search

- Built a distributed search engine in Go that indexed **N** Wikipedia
  documents with compressed inverted indexes and BM25 ranking, serving **Q**
  queries/second at **L ms** p95 latency.
- Implemented rendezvous-hash sharding, replica failover, and write-ahead-log
  recovery, restoring acknowledged updates in **R seconds** during injected
  process failures.

## Impact Experiments

> Built a fault-tolerant streaming experiment engine that joins delayed outcomes
> to treatment and control exposures, estimates conversion lift with confidence
> intervals, and preserves results through duplicate delivery, consumer
> rebalances, and process crashes.

- Dataset/seed: **N / SEED** synthetic subjects with **C percent** control rate,
  **L percentage-point** known lift, **D percent** duplicate delivery, and an
  injected restart/redelivery at **R percent** of the stream.
- Measured **E events/second**, **P50 ms** p50 and **P95 ms** p95 processing lag,
  **W ms** watermark lag, **S bytes/subjects** state size, and **T ms** restart
  recovery time.
- Estimated lift was **EL percentage points** (95% CI **LOW to HIGH**), an
  absolute error of **ERR percentage points** from the known synthetic lift;
  uninterrupted and recovered aggregate checksums were **CHECKSUM / CHECKSUM**.

Leave every placeholder above intact until the full procedure in
`docs/benchmarking.md` has been reproduced on recorded hardware.

## Quorum KV

- Implemented a replicated key-value store in Go with Raft leader election,
  quorum writes, write-ahead logging, and snapshot recovery across **N** nodes.
- Sustained **Q operations/second** while preserving committed reads and writes
  through leader termination and delayed-message tests.

## Pulse Analytics

- Built a Java/Kafka event pipeline processing **E events/second** with
  event-time windows, idempotent consumers, and explicit offset checkpoints.
- Maintained **L-second** p95 processing lag during a **S x** traffic spike
  using bounded queues, partition rebalancing, and backpressure.

## StreamForge

- Built a Go/FFmpeg video platform that converted concurrent uploads into
  multi-bitrate HLS streams using resumable object writes and lease-based jobs.
- Achieved **P percent** job completion during worker termination through
  idempotent outputs, heartbeats, exponential retries, and dead-letter replay.

## Chronos TSDB

- Built a monitoring database in Go that compressed **N** time-series samples
  into immutable delta-of-delta/XOR blocks and served **Q** range queries/second.
- Implemented fsynced WAL ingestion, restart recovery, labeled series lookup,
  and retention deletion while reducing stored bytes per sample to **B**.
