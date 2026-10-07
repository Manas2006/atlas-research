# Measured results

Every number below was measured, not estimated. Rerun the commands on your own
hardware before quoting a number, and quote it with the hardware and workload.

- **Commit:** `2b688ae` (fixes), measured 2026-10-07
- **Machine:** Intel Xeon @ 2.80 GHz, 2 vCPUs, 7.8 GiB RAM, Linux 6.18, ext4,
  go1.24.7. This is a small cloud VM, so expect a laptop to be faster.

## Search query path (single index, in process)

Workload: 100,000 synthetic documents of 200 tokens drawn from a Zipf(1.1)
distribution over a 200,000-word vocabulary; 400 top-10 queries, each pairing a
mid-frequency term with a rare term, with a top-10 common term added to every
fourth query. Both rows use the same benchmark file and seeds.

| | Before (`592b048`) | After |
|---|---|---|
| Index build | 7,462 docs/s | 17,412 docs/s |
| Retained heap | 7,149 bytes/doc | 2,504 bytes/doc |
| Query p50 | 3.448 ms | 0.096 ms |
| Query p95 | 98.911 ms | 0.221 ms |
| Query p99 | 127.983 ms | 0.320 ms |

The new scorer returns exactly the same documents, order and scores as the
previous exhaustive scorer. `TestSearchMatchesReferenceLargeCorpus` checks this
on the full 100K corpus after rewriting 30% of documents.

This is a synthetic workload. MaxScore pruning helps most when queries include
rarer terms, so real corpora and query logs will show a different speedup.

```bash
ATLAS_BENCH=1 go test -run 'TestZipfBenchmark|TestSearchMatchesReferenceLargeCorpus' -v ./internal/search/
```

## Hedged replica requests

Coordinator test with one replica that stalls for 2 s:

- **Stalled replica, before:** about 2 s per query. The old code waited out
  the 2 s client timeout before trying the next replica. This figure comes
  from the code path, not a benchmark.
- **Stalled replica, after:** 50.6 to 52.4 ms across three rounds, with the
  hedge delay at 50 ms.
- **Dead replica:** fails over in under 0.4 ms.

```bash
go test -run TestCoordinator -v ./internal/search/
```

## Impact Experiments demo

Synthetic stream: 12,000 subjects, known absolute lift 4.0 points, duplicate
redelivery, and a restart with redelivery of the uncertain batch.

- **Estimated lift:** 3.78 points, 95% CI 2.67 to 4.88; recommendation
  "launch".
- **Duplicates:** 1,419 absorbed; 0 late events.
- **Durable acknowledgement latency** (fsync included, about 9 events per
  batch): p50 0.33 ms, p95 0.58 ms, p99 1.82 ms.
- **Restart recovery:** 68 ms.

Processing and watermark lag in the demo describe its simulated delivery
delay, not Atlas, so they are not results. The demo's events per second
reflects its small one-second batches; use `BenchmarkProcessBatch` for
throughput.

```bash
make impact-demo
```

## Media job lease

| Queue size | Before | After |
|---|---|---|
| 10,000 jobs | about 100 ms per lease | 0.24 to 0.25 ms per lease, 8 allocs |
| 100,000 jobs | not measured here | 0.24 to 0.26 ms per lease, 8 allocs |

After the fix, lease cost no longer grows with queue size. Most of the
remaining time is the fsync for each durable change.

```bash
go test -run '^$' -bench Lease -benchmem ./internal/video/
```
