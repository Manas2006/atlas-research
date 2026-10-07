# Benchmarking protocol

Do not put unmeasured numbers on a resume. Run benchmarks on an otherwise idle
machine, capture five trials, and report the median throughput plus p50 and p95
latency. Include the CPU model, memory, operating system, dataset size, process
count, and commit SHA.

## Search

1. Ingest at least 500,000 documents from a fixed Wikipedia dump.
2. Warm the page cache and result cache with one complete query pass.
3. Run a 70/20/10 mix of common, medium, and rare queries for five minutes.
4. Repeat while terminating one replica per shard.

## Key-value store

Use uniform and Zipfian key distributions with 50/50 and 95/5 read/write
mixes. Test one, three, and five nodes. Record leader failover time and verify
the linearizability history with the included checker interface.

## Event analytics

Increase producer rate until processing lag grows for three consecutive
minutes. Record the last stable event rate, duplicate rate, late-event rate,
consumer count, partition count, and p95 end-to-end lag.

## Impact Experiments

Use a fixed seed, subject count, control conversion rate, absolute ground-truth
lift, attribution window, lateness bound, and duplicate percentage. Run five
trials of both uninterrupted processing and a restart/redelivery at 50 percent.
Record events processed/second, p50/p95/p99 ack latency (`ack_latency_*_ms`)
with the batch size and storage device, duplicate and late-event rates, live
subject-state count, WAL size, restart recovery time (`recovery_time_ms`), and
absolute difference between estimated and known lift. Verify that both runs
finish with identical sample/conversion aggregates before publishing numbers.

Ack latency and recovery time measure Atlas. Processing lag
(`processing_lag_*_ms`) and watermark lag (`watermark_lag_ms`) are measured on
the stream's arrival clock, so with synthetic input they reflect the simulated
delivery delay, not Atlas; report them only together with that delivery model.
The exact definitions are in
[Impact Experiments](components/impact-experiments.md#diagnostics).

## Video

Use fixed one-minute 1080p inputs. Record completion latency, worker CPU,
retry counts, and success rate while terminating 10 percent of active workers.

## Time series

Ingest a fixed set of 1,000 labeled series at constant and burst rates. Record
samples/second, bytes/sample after flush, WAL replay time, and p50/p95 range
query latency for one-hour, one-day, and seven-day windows.
