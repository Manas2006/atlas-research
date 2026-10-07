# Impact Experiments

Impact Experiments answer a narrower question than ordinary run tracking: did a treatment cause a measurable change relative to a stable control group? The reference demo measures incremental advertising conversions, but the experiment, assignment, event, and result types do not contain campaign-specific fields.

## Data flow

1. Create a draft with a hypothesis, success outcome, treatment allocation, configuration version, and observation window.
2. Start it, then call the assignment endpoint with a pseudonymous subject ID. Atlas hashes experiment ID, configuration version, and subject ID with SHA-256, so the same inputs always select the same arm.
3. Send versioned `assignment`, `exposure`, and `outcome` events. Every event has an ID, event timestamp, pseudonymous subject ID, experiment ID, arm, and configuration version; outcomes also carry a name and optional numeric value.
4. Atlas keys subject state by pseudonymous subject ID. Outcomes can arrive before exposures: the contribution is recalculated when either side arrives. The first exposure begins the explicit observation window, and only matching outcomes inside that event-time interval are attributed.
5. Aggregate state is re-keyed by experiment and arm. Results report arm sample sizes and conversion rates, absolute/relative lift, incremental conversions/value, a confidence interval, convergence snapshots, and diagnostics.

The stream's watermark is the greatest event time observed minus the configured allowed lateness. An unseen event older than that watermark is recorded as late and does not change results. Duplicates are detected before the late check so retrying an acknowledged event remains idempotent. Subject join state is expired after the attribution deadline passes the watermark; aggregates and event-ID deduplication remain durable.

## Statistics

Conversion is binary per exposed subject: one or more matching outcomes within the observation window count as a conversion. Absolute lift is `p(treatment) - p(control)`; relative lift divides that difference by the control rate. Incremental conversions multiply absolute lift by the treatment sample size. Incremental outcome value compares total attributed value per exposed subject and scales the difference to the treatment population.

The 95% interval is the transparent large-sample Wald interval for a difference in independent proportions:

```text
(p_t - p_c) ± 1.96 × sqrt(p_t(1-p_t)/n_t + p_c(1-p_c)/n_c)
```

Atlas reports **insufficient data** until both arms contain at least 30 exposed subjects and each arm has at least five conversions and five non-conversions. A positive interval recommends launch, a negative interval recommends stop, and an interval crossing zero recommends continuing to collect data. This is a pragmatic decision aid, not a substitute for experiment-design review, power analysis, interference checks, or correction for repeated peeking/multiple comparisons.

## Consistency and recovery contract

The Impact engine is authoritative in an append-only, fsynced WAL under `data/atlas/impact/events.wal`. A delivery batch is serialized as one WAL record and fsynced before in-memory results change or the API acknowledges it. On restart, configurations, event IDs, subject joins, aggregates, diagnostics, and convergence history are deterministically rebuilt by replay.

For an external broker, partition by pseudonymous subject ID and commit source offsets only after the Atlas event request succeeds. A crash before the source offset commit can redeliver a batch; stable event IDs cause replay to absorb it without changing aggregates. The included demo closes/reopens the engine at a batch boundary, redelivers the uncertain batch, and verifies the final sample and estimated lift.

This contract is **durable idempotent processing**, not a claim of end-to-end exactly-once delivery. Atlas cannot atomically commit offsets in an arbitrary external broker. A producer that changes event IDs on retry can double-deliver, and a producer that commits its offset before Atlas acknowledges can lose data. WAL fsync and Chronos metric writes are separate: the WAL/result is authoritative, and a crash can omit a non-authoritative convergence point from Chronos without changing the final experiment result.

## Diagnostics

Diagnostics use two clocks, and the difference matters when quoting them.

- The **stream clock** is when a delivery batch arrived. For live HTTP ingestion it is the server's wall clock when the engine receives the batch. For a backfill or simulation, the caller passes the arrival time with `ProcessBatchAt` (a broker timestamp, or the simulated poll time in the demo), so historical data is not reported as months of lag. Each WAL batch record stores this time as `arrived_at`.
- **Wall-clock durations** are measured with Go's monotonic clock inside the running process.

| Field | Clock | Definition |
| --- | --- | --- |
| `received` | n/a | Events delivered, including duplicates, rejected, and late events. |
| `accepted` | n/a | Events that changed state: not duplicate, rejected, or late. |
| `duplicates` | n/a | Deliveries whose event ID was already seen. |
| `rejected` | n/a | Events with a wrong configuration version, arm, or outcome name. |
| `late` | n/a | Unseen events older than the watermark at arrival; they do not change results. |
| `unmatched_outcomes` | n/a | Outcomes for subjects with no exposure yet, plus those that expired without one. |
| `processing_lag_p50_ms`, `processing_lag_p95_ms`, `processing_lag_p99_ms` | stream | Event-time lag of accepted events: the batch's arrival time minus the event's `event_timestamp`, clamped at zero for producer clock skew. Nearest-rank percentiles over the most recent 2,048 accepted events. |
| `processing_lag_samples` | stream | Number of samples behind the processing-lag percentiles (at most 2,048). |
| `watermark_lag_ms` | stream | The latest arrival time minus the watermark (greatest accepted event time minus allowed lateness). It is evaluated at the latest delivery, not when results are read, so an idle stream does not inflate it. It equals the allowed lateness plus how far the greatest event time trails the latest arrival, so it is at least the allowed lateness unless producer clocks run ahead of the arrival clock. |
| `ack_latency_p50_ms`, `ack_latency_p95_ms`, `ack_latency_p99_ms` | wall | Per delivery batch: from the moment `ProcessBatch` is called (so time spent waiting for the engine lock is included) until the batch's WAL record is written and fsynced and the in-memory aggregates include it. That is the commit point the API acknowledges. It excludes building the returned result, JSON encoding, and HTTP transport. Only successfully acknowledged batches count. Nearest-rank percentiles over the most recent 2,048 batches. |
| `ack_latency_samples` | wall | Number of batches behind the ack-latency percentiles (at most 2,048). |
| `events_per_second` | wall | `received` divided by the wall-clock seconds since the experiment started: an average rate over the experiment's life (replayed events included after a restart), not peak throughput. |
| `recovery_time_ms` | wall | Time this process spent opening the WAL and replaying it. |
| `state_subjects`, `state_bytes_estimate` | n/a | Live subject join state and a rough size estimate of it plus the event-ID index. |

Replay rebuilds the stream-clock diagnostics exactly, because it applies each batch with its persisted `arrived_at`. It never adds ack-latency samples, so how fast replay runs cannot affect those percentiles. Ack latency is process-local: it is not persisted and starts empty after a restart. WAL records written before `arrived_at` existed replay without lag samples rather than with lag measured at replay time.

Ack latency depends on batch size and on the disk's fsync latency, so quote it with both. Processing and watermark lag describe how the input arrived. In the synthetic demo they measure the simulated delivery model below, not Atlas.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET`, `POST` | `/api/impact-experiments` | List summaries or create a draft |
| `GET` | `/api/impact-experiments/{id}` | Inspect configuration, result, history, and diagnostics |
| `POST` | `/api/impact-experiments/{id}/start` | Freeze/start collection |
| `POST` | `/api/impact-experiments/{id}/stop` | Complete and index a searchable conclusion |
| `POST` | `/api/impact-experiments/{id}/assignments` | Resolve a stable arm; optionally persist an assignment event |
| `POST` | `/api/impact-experiments/{id}/events` | Ingest one event or an `events` batch |
| `POST` | `/api/impact-experiments/{id}/demo` | Generate synthetic advertising reference data |

The assignment response includes a 64-way logical subject partition. It makes the required Kafka/keyed-stream partitioning explicit even though the embedded Atlas engine processes HTTP batches directly.

## Synthetic advertising demo

`make impact-demo` creates a treatment/control population with a configured known absolute lift, emits impression exposures, click-like repeat exposures, delayed numeric conversion outcomes, bounded out-of-order delivery, and duplicates. Midstream it closes and reopens the WAL, redelivers the last uncertain batch to model a rebalance/crash before offset commit, and fails unless the sample count, deduplication, recovery, and lift estimate are within the documented tolerance.

The synthetic events are dated from 2026-01-01, so the demo runs on a simulated stream clock. Each record reaches the consumer after a modeled transport delay: a 50 ms floor, a fixed skew of 0 to 2 s for each of the 64 subject partitions, exponential jitter with a 250 ms mean (capped at 5 s), and, for 1% of records, an extra 5 to 45 s straggler delay. Duplicates are redelivered 1 to 20 s after the original. A consumer polls once per second of stream time and takes at most 500 records per poll, and each poll is one `ProcessBatchAt` call with the poll time as its arrival time. Every record is therefore processed within about 74 s of its event time, inside the default two-minute allowed lateness, so the demo reports no late events. The simulated restart takes no stream time; its real cost is `recovery_time_ms`.
