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
