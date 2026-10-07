# Atlas Research

[![CI](https://github.com/Manas2006/atlas-research/actions/workflows/ci.yml/badge.svg)](https://github.com/Manas2006/atlas-research/actions/workflows/ci.yml)
[![Deploy console](https://github.com/Manas2006/atlas-research/actions/workflows/pages.yml/badge.svg)](https://github.com/Manas2006/atlas-research/actions/workflows/pages.yml)

Atlas is a self-hosted research-operations workspace where lab members search technical knowledge, register experiments, write live documents, and inspect runtime signals. Its Impact Experiments workflow uses stable treatment assignment and event-time attribution to estimate lift with a 95% confidence interval, while preserving ordinary experiment/run tracking for work that does not need a holdout. The primary runtime is deliberately compact and understandable; standalone distributed-systems implementations remain available as engineering labs rather than being presented as required product services.

## Five-minute quick start

Prerequisite: Docker with the Compose plugin.

```bash
git clone https://github.com/Manas2006/atlas-research.git
cd atlas-research
docker compose up --build
```

Open [http://localhost:8088](http://localhost:8088). The console connects to the same-origin Atlas runtime automatically, and the named `atlas-data` volume preserves knowledge, runs, live docs, impact events, metrics, and optional media jobs across restarts. Start the optional FFmpeg worker only when a lab needs video processing:

```bash
docker compose --profile media up --build
```

For local Go development, run `make atlas` and open the same address.

## Typical lab workflow

1. Add papers, technical notes, and prior conclusions in **Knowledge**, then retrieve them with ranked full-text search.
2. Register ordinary model or dataset runs in **Experiments**. When the question is causal—“did this change improve the outcome?”—create an Impact Experiment with an outcome, allocation, and observation window.
3. Start the experiment, request stable assignments, and send versioned assignment, exposure, and outcome events. Watch treatment/control rates, lift, uncertainty, convergence, and data-quality diagnostics.
4. Work through interpretation and the launch decision in a shared **Live Doc**. Link its ID when creating the Impact Experiment.
5. Complete the experiment. Atlas writes a searchable conclusion into Knowledge and keeps the lift time series in Chronos for inspection under **Signals**.

The built-in synthetic advertising demo asks: **Did this campaign create additional conversions, or would those users have converted anyway?** It emits synthetic impressions, click-like repeat exposures, delayed conversions, duplicate deliveries, and bounded out-of-order events with a known lift:

```bash
make impact-demo
```

The same engine can measure model rollouts, search-ranking tests, product experiments, and infrastructure changes; none of its data model is advertising-specific.

## Core product capabilities

- **Knowledge** — durable catalog entries and live documents indexed with BM25 search and a replayable write-ahead log.
- **Experiments** — lightweight run registration plus durable treatment-versus-control Impact Experiments.
- **Live Docs** — concurrent human editing, tracked agent suggestions, operation-log recovery, provenance, and searchable completed text.
- **Signals** — measured runtime activity and Chronos time series, including Impact Experiment lift and processing lag.
- **Impact Experiments** — deterministic assignment, event-time exposure/outcome joins, idempotent event IDs, attribution windows, restart recovery, data-quality diagnostics, and understandable difference-in-proportions statistics. See [the design and consistency contract](docs/components/impact-experiments.md).

Media transcoding is an optional tool, not a primary Atlas concept. Its resumable upload and lease-based worker remain available through the Compose `media` profile and are documented in [StreamForge](docs/components/media.md).

To synchronize an extracted Google Drive bundle into the knowledge catalog,
use `atlas-import`. The importer uses stable `gdrive:<file-id>` entry IDs,
records source provenance and content checksums, and skips unchanged records:

```bash
go run ./cmd/atlas-import -input /secure/path/drive-bundle.json
```

Raw Drive exports often contain names, schedules, infrastructure details, or
credentials. Keep bundles under the ignored `data/` directory (or outside the
repository) and see [the Drive ingestion runbook](docs/drive-ingestion.md)
before importing sensitive records.

## Architecture

```text
Browser console
      │ HTTP / WebSocket
      ▼
cmd/atlas ─┬─ Knowledge catalog + Atlas Search
           ├─ Experiment registry + Impact Experiment WAL/engine
           ├─ Live Docs operation logs
           ├─ Chronos metrics
           └─ Optional StreamForge media API ── optional FFmpeg worker

engineering-labs/   (standalone; not required by Atlas)
  quorum-kv/        Raft replicated key-value store
  pulse-analytics/  Java/Kafka event-time analytics
```

The principal source tree is `cmd/atlas` plus focused packages under `internal/`. Product/component notes live in [`docs/components`](docs/components), while standalone demonstrations live under [`engineering-labs`](engineering-labs). Atlas Search's coordinator/node binaries and Chronos's standalone binary remain beside the product because they exercise the same packages the primary runtime integrates.

## Optional engineering labs

- [Quorum KV](engineering-labs/quorum-kv/README.md) — Raft elections, quorum writes, snapshots, TTLs, and range scans. Atlas does not depend on it.
- [Pulse Analytics](engineering-labs/pulse-analytics/README.md) — Java/Kafka event-time windows, deduplication, offset checkpoints, rebalances, and backpressure. It is useful for streaming study but is not Atlas's Impact Experiment engine.
- [Distributed Search](docs/components/search.md) and [Chronos](docs/components/metrics.md) include standalone scale/failure demos backed by product-integrated packages.

## Testing and benchmarking

`make test` is the complete local verification command. It runs Go tests with the race detector, Go vet, Java tests, JavaScript syntax/unit tests, and the live-docs end-to-end failure/restart test.
It requires Go 1.23+, Java 17+, Maven, and Node.js 24+ (the end-to-end test uses Node's built-in WebSocket client).

```bash
make test
make impact-demo
make search-demo
make kv-demo
make tsdb-demo
make analytics-demo       # requires Docker; runs until stopped
make video-demo           # requires FFmpeg
```

Do not publish invented performance claims. Follow the [benchmarking protocol](docs/benchmarking.md), record the hardware, data size, commit, and command, and fill in [the portfolio result template](docs/portfolio.md) only after reproducing a measurement.

## Security and deployment limitations

Atlas currently has **no accounts, authentication, authorization, or tenant isolation**. Deploy it only on a trusted network, restrict browser origins with `-origins`/`ATLAS_ORIGINS`, and do not expose the runtime directly to the public internet. Browser mode is useful for private single-device notes, but connected mode is the intended path for durable shared lab work; Live Docs, Impact Experiments, runtime metrics, and media processing require a reachable Atlas runtime.

The runtime accepts pseudonymous subject IDs for Impact Experiments and the demo uses synthetic identifiers only. Operators are responsible for ensuring event producers do not send names, email addresses, advertising IDs, or other real PII.

See the [operation guide](docs/atlas.md) for API routes, runtime flags, data layout, and recovery behavior. Licensed under the [MIT License](LICENSE).
