# Atlas operation guide

## Runtime modes

The console has two useful modes:

1. **Browser mode** keeps knowledge entries and experiment records created by
   the user in local storage. It works directly from GitHub Pages without an
   account or backend. Impact Experiments, runtime signals, media jobs, and
   live docs require a connected Atlas service. Empty screens display no
   invented records.
2. **Connected mode** sends records to the Go runtime. Entries are durably
   stored, appended to the search WAL, and indexed with BM25. Signals and the
   last 12 minutes of request counts come from the running process; the history
   resets when it restarts. Impact experiment state is replayed from its own
   WAL, metrics use Chronos, and optional media jobs come from StreamForge.

Use the Runtime control under the navigation to change modes. Connection
settings stay on the current device.

## Local data

`make atlas` writes beneath `data/atlas` by default:

| Path | Contents |
| --- | --- |
| `catalog.json` | Knowledge metadata and experiment records |
| `knowledge.wal` | Replayable search documents |
| `docs/<id>.oplog` | One live doc: every edit, suggestion, and decision, in order |
| `docs/.lock` | Held while a process owns the docs, so a second one cannot start on them |
| `metrics/` | Chronos WAL and immutable metric blocks |
| `impact/events.wal` | Impact configurations, delivery batches, event IDs, and replay source |
| `video/jobs.json` | Optional durable media queue snapshot and idempotency keys |
| `video/jobs.json.wal` | Media job changes since that snapshot |
| `video/objects/` | Optional uploaded objects and HLS outputs |

The Docker setup mounts the same layout at `/data` in a named volume.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/health` | Runtime health and durable object counts |
| `GET`, `POST` | `/api/entries` | List or index knowledge entries |
| `GET` | `/api/search?q=...` | Ranked BM25 retrieval |
| `GET`, `POST` | `/api/runs` | List or create experiment records |
| `PUT` | `/api/runs/{id}` | Replace an experiment record |
| `GET`, `POST` | `/api/impact-experiments` | List impact results or create a draft |
| `GET` | `/api/impact-experiments/{id}` | Configuration, result, convergence, diagnostics |
| `POST` | `/api/impact-experiments/{id}/start` | Begin collection |
| `POST` | `/api/impact-experiments/{id}/stop` | Complete and index a conclusion |
| `POST` | `/api/impact-experiments/{id}/assignments` | Resolve/persist a stable assignment |
| `POST` | `/api/impact-experiments/{id}/events` | Ingest one event or a delivery batch |
| `POST` | `/api/impact-experiments/{id}/demo` | Run the synthetic advertising reference data |
| `GET`, `POST` | `/api/docs` | List live docs, agents, and templates, or create a doc |
| `GET` | `/api/docs/{id}` | Current text, revision, and open suggestions |
| `GET` | `/api/docs/{id}/ws` | WebSocket for live editing |
| `POST` | `/api/docs/{id}/invoke` | Ask an agent for a suggestion on a range |
| `GET` | `/api/docs/{id}/provenance` | Who wrote which characters, and each agent's accept record |
| `GET` | `/api/docs/{id}/log` | The full edit history as JSON Lines |
| `GET` | `/api/agents` | Agents this runtime offers |
| `GET` | `/api/signals` | Measured process and search signals |
| any | `/api/metrics/*` | Chronos time-series API |
| `GET` | `/api/video/v1/jobs` | Persisted media jobs |
| any | `/api/video/*` | StreamForge upload and queue API |

Example:

```bash
curl -X POST http://localhost:8088/api/entries \
  -H 'Content-Type: application/json' \
  -d '{"title":"Evaluation notes","body":"Findings from run 23","type":"Run log","tags":["evaluation"]}'

curl 'http://localhost:8088/api/search?q=evaluation+findings'
```

## Runtime flags

| Flag | Environment | Purpose |
| --- | --- | --- |
| `-listen` | | HTTP listen address, default `:8088` |
| `-data` | | Data directory, default `data/atlas` |
| `-llm-url` | `ATLAS_LLM_URL` | Base URL of an OpenAI-compatible API; enables the Writer agent |
| `-llm-model` | `ATLAS_LLM_MODEL` | Model the Writer agent requests |
| | `ATLAS_LLM_API_KEY` | Bearer token for that API, if it needs one |
| `-origins` | `ATLAS_ORIGINS` | Comma-separated web origins allowed to call the API from a browser. Empty allows any, which is only appropriate on localhost |

## Live docs

Each doc is one append-only log. To read a doc's history outside Atlas, fetch
`/api/docs/{id}/log`, or strip the first nine characters (a checksum and a
space) from each line of the `.oplog` file. Records have a kind in `k`:
`meta` for the title, `op` for an edit with its author in `a`, `sug` for an
agent's suggestion, and `res` for a rejection or dismissal. An accepted
suggestion is an `op` whose author is the agent, with `accepts` naming the
suggestion and `by` naming who accepted it.

If a doc's log cannot be replayed, Atlas starts without that doc, lists it as
damaged on the Live docs page, and leaves the file as it found it. See the
[design note](components/live-docs.md) for the protocol and its limits.

## Impact event example

The [Impact Experiments design note](components/impact-experiments.md) defines
the statistics and recovery contract. Events use pseudonymous subject IDs:

```bash
curl -X POST http://localhost:8088/api/impact-experiments/EXPERIMENT_ID/events \
  -H 'Content-Type: application/json' \
  -d '{"events":[
    {"event_id":"imp-1","type":"exposure","subject_id":"anon-42","arm":"treatment","event_timestamp":"2026-10-07T16:00:00Z","config_version":1},
    {"event_id":"order-1","type":"outcome","subject_id":"anon-42","arm":"treatment","event_timestamp":"2026-10-07T16:15:00Z","outcome_name":"conversion","value":42,"config_version":1}
  ]}'
```

## Look

The console follows the HUMAIN Lab website: an off-white page (`#faf9f6`),
black ink, hairline rules (`#d1d1d1`), Libre Baskerville for text and
JetBrains Mono for figures and code. The colors and typefaces are variables
at the top of `internal/atlas/ui/styles.css`, so a change to the lab's palette
is a change in one place.

Both typefaces are served by Atlas itself from `internal/atlas/ui/fonts`, so
the console looks the same with no network. They are licensed under the SIL
Open Font License; the license texts sit beside the font files.

## Extending the distributed topology

Use the standalone Search and Chronos binaries when testing product packages at
multiple-process scale. The standalone Quorum KV and Pulse Analytics projects
are explicitly optional engineering labs under `engineering-labs/`; Atlas does
not call them. The optional media worker scales horizontally because queue
ownership is protected by leases.
