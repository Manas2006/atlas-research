# Atlas operation guide

## Runtime modes

The console has two useful modes:

1. **Browser mode** keeps knowledge entries, experiment records, and simulated
   media jobs in local storage. It works directly from GitHub Pages without an
   account or backend. Live docs are unavailable in this mode because there is
   nothing to share them through.
2. **Connected mode** sends records to the Go runtime. Entries are durably
   stored, appended to the search WAL, and indexed with BM25. Signals come from
   the running process, metrics use Chronos, and media uploads use StreamForge.

Use the Runtime control at the bottom of the sidebar to change modes. Connection
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
| `video/jobs.json` | Durable media queue and idempotency keys |
| `video/objects/` | Uploaded objects and HLS outputs |

The Docker setup mounts the same layout at `/data` in a named volume.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/health` | Runtime health and durable object counts |
| `GET`, `POST` | `/api/entries` | List or index knowledge entries |
| `GET` | `/api/search?q=...` | Ranked BM25 retrieval |
| `GET`, `POST` | `/api/runs` | List or create experiment records |
| `PUT` | `/api/runs/{id}` | Replace an experiment record |
| `GET`, `POST` | `/api/docs` | List live docs, agents, and templates, or create a doc |
| `GET` | `/api/docs/{id}` | Current text, revision, and open suggestions |
| `GET` | `/api/docs/{id}/ws` | WebSocket for live editing |
| `POST` | `/api/docs/{id}/invoke` | Ask an agent for a suggestion on a range |
| `GET` | `/api/docs/{id}/provenance` | Who wrote which characters, and each agent's accept record |
| `GET` | `/api/docs/{id}/log` | The full edit history as JSON Lines |
| `GET` | `/api/agents` | Agents this runtime offers |
| `GET` | `/api/signals` | Measured process and search signals |
| any | `/api/metrics/*` | Chronos time-series API |
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
[design note](live-docs.md) for the protocol and its limits.

## Extending the distributed topology

Use the standalone binaries when testing multiple processes and failures. Atlas
Search supports shard fan-out and replica fallback, Quorum KV provides Raft
replication, Pulse Analytics runs against Kafka, and the media worker can scale
horizontally because queue ownership is protected by leases.
