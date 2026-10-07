# Quorum KV

> **Engineering lab:** Quorum KV is a standalone Raft implementation. The Atlas
> runtime does not depend on it.

Quorum KV is a replicated key-value store implementing the core Raft protocol:
randomized leader election, log matching, majority replication, ordered commit,
and follower repair after divergent logs. The state machine adds TTL values,
deletes, prefix scans, and half-open key ranges.

## Safety path

1. A leader appends and fsyncs a command to its WAL.
2. Followers validate the preceding log index and term before fsyncing entries.
3. The leader commits only after a cluster majority acknowledges the entry.
4. Reads perform a quorum heartbeat before consulting applied state.
5. Metadata stores term, vote, and commit index; snapshots store applied state.

This is an educational implementation. It deliberately prioritizes readable
protocol code over production optimizations such as pipelined replication,
membership changes, batched fsync, and log compaction after snapshots.

## Run a three-node cluster

```bash
go run ./engineering-labs/quorum-kv/cmd/kv-node -id n1 -listen :9001 -data data/n1
go run ./engineering-labs/quorum-kv/cmd/kv-node -id n2 -listen :9002 -data data/n2
go run ./engineering-labs/quorum-kv/cmd/kv-node -id n3 -listen :9003 -data data/n3

curl -X PUT localhost:9001/v1/kv/course \
  -H 'content-type: application/json' -d '{"value":"distributed-systems","ttl_seconds":60}'
curl localhost:9001/v1/kv/course
curl 'localhost:9001/v1/range?prefix=course&limit=100'
```

Poll `/healthz` to locate the elected leader. Followers return HTTP 307 and a
leader hint for client operations.

## Failure tests

```bash
go test -race ./engineering-labs/quorum-kv/internal/kv
```

The test transport drops selected nodes and verifies that one failure retains
write availability while loss of a majority prevents acknowledgement.
