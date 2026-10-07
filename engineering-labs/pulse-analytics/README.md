# Pulse Analytics

> **Engineering lab:** Pulse is standalone and is not required by the Atlas
> runtime or its Impact Experiments workflow.

Pulse is a Java 17 and Kafka event-processing service for impression, click,
view, and conversion streams. Events are keyed by campaign so Kafka preserves
per-campaign ordering while partitions scale across consumer instances.

## Delivery and recovery

- Producers use `acks=all`, idempotence, and LZ4 compression.
- Consumers disable auto-commit and checkpoint offsets only after aggregation.
- A bounded LRU idempotency store absorbs replay under at-least-once delivery.
- Event-time windows accept data within a configurable lateness bound.
- `max.poll.records` bounds each work batch; Kafka lag provides durable
  backpressure instead of an unbounded in-process queue.
- Partition revocation commits completed work before ownership transfers.

## Run

```bash
docker compose -f engineering-labs/pulse-analytics/docker-compose.yml up --build
cd engineering-labs/pulse-analytics
mvn -q exec:java -Dexec.mainClass=dev.manaspathak.pulse.EventGenerator \
  -Dexec.args='localhost:9092 engagement-events 10000'
curl 'localhost:7070/v1/metrics?campaign=campaign-1&limit=10'
```

Run additional `analytics` containers to exercise group rebalancing. Increase
the generator rate until consumer lag grows, then record the last stable event
rate and end-to-end watermark lag.

## Test

```bash
mvn test
```

The aggregation tests cover duplicate delivery, late events, window boundaries,
and conversion-value totals. An end-to-end benchmark should additionally run
against a multi-broker Kafka cluster with replication factor three.
