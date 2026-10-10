
# Phase 5 bis — Integrated Streaming Pipeline

## Overview

An event-driven commerce data pipeline built with
Apache Kafka, PySpark Structured Streaming and Docker.

The pipeline integrates three Spark jobs connected
through Kafka topics.

## Architecture

Python Producers
    |
    v
Kafka: orders / payments / inventory-events
    |
    v
Job 1: Data Validation
    |------------------> dead-letter-events
    v
validated-events
    |
    v
Job 2: Stateful Deduplication
    |
    v
unique-events
    |
    v
Job 3: Business Correlation
    |
    v
order-states

## Implemented Features

- Kafka-based ingestion of commerce events
- JSON parsing and data validation
- Dead Letter Queue for invalid events
- Stateful deduplication using event_id
- Event-time watermarks
- Stream-stream joins with temporal conditions
- Business order-state reconstruction
- Spark checkpointing
- Sequential PowerShell orchestration
- Incremental processing with availableNow

## Validated Tests

| Test | Result |
|---|---|
| Business correlation scenarios | Passed |
| Technical duplicate removal | Passed |
| Restart without new input | No new outputs observed |
| 100 synthetic orders | 100 additional states |
| 1,000 synthetic orders | 1,000 additional states |
| 10,000 synthetic orders | Interrupted; no performance claim |

## Limitations

- Spark executes locally with local[1], not on a
  distributed cluster.
- Jobs run sequentially using availableNow.
- Kafka outputs use at-least-once semantics.
- Event-id deduplication does not guarantee business
  idempotence.
- One order_id may produce multiple business states.
- PENDING and expiration handling are not implemented.
- Stateful metrics are not yet durably persisted.
- The 10,000-order test was interrupted and may have
  produced partial Kafka outputs.
- End-to-end fault recovery has not been fully tested.

## Next Phase

Phase 6: persist business states in Apache Cassandra
using an explicit query-driven data model and
idempotent write strategy.
