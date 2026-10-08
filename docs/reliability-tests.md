
# Reliability and Validation Tests

## Phase 2 — Infrastructure Validation

### Kafka

- Broker started successfully
- Kafka version: 4.1.2
- KRaft mode enabled
- Broker ID: 1
- Four topics created

### Spark

- Spark Master started
- One Spark Worker registered
- Worker status: ALIVE

### Cassandra

- Single Cassandra node started
- nodetool status: UN (Up / Normal)

### Kafka Producer / Consumer Test

Test event:
- event_id: EVT-001
- event_type: order_created
- order_id: ORD-1001

Observed results:
- Topic: orders
- Partition: 1
- Offset: 0
- Consumer successfully retrieved the event

## Resource Monitoring

Observed container memory usage:

- Kafka: 363.7 MiB
- Spark Master: 221.1 MiB
- Spark Worker: 151.3 MiB
- Cassandra: 987.7 MiB

Measurements were taken while services were idle.

## Pending Reliability Tests

- Duplicate event handling
- Invalid event handling
- Late and out-of-order events
- Spark checkpoint recovery
- Idempotent Cassandra writes

These tests have not yet been implemented.


## Phase 3 — Python Producers

### Functional Tests

| Test | Result |
|---|---|
| Order Producer | PASS |
| Payment Producer | PASS |
| Inventory Producer | PASS |
| JSON serialization | PASS |
| Kafka Key = order_id | PASS |
| Kafka delivery confirmation | PASS |
| Kafka console consumption | PASS |

### Anomaly Scenarios

| Scenario | Kafka Topic | Result |
|---|---|---|
| Duplicate event ORD-3001 | orders | PASS |
| Late event ORD-3004 | orders | PASS |
| Out-of-order payment ORD-3003 | payments + orders | PASS |
| Missing customer_id ORD-3005 | orders | PASS |
| Malformed JSON ORD-3006 | orders | PASS |

### Observations

- Duplicate messages were accepted by Kafka with identical event_id.
- Event timestamps can differ from publication time.
- Payment was published before its corresponding order for ORD-3003.
- Invalid JSON and missing required fields were accepted by Kafka.
- Cross-topic ordering is not guaranteed.
- Kafka console consumers successfully retrieved the test messages.

### Known Limitations

- Kafka uses a single broker without replication-based fault tolerance.
- Event producers currently use simulated data.
- Re-running producers creates additional events.
- Some Kafka connection attempts used IPv6 localhost before successfully connecting over IPv4.
- Anomaly detection and correction are not implemented yet.

### Planned Phase 4 Validation

- JSON schema validation
- Dead Letter Topic routing
- Deduplication by event_id
- Event-time and watermark behavior
- Checkpoint and recovery tests

