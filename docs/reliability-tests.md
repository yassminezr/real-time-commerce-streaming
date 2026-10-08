
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
