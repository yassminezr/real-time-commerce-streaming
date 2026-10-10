
# Phase 5 — Business Correlation Validation

## Architecture

Kafka events are parsed, validated and separated into
Orders, Payments and Inventory streams.

A stream-stream inner join correlates Orders and Payments
using order_id and a 15-minute event-time condition.

Payment-authorized events are correlated with Inventory
using a second temporal inner join.

Payment-failed events are preserved without requiring
an Inventory event.

## Business State Reconstruction

| Order | Payment | Inventory | Final State |
|---|---|---|---|
| ORD-P5-001 | Authorized | Reserved | COMPLETED |
| ORD-P5-002 | Failed | Missing | PAYMENT_FAILED |
| ORD-P5-003 | Authorized | Rejected | INVENTORY_REJECTED |

## Streaming Metrics

- Order volume: 3 orders in the 5-minute test window
- Completed orders: 1
- Finalized orders: 3
- Completion rate: 33.33%

## Validation Tests

| Test | Expected Result | Status |
|---|---|---|
| Normal business scenarios | 3 correct matches | PENDING |
| Payment outside 15-minute window | Excluded | PENDING |
| Failed payment without Inventory | Preserved | PENDING |
| Two distinct payments for one order | 2 possible matches | PENDING |

## Technical Limitations

- The business test suite verifies join rules in Spark batch mode.
- Stream-stream joins were validated separately with Kafka.
- Stateful event-id deduplication is not yet integrated into
  the business correlation pipeline.
- Business-level idempotence is not yet implemented.
- PENDING state and expiration handling remain to be integrated.
- Metrics are not yet durably persisted.
- Cassandra integration is planned for Phase 6.

