# Real-Time Commerce Streaming Platform

**Event-Driven Order Processing with Apache Kafka, Spark Structured Streaming, Cassandra & Docker**

An end-to-end data engineering project that simulates e-commerce transactions and processes order, payment, and inventory events through an event-driven streaming architecture.

The platform integrates event validation, dead-letter handling, stateful deduplication, temporal stream-stream joins, business-state reconstruction, and persistent storage in Apache Cassandra.

## Tech Stack

| Category | Technologies |
|---|---|
| Event streaming | Apache Kafka 4.1.2 |
| Stream processing | Apache Spark 4.0.4, PySpark Structured Streaming |
| Database | Apache Cassandra 5.0.9 |
| Programming | Python, SQL/CQL, PowerShell |
| Infrastructure | Docker, Docker Compose |
| Reliability mechanisms | Checkpoints, event-time watermarks, stateful deduplication |
| Data serialization | JSON |

## Architecture

```text
                   Python Event Producers
                             |
              +--------------+--------------+
              |              |              |
           orders         payments    inventory-events
              |              |              |
              +--------------+--------------+
                             |
                      Apache Kafka
                             |
                 JOB 1 - Data Validation
                             |
              +--------------+--------------+
              |                             |
      validated-events              dead-letter-events
              |
       JOB 2 - Stateful Deduplication
              |
         unique-events
              |
       JOB 3 - Business Processing
              |
        Temporal Stream Joins
              |
      Business State Reconstruction
              |
          order-states
              |
       JOB 4 - Cassandra Sink
              |
          Apache Cassandra
              |
      commerce.order_state_history
```

The four Spark jobs are connected through Kafka topics and executed sequentially by a PowerShell orchestration script.

In the current local configuration, each job processes available Kafka data using Spark Structured Streaming's `availableNow` trigger.

## Data Model

Each source event contains:

- `event_id`: unique technical event identifier
- `event_type`: business event type
- `event_time`: event timestamp
- `schema_version`: event schema version
- `order_id`: order identifier
- `customer_id`: customer identifier
- `payload`: event-specific business attributes

Supported events include:

| Stream | Event types |
|---|---|
| Orders | `order_created` |
| Payments | `payment_authorized`, `payment_failed` |
| Inventory | `inventory_reserved`, `inventory_rejected` |

All input transactions are **synthetically generated** for testing and demonstration purposes.

## Processing Pipeline

### Job 1 — Data Validation

Reads the source Kafka topics, parses JSON messages, and applies validation rules.

Valid events are published to `validated-events`. Invalid messages are routed to `dead-letter-events` for inspection.

### Job 2 — Stateful Deduplication

Consumes `validated-events` and removes technical duplicates based on `event_id`, using Spark Structured Streaming state management and a 10-minute event-time watermark.

The resulting events are published to `unique-events`.

Technical deduplication does not imply business-level idempotence when distinct event IDs refer to the same business operation.

### Job 3 — Business Correlation

Consumes `unique-events` and reconstructs business outcomes.

Processing includes:

- Temporal stream-stream join between Orders and Payments
- Second temporal join between authorized payments and Inventory
- Preservation of failed payments without requiring Inventory
- Business-state reconstruction

The temporal joins use a 15-minute matching condition.

Supported output states:

| Business condition | Order state |
|---|---|
| Payment authorized + inventory reserved | `COMPLETED` |
| Payment failed | `PAYMENT_FAILED` |
| Payment authorized + inventory rejected | `INVENTORY_REJECTED` |

`COMPLETED` means successful payment authorization and inventory reservation, not order delivery.

The resulting state records are published to `order-states`.

### Job 4 — Cassandra Persistence

Consumes `order-states` and writes the results into Apache Cassandra.

Storage uses the table:

`commerce.order_state_history`

Primary key:

```sql
PRIMARY KEY ((order_id), state_id)
```

A deterministic SHA-256 hash is calculated from the business-state fields to produce `state_id`.

Writing the same result again uses the same Cassandra primary key, reducing duplicate rows during replay.

The table stores state records rather than guaranteeing a single current state per order.

## Running the Project Locally

### Requirements

- Docker Desktop with Docker Compose
- Python 3 and `confluent-kafka` for the event producers
- PowerShell for the provided orchestration script
- Sufficient local memory for Kafka, Spark, and Cassandra

The project was developed using a resource-constrained local setup with 8 GB of RAM.

### 1. Build the custom Spark image

```powershell
docker compose build spark-master
```

The image includes Spark 4.0.4 and the Cassandra Python driver.

### 2. Start infrastructure

```powershell
docker compose up -d kafka

docker compose --profile streaming up -d spark-master

docker compose --profile storage up -d cassandra
```

Wait for Cassandra to become ready:

```powershell
docker exec rtc-cassandra nodetool status
```

The single-node Cassandra installation should report `UN` (Up/Normal).

### 3. Initialize Kafka topics

The pipeline requires these Kafka topics:

- `orders`
- `payments`
- `inventory-events`
- `dead-letter-events`
- `validated-events`
- `unique-events`
- `order-states`

Topics are configured with replication factor 1 for the local single-broker environment.

### 4. Initialize Cassandra

Create the `commerce` keyspace and `order_state_history` table using the Cassandra schema described in this project.

The local environment uses replication factor 1.

### 5. Generate synthetic events

With the Python dependencies installed, generate 100 test orders:

```powershell
python .\src\producers\load_test_producer.py --orders 100
```

The producer generates related order, payment, and inventory events with unique identifiers.

### 6. Execute the pipeline

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_pipeline.ps1
```

The script runs the four Spark jobs sequentially:

1. Validation
2. Stateful deduplication
3. Business correlation
4. Cassandra persistence

Each streaming job uses an independent persistent checkpoint.

### 7. Query Cassandra

```powershell
docker exec rtc-cassandra cqlsh -e "SELECT order_id, order_status FROM commerce.order_state_history WHERE order_id = 'ORD-P5-001';"
```

Expected business state for the reference scenario:

```text
ORD-P5-001 | COMPLETED
```

### 8. Stop infrastructure

```powershell
docker compose --profile streaming --profile storage stop
```

Named Docker volumes preserve Kafka data, Cassandra data, and Spark checkpoints.

**Important:** The documented pipeline reads existing offsets and state from its checkpoints. Do not delete checkpoints or replay producers casually, as this can change the test results or create duplicate Kafka publications.

## Validation and Results

The project has been tested with controlled scenarios and synthetic workloads.

| Validation | Observed result |
|---|---|
| Business-state scenarios | Passed |
| Payment outside temporal join window | Excluded in batch rule test |
| Failed payment without inventory | Preserved |
| Duplicate `event_id` test | 3 inputs produced 2 deduplicated outputs |
| Pipeline restart without new input | No additional output observed |
| 100-order workload | 100 additional state records |
| 1,000-order workload | 1,000 additional state records |
| Cassandra persistence | 11,105 stored rows observed |

The final observed Cassandra table count was **11,105 state records**.

The larger 10,000-order workload required a lengthy execution on Spark `local[1]`. The completed pipeline run persisted its outputs, but this result is **not a distributed scalability benchmark**.

The project does not claim a measured production throughput or exactly-once end-to-end processing.

## Design Decisions

**Why Kafka instead of direct database writes?**

Kafka decouples event producers from processing consumers, supports asynchronous data flows, and allows processing stages to read durable event logs.

**Why Spark Structured Streaming?**

Spark provides event-time processing, stateful deduplication, temporal joins, micro-batches, and checkpoint-based recovery.

**Why Cassandra?**

Cassandra provides partition-key-oriented access to business records. The history table is designed primarily for retrieving state records associated with a specific order.

**Why multiple Kafka-connected Spark jobs?**

Separating validation, deduplication, and business processing creates clear boundaries between transformations and avoids building a single monolithic stateful query.

**Why a custom Docker image?**

The Spark image includes the Python dependency required to communicate with Cassandra, making container recreation more reproducible.

## Known Limitations

This is a local portfolio implementation, not a production deployment.

- Spark runs in `local[1]` rather than on a distributed cluster.
- Jobs execute sequentially using `availableNow`, not as continuously running concurrent services.
- Kafka writes through `foreachBatch` may be replayed after failures.
- End-to-end exactly-once processing is not guaranteed.
- The Cassandra sink uses synchronous row-by-row writes from the driver, which limits throughput.
- Deduplication is based on technical `event_id`, not business operation identity.
- Multiple distinct state records can exist for one order.
- Automatic `PENDING` and timeout handling are not implemented.
- Streaming metrics were validated separately and are not durably integrated into the final four-job pipeline.
- Comprehensive failure-recovery testing and distributed load benchmarking remain outside the implemented scope.

## Project Focus

This project demonstrates practical experience with event-driven data architectures, Kafka topic-based integration, Spark Structured Streaming, event-time semantics, stateful processing, temporal joins, Cassandra data modeling, Docker-based environments, and reproducible pipeline orchestration.

Built as a hands-on Data Engineering portfolio project.