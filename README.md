
# Real-Time Commerce Streaming Platform

Event-driven e-commerce Data Engineering project using Python,
Apache Kafka, Spark Structured Streaming, Cassandra and Docker.

## Project Objective

Build a streaming platform that processes order, payment and
inventory events to reconstruct the business state of orders.

## Architecture

Python Producers
      |
      v
Apache Kafka
      |
      v
Spark Structured Streaming
      |
      v
Apache Cassandra

## Technology Stack

- Python
- Apache Kafka 4.1.2
- Apache Spark 4.0.4
- Apache Cassandra 5.0
- Docker Compose

## Kafka Topics

| Topic | Partitions | Replication Factor |
|---|---|---|
| orders | 3 | 1 |
| payments | 3 | 1 |
| inventory-events | 3 | 1 |
| dead-letter-events | 1 | 1 |

## Infrastructure Setup

Requirements:
- Docker Desktop
- Docker Compose

Start Kafka:

```bash
docker compose up -d kafka
```

Start Spark:

```bash
docker compose --profile streaming up -d spark-master spark-worker
```

Start Cassandra:

```bash
docker compose --profile storage up -d cassandra
```

Check all services:

```bash
docker compose --profile streaming --profile storage ps
```

Spark Web UI: http://localhost:8080

Stop infrastructure:

```bash
docker compose --profile streaming --profile storage stop
```


## Current Progress

### Completed

**Phase 1 — Architecture**
- Event-driven architecture and event contracts
- Kafka topic and partitioning design
- Cassandra data model design

**Phase 2 — Infrastructure**
- Docker Compose infrastructure
- Kafka broker in KRaft mode
- Four Kafka topics
- Spark Master and Worker startup
- Cassandra single-node startup
- Kafka producer/consumer validation

**Phase 3 — Python Event Producers**
- Order producer (`order_created`)
- Payment producer (`payment_authorized`, `payment_failed`)
- Inventory producer (`inventory_reserved`, `inventory_rejected`)
- Shared JSON event envelope
- Kafka Key based on `order_id`
- Kafka delivery acknowledgments
- Duplicate event simulation
- Late event simulation
- Out-of-order event simulation
- Invalid event simulation

### Next — Phase 4

Spark Structured Streaming:
- Kafka ingestion
- JSON parsing and validation
- Dead Letter Topic
- Event-time processing
- Watermarking
- Deduplication
- Checkpointing

## Running the Python Producers

Activate the virtual environment on Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Install Python dependencies:

```powershell
python -m pip install -r requirements.txt
```

Start Kafka:

```powershell
docker compose up -d kafka
```

Run the producers:

```powershell
python .\src\producers\order_producer.py
python .\src\producers\payment_producer.py
python .\src\producers\inventory_producer.py
```

Generate anomaly scenarios:

```powershell
python .\src\producers\anomaly_producer.py
```

Preview sample events without publishing:

```powershell
python .\src\producers\preview_events.py
```

Each producer execution publishes additional Kafka messages.
Avoid repeated runs when reproducing the initial test results.

The current producers generate simulated business events.
Spark processing and Cassandra persistence are not yet implemented.


**Phase 4 — Spark Structured Streaming**
- Kafka streaming ingestion from three topics
- JSON parsing and event contract validation
- Invalid event routing to dead-letter-events
- Event-time window aggregation and watermarking
- Stateful deduplication using event_id
- Persistent checkpoint and recovery validation

### Next — Phase 5

Business event correlation:
- Orders and payments stream-stream join
- Inventory event correlation
- Order state reconstruction
- Streaming business metrics
