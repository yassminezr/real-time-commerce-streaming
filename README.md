
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

- Docker Compose infrastructure
- Single-broker Kafka in KRaft mode
- Four Kafka topics
- Spark Master and Worker startup
- Single-node Cassandra startup
- Kafka console producer/consumer test

### Planned

- Python event producers
- JSON schema validation
- Event-time and watermarking
- Deduplication
- Stream-stream joins
- Order state reconstruction
- Cassandra persistence
- Checkpointing and reliability scenarios

## Infrastructure Limitations

The project runs locally using a single Kafka broker
and a single Cassandra node.

It does not provide multi-node high availability.

## Status

Phase 2: Infrastructure setup completed.
Phase 3: Python event producers — next.
