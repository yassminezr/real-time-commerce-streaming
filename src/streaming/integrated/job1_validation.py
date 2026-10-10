
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    struct,
    to_json,
)

# Importer le module deja valide en Phase 4
STREAMING_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(STREAMING_DIR))

from validate_events import build_validated_stream


KAFKA_SERVERS = "kafka:19092"

VALIDATED_TOPIC = "validated-events"
DLQ_TOPIC = "dead-letter-events"

CHECKPOINT = (
    "/opt/spark/checkpoints/integrated-validation-v1"
)


def publish_to_kafka(dataframe, topic):
    """Publie un DataFrame batch dans Kafka."""

    (
        dataframe.write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("topic", topic)
        .save()
    )


def process_batch(batch_df, batch_id):
    """Route les evenements valides et invalides."""

    batch_df = batch_df.persist()

    try:
        valid = batch_df.filter(
            col("validation_status") == "VALID"
        )

        invalid = batch_df.filter(
            col("validation_status") == "INVALID"
        )

        valid_count = valid.count()
        invalid_count = invalid.count()

        print(
            f"\n=== VALIDATION | BATCH {batch_id} ===",
            flush=True,
        )

        print(
            f"[COUNT] valid={valid_count} "
            f"invalid={invalid_count}",
            flush=True,
        )

        # 1. Evénements valides : conserver le JSON d'origine
        if valid_count > 0:
            valid_messages = valid.select(
                col("kafka_key").alias("key"),
                col("raw_json").alias("value"),
            )

            publish_to_kafka(
                valid_messages,
                VALIDATED_TOPIC,
            )

            print(
                f"[OK] {valid_count} messages "
                f"publies dans {VALIDATED_TOPIC}",
                flush=True,
            )

        # 2. Evénements invalides : vers la DLQ
        if invalid_count > 0:
            dlq_messages = invalid.select(
                col("kafka_key").alias("key"),
                to_json(
                    struct(
                        col("raw_json").alias(
                            "original_message"
                        ),
                        col("validation_error").alias(
                            "error_reason"
                        ),
                        col("topic").alias("source_topic"),
                        col("partition").alias(
                            "source_partition"
                        ),
                        col("offset").alias("source_offset"),
                        current_timestamp().alias(
                            "rejected_at"
                        ),
                    )
                ).alias("value"),
            )

            publish_to_kafka(
                dlq_messages,
                DLQ_TOPIC,
            )

            print(
                f"[OK] {invalid_count} messages "
                f"publies dans {DLQ_TOPIC}",
                flush=True,
            )

        print(
            f"[OK] Batch {batch_id} traite",
            flush=True,
        )

    finally:
        batch_df.unpersist()


def main():
    spark = (
        SparkSession.builder
        .appName("Commerce-Integrated-Validation")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Relire les topics Kafka sources
        validated_stream = build_validated_stream(spark)

        print(
            "\n=== JOB 1 : INGESTION ET VALIDATION ===",
            flush=True,
        )

        query = (
            validated_stream.writeStream
            .foreachBatch(process_batch)
            .outputMode("append")
            .option(
                "checkpointLocation",
                CHECKPOINT,
            )
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()

        if query.exception() is not None:
            raise RuntimeError(str(query.exception()))

        print(
            "\n[OK] JOB 1 TERMINE",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
