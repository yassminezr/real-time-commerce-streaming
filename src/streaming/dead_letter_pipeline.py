
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    struct,
    to_json,
    current_timestamp,
)

from validate_events import build_validated_stream


KAFKA_SERVERS = "kafka:19092"
DLQ_TOPIC = "dead-letter-events"

CHECKPOINT_PATH = "/tmp/commerce-dlq-checkpoint-v1"


def process_batch(batch_df, batch_id):
    """Traite un micro-batch et envoie les erreurs vers Kafka."""

    # Le meme batch sera utilise plusieurs fois.
    batch_df = batch_df.persist()

    try:
        valid_events = batch_df.filter(
            col("validation_status") == "VALID"
        )

        invalid_events = batch_df.filter(
            col("validation_status") == "INVALID"
        )

        valid_count = valid_events.count()
        invalid_count = invalid_events.count()

        print(
            f"\n=== BATCH {batch_id} ===\n"
            f"Evenements valides   : {valid_count}\n"
            f"Evenements invalides : {invalid_count}",
            flush=True,
        )

        if invalid_count == 0:
            print("Aucun message a envoyer en DLQ.", flush=True)
            return

        # Preparer les messages Kafka de la DLQ
        dlq_messages = invalid_events.select(
            col("kafka_key").cast("string").alias("key"),
            to_json(
                struct(
                    col("raw_json").alias("original_message"),
                    col("validation_error").alias("error_reason"),
                    col("topic").alias("source_topic"),
                    col("partition").alias("source_partition"),
                    col("offset").alias("source_offset"),
                    current_timestamp().alias("rejected_at"),
                )
            ).alias("value"),
        )

        # Ecriture vers le topic dead-letter-events
        (
            dlq_messages.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVERS)
            .option("topic", DLQ_TOPIC)
            .save()
        )

        print(
            f"[OK] {invalid_count} message(s) "
            f"envoye(s) vers {DLQ_TOPIC}.",
            flush=True,
        )

        # Afficher les identifiants et raisons de rejet
        invalid_events.select(
            "kafka_key",
            "validation_error",
        ).show(truncate=False)

    finally:
        batch_df.unpersist()


def main():
    spark = (
        SparkSession.builder
        .appName("Commerce-Dead-Letter-Pipeline")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Reutiliser notre validation existante
        validated_stream = build_validated_stream(spark)

        print("\n=== DEMARRAGE DU PIPELINE DLQ ===", flush=True)

        query = (
            validated_stream.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", CHECKPOINT_PATH)
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()

        if query.exception() is not None:
            raise RuntimeError(str(query.exception()))

        print("\n[OK] Pipeline DLQ termine.", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
