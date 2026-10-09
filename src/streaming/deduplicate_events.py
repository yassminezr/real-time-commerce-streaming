
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

from validate_events import build_validated_stream


CHECKPOINT_PATH = "/tmp/commerce-dedup-checkpoint-v1"


def inspect_batch(batch_df, batch_id):
    """Affiche les evenements apres deduplication."""

    # Evite plusieurs recalculs du meme micro-batch
    batch_df = batch_df.persist()

    try:
        print(f"\n=== BATCH {batch_id} : APRES DEDUPLICATION ===",
              flush=True)

        batch_df.select(
            "event_id",
            "order_id",
            "event_type",
            "topic",
            "partition",
            "offset",
        ).show(100, truncate=False)

        # Verification de notre scenario de doublon
        ord_3001_count = batch_df.filter(
            (col("order_id") == "ORD-3001")
            & (col("event_type") == "order_created")
        ).count()

        print(
            f"[TEST] ORD-3001 : "
            f"{ord_3001_count} evenement(s) conserve(s) "
            f"dans ce batch.",
            flush=True,
        )

    finally:
        batch_df.unpersist()


def main():
    spark = (
        SparkSession.builder
        .appName("Commerce-Streaming-Deduplication")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1. Lire et valider les evenements Kafka
        validated = build_validated_stream(spark)

        # 2. Exclure les evenements invalides
        valid_events = (
            validated
            .filter(col("validation_status") == "VALID")
            .withColumn(
                "event_timestamp",
                to_timestamp(col("event_time"))
            )
            .filter(col("event_timestamp").isNotNull())
        )

        # 3. Activer le watermark
        watermarked = valid_events.withWatermark(
            "event_timestamp",
            "10 minutes"
        )

        # 4. Deduplication stateful par event_id
        deduplicated = (
            watermarked.dropDuplicatesWithinWatermark(
                ["event_id"]
            )
        )

        print("\n=== DEMARRAGE DEDUPLICATION ===",
              flush=True)
        print("Cle de deduplication : event_id",
              flush=True)
        print("Watermark : 10 minutes",
              flush=True)

        # 5. Traiter les messages Kafka disponibles
        query = (
            deduplicated.writeStream
            .foreachBatch(inspect_batch)
            .outputMode("append")
            .option(
                "checkpointLocation",
                CHECKPOINT_PATH
            )
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()

        if query.exception() is not None:
            raise RuntimeError(str(query.exception()))

        print(
            "\n[OK] Test de deduplication termine.",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
