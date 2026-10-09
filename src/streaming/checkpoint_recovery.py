
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

from validate_events import build_validated_stream


CHECKPOINT = "/opt/spark/checkpoints/recovery-v1"


def inspect_batch(batch_df, batch_id):
    batch_df.persist()

    try:
        total = batch_df.count()

        target = batch_df.filter(
            (col("event_id") == "EVT-CHECKPOINT-4001")
            & (col("order_id") == "ORD-4001")
        )

        target_count = target.count()

        print(
            f"\n=== BATCH {batch_id} ===",
            flush=True,
        )

        print(
            f"Evenements conserves : {total}",
            flush=True,
        )

        print(
            f"[TEST] ORD-4001 conserve : "
            f"{target_count}",
            flush=True,
        )

        if target_count:
            target.select(
                "event_id",
                "order_id",
                "topic",
                "partition",
                "offset",
            ).show(truncate=False)

    finally:
        batch_df.unpersist()


def main():
    spark = (
        SparkSession.builder
        .appName("Commerce-Checkpoint-Recovery")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        validated = build_validated_stream(spark)

        valid = (
            validated
            .filter(col("validation_status") == "VALID")
            .withColumn(
                "event_timestamp",
                to_timestamp(col("event_time")),
            )
            .filter(col("event_timestamp").isNotNull())
        )

        deduplicated = (
            valid
            .withWatermark(
                "event_timestamp",
                "10 minutes",
            )
            .dropDuplicatesWithinWatermark(
                ["event_id"]
            )
        )

        print(
            "\n=== TEST CHECKPOINT / RECOVERY ===",
            flush=True,
        )

        query = (
            deduplicated.writeStream
            .foreachBatch(inspect_batch)
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
            raise RuntimeError(
                str(query.exception())
            )

        print(
            "\n[OK] Execution Spark terminee.",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
