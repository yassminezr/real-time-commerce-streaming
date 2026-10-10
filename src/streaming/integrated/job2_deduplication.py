
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    get_json_object,
    to_timestamp,
)


KAFKA_SERVERS = "kafka:19092"

SOURCE_TOPIC = "validated-events"
TARGET_TOPIC = "unique-events"

CHECKPOINT = (
    "/opt/spark/checkpoints/integrated-deduplication-v1"
)


def build_deduplicated_stream(spark):
    """Lit Kafka et elimine les doublons techniques."""

    # 1. Lire les evenements valides dans Kafka
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50)
        .load()
    )

    # 2. Extraire les champs necessaires sans
    # modifier le JSON d'origine.
    events = (
        kafka_stream
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_json"),
        )
        .withColumn(
            "event_id",
            get_json_object(col("raw_json"), "$.event_id"),
        )
        .withColumn(
            "event_time",
            get_json_object(col("raw_json"), "$.event_time"),
        )
        .withColumn(
            "event_timestamp",
            to_timestamp(col("event_time")),
        )
    )

    # Le Job 1 a deja valide les messages.
    # Cette protection evite les cles nulles
    # dans l'etat de deduplication.
    events = events.filter(
        col("event_id").isNotNull()
        & col("event_timestamp").isNotNull()
    )

    # 3. Watermark et deduplication stateful
    deduplicated = (
        events
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicatesWithinWatermark(["event_id"])
    )

    return deduplicated


def process_batch(batch_df, batch_id):
    """Publie les evenements retenus dans Kafka."""

    batch_df.persist()

    try:
        count = batch_df.count()

        print(
            f"\n=== DEDUPLICATION | BATCH {batch_id} ===",
            flush=True,
        )

        print(
            f"[COUNT] unique_events={count}",
            flush=True,
        )

        if count > 0:
            kafka_output = batch_df.select(
                col("kafka_key").alias("key"),
                col("raw_json").alias("value"),
            )

            (
                kafka_output.write
                .format("kafka")
                .option(
                    "kafka.bootstrap.servers",
                    KAFKA_SERVERS,
                )
                .option("topic", TARGET_TOPIC)
                .save()
            )

            print(
                f"[OK] {count} evenements publies "
                f"dans {TARGET_TOPIC}",
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
        .appName("Commerce-Integrated-Deduplication")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        stream = build_deduplicated_stream(spark)

        print(
            "\n=== JOB 2 : DEDUPLICATION STATEFUL ===",
            flush=True,
        )

        query = (
            stream.writeStream
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
            "\n[OK] JOB 2 TERMINE",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
    