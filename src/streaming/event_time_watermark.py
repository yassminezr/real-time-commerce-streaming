
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    window,
    max as spark_max,
    min as spark_min,
)


KAFKA_SERVERS = "kafka:19092"

CHECKPOINT = "/tmp/commerce-watermark-checkpoint-v1"

from validate_events import build_validated_stream


def inspect_event_times(spark):
    """Verifie les timestamps des evenements de test."""

    print("\n=== VERIFICATION DES EVENT TIMES ===", flush=True)

    # Lecture batch pour inspecter les donnees historiques
    raw = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "orders")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    events = raw.selectExpr(
        "CAST(key AS STRING) AS order_id",
        "CAST(value AS STRING) AS raw_json",
    )

    # Rechercher uniquement nos deux evenements tests
    samples = events.filter(
        col("order_id").isin("ORD-3002", "ORD-3004")
    )

    from pyspark.sql.functions import (
        get_json_object,
    )

    samples = samples.withColumn(
        "event_time",
        get_json_object(col("raw_json"), "$.event_time"),
    ).withColumn(
        "event_timestamp",
        to_timestamp(col("event_time")),
    )

    samples.select(
        "order_id",
        "event_time",
        "event_timestamp",
    ).show(truncate=False)

    # Verifier l'ecart entre les timestamps
    stats = samples.agg(
        spark_min("event_timestamp").alias("oldest"),
        spark_max("event_timestamp").alias("newest"),
    ).first()

    if stats["oldest"] is None or stats["newest"] is None:
        raise RuntimeError(
            "Evenements ORD-3002 et ORD-3004 introuvables."
        )

    difference = stats["newest"] - stats["oldest"]

    print(
        f"Ecart entre les Event Times : {difference}",
        flush=True,
    )


def main():
    spark = (
        SparkSession.builder
        .appName("Commerce-Event-Time-Watermark")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1. Verification des donnees historiques
        inspect_event_times(spark)

        # 2. Reutiliser notre flux de validation
        validated = build_validated_stream(spark)

        valid_events = (
            validated
            .filter(col("validation_status") == "VALID")
            .withColumn(
                "event_timestamp",
                to_timestamp(col("event_time")),
            )
            .filter(col("event_timestamp").isNotNull())
        )

        # 3. Definir un watermark de 10 minutes
        watermarked = valid_events.withWatermark(
            "event_timestamp",
            "10 minutes",
        )

        # 4. Aggregation stateful par fenetre de 5 minutes
        window_counts = (
            watermarked
            .groupBy(
                window(
                    col("event_timestamp"),
                    "5 minutes",
                ),
                col("order_id"),
            )
            .count()
        )

        results = window_counts.select(
            col("order_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("count").alias("event_count"),
        )

        print(
            "\n=== DEMARRAGE DU STREAMING AVEC WATERMARK ===",
            flush=True,
        )
        print("Watermark : 10 minutes", flush=True)
        print("Fenetre   : 5 minutes", flush=True)

        # 5. Executer les micro-batches disponibles
        query = (
            results.writeStream
            .format("console")
            .outputMode("update")
            .option("truncate", "false")
            .option("numRows", 100)
            .option("checkpointLocation", CHECKPOINT)
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()

        if query.exception() is not None:
            raise RuntimeError(str(query.exception()))

        # 6. Afficher le watermark observe par Spark
        progress = query.lastProgress

        if progress:
            print(
                "\n=== WATERMARK OBSERVE ===",
                flush=True,
            )
            print(
                progress.get("eventTime", {}),
                flush=True,
            )

        print(
            "\n[OK] Test Event Time et Watermark termine.",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
