
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window

from validate_events import build_validated_stream
from prepare_business_streams import prepare_streams
from correlate_inventory import build_correlated_stream
from reconstruct_order_state import reconstruct_states


VOLUME_CHECKPOINT = (
    "/opt/spark/checkpoints/phase5-volume-v1"
)

SUCCESS_CHECKPOINT = (
    "/opt/spark/checkpoints/phase5-success-rate-v1"
)

FINAL_STATUSES = [
    "COMPLETED",
    "PAYMENT_FAILED",
    "INVENTORY_REJECTED",
]

# Compteurs pour cette execution uniquement.
# Ils ne sont pas persistants entre redemarrages.
running_counts = {
    "COMPLETED": 0,
    "PAYMENT_FAILED": 0,
    "INVENTORY_REJECTED": 0,
}


def inspect_volume_batch(batch_df, batch_id):
    """Affiche le volume par fenetre de 5 minutes."""

    batch_df.persist()

    try:
        print(
            f"\n=== VOLUME | BATCH {batch_id} ===",
            flush=True,
        )

        batch_df.orderBy("window_start").show(
            50,
            truncate=False,
        )

    finally:
        batch_df.unpersist()


def inspect_success_batch(batch_df, batch_id):
    """Calcule le taux parmi les etats finaux recus."""

    batch_df.persist()

    try:
        print(
            f"\n=== SUCCESS RATE | BATCH {batch_id} ===",
            flush=True,
        )

        final_rows = batch_df.filter(
            col("order_status").isin(FINAL_STATUSES)
        )

        counts = (
            final_rows
            .groupBy("order_status")
            .count()
            .collect()
        )

        for row in counts:
            running_counts[row["order_status"]] += row["count"]

        completed = running_counts["COMPLETED"]
        total = sum(running_counts.values())

        rate = (
            round(completed / total * 100, 2)
            if total > 0 else 0.0
        )

        print(
            f"[SUCCESS RATE] batch={batch_id} "
            f"completed={completed} "
            f"final_orders={total} "
            f"rate={rate:.2f}%",
            flush=True,
        )

        print(
            "[STATUS COUNTS] "
            + " ".join(
                f"{status}={count}"
                for status, count in running_counts.items()
            ),
            flush=True,
        )

    finally:
        batch_df.unpersist()


def run_volume_metric(spark):
    """Indicateur 1 : volume des commandes par fenetre."""

    validated = build_validated_stream(spark)
    orders, _, _ = prepare_streams(validated)

    # Les timestamps sont deja convertis par prepare_streams.
    volume = (
        orders
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            window(
                col("event_timestamp"),
                "5 minutes",
            )
        )
        .count()
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("count").alias("order_count"),
        )
    )

    print("\n=== INDICATEUR 1 : VOLUME ===", flush=True)

    query = (
        volume.writeStream
        .foreachBatch(inspect_volume_batch)
        .outputMode("update")
        .option("checkpointLocation", VOLUME_CHECKPOINT)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

    if query.exception() is not None:
        raise RuntimeError(str(query.exception()))

    print("[OK] Volume calcule.", flush=True)


def run_success_metric(spark):
    """Indicateur 2 : taux de reussite des commandes."""

    validated = build_validated_stream(spark)

    correlated = build_correlated_stream(validated)
    states = reconstruct_states(correlated)

    print("\n=== INDICATEUR 2 : TAUX DE REUSSITE ===",
          flush=True)

    query = (
        states.writeStream
        .foreachBatch(inspect_success_batch)
        .outputMode("append")
        .option("checkpointLocation", SUCCESS_CHECKPOINT)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

    if query.exception() is not None:
        raise RuntimeError(str(query.exception()))

    completed = running_counts["COMPLETED"]
    total = sum(running_counts.values())
    rate = (
        round(completed / total * 100, 2)
        if total else 0.0
    )

    print(
        f"[FINAL SUCCESS RATE] "
        f"completed={completed} "
        f"final_orders={total} "
        f"rate={rate:.2f}%",
        flush=True,
    )

    print("[OK] Taux de reussite calcule.", flush=True)


def main():
    spark = (
        SparkSession.builder
        .appName("Commerce-Business-Metrics")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        run_volume_metric(spark)
        run_success_metric(spark)

        print(
            "\n[OK] Tests des deux indicateurs termines.",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
    