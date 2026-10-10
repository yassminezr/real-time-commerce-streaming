
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

from validate_events import build_validated_stream
from correlate_inventory import build_correlated_stream


CHECKPOINT = "/opt/spark/checkpoints/phase5-order-state-v1"

EXPECTED_STATES = {
    "ORD-P5-001": "COMPLETED",
    "ORD-P5-002": "PAYMENT_FAILED",
    "ORD-P5-003": "INVENTORY_REJECTED",
}


def reconstruct_states(correlated):
    """Transforme les evenements correles en etats metier."""

    return (
        correlated
        .withColumn(
            "order_status",
            when(
                col("payment_status") == "payment_failed",
                lit("PAYMENT_FAILED"),
            )
            .when(
                (col("payment_status") == "payment_authorized")
                & (col("inventory_status") == "inventory_reserved"),
                lit("COMPLETED"),
            )
            .when(
                (col("payment_status") == "payment_authorized")
                & (col("inventory_status") == "inventory_rejected"),
                lit("INVENTORY_REJECTED"),
            )
            .otherwise(lit("UNKNOWN")),
        )
        .select(
            "order_id",
            "customer_id",
            "order_amount",
            "order_time",
            "payment_status",
            "inventory_status",
            "order_status",
        )
    )


def inspect_batch(batch_df, batch_id):
    """Affiche et verifie les etats du micro-batch."""

    batch_df.persist()

    try:
        print(
            f"\n=== ORDER STATES | BATCH {batch_id} ===",
            flush=True,
        )

        batch_df.orderBy("order_id").show(
            50,
            truncate=False,
        )

        total = batch_df.count()

        print(
            f"[STATE COUNT] batch={batch_id} rows={total}",
            flush=True,
        )

        for order_id, expected in EXPECTED_STATES.items():
            matches = (
                batch_df
                .filter(
                    (col("order_id") == order_id)
                    & (col("order_status") == expected)
                )
                .count()
            )

            print(
                f"[TEST] {order_id} -> {expected}: "
                f"{matches} row(s)",
                flush=True,
            )

        unknown = batch_df.filter(
            col("order_status") == "UNKNOWN"
        ).count()

        if unknown:
            print(
                f"[WARNING] {unknown} etat(s) UNKNOWN",
                flush=True,
            )

    finally:
        batch_df.unpersist()


def main():
    spark = (
        SparkSession.builder
        .appName("Commerce-Order-State-Reconstruction")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1. Lire les evenements Kafka et les valider
        validated = build_validated_stream(spark)

        # 2. Reutiliser la correlation de l'etape 3
        correlated = build_correlated_stream(validated)

        # 3. Appliquer les regles metier
        order_states = reconstruct_states(correlated)

        print("\n=== SCHEMA ORDER STATES ===", flush=True)
        order_states.printSchema()

        print(
            "\n=== DEMARRAGE RECONSTRUCTION DES ETATS ===",
            flush=True,
        )

        # 4. Executer le traitement streaming
        query = (
            order_states.writeStream
            .foreachBatch(inspect_batch)
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT)
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()

        if query.exception() is not None:
            raise RuntimeError(str(query.exception()))

        print(
            "\n[OK] Reconstruction des etats terminee.",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
