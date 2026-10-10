
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

from validate_events import build_validated_stream


CHECKPOINT = "/opt/spark/checkpoints/phase5-prepare-v1"

TEST_ORDER_IDS = [
    "ORD-P5-001",
    "ORD-P5-002",
    "ORD-P5-003",
]


def prepare_streams(validated):
    """Construit les trois DataFrames streaming métier."""

    base = (
        validated
        .filter(col("validation_status") == "VALID")
        .filter(col("order_id").isin(TEST_ORDER_IDS))
        .withColumn(
            "event_timestamp",
            to_timestamp(col("event_time")),
        )
        .filter(col("event_timestamp").isNotNull())
    )

    orders = (
        base
        .filter(
            (col("topic") == "orders")
            & (col("event_type") == "order_created")
        )
        .select(
            "event_id",
            "order_id",
            "customer_id",
            "event_timestamp",
            col("payload.total_amount").alias("total_amount"),
            col("payload.currency").alias("currency"),
        )
    )

    payments = (
        base
        .filter(col("topic") == "payments")
        .select(
            "event_id",
            "order_id",
            "customer_id",
            "event_timestamp",
            "event_type",
            col("payload.payment_id").alias("payment_id"),
            col("payload.amount").alias("amount"),
            col("payload.currency").alias("currency"),
        )
    )

    inventory = (
        base
        .filter(col("topic") == "inventory-events")
        .select(
            "event_id",
            "order_id",
            "customer_id",
            "event_timestamp",
            "event_type",
            col("payload.sku").alias("sku"),
            col("payload.quantity").alias("quantity"),
            col("payload.reason").alias("reason"),
        )
    )

    return orders, payments, inventory


def inspect_batch(batch_df, batch_id):
    """Affiche les trois catégories sans effectuer de jointure."""

    batch_df.persist()

    try:
        orders = batch_df.filter(
            col("topic") == "orders"
        )
        payments = batch_df.filter(
            col("topic") == "payments"
        )
        inventory = batch_df.filter(
            col("topic") == "inventory-events"
        )

        print(
            f"\n=== PHASE 5 — BATCH {batch_id} ===",
            flush=True,
        )

        print("\nORDERS", flush=True)
        orders.select(
            "order_id",
            "event_type",
            "event_timestamp",
            col("payload.total_amount").alias("total_amount"),
        ).show(truncate=False)

        print("\nPAYMENTS", flush=True)
        payments.select(
            "order_id",
            "event_type",
            "event_timestamp",
            col("payload.amount").alias("amount"),
        ).show(truncate=False)

        print("\nINVENTORY", flush=True)
        inventory.select(
            "order_id",
            "event_type",
            "event_timestamp",
            col("payload.sku").alias("sku"),
        ).show(truncate=False)

        print(
            "[BATCH COUNTS] "
            f"orders={orders.count()} "
            f"payments={payments.count()} "
            f"inventory={inventory.count()}",
            flush=True,
        )

    finally:
        batch_df.unpersist()


def main():
    spark = (
        SparkSession.builder
        .appName("Commerce-Prepare-Business-Streams")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        validated = build_validated_stream(spark)

        orders, payments, inventory = prepare_streams(validated)

        print("\n=== SCHEMAS METIER ===", flush=True)

        print("\nOrders:")
        orders.printSchema()

        print("\nPayments:")
        payments.printSchema()

        print("\nInventory:")
        inventory.printSchema()

        # Une seule requete streaming pour le test :
        # les flux separes seront utilises pour les joins
        # aux prochaines etapes.
        selected = (
            validated
            .filter(col("validation_status") == "VALID")
            .filter(col("order_id").isin(TEST_ORDER_IDS))
            .withColumn(
                "event_timestamp",
                to_timestamp(col("event_time")),
            )
        )

        query = (
            selected.writeStream
            .foreachBatch(inspect_batch)
            .option("checkpointLocation", CHECKPOINT)
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()

        if query.exception() is not None:
            raise RuntimeError(str(query.exception()))

        print(
            "\n[OK] Preparation des flux metier terminee.",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
