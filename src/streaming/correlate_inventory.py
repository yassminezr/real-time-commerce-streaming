
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit

from validate_events import build_validated_stream
from prepare_business_streams import prepare_streams


CHECKPOINT = "/opt/spark/checkpoints/phase5-inventory-v3"

def build_correlated_stream(validated):
    # 1. Preparer les 3 flux metier
    orders, payments, inventory = prepare_streams(validated)

    orders = (
        orders
        .withColumnRenamed("event_timestamp", "order_time")
        .withWatermark("order_time", "10 minutes")
        .alias("o")
    )

    payments = (
        payments
        .withColumnRenamed("event_timestamp", "payment_time")
        .withWatermark("payment_time", "10 minutes")
        .alias("p")
    )

    inventory = (
        inventory
        .withColumnRenamed("event_timestamp", "inventory_time")
        .withWatermark("inventory_time", "10 minutes")
        .alias("i")
    )

    # 2. Premiere jointure streaming : Orders / Payments
    order_payment_condition = expr("""
        o.order_id = p.order_id
        AND p.payment_time >= o.order_time
        AND p.payment_time <= o.order_time + INTERVAL 15 MINUTES
    """)

    order_payments = (
        orders
        .join(payments, order_payment_condition, "inner")
        .select(
            col("o.order_id").alias("order_id"),
            col("o.customer_id").alias("customer_id"),
            col("o.total_amount").alias("order_amount"),
            col("o.order_time").cast("string").alias("order_time"),
            col("p.payment_id").alias("payment_id"),
            col("p.event_type").alias("payment_status"),
            col("p.payment_time").alias("payment_time"),
        )
    )

    # 3. Branche paiements refuses :
    # pas besoin d'attendre un evenement Inventory
    failed_payments = (
        order_payments
        .filter(col("payment_status") == "payment_failed")
        .select(
            "order_id",
            "customer_id",
            "order_amount",
            "order_time",
            "payment_id",
            "payment_status",
            "payment_time",
            lit(None).cast("string").alias("inventory_status"),
            lit(None).cast("string").alias("sku"),
            lit(None).cast("string").alias("inventory_reason"),
            lit(None).cast("timestamp").alias("inventory_time"),
        )
    )

    # 4. Branche paiements autorises
    authorized = (
        order_payments
        .filter(col("payment_status") == "payment_authorized")
        .alias("op")
    )

    # Deuxieme jointure streaming : Payments / Inventory
    inventory_condition = expr("""
        op.order_id = i.order_id
        AND i.inventory_time >= op.payment_time
        AND i.inventory_time <=
            op.payment_time + INTERVAL 15 MINUTES
    """)

    authorized_with_inventory = (
        authorized
        .join(inventory, inventory_condition, "inner")
        .select(
            col("op.order_id").alias("order_id"),
            col("op.customer_id").alias("customer_id"),
            col("op.order_amount").alias("order_amount"),
            col("op.order_time").alias("order_time"),
            col("op.payment_id").alias("payment_id"),
            col("op.payment_status").alias("payment_status"),
            col("op.payment_time").alias("payment_time"),
            col("i.event_type").alias("inventory_status"),
            col("i.sku").alias("sku"),
            col("i.reason").alias("inventory_reason"),
            col("i.inventory_time").alias("inventory_time"),
        )
    )

    # 5. Reunir les deux branches
    correlated = failed_payments.unionByName(
        authorized_with_inventory
    )

    return correlated


def inspect_batch(batch_df, batch_id):
    batch_df.persist()

    try:
        print(
            f"\n=== INVENTORY CORRELATION | BATCH {batch_id} ===",
            flush=True,
        )

        batch_df.orderBy("order_id").show(
            50, truncate=False
        )

        total = batch_df.count()
        print(
            f"[CORRELATION COUNT] batch={batch_id} "
            f"rows={total}",
            flush=True,
        )

        for order_id in [
            "ORD-P5-001",
            "ORD-P5-002",
            "ORD-P5-003",
        ]:
            matches = (
                batch_df
                .filter(col("order_id") == order_id)
                .count()
            )

            print(
                f"[TEST] {order_id}: {matches} row(s)",
                flush=True,
            )

    finally:
        batch_df.unpersist()


def main():
    spark = (
        SparkSession.builder
        .appName("Commerce-Inventory-Correlation")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        validated = build_validated_stream(spark)

        correlated = build_correlated_stream(validated)

        print("\n=== SCHEMA DU RESULTAT ===", flush=True)
        correlated.printSchema()

        print(
            "\n=== DEMARRAGE CORRELATION INVENTORY ===",
            flush=True,
        )

        query = (
            correlated.writeStream
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
            "\n[OK] Correlation Inventory terminee.",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
