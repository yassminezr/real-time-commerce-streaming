
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

from validate_events import build_validated_stream
from prepare_business_streams import prepare_streams


CHECKPOINT = "/opt/spark/checkpoints/phase5-orders-payments-v1"


def inspect_batch(batch_df, batch_id):
    """Affiche les correspondances du micro-batch."""

    batch_df.persist()

    try:
        print(
            f"\n=== ORDERS-PAYMENTS JOIN | BATCH {batch_id} ===",
            flush=True,
        )

        batch_df.orderBy("order_id").show(
            100,
            truncate=False,
        )

        total = batch_df.count()

        print(
            f"[JOIN COUNT] batch={batch_id} "
            f"matches={total}",
            flush=True,
        )

        for order_id in [
            "ORD-P5-001",
            "ORD-P5-002",
            "ORD-P5-003",
        ]:
            count = batch_df.filter(
                col("order_id") == order_id
            ).count()

            print(
                f"[TEST] {order_id}: {count} match(es)",
                flush=True,
            )

    finally:
        batch_df.unpersist()


def main():
    spark = (
        SparkSession.builder
        .appName("Commerce-Orders-Payments-Join")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1. Lire et valider les evenements Kafka
        validated = build_validated_stream(spark)

        # 2. Reutiliser les flux metier de l'etape 1
        orders, payments, _ = prepare_streams(validated)

        # 3. Preparer les colonnes temporelles
        orders = (
            orders
            .withColumnRenamed(
                "event_timestamp",
                "order_time",
            )
            .withWatermark("order_time", "10 minutes")
            .alias("o")
        )

        payments = (
            payments
            .withColumnRenamed(
                "event_timestamp",
                "payment_time",
            )
            .withWatermark("payment_time", "10 minutes")
            .alias("p")
        )

        # 4. Jointure avec deux conditions :
        # - meme identifiant de commande
        # - paiement dans les 15 minutes suivantes
        condition = expr(
            """
            o.order_id = p.order_id
            AND p.payment_time >= o.order_time
            AND p.payment_time <=
                o.order_time + INTERVAL 15 MINUTES
            """
        )

        joined = orders.join(
            payments,
            on=condition,
            how="inner",
        )

        # 5. Construire un resultat metier lisible
        results = joined.select(
            col("o.order_id").alias("order_id"),
            col("o.customer_id").alias("customer_id"),
            col("o.total_amount").alias("order_amount"),
            col("o.currency").alias("order_currency"),
            col("o.order_time").alias("order_time"),
            col("p.payment_id").alias("payment_id"),
            col("p.event_type").alias("payment_status"),
            col("p.amount").alias("payment_amount"),
            col("p.payment_time").alias("payment_time"),
        )

        print(
            "\n=== SCHEMA DU RESULTAT DE JOIN ===",
            flush=True,
        )
        results.printSchema()

        print(
            "\n=== DEMARRAGE STREAM-STREAM JOIN ===",
            flush=True,
        )
        print("Join key: order_id", flush=True)
        print("Time condition: 0 to 15 minutes", flush=True)
        print("Watermark: 10 minutes per stream", flush=True)

        # 6. Lancer le traitement streaming
        query = (
            results.writeStream
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
            "\n[OK] Orders-Payments stream-stream join termine.",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
