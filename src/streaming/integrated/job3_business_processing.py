
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, from_json, to_timestamp,
    lit, struct, to_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType
)

# Reutiliser la reconstruction validee en Phase 5
STREAMING_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(STREAMING_DIR))

from reconstruct_order_state import reconstruct_states


KAFKA = "kafka:19092"
SOURCE_TOPIC = "unique-events"
TARGET_TOPIC = "order-states"

CHECKPOINT = "/opt/spark/checkpoints/integrated-business-v1"


# Schema du JSON publie par nos producteurs
payload_schema = StructType([
    StructField("total_amount", DoubleType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("payment_id", StringType()),
    StructField("sku", StringType()),
    StructField("quantity", IntegerType()),
    StructField("reason", StringType()),
])

event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("schema_version", IntegerType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("payload", payload_schema),
])


def build_business_stream(spark):
    # 1. Lire la sortie du Job 2
    source = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50)
        .load()
    )

    # 2. Reconstituer les colonnes metier
    events = (
        source
        .select(
            from_json(
                col("value").cast("string"),
                event_schema
            ).alias("e")
        )
        .select("e.*")
        .withColumn(
            "event_timestamp",
            to_timestamp(col("event_time"))
        )
        .filter(
            col("event_id").isNotNull()
            & col("order_id").isNotNull()
            & col("event_timestamp").isNotNull()
        )
    )

    # Aucun filtre sur ORD-P5-001/002/003 :
    # toutes les commandes sont concernees.

    orders = (
        events
        .filter(col("event_type") == "order_created")
        .select(
            "order_id",
            "customer_id",
            col("payload.total_amount").alias("total_amount"),
            col("event_timestamp").alias("order_time")
        )
        .withWatermark("order_time", "10 minutes")
        .alias("o")
    )

    payments = (
        events
        .filter(
            col("event_type").isin(
                "payment_authorized",
                "payment_failed"
            )
        )
        .select(
            "order_id",
            "event_type",
            col("payload.payment_id").alias("payment_id"),
            col("event_timestamp").alias("payment_time")
        )
        .withWatermark("payment_time", "10 minutes")
        .alias("p")
    )

    inventory = (
        events
        .filter(
            col("event_type").isin(
                "inventory_reserved",
                "inventory_rejected"
            )
        )
        .select(
            "order_id",
            "event_type",
            col("event_timestamp").alias("inventory_time")
        )
        .withWatermark("inventory_time", "10 minutes")
        .alias("i")
    )

    # 3. Jointure Orders / Payments
    order_payment_condition = expr("""
        o.order_id = p.order_id
        AND p.payment_time >= o.order_time
        AND p.payment_time <=
            o.order_time + INTERVAL 15 MINUTES
    """)

    order_payments = (
        orders
        .join(payments, order_payment_condition, "inner")
        .select(
            col("o.order_id").alias("order_id"),
            col("o.customer_id").alias("customer_id"),
            col("o.total_amount").alias("order_amount"),

            # Evite deux colonnes watermarkees
            # dans la deuxieme jointure.
            col("o.order_time")
            .cast("string").alias("order_time"),

            col("p.payment_id").alias("payment_id"),
            col("p.event_type").alias("payment_status"),
            col("p.payment_time").alias("payment_time")
        )
    )

    # 4. Paiements refuses : pas besoin d'Inventory
    failed = (
        order_payments
        .filter(col("payment_status") == "payment_failed")
        .select(
            "order_id", "customer_id", "order_amount",
            "order_time", "payment_id",
            "payment_status", "payment_time",
            lit(None).cast("string").alias("inventory_status")
        )
    )

    # 5. Paiements autorises : jointure Inventory
    authorized = (
        order_payments
        .filter(col("payment_status") == "payment_authorized")
        .alias("op")
    )

    inventory_condition = expr("""
        op.order_id = i.order_id
        AND i.inventory_time >= op.payment_time
        AND i.inventory_time <=
            op.payment_time + INTERVAL 15 MINUTES
    """)

    authorized_inventory = (
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
            col("i.event_type").alias("inventory_status")
        )
    )

    correlated = failed.unionByName(authorized_inventory)

    # 6. Reutiliser nos regles metier existantes
    return reconstruct_states(correlated)


def publish_batch(batch_df, batch_id):
    batch_df.persist()

    try:
        count = batch_df.count()

        print(
            f"\n=== BUSINESS | BATCH {batch_id} ===",
            flush=True
        )
        print(
            f"[COUNT] order_states={count}",
            flush=True
        )

        if count == 0:
            return

        batch_df.groupBy("order_status").count().show(
            truncate=False
        )

        output = batch_df.select(
            col("order_id").alias("key"),
            to_json(
                struct(*[col(c) for c in batch_df.columns])
            ).alias("value")
        )

        (
            output.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA)
            .option("topic", TARGET_TOPIC)
            .save()
        )

        print(
            f"[OK] {count} etats publies dans {TARGET_TOPIC}",
            flush=True
        )

    finally:
        batch_df.unpersist()


def main():
    spark = (
        SparkSession.builder
        .appName("Commerce-Integrated-Business")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        states = build_business_stream(spark)

        print("\n=== JOB 3 : BUSINESS PROCESSING ===",
              flush=True)

        query = (
            states.writeStream
            .foreachBatch(publish_batch)
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT)
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()

        if query.exception() is not None:
            raise RuntimeError(str(query.exception()))

        print("\n[OK] JOB 3 TERMINE", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
