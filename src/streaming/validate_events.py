
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


KAFKA_SERVERS = "kafka:19092"
TOPICS = "orders,payments,inventory-events"
CHECKPOINT = "/tmp/commerce-validation-checkpoint-v1"


# 1. Schéma du payload métier
payload_schema = StructType([
    StructField("total_amount", DoubleType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("payment_id", StringType()),
    StructField("sku", StringType()),
    StructField("quantity", IntegerType()),
    StructField("reason", StringType()),
])


# 2. Schéma de l'enveloppe commune
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("schema_version", IntegerType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("payload", payload_schema),

    # En cas de JSON mal formé, conserver le texte corrompu.
    StructField("_corrupt_record", StringType()),
])


def build_validated_stream(spark):
    # Lecture streaming des trois topics
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPICS)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50)
        .load()
    )

    # Décoder et parser le JSON
    parsed = (
        raw.selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "topic",
            "partition",
            "offset",
            "CAST(value AS STRING) AS raw_json",
        )
        .withColumn(
            "event",
            from_json(
                col("raw_json"),
                event_schema,
                {
                    "mode": "PERMISSIVE",
                    "columnNameOfCorruptRecord": "_corrupt_record",
                },
            ),
        )
    )

    # Extraire les champs du JSON
    events = parsed.select(
        "kafka_key",
        "topic",
        "partition",
        "offset",
        "raw_json",
        col("event._corrupt_record").alias("corrupt_record"),
        col("event.event_id").alias("event_id"),
        col("event.event_type").alias("event_type"),
        col("event.event_time").alias("event_time"),
        col("event.schema_version").alias("schema_version"),
        col("event.order_id").alias("order_id"),
        col("event.customer_id").alias("customer_id"),
        col("event.payload").alias("payload"),
    )

    # 3. Contrat métier : un type doit appartenir au bon topic
    allowed_topic_type = (
        (
            (col("topic") == "orders")
            & (col("event_type") == "order_created")
        )
        | (
            (col("topic") == "payments")
            & col("event_type").isin(
                "payment_authorized", "payment_failed"
            )
        )
        | (
            (col("topic") == "inventory-events")
            & col("event_type").isin(
                "inventory_reserved", "inventory_rejected"
            )
        )
    )

    # 4. Règles de validation, par ordre de priorité
    validation_error = (
        when(
            col("corrupt_record").isNotNull(),
            lit("INVALID_JSON"),
        )
        .when(
            col("event_id").isNull()
            | col("event_type").isNull()
            | col("event_time").isNull()
            | col("order_id").isNull()
            | col("customer_id").isNull()
            | col("payload").isNull(),
            lit("MISSING_REQUIRED_FIELD"),
        )
        .when(
            col("kafka_key").isNull()
            | (col("kafka_key") != col("order_id")),
            lit("INVALID_KAFKA_KEY"),
        )
        .when(
            col("schema_version").isNull()
            | (col("schema_version") != 1),
            lit("UNSUPPORTED_SCHEMA_VERSION"),
        )
        .when(
            ~allowed_topic_type,
            lit("INVALID_TOPIC_EVENT_TYPE"),
        )
        .when(
            (col("event_type") == "order_created")
            & (
                col("payload.total_amount").isNull()
                | (col("payload.total_amount") <= 0)
                | col("payload.currency").isNull()
            ),
            lit("INVALID_ORDER_PAYLOAD"),
        )
        .when(
            col("event_type").isin(
                "payment_authorized", "payment_failed"
            )
            & (
                col("payload.payment_id").isNull()
                | col("payload.amount").isNull()
                | (col("payload.amount") <= 0)
                | col("payload.currency").isNull()
            ),
            lit("INVALID_PAYMENT_PAYLOAD"),
        )
        .when(
            col("event_type").isin(
                "inventory_reserved", "inventory_rejected"
            )
            & (
                col("payload.sku").isNull()
                | col("payload.quantity").isNull()
                | (col("payload.quantity") <= 0)
            ),
            lit("INVALID_INVENTORY_PAYLOAD"),
        )
        .when(
            (col("event_type") == "payment_failed")
            & col("payload.reason").isNull(),
            lit("MISSING_PAYMENT_FAILURE_REASON"),
        )
        .when(
            (col("event_type") == "inventory_rejected")
            & col("payload.reason").isNull(),
            lit("MISSING_INVENTORY_REJECTION_REASON"),
        )
    )

    validated = (
        events.withColumn("validation_error", validation_error)
        .withColumn(
            "validation_status",
            when(
                col("validation_error").isNull(),
                lit("VALID"),
            ).otherwise(lit("INVALID")),
        )
    )

    return validated


def main():
    spark = (
        SparkSession.builder
        .appName("Commerce-JSON-Validation")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        validated = build_validated_stream(spark)

        # Affichage des colonnes nécessaires aux tests
        results = validated.select(
            "kafka_key",
            "topic",
            "partition",
            "offset",
            "event_type",
            "order_id",
            "validation_status",
            "validation_error",
        )

        print("\n=== VALIDATION DES EVENEMENTS ===", flush=True)

        query = (
            results.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .option("numRows", 100)
            .option("checkpointLocation", CHECKPOINT)
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()

        print("\n[OK] Test de validation termine.", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
