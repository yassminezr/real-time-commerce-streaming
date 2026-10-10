
import hashlib
import json

from cassandra.cluster import Cluster

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)


# =====================================================
# CONFIGURATION
# =====================================================

KAFKA = "kafka:19092"
SOURCE_TOPIC = "order-states"

CASSANDRA_HOST = "cassandra"
KEYSPACE = "commerce"

CHECKPOINT = (
    "/opt/spark/checkpoints/integrated-cassandra-v1"
)


# =====================================================
# SCHEMA DES ETATS METIER
# =====================================================

state_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_amount", DoubleType()),
    StructField("order_time", StringType()),
    StructField("payment_status", StringType()),
    StructField("inventory_status", StringType()),
    StructField("order_status", StringType()),
])


# =====================================================
# IDENTIFIANT DETERMINISTE
# =====================================================

def make_state_id(row):
    """
    Calcule un identifiant stable pour chaque etat metier.

    Si le meme resultat est traite plusieurs fois,
    il conserve le meme state_id dans Cassandra.
    """

    fields = {
        "order_id": row["order_id"],
        "customer_id": row["customer_id"],
        "order_amount": row["order_amount"],
        "order_time": row["order_time"],
        "payment_status": row["payment_status"],
        "inventory_status": row["inventory_status"],
        "order_status": row["order_status"],
    }

    canonical = json.dumps(
        fields,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )

    return hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()


# =====================================================
# LECTURE STREAMING KAFKA
# =====================================================

def build_stream(spark):
    """
    Lit le topic Kafka order-states.

    Parse le JSON et convertit order_time en timestamp
    Spark avant l'ecriture Cassandra.
    """

    source = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 200)
        .load()
    )

    events = (
        source
        .select(
            from_json(
                col("value").cast("string"),
                state_schema,
            ).alias("state")
        )
        .select("state.*")

        # Conversion realisee par Spark
        .withColumn(
            "order_time_parsed",
            to_timestamp(col("order_time"))
        )

        # Ne conserver que les etats exploitables
        .filter(
            col("order_id").isNotNull()
            & col("order_time_parsed").isNotNull()
            & col("order_status").isNotNull()
        )
    )

    return events


# =====================================================
# ECRITURE CASSANDRA
# =====================================================

def write_batch(batch_df, batch_id):
    """
    Ecrit les etats metier dans Cassandra.

    Un batch rejoue utilise les memes cles deterministes,
    evitant de dupliquer les lignes identiques.
    """

    print(
        f"\n=== CASSANDRA | BATCH {batch_id} ===",
        flush=True,
    )

    cluster = None
    written = 0

    try:
        cluster = Cluster(
            [CASSANDRA_HOST],
            port=9042,
            connect_timeout=15,
        )

        session = cluster.connect(KEYSPACE)

        insert = session.prepare("""
            INSERT INTO order_state_history (
                order_id,
                state_id,
                customer_id,
                order_amount,
                order_time,
                payment_status,
                inventory_status,
                order_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)

        # toLocalIterator evite de collecter tout
        # le micro-batch en memoire.
        for row in batch_df.toLocalIterator():

            # Timestamp deja parse par Spark
            order_time = row["order_time_parsed"]

            # Identifiant stable du resultat metier
            state_id = make_state_id(row)

            session.execute(
                insert,
                (
                    row["order_id"],
                    state_id,
                    row["customer_id"],
                    row["order_amount"],
                    order_time,
                    row["payment_status"],
                    row["inventory_status"],
                    row["order_status"],
                ),
            )

            written += 1

        print(
            f"[COUNT] cassandra_writes={written}",
            flush=True,
        )

        print(
            f"[OK] Batch {batch_id} ecrit dans Cassandra",
            flush=True,
        )

    finally:
        if cluster is not None:
            cluster.shutdown()


# =====================================================
# EXECUTION DU JOB
# =====================================================

def main():

    spark = (
        SparkSession.builder
        .appName("Commerce-Cassandra-Sink")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        stream = build_stream(spark)

        print(
            "\n=== JOB 4 : KAFKA TO CASSANDRA ===",
            flush=True,
        )

        query = (
            stream.writeStream
            .foreachBatch(write_batch)
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
            "\n[OK] JOB 4 TERMINE",
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
