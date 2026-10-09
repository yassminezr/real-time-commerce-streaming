
from pyspark.sql import SparkSession


KAFKA_SERVERS = "kafka:19092"

TOPICS = "orders,payments,inventory-events"

CHECKPOINT_PATH = "/tmp/commerce-streaming-checkpoint-v1"


def main():
    spark = (
        SparkSession.builder
        .appName("Commerce-Kafka-Streaming")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n=== DEMARRAGE DU STREAMING ===", flush=True)

        # 1. Lecture continue depuis les trois topics Kafka
        kafka_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVERS)
            .option("subscribe", TOPICS)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 50)
            .load()
        )

        # 2. Convertir la Kafka Key et la Value en texte
        events_stream = kafka_stream.selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "topic",
            "partition",
            "offset",
            "CAST(value AS STRING) AS event_json",
        )

        print("\nTopics : " + TOPICS, flush=True)
        print("Streaming actif - attente des evenements...", flush=True)

        # 3. Afficher les nouveaux micro-batches dans la console
        query = (
            events_stream.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .option("numRows", 50)
            .option("checkpointLocation", CHECKPOINT_PATH)
            .trigger(processingTime="5 seconds")
            .start()
        )

        query.awaitTermination()

    except KeyboardInterrupt:
        print("\nArret demande par l'utilisateur.", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
