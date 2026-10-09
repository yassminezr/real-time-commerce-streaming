
from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("Commerce-Kafka-Connection-Test")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n=== CONNEXION SPARK - KAFKA ===")

        print(f"Spark version : {spark.version}")
        print(f"Spark Master  : {spark.sparkContext.master}")

        # Lecture bornee des messages deja stockes dans Kafka
        kafka_df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:19092")
            .option("subscribe", "orders")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        # Conversion des bytes Kafka en texte lisible
        events_df = kafka_df.selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "topic",
            "partition",
            "offset",
            "CAST(value AS STRING) AS event_json",
        )

        print("\n=== SCHEMA DU DATAFRAME ===")
        events_df.printSchema()

        print("\n=== EXEMPLES D'EVENEMENTS KAFKA ===")
        events_df.show(5, truncate=70)

        print("\n[OK] Spark a lu les evenements Kafka.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
