import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    bootstrap_servers = os.getenv(
        "STREAMGUARD_KAFKA_BOOTSTRAP",
        "kafka:29092",
    )
    topic = os.getenv(
        "STREAMGUARD_KAFKA_TOPIC",
        "streamguard.public.transactions",
    )
    table_name = topic.rsplit(".", 1)[-1]

    if table_name not in {"transactions", "customers", "merchants"}:
        raise ValueError(f"Unsupported Kafka topic: {topic}")

    bronze_path = f"/opt/streamguard/lake/bronze/{table_name}"
    checkpoint_path = f"/opt/streamguard/lake/checkpoints/bronze_{table_name}"

    spark = SparkSession.builder.appName("StreamGuardBronzeCDC").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
    )

    bronze_records = kafka_stream.select(
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("timestampType").alias("kafka_timestamp_type"),
        F.col("key").alias("message_key"),
        F.col("value").alias("raw_value"),
        F.current_timestamp().alias("ingested_at"),
    )

    query = (
        bronze_records.writeStream.format("parquet")
        .outputMode("append")
        .option("path", bronze_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="5 seconds")
        .start()
    )

    print(f"Bronze streaming started for topic: {topic}")
    print(f"Bronze output: {bronze_path}")
    print(f"Checkpoint: {checkpoint_path}")

    try:
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
