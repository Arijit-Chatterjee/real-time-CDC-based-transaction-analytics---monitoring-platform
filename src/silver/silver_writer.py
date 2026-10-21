"""Maintain current-state Silver Delta tables from StreamGuard Bronze CDC files."""

from __future__ import annotations

import os
from collections.abc import Callable
from decimal import Decimal

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

from src.streaming.decoder import decode_amount

BRONZE_ROOT = "/opt/streamguard/lake/bronze"
SILVER_ROOT = "/opt/streamguard/lake/silver"
CHECKPOINT_ROOT = "/opt/streamguard/lake/checkpoints"
TABLES = {
    "transactions": "transaction_id",
    "customers": "customer_id",
    "merchants": "merchant_id",
}


def _decode_amount_or_none(value: str | None) -> Decimal | None:
    """Keep delete events with null amounts from reaching the decimal decoder."""
    if value is None:
        return None
    return decode_amount(value)


_decode_amount_udf = F.udf(_decode_amount_or_none, DecimalType(12, 2))


def _json_field(section: str, field: str):
    return F.get_json_object(F.col("_raw_json"), f"$.payload.{section}.{field}")


def build_events(bronze_batch: DataFrame, table_name: str) -> DataFrame:
    """Parse one Bronze microbatch into typed entity rows plus CDC lineage."""
    primary_key = TABLES[table_name]
    raw = (
        bronze_batch.filter(F.col("raw_value").isNotNull())
        .withColumn("_raw_json", F.col("raw_value").cast("string"))
        .withColumn("_operation", F.get_json_object(F.col("_raw_json"), "$.payload.op"))
    )

    key_expression = F.coalesce(
        _json_field("after", primary_key),
        _json_field("before", primary_key),
    )

    if table_name == "transactions":
        entity_columns = [
            _decode_amount_udf(_json_field("after", "amount")).alias("amount"),
            _json_field("after", "customer_id").cast("long").alias("customer_id"),
            _json_field("after", "merchant_id").cast("long").alias("merchant_id"),
            _json_field("after", "currency").alias("currency"),
            F.to_timestamp(_json_field("after", "event_time")).alias("event_time"),
            F.to_timestamp(_json_field("after", "created_at")).alias("created_at"),
            F.to_timestamp(_json_field("after", "updated_at")).alias("updated_at"),
            _json_field("after", "region").alias("region"),
            _json_field("after", "status").alias("status"),
        ]
    elif table_name == "customers":
        entity_columns = [
            F.to_timestamp(_json_field("after", "created_at")).alias("created_at"),
            _json_field("after", "home_region").alias("home_region"),
            _json_field("after", "account_status").alias("account_status"),
        ]
    else:
        entity_columns = [
            F.to_timestamp(_json_field("after", "created_at")).alias("created_at"),
            _json_field("after", "merchant_name").alias("merchant_name"),
            _json_field("after", "merchant_category").alias("merchant_category"),
            _json_field("after", "merchant_region").alias("merchant_region"),
        ]

    events = raw.select(
        key_expression.cast("string" if table_name == "transactions" else "long").alias(
            primary_key
        ),
        *entity_columns,
        F.col("_operation").alias("operation"),
        (F.col("_operation") == F.lit("d")).alias("is_deleted"),
        _json_field("source", "snapshot").alias("snapshot_type"),
        _json_field("source", "ts_ms").cast("long").alias("source_ts_ms"),
        _json_field("source", "lsn").cast("long").alias("source_lsn"),
        F.col("topic").alias("kafka_topic"),
        F.col("kafka_partition").cast("int").alias("kafka_partition"),
        F.col("kafka_offset").cast("long").alias("kafka_offset"),
        F.col("kafka_timestamp").alias("kafka_timestamp"),
        F.col("kafka_timestamp_type").alias("kafka_timestamp_type"),
        F.col("ingested_at").alias("ingested_at"),
    )
    return events.filter(
        F.col(primary_key).isNotNull() & F.col("operation").isin("r", "c", "u", "d")
    )


def ensure_delta_table(spark: SparkSession, bronze_schema, table_name: str) -> str:
    """Create an empty Delta target once; refuse to overwrite a non-Delta path."""
    target_path = f"{SILVER_ROOT}/{table_name}"
    if DeltaTable.isDeltaTable(spark, target_path):
        return target_path
    if os.path.exists(target_path):
        raise RuntimeError(f"Silver path exists but is not a Delta table: {target_path}")

    empty_bronze = spark.createDataFrame([], bronze_schema)
    empty_events = build_events(empty_bronze, table_name)
    empty_events.write.format("delta").mode("errorifexists").save(target_path)
    return target_path


def make_batch_handler(
    spark: SparkSession, table_name: str, primary_key: str, target_path: str
) -> Callable[[DataFrame, int], None]:
    """Return an idempotent foreachBatch handler that upserts each entity's latest event."""

    def handle_batch(batch_df: DataFrame, batch_id: int) -> None:
        events = build_events(batch_df, table_name)
        order = Window.partitionBy(primary_key).orderBy(
            F.col("kafka_partition").desc(), F.col("kafka_offset").desc()
        )
        latest = (
            events.withColumn("_row_number", F.row_number().over(order))
            .filter(F.col("_row_number") == 1)
            .drop("_row_number")
        )

        target = DeltaTable.forPath(spark, target_path)
        target.alias("t").merge(
            latest.alias("s"), f"t.{primary_key} = s.{primary_key}"
        ).whenMatchedUpdateAll(
            condition=("s.kafka_partition = t.kafka_partition AND s.kafka_offset > t.kafka_offset")
        ).whenNotMatchedInsertAll().execute()

        print(f"Silver {table_name}: processed microbatch {batch_id}", flush=True)

    return handle_batch


def main() -> None:
    spark = (
        SparkSession.builder.appName("StreamGuardSilverCDC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    queries = []
    try:
        for table_name, primary_key in TABLES.items():
            bronze_path = f"{BRONZE_ROOT}/{table_name}"
            checkpoint_path = f"{CHECKPOINT_ROOT}/silver_{table_name}"
            if not os.path.isdir(bronze_path):
                raise FileNotFoundError(f"Bronze input path does not exist: {bronze_path}")

            bronze_schema = spark.read.parquet(bronze_path).schema
            target_path = ensure_delta_table(spark, bronze_schema, table_name)
            bronze_stream = (
                spark.readStream.schema(bronze_schema)
                .option("maxFilesPerTrigger", 20)
                .parquet(bronze_path)
            )
            query = (
                bronze_stream.writeStream.queryName(f"silver_{table_name}")
                .foreachBatch(make_batch_handler(spark, table_name, primary_key, target_path))
                .option("checkpointLocation", checkpoint_path)
                .trigger(processingTime="5 seconds")
                .start()
            )
            queries.append(query)
            print(f"Silver stream started: {table_name}", flush=True)
            print(f"  Input:      {bronze_path}", flush=True)
            print(f"  Delta:      {target_path}", flush=True)
            print(f"  Checkpoint: {checkpoint_path}", flush=True)

        spark.streams.awaitAnyTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
