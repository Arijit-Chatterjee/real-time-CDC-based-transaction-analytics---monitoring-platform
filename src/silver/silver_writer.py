"""Maintain validated current-state Silver Delta tables from Bronze CDC files."""

from __future__ import annotations

import binascii
import os
from collections.abc import Callable
from decimal import Decimal

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

from src.streaming.decoder import decode_amount

BRONZE_ROOT = "/opt/streamguard/lake/bronze"
SILVER_ROOT = "/opt/streamguard/lake/silver"
QUARANTINE_ROOT = "/opt/streamguard/lake/quarantine/silver"
CHECKPOINT_ROOT = "/opt/streamguard/lake/checkpoints"

TABLES = {
    "transactions": "transaction_id",
    "customers": "customer_id",
    "merchants": "merchant_id",
}

VALID_OPERATIONS = ("r", "c", "u", "d")

# Validate fields needed by downstream Gold models while the record is
# in its incoming microbatch, not by repeatedly scanning Silver tables.
REQUIRED_ACTIVE_FIELDS = {
    "transactions": (
        "amount",
        "customer_id",
        "merchant_id",
        "currency",
        "event_time",
        "created_at",
        "updated_at",
        "region",
        "status",
    ),
    "customers": (
        "created_at",
        "account_status",
    ),
    "merchants": (
        "created_at",
        "merchant_name",
        "merchant_category",
    ),
}

NON_BLANK_ACTIVE_FIELDS = {
    "transactions": ("currency", "region", "status"),
    "customers": ("account_status",),
    "merchants": ("merchant_name", "merchant_category"),
}


def _decode_amount_or_none(value: str | None) -> Decimal | None:
    """Return None for missing or malformed encoded amounts."""
    if not value:
        return None

    try:
        return decode_amount(value)
    except binascii.Error:
        return None


_decode_amount_udf = F.udf(_decode_amount_or_none, DecimalType(12, 2))


def _json_field(section: str, field: str):
    return F.get_json_object(F.col("_raw_json"), f"$.payload.{section}.{field}")


def _add_validation_errors(events: DataFrame, table_name: str) -> DataFrame:
    """Attach rejection reasons to invalid events in the current microbatch."""
    primary_key = TABLES[table_name]
    operation = F.col("operation")
    is_deleted = F.col("is_deleted")
    active = ~F.coalesce(is_deleted, F.lit(False))

    checks = [
        (F.col(primary_key).isNull(), "missing primary key"),
        (
            operation.isNull() | ~operation.isin(*VALID_OPERATIONS),
            "invalid CDC operation",
        ),
        (
            is_deleted.isNull() | (is_deleted != (operation == F.lit("d"))),
            "CDC operation and delete flag disagree",
        ),
        (
            F.col("kafka_topic").isNull() | (F.length(F.trim(F.col("kafka_topic"))) == 0),
            "missing Kafka topic",
        ),
        (
            F.col("kafka_partition").isNull() | (F.col("kafka_partition") < 0),
            "invalid Kafka partition",
        ),
        (
            F.col("kafka_offset").isNull() | (F.col("kafka_offset") < 0),
            "invalid Kafka offset",
        ),
        (F.col("kafka_timestamp").isNull(), "missing Kafka timestamp"),
        (F.col("ingested_at").isNull(), "missing ingestion timestamp"),
    ]

    # Delete events legitimately have no "after" image, so business fields
    # are checked only for active records.
    for column in REQUIRED_ACTIVE_FIELDS[table_name]:
        checks.append(
            (
                active & F.col(column).isNull(),
                f"missing required field: {column}",
            )
        )

    for column in NON_BLANK_ACTIVE_FIELDS[table_name]:
        checks.append(
            (
                active & F.col(column).isNotNull() & (F.length(F.trim(F.col(column))) == 0),
                f"blank required field: {column}",
            )
        )

    if table_name == "transactions":
        checks.append(
            (
                active & F.col("amount").isNotNull() & (F.col("amount") < F.lit(0)),
                "negative transaction amount",
            )
        )

    error_text = F.concat_ws(
        "; ",
        *[F.when(condition, F.lit(message)) for condition, message in checks],
    )

    return events.withColumn(
        "_validation_error",
        F.when(
            F.length(error_text) > 0,
            error_text,
        ).otherwise(F.lit(None).cast("string")),
    )


def build_events(bronze_batch: DataFrame, table_name: str) -> DataFrame:
    """Decode one Bronze microbatch into typed rows with CDC lineage."""
    if table_name not in TABLES:
        raise ValueError(f"Unsupported Silver table: {table_name}")

    primary_key = TABLES[table_name]

    raw = (
        bronze_batch.filter(F.col("raw_value").isNotNull())
        .withColumn("_raw_json", F.col("raw_value").cast("string"))
        .withColumn(
            "_operation",
            F.get_json_object(F.col("_raw_json"), "$.payload.op"),
        )
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

    # Invalid keys and operations are retained here so they can be quarantined,
    # rather than silently dropped as they were in the previous writer.
    return _add_validation_errors(events, table_name)


def ensure_delta_table(
    spark: SparkSession,
    bronze_schema,
    table_name: str,
) -> str:
    """Create an empty Silver Delta target once, without replacing existing data."""
    target_path = f"{SILVER_ROOT}/{table_name}"

    if DeltaTable.isDeltaTable(spark, target_path):
        return target_path

    if os.path.exists(target_path):
        raise RuntimeError(f"Silver path exists but is not a Delta table: {target_path}")

    empty_bronze = spark.createDataFrame([], bronze_schema)
    empty_events = build_events(empty_bronze, table_name).drop("_validation_error")
    empty_events.write.format("delta").mode("errorifexists").save(target_path)

    return target_path


def write_quarantine(
    spark: SparkSession,
    invalid_events: DataFrame,
    table_name: str,
) -> None:
    """Persist invalid events idempotently, including their rejection reasons."""
    quarantine_path = f"{QUARANTINE_ROOT}/{table_name}"

    rejected = invalid_events.withColumnRenamed(
        "_validation_error",
        "rejection_reason",
    )

    # The source coordinates make an event stable across foreachBatch retries.
    # Hashing the full rejected row also provides a stable fallback when some
    # source metadata is malformed.
    row_json = F.to_json(F.struct(*[F.col(column) for column in rejected.columns]))
    rejected = rejected.withColumn(
        "quarantine_id",
        F.sha2(row_json, 256),
    )

    if DeltaTable.isDeltaTable(spark, quarantine_path):
        (
            DeltaTable.forPath(spark, quarantine_path)
            .alias("t")
            .merge(
                rejected.alias("s"),
                "t.quarantine_id = s.quarantine_id",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        rejected.write.format("delta").mode("errorifexists").save(quarantine_path)


def make_batch_handler(
    spark: SparkSession,
    table_name: str,
    primary_key: str,
    target_path: str,
) -> Callable[[DataFrame, int], None]:
    """Validate, quarantine, deduplicate, and merge one Silver microbatch."""

    def handle_batch(batch_df: DataFrame, batch_id: int) -> None:
        events = build_events(batch_df, table_name).persist(StorageLevel.MEMORY_AND_DISK)

        try:
            # One batch-level aggregation determines whether valid or invalid
            # rows exist. This is not a full-table audit.
            stats = events.agg(
                F.count(F.when(F.col("_validation_error").isNotNull(), 1)).alias("invalid_count"),
                F.count(F.when(F.col("_validation_error").isNull(), 1)).alias("valid_count"),
            ).first()

            invalid_count = int(stats["invalid_count"] or 0)
            valid_count = int(stats["valid_count"] or 0)

            if invalid_count:
                write_quarantine(
                    spark,
                    events.filter(F.col("_validation_error").isNotNull()),
                    table_name,
                )
                print(
                    f"Silver {table_name}: quarantined {invalid_count} "
                    f"invalid event(s) in microbatch {batch_id}",
                    flush=True,
                )

            if not valid_count:
                print(
                    f"Silver {table_name}: no valid events in microbatch {batch_id}",
                    flush=True,
                )
                return

            valid_events = events.filter(F.col("_validation_error").isNull()).drop(
                "_validation_error"
            )

            order = Window.partitionBy(primary_key).orderBy(
                F.col("kafka_partition").desc(),
                F.col("kafka_offset").desc(),
            )

            latest = (
                valid_events.withColumn(
                    "_row_number",
                    F.row_number().over(order),
                )
                .filter(F.col("_row_number") == 1)
                .drop("_row_number")
            )

            target = DeltaTable.forPath(spark, target_path)

            (
                target.alias("t")
                .merge(
                    latest.alias("s"),
                    f"t.{primary_key} = s.{primary_key}",
                )
                .whenMatchedUpdateAll(
                    condition=(
                        "s.kafka_partition = t.kafka_partition AND s.kafka_offset > t.kafka_offset"
                    )
                )
                .whenNotMatchedInsertAll()
                .execute()
            )

            print(
                f"Silver {table_name}: merged valid events in microbatch {batch_id}",
                flush=True,
            )

        finally:
            events.unpersist(blocking=False)

    return handle_batch


def main() -> None:
    spark = (
        SparkSession.builder.appName("StreamGuardSilverCDC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
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
            target_path = ensure_delta_table(
                spark,
                bronze_schema,
                table_name,
            )

            bronze_stream = (
                spark.readStream.schema(bronze_schema)
                .option("maxFilesPerTrigger", 20)
                .parquet(bronze_path)
            )

            query = (
                bronze_stream.writeStream.queryName(f"silver_{table_name}")
                .foreachBatch(
                    make_batch_handler(
                        spark,
                        table_name,
                        primary_key,
                        target_path,
                    )
                )
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
