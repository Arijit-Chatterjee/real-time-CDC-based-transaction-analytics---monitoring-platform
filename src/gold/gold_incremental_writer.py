"""Incrementally maintain StreamGuard Gold tables from Silver Delta CDF.

The existing full-refresh Gold pipeline remains the recovery path. Start this
writer only after a successful Gold baseline and with CDF enabled on all three
Silver tables. Do not run the full-refresh builder concurrently with this writer.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_ROOT = "/opt/streamguard/lake/silver"
GOLD_ROOT = "/opt/streamguard/lake/gold"
CHECKPOINT_ROOT = "/opt/streamguard/lake/checkpoints/gold_incremental"

# CDF was enabled when these Silver table versions were current. These versions
# establish the starting point; the already-built Gold baseline covers their
# current rows. CDF is only recorded for data changes after it is enabled.
STARTING_VERSIONS = {
    "transactions": 6,
    "customers": 2,
    "merchants": 2,
}

INR_HIGH_VALUE_THRESHOLD = 50_000
PROCESSING_INTERVAL = "10 seconds"
MAX_FILES_PER_TRIGGER = 5

LOG = logging.getLogger("streamguard.gold.incremental")


def read_gold(spark: SparkSession, table_name: str) -> DataFrame:
    return spark.read.format("delta").load(f"{GOLD_ROOT}/{table_name}")


def cdf_stream(spark: SparkSession, table_name: str) -> DataFrame:
    """Read row-level Silver changes from the configured baseline version."""
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", STARTING_VERSIONS[table_name])
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .load(f"{SILVER_ROOT}/{table_name}")
    )


def latest_changes(batch: DataFrame, key_column: str) -> DataFrame:
    """Keep the newest post-image or physical delete for each business key.

    CDF update_preimage rows are intentionally ignored. If a key changes more
    than once in a micro-batch, the greatest Silver commit version wins.
    """
    changes = batch.filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
    window = Window.partitionBy(key_column).orderBy(
        F.col("_commit_version").desc(),
        F.col("kafka_offset").desc_nulls_last(),
    )
    return (
        changes.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def ensure_valid_source_keys(df: DataFrame, key_column: str, label: str) -> None:
    if df.filter(F.col(key_column).isNull()).limit(1).count():
        raise ValueError(f"{label} CDF batch contains a null {key_column}")
    if df.groupBy(key_column).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"{label} CDF batch contains duplicate {key_column} values")


def merge_customer_dimension(spark: SparkSession, source: DataFrame) -> None:
    source = source.withColumn(
        "is_deleted",
        F.when(F.col("_change_type") == "delete", F.lit(True)).otherwise(
            F.coalesce(F.col("is_deleted"), F.lit(False))
        ),
    ).select(
        F.col("customer_id").alias("customer_key"),
        "customer_id",
        "created_at",
        "home_region",
        "account_status",
        "is_deleted",
    )
    ensure_valid_source_keys(source, "customer_key", "customers")

    target_path = f"{GOLD_ROOT}/dim_customer"
    (
        DeltaTable.forPath(spark, target_path)
        .alias("t")
        .merge(source.alias("s"), "t.customer_key = s.customer_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_merchant_dimension(spark: SparkSession, source: DataFrame) -> None:
    source = source.withColumn(
        "is_deleted",
        F.when(F.col("_change_type") == "delete", F.lit(True)).otherwise(
            F.coalesce(F.col("is_deleted"), F.lit(False))
        ),
    ).select(
        F.col("merchant_id").alias("merchant_key"),
        "merchant_id",
        "created_at",
        "merchant_name",
        "merchant_category",
        "merchant_region",
        "is_deleted",
    )
    ensure_valid_source_keys(source, "merchant_key", "merchants")

    target_path = f"{GOLD_ROOT}/dim_merchant"
    (
        DeltaTable.forPath(spark, target_path)
        .alias("t")
        .merge(source.alias("s"), "t.merchant_key = s.merchant_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def process_dimension_batch(
    batch: DataFrame,
    batch_id: int,
    merge_function: Callable[[SparkSession, DataFrame], None],
    label: str,
) -> None:
    if batch.limit(1).count() == 0:
        return

    changes = latest_changes(batch, "customer_id" if label == "customers" else "merchant_id")
    if changes.limit(1).count() == 0:
        LOG.info("%s batch %s: no applicable CDF changes", label, batch_id)
        return

    merge_function(batch.sparkSession, changes)
    LOG.info("%s batch %s merged into Gold", label, batch_id)


def build_date_dimension(spark: SparkSession, start_date: date, end_date: date) -> DataFrame:
    calendar = spark.range(1).select(
        F.explode(
            F.sequence(
                F.lit(start_date).cast("date"),
                F.lit(end_date).cast("date"),
                F.expr("INTERVAL 1 DAY"),
            )
        ).alias("full_date")
    )
    day_of_week = F.dayofweek(F.col("full_date"))
    return calendar.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day_of_month"),
        day_of_week.alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        day_of_week.isin(1, 7).alias("is_weekend"),
    )


def ensure_date_keys(spark: SparkSession, event_times: DataFrame) -> None:
    """Extend dim_date before writing facts that reference new event dates."""
    incoming_dates = (
        event_times.select(F.to_date("event_time").alias("full_date"))
        .filter(F.col("full_date").isNotNull())
        .distinct()
    )
    incoming_bounds = incoming_dates.agg(
        F.min("full_date").alias("min_date"), F.max("full_date").alias("max_date")
    ).first()
    if incoming_bounds["min_date"] is None:
        raise ValueError("Transaction CDF batch has no valid event_time values")

    date_path = f"{GOLD_ROOT}/dim_date"
    existing = read_gold(spark, "dim_date")
    existing_bounds = existing.agg(
        F.min("full_date").alias("min_date"), F.max("full_date").alias("max_date")
    ).first()

    min_date = min(existing_bounds["min_date"], incoming_bounds["min_date"])
    max_date = max(existing_bounds["max_date"], incoming_bounds["max_date"])
    expanded_calendar = build_date_dimension(spark, min_date, max_date)
    missing_dates = expanded_calendar.join(existing.select("date_key"), "date_key", "left_anti")

    if missing_dates.limit(1).count() == 0:
        return

    (
        DeltaTable.forPath(spark, date_path)
        .alias("t")
        .merge(missing_dates.alias("s"), "t.date_key = s.date_key")
        .whenNotMatchedInsertAll()
        .execute()
    )
    LOG.info("Extended Gold dim_date to cover %s through %s", min_date, max_date)


def build_fact_rows(spark: SparkSession, transactions: DataFrame) -> DataFrame:
    customers = read_gold(spark, "dim_customer").select("customer_key", "customer_id")
    merchants = read_gold(spark, "dim_merchant").select("merchant_key", "merchant_id")
    dates = read_gold(spark, "dim_date").select("date_key", "full_date")

    joined = (
        transactions.alias("t")
        .join(customers.alias("c"), F.col("t.customer_id") == F.col("c.customer_id"), "left")
        .join(merchants.alias("m"), F.col("t.merchant_id") == F.col("m.merchant_id"), "left")
        .join(dates.alias("d"), F.to_date(F.col("t.event_time")) == F.col("d.full_date"), "left")
    )

    missing_key = (
        joined.filter(
            F.col("c.customer_key").isNull()
            | F.col("m.merchant_key").isNull()
            | F.col("d.date_key").isNull()
        )
        .limit(1)
        .count()
    )
    if missing_key:
        raise ValueError(
            "Cannot update Gold facts: a transaction has a missing customer, merchant, or date key. "
            "The batch will fail without advancing its checkpoint; check the dimension streams and retry."
        )

    bad_rows = (
        transactions.filter(
            F.col("transaction_id").isNull()
            | F.col("customer_id").isNull()
            | F.col("merchant_id").isNull()
            | F.col("event_time").isNull()
            | F.col("amount").isNull()
            | F.col("currency").isNull()
            | F.col("status").isNull()
            | (F.col("amount") < F.lit(0))
            | (F.length(F.trim(F.col("currency"))) == 0)
            | (F.length(F.trim(F.col("status"))) == 0)
        )
        .limit(1)
        .count()
    )
    if bad_rows:
        raise ValueError("Transaction CDF batch violates required Gold fact data-quality rules")

    high_value = (F.col("t.currency") == F.lit("INR")) & (
        F.col("t.amount") > F.lit(INR_HIGH_VALUE_THRESHOLD).cast("decimal(12,2)")
    )

    return joined.select(
        F.col("t.transaction_id").alias("transaction_id"),
        F.col("c.customer_key").alias("customer_key"),
        F.col("m.merchant_key").alias("merchant_key"),
        F.col("d.date_key").alias("date_key"),
        F.col("t.amount").alias("amount"),
        F.col("t.currency").alias("currency"),
        F.col("t.status").alias("transaction_status"),
        F.col("t.region").alias("transaction_region"),
        F.col("t.event_time").alias("event_time"),
        F.col("t.created_at").alias("transaction_created_at"),
        F.col("t.updated_at").alias("transaction_updated_at"),
        high_value.alias("is_high_value_inr"),
        F.when(high_value, F.lit("INR transaction exceeds ₹50,000.00"))
        .otherwise(F.lit(None).cast("string"))
        .alias("risk_reason"),
        F.col("t.operation").alias("last_cdc_operation"),
        F.col("t.snapshot_type").alias("snapshot_type"),
        F.col("t.source_ts_ms").alias("source_ts_ms"),
        F.col("t.source_lsn").alias("source_lsn"),
        F.col("t.kafka_topic").alias("kafka_topic"),
        F.col("t.kafka_partition").alias("kafka_partition"),
        F.col("t.kafka_offset").alias("kafka_offset"),
        F.col("t.kafka_timestamp").alias("kafka_timestamp"),
        F.col("t.kafka_timestamp_type").alias("kafka_timestamp_type"),
        F.col("t.ingested_at").alias("ingested_at"),
    )


def process_transaction_batch(batch: DataFrame, batch_id: int) -> None:
    if batch.limit(1).count() == 0:
        return

    changes = latest_changes(batch, "transaction_id")
    if changes.limit(1).count() == 0:
        LOG.info("transactions batch %s: no applicable CDF changes", batch_id)
        return

    ensure_valid_source_keys(changes, "transaction_id", "transactions")
    spark = batch.sparkSession

    # A physical Silver delete or a Silver soft-delete tombstone removes the
    # corresponding current-state row from Gold fact_transactions.
    delete_ids = (
        changes.filter(
            (F.col("_change_type") == "delete") | F.coalesce(F.col("is_deleted"), F.lit(False))
        )
        .select("transaction_id")
        .distinct()
    )

    if delete_ids.limit(1).count() > 0:
        fact_path = f"{GOLD_ROOT}/fact_transactions"
        (
            DeltaTable.forPath(spark, fact_path)
            .alias("t")
            .merge(delete_ids.alias("s"), "t.transaction_id = s.transaction_id")
            .whenMatchedDelete()
            .execute()
        )

    upserts = changes.filter(
        (F.col("_change_type") != "delete") & ~F.coalesce(F.col("is_deleted"), F.lit(False))
    )
    if upserts.limit(1).count() == 0:
        LOG.info("transactions batch %s: applied deletes only", batch_id)
        return

    ensure_date_keys(spark, upserts.select("event_time"))
    fact_rows = build_fact_rows(spark, upserts)
    ensure_valid_source_keys(fact_rows, "transaction_id", "Gold fact upserts")

    fact_path = f"{GOLD_ROOT}/fact_transactions"
    (
        DeltaTable.forPath(spark, fact_path)
        .alias("t")
        .merge(fact_rows.alias("s"), "t.transaction_id = s.transaction_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    LOG.info(
        "transactions batch %s merged into Gold; merchant mart refresh is handled separately from this writer",
        batch_id,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    spark = (
        SparkSession.builder.appName("StreamGuardGoldIncrementalWriter")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Check that the Gold baseline exists before consuming CDF changes.
    for table_name in ("dim_customer", "dim_merchant", "dim_date", "fact_transactions"):
        path = f"{GOLD_ROOT}/{table_name}"
        if not DeltaTable.isDeltaTable(spark, path):
            raise RuntimeError(
                f"Gold baseline table {table_name} is missing at {path}; run the full-refresh Gold pipeline first"
            )

    streams = [
        (
            "customers",
            cdf_stream(spark, "customers"),
            lambda batch, batch_id: process_dimension_batch(
                batch, batch_id, merge_customer_dimension, "customers"
            ),
        ),
        (
            "merchants",
            cdf_stream(spark, "merchants"),
            lambda batch, batch_id: process_dimension_batch(
                batch, batch_id, merge_merchant_dimension, "merchants"
            ),
        ),
        ("transactions", cdf_stream(spark, "transactions"), process_transaction_batch),
    ]

    queries = []
    try:
        for name, stream_df, handler in streams:
            checkpoint = f"{CHECKPOINT_ROOT}/{name}"
            query = (
                stream_df.writeStream.foreachBatch(handler)
                .option("checkpointLocation", checkpoint)
                .trigger(processingTime=PROCESSING_INTERVAL)
                .queryName(f"streamguard_gold_incremental_{name}")
                .start()
            )
            queries.append(query)
            LOG.info(
                "Started Gold %s CDF stream; checkpoint=%s; new checkpoint start version=%s",
                name,
                checkpoint,
                STARTING_VERSIONS[name],
            )

        LOG.info("All StreamGuard Gold incremental CDF streams are running")
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
