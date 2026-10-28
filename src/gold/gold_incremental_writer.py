"""Incrementally maintain StreamGuard Gold tables from Silver Delta CDF.

The existing full-refresh Gold pipeline remains the recovery path. Start this
writer only after a successful Gold baseline and with CDF enabled on all three
Silver tables. Do not run the full-refresh builder concurrently with this writer.

Silver owns row-level business validation. This writer performs Gold
transformations, dimension resolution, and idempotent Delta merges.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import date, timedelta

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_ROOT = "/opt/streamguard/lake/silver"
GOLD_ROOT = "/opt/streamguard/lake/gold"
CHECKPOINT_ROOT = "/opt/streamguard/lake/checkpoints/gold_incremental"
_DATE_BOUNDS: tuple[date, date] | None = None

# These versions establish the starting point covered by the existing
# Gold baseline. Keep them aligned with your current Gold checkpoints.
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
    """Read the current Delta snapshot of a Gold table."""
    return spark.read.format("delta").load(f"{GOLD_ROOT}/{table_name}")


def cdf_stream(spark: SparkSession, table_name: str) -> DataFrame:
    """Read row-level Silver changes after the established baseline."""
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", STARTING_VERSIONS[table_name])
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .load(f"{SILVER_ROOT}/{table_name}")
    )


def latest_changes(batch: DataFrame, key_column: str) -> DataFrame:
    """Keep the latest post-image or physical delete for each business key.

    Update-preimage rows are ignored. The resulting data has at most one
    row per business key, so a separate duplicate-key Spark scan is redundant.
    """
    changes = batch.filter(
        F.col("_change_type").isin(
            "insert",
            "update_postimage",
            "delete",
        )
    )

    window = Window.partitionBy(key_column).orderBy(
        F.col("_commit_version").desc(),
        F.col("kafka_offset").desc_nulls_last(),
    )

    return (
        changes.withColumn(
            "_row_number",
            F.row_number().over(window),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def merge_customer_dimension(
    spark: SparkSession,
    source: DataFrame,
) -> None:
    """Merge customer changes into the Gold customer dimension."""
    source = source.withColumn(
        "is_deleted",
        F.when(
            F.col("_change_type") == "delete",
            F.lit(True),
        ).otherwise(F.coalesce(F.col("is_deleted"), F.lit(False))),
    ).select(
        F.col("customer_id").alias("customer_key"),
        "customer_id",
        "created_at",
        "home_region",
        "account_status",
        "is_deleted",
    )

    target_path = f"{GOLD_ROOT}/dim_customer"

    (
        DeltaTable.forPath(spark, target_path)
        .alias("t")
        .merge(
            source.alias("s"),
            "t.customer_key = s.customer_key",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_merchant_dimension(
    spark: SparkSession,
    source: DataFrame,
) -> None:
    """Merge merchant changes into the Gold merchant dimension."""
    source = source.withColumn(
        "is_deleted",
        F.when(
            F.col("_change_type") == "delete",
            F.lit(True),
        ).otherwise(F.coalesce(F.col("is_deleted"), F.lit(False))),
    ).select(
        F.col("merchant_id").alias("merchant_key"),
        "merchant_id",
        "created_at",
        "merchant_name",
        "merchant_category",
        "merchant_region",
        "is_deleted",
    )

    target_path = f"{GOLD_ROOT}/dim_merchant"

    (
        DeltaTable.forPath(spark, target_path)
        .alias("t")
        .merge(
            source.alias("s"),
            "t.merchant_key = s.merchant_key",
        )
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
    """Merge the applicable CDF changes for one dimension microbatch."""
    key_column = "customer_id" if label == "customers" else "merchant_id"

    changes = latest_changes(batch, key_column)

    # A batch may contain only update-preimage rows, which are ignored.
    # One short action avoids issuing a Delta merge with an empty source.
    if changes.limit(1).count() == 0:
        LOG.info(
            "%s batch %s: no applicable CDF changes",
            label,
            batch_id,
        )
        return

    merge_function(batch.sparkSession, changes)

    LOG.info(
        "%s batch %s merged into Gold",
        label,
        batch_id,
    )


def build_date_dimension(
    spark: SparkSession,
    start_date: date,
    end_date: date,
) -> DataFrame:
    """Generate the calendar rows used by the Gold date dimension."""
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


def initialize_date_bounds(spark: SparkSession) -> None:
    """Read the existing Gold date range once at startup."""
    global _DATE_BOUNDS

    bounds = (
        read_gold(spark, "dim_date")
        .agg(
            F.min("full_date").alias("min_date"),
            F.max("full_date").alias("max_date"),
        )
        .first()
    )

    if bounds["min_date"] is None or bounds["max_date"] is None:
        raise ValueError("Gold dim_date is empty or has no valid date bounds")

    _DATE_BOUNDS = (bounds["min_date"], bounds["max_date"])

    LOG.info(
        "Loaded Gold dim_date bounds: %s through %s",
        bounds["min_date"],
        bounds["max_date"],
    )


def ensure_date_keys(
    spark: SparkSession,
    incoming_min: date | None,
    incoming_max: date | None,
) -> None:
    """Extend dim_date only when an incoming event falls outside its range."""
    global _DATE_BOUNDS

    if incoming_min is None or incoming_max is None:
        raise ValueError("Transaction batch has no valid event_time values")

    if _DATE_BOUNDS is None:
        initialize_date_bounds(spark)

    assert _DATE_BOUNDS is not None
    existing_min, existing_max = _DATE_BOUNDS

    # Normal transactions use dates already covered by the calendar.
    # No Spark action is required for this common case.
    if incoming_min >= existing_min and incoming_max <= existing_max:
        return

    new_ranges: list[tuple[date, date]] = []

    if incoming_min < existing_min:
        new_ranges.append((incoming_min, existing_min - timedelta(days=1)))

    if incoming_max > existing_max:
        new_ranges.append((existing_max + timedelta(days=1), incoming_max))

    new_dates = [
        build_date_dimension(spark, start_date, end_date)
        for start_date, end_date in new_ranges
        if start_date <= end_date
    ]

    if not new_dates:
        return

    missing_dates = new_dates[0]
    for additional_dates in new_dates[1:]:
        missing_dates = missing_dates.unionByName(additional_dates)

    date_path = f"{GOLD_ROOT}/dim_date"

    (
        DeltaTable.forPath(spark, date_path)
        .alias("t")
        .merge(
            missing_dates.alias("s"),
            "t.date_key = s.date_key",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    _DATE_BOUNDS = (
        min(existing_min, incoming_min),
        max(existing_max, incoming_max),
    )

    LOG.info(
        "Extended Gold dim_date to cover %s through %s",
        _DATE_BOUNDS[0],
        _DATE_BOUNDS[1],
    )


def build_fact_rows(
    spark: SparkSession,
    transactions: DataFrame,
) -> DataFrame:
    """Resolve Gold dimension keys and build transaction fact rows.

    Silver validates transaction fields. Gold retains the important guard
    that all foreign keys can be resolved before writing the fact table.
    The output is persisted because it is first checked and then merged.
    """
    customers = read_gold(spark, "dim_customer").select(
        "customer_key",
        "customer_id",
    )
    merchants = read_gold(spark, "dim_merchant").select(
        "merchant_key",
        "merchant_id",
    )
    dates = read_gold(spark, "dim_date").select(
        "date_key",
        "full_date",
    )

    joined = (
        transactions.alias("t")
        .join(
            customers.alias("c"),
            F.col("t.customer_id") == F.col("c.customer_id"),
            "left",
        )
        .join(
            merchants.alias("m"),
            F.col("t.merchant_id") == F.col("m.merchant_id"),
            "left",
        )
        .join(
            dates.alias("d"),
            F.to_date(F.col("t.event_time")) == F.col("d.full_date"),
            "left",
        )
    )

    high_value = (F.col("t.currency") == F.lit("INR")) & (
        F.col("t.amount") > F.lit(INR_HIGH_VALUE_THRESHOLD).cast("decimal(12,2)")
    )

    fact_rows = joined.select(
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
        F.when(
            high_value,
            F.lit("INR transaction exceeds ₹50,000.00"),
        )
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
    ).persist(StorageLevel.MEMORY_AND_DISK)

    missing_key = (
        fact_rows.filter(
            F.col("customer_key").isNull()
            | F.col("merchant_key").isNull()
            | F.col("date_key").isNull()
        )
        .limit(1)
        .count()
    )

    if missing_key:
        fact_rows.unpersist(blocking=False)
        raise ValueError(
            "Cannot update Gold facts: a transaction has a missing "
            "customer, merchant, or date key. The batch will fail without "
            "advancing its checkpoint; check the dimension streams and retry."
        )

    return fact_rows


def process_transaction_batch(batch: DataFrame, batch_id: int) -> None:
    batch_started = time.perf_counter()
    timings = {}
    changes = latest_changes(batch, "transaction_id").persist(StorageLevel.MEMORY_AND_DISK)

    try:
        stage_started = time.perf_counter()

        delete_condition = (F.col("_change_type") == "delete") | F.coalesce(
            F.col("is_deleted"), F.lit(False)
        )
        upsert_condition = (F.col("_change_type") != "delete") & ~F.coalesce(
            F.col("is_deleted"), F.lit(False)
        )

        stats = changes.agg(
            F.count(F.lit(1)).alias("row_count"),
            F.max(F.when(delete_condition, 1).otherwise(0)).alias("has_deletes"),
            F.max(F.when(upsert_condition, 1).otherwise(0)).alias("has_upserts"),
            F.min(F.when(upsert_condition, F.to_date("event_time"))).alias("min_event_date"),
            F.max(F.when(upsert_condition, F.to_date("event_time"))).alias("max_event_date"),
        ).first()

        timings["classify"] = time.perf_counter() - stage_started

        row_count = int(stats["row_count"] or 0)
        has_deletes = bool(stats["has_deletes"] or 0)
        has_upserts = bool(stats["has_upserts"] or 0)

        if row_count == 0:
            return

        spark = batch.sparkSession
        fact_path = f"{GOLD_ROOT}/fact_transactions"

        if has_deletes:
            stage_started = time.perf_counter()

            delete_ids = changes.filter(delete_condition).select("transaction_id")

            (
                DeltaTable.forPath(spark, fact_path)
                .alias("t")
                .merge(
                    delete_ids.alias("s"),
                    "t.transaction_id = s.transaction_id",
                )
                .whenMatchedDelete()
                .execute()
            )

            timings["delete_merge"] = time.perf_counter() - stage_started

        if not has_upserts:
            LOG.info(
                "transactions batch %s: deletes-only batch",
                batch_id,
            )
            return

        upserts = changes.filter(upsert_condition)

        stage_started = time.perf_counter()
        ensure_date_keys(
            spark,
            stats["min_event_date"],
            stats["max_event_date"],
        )
        timings["ensure_date_keys"] = time.perf_counter() - stage_started

        stage_started = time.perf_counter()
        fact_rows = build_fact_rows(spark, upserts)
        timings["build_fact_rows_and_key_check"] = time.perf_counter() - stage_started

        try:
            stage_started = time.perf_counter()

            (
                DeltaTable.forPath(spark, fact_path)
                .alias("t")
                .merge(
                    fact_rows.alias("s"),
                    "t.transaction_id = s.transaction_id",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            timings["fact_merge"] = time.perf_counter() - stage_started

        finally:
            fact_rows.unpersist(blocking=False)

        LOG.info(
            "transactions batch %s merged into Gold",
            batch_id,
        )

    finally:
        changes.unpersist(blocking=False)

        timings["total"] = time.perf_counter() - batch_started

        LOG.info(
            "transactions batch %s timing_seconds=%s",
            batch_id,
            {name: round(seconds, 2) for name, seconds in timings.items()},
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

    queries = []

    try:
        # The existing full-refresh pipeline must have built these tables.
        for table_name in (
            "dim_customer",
            "dim_merchant",
            "dim_date",
            "fact_transactions",
        ):
            path = f"{GOLD_ROOT}/{table_name}"

            if not DeltaTable.isDeltaTable(spark, path):
                raise RuntimeError(
                    f"Gold baseline table {table_name} is missing at {path}; "
                    "run the full-refresh Gold pipeline first."
                )

        streams = [
            (
                "customers",
                cdf_stream(spark, "customers"),
                lambda batch, batch_id: process_dimension_batch(
                    batch,
                    batch_id,
                    merge_customer_dimension,
                    "customers",
                ),
            ),
            (
                "merchants",
                cdf_stream(spark, "merchants"),
                lambda batch, batch_id: process_dimension_batch(
                    batch,
                    batch_id,
                    merge_merchant_dimension,
                    "merchants",
                ),
            ),
            (
                "transactions",
                cdf_stream(spark, "transactions"),
                process_transaction_batch,
            ),
        ]

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
                "Started Gold %s CDF stream; checkpoint=%s; configured starting version=%s",
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
