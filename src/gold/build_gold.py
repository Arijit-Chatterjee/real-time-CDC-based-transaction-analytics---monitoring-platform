"""Build StreamGuard Gold star-schema tables from the current Silver Delta tables.

This is an idempotent batch refresh: each run reads a consistent Delta snapshot of
Silver and overwrites the four Gold tables. It does not modify Silver or its
streaming checkpoints.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SILVER_BASE = "/opt/streamguard/lake/silver"
GOLD_BASE = "/opt/streamguard/lake/gold"
INR_HIGH_VALUE_THRESHOLD = 50_000


def read_silver(spark: SparkSession, table_name: str) -> DataFrame:
    """Read a Silver Delta table from the project lake."""
    path = f"{SILVER_BASE}/{table_name}"
    return spark.read.format("delta").load(path)


def assert_key_is_unique(df: DataFrame, key_column: str, table_name: str) -> None:
    """Fail the build rather than publish dimensions with missing or duplicate keys."""
    if df.filter(F.col(key_column).isNull()).limit(1).count():
        raise ValueError(f"{table_name}.{key_column} contains null values")

    duplicates_exist = (
        df.groupBy(key_column).count().filter(F.col("count") > 1).limit(1).count() > 0
    )
    if duplicates_exist:
        raise ValueError(f"{table_name}.{key_column} is not unique")


def write_delta(df: DataFrame, table_name: str) -> int:
    """Overwrite one Gold Delta table and return the number of rows written."""
    path = f"{GOLD_BASE}/{table_name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    return df.count()


def main() -> None:
    spark = SparkSession.builder.appName("StreamGuardGoldStarSchema").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Silver is current-state data: one latest row per source primary key.
        transactions = read_silver(spark, "transactions")
        customers = read_silver(spark, "customers")
        merchants = read_silver(spark, "merchants")

        # Exclude transaction tombstones/deleted current-state rows from the fact.
        active_transactions = transactions.filter(F.col("is_deleted") == F.lit(False))

        # Type-1 dimensions for this first iteration. Source IDs are used as stable
        # dimension keys; SCD Type 2/surrogate-key history can be added later.
        dim_customer = customers.select(
            F.col("customer_id").alias("customer_key"),
            "customer_id",
            "created_at",
            "home_region",
            "account_status",
            "is_deleted",
        )
        dim_merchant = merchants.select(
            F.col("merchant_id").alias("merchant_key"),
            "merchant_id",
            "created_at",
            "merchant_name",
            "merchant_category",
            "merchant_region",
            "is_deleted",
        )

        assert_key_is_unique(dim_customer, "customer_key", "dim_customer")
        assert_key_is_unique(dim_merchant, "merchant_key", "dim_merchant")

        # Generate a continuous calendar between the first and last active
        # transaction dates, rather than only dates that happened to have activity.
        transaction_dates = active_transactions.select(F.to_date("event_time").alias("event_date"))
        date_bounds = transaction_dates.agg(
            F.min("event_date").alias("min_date"),
            F.max("event_date").alias("max_date"),
        )
        calendar_dates = date_bounds.select(
            F.explode(
                F.sequence(
                    F.col("min_date"),
                    F.col("max_date"),
                    F.expr("INTERVAL 1 DAY"),
                )
            ).alias("full_date")
        )

        dim_date = (
            calendar_dates.withColumn(
                "date_key", F.date_format("full_date", "yyyyMMdd").cast("int")
            )
            .withColumn("year", F.year("full_date"))
            .withColumn("quarter", F.quarter("full_date"))
            .withColumn("month", F.month("full_date"))
            .withColumn("month_name", F.date_format("full_date", "MMMM"))
            .withColumn("day_of_month", F.dayofmonth("full_date"))
            .withColumn("day_of_week", F.dayofweek("full_date"))
            .withColumn("day_name", F.date_format("full_date", "EEEE"))
            .withColumn(
                "is_weekend",
                F.dayofweek("full_date").isin([1, 7]),
            )
            .select(
                "date_key",
                "full_date",
                "year",
                "quarter",
                "month",
                "month_name",
                "day_of_month",
                "day_of_week",
                "day_name",
                "is_weekend",
            )
        )
        assert_key_is_unique(dim_date, "date_key", "dim_date")

        # Resolve dimension keys with left joins first so missing lookups are
        # detected instead of silently dropping fact rows.
        tx = active_transactions.withColumn("event_date", F.to_date("event_time")).alias("t")
        joined = (
            tx.join(
                dim_customer.alias("dc"),
                F.col("t.customer_id") == F.col("dc.customer_id"),
                "left",
            )
            .join(
                dim_merchant.alias("dm"),
                F.col("t.merchant_id") == F.col("dm.merchant_id"),
                "left",
            )
            .join(
                dim_date.alias("dd"),
                F.col("t.event_date") == F.col("dd.full_date"),
                "left",
            )
        )

        missing_dimension_keys = (
            joined.filter(
                F.col("dc.customer_key").isNull()
                | F.col("dm.merchant_key").isNull()
                | F.col("dd.date_key").isNull()
            )
            .limit(1)
            .count()
        )
        if missing_dimension_keys:
            raise ValueError(
                "Gold fact build found a transaction without a customer, merchant, "
                "or date dimension match. Gold tables were not written."
            )

        high_value_condition = (F.col("t.currency") == F.lit("INR")) & (
            F.col("t.amount") > F.lit(INR_HIGH_VALUE_THRESHOLD).cast("decimal(12,2)")
        )

        fact_transactions = joined.select(
            F.col("t.transaction_id"),
            F.col("dc.customer_key"),
            F.col("dm.merchant_key"),
            F.col("dd.date_key"),
            F.col("t.amount"),
            F.col("t.currency"),
            F.col("t.status").alias("transaction_status"),
            F.col("t.region").alias("transaction_region"),
            F.col("t.event_time"),
            F.col("t.created_at").alias("transaction_created_at"),
            F.col("t.updated_at").alias("transaction_updated_at"),
            high_value_condition.alias("is_high_value_inr"),
            F.when(
                high_value_condition,
                F.lit(f"INR transaction exceeds ₹{INR_HIGH_VALUE_THRESHOLD:,.2f}"),
            )
            .otherwise(F.lit(None).cast("string"))
            .alias("risk_reason"),
            F.col("t.operation").alias("last_cdc_operation"),
            F.col("t.snapshot_type"),
            F.col("t.source_ts_ms"),
            F.col("t.source_lsn"),
            F.col("t.kafka_topic"),
            F.col("t.kafka_partition"),
            F.col("t.kafka_offset"),
            F.col("t.kafka_timestamp"),
            F.col("t.kafka_timestamp_type"),
            F.col("t.ingested_at"),
        )

        assert_key_is_unique(fact_transactions, "transaction_id", "fact_transactions")

        # Dimensions are written before the fact so the fact never points to a
        # dimension version that this run has not yet written.
        counts = {
            "dim_customer": write_delta(dim_customer, "dim_customer"),
            "dim_merchant": write_delta(dim_merchant, "dim_merchant"),
            "dim_date": write_delta(dim_date, "dim_date"),
            "fact_transactions": write_delta(fact_transactions, "fact_transactions"),
        }

        print("StreamGuard Gold star schema refreshed successfully.")
        for table_name, count in counts.items():
            print(f"{table_name}: {count:,} rows -> {GOLD_BASE}/{table_name}")

        high_value_count = fact_transactions.filter(F.col("is_high_value_inr")).count()
        print(f"High-value INR transactions: {high_value_count:,}")
        print("Note: high-value is a rule-based review flag, not a confirmed fraud label.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
