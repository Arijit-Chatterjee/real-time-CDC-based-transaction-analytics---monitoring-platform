"""Build a currency-safe merchant performance mart from StreamGuard Gold facts.

The mart grain is one row per merchant and currency. Amounts are never combined
across currencies. The review rank is a transparent rule-based ordering, not a
fraud score or a claim that a merchant is fraudulent.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD_ROOT = "/opt/streamguard/lake/gold"
OUTPUT_TABLE = "mart_merchant_performance"
MIN_TRANSACTIONS_FOR_RANK = 20


def read_gold_delta(spark: SparkSession, table_name: str) -> DataFrame:
    """Read a Gold Delta table stored at the project's lake path."""
    return spark.read.format("delta").load(f"{GOLD_ROOT}/{table_name}")


def main() -> None:
    spark = (
        SparkSession.builder.appName("StreamGuardMerchantPerformanceMart")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        fact = read_gold_delta(spark, "fact_transactions")
        merchants = read_gold_delta(spark, "dim_merchant").select(
            "merchant_key",
            "merchant_id",
            "merchant_name",
            "merchant_category",
            "merchant_region",
        )

        # Fail rather than silently omit facts with missing merchant dimension keys.
        unmatched = (
            fact.select("merchant_key").distinct()
            .join(merchants.select("merchant_key"), "merchant_key", "left_anti")
            .limit(1)
            .count()
        )
        if unmatched:
            raise ValueError(
                "fact_transactions contains merchant keys missing from dim_merchant"
            )

        normalized = fact.withColumn(
            "_status", F.lower(F.trim(F.col("transaction_status")))
        )

        # Currency is part of the grain so INR and USD values are never added together.
        metrics = normalized.groupBy("merchant_key", "currency").agg(
            F.count(F.lit(1)).cast("long").alias("transaction_count"),
            F.countDistinct("customer_key").cast("long").alias("unique_customer_count"),
            F.sum(F.when(F.col("_status") == "approved", 1).otherwise(0))
            .cast("long")
            .alias("approved_count"),
            F.sum(F.when(F.col("_status") == "declined", 1).otherwise(0))
            .cast("long")
            .alias("declined_count"),
            F.sum(
                F.when(
                    F.col("_status").isNull()
                    | (~F.col("_status").isin("approved", "declined")),
                    1,
                ).otherwise(0)
            )
            .cast("long")
            .alias("other_status_count"),
            F.sum("amount").alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("average_transaction_amount"),
            F.min("amount").alias("minimum_transaction_amount"),
            F.max("amount").alias("maximum_transaction_amount"),
            F.sum(
                F.when(F.col("is_high_value_inr") == F.lit(True), 1).otherwise(0)
            )
            .cast("long")
            .alias("high_value_inr_count"),
            F.sum(
                F.when(
                    F.col("is_high_value_inr") == F.lit(True),
                    F.col("amount"),
                ).otherwise(F.lit(0).cast("decimal(12,2)"))
            ).alias("high_value_inr_amount"),
            F.min("event_time").alias("first_event_time"),
            F.max("event_time").alias("last_event_time"),
        )

        metrics = (
            metrics
            .withColumn(
                "approval_rate_pct",
                F.round(F.col("approved_count") * F.lit(100.0) / F.col("transaction_count"), 2),
            )
            .withColumn(
                "decline_rate_pct",
                F.round(F.col("declined_count") * F.lit(100.0) / F.col("transaction_count"), 2),
            )
        )

        # The rank prioritizes higher decline rates, then more high-value INR
        # transactions, then larger samples. Small samples remain unranked.
        ranking_window = Window.partitionBy("currency").orderBy(
            F.col("decline_rate_pct").desc(),
            F.col("high_value_inr_count").desc(),
            F.col("transaction_count").desc(),
            F.col("merchant_key").asc(),
        )
        ranks = (
            metrics.filter(F.col("transaction_count") >= MIN_TRANSACTIONS_FOR_RANK)
            .withColumn("review_priority_rank", F.row_number().over(ranking_window))
            .select("merchant_key", "currency", "review_priority_rank")
        )

        mart = (
            metrics.join(merchants, "merchant_key", "left")
            .join(ranks, ["merchant_key", "currency"], "left")
            .withColumn("minimum_transactions_for_rank", F.lit(MIN_TRANSACTIONS_FOR_RANK))
            .select(
                "merchant_key",
                "merchant_id",
                "merchant_name",
                "merchant_category",
                "merchant_region",
                "currency",
                "transaction_count",
                "unique_customer_count",
                "approved_count",
                "declined_count",
                "other_status_count",
                "approval_rate_pct",
                "decline_rate_pct",
                "total_amount",
                "average_transaction_amount",
                "minimum_transaction_amount",
                "maximum_transaction_amount",
                "high_value_inr_count",
                "high_value_inr_amount",
                "first_event_time",
                "last_event_time",
                "review_priority_rank",
                "minimum_transactions_for_rank",
            )
        )

        duplicate_grain = (
            mart.groupBy("merchant_key", "currency")
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )
        if duplicate_grain:
            raise ValueError("Merchant performance mart grain is not unique")

        output_path = f"{GOLD_ROOT}/{OUTPUT_TABLE}"
        mart.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(output_path)

        row_count = mart.count()
        ranked_count = mart.filter(F.col("review_priority_rank").isNotNull()).count()
        print("StreamGuard merchant performance mart built successfully.", flush=True)
        print(f"Rows (merchant + currency): {row_count:,}", flush=True)
        print(f"Ranked rows (minimum {MIN_TRANSACTIONS_FOR_RANK} transactions): {ranked_count:,}", flush=True)
        print(f"Output: {output_path}", flush=True)
        print(
            "Note: review_priority_rank is a rule-based review ordering, not a fraud label.",
            flush=True,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
