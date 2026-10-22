"""Read-only quality checks for the merchant performance Gold mart."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MART_PATH = "/opt/streamguard/lake/gold/mart_merchant_performance"
FACT_PATH = "/opt/streamguard/lake/gold/fact_transactions"


def main() -> None:
    spark = SparkSession.builder.appName("StreamGuard-Merchant-Mart-Validation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        mart = spark.read.format("delta").load(MART_PATH)
        fact = spark.read.format("delta").load(FACT_PATH)

        checks = {}

        # 1. Every Gold fact transaction must be counted in the mart.
        fact_count = fact.count()
        mart_transaction_count = mart.agg(F.sum("transaction_count").alias("total")).first()[
            "total"
        ]

        checks["mart transaction counts reconcile with Gold facts"] = (
            mart_transaction_count == fact_count
        )

        # 2. The mart must contain exactly the fact table's merchant/currency groups.
        fact_grain = fact.select("merchant_key", "currency").distinct()
        mart_grain = mart.select("merchant_key", "currency").distinct()

        missing_grains = (
            fact_grain.join(mart_grain, ["merchant_key", "currency"], "left_anti").limit(1).count()
        )
        extra_grains = (
            mart_grain.join(fact_grain, ["merchant_key", "currency"], "left_anti").limit(1).count()
        )

        checks["mart merchant/currency groups match Gold facts"] = (
            missing_grains == 0 and extra_grains == 0
        )

        # 3. Reconcile monetary totals separately for each currency.
        fact_currency_totals = fact.groupBy("currency").agg(
            F.sum("amount").alias("fact_total_amount")
        )
        mart_currency_totals = mart.groupBy("currency").agg(
            F.sum("total_amount").alias("mart_total_amount")
        )

        amount_mismatches = (
            fact_currency_totals.join(mart_currency_totals, "currency", "full_outer")
            .filter(
                F.col("fact_total_amount").isNull()
                | F.col("mart_total_amount").isNull()
                | (F.col("fact_total_amount") != F.col("mart_total_amount"))
            )
            .limit(1)
            .count()
        )

        checks["merchant amount totals reconcile by currency"] = amount_mismatches == 0

        # 4. Each merchant group must have sensible transaction/customer counts.
        checks["transaction and customer counts are valid"] = (
            mart.filter(
                F.col("transaction_count").isNull()
                | (F.col("transaction_count") <= 0)
                | F.col("unique_customer_count").isNull()
                | (F.col("unique_customer_count") <= 0)
                | (F.col("unique_customer_count") > F.col("transaction_count"))
            )
            .limit(1)
            .count()
            == 0
        )

        # 5. Approved, declined, and other statuses must account for every row.
        checks["status counts add up to transaction count"] = (
            mart.filter(
                F.col("approved_count").isNull()
                | F.col("declined_count").isNull()
                | F.col("other_status_count").isNull()
                | (F.col("approved_count") < 0)
                | (F.col("declined_count") < 0)
                | (F.col("other_status_count") < 0)
                | (
                    F.col("approved_count") + F.col("declined_count") + F.col("other_status_count")
                    != F.col("transaction_count")
                )
            )
            .limit(1)
            .count()
            == 0
        )

        # 6. Monetary aggregates must be present and internally consistent.
        checks["amount aggregates are internally consistent"] = (
            mart.filter(
                F.col("total_amount").isNull()
                | F.col("average_transaction_amount").isNull()
                | F.col("minimum_transaction_amount").isNull()
                | F.col("maximum_transaction_amount").isNull()
                | (F.col("total_amount") < 0)
                | (F.col("minimum_transaction_amount") < 0)
                | (F.col("minimum_transaction_amount") > F.col("maximum_transaction_amount"))
                | (F.col("average_transaction_amount") < F.col("minimum_transaction_amount"))
                | (F.col("average_transaction_amount") > F.col("maximum_transaction_amount"))
            )
            .limit(1)
            .count()
            == 0
        )

        # 7. Each merchant + currency must appear only once.
        checks["unique merchant/currency grain"] = (
            mart.groupBy("merchant_key", "currency")
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
            == 0
        )

        # 8. Required merchant identifiers and currency must not be null.
        checks["non-null merchant and currency identifiers"] = (
            mart.filter(
                F.col("merchant_key").isNull()
                | F.col("merchant_id").isNull()
                | F.col("currency").isNull()
            )
            .limit(1)
            .count()
            == 0
        )

        # 9. Every ranked merchant must meet the minimum transaction count.
        checks["ranked rows meet minimum transaction count"] = (
            mart.filter(
                F.col("review_priority_rank").isNotNull()
                & (F.col("transaction_count") < F.col("minimum_transactions_for_rank"))
            )
            .limit(1)
            .count()
            == 0
        )

        # 10. Eligible merchants should all have a rank.
        checks["eligible rows are ranked"] = (
            mart.filter(
                (F.col("transaction_count") >= F.col("minimum_transactions_for_rank"))
                & F.col("review_priority_rank").isNull()
            )
            .limit(1)
            .count()
            == 0
        )

        # 11. Ranks must be positive when present.
        checks["review ranks are positive"] = (
            mart.filter(
                F.col("review_priority_rank").isNotNull() & (F.col("review_priority_rank") < 1)
            )
            .limit(1)
            .count()
            == 0
        )

        # 12. Ranks must be unique within each currency.
        checks["unique ranks within each currency"] = (
            mart.filter(F.col("review_priority_rank").isNotNull())
            .groupBy("currency", "review_priority_rank")
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
            == 0
        )

        total_rows = mart.count()
        ranked_rows = mart.filter(F.col("review_priority_rank").isNotNull()).count()
        unranked_rows = total_rows - ranked_rows

        print(f"Gold fact transactions: {fact_count:,}", flush=True)
        print(f"Mart transaction count: {mart_transaction_count:,}", flush=True)
        print(f"Total mart rows: {total_rows:,}", flush=True)
        print(f"Ranked rows: {ranked_rows:,}", flush=True)
        print(f"Unranked rows: {unranked_rows:,}", flush=True)
        print("\nQuality checks:", flush=True)

        for name, passed in checks.items():
            print(
                f"{'PASS' if passed else 'FAIL'}: {name}",
                flush=True,
            )

        if not all(checks.values()):
            failed = [name for name, passed in checks.items() if not passed]
            raise ValueError("Merchant mart quality checks failed: " + ", ".join(failed))

        print("\nAll merchant mart quality checks passed.", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
