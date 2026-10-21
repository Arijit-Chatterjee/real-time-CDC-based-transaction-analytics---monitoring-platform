"""Read-only quality checks for the merchant performance Gold mart."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MART_PATH = "/opt/streamguard/lake/gold/mart_merchant_performance"


def main():
    spark = SparkSession.builder.appName("StreamGuard-Merchant-Mart-Validation").getOrCreate()

    try:
        mart = spark.read.format("delta").load(MART_PATH)

        checks = {}

        # 1. Each merchant + currency must appear only once.
        checks["unique merchant/currency grain"] = (
            mart.groupBy("merchant_key", "currency")
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
            == 0
        )

        # 2. Required identifiers must not be null.
        checks["non-null merchant and currency keys"] = (
            mart.filter(
                F.col("merchant_key").isNull()
                | F.col("merchant_id").isNull()
                | F.col("currency").isNull()
            )
            .limit(1)
            .count()
            == 0
        )

        # 3. Every ranked merchant must meet the minimum transaction count.
        checks["ranked rows meet minimum transaction count"] = (
            mart.filter(
                F.col("review_priority_rank").isNotNull()
                & (F.col("transaction_count") < F.col("minimum_transactions_for_rank"))
            )
            .limit(1)
            .count()
            == 0
        )

        # 4. Eligible merchants should all have a rank.
        checks["eligible rows are ranked"] = (
            mart.filter(
                (F.col("transaction_count") >= F.col("minimum_transactions_for_rank"))
                & F.col("review_priority_rank").isNull()
            )
            .limit(1)
            .count()
            == 0
        )

        # 5. Ranks must be unique within each currency.
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

        print(f"Total rows: {total_rows:,}")
        print(f"Ranked rows: {ranked_rows:,}")
        print(f"Unranked rows: {unranked_rows:,}")
        print("\nQuality checks:")

        for name, passed in checks.items():
            print(f"{'PASS' if passed else 'FAIL'}: {name}")

        if not all(checks.values()):
            raise ValueError("One or more merchant mart checks failed.")

        print("\nAll merchant mart quality checks passed.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
