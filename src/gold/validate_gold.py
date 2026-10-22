"""Read-only quality checks for the StreamGuard Gold star schema."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GOLD_BASE = "/opt/streamguard/lake/gold"


def require_no_rows(df, check_name: str) -> None:
    """Fail if a quality check finds any invalid rows."""
    if df.limit(1).count() > 0:
        raise ValueError(f"FAIL: {check_name}")
    print(f"PASS: {check_name}", flush=True)


def main() -> None:
    spark = SparkSession.builder.appName("StreamGuardGoldValidation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        customer = spark.read.format("delta").load(f"{GOLD_BASE}/dim_customer")
        merchant = spark.read.format("delta").load(f"{GOLD_BASE}/dim_merchant")
        date = spark.read.format("delta").load(f"{GOLD_BASE}/dim_date")
        fact = spark.read.format("delta").load(f"{GOLD_BASE}/fact_transactions")

        tables = {
            "dim_customer": (customer, "customer_key"),
            "dim_merchant": (merchant, "merchant_key"),
            "dim_date": (date, "date_key"),
            "fact_transactions": (fact, "transaction_id"),
        }

        print("Gold table row counts:", flush=True)
        for name, (df, key) in tables.items():
            count = df.count()
            print(f"  {name}: {count:,}", flush=True)

            require_no_rows(
                df.filter(F.col(key).isNull()),
                f"{name} has no null {key}",
            )
            require_no_rows(
                df.groupBy(key).count().filter(F.col("count") > 1),
                f"{name} has unique {key}",
            )

        require_no_rows(
            fact.filter(
                F.col("customer_key").isNull()
                | F.col("merchant_key").isNull()
                | F.col("date_key").isNull()
                | F.col("amount").isNull()
                | F.col("currency").isNull()
            ),
            "fact rows have required keys and amounts",
        )
        require_no_rows(
            fact.filter(F.col("amount") < 0),
            "fact transaction amounts are non-negative",
        )

        require_no_rows(
            fact.filter(
                (F.length(F.trim(F.col("currency"))) == 0)
                | (F.length(F.trim(F.col("transaction_status"))) == 0)
            ),
            "fact currency and transaction status are non-blank",
        )

        require_no_rows(
            fact.join(
                customer.select("customer_key"),
                "customer_key",
                "left_anti",
            ),
            "all fact customer keys exist in dim_customer",
        )

        require_no_rows(
            fact.join(
                merchant.select("merchant_key"),
                "merchant_key",
                "left_anti",
            ),
            "all fact merchant keys exist in dim_merchant",
        )

        require_no_rows(
            fact.join(
                date.select("date_key"),
                "date_key",
                "left_anti",
            ),
            "all fact date keys exist in dim_date",
        )

        print("\nAll Gold star-schema quality checks passed.", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
