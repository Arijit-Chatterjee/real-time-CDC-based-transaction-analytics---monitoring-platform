from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GOLD_PATH = "/opt/streamguard/lake/gold/fact_transactions"


def main():
    spark = (
        SparkSession.builder
        .appName("StreamGuardLatestTransactions")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        transactions = spark.read.format("delta").load(GOLD_PATH)

        print("\n=== Latest 3 StreamGuard Transactions ===")

        (
            transactions
            .orderBy(F.col("event_time").desc_nulls_last())
            .limit(3)
            .show(truncate=False, vertical=True)
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()