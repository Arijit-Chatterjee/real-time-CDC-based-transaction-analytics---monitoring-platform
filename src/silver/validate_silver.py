"""Read-only validation of StreamGuard Silver Delta tables."""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

SILVER_ROOT = "/opt/streamguard/lake/silver"

TABLES = {
    "transactions": "transaction_id",
    "customers": "customer_id",
    "merchants": "merchant_id",
}


def main() -> None:
    spark = (
        SparkSession.builder.appName("ValidateStreamGuardSilver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    failures = []

    try:
        for table_name, primary_key in TABLES.items():
            path = f"{SILVER_ROOT}/{table_name}"

            print(f"\n=== Validate Silver: {table_name} ===", flush=True)

            if not DeltaTable.isDeltaTable(spark, path):
                print(f"FAIL: Not a Delta table: {path}", flush=True)
                failures.append(table_name)
                continue

            df = spark.read.format("delta").load(path)
            total_rows = df.count()
            null_keys = df.filter(df[primary_key].isNull()).count()
            duplicate_keys = df.groupBy(primary_key).count().filter("count > 1").count()

            print(f"Rows: {total_rows}", flush=True)
            print(f"Null primary keys: {null_keys}", flush=True)
            print(f"Duplicate primary keys: {duplicate_keys}", flush=True)
            print("Schema:", flush=True)
            df.printSchema()

            if null_keys or duplicate_keys:
                failures.append(table_name)
                print("FAIL: Primary-key quality checks failed.", flush=True)
            else:
                print("PASS: Primary-key quality checks passed.", flush=True)

        if failures:
            raise RuntimeError(f"Silver validation failed for: {', '.join(failures)}")

        print("\nAll Silver tables passed validation.", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
