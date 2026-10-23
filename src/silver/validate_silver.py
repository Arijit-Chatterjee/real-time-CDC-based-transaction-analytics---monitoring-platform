"""Read-only validation of StreamGuard Silver Delta tables."""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_ROOT = "/opt/streamguard/lake/silver"

TABLES = {
    "transactions": "transaction_id",
    "customers": "customer_id",
    "merchants": "merchant_id",
}

REQUIRED_COLUMNS = {
    "transactions": [
        "amount",
        "customer_id",
        "merchant_id",
        "currency",
        "event_time",
        "operation",
    ],
    "customers": [
        "created_at",
        "account_status",
        "operation",
    ],
    "merchants": [
        "created_at",
        "merchant_name",
        "merchant_category",
        "operation",
    ],
}

VALID_OPERATIONS = ("r", "c", "u", "d")


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

            # Primary-key quality checks apply to every row, including deleted records.
            total_rows = df.count()
            null_keys = df.filter(F.col(primary_key).isNull()).count()
            duplicate_keys = (
                df.groupBy(primary_key)
                .count()
                .filter(F.col("count") > 1)
                .count()
            )

            # Treat null deletion flags as active for required-field checks.
            active_df = df.filter(
                ~F.coalesce(F.col("is_deleted"), F.lit(False))
            )

            null_required_columns = {
                column: active_df.filter(F.col(column).isNull()).count()
                for column in REQUIRED_COLUMNS[table_name]
            }

            # Only valid Debezium operation codes are accepted.
            invalid_operations = df.filter(
                F.col("operation").isNull()
                | ~F.col("operation").isin(*VALID_OPERATIONS)
            ).count()

            # A row is deleted if and only if its operation is "d".
            expected_delete_flag = F.coalesce(
                F.col("operation") == F.lit("d"),
                F.lit(False),
            )

            inconsistent_delete_flags = df.filter(
                F.col("is_deleted").isNull()
                | (F.col("is_deleted") != expected_delete_flag)
            ).count()

            print(f"Rows: {total_rows}", flush=True)
            print(f"Null primary keys: {null_keys}", flush=True)
            print(f"Duplicate primary keys: {duplicate_keys}", flush=True)

            for column, count in null_required_columns.items():
                print(f"Null {column} (active rows): {count}", flush=True)

            print(f"Invalid operations: {invalid_operations}", flush=True)
            print(
                f"Inconsistent delete flags: {inconsistent_delete_flags}",
                flush=True,
            )

            print("Schema:", flush=True)
            df.printSchema()

            table_failed = (
                total_rows == 0
                or null_keys > 0
                or duplicate_keys > 0
                or any(null_required_columns.values())
                or invalid_operations > 0
                or inconsistent_delete_flags > 0
            )

            if table_failed:
                failures.append(table_name)
                print("FAIL: One or more Silver data-quality checks failed.", flush=True)
            else:
                print("PASS: All Silver data-quality checks passed.", flush=True)

        if failures:
            raise RuntimeError(
                f"Silver validation failed for: {', '.join(failures)}"
            )

        print("\nAll Silver tables passed validation.", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
