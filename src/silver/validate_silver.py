"""Read-only data-quality validation for StreamGuard Silver Delta tables."""

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

# These string fields must not be empty or contain only whitespace.
NON_BLANK_COLUMNS = {
    "transactions": ["currency"],
    "customers": ["account_status"],
    "merchants": ["merchant_name", "merchant_category"],
}

REQUIRED_LINEAGE_COLUMNS = [
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
]

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
    silver_tables = {}

    try:
        for table_name, primary_key in TABLES.items():
            path = f"{SILVER_ROOT}/{table_name}"

            print(f"\n=== Validate Silver: {table_name} ===", flush=True)

            # 1. Confirm that the table exists and is a Delta table.
            if not DeltaTable.isDeltaTable(spark, path):
                print(f"FAIL: Not a Delta table: {path}", flush=True)
                failures.append(table_name)
                continue

            df = spark.read.format("delta").load(path)

            # 2. Confirm that the columns required by these checks exist.
            required_schema = (
                {primary_key, "is_deleted"}
                | set(REQUIRED_COLUMNS[table_name])
                | set(REQUIRED_LINEAGE_COLUMNS)
            )
            missing_columns = sorted(required_schema - set(df.columns))

            if missing_columns:
                print(
                    f"FAIL: Missing columns: {', '.join(missing_columns)}",
                    flush=True,
                )
                failures.append(table_name)
                continue

            silver_tables[table_name] = df

            # 3. Table-level and primary-key checks.
            total_rows = df.count()
            null_keys = df.filter(F.col(primary_key).isNull()).count()

            duplicate_keys = df.groupBy(primary_key).count().filter(F.col("count") > 1).count()

            # Null is_deleted values are treated as active here, but they
            # are independently rejected by the delete-flag check below.
            active_df = df.filter(~F.coalesce(F.col("is_deleted"), F.lit(False)))

            # 4. Required business fields must be populated on active rows.
            null_required_columns = {
                column: active_df.filter(F.col(column).isNull()).count()
                for column in REQUIRED_COLUMNS[table_name]
            }

            # 5. Required business strings must not be blank.
            blank_required_columns = {
                column: active_df.filter(
                    F.col(column).isNotNull() & (F.length(F.trim(F.col(column))) == 0)
                ).count()
                for column in NON_BLANK_COLUMNS[table_name]
            }

            # 6. Check Debezium CDC operation codes.
            invalid_operations = df.filter(
                F.col("operation").isNull() | ~F.col("operation").isin(*VALID_OPERATIONS)
            ).count()

            # 7. The delete flag must agree with the operation.
            expected_deleted = F.col("operation") == F.lit("d")

            inconsistent_delete_flags = df.filter(
                F.col("is_deleted").isNull()
                | expected_deleted.isNull()
                | (F.col("is_deleted") != expected_deleted)
            ).count()

            # 8. Validate source lineage metadata.
            invalid_topics = df.filter(
                F.col("kafka_topic").isNull() | (F.length(F.trim(F.col("kafka_topic"))) == 0)
            ).count()

            invalid_partitions = df.filter(
                F.col("kafka_partition").isNull() | (F.col("kafka_partition") < 0)
            ).count()

            invalid_offsets = df.filter(
                F.col("kafka_offset").isNull() | (F.col("kafka_offset") < 0)
            ).count()

            missing_kafka_timestamps = df.filter(F.col("kafka_timestamp").isNull()).count()

            missing_ingestion_timestamps = df.filter(F.col("ingested_at").isNull()).count()

            # Each Kafka topic/partition/offset combination should identify
            # one source message within this entity's Silver table.
            duplicate_lineage_keys = (
                df.groupBy("kafka_topic", "kafka_partition", "kafka_offset")
                .count()
                .filter(F.col("count") > 1)
                .count()
            )

            # 9. Print the results.
            print(f"Rows: {total_rows}", flush=True)
            print(f"Null primary keys: {null_keys}", flush=True)
            print(f"Duplicate primary keys: {duplicate_keys}", flush=True)

            for column, count in null_required_columns.items():
                print(f"Null {column} (active rows): {count}", flush=True)

            for column, count in blank_required_columns.items():
                print(f"Blank {column} (active rows): {count}", flush=True)

            print(f"Invalid operations: {invalid_operations}", flush=True)
            print(
                f"Inconsistent delete flags: {inconsistent_delete_flags}",
                flush=True,
            )
            print(f"Invalid Kafka topics: {invalid_topics}", flush=True)
            print(f"Invalid Kafka partitions: {invalid_partitions}", flush=True)
            print(f"Invalid Kafka offsets: {invalid_offsets}", flush=True)
            print(
                f"Missing Kafka timestamps: {missing_kafka_timestamps}",
                flush=True,
            )
            print(
                f"Missing ingestion timestamps: {missing_ingestion_timestamps}",
                flush=True,
            )
            print(
                f"Duplicate Kafka message coordinates: {duplicate_lineage_keys}",
                flush=True,
            )

            # 10. Fail the table if any required check fails.
            table_failed = (
                total_rows == 0
                or null_keys > 0
                or duplicate_keys > 0
                or any(null_required_columns.values())
                or any(blank_required_columns.values())
                or invalid_operations > 0
                or inconsistent_delete_flags > 0
                or invalid_topics > 0
                or invalid_partitions > 0
                or invalid_offsets > 0
                or missing_kafka_timestamps > 0
                or missing_ingestion_timestamps > 0
                or duplicate_lineage_keys > 0
            )

            if table_failed:
                failures.append(table_name)
                print("FAIL: Silver data-quality checks failed.", flush=True)
            else:
                print("PASS: All Silver data-quality checks passed.", flush=True)

        # 11. Check referential integrity for active transactions.
        # Deleted dimension records are retained as soft-delete rows, so
        # reference existence is checked against all dimension keys.
        if all(name in silver_tables for name in TABLES):
            transactions = silver_tables["transactions"]
            customers = silver_tables["customers"]
            merchants = silver_tables["merchants"]

            active_transactions = transactions.filter(
                ~F.coalesce(F.col("is_deleted"), F.lit(False))
            )

            customer_keys = customers.select("customer_id").distinct()
            merchant_keys = merchants.select("merchant_id").distinct()

            missing_customers = (
                active_transactions.select("transaction_id", "customer_id")
                .join(customer_keys, on="customer_id", how="left_anti")
                .count()
            )

            missing_merchants = (
                active_transactions.select("transaction_id", "merchant_id")
                .join(merchant_keys, on="merchant_id", how="left_anti")
                .count()
            )

            print("\n=== Validate Silver: transaction references ===", flush=True)
            print(f"Transactions with missing customers: {missing_customers}", flush=True)
            print(f"Transactions with missing merchants: {missing_merchants}", flush=True)

            if missing_customers > 0 or missing_merchants > 0:
                failures.append("transaction_references")
                print("FAIL: Referential integrity checks failed.", flush=True)
            else:
                print("PASS: Transaction references are valid.", flush=True)

        if failures:
            raise RuntimeError(f"Silver validation failed for: {', '.join(failures)}")

        print("\nAll Silver data-quality checks passed.", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
