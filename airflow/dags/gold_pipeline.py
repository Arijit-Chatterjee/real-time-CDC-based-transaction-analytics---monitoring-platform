from datetime import datetime
import logging

import docker
from airflow.sdk import dag, task


SPARK_CONTAINER = "streamguard-spark"

SPARK_SUBMIT = (
    "export HOME=/tmp/streamguard-home; "
    'export PYTHONPATH="$(printf "%s:" /opt/spark/python/lib/*.zip)"; '
    '/opt/spark/bin/spark-submit --master "local[2]" '
    '--packages "io.delta:delta-spark_4.2_2.13:4.4.1" '
    "--conf spark.jars.ivy=/tmp/streamguard-ivy "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
)


def run_spark_script(script_path: str) -> None:
    client = docker.from_env()

    try:
        spark = client.containers.get(SPARK_CONTAINER)

        command = f"{SPARK_SUBMIT}{script_path}"
        result = spark.exec_run(["bash", "-lc", command])

        output = result.output
        if isinstance(output, bytes):
            logging.info(output.decode("utf-8", errors="replace"))
        elif output:
            for chunk in output:
                logging.info(chunk.decode("utf-8", errors="replace"))

        if result.exit_code != 0:
            error_output = (
                output.decode("utf-8", errors="replace")
                if isinstance(output, bytes)
                else str(output or "")
            )

            raise RuntimeError(
                f"Spark script failed: {script_path}; "
                f"exit code: {result.exit_code}\n"
                f"Spark output:\n{error_output[-12000:]}"
            )
    finally:
        client.close()


@dag(
    dag_id="streamguard_gold_pipeline",
    description="Build and validate StreamGuard Gold tables and merchant mart",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["streamguard", "gold"],
)
def gold_pipeline():

    @task(retries=1)
    def build_gold():
        run_spark_script("/opt/streamguard/src/gold/build_gold.py")

    @task(retries=1)
    def validate_gold():
        run_spark_script("/opt/streamguard/src/gold/validate_gold.py")

    @task(retries=1)
    def build_merchant_performance():
        run_spark_script("/opt/streamguard/src/gold/build_merchant_performance.py")

    @task(retries=1)
    def validate_merchant_performance():
        run_spark_script("/opt/streamguard/src/gold/validate_merchant_performance.py")

    first = build_gold()
    second = validate_gold()
    third = build_merchant_performance()
    fourth = validate_merchant_performance()

    first >> second >> third >> fourth


gold_pipeline()
