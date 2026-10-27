
$ErrorActionPreference = "Stop"

$SparkContainer = "streamguard-spark"

# These services must be running before the streaming writers can work.
$RequiredContainers = @(
    "streamguard-postgres",
    "streamguard-kafka",
    "streamguard-debezium",
    "streamguard-spark"
)

foreach ($container in $RequiredContainers) {
    $stateOutput = & docker inspect --format '{{.State.Running}}' $container 2>$null
    $dockerExitCode = $LASTEXITCODE
    $state = (@($stateOutput) -join "`n").Trim()

    if ($dockerExitCode -ne 0 -or $state -ne "true") {
        throw "Required container '$container' is not running. Start the main Docker Compose services first."
    }
}

# Check the actual process and, for Bronze, its Kafka topic.
# This also recognizes jobs started manually before this script existed.
$checkCode = @'
import glob
import os
import sys

script_path, topic = sys.argv[1], sys.argv[2]

for proc_dir in glob.glob("/proc/[0-9]*"):
    try:
        pid = int(proc_dir.rsplit("/", 1)[-1])
        if pid == os.getpid():
            continue

        with open(os.path.join(proc_dir, "comm"), encoding="utf-8") as f:
            comm = f.read().strip()

        if comm != "java" and not comm.startswith("python"):
            continue

        with open(os.path.join(proc_dir, "cmdline"), "rb") as f:
            command_line = f.read().replace(b"\0", b" ").decode(errors="ignore")

        if script_path not in command_line:
            continue

        if topic:
            with open(os.path.join(proc_dir, "environ"), "rb") as f:
                environment = f.read().split(b"\0")

            expected_topic = f"STREAMGUARD_KAFKA_TOPIC={topic}".encode()
            if expected_topic not in environment:
                continue

        print(f"RUNNING pid={pid} process={comm}")
        sys.exit(0)

    except (OSError, ValueError):
        continue

print("MISSING")
'@

$EncodedCheck = [Convert]::ToBase64String(
    [System.Text.Encoding]::UTF8.GetBytes($checkCode)
)

function Get-WriterStatus {
    param(
        [Parameter(Mandatory)]
        [string]$ScriptPath,

        [string]$Topic = ""
    )

    $checkCommand = "printf '%s' '$script:EncodedCheck' | base64 -d | python3 - '$ScriptPath' '$Topic'"

    $output = & docker exec $script:SparkContainer bash -lc $checkCommand
    $exitCode = $LASTEXITCODE

    if ($exitCode -ne 0) {
        throw "Could not inspect Spark processes. Check Docker and the Spark container."
    }

    $result = (@($output) -join "`n").Trim()

    if ([string]::IsNullOrWhiteSpace($result)) {
        throw "The process check returned no result for '$ScriptPath'."
    }

    return $result
}

$Writers = @(
    [pscustomobject]@{
        Name = "Bronze transactions"
        ScriptPath = "/opt/streamguard/src/streaming/bronze_writer.py"
        Topic = "streamguard.public.transactions"
        LogPath = "/tmp/bronze_transactions_recovery.log"
        LaunchCommand = 'mkdir -p /tmp/streamguard-home/.ivy2.5.2; export HOME=/tmp/streamguard-home; export PYTHONPATH="/opt/streamguard:$(printf "%s:" /opt/spark/python/lib/*.zip)"; export STREAMGUARD_KAFKA_BOOTSTRAP=kafka:29092; export STREAMGUARD_KAFKA_TOPIC=streamguard.public.transactions; /opt/spark/bin/spark-submit --master "local[1]" --conf spark.jars.ivy=/tmp/streamguard-home/.ivy2.5.2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /opt/streamguard/src/streaming/bronze_writer.py > /tmp/bronze_transactions_recovery.log 2>&1'
    },
    [pscustomobject]@{
        Name = "Bronze customers"
        ScriptPath = "/opt/streamguard/src/streaming/bronze_writer.py"
        Topic = "streamguard.public.customers"
        LogPath = "/tmp/bronze_customers_recovery.log"
        LaunchCommand = 'mkdir -p /tmp/streamguard-home/.ivy2.5.2; export HOME=/tmp/streamguard-home; export PYTHONPATH="/opt/streamguard:$(printf "%s:" /opt/spark/python/lib/*.zip)"; export STREAMGUARD_KAFKA_BOOTSTRAP=kafka:29092; export STREAMGUARD_KAFKA_TOPIC=streamguard.public.customers; /opt/spark/bin/spark-submit --master "local[1]" --conf spark.jars.ivy=/tmp/streamguard-home/.ivy2.5.2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /opt/streamguard/src/streaming/bronze_writer.py > /tmp/bronze_customers_recovery.log 2>&1'
    },
    [pscustomobject]@{
        Name = "Bronze merchants"
        ScriptPath = "/opt/streamguard/src/streaming/bronze_writer.py"
        Topic = "streamguard.public.merchants"
        LogPath = "/tmp/bronze_merchants_recovery.log"
        LaunchCommand = 'mkdir -p /tmp/streamguard-home/.ivy2.5.2; export HOME=/tmp/streamguard-home; export PYTHONPATH="/opt/streamguard:$(printf "%s:" /opt/spark/python/lib/*.zip)"; export STREAMGUARD_KAFKA_BOOTSTRAP=kafka:29092; export STREAMGUARD_KAFKA_TOPIC=streamguard.public.merchants; /opt/spark/bin/spark-submit --master "local[1]" --conf spark.jars.ivy=/tmp/streamguard-home/.ivy2.5.2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /opt/streamguard/src/streaming/bronze_writer.py > /tmp/bronze_merchants_recovery.log 2>&1'
    },
    [pscustomobject]@{
        Name = "Silver"
        ScriptPath = "/opt/streamguard/src/silver/silver_writer.py"
        Topic = ""
        LogPath = "/tmp/silver_writer_recovery.log"
        LaunchCommand = 'cd /opt/streamguard; mkdir -p /tmp/streamguard-ivy; export HOME=/tmp/streamguard-home; export PYTHONPATH="/opt/streamguard:$(printf "%s:" /opt/spark/python/lib/*.zip)"; /opt/spark/bin/spark-submit --master "local[1]" --packages "io.delta:delta-spark_4.2_2.13:4.4.1" --conf spark.jars.ivy=/tmp/streamguard-ivy --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /opt/streamguard/src/silver/silver_writer.py > /tmp/silver_writer_recovery.log 2>&1'
    },
    [pscustomobject]@{
        Name = "Gold incremental"
        ScriptPath = "/opt/streamguard/src/gold/gold_incremental_writer.py"
        Topic = ""
        LogPath = "/tmp/gold_incremental_writer_recovery.log"
        LaunchCommand = 'cd /opt/streamguard; mkdir -p /tmp/streamguard-ivy; export HOME=/tmp/streamguard-home; export PYTHONPATH="/opt/streamguard:$(printf "%s:" /opt/spark/python/lib/*.zip)"; /opt/spark/bin/spark-submit --master "local[1]" --packages "io.delta:delta-spark_4.2_2.13:4.4.1" --conf spark.jars.ivy=/tmp/streamguard-ivy --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /opt/streamguard/src/gold/gold_incremental_writer.py > /tmp/gold_incremental_writer_recovery.log 2>&1'
    }
)

Write-Host "`n=== StreamGuard Streaming Writers ===" -ForegroundColor Cyan

foreach ($writer in $Writers) {
    $status = Get-WriterStatus -ScriptPath $writer.ScriptPath -Topic $writer.Topic

    if ($status.StartsWith("RUNNING")) {
        Write-Host "SKIP    $($writer.Name): already running ($status)" -ForegroundColor DarkGray
        continue
    }

    if ($status -ne "MISSING") {
        throw "Unexpected process-check result for $($writer.Name): $status"
    }

    Write-Host "START   $($writer.Name)..." -ForegroundColor Yellow

    & docker exec -d $SparkContainer bash -lc $writer.LaunchCommand | Out-Null

    if ($LASTEXITCODE -ne 0) {
        throw "Docker could not launch $($writer.Name)."
    }

    # Give Spark time to create the driver process before checking it.
    Start-Sleep -Seconds 10

    $afterStart = Get-WriterStatus -ScriptPath $writer.ScriptPath -Topic $writer.Topic

    if (-not $afterStart.StartsWith("RUNNING")) {
        Write-Warning "$($writer.Name) was launched but is not yet confirmed running. Inspect $($writer.LogPath) before retrying."
        break
    }

    Write-Host "STARTED $($writer.Name): $afterStart" -ForegroundColor Green
}

Write-Host "`nStartup pass finished. Check the reported status and logs for any writer that did not start." -ForegroundColor Cyan
