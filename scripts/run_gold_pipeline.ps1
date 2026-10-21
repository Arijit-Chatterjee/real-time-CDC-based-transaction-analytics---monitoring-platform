
$ErrorActionPreference = "Stop"

$steps = @(
    @{
        Name   = "Build Gold star schema"
        Script = "/opt/streamguard/src/gold/build_gold.py"
    },
    @{
        Name   = "Validate Gold star schema"
        Script = "/opt/streamguard/src/gold/validate_gold.py"
    },
    @{
        Name   = "Build merchant performance mart"
        Script = "/opt/streamguard/src/gold/build_merchant_performance.py"
    },
    @{
        Name   = "Validate merchant performance mart"
        Script = "/opt/streamguard/src/gold/validate_merchant_performance.py"
    }
)

foreach ($step in $steps) {
    Write-Host "`n=== $($step.Name) ===" -ForegroundColor Cyan

    $bashCommand = 'export HOME=/tmp/streamguard-home; export PYTHONPATH=$(printf "%s:" /opt/spark/python/lib/*.zip); /opt/spark/bin/spark-submit --master "local[2]" --packages "io.delta:delta-spark_4.2_2.13:4.4.1" --conf spark.jars.ivy=/tmp/streamguard-ivy --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "{0}"' -f $step.Script

    docker compose exec -T spark bash -lc "$bashCommand"
    $stepExitCode = $LASTEXITCODE

    if ($stepExitCode -ne 0) {
        Write-Host "FAILED: $($step.Name). Exit code: $stepExitCode" -ForegroundColor Red
        exit $stepExitCode
    }
}

Write-Host "`nGold pipeline completed successfully." -ForegroundColor Green
