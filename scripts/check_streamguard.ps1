$ErrorActionPreference = "Stop"

$expected = @(
    "streamguard-postgres",
    "streamguard-kafka",
    "streamguard-debezium",
    "streamguard-debezium-ui",
    "streamguard-kafka-ui",
    "streamguard-spark",
    "streamguard-airflow"
)

$failed = 0

foreach ($name in $expected) {
    $status = docker inspect --format '{{.State.Status}}' $name 2>$null

    if ($LASTEXITCODE -eq 0 -and $status -eq "running") {
        Write-Host "[PASS] $name is running" -ForegroundColor Green
    }
    else {
        Write-Host "[FAIL] $name is not running" -ForegroundColor Red
        $failed++
    }
}
$endpoints = @(
    @{ Name = "Debezium REST API"; Url = "http://localhost:8083/" },
    @{ Name = "Airflow Web UI"; Url = "http://localhost:8082/" }
)

foreach ($endpoint in $endpoints) {
    try {
        $response = Invoke-WebRequest `
            -Uri $endpoint.Url `
            -UseBasicParsing `
            -TimeoutSec 5

        if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 400) {
            Write-Host "[PASS] $($endpoint.Name) returned HTTP $($response.StatusCode)" -ForegroundColor Green
        }
        else {
            Write-Host "[FAIL] $($endpoint.Name) returned HTTP $($response.StatusCode)" -ForegroundColor Red
            $failed++
        }
    }
    catch {
        Write-Host "[FAIL] $($endpoint.Name) is unreachable: $($_.Exception.Message)" -ForegroundColor Red
        $failed++
    }
}
Write-Host ""
if ($failed -eq 0) {
    Write-Host "StreamGuard health check passed: all checks succeeded."
    exit 0
}
else {
    Write-Host "StreamGuard health check failed: $failed check(s) failed."
    exit 1
}
