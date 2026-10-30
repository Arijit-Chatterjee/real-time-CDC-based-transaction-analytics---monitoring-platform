$ErrorActionPreference = "Stop"

$failed = 0

# Core services required for StreamGuard streaming.
$requiredContainers = @(
    "streamguard-postgres",
    "streamguard-kafka",
    "streamguard-debezium",
    "streamguard-spark"
)

# Optional services that can remain stopped to save memory.
$optionalContainers = @(
    "streamguard-debezium-ui",
    "streamguard-kafka-ui",
    "streamguard-airflow"
)

function Test-Container {
    param(
        [string]$Name,
        [bool]$Required
    )

    $status = docker inspect --format '{{.State.Status}}' $Name 2>$null

    if ($LASTEXITCODE -eq 0 -and "$status".Trim() -eq "running") {
        Write-Host "[PASS] $Name is running" -ForegroundColor Green
    }
    elseif ($Required) {
        Write-Host "[FAIL] $Name is not running" -ForegroundColor Red
        $script:failed++
    }
    else {
        Write-Host "[INFO] $Name is stopped (optional)" -ForegroundColor Yellow
    }
}

function Test-TcpPort {
    param(
        [string]$ComputerName,
        [int]$Port
    )

    $client = New-Object System.Net.Sockets.TcpClient

    try {
        $connection = $client.BeginConnect(
            $ComputerName,
            $Port,
            $null,
            $null
        )

        if (-not $connection.AsyncWaitHandle.WaitOne(2000, $false)) {
            return $false
        }

        $client.EndConnect($connection)
        return $true
    }
    catch {
        return $false
    }
    finally {
        $client.Close()
    }
}

Write-Host "`n=== Core Docker Services ===" -ForegroundColor Cyan

foreach ($name in $requiredContainers) {
    Test-Container -Name $name -Required $true
}

Write-Host "`n=== Optional Services ===" -ForegroundColor Cyan

foreach ($name in $optionalContainers) {
    Test-Container -Name $name -Required $false
}

Write-Host "`n=== PostgreSQL and Kafka Connectivity ===" -ForegroundColor Cyan

$ports = @(
    @{ Name = "PostgreSQL"; Port = 5433 },
    @{ Name = "Kafka"; Port = 9092 }
)

foreach ($service in $ports) {
    if (Test-TcpPort -ComputerName "127.0.0.1" -Port $service.Port) {
        Write-Host "[PASS] $($service.Name) port $($service.Port) is reachable" -ForegroundColor Green
    }
    else {
        Write-Host "[FAIL] $($service.Name) port $($service.Port) is unreachable" -ForegroundColor Red
        $failed++
    }
}

Write-Host "`n=== Debezium Connector Health ===" -ForegroundColor Cyan

try {
    $response = Invoke-RestMethod `
        -Uri "http://localhost:8083/connectors/streamguard-postgres-connector/status" `
        -TimeoutSec 5

    $connectorState = $response.connector.state
    $taskStates = @($response.tasks | ForEach-Object { $_.state })

    if (
        $connectorState -eq "RUNNING" -and
        $taskStates.Count -gt 0 -and
        @($taskStates | Where-Object { $_ -ne "RUNNING" }).Count -eq 0
    ) {
        Write-Host "[PASS] Debezium connector is RUNNING" -ForegroundColor Green
        Write-Host "[PASS] All $($taskStates.Count) connector task(s) are RUNNING" -ForegroundColor Green
    }
    else {
        Write-Host "[FAIL] Debezium connector or tasks are unhealthy" -ForegroundColor Red
        Write-Host "Connector: $connectorState"
        Write-Host "Tasks: $($taskStates -join ', ')"
        $failed++
    }
}
catch {
    Write-Host "[FAIL] Debezium REST API is unreachable: $($_.Exception.Message)" -ForegroundColor Red
    $failed++
}

Write-Host "`n=== Optional Airflow Web UI ===" -ForegroundColor Cyan

$airflowStatus = docker inspect --format '{{.State.Status}}' streamguard-airflow 2>$null

if ($LASTEXITCODE -eq 0 -and "$airflowStatus".Trim() -eq "running") {
    try {
        $response = Invoke-WebRequest `
            -Uri "http://localhost:8082/" `
            -UseBasicParsing `
            -TimeoutSec 5

        Write-Host "[PASS] Airflow UI returned HTTP $($response.StatusCode)" -ForegroundColor Green
    }
    catch {
        Write-Host "[FAIL] Airflow is running but its UI is unreachable" -ForegroundColor Red
        $failed++
    }
}
else {
    Write-Host "[INFO] Airflow UI check skipped (Airflow is stopped)" -ForegroundColor Yellow
}

Write-Host ""

if ($failed -eq 0) {
    Write-Host "StreamGuard health check passed." -ForegroundColor Green
    exit 0
}
else {
    Write-Host "StreamGuard health check failed: $failed check(s) failed." -ForegroundColor Red
    exit 1
}
