$ErrorActionPreference = "Stop"

$PostgresContainer = "streamguard-postgres"
$transactionId = [guid]::NewGuid().ToString()

# Choose varied, valid synthetic transaction details.
$regions = @(
    "Kolkata",
    "Mumbai",
    "Delhi",
    "Bengaluru",
    "Hyderabad",
    "Chennai",
    "Pune"
)

$statuses = @("approved", "declined")

$region = Get-Random -InputObject $regions
$status = Get-Random -InputObject $statuses

# Generate a positive INR amount from 100.00 through 80,000.00.
# Values above 50,000 can exercise the high-value INR rule.
$amountCents = Get-Random -Minimum 10000 -Maximum 8000001
$amount = ([decimal]$amountCents / 100).ToString(
    "F2",
    [Globalization.CultureInfo]::InvariantCulture
)

$sql = @"
INSERT INTO public.transactions (
    transaction_id,
    customer_id,
    merchant_id,
    amount,
    currency,
    event_time,
    created_at,
    updated_at,
    region,
    status
)
VALUES (
    '$transactionId'::uuid,
    1,
    1,
    $amount,
    'INR',
    NOW(),
    NOW(),
    NOW(),
    '$region',
    '$status'
)
RETURNING
    transaction_id,
    amount,
    currency,
    event_time,
    region,
    status;
"@

Write-Host "`n=== StreamGuard Transaction Demo ===" -ForegroundColor Cyan
Write-Host "Transaction ID: $transactionId"
Write-Host "Amount:         INR $amount"
Write-Host "Region:         $region"
Write-Host "Status:         $status"
Write-Host "`nInserting transaction into PostgreSQL..."

$sql | docker exec -i $PostgresContainer sh -lc 'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'

if ($LASTEXITCODE -ne 0) {
    throw "Transaction insert failed."
}

Write-Host "`nTransaction inserted successfully." -ForegroundColor Green
Write-Host "Wait for Gold to process the event, then check the dashboard."
Write-Host "This script does not launch Spark or poll the Gold table."
