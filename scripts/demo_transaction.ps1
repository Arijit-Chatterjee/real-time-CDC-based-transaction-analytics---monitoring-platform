$ErrorActionPreference = "Stop"

$PostgresContainer = "streamguard-postgres"
$transactionId = [guid]::NewGuid().ToString()

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
    4321.00,
    'INR',
    NOW(),
    NOW(),
    NOW(),
    'Kolkata',
    'approved'
)
RETURNING transaction_id, amount, currency, event_time, region, status;
"@

Write-Host "`n=== StreamGuard Transaction Demo ===" -ForegroundColor Cyan
Write-Host "Inserting transaction into PostgreSQL..."

$sql | docker exec -i $PostgresContainer sh -lc 'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'

if ($LASTEXITCODE -ne 0) {
    throw "Transaction insert failed."
}

Write-Host "`nTransaction inserted: $transactionId" -ForegroundColor Green
Write-Host "Allow the streaming pipeline to process it, then run the Gold viewer."
Write-Host "This script does not start Spark jobs or poll the Gold table."
