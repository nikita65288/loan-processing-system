# Idempotent local DLT provisioning against the Docker Compose KRaft broker.
# Run from any Windows PowerShell working directory after docker compose up -d kafka.
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
Set-Location $root
$sourceTopics = @(
    'loan.application.created',
    'loan.scoring.requested', 'loan.scoring.completed', 'loan.scoring.failed',
    'loan.bureau.check.requested', 'loan.bureau.check.completed', 'loan.bureau.check.failed',
    'loan.decision.requested', 'loan.decision.completed', 'loan.decision.failed',
    'loan.disbursement.requested', 'loan.disbursement.completed', 'loan.disbursement.failed',
    'loan.disbursement.status.inquiry.requested', 'loan.disbursement.status.inquiry.result',
    'loan.notification.requested', 'loan.notification.completed', 'loan.notification.failed'
)
foreach ($topic in $sourceTopics) {
    $dlt = "$topic.DLT"
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server kafka:29092 --create --if-not-exists `
        --topic $dlt --partitions 3 --replication-factor 1 `
        --config retention.ms=1209600000
    if ($LASTEXITCODE -ne 0) { throw "Failed to provision $dlt" }
    Write-Host "Ready: $dlt"
}
Write-Host 'DLT provision complete; existing topic partition/retention settings were NOT modified.'
