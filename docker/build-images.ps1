param([string]$Tag = 'local-v1')
$ErrorActionPreference = 'Stop'
# Always execute from the repository root, independently of the shell's current directory.
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location $root
$services = @(
    'loan-application-service', 'credit-scoring-service', 'bureau-service',
    'decision-service', 'saga-orchestrator-service',
    'loan-disbursement-service', 'notification-service'
)
$artifacts = Join-Path $root 'docker/artifacts'
New-Item -ItemType Directory -Force -Path $artifacts | Out-Null

foreach ($service in $services) {
    # The reactor has already resolved lps-contracts before this script is executed.
    $jars = @(Get-ChildItem "services/$service/target" -Filter '*.jar' -File |
        Where-Object { $_.Name -notmatch '^(original-)' -and
                       $_.Name -notmatch '(-sources|-javadoc|-tests)\.jar$' })
    if ($jars.Count -ne 1) { throw "Expected one executable JAR for $service; found $($jars.Count)" }

    $artifact = "docker/artifacts/$service.jar"
    Copy-Item $jars[0].FullName $artifact -Force
    docker build --file docker/Dockerfile --build-arg "JAR_FILE=$artifact" `
        --tag "lps/${service}:$Tag" .
    if ($LASTEXITCODE -ne 0) { throw "Docker image build failed: $service" }
}
Write-Host "Built $($services.Count) images tagged $Tag"
