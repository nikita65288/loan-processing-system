# load testing: one command = k6 Kubernetes Job + read-only SQL reconciliation + Markdown report.
# Unlike a manual sequence, reconciliation also runs after a failed k6 threshold.
param(
    [ValidateSet('smoke','baseline','ramp-up','target','steady-state','spike','stress','soak','cache-read')]
    [string]$Scenario = 'smoke',
    [ValidateRange(1,1000)][int]$Rate = 2,
    [ValidateRange(1,1000)][int]$MaxRate = 100,
    [ValidateRange(1,3600)][int]$DurationSeconds = 20,
    [ValidateRange(1,3600)][int]$MaxDurationSeconds = 600,
    [ValidateRange(1,1000)][int]$Vus = 10,
    [ValidateRange(1,1000)][int]$MaxVus = 50,
    [ValidateRange(1,120)][int]$TimeoutSeconds = 5,
    [string]$ApplicationId = '',
    [string]$Seed = '',
    [ValidatePattern('^[0-9a-fA-F]{8}$')][string]$RunId = '',
    [ValidateRange(0,3600)][int]$DrainTimeoutSeconds = 300,
    [ValidateRange(5,120)][int]$PollSeconds = 15
)
$ErrorActionPreference = 'Stop'
if (-not $RunId) { $RunId = [guid]::NewGuid().ToString('N').Substring(0,8) }
$RunId = $RunId.ToLowerInvariant()
if ($RunId -eq '00000000') { throw 'Invalid RunId.' }
$runnerArgs = @{ Scenario=$Scenario; RunId=$RunId }
# Forward only explicitly supplied knobs. The existing k6 runner selects per-scenario defaults.
foreach ($key in @('Rate','MaxRate','DurationSeconds','MaxDurationSeconds','Vus','MaxVus','TimeoutSeconds','ApplicationId','Seed')) {
    if ($PSBoundParameters.ContainsKey($key)) { $runnerArgs[$key] = $PSBoundParameters[$key] }
}
$runnerError = ''
# Snapshot the Kubernetes pods before load so restart counts can be compared later.
$runFolder = Join-Path $PSScriptRoot "results/$Scenario-$RunId"
New-Item -ItemType Directory -Force -Path $runFolder | Out-Null
try {
    $beforePods = & kubectl --request-timeout=10s -n lps get pods -o wide 2>&1
    if ($LASTEXITCODE -eq 0) {
        [System.IO.File]::WriteAllText((Join-Path $runFolder 'pods-before.txt'), ($beforePods -join "`n"), [System.Text.UTF8Encoding]::new($false))
    }
} catch { Write-Warning 'Could not capture pre-run pods; continuing without this optional snapshot.' }
Write-Host "=== load testing: running $Scenario, RUN_ID=$RunId ==="
try {
    & (Join-Path $PSScriptRoot 'run-k8s.ps1') @runnerArgs
} catch {
    $runnerError = $_.Exception.Message
    Write-Warning "k6 Job/threshold failure: $runnerError. Continuing with best-effort SQL report."
}
Write-Host "=== load testing: collecting SQL + report for RUN_ID=$RunId ==="
try {
    & (Join-Path $PSScriptRoot 'collect-report.ps1') -RunId $RunId -Scenario $Scenario `
        -DrainTimeoutSeconds $DrainTimeoutSeconds -PollSeconds $PollSeconds -RunnerError $runnerError
} catch {
    Write-Error "Report collector failed: $($_.Exception.Message). RUN_ID=$RunId. Re-run collect-report.ps1 for this ID."
    exit 2
}
if ($runnerError) {
    Write-Warning 'The report was saved but k6 runner failed; inspect the report instead of assuming PASS.'
    exit 1
}
Write-Host 'Completed: review load-tests/results/<scenario>-<RUN_ID>/report.md before running the next scenario.'
