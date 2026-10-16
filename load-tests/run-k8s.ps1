# load testing: safely run a uniquely named k6 Job in Docker Desktop's local Kubernetes.
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
    [string]$RunId = ''
)
$ErrorActionPreference = 'Stop'
$defaultRates = @{'smoke'=2;'baseline'=10;'ramp-up'=100;'target'=500;'steady-state'=20;'spike'=60;'stress'=150;'soak'=10;'cache-read'=20}
$defaultSeconds = @{'smoke'=20;'baseline'=60;'ramp-up'=120;'target'=60;'steady-state'=180;'spike'=60;'stress'=120;'soak'=600;'cache-read'=60}
if (-not $PSBoundParameters.ContainsKey('Rate')) { $Rate = $defaultRates[$Scenario] }
if (-not $PSBoundParameters.ContainsKey('DurationSeconds')) { $DurationSeconds = $defaultSeconds[$Scenario] }
if (-not $PSBoundParameters.ContainsKey('Vus')) { $Vus = [Math]::Max(10, [Math]::Ceiling($Rate / 5.0)) }
if (-not $PSBoundParameters.ContainsKey('MaxVus')) { $MaxVus = [Math]::Max($Vus, [Math]::Ceiling($Rate / 2.0)) }
if ((kubectl config current-context).Trim() -ne 'docker-desktop') {
    throw 'Refusing to create load against non-local kubectl context (expected docker-desktop).'
}
if ($Rate -gt $MaxRate -or $DurationSeconds -gt $MaxDurationSeconds -or $Vus -gt $MaxVus) {
    throw 'Safety limit exceeded: rate/duration/VUs. Explicitly raise matching caps for larger tests.'
}
if (-not $RunId) { $RunId = [guid]::NewGuid().ToString('N').Substring(0, 8) }
$RunId = $RunId.ToLowerInvariant()
if ($RunId -notmatch '^[0-9a-f]{8}$' -or $RunId -eq '00000000') { throw 'RunId must be 8 hex characters.' }
if ($Scenario -eq 'cache-read' -and $ApplicationId -notmatch '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$') {
    throw 'cache-read needs -ApplicationId (a valid application UUID).'
}
if ($Seed -and $Seed -notmatch '^[0-9a-zA-Z_-]{1,32}$') { throw 'Seed must be 1-32 alphanumeric, dash or underscore characters.' }
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$scripts = Join-Path $PSScriptRoot 'scripts'
# Flatten ConfigMap keys and project them into /scripts/lib via the Job volume.items mapping.
$cm = @(
    '-n','lps','create','configmap','lps-k6-scripts',
    "--from-file=load.js=$(Join-Path $scripts 'load.js')",
    "--from-file=config.js=$(Join-Path $scripts 'lib/config.js')",
    "--from-file=data.js=$(Join-Path $scripts 'lib/data.js')",
    '--dry-run=client','-o','yaml'
)
& kubectl @cm | kubectl apply -f - | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'Failed to apply ConfigMap.' }
$yaml = Get-Content -Raw -Encoding UTF8 (Join-Path $repo 'k8s/performance/k6-job.yaml')
$job = "lps-k6-$RunId"
$yaml = $yaml.Replace('name: lps-k6-smoke',"name: $job")
$values = @{
    RUN_ID=$RunId; SCENARIO=$Scenario; RATE="$Rate"; MAX_RATE="$MaxRate"
    DURATION_SECONDS="$DurationSeconds"; MAX_DURATION_SECONDS="$MaxDurationSeconds"
    VUS="$Vus"; MAX_VUS="$MaxVus"; TIMEOUT_SECONDS="$TimeoutSeconds"
    SEED=$Seed; APPLICATION_ID=$ApplicationId
}
foreach ($key in $values.Keys) {
    $regex = "(?m)(- name: $key\r?\n\s*value:\s*)[^\r\n]*"
    if ([regex]::Matches($yaml,$regex).Count -ne 1) { throw "Missing or ambiguous YAML env: $key" }
    $newValue = "'" + $values[$key] + "'"
    $yaml = [regex]::Replace($yaml, $regex, [System.Text.RegularExpressions.MatchEvaluator]{
        param($m) $m.Groups[1].Value + $newValue
    })
}
$tmp = New-TemporaryFile
try {
    [System.IO.File]::WriteAllText($tmp.FullName, $yaml, [System.Text.UTF8Encoding]::new($false))
    kubectl apply -f $tmp.FullName
    if ($LASTEXITCODE -ne 0) { throw 'Failed to create load Job.' }
} finally { Remove-Item $tmp.FullName -ErrorAction SilentlyContinue }
Write-Host "K6 Job $job started; RUN_ID=$RunId"
$resultsDir = Join-Path $PSScriptRoot 'results'
New-Item -ItemType Directory -Path $resultsDir -Force | Out-Null

# A failed k6 threshold marks the Kubernetes Job Failed, never Complete.
# Poll both terminal conditions; always collect logs and the report before reporting failure.
$deadline = [DateTime]::UtcNow.AddSeconds($DurationSeconds + 180)
$jobState = 'TIMEOUT'
do {
    $jobJson = & kubectl -n lps get "job/$job" -o json 2>&1
    if ($LASTEXITCODE -ne 0) { throw "Cannot inspect Job ${job}: $($jobJson -join ' ')" }
    $status = ($jobJson -join "`n") | ConvertFrom-Json
    $conditions = @($status.status.conditions)
    if (@($conditions | Where-Object { $_.type -eq 'Complete' -and $_.status -eq 'True' }).Count -gt 0) {
        $jobState = 'COMPLETE'
        break
    }
    if (@($conditions | Where-Object { $_.type -eq 'Failed' -and $_.status -eq 'True' }).Count -gt 0) {
        $jobState = 'FAILED'
        break
    }
    Start-Sleep -Seconds 2
} while ([DateTime]::UtcNow -lt $deadline)

$podName = & kubectl -n lps get pods -l "job-name=$job" -o 'jsonpath={.items[0].metadata.name}' 2>&1
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($podName)) {
    throw "Job $job is $jobState; could not find Pod. Inspect: kubectl -n lps describe job $job"
}
$logLines = & kubectl -n lps logs "pod/$podName" 2>&1
if ($LASTEXITCODE -ne 0) { throw "Could not fetch Pod logs for $podName" }
$logText = $logLines -join "`n"
$utf8 = [System.Text.UTF8Encoding]::new($false)
$logPath = Join-Path $resultsDir "$RunId-$Scenario-k6.log"
[System.IO.File]::WriteAllText($logPath, $logText, $utf8)

# k6 stdout embeds JSON between markers; no 'tar' or kubectl cp is required.
$markerRegex = '(?s)LPS_K6_JSON_BEGIN\r?\n(?<json>.*?)\r?\nLPS_K6_JSON_END'
$found = [regex]::Match($logText, $markerRegex)
if ($found.Success) {
    $jsonText = $found.Groups['json'].Value
    $report = $jsonText | ConvertFrom-Json
    $reportPath = Join-Path $resultsDir "$RunId-$Scenario-summary.json"
    [System.IO.File]::WriteAllText($reportPath, $jsonText, $utf8)
    Write-Host "JSON report saved: $reportPath"
    $droppedCount = $report.metrics.dropped_iterations.values.count
    if ($null -ne $droppedCount -and [double]$droppedCount -gt 0) {
        Write-Warning "Workload was not fully achieved: $droppedCount dropped iterations. Do not use target rate as measured throughput."
    }
    $visibleText = $logText.Substring(0, $found.Index) + $logText.Substring($found.Index + $found.Length)
} else {
    Write-Warning "Structured JSON not found in Pod logs. Verify that lps-k6-scripts ConfigMap contains the updated load.js."
    $visibleText = $logText
}
Write-Host $visibleText
Write-Host "Job $job terminal state: $jobState; full logs: $logPath"
Write-Host "RUN_ID=$RunId. After backlog drains, run read-only SQL from load-tests/sql/post-run.sql."
if ($jobState -ne 'COMPLETE') {
    throw "k6 Job $job ended with state $jobState (typically a threshold failure, container failure, or timeout). Logs and any JSON report have been saved. Inspect Pod reason and metrics."
}
