# load testing: capture read-only PostgreSQL reconciliation and generate a durable report.
# Can be re-run for an existing RUN_ID without starting a new k6 Job.
param(
    [Parameter(Mandatory=$true)][ValidatePattern('^[0-9a-fA-F]{8}$')][string]$RunId,
    [Parameter(Mandatory=$true)][ValidateSet('smoke','baseline','ramp-up','target','steady-state','spike','stress','soak','cache-read')][string]$Scenario,
    [ValidateRange(0,3600)][int]$DrainTimeoutSeconds = 300,
    [ValidateRange(5,120)][int]$PollSeconds = 15,
    [string]$RunnerError = ''
)
$ErrorActionPreference = 'Stop'
$RunId = $RunId.ToLowerInvariant()
if ($RunId -eq '00000000') { throw 'RUN_ID must not be 00000000.' }
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$utf8 = [System.Text.UTF8Encoding]::new($false)
$resultsRoot = Join-Path $PSScriptRoot 'results'
$outDir = Join-Path $resultsRoot "$Scenario-$RunId"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null
$issues = New-Object 'System.Collections.Generic.List[string]'
$startReport = Get-Date

function Save-Text([string]$Path, [string]$Text) {
    [System.IO.File]::WriteAllText($Path, $Text, $utf8)
}
function MetricValue($Metrics, [string]$Name, [string]$Field) {
    if ($null -eq $Metrics) { return $null }
    $entry = $Metrics.PSObject.Properties[$Name]
    if ($null -eq $entry -or $null -eq $entry.Value.values) { return $null }
    $value = $entry.Value.values.PSObject.Properties[$Field]
    if ($null -eq $value) { return $null }
    return $value.Value
}
function Show-Value($Value, [string]$Suffix = '') {
    if ($null -eq $Value) { return 'н/д' }
    return "$Value$Suffix"
}

# Executed inside the existing Docker Compose postgres container, not on the Windows host.
# PostgreSQL credentials/database name come from the container's own environment.
# The -T switch enables safe stdin redirection and avoids a pseudo-TTY.
function Invoke-LpsSql([string]$Sql, [string]$File, [switch]$Compact) {
    Push-Location $repo
    try {
        if ($Compact) {
            $output = @($Sql | & docker compose exec -T postgres sh -c 'exec psql -X -q -t -A -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' 2>&1)
        } else {
            $output = @($Sql | & docker compose exec -T postgres sh -c 'exec psql -X -v ON_ERROR_STOP=1 -P pager=off -U "$POSTGRES_USER" -d "$POSTGRES_DB"' 2>&1)
        }
        $code = $LASTEXITCODE
    } finally {
        Pop-Location
    }
    $text = ($output | ForEach-Object { "$_" }) -join "`n"
    Save-Text (Join-Path $outDir $File) ($text + "`n")
    if ($code -ne 0) { throw "psql exit code $code. Details: $File" }
    return $text
}

# k6 writes the real JSON into ./results after the runner hotfix.
$summarySource = Join-Path $resultsRoot "$RunId-$Scenario-summary.json"
$logSource = Join-Path $resultsRoot "$RunId-$Scenario-k6.log"
$summary = $null
if (Test-Path $summarySource) {
    Copy-Item $summarySource (Join-Path $outDir 'summary.json') -Force
    try { $summary = Get-Content -Raw -Encoding UTF8 $summarySource | ConvertFrom-Json }
    catch { $issues.Add("Некорректный JSON k6: $($_.Exception.Message)") }
} else { $issues.Add('k6 summary.json отсутствует (например, Job завершился до публикации метрик).') }
if (Test-Path $logSource) { Copy-Item $logSource (Join-Path $outDir 'k6.log') -Force }
else { $issues.Add('k6.log отсутствует.') }
if ($summary -and ($summary.run_id -ne $RunId -or $summary.scenario -ne $Scenario)) {
    $issues.Add('RUN_ID/Scenario в k6 JSON не совпадает с параметрами отчёта.')
    $summary = $null
}
if ($RunnerError) { $issues.Add('Ошибка запуска k6: ' + $RunnerError) }

# Save small pieces of infrastructure evidence. Failure does not block SQL results.
try {
    $pods = & kubectl --request-timeout=10s -n lps get pods -o wide 2>&1
    Save-Text (Join-Path $outDir 'pods.txt') (($pods | Out-String).Trim())
} catch { $issues.Add('Не удалось собрать kubectl pods: ' + $_.Exception.Message) }
try {
    $job = & kubectl --request-timeout=10s -n lps get "job/lps-k6-$RunId" -o json 2>&1
    if ($LASTEXITCODE -eq 0) { Save-Text (Join-Path $outDir 'job.json') (($job -join "`n") + "`n") }
} catch { }
try {
    $images = & kubectl --request-timeout=10s -n lps get deployments -o wide 2>&1
    if ($LASTEXITCODE -eq 0) { Save-Text (Join-Path $outDir 'deployments.txt') (($images -join "`n") + "`n") }
} catch { }

$initial = $null
$final = $null
$sqlError = ''
$drainTime = $null
$drainTimedOut = $false
$readScenario = $Scenario -eq 'cache-read'
if (-not $readScenario) {
    try {
        $jsonSql = (Get-Content -Raw -Encoding UTF8 (Join-Path $PSScriptRoot 'sql/run-summary.sql')).Replace('__RUN_ID__', $RunId)
        $rawSql  = (Get-Content -Raw -Encoding UTF8 (Join-Path $PSScriptRoot 'sql/post-run.sql')).Replace("'abcdef12'", "'$RunId'")
        $jsonText = Invoke-LpsSql $jsonSql 'snapshot-initial-raw.txt' -Compact
        # The JSON query returns one line; PostgreSQL NOTICE/warnings break parsing rather than being silently ignored.
        $initial = ($jsonText.Trim() | ConvertFrom-Json)
        Save-Text (Join-Path $outDir 'snapshot-initial.json') ($initial | ConvertTo-Json -Depth 12)
        $firstDone = Get-Date
        $final = $initial
        if ($DrainTimeoutSeconds -gt 0) {
            $deadline = $firstDone.AddSeconds($DrainTimeoutSeconds)
            while ($true) {
                $pending = ([int64]$final.missing_sagas -gt 0) -or ([int64]$final.saga_in_progress -gt 0) -or ([int64]$final.outbox_pending -gt 0)
                if (-not $pending -and [int64]$final.created_applications -gt 0) {
                    $drainTime = [math]::Round(((Get-Date) - $firstDone).TotalSeconds,1)
                    break
                }
                if ((Get-Date).AddSeconds($PollSeconds) -ge $deadline) { $drainTimedOut = $true; break }
                Start-Sleep -Seconds $PollSeconds
                $jsonText = Invoke-LpsSql $jsonSql 'snapshot-last-poll-raw.txt' -Compact
                $final = ($jsonText.Trim() | ConvertFrom-Json)
                Save-Text (Join-Path $outDir 'snapshot-last-poll.json') ($final | ConvertTo-Json -Depth 12)
            }
        }
        # Always capture a final snapshot and run ALL nine original SQL checks in one psql session.
        $finalText = Invoke-LpsSql $jsonSql 'snapshot-final-raw.txt' -Compact
        $final = ($finalText.Trim() | ConvertFrom-Json)
        Save-Text (Join-Path $outDir 'snapshot-final.json') ($final | ConvertTo-Json -Depth 12)
        $null = Invoke-LpsSql $rawSql 'post-run-all-nine-queries.txt'
    } catch {
        $sqlError = $_.Exception.Message
        $issues.Add('SQL reconciliation не завершился: ' + $sqlError)
    }
}

$accepted = MetricValue $summary.metrics 'lps_accepted_posts' 'count'
$requests = MetricValue $summary.metrics 'http_reqs' 'count'
$rps = MetricValue $summary.metrics 'http_reqs' 'rate'
$p50 = MetricValue $summary.metrics 'http_req_duration' 'med'
$p95 = MetricValue $summary.metrics 'http_req_duration' 'p(95)'
$p99 = MetricValue $summary.metrics 'http_req_duration' 'p(99)'
$dropped = MetricValue $summary.metrics 'dropped_iterations' 'count'
$httpFail = MetricValue $summary.metrics 'http_req_failed' 'rate'
$businessOk = MetricValue $summary.metrics 'lps_http_business_ok' 'rate'

$integrity = 'INCONCLUSIVE'
$integrityNotes = New-Object 'System.Collections.Generic.List[string]'
if ($readScenario) {
    $integrityNotes.Add('GET cache-read не создаёт заявок: SQL сверка POST неприменима.')
} elseif ($null -ne $final) {
    $badFinancial = ([int64]$final.duplicate_disbursement_application_ids -gt 0) -or
        ([int64]$final.rejected_with_disbursement -gt 0) -or
        ([int64]$final.completed_approved_missing_disbursement -gt 0) -or
        ([int64]$final.duplicate_sagas -gt 0)
    if ($badFinancial) { $integrityNotes.Add('Нарушены финансовые инварианты или обнаружены дубли Saga.') }
    $acceptedKnown = $null -ne $accepted
    if ($acceptedKnown -and [int64]$final.created_applications -ne [int64]$accepted) {
        $integrityNotes.Add('Количество сохранённых заявок не равно количеству принятых k6 POST.')
    }
    $terminalBad = ([int64]$final.saga_failed -gt 0) -or ([int64]$final.saga_requires_reconciliation -gt 0) -or ([int64]$final.outbox_failed -gt 0)
    if ($terminalBad) { $integrityNotes.Add('Обнаружены FAILED / REQUIRES_RECONCILIATION Saga или FAILED Outbox.') }
    $pending = ([int64]$final.missing_sagas -gt 0) -or ([int64]$final.saga_in_progress -gt 0) -or ([int64]$final.outbox_pending -gt 0)
    if ($pending) { $integrityNotes.Add('После ожидания остался backlog; итоговая корректность не подтверждена.') }
    if ($badFinancial -or ($acceptedKnown -and [int64]$final.created_applications -ne [int64]$accepted) -or $terminalBad) {
        $integrity = 'FAIL'
    } elseif ($pending -or -not $acceptedKnown -or [int64]$final.created_applications -eq 0 -or $sqlError) {
        $integrity = 'INCONCLUSIVE'
    } else { $integrity = 'PASS' }
}

$performance = 'INCONCLUSIVE'
$perfNotes = New-Object 'System.Collections.Generic.List[string]'
if ($summary -and $null -ne $p99 -and $null -ne $httpFail) {
    $performance = 'PASS'
    if ([double]$p99 -ge 2000) { $performance = 'FAIL'; $perfNotes.Add('HTTP p99 >= 2000 ms.') }
    if ([double]$httpFail -gt 0) { $performance = 'FAIL'; $perfNotes.Add('Есть HTTP failures.') }
    if ($null -ne $businessOk -and [double]$businessOk -lt 1) { $performance = 'FAIL'; $perfNotes.Add('Не все HTTP ответы прошли business check.') }
    if ($null -ne $dropped -and [double]$dropped -gt 0) {
        if ($performance -eq 'PASS') { $performance = 'INCONCLUSIVE' }
        $perfNotes.Add('Есть dropped iterations: offered rate не достигнут полностью.')
    }
    if ($RunnerError) { $performance = 'FAIL'; $perfNotes.Add('k6 runner сообщил ошибку.') }
}

$k6State = 'UNKNOWN'
if (Test-Path (Join-Path $outDir 'job.json')) {
    try {
        $jobInfo = Get-Content -Raw -Encoding UTF8 (Join-Path $outDir 'job.json') | ConvertFrom-Json
        foreach ($condition in @($jobInfo.status.conditions)) {
            if ($condition.status -eq 'True' -and $condition.type -eq 'Complete') { $k6State='COMPLETE' }
            if ($condition.status -eq 'True' -and $condition.type -eq 'Failed') { $k6State='FAILED' }
        }
    } catch { }
}

$outboxDetails = @()
if ($final) {
    foreach ($state in @($final.outbox_statuses)) {
        $outboxDetails += "| $($state.owner) | $($state.status) | $($state.events) |"
    }
}
if ($outboxDetails.Count -eq 0) { $outboxDetails += '| — | — | — |' }
$allNotes = @($issues.ToArray()) + @($integrityNotes.ToArray()) + @($perfNotes.ToArray())
$notesText = if ($allNotes.Count) { ($allNotes | ForEach-Object { "- $_" }) -join "`n" } else { '- Явных расхождений в собранных показателях не обнаружено.' }
$report = @"
# LPS load testing — Automated Load Test Report

**RUN_ID:** ``$RunId``  
**Scenario:** ``$Scenario``  
**Generated:** $((Get-Date).ToString('yyyy-MM-dd HH:mm:ss zzz'))  
**k6 Job:** ``lps-k6-$RunId`` — $k6State  
**Data integrity:** **$integrity**  
**HTTP performance:** **$performance**

## 1. Load and HTTP (k6)

| Metric | Measured |
|---|---:|
| Requested arrival rate | $(Show-Value $summary.target_rate '/s') |
| Planned duration | $(Show-Value $summary.duration_seconds ' s') |
| HTTP requests | $(Show-Value $requests) |
| Accepted POST | $(Show-Value $accepted) |
| Achieved HTTP req/s | $(Show-Value $rps) |
| HTTP p50 | $(Show-Value $p50 ' ms') |
| HTTP p95 | $(Show-Value $p95 ' ms') |
| HTTP p99 | $(Show-Value $p99 ' ms') |
| HTTP failed rate | $(Show-Value $httpFail) |
| Business success rate | $(Show-Value $businessOk) |
| Dropped iterations | $(Show-Value $dropped) |

**Note:** ramp/spike/stress target rate is a peak, not average. HTTP latency and Saga duration are distinct measurements.

## 2. PostgreSQL reconciliation (final snapshot)

| Metric | Database value |
|---|---:|
| Stored applications | $(Show-Value $final.created_applications) |
| Saga COMPLETED | $(Show-Value $final.saga_completed) |
| Saga IN_PROGRESS | $(Show-Value $final.saga_in_progress) |
| Saga FAILED | $(Show-Value $final.saga_failed) |
| Saga REQUIRES_RECONCILIATION | $(Show-Value $final.saga_requires_reconciliation) |
| Missing Saga | $(Show-Value $final.missing_sagas) |
| Duplicate Saga | $(Show-Value $final.duplicate_sagas) |
| Outbox pending (not PUBLISHED) | $(Show-Value $final.outbox_pending) |
| Outbox FAILED | $(Show-Value $final.outbox_failed) |
| Duplicate disbursement application IDs | $(Show-Value $final.duplicate_disbursement_application_ids) |
| Rejected with disbursement | $(Show-Value $final.rejected_with_disbursement) |
| Approved COMPLETED payout missing | $(Show-Value $final.completed_approved_missing_disbursement) |
| Disbursement rows | $(Show-Value $final.disbursement_rows) |
| Notification rows | $(Show-Value $final.notification_rows) |
| Saga step attempt rows | $(Show-Value $final.saga_attempt_rows) |
| Global inbox messages (NOT scoped to RUN_ID) | $(Show-Value $final.global_inbox_messages_not_scoped_to_run) |

## 3. Saga latency (Saga created to completed; seconds)

| Metric | Result |
|---|---:|
| Completed in latency sample | $(Show-Value $final.saga_duration_seconds.count) |
| Average | $(Show-Value $final.saga_duration_seconds.average) |
| p50 | $(Show-Value $final.saga_duration_seconds.p50) |
| p95 | $(Show-Value $final.saga_duration_seconds.p95) |
| p99 | $(Show-Value $final.saga_duration_seconds.p99) |
| Maximum | $(Show-Value $final.saga_duration_seconds.maximum) |

**Caution:** these percentiles exclude unfinished Saga and the HTTP-to-Saga creation delay.

## 4. Outbox by service and status

| Service | Status | Events |
|---|---|---:|
$($outboxDetails -join "`n")

## 5. Drain / recovery after load

- Initial stored applications: $(Show-Value $initial.created_applications)
- Initial Saga IN_PROGRESS: $(Show-Value $initial.saga_in_progress)
- Initial Missing Saga: $(Show-Value $initial.missing_sagas)
- Initial Outbox pending: $(Show-Value $initial.outbox_pending)
- Final snapshot at: $(Show-Value $final.captured_at_utc)
- Time to observed drain from initial SQL snapshot: $(Show-Value $drainTime ' s')
- Drain deadline exceeded: $drainTimedOut

Drain is based on RUN_ID-scoped PostgreSQL Saga/Outbox only; it does **not** prove Kafka consumer lag reached zero. This duration does not include time elapsed between end of k6 and first SQL snapshot.

## 6. Warnings and conclusion

$notesText

**Known measurement gaps:** Kafka lag peaks, Prometheus CPU/heap/Hikari and Kubernetes OOM events are not automatically aggregated by this SQL report. Use Grafana/Prometheus if a production-grade performance claim is required. A PASS for integrity is not a substitute for those measurements.

## 7. Evidence for reliability review

- ``summary.json`` — k6 metrics (when available).
- ``k6.log`` — full k6 output (when available).
- ``snapshot-initial.json`` / ``snapshot-final.json`` — structured PostgreSQL snapshots.
- ``post-run-all-nine-queries.txt`` — original nine SQL checks, executed in one psql session.
- ``pods.txt``, ``deployments.txt``, ``job.json`` — environment context, if accessible.
- ``report-data.json`` — machine-readable pass/fail and key metrics.

No application tables, Kafka offsets, Redis keys or Liquibase changelogs were modified by reconciliation.
"@
Save-Text (Join-Path $outDir 'report.md') $report
$reportData = [pscustomobject]@{
    run_id=$RunId;scenario=$Scenario;generated_utc=[DateTime]::UtcNow.ToString('o')
    integrity=$integrity; performance=$performance;k6_job=$k6State
    k6_accepted_posts=$accepted;k6_requests=$requests;k6_achieved_rps=$rps;k6_http_p99_ms=$p99;k6_dropped_iterations=$dropped
    sql_created_applications=$(if ($final) { $final.created_applications } else { $null })
    sql_saga_completed=$(if ($final) { $final.saga_completed } else { $null })
    sql_saga_in_progress=$(if ($final) { $final.saga_in_progress } else { $null })
    sql_missing_sagas=$(if ($final) { $final.missing_sagas } else { $null })
    sql_outbox_pending=$(if ($final) { $final.outbox_pending } else { $null })
    drain_timed_out=$drainTimedOut;drain_observed_seconds=$drainTime;issues=$allNotes
}
Save-Text (Join-Path $outDir 'report-data.json') ($reportData | ConvertTo-Json -Depth 10)
Write-Host "load testing report: $(Join-Path $outDir 'report.md')"
Write-Host "RUN_ID=$RunId | SQL integrity=$integrity | HTTP performance=$performance | Job=$k6State"
if ($sqlError) { Write-Warning "SQL unavailable: $sqlError. The report is partial; do not treat missing numbers as zero." }
