# Performance Experiment 2 — Saga Outbox phase timings

Scope: **observability only**, on top of Patch 19.4 and Performance Experiment 1. No changes to Saga semantics, Kafka contracts, Outbox claim/acknowledgement/retry logic, batch size or polling interval. PostgreSQL remains the durable source of truth.

## What is measured

The new Micrometer meters are registered **only by Saga Orchestrator**, with fixed names and the application's existing `application` tag (no record/customer IDs or other high-cardinality labels):

| Meter (Micrometer name) | Meaning |
|---|---|
| `lps.outbox.kafka.ack.duration` | Time around `producer.publish(...)` **and** `future.get(...)`. Includes producer-side enqueue/serialization and wait for broker ack. Recorded even if send fails or times out. **Not** just broker network latency. |
| `lps.outbox.database.confirm.duration` | Time for PostgreSQL transaction confirming a Kafka-acknowledged Outbox record, including obtaining a connection, SQL, updating an optional Saga step attempt, and committing. Recorded even if the transaction fails or ownership changed. Not recorded when Kafka failed first. |
| `lps.outbox.claim.batch.duration` | Transaction time for selecting/claiming a batch (including PostgreSQL work and commit). |
| `lps.outbox.stale.claim.scan.duration` | Transaction time for releasing expired claims, including empty scans. |
| `lps.outbox.poll.cycle.duration` | Entire scheduled method execution, including recovery, claim, publishOne calls and application overhead; **excludes** Spring `fixedDelay` after return. |
| `lps.outbox.claim.batch.size` | Distribution summary of number of claimed records in each poll, including zero on empty polls. |

Existing meters `lps.outbox.publication.duration` (whole `publishOne`) and `lps.outbox.published` remain unchanged. Timers use `System.nanoTime()` (monotonic elapsed time) and record in `finally` so failure paths are observable. Metrics are best-effort and cannot alter the business result.

## Build and deploy *only* Saga Orchestrator (Windows PowerShell)

Run from the repository root. Ensure Docker Desktop is running and the Kubernetes context is `docker-desktop`. Preserve existing volumes, database and Kafka offsets. Only run this when earlier test queues have drained.

```powershell
.\mvnw.cmd -pl services/saga-orchestrator-service -am test
.\mvnw.cmd -pl services/saga-orchestrator-service -am -DskipTests package
Copy-Item services/saga-orchestrator-service/target/saga-orchestrator-service-0.0.1-SNAPSHOT.jar docker/artifacts/saga-orchestrator-service.jar -Force
docker build --file docker/Dockerfile --build-arg JAR_FILE=docker/artifacts/saga-orchestrator-service.jar --tag lps/saga-orchestrator-service:perf2-metrics-v1 .
kubectl config current-context
kubectl -n lps set image deployment/saga-orchestrator-service saga-orchestrator-service=lps/saga-orchestrator-service:perf2-metrics-v1
kubectl -n lps rollout status deployment/saga-orchestrator-service --timeout=180s
kubectl -n lps get deployment saga-orchestrator-service -o jsonpath='{.spec.template.spec.containers[0].image}'
kubectl -n lps exec deployment/saga-orchestrator-service -- printenv LPS_SAGA_OUTBOX_POLLING_INTERVAL_MS
```

Expected: image tag `perf2-metrics-v1`, polling interval `100`. Keep other services unchanged, Kafka DLT/replay enabled per Patch 19.4. Building/deploying a new image is necessary because the timers are **Java code changes**, not configuration-only changes.

Validate timer registration and scraping using Postman (`GET http://localhost:18084/actuator/metrics/lps.outbox.kafka.ack.duration`) after running `kubectl -n lps port-forward service/saga-orchestrator-service 18084:8080` in another terminal. At idle, Kafka/confirm counts can be zero: execute smoke first to populate them. `GET /actuator/prometheus` should contain the corresponding exported metrics.

## PromQL for Grafana → Explore → Prometheus

Select the exact time interval of the **new** 300-second steady-state run. Run each query separately. Prometheus time units for these timers are seconds. Results are *per-observation means over a sliding 1-minute window*, not p99s.

**Kafka enqueue + acknowledgement, mean seconds:**

```promql
sum(rate(lps_outbox_kafka_ack_duration_seconds_sum{application="saga-orchestrator-service"}[1m]))
/
clamp_min(sum(rate(lps_outbox_kafka_ack_duration_seconds_count{application="saga-orchestrator-service"}[1m])), 1e-12)
```

**PostgreSQL confirmation transaction, mean seconds:**

```promql
sum(rate(lps_outbox_database_confirm_duration_seconds_sum{application="saga-orchestrator-service"}[1m]))
/
clamp_min(sum(rate(lps_outbox_database_confirm_duration_seconds_count{application="saga-orchestrator-service"}[1m])), 1e-12)
```

**Claim batch transaction, mean seconds:**

```promql
sum(rate(lps_outbox_claim_batch_duration_seconds_sum{application="saga-orchestrator-service"}[1m]))
/
clamp_min(sum(rate(lps_outbox_claim_batch_duration_seconds_count{application="saga-orchestrator-service"}[1m])), 1e-12)
```

**Stale claim recovery scan transaction, mean seconds:**

```promql
sum(rate(lps_outbox_stale_claim_scan_duration_seconds_sum{application="saga-orchestrator-service"}[1m]))
/
clamp_min(sum(rate(lps_outbox_stale_claim_scan_duration_seconds_count{application="saga-orchestrator-service"}[1m])), 1e-12)
```

**Entire poll cycle, mean seconds (excludes fixedDelay):**

```promql
sum(rate(lps_outbox_poll_cycle_duration_seconds_sum{application="saga-orchestrator-service"}[1m]))
/
clamp_min(sum(rate(lps_outbox_poll_cycle_duration_seconds_count{application="saga-orchestrator-service"}[1m])), 1e-12)
```

**Average number of claimed events per poll:**

```promql
sum(rate(lps_outbox_claim_batch_size_sum{application="saga-orchestrator-service"}[1m]))
/
clamp_min(sum(rate(lps_outbox_claim_batch_size_count{application="saga-orchestrator-service"}[1m])), 1e-12)
```

**Existing publisher throughput, backlog and Hikari pending connections:**

```promql
sum(rate(lps_outbox_published_total{application="saga-orchestrator-service"}[1m]))
```

```promql
sum(lps_outbox_records{application="saga-orchestrator-service",status="NEW"})
```

```promql
sum(hikaricp_connections_pending{application="saga-orchestrator-service"})
```

If `No data`, verify metrics are actually exported at `/actuator/prometheus`, Prometheus scrape target is `UP`, and the selected time range overlaps the **new image** and test. Do not treat absent series as zero. Duration and published count are not the same: a Kafka ack can succeed yet ownership verification can fail; on failures the timer counts can differ. Interpret mean durations alongside timer `_count` and publication/retry counters.

## Run & evaluation

First test with `run-and-report.ps1 -Scenario smoke` and ensure SQL reconciliation passes. Then, in a stable environment, execute:

```powershell
.\load-tests\run-and-report.ps1 -Scenario steady-state -Rate 10 -DurationSeconds 300 -DrainTimeoutSeconds 900
```

Capture k6 summary, first and final SQL snapshots, run-specific Saga p50/p95/p99, Outbox created/published/pending by minute, Grafana graph screenshots or CSV, Kafka lag, Pod restart count, Hikari pending and any resource metrics available. Keep the old results (`c4932c65`) as the pre-instrumentation reference. The instrumentation may have a small measurement overhead, so compare on identical workload with warmed-up Pods. Do not claim the Kafka or database component is dominant until measured.

## Acceptance criteria / failure scenarios

- All existing Maven tests pass, including the Saga Outbox integration tests (Testcontainers/Docker required).
- Successful Kafka ack and committed DB confirmation each increment their respective timer once.
- Kafka failure/timeout is counted by the ack timer but does not enter the PostgreSQL confirmation transaction; existing retry and claim ownership are unchanged.
- Empty polling cycles record recovery/claim/cycle durations and a batch sample of zero, without Kafka sends.
- No new high-cardinality tags; no changes to Saga transitions, DLT/replay, Kafka keys, retry/timeout, or database schema.
- Smoke and steady-state run finish with SQL reconciliation PASS, no duplicate disbursements, all pending Outbox events eventually drained; verify crash/restart tests separately because happy path does **not** establish crash consistency.

**Rollback:** restore the prior Saga image only:

```powershell
kubectl -n lps set image deployment/saga-orchestrator-service saga-orchestrator-service=lps/saga-orchestrator-service:patch19.4-v1
kubectl -n lps rollout status deployment/saga-orchestrator-service --timeout=180s
```

Rollback image changes; it does **not** require reverting PostgreSQL, Kafka topics, volumes or Schema/Liquibase.

## Comparing component timings with the preexisting `publishOne()` timer

The original timer remains available:

```promql
sum(rate(lps_outbox_publication_duration_seconds_sum{application="saga-orchestrator-service"}[1m]))
/
clamp_min(sum(rate(lps_outbox_publication_duration_seconds_count{application="saga-orchestrator-service"}[1m])), 1e-12)
```

For successful records, the difference between the **mean** whole-method duration and the mean Kafka/DB timings can point to JSON deserialization, tracing, logging and other work. Do not subtract p95/p99 values or compare rates with different success/failure counts as though they were the same sample set.

Existing Hikari metrics may include connection-acquisition timings (if exported), not only `hikaricp_connections_pending`. Check the available `hikaricp_connections_acquire...` names in Prometheus before using an exact query.
