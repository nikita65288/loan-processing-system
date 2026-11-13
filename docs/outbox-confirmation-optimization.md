# Performance Experiment #2B — Saga Outbox confirmation

## Goal and evidence

The Patch 19.4 / instrumented Saga Publisher runs with a 100 ms polling interval, batch size 20 and one publisher instance. During an instrumented 10 req/s, 300 s test (`ab601801`), the previous implementation showed approximately 3–4 ms per Kafka send + acknowledgement, 12–15 ms per PostgreSQL confirmation transaction, and 35–45 ms per batch claim (visual estimates from Grafana). The Outbox queue grew during the run. These measurements point to the confirmation transaction as the largest **measured per-record** cost; they do not establish a guaranteed improvement from this change.

## Minimal change

Before, after a successful Kafka acknowledgement, one database transaction executed:

1. Conditional owned Outbox `UPDATE` to `PUBLISHED`.
2. JPA `SELECT saga_step_attempts WHERE outbox_event_id = ?`.
3. Hibernate dirty-checking `UPDATE` for `published_at`, `deadline_at`, `retry_after_at` (when an attempt exists).

After, the same transaction executes:

1. The **unchanged** conditional owned Outbox `UPDATE`.
2. A native conditional `UPDATE saga_step_attempts` directly computing timestamps from the persisted `timeout_seconds` and `backoff_seconds`, with `WHERE outbox_event_id = ? AND published_at IS NULL`.

The SQL updates all three fields in one statement to satisfy the existing `chk_saga_attempt_deadline` constraint. The `SagaStepAttempt.markPublished()` domain method remains unchanged for its other callers. No DB migration, topic/contract change, or concurrent publishing is introduced.

## Critical invariants

- Kafka acknowledgement occurs **before** any `PUBLISHED` status is committed.
- Outbox `markPublishedIfOwned(...)` must return **1** before the attempt UPDATE runs.
- Both updates commit atomically in the **same transaction**; if the attempt update fails, the Outbox confirmation rolls back. Existing failure/retry handling remains responsible for retrying.
- A missing attempt row (legacy/inquiry command) is valid: the UPDATE affects 0 rows, but an owned Outbox confirmation may still succeed.
- Previously published attempts are immutable (`published_at IS NULL`); `deadline_at = confirmed_at + timeout_seconds` and `retry_after_at = deadline_at + backoff_seconds` use the values durably stored with that attempt.
- `CLAIMED` ownership, stale claim recovery, duplicate Kafka delivery, Persistent Inbox, timeout/retry behavior and DLT/Controlled Replay remain untouched.

## Local verification

1. Ensure Docker Desktop Linux Engine is running: `docker version` must show both Client and Server, and `docker ps` must work. Testcontainers require Docker.
2. From project root, apply incremental `perf2b-outbox-confirmation.patch` **on top of the existing instrumentation patch**:
   `git apply --check .\perf2b-outbox-confirmation.patch` then `git apply .\perf2b-outbox-confirmation.patch`.
3. Run `git diff --check` and `git diff --stat`.
4. Run targeted integration tests (PowerShell):
   ` .\mvnw.cmd -pl services/saga-orchestrator-service -am -Dtest=SagaOutboxPublisherIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test`
5. Run full module + dependencies: ` .\mvnw.cmd -pl services/saga-orchestrator-service -am test`.
6. Build the Saga service JAR and image using the project's established Docker artifacts workflow. Tag the experimental image distinctly (for example `lps/saga-orchestrator-service:perf2b-confirm-v1`). Do not rebuild or retag the Patch 19.4 image.
7. Deploy **only** Saga Orchestrator to Kubernetes, preserving polling = 100 ms and batch = 20. Verify the running image, environment variables, rollout readiness, and Prometheus scrape.
8. Run `run-and-report.ps1` smoke, baseline (10 req/s, 60 s), then steady-state (10 req/s, 300 s) **after prior backlog drains**. Record exact run IDs and config/image digests. Use comparable warm-up and host conditions. Keep each report's SQL reconciliation.
9. Compare the pre-change instrumented steady-state (`ab601801`) to the new steady-state: HTTP p95/p99; Saga p50/p95/p99; Outbox `NEW`/`CLAIMED` and oldest age; published events/sec; duration of `lps_outbox_database_confirm_duration_seconds`; Kafka ack and claim batch metrics; CPU/Hikari metrics if available; drain duration; duplicate/missing disbursements. Do not claim higher sustainable throughput solely from lower latency in one run.
10. Explicitly exercise existing restart/recovery, idempotency and DLT/replay regression tests. A happy-path SQL reconciliation PASS alone does not verify crash consistency.

**Acceptance:** tests pass (including real PostgreSQL integration), no incorrect/deadline overwrites, Kafka + Outbox/attempt atomicity preserved, all performance test reconciliations PASS. Keep the change for a measured improvement; otherwise revert it while retaining the instrumentation metrics.

## Rollback and commit discipline

For a runtime-only rollback, restore the previous `perf2-metrics-v1` Saga image and wait for rollout. To revert source, reverse the incremental patch (if it remains applicable): `git apply -R .\perf2b-outbox-confirmation.patch`. Keep reliability Patch 19.4 commits separate from performance commits. The data model and Liquibase schema are unchanged, so this experiment requires no data migration.
