# Architecture and design decisions

LPS is a locally deployable distributed-system demonstration, not a production lending platform.

| Component | Responsibility | Persistence |
|---|---|---|
| Loan Application | REST creation/read, origin event | PostgreSQL + Outbox |
| Saga Orchestrator | Workflow state machine, commands, retries, timeouts, recovery | PostgreSQL + Inbox/Outbox |
| Scoring / Bureau / Decision | Deterministic simulated decision processing | Kafka commands and events |
| Disbursement | Simulated, idempotent payout effect | PostgreSQL + Inbox/Outbox |
| Notification | Simulated decision/payout notifications | PostgreSQL + Inbox/Outbox |
| Redis | Cache-aside optimization for read-only views | Non-authoritative cache |

## Why these mechanisms

- **Transactional Outbox:** business-state mutation and outgoing event are committed in one PostgreSQL transaction. A publisher later sends the record to Kafka. Publication can repeat if it succeeds before its DB state is confirmed; consumers must handle duplicates.
- **Persistent Inbox:** the message identifier is recorded transactionally with business state. Duplicate records do not repeat committed effects. Duplicate protection is conditional on retained identifiers and transaction boundaries.
- **Saga Orchestrator:** coordinates scoring, bureau, decision, simulated disbursement and notification over commands/events without distributed transactions. Durable state, guarded transitions, bounded business retries and recovery are used. A decision of `REJECTED` is a business outcome, distinct from technical `FAILED`.
- **Financial idempotency:** simulated payout operations use stable identifiers and PostgreSQL uniqueness constraints. Missing payout confirmation can leave an **unknown outcome** requiring reconciliation; don't initiate a new independent payout.
- **Redis:** cache-aside with PostgreSQL fallback; cached Saga/application views are not authoritative and can be briefly stale.
- **Kafka:** at-least-once message processing, partition key/order and consumer group ownership must be considered. There is no claim of distributed exactly-once semantics.

## Trade-offs and failures

| Choice | Benefit | Trade-off / remaining failure |
|---|---|---|
| PostgreSQL Outbox polling | No DB/Kafka dual-write loss after DB commit | Extra writes and polling lag; duplicate Kafka publishing possible |
| Durable Inbox | Idempotent committed effects | Persistence overhead, retention and semantic deduplication complexity |
| Saga Orchestrator | Explicit workflow/recovery state | Central coordination logic and concurrency management |
| Redis read cache | Faster repeated reads | Cache invalidation, TTL and stale read risk |
| Local Docker Desktop Kubernetes | Repeatable developer deployment / restarts | Single-node only; broker and database outside K8s; not HA |

## Operations and observability

- **Metrics:** Spring Actuator/Micrometer, Prometheus/Grafana; inspect HTTP duration, JVM, Kafka consumer lag, Outbox backlog and Saga states. Prometheus collects aggregate metrics, not individual application identifiers.
- **Tracing/logs:** OpenTelemetry traces to Jaeger, JSON logs and optional Compose ELK. K8s container logs use `kubectl logs`; Pod-to-ELK log shipping is not included.
- **Local monitoring (K8s):** run `docker compose --profile k8s-monitoring up -d kafka-exporter`, deploy `kubectl apply -k k8s/monitoring` after creating `lps-grafana` Secret, then port-forward Prometheus/Grafana as described in [Kubernetes notes](../k8s/README.md).
- **Load tests:** `load-tests/run-and-report.ps1` invokes a Kubernetes k6 Job and read-only PostgreSQL reconciliation, producing per-run results ignored by Git.

## Known boundaries

There is no real financial integration, external bureau, payment rail, email/SMS or production-grade security boundary. DLT, safe replay, UNKNOWN payout inquiry, restore drills and reliability alerts are planned and should not be assumed to exist. See [README](../README.md#known-limitations--planned-reliability-work).
