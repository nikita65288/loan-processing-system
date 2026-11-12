# Loan Processing System (LPS)

A Java 17 / Spring Boot 3 **event-driven backend** that processes simulated loan applications across seven microservices. It focuses on reliable asynchronous messaging, durable workflow coordination and observable failure recovery. This is a personal engineering project; **no real loans, payments, credit bureau calls or notifications** are performed.

## Architecture

```text
Postman / HTTP
       |
       v
Loan Application Service --(PostgreSQL + Transactional Outbox)--> Kafka
                                                                |
                                                                v
                                                     Saga Orchestrator
                                                    (PostgreSQL + Inbox/Outbox)
                                                                |
                                                  Commands / completion events
                                                                |
                                    Credit Scoring -> Bureau -> Decision
                                                                |
                                              +-----------------+------------------+
                                              |                                    |
                                           APPROVED                              REJECTED
                                              |                                    |
                                  Simulated Disbursement                 Simulated Notification
                                              |                                    |
                                        Notification                         Saga terminal
                                              |
                                         Saga terminal

Redis: cache-aside reads; PostgreSQL: durable application, Saga and business state.
```

**Services:** `loan-application-service`, `credit-scoring-service`, `bureau-service`, `decision-service`, `saga-orchestrator-service`, `loan-disbursement-service`, `notification-service`. `lps-contracts` is a shared Kafka contract **library**, not another service.

**Key decisions:** Kafka with at-least-once processing; Transactional Outbox avoids DB/Kafka dual writes; Persistent Inbox and business uniqueness guard duplicate effects; PostgreSQL-backed Saga Orchestrator coordinates retries, timeouts and recovery; Redis is a non-authoritative read cache. These provide **idempotent business effects within implemented boundaries**, not end-to-end exactly-once delivery. See [design notes](docs/architecture.md).

**Stack:** Java 17, Spring Boot 3, Spring Kafka, PostgreSQL, Liquibase, Redis, Kafka, Docker Compose, Docker Desktop Kubernetes, Testcontainers, JUnit 5, Prometheus/Grafana, OpenTelemetry/Jaeger, optional ELK, k6.

## Run locally (Windows / PowerShell)

Requires **JDK 17**, Docker Desktop in Linux-containers mode with **local Kubernetes enabled**, Maven Wrapper, `kubectl` and Postman. Commands below are run **from the repository root**. Allow enough Docker Desktop resources for seven JVMs and infrastructure.

1. **Start durable infrastructure** (do not remove volumes):
   ```powershell
   if (-not (Test-Path .env)) { Copy-Item .env.example .env }
   docker compose up -d postgres kafka redis
   docker compose ps
   .\k8s\scripts\provision-dlt-topics.ps1
   ```
2. **Build / test Java and Docker images:**
   ```powershell
   .\mvnw.cmd clean test
   .\mvnw.cmd -DskipTests package
   .\docker\build-images.ps1 -Tag patch19.4-v1
   ```
3. **Deploy to local Kubernetes** (the cluster context **must** be `docker-desktop`):
   ```powershell
   kubectl config current-context
   kubectl apply -f k8s/local/namespace.yaml
   Copy-Item k8s/local/secrets.env.example k8s/local/secrets.env
   # Edit k8s/local/secrets.env: set the same DB user/password as in .env
   kubectl -n lps create secret generic lps-postgres --from-env-file=k8s/local/secrets.env
   .\k8s\scripts\check-local.ps1
   kubectl apply -k k8s/local
   kubectl -n lps get pods
   ```
4. **Forward HTTP endpoints** in separate PowerShell terminals:
   ```powershell
   kubectl -n lps port-forward service/loan-application-service 18080:8080
   ```
   ```powershell
   kubectl -n lps port-forward service/saga-orchestrator-service 18084:8080
   ```
5. **Verify in Postman:** `POST http://localhost:18080/applications` with body:
   ```json
   {"customerId":"11111111-1111-4111-8111-111111111111", "amount":10000.00, "term":12, "currency":"EUR"}
   ```
   Optionally add `Idempotency-Key: <client-generated-UUID>` and use the **same key** for all retries of this logical POST. Expect `201 Created` with `id`. Then call `GET http://localhost:18080/applications/{id}` and `GET http://localhost:18084/sagas/applications/{id}`. Follow Saga status to a terminal outcome.

Existing Kubernetes Secrets can be updated with `kubectl -n lps create secret generic lps-postgres --from-env-file=k8s/local/secrets.env --dry-run=client -o yaml | kubectl apply -f -`. The example credentials are **local-only**; never expose these ports to untrusted networks. **Do not run `docker compose down -v`**: it deletes persisted data. See [Kubernetes troubleshooting](k8s/README.md).

## Tests and observability

- Java regressions: `.\mvnw.cmd clean test` (Testcontainers requires Docker).
- k6 + SQL reconciliation: `.\load-tests\run-and-report.ps1 -Scenario smoke` (requires running local cluster). See [load tests](load-tests/README.md).
- Optional observability: Prometheus/Grafana/Jaeger; see [monitoring instructions](docs/architecture.md#operations-and-observability).

After **Performance Experiment №1** (10 applications/sec for 60 seconds): **601/601** Saga COMPLETED after backlog drain; HTTP p99 **229.13 ms**, Saga p99 **36.53 s**, observed drain **31.1 s**, reconciliation PASS. The only intentional optimization was Saga Outbox polling **1000 ms → 100 ms** in the Kubernetes manifest; a backlog still existed at the end of generation, so **sustainable 10 completed Saga/sec is not proven**. See [historical performance notes](docs/performance.md).

## Known limitations / planned reliability work

- **Patch 19.1 (implemented and published):** `POST /applications` accepts optional `Idempotency-Key`. Repeating the same key/payload replays the original `201 Created` without a new application/Outbox; the same key with a changed payload yields `409`. Requests without a key remain **non-idempotent**.
- **Patch 19.4 (code prepared, verification pending):** typed Kafka listeners use `ErrorHandlingDeserializer`, explicit invalid-message classification and acknowledged `.DLT` publication; an opt-in Java CLI can inspect/replay one retained record. Transient/business failures retain retry semantics; no automatic replay. See [Kafka DLT runbook](docs/kafka-dlt-runbook.md). Until failure/restart/Kubernetes smoke tests pass, this is **not a verified guarantee**.
- **Patch 19.2 (verified):** late matching Disbursement completion can safely resume a Saga after `DISBURSEMENT_UNKNOWN`, without duplicate payout or Notification.
- **Patch 19.3 (implemented, tested and published):** a bounded durable read-only status inquiry requests the committed payout result over two Kafka topics. It never invokes payout execution; a verified COMPLETED applies the same guarded Saga transition as Patch 19.2. `NOT_FOUND` and `CONFLICT` keep the Saga in reconciliation. Simulated payouts must never be presented as exactly-once real bank transfers.
- **FAILED Outbox recovery**, DLT failure/replay verification, backup/restore drills, end-to-end fault injection and recovery-time measurements need broader verification.
- Under load, the Saga/Outbox pipeline can lag behind accepted HTTP requests; end-to-end sustainable throughput and scaling improvements are work in progress.
- Kubernetes runs locally on one node, with PostgreSQL/Kafka/Redis in Docker Compose: **not production high availability or disaster recovery**. Scoring, bureau, decision, disbursement and notification are simulated.

This list documents **work in progress**, not guarantees already implemented. Current releases prioritize correctness and recoverability over synthetic throughput targets.
