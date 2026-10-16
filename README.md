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
   Copy-Item .env.example .env
   docker compose up -d postgres kafka redis
   docker compose ps
   ```
2. **Build / test Java and Docker images:**
   ```powershell
   .\mvnw.cmd clean test
   .\mvnw.cmd -DskipTests package
   .\docker\build-images.ps1 -Tag local-v1
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
   Expect `201 Created` with `id`. Then call `GET http://localhost:18080/applications/{id}` and `GET http://localhost:18084/sagas/applications/{id}`. Follow Saga status to a terminal outcome.

Existing Kubernetes Secrets can be updated with `kubectl -n lps create secret generic lps-postgres --from-env-file=k8s/local/secrets.env --dry-run=client -o yaml | kubectl apply -f -`. The example credentials are **local-only**; never expose these ports to untrusted networks. **Do not run `docker compose down -v`**: it deletes persisted data. See [Kubernetes troubleshooting](k8s/README.md).

## Tests and observability

- Java regressions: `.\mvnw.cmd clean test` (Testcontainers requires Docker).
- k6 + SQL reconciliation: `.\load-tests\run-and-report.ps1 -Scenario smoke` (requires running local cluster). See [load tests](load-tests/README.md).
- Optional observability: Prometheus/Grafana/Jaeger; see [monitoring instructions](docs/architecture.md#operations-and-observability).

In one measured local baseline run: **601/601** accepted applications reached completed Saga state (10 HTTP requests/s, HTTP p99 **47.6 ms**, Saga p99 **144 s**). Higher-load runs accumulated unprocessed Saga/Outbox backlog and remain **inconclusive** for end-to-end throughput. HTTP response latency is **not** Saga completion latency. Results, scope and caveats: [load-test summary](docs/performance.md).

## Known limitations / planned reliability work

- HTTP `POST /applications` currently has **no client idempotency key**; an HTTP retry after a lost response can create a second application.
- Kafka poison-message isolation, **DLT and operator-controlled replay** are not implemented consistently across consumers; indefinite retries may stall a partition.
- **UNKNOWN disbursement reconciliation** needs a defined durable status inquiry / recovery workflow. Simulated payouts must never be presented as exactly-once real bank transfers.
- **FAILED Outbox recovery**, backup/restore drills, infrastructure fault injection and recovery-time measurements need broader verification.
- Under load, the Saga/Outbox pipeline can lag behind accepted HTTP requests; end-to-end sustainable throughput and scaling improvements are work in progress.
- Kubernetes runs locally on one node, with PostgreSQL/Kafka/Redis in Docker Compose: **not production high availability or disaster recovery**. Scoring, bureau, decision, disbursement and notification are simulated.

This list documents **work in progress**, not guarantees already implemented. Current releases prioritize correctness and recoverability over synthetic throughput targets.
