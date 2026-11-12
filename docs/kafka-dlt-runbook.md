# Patch 19.4 — Kafka Dead Letter Topics + Controlled Replay (verification pending)

## Purpose, existing code audit

The review is based on the post-Patch 19.3 / Performance Experiment №1 ZIP, **not** the original Stage 19.
The code below does not change the Kafka business DTOs, Saga state transitions, Inbox/Outbox tables,
Disbursement identity, or the **100 ms Saga Outbox polling** Kubernetes override.

### Listener inventory (all use value JSON, applicationId Kafka key, AckMode.RECORD, auto commit off)

| Service | Listener(s) | Input topic(s) | Group | Durable processing boundary |
| --- | --- | --- | --- | --- |
| Loan Application | ApplicationCreatedConsumer (diagnostic) | `loan.application.created` | `loan-application-service` | logging only; no expected DB effect |
| Saga Orchestrator | ApplicationCreatedListener | `loan.application.created` | `saga-orchestrator-service` | PostgreSQL Inbox + Saga + Outbox, transaction |
| Saga Orchestrator | CreditScoringCompletedListener, CreditScoringFailedListener | `loan.scoring.completed`, `loan.scoring.failed` | same Saga group | Inbox + Saga + Outbox, transaction |
| Saga Orchestrator | BureauCheckCompletedListener, BureauCheckFailedListener | `loan.bureau.check.completed`, `loan.bureau.check.failed` | same Saga group | Inbox + Saga + Outbox, transaction |
| Saga Orchestrator | DecisionCompletedListener, DecisionFailedListener | `loan.decision.completed`, `loan.decision.failed` | same Saga group | Inbox + Saga + Outbox, transaction |
| Saga Orchestrator | LoanDisbursementCompletedListener, LoanDisbursementFailedListener | `loan.disbursement.completed`, `loan.disbursement.failed` | same Saga group | Guarded Saga transition, Inbox, transaction |
| Saga Orchestrator | DisbursementStatusInquiryResultListener | `loan.disbursement.status.inquiry.result` | same Saga group | Inbox + verified inquiry state + Saga, transaction |
| Saga Orchestrator | LoanNotificationCompletedListener, LoanNotificationFailedListener | `loan.notification.completed`, `loan.notification.failed` | same Saga group | Inbox + Saga, transaction |
| Credit Scoring | CreditScoringConsumer | `loan.scoring.requested` | `credit-scoring-service` | synchronous Kafka confirmation of stub result / typed failure; no durable command Inbox |
| Bureau | BureauConsumer | `loan.bureau.check.requested` | `bureau-service` | synchronous Kafka confirmation of stub result / typed failure; no durable command Inbox |
| Decision | DecisionConsumer | `loan.decision.requested` | `decision-service` | synchronous Kafka confirmation of stub result / typed failure; no durable command Inbox |
| Disbursement | DisburseLoanCommandListener | `loan.disbursement.requested` | `loan-disbursement-service` | durable payout/inbox + Outbox in one PostgreSQL transaction |
| Disbursement | DisbursementStatusInquiryListener | `loan.disbursement.status.inquiry.requested` | `loan-disbursement-service-status-inquiry` | immutable inquiry identity + Inbox + read-only lookup + reply Outbox in one transaction |
| Notification | SendLoanDecisionNotificationListener | `loan.notification.requested` | `notification-service` | durable simulated delivery/inbox + Outbox in one PostgreSQL transaction |

**19 production listeners; 18 unique consumed topics; 8 consumer groups** (the `loan.application.created`
record is deliberately consumed by both the diagnostic group and the Saga group).
All original explicit `DefaultErrorHandler`s used 1 s unlimited backoff, and the Loan Application
diagnostic listener used the Spring container default handler. Value deserialization was previously
`JsonDeserializer` without an `ErrorHandlingDeserializer` wrapper.

**Existing risk:** malformed bytes can fail before the listener and stall a partition; the
Loan Application diagnostic listener's default bounded error handler can recover/skip after its
configured attempts without a durable DLT. Stub consumers also have no durable command Inbox,
so their broker-acknowledged outgoing events and deterministic command IDs matter. Saga has
intentional Inbox no-ops for duplicates and late/out-of-order events; these are not all poison.
Do **not** infer that a Saga Inbox `processed` marker guarantees an unknown external financial outcome.

## Policy and trade-offs

| Failure | Classification | Retry / offset | Next action |
| --- | --- | --- | --- |
| Broken JSON, incompatible typed DTO | `DeserializationException` from `ErrorHandlingDeserializer` | no listener invocation; send raw bytes to `<source>.DLT` and await Kafka producer ack before source offset may advance | fix contract/consumer, inspect, manually replay |
| Missing mandatory envelope/key (`applicationId`, event/command IDs etc.) | explicit `InvalidKafkaRecordException` | no retry; confirmed DLT (ordinary DTO serialized back to JSON) | investigate/fix; replay only when safe |
| PostgreSQL unavailable, Kafka broker unavailable, DB lock, business conflict | ordinary exception | **never DLT automatically**; delay 2 s and retry at same offset until resolved/operator intervention | restore infrastructure, diagnose state; operator may stop deployment |
| Duplicate event/command | existing Inbox/idempotency | business no-op, record commit if local transaction succeeds | no DLT; no replay needed |
| UNKNOWN payout, stale result, late confirmation, NOT_FOUND inquiry result | existing guarded Saga handling | handled by existing Saga/inquiry policy; never treated as generic poison | investigate/reconcile, allow approved retry/inquiry |
| DLT producer broker/ACL/topic failure | recovery exception | **source not safely recovered/committed**; retry on broker restoration | provision/fix DLT, check failure metric |

The per-topic `DeadLetterPublishingRecoverer` uses `<original-topic>.DLT` and **partition -1**:
Kafka assigns a DLT partition; its partition number need not equal the source partition. The source
partition/offset are preserved in Spring Kafka DLT headers. All original keys are preserved for
normal string-key records. The DLT producer uses `acks=all`, idempotence enabled, a send-result
wait and raw-byte-aware serializers. It intentionally excludes large exception stacktrace/message
headers and does not add secret/payloads to logs. No Kafka transaction or exactly-once claim is made.

The default error handler is intentionally **unlimited only for transient/unknown/business
exceptions** (2 s delay). This preserves existing no-silent-skip semantics but can still stall
an affected partition. A persistent business conflict requires operator investigation/controlled
consumer stop; **automated pause-after-N policy is not yet implemented**. If DLT publication
succeeded but the consumer crashed before source-offset commit, the record and its DLT entry may
both be delivered again; duplicate DLT entries are allowed.

DLT retention is **14 days in local Compose** (1 replica, 3 partitions); source topics retain
existing settings. Do not equate the presence of a DLT record with failed financial processing:
follow Saga/Disbursement durable state. Do not equate DLT total Kafka offsets with *unresolved*
incident count. Retention and single-broker durability limit recovery; no backup/export was added.

## Build, local test and rollout (Windows PowerShell)

From the repository root. Use local Docker Desktop, not a shared environment.

```powershell
# No volumes are deleted.
docker compose up -d postgres kafka redis
.\k8s\scripts\provision-dlt-topics.ps1

# Compile/test all modules; Docker must be healthy for Testcontainers.
.\mvnw.cmd clean test
.\mvnw.cmd -DskipTests package

# DLT integration test specifically (requires a real Testcontainers Kafka broker).
.\mvnw.cmd -pl lps-kafka-reliability -am -Dtest=KafkaDltKafkaIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test

# Seven services consume Kafka; rebuild all seven affected Java images.
.\docker\build-images.ps1 -Tag patch19.4-v1
kubectl config current-context         # must be docker-desktop
kubectl apply -k k8s/local
kubectl -n lps get pods
kubectl -n lps rollout status deployment/saga-orchestrator-service
kubectl -n lps rollout status deployment/loan-disbursement-service
```

Provisioned **18 DLT topics** correspond only to actively consumed source topics. Each is
`<source>.DLT` with 3 partitions, replication factor 1, and `retention.ms=1209600000`
(14 days). Source topics and group offsets remain untouched. If a DLT already exists, the
provisioning command will **not** change its partitions or retention: inspect and reconcile them
manually. Example:

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic loan.scoring.requested.DLT
docker compose exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:29092 --entity-type topics --entity-name loan.scoring.requested.DLT --describe
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group saga-orchestrator-service
```

Kubernetes mixed-version rolling updates preserve their previous, possibly unsafe policy until
**every** old consumer Pod has exited. Topic creation must precede deployment. Rollback reverts
images only: it does not undo DLT records, replayed records or source offset commits. Do not
reset consumer groups to fix this patch; do not run `docker compose down -v`.

### Normal Postman smoke

Forward HTTP endpoints as described in `README.md` and send `POST http://localhost:18080/applications`
with an `Idempotency-Key` header and a valid body. Query Saga through
`GET http://localhost:18084/sagas/applications/{id}`. Check approved/rejected paths, Kafka
consumer groups, outbox statuses and SQL reconciliation. Running `load-tests/run-and-report.ps1`
with `-Scenario smoke` is part of acceptance; no performance tuning belongs in this patch.

### Poison test (prefer integration test; never inject in a shared business topic)

Run `KafkaDltKafkaIntegrationTest` first. It creates an isolated Testcontainers broker,
publishes malformed `{"` bytes then valid JSON to `loan.scoring.requested` in one partition,
and checks DLT metadata and that the valid record progresses. To test manually in a
**separate disposable local Kafka environment only**, use a test producer (not a normal
business topic) that can send precisely malformed raw bytes; inspect source and DLT. A plain
console consumer can display Kafka keys but not reliably show all binary DLT metadata.

### Inspect → dry-run → replay ONE record

Use the `-operator.jar` packaged from `lps-kafka-reliability`. No REST admin endpoint is opened.
The utility reads the selected `topic:partition:offset` without committing a group offset,
prints only key, source reference, payload length and SHA-256 (no sensitive payload), and refuses
unknown source topics or inconsistent original topic metadata. **Dry-run is the default.**

```powershell
# Replace partition and offset with the actual retained DLT record location.
java -jar .\lps-kafka-reliability\target\lps-kafka-reliability-0.0.1-SNAPSHOT-operator.jar `
  --environment=local --bootstrap=localhost:9092 `
  --dlt-topic=loan.scoring.requested.DLT --partition=0 --offset=0

# Only after verifying business state, root cause and dry-run:
java -jar .\lps-kafka-reliability\target\lps-kafka-reliability-0.0.1-SNAPSHOT-operator.jar `
  --environment=local --bootstrap=localhost:9092 `
  --dlt-topic=loan.scoring.requested.DLT --partition=0 --offset=0 `
  --execute --confirm=loan.scoring.requested.DLT:0:0
```

The command never invents new business IDs or transforms payload. It removes DLT/exception
headers but keeps business/tracing headers. It uses the **original topic and partition**
(from DLT metadata) and preserves the original raw JSON/key. Kafka producer acknowledgement
is printed; a successful broker publication is **not** proof the consumer handled the message.
The console transcript is the operator audit (save it with incident ID); no new audit DB table.
Multiple executions may publish duplicate *transport records*.

**Before replay:** inspect the actual Saga status/current step, Inbox, Outbox and Disbursement
operation identity; fix the consumer/schema/config; ensure the old event is still a valid input
for its current business state. Do not replay a contradictory financial outcome without manual
reconciliation. Do not delete an Inbox row to force re-execution. A repeated inquiry with an
already-processed `inquiryId` may be ignored; create a new inquiry only via the existing bounded
Saga status-inquiry recovery process. `NOT_FOUND` does not authorize a second payout.

**After replay:** inspect Kafka consumer logs and lag, the Saga Inbox/Outbox transaction, Saga
state transitions, and unique Disbursement/Notification records. Check that no second payout or
Notification was created. For wrong destination, the CLI refuses publication. If the original
payload remains invalid, it may simply reenter DLT; that is not recovery.

### Monitoring and incident operations

- Micrometer counters per service/source topic: `lps.kafka.dlt.published` and `lps.kafka.dlt.publish.failed` (`lps_kafka_dlt_published_total`, `lps_kafka_dlt_publish_failed_total` in Prometheus).
- Source consumer group lag via existing Kafka exporter. Do not treat consumer lag alone as a count of unresolved DLT incidents.
- Use Kafka CLI to list `.DLT` topics and read partition offsets; correlate individual offsets with saved operator reports and business DB state.
- No application IDs/command IDs as metric labels. Avoid logging raw DLT value or full exception message: can contain PII.
- If infrastructure is down, restore PostgreSQL/Kafka rather than replaying commands into broken consumers. For persistent business conflicts, consider intentionally scaling the affected consumer deployment to zero while investigating; restart once safe.
- Distinguish diagnostic side effects from business ones: a `loan.application.created` event is consumed by **two groups** and can create two DLT entries for the same original source record.

### Verification checklist (must be run on the developer machine)

1. Unit tests + Kafka Testcontainers DLT test (malformed JSON, same-partition progress, metadata).
2. DLT unavailable test: no offset committed, next record cannot overtake failed recovery.
3. Source commit race/crash test: duplicate DLT records tolerated, no second business effect.
4. Outage/restart/rebalance/two-replica tests for real consumer group offset behavior.
5. Dry-run no-publish, wrong-topic refusal, repeated replay, header/identity preservation.
6. Loan Application Idempotency-Key regression; Saga late Disbursement and Status Inquiry regression.
7. Kubernetes smoke + SQL reconciliation; optional comparable baseline after draining backlog.

The written test suite does **not** yet implement every injected failure in items 2–7. Those are
manual/integration acceptance tasks; mark them NOT RUN until there is actual evidence.

## Separate Performance Experiment №2 — plan only

**Performance Experiment №1, measured (before → after):** polling 1000 → 100 ms,
HTTP p99 782.73 → 229.13 ms; Saga p99 178.53 → 36.53 s; drain 170.6 → 31.1 s;
601/601 baseline Saga complete after drain; no duplicate Disbursement; reconciliation PASS.
Backlog existed at the end of the 60-second 10 applications/sec load phase.

Experiment №2 must run as an independent investigation/commit after Patch 19.4 correctness
verification. Hold workload constant over a longer steady state; record offered/accepted HTTP
rps, completed Saga/sec, Kafka command publication/sec, Outbox NEW/CLAIMED/PUBLISHED/FAILED,
oldest pending age, broker ack durations, PostgreSQL connections/update cost, Kafka group lag,
CPU/memory and drain times. Propose **one** change per controlled run (batch size vs publisher
concurrency vs scheduling), retain only correctness-verified improvements, and make a separate
`perf:` commit. Do not confuse 10 incoming HTTP req/s with 10 Saga completions/s; an approved
Saga publishes approximately five commands in the measured path.
