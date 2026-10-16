# Local load-test evidence

Automated k6 HTTP load and run-scoped PostgreSQL reconciliation, on Docker Desktop Kubernetes with PostgreSQL/Kafka/Redis in Compose. These are **observations**, not universal capacity claims.

| Test | HTTP accepted | HTTP p99 | Saga COMPLETED at final snapshot | Result |
|---|---:|---:|---:|---|
| Smoke (2/s) | 40 | 23.0 ms | 40/40 | Data integrity PASS |
| Baseline (10/s) | 601 | 47.6 ms | 601/601 | Data integrity PASS |
| Ramp-up (peak 100/s) | 6,207 | 640.8 ms | 194/6,207 | INCONCLUSIVE; 242 dropped iterations |
| Steady-state (20/s) | 3,600 | 107.1 ms | 0/3,600 | INCONCLUSIVE; large backlog |
| Spike (configured 20/s) | 390 | 49.1 ms | 0/390 | INCONCLUSIVE; low achieved rate, backlog |

Baseline Saga duration p99 from Saga creation to completion was **143.95 s** (completed only). HTTP latency does not include Saga processing. A successful POST does not prove a completed Saga. Inconclusive runs had nonterminal Saga / unpublished Outbox records at the report snapshot; this is not proof that events were lost. Some scenarios had pre-existing backlog and k6 saturation, so they cannot establish independent sustainable throughput.

The reports' SQL counts are scoped by synthetic run identifier where possible; some Inbox metrics are global. Raw per-run logs and local database snapshots are intentionally excluded from the public repository. To reproduce and compare results, use [the k6 scripts](../load-tests/README.md) and keep the generated reports locally.

**Next investigation:** isolate Outbox publishing capacity vs Kafka/database contention, measure drain time and Kafka lag, improve sustained end-to-end throughput without relaxing idempotency or delivery guarantees; then repeat the tests under controlled baseline conditions.
