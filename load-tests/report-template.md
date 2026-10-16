# LPS load testing — Performance Report

## 1. Benchmark environment
- Run ID (8 hexadecimal chars): TODO
- Date/time + timezone: TODO
- Git SHA / Java image tags / k6 image (`grafana/k6:2.3.0`): TODO
- Hardware CPU / RAM / free disk / Docker Desktop limits: TODO
- Docker Desktop Kubernetes node count, namespace, kubectl context: TODO
- Kubernetes Deployment replicas, requests/limits, CPU throttling observations: TODO
- Postgres max_connections=50; actual DB pool settings, Kafka partitions=3 (verify actual topics): TODO
- Monitoring scrape interval, tracing sampling rate, logging level: TODO

## 2. Workload definition
- Test scenario and base URL / network path: TODO
- Warm-up: TODO
- Measurement window and cooldown: TODO
- Configured arrival rate, VUS, maxVUS, duration, timeout: TODO
- Number of expected requests (offered) and actual iterations, dropped iterations: TODO
- DB contains pre-existing test records? How isolated? TODO

## 3. Observed results (fill ONLY from real measurements)
| Metric | Actual value / screenshot or query | Observation |
|---|---|---|
| Successful HTTP POST count | TODO | k6 `lps_accepted_posts` |
| HTTP POST achieved rate | TODO | `lps_accepted_posts / measurement seconds` |
| HTTP POST p50 / p95 / p99 | TODO | client-side k6, milliseconds |
| HTTP 5xx / validation failures | TODO | k6 and Prometheus |
| Saga COMPLETED / FAILED / IN_PROGRESS / reconciliation | TODO | SQL for RUN_ID |
| Saga COMPLETED/sec | TODO | Prometheus + SQL over window |
| Saga p50 / p95 / p99 | TODO | completedAt-createdAt (Saga-only) |
| Kafka lag peak and post-test | TODO | kafka exporter + Kafka CLI |
| Outbox NEW/CLAIMED/FAILED peak and post-test | TODO | metrics + SQL |
| Outbox pending oldest age | TODO | Prometheus |
| Hikari active/pending | TODO | Prometheus |
| CPU/heap/GC / OOM/throttling | TODO | Prometheus + kubectl top |
| Cache HIT/MISS/fallback (GET only) | TODO | Prometheus |
| Backlog drain time | TODO | stop load -> no pending nonterminal Saga/lag |
| Duplicate logical payouts/rejected payouts | TODO | expected zero, SQL |

## 4. Bottleneck analysis
- Evidence (metric, graph, timestamps): TODO
- Hypothesis and likely limiting resource: TODO
- One changed parameter (if justified): TODO
- Same-load before/after results: TODO
- Failure scenarios/reliability signals and rollback: TODO

## 5. Conclusion
- Maximum tested arrival rate vs. achieved accepted rate: TODO
- Sustainable end-to-end processing throughput: TODO
- HTTP p99 <2s? TODO
- Saga p99 <2s? TODO (independent metric, NEVER inferred from HTTP p99)
- Correctness / missing Saga / FAILED / reconciliation counts: TODO
- Unverified assumptions and local hardware limits: TODO
- Candidates for reliability review (only if evidence warrants it): TODO
