# k6 load testing and PostgreSQL reconciliation

Runs **locally** against the seven-service Docker Desktop Kubernetes deployment. `kubectl` must point to `docker-desktop`; all commands are run in PowerShell from repository root. Load testing is separated from `mvn test`.

```powershell
# Optional Node-based tests of test scripts (requires Node.js):
cd load-tests
npm test
cd ..

# k6 Job + automatic run-scoped SQL reconciliation + Markdown report:
.\load-tests\run-and-report.ps1 -Scenario smoke
.\load-tests\run-and-report.ps1 -Scenario baseline
# Advanced scenarios only after verifying queue drain and sufficient local capacity:
.\load-tests\run-and-report.ps1 -Scenario ramp-up
```

The wrapper invokes `run-k8s.ps1`, extracts machine-readable k6 output, and uses read-only SQL via `docker compose exec -T postgres` to check accepted applications, Saga terminal states, Inbox/Outbox and simulated disbursement uniqueness. Output is written to ignored `load-tests/results/` and reports can be regenerated using `collect-report.ps1 -RunId <8-hex-id> -Scenario baseline -DrainTimeoutSeconds 0`.

**Interpretation:** HTTP p99 is not Saga end-to-end duration; target arrival rate is not achieved rate. k6 dropped iterations, remaining Outbox backlog, unresolved Saga and missing telemetry must be reported. Workload shares resources with the local cluster and cannot establish production scalability. See [measured results](../docs/performance.md).

To inspect SQL independently: `load-tests/sql/post-run.sql` and `load-tests/sql/run-summary.sql` (read-only queries). No database cleanup is required by the scripts. Do not run tests against real/shared infrastructure; no real financial operations are performed.
