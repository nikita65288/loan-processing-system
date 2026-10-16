# Local Kubernetes deployment (Docker Desktop)

This is a hybrid developer topology: **7 Java Deployments** in Docker Desktop Kubernetes, PostgreSQL/Kafka/Redis in Docker Compose with named volumes. `lps-contracts` is a library, not a Deployment. The supplied manifests assume the `docker-desktop` context and `host.docker.internal` DNS connectivity; **do not apply to a shared/production cluster**.

Use the short [root quick start](../README.md#run-locally-windows--powershell), then diagnose with these commands from the repository root:

```powershell
kubectl config current-context
kubectl -n lps get deployments,pods,services
kubectl -n lps get events --sort-by=.lastTimestamp
kubectl -n lps logs deployment/saga-orchestrator-service --tail=100
kubectl -n lps describe pod <pod-name>
docker compose ps
```

- On first setup, create `k8s/local/secrets.env` from `secrets.env.example`, using the same PostgreSQL username/password as `.env`. The `lps-postgres` Kubernetes Secret must exist before Deployments start. Existing Secret can be updated via `--dry-run=client -o yaml | kubectl apply -f -` and the affected Pods restarted.
- The image tag must match **both** `docker/build-images.ps1 -Tag local-v1` and seven `k8s/local/deployments/*.yaml` files. Docker Desktop Kubernetes may require images to be available in its container runtime; if Pods show `ImagePullBackOff`, verify runtime/image store and `imagePullPolicy: IfNotPresent`.
- Kafka listeners: Windows `localhost:9092`, Compose `kafka:29092`, K8s `host.docker.internal:19092`. Pods access Compose PostgreSQL on `host.docker.internal:5432` and Redis on `6379`. Verify access inside Pods if connections fail.
- Use `kubectl -n lps port-forward service/loan-application-service 18080:8080` and `kubectl -n lps port-forward service/saga-orchestrator-service 18084:8080` in separate PowerShell terminals. Test API via **Postman** as documented in the root README.
- For optional K8s monitoring: copy `k8s/local/grafana-secrets.env.example` to the ignored `k8s/local/grafana-secrets.env` and edit it. Run `kubectl -n lps create secret generic lps-grafana --from-env-file=k8s/local/grafana-secrets.env`, then `kubectl apply -k k8s/monitoring` and `docker compose --profile k8s-monitoring up -d kafka-exporter`. Port-forward `service/lps-k8s-prometheus 19090:9090` and `service/lps-k8s-grafana 13000:3000`.
- Trace UI: `docker compose --profile tracing up -d jaeger`, visit `http://localhost:16686`.
- Rollout health: `kubectl -n lps rollout status deployment/loan-application-service` and similar for other deployments. The cluster is single-node, **not** HA.

**Cleanup:** `kubectl delete -k k8s/monitoring` for optional monitoring. `kubectl delete -k k8s/local` also deletes the `lps` namespace and all namespaced workloads in it. Never run `docker compose down -v` unless intentionally deleting all persisted local data.
