$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
Set-Location $root
$context = (kubectl config current-context).Trim()
if ($LASTEXITCODE -ne 0) { throw 'kubectl is unavailable' }
if ($context -ne 'docker-desktop') { throw "Unexpected Kubernetes context '$context': expected docker-desktop" }
Write-Host 'Checking LPS Kustomize manifests'
kubectl kustomize k8s/local | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Kustomize rendering failed' }
# The API server verifies supported fields and resource validation, without persistence.
kubectl apply --dry-run=server -k k8s/local
if ($LASTEXITCODE -ne 0) { throw 'Kubernetes schema validation failed' }
Write-Host 'Checking optional monitoring Kustomize manifests'
kubectl kustomize k8s/monitoring | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Monitoring rendering failed' }
Write-Host 'Static/server dry-run checks passed; no resources were deployed.'
