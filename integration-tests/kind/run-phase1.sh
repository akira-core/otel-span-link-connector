#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-spanlink-e2e}"
COLLECTOR_IMAGE="otelcol-spanlink-contrib:latest"
TESTAPP_IMAGE="testapp-phase1:latest"

echo "=== Phase 1: Kind E2E Test ==="

# 1. Create kind cluster
echo "--- Creating kind cluster: $KIND_CLUSTER_NAME ---"
if kind get clusters 2>/dev/null | grep -q "^${KIND_CLUSTER_NAME}$"; then
  echo "Cluster already exists, reusing."
else
  kind create cluster --name "$KIND_CLUSTER_NAME"
fi

# 2. Build images
echo "--- Building collector image ---"
docker build -t "$COLLECTOR_IMAGE" -f "$SCRIPT_DIR/collector/Dockerfile" "$ROOT_DIR"

echo "--- Building testapp-phase1 image ---"
docker build -t "$TESTAPP_IMAGE" -f "$SCRIPT_DIR/testapp-phase1/Dockerfile" "$SCRIPT_DIR/testapp-phase1"

# 3. Load images into kind
echo "--- Loading images into kind ---"
kind load docker-image "$COLLECTOR_IMAGE" --name "$KIND_CLUSTER_NAME"
kind load docker-image "$TESTAPP_IMAGE" --name "$KIND_CLUSTER_NAME"

# 4. Deploy manifests
echo "--- Deploying RBAC ---"
kubectl apply -f "$SCRIPT_DIR/manifests/rbac.yaml"

echo "--- Deploying VictoriaMetrics ---"
kubectl apply -f "$SCRIPT_DIR/manifests/victoriametrics.yaml"

echo "--- Deploying Collector ---"
kubectl apply -f "$SCRIPT_DIR/manifests/collector.yaml"

echo "--- Waiting for collector to be ready ---"
kubectl rollout status deployment/otel-collector --timeout=120s
kubectl rollout status deployment/victoriametrics --timeout=120s

# 5. Deploy testapp jobs (remove any existing jobs so re-runs use new pods/image)
echo "--- Deploying Phase1 testapp jobs ---"
kubectl delete job testapp-order-service testapp-payment-service --ignore-not-found
kubectl apply -f "$SCRIPT_DIR/manifests/testapp-phase1.yaml"

# 6. Wait for jobs to complete
echo "--- Waiting for testapp jobs to complete ---"
kubectl wait --for=condition=complete job/testapp-order-service --timeout=120s
kubectl wait --for=condition=complete job/testapp-payment-service --timeout=120s

# 7. Wait for metrics flush
echo "--- Waiting 15s for metrics flush ---"
sleep 15

# 8. Verify
echo "--- Running Phase1 verification ---"
kubectl port-forward svc/victoriametrics 8428:8428 &
PF_PID=$!
sleep 2

cd "$SCRIPT_DIR/verify-phase1"
VM_URL="http://localhost:8428" go test -v -count=1 ./...
TEST_EXIT=$?

kill $PF_PID 2>/dev/null || true

if [ $TEST_EXIT -eq 0 ]; then
  echo "=== Phase 1 PASSED ==="
else
  echo "=== Phase 1 FAILED ==="
  exit 1
fi
