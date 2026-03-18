#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-spanlink-e2e}"
TESTAPP_P2_IMAGE="testapp-phase2:latest"

echo "=== Phase 2: Kind E2E Test (NATS + MongoDB) ==="
echo "Note: Phase 1 infrastructure must already be deployed."

# 1. Build Phase2 testapp image (reuses existing integration-tests/testapp)
echo "--- Building Phase2 testapp image ---"
docker build -t "$TESTAPP_P2_IMAGE" -f "$ROOT_DIR/integration-tests/testapp/Dockerfile" "$ROOT_DIR/integration-tests/testapp"

echo "--- Loading Phase2 testapp image into kind ---"
kind load docker-image "$TESTAPP_P2_IMAGE" --name "$KIND_CLUSTER_NAME"

# 2. Deploy NATS and MongoDB
echo "--- Deploying NATS ---"
kubectl apply -f "$SCRIPT_DIR/manifests/nats.yaml"

echo "--- Deploying MongoDB ---"
kubectl apply -f "$SCRIPT_DIR/manifests/mongodb.yaml"

echo "--- Waiting for NATS and MongoDB ---"
kubectl rollout status deployment/nats --timeout=120s
kubectl rollout status deployment/mongodb --timeout=120s

echo "--- Waiting for MongoDB init job ---"
kubectl wait --for=condition=complete job/mongodb-init --timeout=120s

# 3. Deploy Phase2 testapp
echo "--- Deploying Phase2 testapp ---"
kubectl apply -f "$SCRIPT_DIR/manifests/testapp-phase2.yaml"

echo "--- Waiting for Phase2 testapp job to complete ---"
kubectl wait --for=condition=complete job/testapp-phase2 --timeout=180s

# 4. Wait for metrics flush
echo "--- Waiting 15s for metrics flush ---"
sleep 15

# 5. Verify
echo "--- Running Phase2 verification ---"
kubectl port-forward svc/victoriametrics 8428:8428 &
PF_PID=$!
sleep 2

cd "$ROOT_DIR/integration-tests/verify"
VM_URL="http://localhost:8428" go test -v -count=1 ./...
TEST_EXIT=$?

kill $PF_PID 2>/dev/null || true

if [ $TEST_EXIT -eq 0 ]; then
  echo "=== Phase 2 PASSED ==="
else
  echo "=== Phase 2 FAILED ==="
  exit 1
fi
