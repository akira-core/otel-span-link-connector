KIND_CLUSTER_NAME ?= spanlink-e2e
COLLECTOR_IMAGE   ?= otelcol-spanlink-contrib:latest
TESTAPP_P1_IMAGE  ?= testapp-phase1:latest
TESTAPP_P2_IMAGE  ?= testapp-phase2:latest
VM_IMAGE          ?= victoriametrics/victoria-metrics:latest

ROOT_DIR := $(shell cd ../.. && pwd)
KIND_DIR := $(shell pwd)

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-28s\033[0m %s\n", $$1, $$2}'

# ──────────────────────────── Cluster ────────────────────────────

.PHONY: kind
kind: ## Create kind cluster
	@if kind get clusters 2>/dev/null | grep -q "^$(KIND_CLUSTER_NAME)$$"; then \
		echo "Cluster $(KIND_CLUSTER_NAME) already exists"; \
	else \
		kind create cluster --name $(KIND_CLUSTER_NAME); \
	fi

.PHONY: kind-delete
kind-delete: ## Delete kind cluster
	kind delete cluster --name $(KIND_CLUSTER_NAME)

# ──────────────────────────── Build ────────────────────────────

.PHONY: build-collector
build-collector: ## Build collector image (full contrib + spanlinkservicegraph)
	docker build -t $(COLLECTOR_IMAGE) -f $(KIND_DIR)/collector/Dockerfile $(ROOT_DIR)

.PHONY: build-testapp-phase1
build-testapp-phase1: ## Build Phase1 testapp image
	docker build -t $(TESTAPP_P1_IMAGE) -f $(KIND_DIR)/testapp-phase1/Dockerfile $(KIND_DIR)/testapp-phase1

.PHONY: build-testapp-phase2
build-testapp-phase2: ## Build Phase2 testapp image (reuses integration-tests/testapp)
	docker build -t $(TESTAPP_P2_IMAGE) -f $(ROOT_DIR)/integration-tests/testapp/Dockerfile $(ROOT_DIR)/integration-tests/testapp

.PHONY: build-all
build-all: build-collector build-testapp-phase1 ## Build all images for Phase1

# ──────────────────────────── Load ────────────────────────────

.PHONY: load-collector
load-collector: ## Load collector image into kind
	kind load docker-image $(COLLECTOR_IMAGE) --name $(KIND_CLUSTER_NAME)

.PHONY: load-testapp-phase1
load-testapp-phase1: ## Load Phase1 testapp image into kind
	kind load docker-image $(TESTAPP_P1_IMAGE) --name $(KIND_CLUSTER_NAME)

.PHONY: load-testapp-phase2
load-testapp-phase2: ## Load Phase2 testapp image into kind
	kind load docker-image $(TESTAPP_P2_IMAGE) --name $(KIND_CLUSTER_NAME)

.PHONY: load-images
load-images: load-collector load-testapp-phase1 ## Load all Phase1 images into kind

.PHONY: load-images-phase2
load-images-phase2: load-collector load-testapp-phase1 load-testapp-phase2 ## Load all images (Phase1+2) into kind

# ──────────────────────────── Deploy ────────────────────────────

.PHONY: deploy-rbac
deploy-rbac: ## Apply RBAC manifests (ServiceAccount, ClusterRole, ClusterRoleBinding)
	kubectl apply -f $(KIND_DIR)/manifests/rbac.yaml

.PHONY: deploy-victoriametrics
deploy-victoriametrics: ## Deploy VictoriaMetrics
	kubectl apply -f $(KIND_DIR)/manifests/victoriametrics.yaml

.PHONY: deploy-collector
deploy-collector: ## Deploy OTel Collector (ConfigMap + Deployment + Service)
	kubectl apply -f $(KIND_DIR)/manifests/collector.yaml

.PHONY: deploy-testapp-phase1
deploy-testapp-phase1: ## Deploy Phase1 testapp Jobs
	kubectl apply -f $(KIND_DIR)/manifests/testapp-phase1.yaml

.PHONY: deploy-phase1
deploy-phase1: deploy-rbac deploy-victoriametrics deploy-collector ## Deploy all Phase1 resources (RBAC + VM + Collector)
	@echo "Waiting for deployments..."
	kubectl rollout status deployment/otel-collector --timeout=120s
	kubectl rollout status deployment/victoriametrics --timeout=120s
	@echo "Phase1 infrastructure ready. Run 'make run-testapp-phase1' to start test jobs."

.PHONY: run-testapp-phase1
run-testapp-phase1: deploy-testapp-phase1 ## Deploy and wait for Phase1 testapp jobs
	kubectl wait --for=condition=complete job/testapp-order-service --timeout=120s
	kubectl wait --for=condition=complete job/testapp-payment-service --timeout=120s
	@echo "Phase1 testapp jobs completed."

.PHONY: deploy-nats
deploy-nats: ## Deploy NATS (Phase2)
	kubectl apply -f $(KIND_DIR)/manifests/nats.yaml

.PHONY: deploy-mongodb
deploy-mongodb: ## Deploy MongoDB (Phase2)
	kubectl apply -f $(KIND_DIR)/manifests/mongodb.yaml

.PHONY: deploy-testapp-phase2
deploy-testapp-phase2: ## Deploy Phase2 testapp
	kubectl apply -f $(KIND_DIR)/manifests/testapp-phase2.yaml

.PHONY: deploy-phase2
deploy-phase2: deploy-nats deploy-mongodb ## Deploy Phase2 additional resources (NATS + MongoDB)
	@echo "Waiting for NATS and MongoDB..."
	kubectl rollout status deployment/nats --timeout=120s
	kubectl rollout status deployment/mongodb --timeout=120s
	@echo "Phase2 infrastructure ready."

# ──────────────────────────── Verify ────────────────────────────

.PHONY: port-forward-vm
port-forward-vm: ## Port-forward VictoriaMetrics to localhost:8428
	@echo "Port-forwarding VictoriaMetrics to localhost:8428 (Ctrl+C to stop)"
	kubectl port-forward svc/victoriametrics 8428:8428

.PHONY: verify-phase1
verify-phase1: ## Run Phase1 verification tests (requires port-forward-vm in another terminal)
	cd $(KIND_DIR)/verify-phase1 && VM_URL="http://localhost:8428" go test -v -count=1 ./...

.PHONY: verify-phase2
verify-phase2: ## Run Phase2 verification tests (requires port-forward-vm in another terminal)
	cd $(ROOT_DIR)/integration-tests/verify && VM_URL="http://localhost:8428" go test -v -count=1 ./...

# ──────────────────────────── All-in-one ────────────────────────────

.PHONY: e2e-phase1
e2e-phase1: kind build-all load-images deploy-phase1 run-testapp-phase1 ## Full Phase1 E2E: cluster + build + deploy + test jobs
	@echo "Waiting 15s for metrics flush..."
	@sleep 15
	@echo "Starting port-forward and verification..."
	@kubectl port-forward svc/victoriametrics 8428:8428 &
	@sleep 2
	@cd $(KIND_DIR)/verify-phase1 && VM_URL="http://localhost:8428" go test -v -count=1 ./... ; \
		TEST_EXIT=$$? ; \
		kill %1 2>/dev/null || true ; \
		exit $$TEST_EXIT

# ──────────────────────────── Cleanup ────────────────────────────

.PHONY: clean-jobs
clean-jobs: ## Delete all testapp jobs
	kubectl delete jobs -l app=testapp-phase1 --ignore-not-found
	kubectl delete jobs -l app=testapp-phase2 --ignore-not-found

.PHONY: clean
clean: kind-delete ## Delete kind cluster and clean up
	@echo "Cleaned up."

# ──────────────────────────── Debug ────────────────────────────

.PHONY: logs-collector
logs-collector: ## Show collector logs
	kubectl logs -l app=otel-collector --tail=100

.PHONY: logs-testapp
logs-testapp: ## Show testapp job logs
	@for job in $$(kubectl get jobs -l app=testapp-phase1 -o name); do \
		echo "--- $$job ---"; \
		kubectl logs $$job --tail=50; \
	done

.PHONY: check-rbac
check-rbac: ## Verify RBAC permissions for collector service account
	@echo "Checking RBAC for otel-collector service account..."
	@kubectl auth can-i list pods --as=system:serviceaccount:default:otel-collector
	@kubectl auth can-i watch pods --as=system:serviceaccount:default:otel-collector
	@kubectl auth can-i list namespaces --as=system:serviceaccount:default:otel-collector
	@kubectl auth can-i list jobs --as=system:serviceaccount:default:otel-collector --namespace=default
