# Kind E2E 整合測試

在 [kind](https://kind.sigs.k8s.io/) 模擬的 Kubernetes 叢集中，驗證 **spanlinkservicegraph connector** 搭配 **k8sattributes processor** 的端到端行為。

## 架構

```
testapp Jobs ──OTLP──▶ OTel Collector ──prometheusremotewrite──▶ VictoriaMetrics
                         │
                   k8sattributes processor
                   (enriches traces with pod/namespace/job metadata)
                         │
                   spanlinkservicegraph connector
                   (generates service graph metrics from span links)
```

## 前置需求

- [Docker](https://docs.docker.com/get-docker/)
- [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- Go 1.24+

## 兩階段驗證

### Phase 1：最小閉環（不含 NATS/MongoDB）

驗證 k8sattributes enrichment + spanlinkservicegraph metrics 產出，確認 metrics 帶有 `k8s.pod.name`、`k8s.namespace.name`、`k8s.job.name` 等 labels。

### Phase 2：完整情境（NATS + MongoDB）

在 Phase 1 基礎上加入 NATS 與 MongoDB，重現 queue/change-stream 的 span link 情境。

## 快速開始（一鍵執行）

```bash
# Phase 1
./run-phase1.sh

# Phase 2（需先完成 Phase 1 部署）
./run-phase2.sh
```

## 使用 Makefile（分步操作）

所有 Makefile target 都在 `integration-tests/kind/` 目錄下執行：

```bash
cd integration-tests/kind
```

### 查看所有 target

```bash
make help
```

### Phase 1 分步操作

```bash
# 1. 建立 kind cluster
make kind

# 2. 建置 images
make build-collector
make build-testapp-phase1

# 3. 載入 images 到 kind
make load-images

# 4. 部署基礎設施（RBAC + VictoriaMetrics + Collector）
make deploy-phase1

# 5. 檢查 RBAC 是否正確
make check-rbac

# 6. 執行測試 Jobs
make run-testapp-phase1

# 7. 等待 metrics flush（約 15 秒）
sleep 15

# 8. 在另一個 terminal 做 port-forward
make port-forward-vm

# 9. 執行驗證
make verify-phase1
```

### Phase 2 分步操作

```bash
# 1. 部署 NATS + MongoDB
make deploy-phase2

# 2. 建置並載入 Phase2 testapp
make build-testapp-phase2
make load-testapp-phase2

# 3. 執行 Phase2 testapp
make deploy-testapp-phase2

# 4. 驗證
make verify-phase2
```

### 全自動 Phase 1

```bash
make e2e-phase1
```

### 清理

```bash
# 刪除所有 testapp jobs
make clean-jobs

# 刪除 kind cluster
make clean
```

## 除錯

```bash
# 檢視 collector logs
make logs-collector

# 檢視 testapp logs
make logs-testapp

# 驗證 RBAC 權限
make check-rbac

# 手動查詢 VictoriaMetrics（需先 port-forward）
curl 'http://localhost:8428/api/v1/query?query=traces_service_graph_request_total'
```

## 常見問題

### RBAC 錯誤

如果 collector logs 出現 `forbidden` 或 `unauthorized` 錯誤：

1. 確認 RBAC manifest 已套用：`make deploy-rbac`
2. 驗證權限：`make check-rbac`（應全部回 `yes`）
3. 確認 collector Deployment 的 `serviceAccountName` 設為 `otel-collector`

### k8sattributes 無法取得 pod 資訊

1. 確認 `pod_association` 設定為 `from: connection`
2. 檢查 collector log 中是否有 `k8s.watcher.pod.added` 相關訊息
3. 確認 ClusterRole 包含 `batch/jobs` 的 get/list/watch（若要取 `k8s.job.name`）

### Metrics 沒有 k8s labels

1. 確認 collector config 中 `processors: [k8sattributes]` 在 traces pipeline
2. 確認 spanlinkservicegraph 的 `dimensions` 設定包含 `k8s.pod.name` 等 source_attribute
3. 確認 connector 版本支援 resource attributes 作為 dimensions 來源

## 目錄結構

```
integration-tests/kind/
├── Makefile                    # 個別操作 target
├── README.md                   # 本文件
├── run-phase1.sh               # Phase 1 一鍵腳本
├── run-phase2.sh               # Phase 2 一鍵腳本
├── collector/
│   ├── Dockerfile              # 全量 contrib + spanlinkservicegraph
│   ├── builder-config.yaml     # OCB manifest (otelcontribcol v0.147.0 全量)
│   └── otelcol-config.yaml     # K8s 專用 collector pipeline 設定
├── testapp-phase1/
│   ├── Dockerfile
│   ├── go.mod
│   └── main.go                 # 簡化版 testapp（in-process span links）
├── verify-phase1/
│   ├── go.mod
│   └── verify_test.go          # Phase1 驗證（k8s labels 存在性）
└── manifests/
    ├── rbac.yaml               # ServiceAccount + ClusterRole + Binding
    ├── collector.yaml          # ConfigMap + Deployment + Service
    ├── victoriametrics.yaml    # VM Deployment + Service
    ├── testapp-phase1.yaml     # Phase1 testapp Jobs
    ├── nats.yaml               # NATS Deployment + Service (Phase2)
    ├── mongodb.yaml            # MongoDB Deployment + Service + Init Job (Phase2)
    └── testapp-phase2.yaml     # Phase2 testapp Job
```
