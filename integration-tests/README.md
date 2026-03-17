# 整合測試

本目錄包含 Span Link Service Graph Connector 的整合測試環境，使用 Docker Compose 啟動 Collector、NATS、MongoDB、VictoriaMetrics，並由 testapp 產生帶 span link 的 traces，最後由 verify 透過 Prometheus API 查詢 VictoriaMetrics 驗證 metrics。

## 架構

- **testapp**：Go 程式，依序執行場景 A～F，產生 OTLP traces（含 span link）送至 Collector
- **collector**：自訂 OTel Collector（含 spanlinkservicegraph connector + Prometheus Remote Write exporter）
- **victoriametrics**：接收 metrics，提供 Prometheus 查詢 API
- **verify**：Go 測試，以 `prometheus/client_golang` 查詢 VM 並斷言預期 series 存在

## 前置需求

- Docker、Docker Compose
- 從專案根目錄執行

## 執行方式

```bash
# 從專案根目錄（需 Go 1.24+、Docker、Docker Compose）
chmod +x integration-tests/scripts/run-tests.sh
./integration-tests/scripts/run-tests.sh
```

或手動步驟：

```bash
docker compose -f integration-tests/docker-compose.yaml up -d --build
docker compose -f integration-tests/docker-compose.yaml wait testapp 2>/dev/null || true
sleep 20
docker compose -f integration-tests/docker-compose.yaml run --rm verify
docker compose -f integration-tests/docker-compose.yaml down -v
```

若 verify 出現「no data for query」：表示 metrics 尚未進入 VictoriaMetrics，可先確認 testapp 能連上 collector（testapp 啟動後會等待約 15 秒再送 trace）。必要時可先 `up -d` 僅基礎服務，等待約 30 秒後再 `docker compose run --rm testapp`，接著 sleep 20 再跑 verify。

## 目錄說明

| 路徑 | 說明 |
|------|------|
| `collector/` | Collector 多階段建置、OCB 設定、otelcol 設定 |
| `testapp/` | 產生各場景 traces 的 Go 程式 |
| `verify/` | 查詢 VM 並斷言的 Go 測試 |
| `scripts/run-tests.sh` | 整合測試入口腳本 |

## 測試場景

- **A**：NATS Queue（order-service → payment-service）
- **B**：MongoDB Change Stream（order-service → sync-service）
- **C**：NATS Fan-out（event-source → payment-svc, inventory-svc, notification-svc）
- **D**：跨批次到達（consumer 先到，producer 後到）
- **E**：Retry（order-service → order-service）
- **F**：Mixed（order-service → payment-service → sync-service，NATS + MongoDB）
- **Histograms**：驗證 server/client duration histogram 存在且 sum > 0

詳見 [docs/integration-test-plan.md](../docs/integration-test-plan.md)。
