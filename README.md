# otel-span-link-connector

An OpenTelemetry Collector **traces-to-metrics connector** that derives service graph metrics from **span links**, complementing the existing [servicegraph connector](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/connector/servicegraphconnector).

## Why

The existing `servicegraph` connector only builds service-to-service edges from parent-child span relationships. This misses asynchronous communication patterns where services are connected through **span links** — message queues (Kafka, NATS), database change streams (MongoDB CDC), batch processing, trust boundaries, retry flows, and scatter/gather patterns.

This connector fills that gap by processing span links to produce compatible `traces_service_graph_request_total` metrics with an `edge_relation="link"` label for differentiation.

## Structure

```
connector/spanlinkservicegraphconnector/   # The connector (independent Go module)
docs/                                       # Design plan & contribution guide
examples/                                   # Collector config & docker-compose
```

## Documentation

- **[專案說明與程式結構](docs/project-explanation.md)** — 專案用途、資料流、程式碼結構與核心方法說明（繁體中文）。
- [Design plan](docs/design-plan.md) — 設計計畫與應用端 span link 規範。
- [Contribution guide](docs/contrib-guide.md) — 捐贈至 opentelemetry-collector-contrib 的流程。

## Quick Start

```bash
cd connector/spanlinkservicegraphconnector
go test ./...
```

See [connector README](connector/spanlinkservicegraphconnector/README.md) for configuration details and [examples/](examples/) for a full Collector setup.

## Covered Span Link Use Cases

| Use Case | Status |
|----------|--------|
| Async queued operations (Kafka, NATS, RabbitMQ) | Covered |
| Database change stream / CDC (MongoDB, PostgreSQL) | Covered |
| Batch processing | Covered |
| Trust boundary / new trace generation | Covered |
| Long-running async processing | Covered |
| Retry / reprocessing | Covered |
| Scatter/gather (fork/join) | Covered |

## Contribution Target

This project is structured to align with [opentelemetry-collector-contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib) conventions for eventual contribution. See [docs/contrib-guide.md](docs/contrib-guide.md) for details.

### How to use this package inside `opentelemetry-collector-contrib`

At a high level you have two options:

- **(A) Use as an external Go module via OCB (recommended for early adoption)**
- **(B) Donate the component into `opentelemetry-collector-contrib` and use the official image**

#### A. Use as external module with OpenTelemetry Collector Builder (OCB)

1. **Publish this module** to your Git hosting (e.g., GitHub) and tag a version, for example:

   ```bash
   git remote add origin git@github.com:<your-org>/otel-span-link-connector.git
   git push -u origin feat/spanlinkservicegraph
   git tag v0.1.0
   git push origin v0.1.0
   ```

2. **In your custom Collector builder config**, reference this connector module:

   ```yaml
   # builder-config.yaml
   dist:
     name: otelcol-spanlink
     output_path: ./build

   connectors:
     - gomod: github.com/<your-org>/otel-span-link-connector/connector/spanlinkservicegraphconnector v0.1.0
   ```

3. **Build your custom Collector**:

   ```bash
   ocb --config builder-config.yaml
   ```

4. In the generated Collector’s `config.yaml`, configure the connector as shown in `examples/collector-config.yaml`.

This path完全不需要修改本 repo 的程式碼，只要維持 module path 為：

```text
github.com/<your-org>/otel-span-link-connector/connector/spanlinkservicegraphconnector
```

#### B. Donate into `opentelemetry-collector-contrib` and use officially

當你準備將元件貢獻回 `opentelemetry-collector-contrib` 時，大致步驟如下（細節見 `docs/contrib-guide.md`）：

1. **在 contrib 中建立相同目錄結構**：

   ```text
   connector/spanlinkservicegraphconnector/
   ```

2. **調整 module path**  
   在 `go.mod` 中把 module 改為：

   ```text
   module github.com/open-telemetry/opentelemetry-collector-contrib/connector/spanlinkservicegraphconnector
   ```

   由於本專案一開始就依照 contrib 的檔案結構與 API 設計，因此**程式碼本身幾乎不需要改動**，只需：

   - 更新 `go.mod` module 路徑
   - 更新 import path（若有自引用）

3. **在 contrib 的 `cmd/otelcontribcol/builder-config.yaml` 中登錄此 connector**，使官方 `otelcol-contrib` 二進位能夠載入：

   ```yaml
   connectors:
     - spanlinkservicegraph
   ```

4. **提交 PR 並通過 CI**  
   依照 contrib 的 new-component 流程，開 issue、找 sponsor、送 PR。這部分的細節、CI checklist（`make checkdoc`、`make checkmetadata` 等）已整理在 `docs/contrib-guide.md`。

完成捐贈後，使用者就可以直接在官方 `otelcol-contrib` 映像檔中，於 `connectors:` 區塊加入：

```yaml
connectors:
  spanlinkservicegraph:
    # ... configuration ...
```

整體來說，目前的結構 **同時支援**：

- 直接作為獨立 Go module（透過 OCB 引入）使用
- 幾乎零成本搬移到 `opentelemetry-collector-contrib` 中，作為官方 connector 使用

