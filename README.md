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
