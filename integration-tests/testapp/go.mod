module github.com/qiumingzhi/otel-span-link-connector/integration-tests/testapp

go 1.24

require (
	go.opentelemetry.io/otel v1.40.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.40.0
	go.opentelemetry.io/otel/sdk v1.40.0
	go.opentelemetry.io/otel/trace v1.40.0
	github.com/nats-io/nats.go v1.37.0
	go.mongodb.org/mongo-driver v1.17.3
)
