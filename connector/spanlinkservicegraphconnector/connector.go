package spanlinkservicegraphconnector

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type serviceGraphConnector struct {
	config          *Config
	metricsConsumer consumer.Metrics
	telemetry       component.TelemetrySettings
}

func newConnector(telemetry component.TelemetrySettings, cfg *Config, nextConsumer consumer.Metrics) (*serviceGraphConnector, error) {
	return &serviceGraphConnector{
		config:          cfg,
		metricsConsumer: nextConsumer,
		telemetry:       telemetry,
	}, nil
}

func (c *serviceGraphConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *serviceGraphConnector) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (c *serviceGraphConnector) Shutdown(_ context.Context) error {
	return nil
}

func (c *serviceGraphConnector) ConsumeTraces(_ context.Context, _ ptrace.Traces) error {
	return nil
}
