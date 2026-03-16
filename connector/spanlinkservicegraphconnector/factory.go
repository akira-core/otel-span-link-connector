package spanlinkservicegraphconnector

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"

	"github.com/qiumingzhi/otel-span-link-connector/connector/spanlinkservicegraphconnector/internal/metadata"
)

var defaultLatencyHistogramBuckets = []time.Duration{
	5 * time.Millisecond,
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
	2 * time.Second,
	5 * time.Second,
	10 * time.Second,
}

const (
	defaultStoreTTL             = 30 * time.Second
	defaultStoreMaxItems        = 10000
	defaultCacheLoop            = 1 * time.Minute
	defaultStoreExpirationLoop  = 2 * time.Second
	defaultMetricsFlushInterval = 60 * time.Second
)

func NewFactory() connector.Factory {
	return connector.NewFactory(
		metadata.Type,
		createDefaultConfig,
		connector.WithTracesToMetrics(createTracesToMetricsConnector, metadata.TracesToMetricsStability),
	)
}

func createDefaultConfig() component.Config {
	flushInterval := defaultMetricsFlushInterval
	return &Config{
		LatencyHistogramBuckets: defaultLatencyHistogramBuckets,
		Store: StoreConfig{
			TTL:      defaultStoreTTL,
			MaxItems: defaultStoreMaxItems,
		},
		CacheLoop:            defaultCacheLoop,
		StoreExpirationLoop:  defaultStoreExpirationLoop,
		MetricsFlushInterval: &flushInterval,
	}
}

func createTracesToMetricsConnector(
	_ context.Context,
	params connector.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (connector.Traces, error) {
	c := cfg.(*Config)
	return newConnector(params.TelemetrySettings, c, nextConsumer)
}
