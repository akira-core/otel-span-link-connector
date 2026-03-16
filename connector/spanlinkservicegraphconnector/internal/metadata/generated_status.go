package metadata

import "go.opentelemetry.io/collector/component"

var (
	Type                      = component.MustNewType("spanlinkservicegraph")
	ScopeName                 = "github.com/qiumingzhi/otel-span-link-connector/connector/spanlinkservicegraphconnector"
	TracesToMetricsStability   = component.StabilityLevelDevelopment
)
