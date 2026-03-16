//go:generate mdatagen metadata.yaml

// Package spanlinkservicegraphconnector provides a connector that derives
// service graph metrics from span links, complementing the existing
// servicegraph connector which only uses parent-child relationships.
package spanlinkservicegraphconnector // import "github.com/qiumingzhi/otel-span-link-connector/connector/spanlinkservicegraphconnector"
