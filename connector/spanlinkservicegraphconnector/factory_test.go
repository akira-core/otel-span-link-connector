package spanlinkservicegraphconnector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer/consumertest"

	"github.com/qiumingzhi/otel-span-link-connector/connector/spanlinkservicegraphconnector/internal/metadata"
)

func TestNewFactory(t *testing.T) {
	f := NewFactory()
	assert.Equal(t, metadata.Type, f.Type())
}

func TestCreateDefaultConfig(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig()
	assert.NotNil(t, cfg)
	assert.NoError(t, cfg.(*Config).Validate())
}

func TestCreateTracesToMetrics(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig()

	conn, err := f.CreateTracesToMetrics(
		context.Background(),
		connectortest.NewNopSettings(metadata.Type),
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)
	assert.NotNil(t, conn)
}

func TestCreateTracesToMetrics_NilConsumer(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig()

	_, err := f.CreateTracesToMetrics(
		context.Background(),
		connectortest.NewNopSettings(metadata.Type),
		cfg,
		nil,
	)
	assert.Error(t, err)
}
