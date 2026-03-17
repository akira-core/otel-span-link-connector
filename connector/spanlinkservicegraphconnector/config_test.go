package spanlinkservicegraphconnector

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestLoadConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	sub, err := cm.Sub("spanlinkservicegraph")
	require.NoError(t, err)

	cfg := createDefaultConfig().(*Config)
	err = sub.Unmarshal(cfg)
	require.NoError(t, err)

	assert.Equal(t, 30*time.Second, cfg.Store.TTL)
	assert.Equal(t, 10000, cfg.Store.MaxItems)
	assert.Equal(t, 1*time.Minute, cfg.CacheLoop)
	assert.Equal(t, 2*time.Second, cfg.StoreExpirationLoop)
	assert.Equal(t, 60*time.Second, *cfg.MetricsFlushInterval)

	require.Len(t, cfg.Dimensions, 3)
	assert.Equal(t, "messaging_system", cfg.Dimensions[0].Name)
	assert.Equal(t, "messaging.system", cfg.Dimensions[0].SourceAttribute)
	assert.Equal(t, "link_type", cfg.Dimensions[1].Name)
	assert.Equal(t, "link_type", cfg.Dimensions[1].SourceAttribute)
	assert.Equal(t, "deployment_environment", cfg.Dimensions[2].Name)
	assert.Equal(t, "deployment.environment", cfg.Dimensions[2].SourceAttribute)

	require.Len(t, cfg.LatencyHistogramBuckets, 11)
}

func TestConfigValidate_Valid(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	assert.NoError(t, cfg.Validate())
}

func TestConfigValidate_InvalidTTL(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Store.TTL = 0
	assert.Error(t, cfg.Validate())
}

func TestConfigValidate_InvalidMaxItems(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Store.MaxItems = 0
	assert.Error(t, cfg.Validate())
}

func TestConfigValidate_EmptyDimensionName(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Dimensions = []Dimension{{Name: "", SourceAttribute: "foo"}}
	assert.Error(t, cfg.Validate())
}

func TestConfigValidate_EmptyDimensionSource(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Dimensions = []Dimension{{Name: "foo", SourceAttribute: ""}}
	assert.Error(t, cfg.Validate())
}

func TestDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)

	assert.Equal(t, 30*time.Second, cfg.Store.TTL)
	assert.Equal(t, 10000, cfg.Store.MaxItems)
	assert.Equal(t, 1*time.Minute, cfg.CacheLoop)
	assert.Equal(t, 2*time.Second, cfg.StoreExpirationLoop)
	require.NotNil(t, cfg.MetricsFlushInterval)
	assert.Equal(t, 60*time.Second, *cfg.MetricsFlushInterval)
	assert.Len(t, cfg.LatencyHistogramBuckets, 11)
	assert.Empty(t, cfg.Dimensions)
}
