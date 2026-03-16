package spanlinkservicegraphconnector

import (
	"fmt"
	"time"
)

type Config struct {
	LatencyHistogramBuckets []time.Duration `mapstructure:"latency_histogram_buckets"`
	Dimensions              []Dimension     `mapstructure:"dimensions"`
	Store                   StoreConfig     `mapstructure:"store"`
	CacheLoop               time.Duration   `mapstructure:"cache_loop"`
	StoreExpirationLoop     time.Duration   `mapstructure:"store_expiration_loop"`
	MetricsFlushInterval    *time.Duration  `mapstructure:"metrics_flush_interval"`
}

type Dimension struct {
	Name            string `mapstructure:"name"`
	SourceAttribute string `mapstructure:"source_attribute"`
}

type StoreConfig struct {
	MaxItems int           `mapstructure:"max_items"`
	TTL      time.Duration `mapstructure:"ttl"`
}

func (c *Config) Validate() error {
	if c.Store.TTL <= 0 {
		return fmt.Errorf("store.ttl must be positive, got %v", c.Store.TTL)
	}
	if c.Store.MaxItems <= 0 {
		return fmt.Errorf("store.max_items must be positive, got %d", c.Store.MaxItems)
	}
	for i, d := range c.Dimensions {
		if d.Name == "" {
			return fmt.Errorf("dimensions[%d].name must not be empty", i)
		}
		if d.SourceAttribute == "" {
			return fmt.Errorf("dimensions[%d].source_attribute must not be empty", i)
		}
	}
	return nil
}
