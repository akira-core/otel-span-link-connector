package spanlinkservicegraphconnector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/qiumingzhi/otel-span-link-connector/connector/spanlinkservicegraphconnector/internal/metadata"
	"github.com/qiumingzhi/otel-span-link-connector/connector/spanlinkservicegraphconnector/internal/store"
)

const (
	metricReqTotal       = "traces_service_graph_request_total"
	metricReqFailedTotal = "traces_service_graph_request_failed_total"
	metricReqServerHist  = "traces_service_graph_request_server"
	metricReqClientHist  = "traces_service_graph_request_client"
)

type metricSeries struct {
	reqTotal       int64
	reqFailedTotal int64
	serverDurations []float64
	clientDurations []float64
	dimensions      map[string]string
}

type serviceGraphConnector struct {
	config          *Config
	metricsConsumer consumer.Metrics
	telemetry       component.TelemetrySettings
	logger          *zap.Logger

	spanIndex    *store.SpanIndexStore
	pendingLinks *store.PendingLinksStore

	seriesMu     sync.Mutex
	keyToMetric  map[string]*metricSeries

	shutdownCh chan struct{}
	doneWg     sync.WaitGroup
}

func newConnector(telemetry component.TelemetrySettings, cfg *Config, nextConsumer consumer.Metrics) (*serviceGraphConnector, error) {
	c := &serviceGraphConnector{
		config:          cfg,
		metricsConsumer: nextConsumer,
		telemetry:       telemetry,
		logger:          telemetry.Logger,
		spanIndex:       store.NewSpanIndexStore(cfg.Store.TTL, cfg.Store.MaxItems),
		pendingLinks:    store.NewPendingLinksStore(cfg.Store.TTL, cfg.Store.MaxItems),
		keyToMetric:     make(map[string]*metricSeries),
		shutdownCh:      make(chan struct{}),
	}

	c.spanIndex.SetOnExpire(func(n int) {
		c.logger.Debug("expired spans from index", zap.Int("count", n))
	})
	c.spanIndex.SetOnDrop(func(n int) {
		c.logger.Warn("dropped spans due to store capacity", zap.Int("count", n))
	})
	c.pendingLinks.SetOnExpire(func(n int) {
		c.logger.Debug("expired pending links", zap.Int("count", n))
	})

	return c, nil
}

func (c *serviceGraphConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *serviceGraphConnector) Start(_ context.Context, _ component.Host) error {
	if c.config.MetricsFlushInterval != nil && *c.config.MetricsFlushInterval > 0 {
		c.doneWg.Add(1)
		go c.metricFlushLoop()
	}

	c.doneWg.Add(1)
	go c.storeExpirationLoop()

	return nil
}

func (c *serviceGraphConnector) Shutdown(_ context.Context) error {
	close(c.shutdownCh)
	c.doneWg.Wait()
	return nil
}

func (c *serviceGraphConnector) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	c.aggregateMetrics(td)

	if c.config.MetricsFlushInterval != nil && *c.config.MetricsFlushInterval == 0 {
		return c.flushMetrics(ctx)
	}
	return nil
}

func (c *serviceGraphConnector) aggregateMetrics(td ptrace.Traces) {
	rss := td.ResourceSpans()

	// Phase 1: Index all spans and resolve any pending links waiting for them
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		serviceName := findServiceName(rs.Resource())
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				key := store.SpanKey{
					TraceID: span.TraceID(),
					SpanID:  span.SpanID(),
				}

				info := store.SpanInfo{
					ServiceName: serviceName,
					StartTime:   span.StartTimestamp(),
					EndTime:     span.EndTimestamp(),
					StatusCode:  span.Status().Code(),
					Attributes:  c.extractRelevantAttributes(span),
				}

				c.spanIndex.Put(key, info)

				if pending, ok := c.pendingLinks.GetAndDelete(key); ok {
					for _, p := range pending {
						c.onLinkResolved(info.ServiceName, p.DstService, &info, &p.DstSpanInfo, p.LinkAttrs)
					}
				}
			}
		}
	}

	// Phase 2: Process span links
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		dstService := findServiceName(rs.Resource())
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				links := span.Links()
				if links.Len() == 0 {
					continue
				}

				dstSpanInfo := store.SpanInfo{
					ServiceName: dstService,
					StartTime:   span.StartTimestamp(),
					EndTime:     span.EndTimestamp(),
					StatusCode:  span.Status().Code(),
					Attributes:  c.extractRelevantAttributes(span),
				}

				for l := 0; l < links.Len(); l++ {
					link := links.At(l)
					linkKey := store.SpanKey{
						TraceID: link.TraceID(),
						SpanID:  link.SpanID(),
					}

					if srcInfo, ok := c.spanIndex.Get(linkKey); ok {
						c.onLinkResolved(srcInfo.ServiceName, dstService, &srcInfo, &dstSpanInfo, link.Attributes())
					} else {
						c.pendingLinks.Append(linkKey, store.PendingEdge{
							DstService:  dstService,
							DstSpanInfo: dstSpanInfo,
							LinkAttrs:   copyMap(link.Attributes()),
						})
					}
				}
			}
		}
	}
}

func (c *serviceGraphConnector) onLinkResolved(clientService, serverService string, srcInfo, dstInfo *store.SpanInfo, linkAttrs pcommon.Map) {
	srcAttrs := srcInfo.Attributes
	dstAttrs := dstInfo.Attributes

	connectionType := resolveConnectionType(linkAttrs, dstAttrs, srcAttrs)
	failed := dstInfo.StatusCode == ptrace.StatusCodeError

	dims := make(map[string]string)
	dims["client"] = clientService
	dims["server"] = serverService
	dims["connection_type"] = connectionType
	dims["edge_relation"] = "link"
	if failed {
		dims["failed"] = "true"
	} else {
		dims["failed"] = "false"
	}

	for _, dim := range c.config.Dimensions {
		dims[dim.Name] = resolveDimension(dim, linkAttrs, dstAttrs, srcAttrs)
	}

	metricKey := buildMetricKey(dims)

	c.seriesMu.Lock()
	series, ok := c.keyToMetric[metricKey]
	if !ok {
		series = &metricSeries{dimensions: dims}
		c.keyToMetric[metricKey] = series
	}
	series.reqTotal++
	if failed {
		series.reqFailedTotal++
	}

	serverDuration := float64(dstInfo.EndTime-dstInfo.StartTime) / float64(time.Millisecond)
	series.serverDurations = append(series.serverDurations, serverDuration)

	clientDuration := float64(srcInfo.EndTime-srcInfo.StartTime) / float64(time.Millisecond)
	series.clientDurations = append(series.clientDurations, clientDuration)

	c.seriesMu.Unlock()
}

func (c *serviceGraphConnector) buildMetrics() pmetric.Metrics {
	md := pmetric.NewMetrics()

	c.seriesMu.Lock()
	defer c.seriesMu.Unlock()

	if len(c.keyToMetric) == 0 {
		return md
	}

	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName(metadata.ScopeName)

	c.collectCountMetrics(sm)
	c.collectLatencyMetrics(sm)
	c.resetAccumulators()

	return md
}

func (c *serviceGraphConnector) collectCountMetrics(sm pmetric.ScopeMetrics) {
	totalMetric := sm.Metrics().AppendEmpty()
	totalMetric.SetName(metricReqTotal)
	totalMetric.SetDescription("Total number of link-derived service graph requests")
	totalSum := totalMetric.SetEmptySum()
	totalSum.SetIsMonotonic(true)
	totalSum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	failedMetric := sm.Metrics().AppendEmpty()
	failedMetric.SetName(metricReqFailedTotal)
	failedMetric.SetDescription("Total number of failed link-derived service graph requests")
	failedSum := failedMetric.SetEmptySum()
	failedSum.SetIsMonotonic(true)
	failedSum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	for _, series := range c.keyToMetric {
		dp := totalSum.DataPoints().AppendEmpty()
		dp.SetIntValue(series.reqTotal)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		setDimensionAttributes(dp.Attributes(), series.dimensions)

		if series.reqFailedTotal > 0 {
			fdp := failedSum.DataPoints().AppendEmpty()
			fdp.SetIntValue(series.reqFailedTotal)
			fdp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			setDimensionAttributes(fdp.Attributes(), series.dimensions)
		}
	}
}

func (c *serviceGraphConnector) collectLatencyMetrics(sm pmetric.ScopeMetrics) {
	serverHistMetric := sm.Metrics().AppendEmpty()
	serverHistMetric.SetName(metricReqServerHist)
	serverHistMetric.SetDescription("Server span duration for link-derived service graph edges")
	serverHistMetric.SetUnit("ms")
	serverHist := serverHistMetric.SetEmptyHistogram()
	serverHist.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	clientHistMetric := sm.Metrics().AppendEmpty()
	clientHistMetric.SetName(metricReqClientHist)
	clientHistMetric.SetDescription("Client span duration for link-derived service graph edges")
	clientHistMetric.SetUnit("ms")
	clientHist := clientHistMetric.SetEmptyHistogram()
	clientHist.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	bucketBounds := make([]float64, len(c.config.LatencyHistogramBuckets))
	for i, b := range c.config.LatencyHistogramBuckets {
		bucketBounds[i] = float64(b) / float64(time.Millisecond)
	}

	for _, series := range c.keyToMetric {
		if len(series.serverDurations) > 0 {
			dp := serverHist.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			setDimensionAttributes(dp.Attributes(), series.dimensions)
			buildHistogramDataPoint(dp, series.serverDurations, bucketBounds)
		}

		if len(series.clientDurations) > 0 {
			dp := clientHist.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			setDimensionAttributes(dp.Attributes(), series.dimensions)
			buildHistogramDataPoint(dp, series.clientDurations, bucketBounds)
		}
	}
}

func (c *serviceGraphConnector) resetAccumulators() {
	c.keyToMetric = make(map[string]*metricSeries)
}

func (c *serviceGraphConnector) flushMetrics(ctx context.Context) error {
	md := c.buildMetrics()
	if md.ResourceMetrics().Len() == 0 {
		return nil
	}
	return c.metricsConsumer.ConsumeMetrics(ctx, md)
}

func (c *serviceGraphConnector) metricFlushLoop() {
	defer c.doneWg.Done()
	interval := *c.config.MetricsFlushInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.flushMetrics(context.Background()); err != nil {
				c.logger.Error("failed to flush metrics", zap.Error(err))
			}
		case <-c.shutdownCh:
			return
		}
	}
}

func (c *serviceGraphConnector) storeExpirationLoop() {
	defer c.doneWg.Done()
	ticker := time.NewTicker(c.config.StoreExpirationLoop)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.spanIndex.Expire()
			c.pendingLinks.Expire()
		case <-c.shutdownCh:
			return
		}
	}
}

func (c *serviceGraphConnector) extractRelevantAttributes(span ptrace.Span) pcommon.Map {
	attrs := pcommon.NewMap()

	relevantKeys := map[string]struct{}{
		"messaging.system":           {},
		"messaging.destination.name": {},
		"db.system":                  {},
		"db.namespace":               {},
		"connection_type":            {},
		"link_type":                  {},
	}
	for _, dim := range c.config.Dimensions {
		relevantKeys[dim.SourceAttribute] = struct{}{}
	}

	span.Attributes().Range(func(k string, v pcommon.Value) bool {
		if _, ok := relevantKeys[k]; ok {
			v.CopyTo(attrs.PutEmpty(k))
		}
		return true
	})
	return attrs
}

func buildMetricKey(dims map[string]string) string {
	return fmt.Sprintf("%s→%s|%s|%s|%s",
		dims["client"],
		dims["server"],
		dims["connection_type"],
		dims["failed"],
		dims["edge_relation"],
	)
}

func setDimensionAttributes(dest pcommon.Map, dims map[string]string) {
	for k, v := range dims {
		dest.PutStr(k, v)
	}
}

func buildHistogramDataPoint(dp pmetric.HistogramDataPoint, values []float64, bucketBounds []float64) {
	dp.ExplicitBounds().FromRaw(bucketBounds)
	bucketCounts := make([]uint64, len(bucketBounds)+1)

	var sum float64
	for _, v := range values {
		sum += v
		placed := false
		for bi, bound := range bucketBounds {
			if v <= bound {
				bucketCounts[bi]++
				placed = true
				break
			}
		}
		if !placed {
			bucketCounts[len(bucketBounds)]++
		}
	}

	dp.BucketCounts().FromRaw(bucketCounts)
	dp.SetCount(uint64(len(values)))
	dp.SetSum(sum)
}

func copyMap(src pcommon.Map) pcommon.Map {
	dest := pcommon.NewMap()
	src.CopyTo(dest)
	return dest
}
