package spanlinkservicegraphconnector

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/qiumingzhi/otel-span-link-connector/connector/spanlinkservicegraphconnector/internal/metadata"
)

func newTestConnector(t *testing.T, cfg *Config) (*serviceGraphConnector, *consumertest.MetricsSink) {
	t.Helper()
	sink := &consumertest.MetricsSink{}
	zero := time.Duration(0)
	cfg.MetricsFlushInterval = &zero

	conn, err := createTracesToMetricsConnector(
		context.Background(),
		connectortest.NewNopSettings(metadata.Type),
		cfg,
		sink,
	)
	require.NoError(t, err)
	require.NoError(t, conn.Start(context.Background(), componenttest.NewNopHost()))
	return conn.(*serviceGraphConnector), sink
}

func defaultTestConfig() *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.Dimensions = []Dimension{
		{Name: "messaging_system", SourceAttribute: "messaging.system"},
		{Name: "link_type", SourceAttribute: "link_type"},
	}
	return cfg
}

func buildTrace(resources ...resourceDef) ptrace.Traces {
	td := ptrace.NewTraces()
	for _, r := range resources {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", r.serviceName)
		ss := rs.ScopeSpans().AppendEmpty()
		for _, s := range r.spans {
			span := ss.Spans().AppendEmpty()
			span.SetTraceID(s.traceID)
			span.SetSpanID(s.spanID)
			span.SetStartTimestamp(pcommon.Timestamp(s.startTime))
			span.SetEndTimestamp(pcommon.Timestamp(s.endTime))
			span.Status().SetCode(s.statusCode)
			span.SetKind(s.kind)
			for k, v := range s.attrs {
				span.Attributes().PutStr(k, v)
			}
			for _, l := range s.links {
				link := span.Links().AppendEmpty()
				link.SetTraceID(l.traceID)
				link.SetSpanID(l.spanID)
				for k, v := range l.attrs {
					link.Attributes().PutStr(k, v)
				}
			}
		}
	}
	return td
}

type resourceDef struct {
	serviceName string
	spans       []spanDef
}

type spanDef struct {
	traceID    pcommon.TraceID
	spanID     pcommon.SpanID
	startTime  uint64
	endTime    uint64
	statusCode ptrace.StatusCode
	kind       ptrace.SpanKind
	attrs      map[string]string
	links      []linkDef
}

type linkDef struct {
	traceID pcommon.TraceID
	spanID  pcommon.SpanID
	attrs   map[string]string
}

func tid(suffix byte) pcommon.TraceID {
	var id pcommon.TraceID
	id[15] = suffix
	return id
}

func sid(suffix byte) pcommon.SpanID {
	var id pcommon.SpanID
	id[7] = suffix
	return id
}

func assertMetricValue(t *testing.T, metrics []pmetric.Metrics, metricName, client, server string, expectedValue int64) {
	t.Helper()
	for _, md := range metrics {
		for i := 0; i < md.ResourceMetrics().Len(); i++ {
			for j := 0; j < md.ResourceMetrics().At(i).ScopeMetrics().Len(); j++ {
				sm := md.ResourceMetrics().At(i).ScopeMetrics().At(j)
				for k := 0; k < sm.Metrics().Len(); k++ {
					m := sm.Metrics().At(k)
					if m.Name() != metricName {
						continue
					}
					switch m.Type() {
					case pmetric.MetricTypeSum:
						for d := 0; d < m.Sum().DataPoints().Len(); d++ {
							dp := m.Sum().DataPoints().At(d)
							c, _ := dp.Attributes().Get("client")
							s, _ := dp.Attributes().Get("server")
							if c.Str() == client && s.Str() == server {
								assert.Equal(t, expectedValue, dp.IntValue())
								return
							}
						}
					}
				}
			}
		}
	}
	t.Errorf("metric %s{client=%s, server=%s} not found", metricName, client, server)
}

func assertHistogramExists(t *testing.T, metrics []pmetric.Metrics, metricName, client, server string, expectedCount uint64) {
	t.Helper()
	for _, md := range metrics {
		for i := 0; i < md.ResourceMetrics().Len(); i++ {
			for j := 0; j < md.ResourceMetrics().At(i).ScopeMetrics().Len(); j++ {
				sm := md.ResourceMetrics().At(i).ScopeMetrics().At(j)
				for k := 0; k < sm.Metrics().Len(); k++ {
					m := sm.Metrics().At(k)
					if m.Name() != metricName {
						continue
					}
					if m.Type() == pmetric.MetricTypeHistogram {
						for d := 0; d < m.Histogram().DataPoints().Len(); d++ {
							dp := m.Histogram().DataPoints().At(d)
							c, _ := dp.Attributes().Get("client")
							s, _ := dp.Attributes().Get("server")
							if c.Str() == client && s.Str() == server {
								assert.Equal(t, expectedCount, dp.Count())
								return
							}
						}
					}
				}
			}
		}
	}
	t.Errorf("histogram %s{client=%s, server=%s} not found", metricName, client, server)
}

func assertEdgeRelation(t *testing.T, metrics []pmetric.Metrics) {
	t.Helper()
	for _, md := range metrics {
		for i := 0; i < md.ResourceMetrics().Len(); i++ {
			for j := 0; j < md.ResourceMetrics().At(i).ScopeMetrics().Len(); j++ {
				sm := md.ResourceMetrics().At(i).ScopeMetrics().At(j)
				for k := 0; k < sm.Metrics().Len(); k++ {
					m := sm.Metrics().At(k)
					switch m.Type() {
					case pmetric.MetricTypeSum:
						for d := 0; d < m.Sum().DataPoints().Len(); d++ {
							v, ok := m.Sum().DataPoints().At(d).Attributes().Get("edge_relation")
							assert.True(t, ok, "edge_relation attribute missing")
							assert.Equal(t, "link", v.Str())
						}
					case pmetric.MetricTypeHistogram:
						for d := 0; d < m.Histogram().DataPoints().Len(); d++ {
							v, ok := m.Histogram().DataPoints().At(d).Attributes().Get("edge_relation")
							assert.True(t, ok, "edge_relation attribute missing")
							assert.Equal(t, "link", v.Str())
						}
					}
				}
			}
		}
	}
}

// Scenario A: Kafka message queue link
func TestConsumeTraces_KafkaLink(t *testing.T) {
	cfg := defaultTestConfig()
	conn, sink := newTestConnector(t, cfg)
	defer func() { require.NoError(t, conn.Shutdown(context.Background())) }()

	td := buildTrace(
		resourceDef{serviceName: "order-service", spans: []spanDef{{
			traceID: tid(1), spanID: sid(1),
			startTime: 1000, endTime: 2000,
			kind: ptrace.SpanKindProducer,
			attrs: map[string]string{
				"messaging.system":           "kafka",
				"messaging.destination.name": "orders-topic",
			},
		}}},
		resourceDef{serviceName: "payment-service", spans: []spanDef{{
			traceID: tid(2), spanID: sid(2),
			startTime: 3000, endTime: 5000,
			kind: ptrace.SpanKindConsumer,
			attrs: map[string]string{
				"messaging.system":           "kafka",
				"messaging.destination.name": "orders-topic",
			},
			links: []linkDef{{
				traceID: tid(1), spanID: sid(1),
				attrs: map[string]string{"link_type": "queue_enq_deq"},
			}},
		}}},
	)

	require.NoError(t, conn.ConsumeTraces(context.Background(), td))
	metrics := sink.AllMetrics()
	require.Len(t, metrics, 1)

	assertMetricValue(t, metrics, metricReqTotal, "order-service", "payment-service", 1)
	assertHistogramExists(t, metrics, metricReqServerHist, "order-service", "payment-service", 1)
	assertHistogramExists(t, metrics, metricReqClientHist, "order-service", "payment-service", 1)
	assertEdgeRelation(t, metrics)
}

// Scenario B: MongoDB change stream
func TestConsumeTraces_MongoDBChangeStream(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.Dimensions = append(cfg.Dimensions, Dimension{Name: "db_system", SourceAttribute: "db.system"})
	conn, sink := newTestConnector(t, cfg)
	defer func() { require.NoError(t, conn.Shutdown(context.Background())) }()

	td := buildTrace(
		resourceDef{serviceName: "order-service", spans: []spanDef{{
			traceID: tid(1), spanID: sid(1),
			startTime: 1000, endTime: 1500,
			kind: ptrace.SpanKindClient,
			attrs: map[string]string{
				"db.system":    "mongodb",
				"db.namespace": "ecommerce.orders",
			},
		}}},
		resourceDef{serviceName: "sync-service", spans: []spanDef{{
			traceID: tid(2), spanID: sid(2),
			startTime: 2000, endTime: 3000,
			kind: ptrace.SpanKindClient,
			attrs: map[string]string{
				"db.system":    "mongodb",
				"db.namespace": "ecommerce.orders",
			},
			links: []linkDef{{
				traceID: tid(1), spanID: sid(1),
				attrs: map[string]string{"link_type": "change_stream"},
			}},
		}}},
	)

	require.NoError(t, conn.ConsumeTraces(context.Background(), td))
	metrics := sink.AllMetrics()
	require.Len(t, metrics, 1)

	assertMetricValue(t, metrics, metricReqTotal, "order-service", "sync-service", 1)
	assertEdgeRelation(t, metrics)
}

// Scenario C: Batch processing (N links → N edges)
func TestConsumeTraces_BatchProcessing(t *testing.T) {
	cfg := defaultTestConfig()
	conn, sink := newTestConnector(t, cfg)
	defer func() { require.NoError(t, conn.Shutdown(context.Background())) }()

	td := buildTrace(
		resourceDef{serviceName: "api-gateway", spans: []spanDef{
			{traceID: tid(1), spanID: sid(1), startTime: 1000, endTime: 1100, kind: ptrace.SpanKindServer},
			{traceID: tid(2), spanID: sid(2), startTime: 1200, endTime: 1300, kind: ptrace.SpanKindServer},
			{traceID: tid(3), spanID: sid(3), startTime: 1400, endTime: 1500, kind: ptrace.SpanKindServer},
		}},
		resourceDef{serviceName: "batch-processor", spans: []spanDef{{
			traceID: tid(10), spanID: sid(10),
			startTime: 2000, endTime: 5000,
			kind: ptrace.SpanKindInternal,
			links: []linkDef{
				{traceID: tid(1), spanID: sid(1), attrs: map[string]string{"link_type": "batch_input"}},
				{traceID: tid(2), spanID: sid(2), attrs: map[string]string{"link_type": "batch_input"}},
				{traceID: tid(3), spanID: sid(3), attrs: map[string]string{"link_type": "batch_input"}},
			},
		}}},
	)

	require.NoError(t, conn.ConsumeTraces(context.Background(), td))
	metrics := sink.AllMetrics()
	require.Len(t, metrics, 1)

	assertMetricValue(t, metrics, metricReqTotal, "api-gateway", "batch-processor", 3)
	assertHistogramExists(t, metrics, metricReqServerHist, "api-gateway", "batch-processor", 3)
}

// Scenario D: Trust boundary
func TestConsumeTraces_TrustBoundary(t *testing.T) {
	cfg := defaultTestConfig()
	conn, sink := newTestConnector(t, cfg)
	defer func() { require.NoError(t, conn.Shutdown(context.Background())) }()

	td := buildTrace(
		resourceDef{serviceName: "external-partner", spans: []spanDef{{
			traceID: tid(1), spanID: sid(1),
			startTime: 1000, endTime: 1500,
			kind: ptrace.SpanKindClient,
		}}},
		resourceDef{serviceName: "payment-gateway", spans: []spanDef{{
			traceID: tid(2), spanID: sid(2),
			startTime: 2000, endTime: 3000,
			kind: ptrace.SpanKindServer,
			links: []linkDef{{
				traceID: tid(1), spanID: sid(1),
				attrs: map[string]string{"link_type": "trust_boundary"},
			}},
		}}},
	)

	require.NoError(t, conn.ConsumeTraces(context.Background(), td))
	metrics := sink.AllMetrics()
	require.Len(t, metrics, 1)

	assertMetricValue(t, metrics, metricReqTotal, "external-partner", "payment-gateway", 1)
	assertEdgeRelation(t, metrics)
}

// Scenario E: Long-running async (fan-in with many links)
func TestConsumeTraces_LongRunningAsync(t *testing.T) {
	cfg := defaultTestConfig()
	conn, sink := newTestConnector(t, cfg)
	defer func() { require.NoError(t, conn.Shutdown(context.Background())) }()

	var triggerSpans []spanDef
	var links []linkDef
	for i := byte(1); i <= 5; i++ {
		triggerSpans = append(triggerSpans, spanDef{
			traceID: tid(i), spanID: sid(i),
			startTime: uint64(1000 + int(i)*100), endTime: uint64(1050 + int(i)*100),
			kind: ptrace.SpanKindServer,
		})
		links = append(links, linkDef{
			traceID: tid(i), spanID: sid(i),
			attrs: map[string]string{"link_type": "async_trigger"},
		})
	}

	td := buildTrace(
		resourceDef{serviceName: "event-ingestion", spans: triggerSpans},
		resourceDef{serviceName: "etl-service", spans: []spanDef{{
			traceID: tid(20), spanID: sid(20),
			startTime: 5000, endTime: 50000,
			kind: ptrace.SpanKindInternal,
			links: links,
		}}},
	)

	require.NoError(t, conn.ConsumeTraces(context.Background(), td))
	metrics := sink.AllMetrics()
	require.Len(t, metrics, 1)

	assertMetricValue(t, metrics, metricReqTotal, "event-ingestion", "etl-service", 5)
}

// Scenario F: Retry (self-referencing edge)
func TestConsumeTraces_Retry(t *testing.T) {
	cfg := defaultTestConfig()
	conn, sink := newTestConnector(t, cfg)
	defer func() { require.NoError(t, conn.Shutdown(context.Background())) }()

	td := buildTrace(
		resourceDef{serviceName: "order-service", spans: []spanDef{
			{
				traceID: tid(1), spanID: sid(1),
				startTime: 1000, endTime: 2000,
				statusCode: ptrace.StatusCodeError,
				kind:       ptrace.SpanKindServer,
			},
			{
				traceID: tid(2), spanID: sid(2),
				startTime: 3000, endTime: 4000,
				kind: ptrace.SpanKindServer,
				links: []linkDef{{
					traceID: tid(1), spanID: sid(1),
					attrs: map[string]string{"link_type": "retry"},
				}},
			},
		}},
	)

	require.NoError(t, conn.ConsumeTraces(context.Background(), td))
	metrics := sink.AllMetrics()
	require.Len(t, metrics, 1)

	assertMetricValue(t, metrics, metricReqTotal, "order-service", "order-service", 1)
}

// Scenario G: Scatter/Gather (fan-out)
func TestConsumeTraces_ScatterGather(t *testing.T) {
	cfg := defaultTestConfig()
	conn, sink := newTestConnector(t, cfg)
	defer func() { require.NoError(t, conn.Shutdown(context.Background())) }()

	td := buildTrace(
		resourceDef{serviceName: "order-svc", spans: []spanDef{{
			traceID: tid(1), spanID: sid(1),
			startTime: 1000, endTime: 2000,
			kind: ptrace.SpanKindProducer,
		}}},
		resourceDef{serviceName: "payment-svc", spans: []spanDef{{
			traceID: tid(2), spanID: sid(2),
			startTime: 2000, endTime: 3000,
			kind: ptrace.SpanKindConsumer,
			links: []linkDef{{traceID: tid(1), spanID: sid(1)}},
		}}},
		resourceDef{serviceName: "inventory-svc", spans: []spanDef{{
			traceID: tid(3), spanID: sid(3),
			startTime: 2000, endTime: 2500,
			kind: ptrace.SpanKindConsumer,
			links: []linkDef{{traceID: tid(1), spanID: sid(1)}},
		}}},
		resourceDef{serviceName: "notification-svc", spans: []spanDef{{
			traceID: tid(4), spanID: sid(4),
			startTime: 2000, endTime: 2800,
			kind: ptrace.SpanKindConsumer,
			links: []linkDef{{traceID: tid(1), spanID: sid(1)}},
		}}},
	)

	require.NoError(t, conn.ConsumeTraces(context.Background(), td))
	metrics := sink.AllMetrics()
	require.Len(t, metrics, 1)

	assertMetricValue(t, metrics, metricReqTotal, "order-svc", "payment-svc", 1)
	assertMetricValue(t, metrics, metricReqTotal, "order-svc", "inventory-svc", 1)
	assertMetricValue(t, metrics, metricReqTotal, "order-svc", "notification-svc", 1)
}

// Cross-batch: source span arrives in batch 2 (pending link scenario)
func TestConsumeTraces_CrossBatch(t *testing.T) {
	cfg := defaultTestConfig()
	conn, sink := newTestConnector(t, cfg)
	defer func() { require.NoError(t, conn.Shutdown(context.Background())) }()

	// Batch 1: consumer arrives first, source not yet indexed
	batch1 := buildTrace(
		resourceDef{serviceName: "payment-service", spans: []spanDef{{
			traceID: tid(2), spanID: sid(2),
			startTime: 3000, endTime: 5000,
			kind: ptrace.SpanKindConsumer,
			attrs: map[string]string{"messaging.system": "kafka"},
			links: []linkDef{{
				traceID: tid(1), spanID: sid(1),
				attrs: map[string]string{"link_type": "queue_enq_deq"},
			}},
		}}},
	)

	// Should produce no metrics yet (link pending)
	require.NoError(t, conn.ConsumeTraces(context.Background(), batch1))
	assert.Empty(t, sink.AllMetrics())

	// Batch 2: source span arrives
	batch2 := buildTrace(
		resourceDef{serviceName: "order-service", spans: []spanDef{{
			traceID: tid(1), spanID: sid(1),
			startTime: 1000, endTime: 2000,
			kind: ptrace.SpanKindProducer,
			attrs: map[string]string{
				"messaging.system":           "kafka",
				"messaging.destination.name": "orders-topic",
			},
		}}},
	)

	require.NoError(t, conn.ConsumeTraces(context.Background(), batch2))
	metrics := sink.AllMetrics()
	require.Len(t, metrics, 1)

	assertMetricValue(t, metrics, metricReqTotal, "order-service", "payment-service", 1)
	assertEdgeRelation(t, metrics)
}

// Test failed span produces failed_total metric
func TestConsumeTraces_FailedSpan(t *testing.T) {
	cfg := defaultTestConfig()
	conn, sink := newTestConnector(t, cfg)
	defer func() { require.NoError(t, conn.Shutdown(context.Background())) }()

	td := buildTrace(
		resourceDef{serviceName: "producer-svc", spans: []spanDef{{
			traceID: tid(1), spanID: sid(1),
			startTime: 1000, endTime: 2000,
			kind: ptrace.SpanKindProducer,
		}}},
		resourceDef{serviceName: "consumer-svc", spans: []spanDef{{
			traceID: tid(2), spanID: sid(2),
			startTime: 3000, endTime: 5000,
			statusCode: ptrace.StatusCodeError,
			kind:       ptrace.SpanKindConsumer,
			links:      []linkDef{{traceID: tid(1), spanID: sid(1)}},
		}}},
	)

	require.NoError(t, conn.ConsumeTraces(context.Background(), td))
	metrics := sink.AllMetrics()
	require.Len(t, metrics, 1)

	assertMetricValue(t, metrics, metricReqTotal, "producer-svc", "consumer-svc", 1)
	assertMetricValue(t, metrics, metricReqFailedTotal, "producer-svc", "consumer-svc", 1)
}

// Test no links produces no metrics
func TestConsumeTraces_NoLinks(t *testing.T) {
	cfg := defaultTestConfig()
	conn, sink := newTestConnector(t, cfg)
	defer func() { require.NoError(t, conn.Shutdown(context.Background())) }()

	td := buildTrace(
		resourceDef{serviceName: "svc-a", spans: []spanDef{{
			traceID: tid(1), spanID: sid(1),
			startTime: 1000, endTime: 2000,
			kind: ptrace.SpanKindServer,
		}}},
	)

	require.NoError(t, conn.ConsumeTraces(context.Background(), td))
	assert.Empty(t, sink.AllMetrics())
}

// Test NATS queue scenario
func TestConsumeTraces_NATSLink(t *testing.T) {
	cfg := defaultTestConfig()
	conn, sink := newTestConnector(t, cfg)
	defer func() { require.NoError(t, conn.Shutdown(context.Background())) }()

	td := buildTrace(
		resourceDef{serviceName: "publisher-svc", spans: []spanDef{{
			traceID: tid(1), spanID: sid(1),
			startTime: 1000, endTime: 1500,
			kind:  ptrace.SpanKindProducer,
			attrs: map[string]string{"messaging.system": "nats"},
		}}},
		resourceDef{serviceName: "subscriber-svc", spans: []spanDef{{
			traceID: tid(2), spanID: sid(2),
			startTime: 2000, endTime: 3000,
			kind:  ptrace.SpanKindConsumer,
			attrs: map[string]string{"messaging.system": "nats"},
			links: []linkDef{{traceID: tid(1), spanID: sid(1)}},
		}}},
	)

	require.NoError(t, conn.ConsumeTraces(context.Background(), td))
	metrics := sink.AllMetrics()
	require.Len(t, metrics, 1)

	assertMetricValue(t, metrics, metricReqTotal, "publisher-svc", "subscriber-svc", 1)
}

func TestConnector_StartStop(t *testing.T) {
	cfg := defaultTestConfig()
	flushInterval := 100 * time.Millisecond
	cfg.MetricsFlushInterval = &flushInterval

	sink := &consumertest.MetricsSink{}
	conn, err := createTracesToMetricsConnector(
		context.Background(),
		connectortest.NewNopSettings(metadata.Type),
		cfg,
		sink,
	)
	require.NoError(t, err)

	err = conn.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	err = conn.Shutdown(context.Background())
	require.NoError(t, err)
}
