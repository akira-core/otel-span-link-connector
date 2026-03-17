package spanlinkservicegraphconnector

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func newMapWithAttrs(kvs ...string) pcommon.Map {
	m := pcommon.NewMap()
	for i := 0; i < len(kvs)-1; i += 2 {
		m.PutStr(kvs[i], kvs[i+1])
	}
	return m
}

func TestResolveConnectionType_ExplicitConnectionType(t *testing.T) {
	linkAttrs := newMapWithAttrs("connection_type", "kafka")
	empty := pcommon.NewMap()

	result := resolveConnectionType(linkAttrs, empty, empty, empty, empty)
	assert.Equal(t, "kafka", result)
}

func TestResolveConnectionType_MessagingSystem(t *testing.T) {
	dstAttrs := newMapWithAttrs("messaging.system", "nats")
	empty := pcommon.NewMap()

	result := resolveConnectionType(empty, dstAttrs, empty, empty, empty)
	assert.Equal(t, "nats", result)
}

func TestResolveConnectionType_DbSystem(t *testing.T) {
	srcAttrs := newMapWithAttrs("db.system", "mongodb")
	empty := pcommon.NewMap()

	result := resolveConnectionType(empty, empty, srcAttrs, empty, empty)
	assert.Equal(t, "mongodb", result)
}

func TestResolveConnectionType_FallbackEmpty(t *testing.T) {
	empty := pcommon.NewMap()
	result := resolveConnectionType(empty, empty, empty, empty, empty)
	assert.Equal(t, "", result)
}

func TestResolveConnectionType_MessagingPrecedenceOverDb(t *testing.T) {
	dstAttrs := newMapWithAttrs("messaging.system", "kafka", "db.system", "mongodb")
	empty := pcommon.NewMap()

	result := resolveConnectionType(empty, dstAttrs, empty, empty, empty)
	assert.Equal(t, "kafka", result)
}

func TestResolveConnectionType_LinkAttrsPrecedence(t *testing.T) {
	linkAttrs := newMapWithAttrs("messaging.system", "rabbitmq")
	dstAttrs := newMapWithAttrs("messaging.system", "kafka")
	empty := pcommon.NewMap()

	result := resolveConnectionType(linkAttrs, dstAttrs, empty, empty, empty)
	assert.Equal(t, "rabbitmq", result)
}

func TestResolveConnectionType_ResourceAttrsFallback(t *testing.T) {
	dstResourceAttrs := newMapWithAttrs("messaging.system", "kafka")
	empty := pcommon.NewMap()

	result := resolveConnectionType(empty, empty, empty, dstResourceAttrs, empty)
	assert.Equal(t, "kafka", result)
}

func TestResolveConnectionType_SpanAttrsPrecedenceOverResource(t *testing.T) {
	dstAttrs := newMapWithAttrs("messaging.system", "nats")
	dstResourceAttrs := newMapWithAttrs("messaging.system", "kafka")
	empty := pcommon.NewMap()

	result := resolveConnectionType(empty, dstAttrs, empty, dstResourceAttrs, empty)
	assert.Equal(t, "nats", result)
}

func TestResolveDimensionForSide_FromResourceAttrs(t *testing.T) {
	dim := Dimension{Name: "messaging_system", SourceAttribute: "messaging.system"}
	resourceAttrs := newMapWithAttrs("messaging.system", "kafka")
	empty := pcommon.NewMap()

	result := resolveDimensionForSide(dim, resourceAttrs, empty, empty)
	assert.Equal(t, "kafka", result)
}

func TestResolveDimensionForSide_FromSpanAttrs(t *testing.T) {
	dim := Dimension{Name: "messaging_system", SourceAttribute: "messaging.system"}
	spanAttrs := newMapWithAttrs("messaging.system", "nats")
	empty := pcommon.NewMap()

	result := resolveDimensionForSide(dim, empty, spanAttrs, empty)
	assert.Equal(t, "nats", result)
}

func TestResolveDimensionForSide_FromLinkAttrs(t *testing.T) {
	dim := Dimension{Name: "link_type", SourceAttribute: "link_type"}
	linkAttrs := newMapWithAttrs("link_type", "queue_enq_deq")
	empty := pcommon.NewMap()

	result := resolveDimensionForSide(dim, empty, empty, linkAttrs)
	assert.Equal(t, "queue_enq_deq", result)
}

func TestResolveDimensionForSide_ResourcePrecedenceOverSpan(t *testing.T) {
	dim := Dimension{Name: "messaging_system", SourceAttribute: "messaging.system"}
	resourceAttrs := newMapWithAttrs("messaging.system", "kafka")
	spanAttrs := newMapWithAttrs("messaging.system", "nats")

	result := resolveDimensionForSide(dim, resourceAttrs, spanAttrs, pcommon.NewMap())
	assert.Equal(t, "kafka", result)
}

func TestResolveDimensionForSide_SpanPrecedenceOverLink(t *testing.T) {
	dim := Dimension{Name: "link_type", SourceAttribute: "link_type"}
	spanAttrs := newMapWithAttrs("link_type", "from_span")
	linkAttrs := newMapWithAttrs("link_type", "from_link")

	result := resolveDimensionForSide(dim, pcommon.NewMap(), spanAttrs, linkAttrs)
	assert.Equal(t, "from_span", result)
}

func TestResolveDimensionForSide_Missing(t *testing.T) {
	dim := Dimension{Name: "missing", SourceAttribute: "nonexistent"}
	empty := pcommon.NewMap()
	result := resolveDimensionForSide(dim, empty, empty, empty)
	assert.Equal(t, "", result)
}

func TestFindServiceName(t *testing.T) {
	res := pcommon.NewResource()
	res.Attributes().PutStr("service.name", "my-service")
	assert.Equal(t, "my-service", findServiceName(res))
}

func TestFindServiceName_Missing(t *testing.T) {
	res := pcommon.NewResource()
	assert.Equal(t, "", findServiceName(res))
}
