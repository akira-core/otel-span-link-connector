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
	dstAttrs := pcommon.NewMap()
	srcAttrs := pcommon.NewMap()

	result := resolveConnectionType(linkAttrs, dstAttrs, srcAttrs)
	assert.Equal(t, "kafka", result)
}

func TestResolveConnectionType_MessagingSystem(t *testing.T) {
	linkAttrs := pcommon.NewMap()
	dstAttrs := newMapWithAttrs("messaging.system", "nats")
	srcAttrs := pcommon.NewMap()

	result := resolveConnectionType(linkAttrs, dstAttrs, srcAttrs)
	assert.Equal(t, "nats", result)
}

func TestResolveConnectionType_DbSystem(t *testing.T) {
	linkAttrs := pcommon.NewMap()
	dstAttrs := pcommon.NewMap()
	srcAttrs := newMapWithAttrs("db.system", "mongodb")

	result := resolveConnectionType(linkAttrs, dstAttrs, srcAttrs)
	assert.Equal(t, "mongodb", result)
}

func TestResolveConnectionType_FallbackEmpty(t *testing.T) {
	result := resolveConnectionType(pcommon.NewMap(), pcommon.NewMap(), pcommon.NewMap())
	assert.Equal(t, "", result)
}

func TestResolveConnectionType_MessagingPrecedenceOverDb(t *testing.T) {
	linkAttrs := pcommon.NewMap()
	dstAttrs := newMapWithAttrs("messaging.system", "kafka", "db.system", "mongodb")
	srcAttrs := pcommon.NewMap()

	result := resolveConnectionType(linkAttrs, dstAttrs, srcAttrs)
	assert.Equal(t, "kafka", result)
}

func TestResolveConnectionType_LinkAttrsPrecedence(t *testing.T) {
	linkAttrs := newMapWithAttrs("messaging.system", "rabbitmq")
	dstAttrs := newMapWithAttrs("messaging.system", "kafka")
	srcAttrs := pcommon.NewMap()

	result := resolveConnectionType(linkAttrs, dstAttrs, srcAttrs)
	assert.Equal(t, "rabbitmq", result)
}

func TestResolveDimension(t *testing.T) {
	dim := Dimension{Name: "messaging_system", SourceAttribute: "messaging.system"}
	linkAttrs := pcommon.NewMap()
	dstAttrs := newMapWithAttrs("messaging.system", "kafka")
	srcAttrs := pcommon.NewMap()

	result := resolveDimension(dim, linkAttrs, dstAttrs, srcAttrs)
	assert.Equal(t, "kafka", result)
}

func TestResolveDimension_LinkAttrsPrecedence(t *testing.T) {
	dim := Dimension{Name: "link_type", SourceAttribute: "link_type"}
	linkAttrs := newMapWithAttrs("link_type", "queue_enq_deq")
	dstAttrs := newMapWithAttrs("link_type", "other")
	srcAttrs := pcommon.NewMap()

	result := resolveDimension(dim, linkAttrs, dstAttrs, srcAttrs)
	assert.Equal(t, "queue_enq_deq", result)
}

func TestResolveDimension_Missing(t *testing.T) {
	dim := Dimension{Name: "missing", SourceAttribute: "nonexistent"}
	result := resolveDimension(dim, pcommon.NewMap(), pcommon.NewMap(), pcommon.NewMap())
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
