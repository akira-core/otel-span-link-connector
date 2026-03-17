package spanlinkservicegraphconnector

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func findServiceName(resource pcommon.Resource) string {
	v, ok := resource.Attributes().Get("service.name")
	if !ok {
		return ""
	}
	return v.Str()
}

func resolveConnectionType(linkAttrs, dstAttrs, srcAttrs, dstResourceAttrs, srcResourceAttrs pcommon.Map) string {
	if v, ok := linkAttrs.Get("connection_type"); ok && v.Str() != "" {
		return v.Str()
	}
	if v := firstNonEmptyAttr("messaging.system", linkAttrs, dstAttrs, srcAttrs, dstResourceAttrs, srcResourceAttrs); v != "" {
		return v
	}
	if v := firstNonEmptyAttr("db.system", linkAttrs, dstAttrs, srcAttrs, dstResourceAttrs, srcResourceAttrs); v != "" {
		return v
	}
	return ""
}

// resolveDimensionForSide resolves a dimension value for one side of an edge
// (client or server). Priority: resource attrs > span attrs > link attrs.
func resolveDimensionForSide(dim Dimension, resourceAttrs, spanAttrs, linkAttrs pcommon.Map) string {
	return firstNonEmptyAttr(dim.SourceAttribute, resourceAttrs, spanAttrs, linkAttrs)
}

func firstNonEmptyAttr(key string, maps ...pcommon.Map) string {
	for _, m := range maps {
		if v, ok := m.Get(key); ok && v.Str() != "" {
			return v.Str()
		}
	}
	return ""
}
