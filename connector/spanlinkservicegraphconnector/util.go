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

func resolveConnectionType(linkAttrs, dstAttrs, srcAttrs pcommon.Map) string {
	if v, ok := linkAttrs.Get("connection_type"); ok && v.Str() != "" {
		return v.Str()
	}
	if v := firstNonEmptyAttr("messaging.system", linkAttrs, dstAttrs, srcAttrs); v != "" {
		return v
	}
	if v := firstNonEmptyAttr("db.system", linkAttrs, dstAttrs, srcAttrs); v != "" {
		return v
	}
	return ""
}

func resolveDimension(dim Dimension, linkAttrs, dstAttrs, srcAttrs pcommon.Map) string {
	return firstNonEmptyAttr(dim.SourceAttribute, linkAttrs, dstAttrs, srcAttrs)
}

func firstNonEmptyAttr(key string, maps ...pcommon.Map) string {
	for _, m := range maps {
		if v, ok := m.Get(key); ok && v.Str() != "" {
			return v.Str()
		}
	}
	return ""
}
