package store

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func makeKey(traceIDSuffix, spanIDSuffix byte) SpanKey {
	var tid pcommon.TraceID
	tid[15] = traceIDSuffix
	var sid pcommon.SpanID
	sid[7] = spanIDSuffix
	return SpanKey{TraceID: tid, SpanID: sid}
}

func makeSpanInfo(svc string) SpanInfo {
	return SpanInfo{
		ServiceName: svc,
		StartTime:   pcommon.Timestamp(1000),
		EndTime:     pcommon.Timestamp(2000),
		StatusCode:  ptrace.StatusCodeOk,
		Attributes:  pcommon.NewMap(),
	}
}

func TestSpanIndexStore_PutAndGet(t *testing.T) {
	s := NewSpanIndexStore(5*time.Second, 100)

	key := makeKey(1, 1)
	info := makeSpanInfo("svc-a")
	s.Put(key, info)

	got, ok := s.Get(key)
	require.True(t, ok)
	assert.Equal(t, "svc-a", got.ServiceName)
	assert.Equal(t, 1, s.Len())
}

func TestSpanIndexStore_GetMiss(t *testing.T) {
	s := NewSpanIndexStore(5*time.Second, 100)

	_, ok := s.Get(makeKey(99, 99))
	assert.False(t, ok)
}

func TestSpanIndexStore_PutOverwrite(t *testing.T) {
	s := NewSpanIndexStore(5*time.Second, 100)

	key := makeKey(1, 1)
	s.Put(key, makeSpanInfo("svc-a"))
	s.Put(key, makeSpanInfo("svc-b"))

	got, ok := s.Get(key)
	require.True(t, ok)
	assert.Equal(t, "svc-b", got.ServiceName)
	assert.Equal(t, 1, s.Len())
}

func TestSpanIndexStore_Expire(t *testing.T) {
	s := NewSpanIndexStore(10*time.Millisecond, 100)

	var expiredCount int
	s.SetOnExpire(func(n int) { expiredCount += n })

	s.Put(makeKey(1, 1), makeSpanInfo("svc-a"))
	s.Put(makeKey(2, 2), makeSpanInfo("svc-b"))
	assert.Equal(t, 2, s.Len())

	time.Sleep(20 * time.Millisecond)
	n := s.Expire()
	assert.Equal(t, 2, n)
	assert.Equal(t, 0, s.Len())
	assert.Equal(t, 2, expiredCount)
}

func TestSpanIndexStore_MaxItems(t *testing.T) {
	s := NewSpanIndexStore(5*time.Second, 2)

	var droppedCount int
	s.SetOnDrop(func(n int) { droppedCount += n })

	s.Put(makeKey(1, 1), makeSpanInfo("svc-a"))
	s.Put(makeKey(2, 2), makeSpanInfo("svc-b"))
	s.Put(makeKey(3, 3), makeSpanInfo("svc-c"))

	assert.Equal(t, 2, s.Len())
	assert.Equal(t, 1, droppedCount)

	_, ok := s.Get(makeKey(1, 1))
	assert.False(t, ok, "oldest entry should be evicted")

	_, ok = s.Get(makeKey(3, 3))
	assert.True(t, ok)
}

func TestSpanIndexStore_Concurrent(t *testing.T) {
	s := NewSpanIndexStore(5*time.Second, 1000)
	var wg sync.WaitGroup

	for i := byte(0); i < 100; i++ {
		wg.Add(1)
		go func(i byte) {
			defer wg.Done()
			s.Put(makeKey(i, i), makeSpanInfo("svc"))
			s.Get(makeKey(i, i))
		}(i)
	}
	wg.Wait()
	assert.Equal(t, 100, s.Len())
}

func TestPendingLinksStore_AppendAndGetDelete(t *testing.T) {
	s := NewPendingLinksStore(5*time.Second, 100)

	key := makeKey(1, 1)
	edge1 := PendingEdge{DstService: "consumer-1", DstSpanInfo: makeSpanInfo("consumer-1"), LinkAttrs: pcommon.NewMap()}
	edge2 := PendingEdge{DstService: "consumer-2", DstSpanInfo: makeSpanInfo("consumer-2"), LinkAttrs: pcommon.NewMap()}

	s.Append(key, edge1)
	s.Append(key, edge2)
	assert.Equal(t, 1, s.Len())

	edges, ok := s.GetAndDelete(key)
	require.True(t, ok)
	assert.Len(t, edges, 2)
	assert.Equal(t, "consumer-1", edges[0].DstService)
	assert.Equal(t, "consumer-2", edges[1].DstService)
	assert.Equal(t, 0, s.Len())

	// Second call should return false
	_, ok = s.GetAndDelete(key)
	assert.False(t, ok)
}

func TestPendingLinksStore_Expire(t *testing.T) {
	s := NewPendingLinksStore(10*time.Millisecond, 100)

	var expiredEdges int
	s.SetOnExpire(func(n int) { expiredEdges += n })

	key1 := makeKey(1, 1)
	key2 := makeKey(2, 2)
	s.Append(key1, PendingEdge{DstService: "c1", DstSpanInfo: makeSpanInfo("c1"), LinkAttrs: pcommon.NewMap()})
	s.Append(key1, PendingEdge{DstService: "c2", DstSpanInfo: makeSpanInfo("c2"), LinkAttrs: pcommon.NewMap()})
	s.Append(key2, PendingEdge{DstService: "c3", DstSpanInfo: makeSpanInfo("c3"), LinkAttrs: pcommon.NewMap()})

	time.Sleep(20 * time.Millisecond)
	n := s.Expire()
	assert.Equal(t, 3, n)
	assert.Equal(t, 0, s.Len())
	assert.Equal(t, 3, expiredEdges)
}

func TestPendingLinksStore_MaxItems(t *testing.T) {
	s := NewPendingLinksStore(5*time.Second, 2)

	s.Append(makeKey(1, 1), PendingEdge{DstService: "c1", DstSpanInfo: makeSpanInfo("c1"), LinkAttrs: pcommon.NewMap()})
	s.Append(makeKey(2, 2), PendingEdge{DstService: "c2", DstSpanInfo: makeSpanInfo("c2"), LinkAttrs: pcommon.NewMap()})
	s.Append(makeKey(3, 3), PendingEdge{DstService: "c3", DstSpanInfo: makeSpanInfo("c3"), LinkAttrs: pcommon.NewMap()})

	assert.Equal(t, 2, s.Len())

	_, ok := s.GetAndDelete(makeKey(1, 1))
	assert.False(t, ok, "oldest entry should be evicted")
}

func TestPendingLinksStore_Concurrent(t *testing.T) {
	s := NewPendingLinksStore(5*time.Second, 1000)
	var wg sync.WaitGroup

	for i := byte(0); i < 100; i++ {
		wg.Add(1)
		go func(i byte) {
			defer wg.Done()
			s.Append(makeKey(i, i), PendingEdge{DstService: "svc", DstSpanInfo: makeSpanInfo("svc"), LinkAttrs: pcommon.NewMap()})
			s.GetAndDelete(makeKey(i, i))
		}(i)
	}
	wg.Wait()
}
