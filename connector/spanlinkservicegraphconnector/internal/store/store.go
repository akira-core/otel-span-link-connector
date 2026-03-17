package store

import (
	"container/list"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// SpanKey uniquely identifies a span by its trace ID and span ID.
type SpanKey struct {
	TraceID pcommon.TraceID
	SpanID  pcommon.SpanID
}

// SpanInfo holds the essential information about an indexed span.
type SpanInfo struct {
	ServiceName        string
	StartTime          pcommon.Timestamp
	EndTime            pcommon.Timestamp
	StatusCode         ptrace.StatusCode
	Attributes         pcommon.Map
	ResourceAttributes pcommon.Map
}

// PendingEdge represents an unresolved link waiting for its source span.
type PendingEdge struct {
	DstService  string
	DstSpanInfo SpanInfo
	LinkAttrs   pcommon.Map
}

type spanEntry struct {
	key       SpanKey
	info      SpanInfo
	expireAt  time.Time
}

type pendingEntry struct {
	key       SpanKey
	edges     []PendingEdge
	expireAt  time.Time
}

// SpanIndexStore is a TTL-based cache that indexes spans by (trace_id, span_id)
// for quick lookup when resolving span links.
type SpanIndexStore struct {
	mu       sync.RWMutex
	ttl      time.Duration
	maxItems int
	items    map[SpanKey]*list.Element
	ll       *list.List // ordered by insertion time for TTL eviction

	onExpire func(int) // callback when entries expire, receives count
	onDrop   func(int) // callback when entries are dropped due to capacity
}

func NewSpanIndexStore(ttl time.Duration, maxItems int) *SpanIndexStore {
	return &SpanIndexStore{
		ttl:      ttl,
		maxItems: maxItems,
		items:    make(map[SpanKey]*list.Element),
		ll:       list.New(),
	}
}

func (s *SpanIndexStore) SetOnExpire(fn func(int)) { s.onExpire = fn }
func (s *SpanIndexStore) SetOnDrop(fn func(int))   { s.onDrop = fn }

func (s *SpanIndexStore) Put(key SpanKey, info SpanInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if el, ok := s.items[key]; ok {
		entry := el.Value.(*spanEntry)
		entry.info = info
		entry.expireAt = time.Now().Add(s.ttl)
		s.ll.MoveToBack(el)
		return
	}

	if s.ll.Len() >= s.maxItems {
		s.evictOldest(1)
	}

	entry := &spanEntry{
		key:      key,
		info:     info,
		expireAt: time.Now().Add(s.ttl),
	}
	el := s.ll.PushBack(entry)
	s.items[key] = el
}

func (s *SpanIndexStore) Get(key SpanKey) (SpanInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	el, ok := s.items[key]
	if !ok {
		return SpanInfo{}, false
	}
	return el.Value.(*spanEntry).info, true
}

// Expire removes all entries that have exceeded their TTL. Returns the count of expired entries.
func (s *SpanIndexStore) Expire() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	count := 0
	for {
		front := s.ll.Front()
		if front == nil {
			break
		}
		entry := front.Value.(*spanEntry)
		if entry.expireAt.After(now) {
			break
		}
		s.ll.Remove(front)
		delete(s.items, entry.key)
		count++
	}
	if count > 0 && s.onExpire != nil {
		s.onExpire(count)
	}
	return count
}

func (s *SpanIndexStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ll.Len()
}

func (s *SpanIndexStore) evictOldest(n int) {
	dropped := 0
	for i := 0; i < n; i++ {
		front := s.ll.Front()
		if front == nil {
			break
		}
		entry := front.Value.(*spanEntry)
		s.ll.Remove(front)
		delete(s.items, entry.key)
		dropped++
	}
	if dropped > 0 && s.onDrop != nil {
		s.onDrop(dropped)
	}
}

// PendingLinksStore is a TTL-based cache that stores unresolved span links
// waiting for their target spans to arrive.
type PendingLinksStore struct {
	mu       sync.Mutex
	ttl      time.Duration
	maxItems int
	items    map[SpanKey]*list.Element
	ll       *list.List

	onExpire func(int)
}

func NewPendingLinksStore(ttl time.Duration, maxItems int) *PendingLinksStore {
	return &PendingLinksStore{
		ttl:      ttl,
		maxItems: maxItems,
		items:    make(map[SpanKey]*list.Element),
		ll:       list.New(),
	}
}

func (s *PendingLinksStore) SetOnExpire(fn func(int)) { s.onExpire = fn }

// Append adds a pending edge for the given span key. Multiple edges can
// be pending for the same key (e.g. multiple consumers linking to the same producer).
func (s *PendingLinksStore) Append(key SpanKey, edge PendingEdge) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if el, ok := s.items[key]; ok {
		entry := el.Value.(*pendingEntry)
		entry.edges = append(entry.edges, edge)
		return
	}

	if s.ll.Len() >= s.maxItems {
		s.evictOldest(1)
	}

	entry := &pendingEntry{
		key:      key,
		edges:    []PendingEdge{edge},
		expireAt: time.Now().Add(s.ttl),
	}
	el := s.ll.PushBack(entry)
	s.items[key] = el
}

// GetAndDelete retrieves and removes all pending edges for the given key.
func (s *PendingLinksStore) GetAndDelete(key SpanKey) ([]PendingEdge, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	el, ok := s.items[key]
	if !ok {
		return nil, false
	}
	entry := el.Value.(*pendingEntry)
	edges := entry.edges
	s.ll.Remove(el)
	delete(s.items, key)
	return edges, true
}

// Expire removes all entries that have exceeded their TTL. Returns the total
// number of pending edges expired.
func (s *PendingLinksStore) Expire() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	totalEdges := 0
	for {
		front := s.ll.Front()
		if front == nil {
			break
		}
		entry := front.Value.(*pendingEntry)
		if entry.expireAt.After(now) {
			break
		}
		totalEdges += len(entry.edges)
		s.ll.Remove(front)
		delete(s.items, entry.key)
	}
	if totalEdges > 0 && s.onExpire != nil {
		s.onExpire(totalEdges)
	}
	return totalEdges
}

func (s *PendingLinksStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ll.Len()
}

func (s *PendingLinksStore) evictOldest(n int) {
	for i := 0; i < n; i++ {
		front := s.ll.Front()
		if front == nil {
			break
		}
		entry := front.Value.(*pendingEntry)
		s.ll.Remove(front)
		delete(s.items, entry.key)
	}
}
