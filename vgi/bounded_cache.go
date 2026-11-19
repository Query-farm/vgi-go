// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"container/list"
	"sync"
	"time"
)

// boundedCache is a small LRU with a per-entry TTL and an optional cost budget.
// The framework's per-execution caches (buffering init, aggregate constant
// arguments, decoded window partitions) use it so an execution whose destructor
// never arrives cannot pin memory -- or resolved secrets -- forever.
type boundedCache[K comparable, V any] struct {
	mu       sync.Mutex
	ll       *list.List
	items    map[K]*list.Element
	maxLen   int
	maxCost  int64 // 0: no cost budget
	cost     int64
	ttl      time.Duration
	now      func() time.Time
	costOf   func(V) int64
	onRemove func(V)
}

type boundedCacheEntry[K comparable, V any] struct {
	key     K
	val     V
	cost    int64
	expires time.Time
}

func newBoundedCache[K comparable, V any](maxLen int, ttl time.Duration) *boundedCache[K, V] {
	return &boundedCache[K, V]{
		ll:     list.New(),
		items:  make(map[K]*list.Element),
		maxLen: maxLen,
		ttl:    ttl,
		now:    time.Now,
	}
}

// Get returns the live value for k and marks it recently used.
func (c *boundedCache[K, V]) Get(k K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var zero V
	el, ok := c.items[k]
	if !ok {
		return zero, false
	}
	e := el.Value.(*boundedCacheEntry[K, V])
	if c.ttl > 0 && !c.now().Before(e.expires) {
		c.removeLocked(el)
		return zero, false
	}
	c.ll.MoveToFront(el)
	return e.val, true
}

// Put stores v under k, evicting least-recently-used entries past the bounds.
func (c *boundedCache[K, V]) Put(k K, v V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var cost int64
	if c.costOf != nil {
		cost = c.costOf(v)
	}
	if el, ok := c.items[k]; ok {
		c.removeLocked(el)
	}
	if c.maxCost > 0 && cost > c.maxCost {
		return // never cacheable; the caller still has its value
	}
	e := &boundedCacheEntry[K, V]{key: k, val: v, cost: cost, expires: c.now().Add(c.ttl)}
	c.items[k] = c.ll.PushFront(e)
	c.cost += cost
	for c.ll.Len() > 0 && (c.ll.Len() > c.maxLen || (c.maxCost > 0 && c.cost > c.maxCost)) {
		c.removeLocked(c.ll.Back())
	}
}

// Delete drops k.
func (c *boundedCache[K, V]) Delete(k K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[k]; ok {
		c.removeLocked(el)
	}
}

// DeleteFunc drops every entry whose key matches.
func (c *boundedCache[K, V]) DeleteFunc(match func(K) bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, el := range c.items {
		if match(k) {
			c.removeLocked(el)
		}
	}
}

// Len is the number of entries held (expired ones included until touched).
func (c *boundedCache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

func (c *boundedCache[K, V]) removeLocked(el *list.Element) {
	e := el.Value.(*boundedCacheEntry[K, V])
	c.ll.Remove(el)
	delete(c.items, e.key)
	c.cost -= e.cost
	if c.onRemove != nil {
		c.onRemove(e.val)
	}
}
