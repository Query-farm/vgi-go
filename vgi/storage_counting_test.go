// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// countingStorage wraps a FunctionStorage (and its state log) and counts the
// calls the framework makes -- each one a network round trip on a remote
// backend such as cloudflare-do.
type countingStorage struct {
	FunctionStorage
	mu    sync.Mutex
	calls map[string]int
}

func (c *countingStorage) hit(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.calls == nil {
		c.calls = map[string]int{}
	}
	c.calls[name]++
}

func (c *countingStorage) count(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[name]
}

func (c *countingStorage) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, v := range c.calls {
		n += v
	}
	return n
}

func (c *countingStorage) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = map[string]int{}
}

func (c *countingStorage) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]string, 0, len(c.calls))
	for k := range c.calls {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, c.calls[k]))
	}
	return strings.Join(parts, " ")
}

func (c *countingStorage) WorkerPut(e, k, s []byte) error {
	c.hit("WorkerPut")
	return c.FunctionStorage.WorkerPut(e, k, s)
}
func (c *countingStorage) WorkerCollect(e []byte) ([][]byte, error) {
	c.hit("WorkerCollect")
	return c.FunctionStorage.WorkerCollect(e)
}
func (c *countingStorage) WorkerScan(e []byte) ([]WorkerStateEntry, error) {
	c.hit("WorkerScan")
	return c.FunctionStorage.WorkerScan(e)
}
func (c *countingStorage) QueuePush(e []byte, items [][]byte) (int, error) {
	c.hit("QueuePush")
	return c.FunctionStorage.QueuePush(e, items)
}
func (c *countingStorage) QueuePop(e []byte) ([]byte, error) {
	c.hit("QueuePop")
	return c.FunctionStorage.QueuePop(e)
}
func (c *countingStorage) AggregateStateGet(e []byte, g []int64) ([]AggregateStateEntry, error) {
	c.hit("AggregateStateGet")
	return c.FunctionStorage.AggregateStateGet(e, g)
}
func (c *countingStorage) AggregateStatePut(e []byte, entries []AggregateStateEntry) error {
	c.hit("AggregateStatePut")
	return c.FunctionStorage.AggregateStatePut(e, entries)
}
func (c *countingStorage) AggregateStateClear(e []byte) error {
	c.hit("AggregateStateClear")
	return c.FunctionStorage.AggregateStateClear(e)
}
func (c *countingStorage) AggregateConstArgsPut(e []byte, fn string, a []byte) error {
	c.hit("AggregateConstArgsPut")
	return c.FunctionStorage.AggregateConstArgsPut(e, fn, a)
}
func (c *countingStorage) AggregateConstArgsGet(e []byte, fn string) ([]byte, error) {
	c.hit("AggregateConstArgsGet")
	return c.FunctionStorage.AggregateConstArgsGet(e, fn)
}
func (c *countingStorage) AggregateWindowPartitionPut(e []byte, p int64, d []byte) error {
	c.hit("AggregateWindowPartitionPut")
	return c.FunctionStorage.AggregateWindowPartitionPut(e, p, d)
}
func (c *countingStorage) AggregateWindowPartitionGet(e []byte, p int64) ([]byte, error) {
	c.hit("AggregateWindowPartitionGet")
	return c.FunctionStorage.AggregateWindowPartitionGet(e, p)
}
func (c *countingStorage) AggregateWindowPartitionDelete(e []byte, p int64) error {
	c.hit("AggregateWindowPartitionDelete")
	return c.FunctionStorage.AggregateWindowPartitionDelete(e, p)
}
func (c *countingStorage) AggregateWindowPartitionClear(e []byte) error {
	c.hit("AggregateWindowPartitionClear")
	return c.FunctionStorage.AggregateWindowPartitionClear(e)
}

func (c *countingStorage) log() StateLogStorage { return c.FunctionStorage.(StateLogStorage) }

func (c *countingStorage) StateAppend(e, k, v []byte) (int64, error) {
	c.hit("StateAppend")
	return c.log().StateAppend(e, k, v)
}
func (c *countingStorage) StateLogScan(e, k []byte, after int64, limit int) ([]StateLogEntry, error) {
	c.hit("StateLogScan")
	return c.log().StateLogScan(e, k, after, limit)
}
func (c *countingStorage) StateLogClear(e []byte) error {
	c.hit("StateLogClear")
	return c.log().StateLogClear(e)
}
