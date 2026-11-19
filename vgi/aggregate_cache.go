// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"encoding/hex"
	"fmt"
	"time"
)

// Per-process caches of an aggregate execution's invariant inputs, so the
// per-batch RPCs (update, combine, finalize, window) do not re-read them from
// FunctionStorage -- a 30-90 ms round trip on a remote backend. Both are
// bounded (LRU + TTL) and keyed by the attach shard as well as the execution,
// so another attach's request never reuses them.

// aggConstKey names an aggregate execution's constant arguments.
type aggConstKey struct {
	shardKey     string
	executionID  string
	functionName string
}

const (
	aggConstCacheMaxEntries = 1024
	aggConstCacheTTL        = 30 * time.Minute

	windowPartitionCacheMaxEntries = 64
	windowPartitionCacheMaxBytes   = 256 << 20
	windowPartitionCacheTTL        = 15 * time.Minute
)

func newAggConstArgsCache() *boundedCache[aggConstKey, []byte] {
	return newBoundedCache[aggConstKey, []byte](aggConstCacheMaxEntries, aggConstCacheTTL)
}

// aggConstSlot is where bind stores the sealed constant arguments.
func aggConstSlot(shardKey string, executionID []byte, functionName string) storageSlot {
	return storageSlot{ShardKey: shardKey, ScopeID: executionID, NS: storageNSAggregateConst, Key: []byte(functionName)}
}

// noConstArgs is the cached "bind stored none" marker (distinct from a miss).
var noConstArgs = []byte{}

// declaresConstArgs reports whether fn has any constant parameter -- if not,
// bind never stores const args and nothing needs to be read.
func declaresConstArgs(fn AggregateFunction) bool {
	for _, s := range fn.ArgumentSpecs() {
		if s.IsConst {
			return true
		}
	}
	return false
}

// putAggConstArgs seals and stores the bind-time constant arguments (nil:
// none), and primes this process's cache either way.
func (w *Worker) putAggConstArgs(fnName string, execID []byte, shardKey string, args []byte) error {
	key := aggConstKey{shardKey: shardKey, executionID: hex.EncodeToString(execID), functionName: fnName}
	if len(args) == 0 {
		w.aggConstArgs.Put(key, noConstArgs)
		return nil
	}
	sealed, err := w.sealStoragePayload(args, aggConstSlot(shardKey, execID, fnName))
	if err != nil {
		return err
	}
	if err := w.aggStorage.bucket(fnName, execID, shardKey).putConstArgs(sealed); err != nil {
		return err
	}
	w.aggConstArgs.Put(key, append([]byte(nil), args...))
	return nil
}

// loadAggArgs returns the bind-time arguments stashed by handleAggregateBind:
// from this process's cache, else (once per execution) from storage. A function
// with no constant parameter never reads storage.
func (w *Worker) loadAggArgs(fn AggregateFunction, funcName string, execID []byte, shardKey string) (*Arguments, error) {
	key := aggConstKey{shardKey: shardKey, executionID: hex.EncodeToString(execID), functionName: funcName}
	data, ok := w.aggConstArgs.Get(key)
	if !ok {
		if fn != nil && !declaresConstArgs(fn) {
			return nil, nil
		}
		sealed, err := w.aggStorage.bucket(funcName, execID, shardKey).getConstArgs()
		if err != nil {
			return nil, fmt.Errorf("aggregate: loading constant arguments: %w", err)
		}
		data = noConstArgs
		if len(sealed) > 0 {
			if data, err = w.openStoragePayload(sealed, aggConstSlot(shardKey, execID, funcName)); err != nil {
				return nil, err
			}
		}
		w.aggConstArgs.Put(key, data)
	}
	if len(data) == 0 {
		return nil, nil
	}
	args, err := ParseArguments(data)
	if err != nil {
		return nil, fmt.Errorf("aggregate: deserializing constant arguments: %w", err)
	}
	return args, nil
}

// evictAggregateExecution drops this process's cached inputs of one aggregate
// execution (destructor).
func (w *Worker) evictAggregateExecution(shardKey string, execID []byte) {
	h := hex.EncodeToString(execID)
	w.aggConstArgs.DeleteFunc(func(k aggConstKey) bool { return k.shardKey == shardKey && k.executionID == h })
	w.windowPartitions.DeleteFunc(func(k windowPartitionKey) bool { return k.shardKey == shardKey && k.executionID == h })
}

// windowPartitionKey names one window partition of an execution.
type windowPartitionKey struct {
	shardKey     string
	executionID  string
	functionName string
	partitionID  int64
}

// cachedWindowPartition is a decoded partition plus the raw WindowInit state.
// The partition is shared read-only between calls; the window state is decoded
// per call, so a Window implementation that mutates its state never races
// another evaluate block.
type cachedWindowPartition struct {
	partition   *WindowPartition
	windowState []byte
	size        int64
}

func newWindowPartitionCache() *boundedCache[windowPartitionKey, *cachedWindowPartition] {
	c := newBoundedCache[windowPartitionKey, *cachedWindowPartition](windowPartitionCacheMaxEntries, windowPartitionCacheTTL)
	c.maxCost = windowPartitionCacheMaxBytes
	c.costOf = func(p *cachedWindowPartition) int64 { return p.size }
	return c
}
