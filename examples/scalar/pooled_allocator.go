// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// pooledAllocator is a drop-in replacement for memory.GoAllocator that
// recycles backing slices via sync.Pool. The default GoAllocator is a
// near-no-op around `make([]byte, size+64)` — every per-batch Arrow
// builder allocation lands fresh on Go's heap and is fed straight to the
// GC. Under the scalar_multiply workload the worker was producing several
// GB/s of heap traffic, which in turn spent ~24% of worker CPU in
// runtime.gcDrain / scanObjectsSmall / madvise (measured via pprof).
//
// Routing those allocations through a sync.Pool keeps the underlying
// memory alive across batches: the GC still scans the pooled slices, but
// the trace footprint is bounded by the number of distinct sizes the
// workload exercises rather than by the batch rate, and the per-batch
// allocate/free cycle reduces to a sync.Pool.Get/Put pair (~50 ns each on
// uncontended pools, ~100-200 ns under contention) rather than a fresh
// 16 KB make() + GC mark-and-sweep work.
//
// Alignment matches memory.GoAllocator: every returned slice starts on a
// 64-byte boundary, computed by allocating size+64 bytes (when the pool
// misses) and slicing forward to the next aligned offset. When the slice
// comes from the pool it is already aligned because it was previously
// derived the same way.
//
// Safety: the Allocator interface requires that Free not retain references
// to slices owned by the caller; sync.Pool.Put is the only place we hold
// references back, and Allocate returns a re-sliced view, so two
// concurrent users that respect the contract can never see each other's
// memory.
type pooledAllocator struct {
	pool sync.Pool
}

// global default; safe to share — sync.Pool is goroutine-safe.
var sharedPooledAllocator = &pooledAllocator{}

// Compile-time check that we satisfy the interface; mirrors GoAllocator.
var _ memory.Allocator = (*pooledAllocator)(nil)

// Allocate returns a 64-byte-aligned []byte of the requested size,
// pulling from the pool when an existing buffer is large enough.
//
// When the pool returns a buffer smaller than size it is returned to the
// pool — we'd rather take the GC hit of one fresh allocation than discard
// a useful (smaller-but-not-zero) cache entry. With workloads dominated
// by a single batch size (the common case for scalar functions) the pool
// converges to a small number of equally-sized slices in steady state.
func (a *pooledAllocator) Allocate(size int) []byte {
	if size <= 0 {
		return nil
	}
	if v := a.pool.Get(); v != nil {
		b := v.([]byte)
		if cap(b) >= size {
			return b[:size]
		}
		a.pool.Put(b)
	}
	// Miss — mirror GoAllocator's alignment dance: over-allocate by one
	// alignment quantum (64), find the next aligned offset, and return a
	// 3-arg slice so cap == size. Any reslicing the caller does (via
	// builder.Reserve growth, etc.) goes through Reallocate.
	const alignment = 64
	buf := make([]byte, size+alignment)
	addr := uintptr(unsafe.Pointer(&buf[0]))
	next := (addr + alignment - 1) &^ (alignment - 1)
	shift := int(next - addr)
	return buf[shift : size+shift : size+shift]
}

// Reallocate matches GoAllocator: when the existing buffer's cap is
// already big enough, just reslice; otherwise allocate fresh and copy
// the live bytes. The old buffer is returned to the pool so the next
// shrink-then-grow cycle can re-use it.
func (a *pooledAllocator) Reallocate(size int, b []byte) []byte {
	if cap(b) >= size {
		return b[:size]
	}
	newBuf := a.Allocate(size)
	copy(newBuf, b)
	a.Free(b)
	return newBuf
}

// Free returns the slice to the pool. Caller must not touch the slice
// after this call (the standard Allocator contract).
//
// We pool only non-trivial slices: empty / nil-backed inputs are
// dropped on the floor. The sync.Pool tolerates arbitrary cap()s, so we
// don't classify by size class — under a single dominant batch size the
// pool's age-based scavenging keeps the footprint bounded.
func (a *pooledAllocator) Free(b []byte) {
	if cap(b) == 0 {
		return
	}
	a.pool.Put(b[:0])
}
