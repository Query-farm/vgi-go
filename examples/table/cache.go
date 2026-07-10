// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Result-cache fixtures — table generators that advertise vgi.cache.*.
//
// These exist so SQL integration tests (and the C++ result cache) can exercise
// cacheable table-function results end to end. Each generator returns a small
// deterministic result and folds cache-control metadata onto its FIRST emitted
// batch via vgi.Emit(batch, vgi.WithCacheControl(...)). Mirrors vgi-python's
// vgi/_test_fixtures/table/cache.py.
//
// The multi-worker / nested-type / partitioned fixtures live in
// cache_advanced.go.

package table

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// cacheDefaultTTL is the freshness lifetime (seconds) for the fixtures that
// don't take a ttl argument. Long enough that TTL never lapses mid-test.
const cacheDefaultTTL = 300

// cacheNonceCounter is a process-global monotonic counter. It advances once per
// REAL invocation of the nonce fixtures (in NewState, which the client only
// reaches on a cache MISS). A pooled worker persists it across calls, so a
// served-from-cache hit never advances it — that is exactly the HIT/MISS signal
// the tests assert on.
var cacheNonceCounter atomic.Int64

// nextCacheNonce mints the next nonce, starting at 0 (mirrors itertools.count()).
func nextCacheNonce() int64 { return cacheNonceCounter.Add(1) - 1 }

// cacheTTL builds a plain TTL cache-control, or nil when this is not the
// stream's first batch (the client reads cache-control off the first batch
// only).
func cacheTTL(firstBatch bool, ttl int64) *vgi.CacheControl {
	if !firstBatch {
		return nil
	}
	return &vgi.CacheControl{Ttl: vgi.Seconds(ttl)}
}

// cacheCountdownState tracks remaining rows and the current position for the
// batch-splitting cache fixtures. Ttl is the advertised freshness lifetime, read
// once at NewState by the fixtures that take it as an argument; the rest leave
// it zero and pass cacheDefaultTTL explicitly.
type cacheCountdownState struct {
	Remaining    int64
	CurrentIndex int64
	Ttl          int64
}

// emitRange emits the int64 rows [state.CurrentIndex, +size) under column name
// col, advertising cc on the first batch, and advances the countdown.
func (s *cacheCountdownState) emitRange(params *vgi.ProcessParams, out *vgirpc.OutputCollector, batchSize int64, cc *vgi.CacheControl) error {
	size := min(s.Remaining, batchSize)
	start := s.CurrentIndex
	arr := vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
	defer arr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, size)
	if err := vgi.Emit(out, batch, vgi.WithCacheControl(cc)); err != nil {
		return err
	}
	s.CurrentIndex += size
	s.Remaining -= size
	return nil
}

var cacheSingleInt64Schema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
}, nil)

var cacheNonceSchema = arrow.NewSchema([]arrow.Field{
	{Name: "nonce", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
}, nil)

// ---------------------------------------------------------------------------
// cacheable_numbers — the baseline cacheable generator
// ---------------------------------------------------------------------------

type cacheableNumbersArgs struct {
	N   int64 `vgi:"default=10,ge=0,doc=Number of rows to generate"`
	Ttl int64 `vgi:"default=300,ge=0,doc=Cache TTL in seconds"`
}

// CacheableNumbersFunction emits n rows [0..n) and advertises a cache TTL.
//
// A fresh call MISSes and stores; an identical repeat within ttl seconds serves
// from the client cache.
type CacheableNumbersFunction struct{}

var _ vgi.TypedTableFunc[cacheCountdownState] = (*CacheableNumbersFunction)(nil)

func (f *CacheableNumbersFunction) Name() string { return "cacheable_numbers" }

func (f *CacheableNumbersFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Emits n rows [0..n) and advertises a cache TTL",
		Categories:  []string{"generator", "cache"},
		Tags:        map[string]string{"category": "cache", "type": "generator"},
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT * FROM cacheable_numbers(10)", Description: "Cacheable sequence 0-9 with the default TTL"},
			{SQL: "SELECT * FROM cacheable_numbers(10, ttl := 60)", Description: "Cacheable sequence 0-9 with a 60s TTL"},
		},
	}
}

func (f *CacheableNumbersFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cacheableNumbersArgs{})
}

func (f *CacheableNumbersFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(cacheSingleInt64Schema)
}

func (f *CacheableNumbersFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CacheableNumbersFunction) NewState(params *vgi.ProcessParams) (*cacheCountdownState, error) {
	var args cacheableNumbersArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &cacheCountdownState{Remaining: args.N, Ttl: args.Ttl}, nil
}

func (f *CacheableNumbersFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cacheCountdownState, out *vgirpc.OutputCollector) error {
	if state.Remaining <= 0 {
		return out.Finish()
	}
	return state.emitRange(params, out, 1000, cacheTTL(state.CurrentIndex == 0, state.Ttl))
}

func NewCacheableNumbersFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cacheCountdownState](&CacheableNumbersFunction{})
}

// ---------------------------------------------------------------------------
// cache_nonce — value-level proof of a cache HIT
// ---------------------------------------------------------------------------

type cacheNoArgs struct{}

// cacheNonceState carries the per-invocation nonce.
type cacheNonceState struct {
	Nonce int64
	Done  bool
}

// CacheNonceFunction emits ONE row whose nonce changes on every real invocation.
//
// NewState (reached only on a cache MISS) advances a process-global counter, so
// the emitted value is stable across cache HITs and changes across MISSes — a
// value-level proof of cache behaviour independent of the log. The row count is
// always 1 and never depends on the wall clock.
type CacheNonceFunction struct{}

var _ vgi.TypedTableFunc[cacheNonceState] = (*CacheNonceFunction)(nil)

func (f *CacheNonceFunction) Name() string { return "cache_nonce" }

func (f *CacheNonceFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Emits one row with a per-invocation nonce; cacheable",
		Categories:  []string{"generator", "cache", "testing"},
		Tags:        map[string]string{"category": "cache", "type": "nonce"},
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT * FROM cache_nonce()", Description: "One-row cacheable result; nonce is stable on a cache hit"},
		},
	}
}

func (f *CacheNonceFunction) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(cacheNoArgs{}) }

func (f *CacheNonceFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(cacheNonceSchema)
}

func (f *CacheNonceFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CacheNonceFunction) NewState(params *vgi.ProcessParams) (*cacheNonceState, error) {
	return &cacheNonceState{Nonce: nextCacheNonce()}, nil
}

func (f *CacheNonceFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cacheNonceState, out *vgirpc.OutputCollector) error {
	if state.Done {
		return out.Finish()
	}
	nonce := state.Nonce
	arr := vgi.BuildInt64Array(1, func(int64) int64 { return nonce })
	defer arr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, 1)
	if err := vgi.Emit(out, batch, vgi.WithCacheControl(&vgi.CacheControl{Ttl: vgi.Seconds(cacheDefaultTTL)})); err != nil {
		return err
	}
	state.Done = true
	return nil
}

func NewCacheNonceFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cacheNonceState](&CacheNonceFunction{})
}

// ---------------------------------------------------------------------------
// cache_no_store — advertises cache metadata that forbids caching
// ---------------------------------------------------------------------------

type cacheNoStoreArgs struct {
	N int64 `vgi:"default=10,ge=0,doc=Number of rows to generate"`
}

// CacheNoStoreFunction emits n rows but advertises vgi.cache.no_store.
//
// The client must always re-invoke the worker even though the result carries
// cache metadata — no_store overrides any freshness key.
type CacheNoStoreFunction struct{}

var _ vgi.TypedTableFunc[cacheCountdownState] = (*CacheNoStoreFunction)(nil)

func (f *CacheNoStoreFunction) Name() string { return "cache_no_store" }

func (f *CacheNoStoreFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Emits n rows but advertises no_store (never cached)",
		Categories:  []string{"generator", "cache", "testing"},
		Tags:        map[string]string{"category": "cache", "type": "no_store"},
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT * FROM cache_no_store(5)", Description: "Emit 5 rows that must never be cached"},
		},
	}
}

func (f *CacheNoStoreFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cacheNoStoreArgs{})
}

func (f *CacheNoStoreFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(cacheSingleInt64Schema)
}

func (f *CacheNoStoreFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CacheNoStoreFunction) NewState(params *vgi.ProcessParams) (*cacheCountdownState, error) {
	var args cacheNoStoreArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &cacheCountdownState{Remaining: args.N}, nil
}

func (f *CacheNoStoreFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cacheCountdownState, out *vgirpc.OutputCollector) error {
	if state.Remaining <= 0 {
		return out.Finish()
	}
	var cc *vgi.CacheControl
	if state.CurrentIndex == 0 {
		cc = &vgi.CacheControl{NoStore: true}
	}
	return state.emitRange(params, out, 1000, cc)
}

func NewCacheNoStoreFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cacheCountdownState](&CacheNoStoreFunction{})
}

// ---------------------------------------------------------------------------
// cache_scoped_txn — scope=transaction
// ---------------------------------------------------------------------------

type cacheScopedTxnArgs struct {
	N int64 `vgi:"default=10,ge=0,doc=Number of rows to generate"`
}

// cacheScopedTxnState is a countdown plus a per-invocation nonce, which proves a
// same-txn hit vs a new-txn miss.
type cacheScopedTxnState struct {
	Remaining    int64
	CurrentIndex int64
	Nonce        int64
}

// CacheScopedTxnFunction emits (n, nonce) rows and advertises scope=transaction.
//
// The result is only reusable within the same transaction (the client folds the
// transaction id into the cache key); a fresh transaction MISSes. Nonce is
// bumped once per REAL invocation, so a same-transaction HIT returns the SAME
// nonce while a new-transaction MISS returns a fresh one — the hit/miss is
// provable from the value, not just the logs.
type CacheScopedTxnFunction struct{}

var _ vgi.TypedTableFunc[cacheScopedTxnState] = (*CacheScopedTxnFunction)(nil)

func (f *CacheScopedTxnFunction) Name() string { return "cache_scoped_txn" }

func (f *CacheScopedTxnFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Emits n rows and advertises scope=transaction",
		Categories:  []string{"generator", "cache", "testing"},
		Tags:        map[string]string{"category": "cache", "type": "scope"},
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT * FROM cache_scoped_txn(5)", Description: "Transaction-scoped cacheable result"},
		},
	}
}

func (f *CacheScopedTxnFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cacheScopedTxnArgs{})
}

func (f *CacheScopedTxnFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "nonce", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil))
}

func (f *CacheScopedTxnFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CacheScopedTxnFunction) NewState(params *vgi.ProcessParams) (*cacheScopedTxnState, error) {
	var args cacheScopedTxnArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &cacheScopedTxnState{Remaining: args.N, Nonce: nextCacheNonce()}, nil
}

func (f *CacheScopedTxnFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cacheScopedTxnState, out *vgirpc.OutputCollector) error {
	if state.Remaining <= 0 {
		return out.Finish()
	}
	size := min(state.Remaining, 1000)
	start := state.CurrentIndex
	nArr := vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
	defer nArr.Release()
	nonce := state.Nonce
	nonceArr := vgi.BuildInt64Array(size, func(int64) int64 { return nonce })
	defer nonceArr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{nArr, nonceArr}, size)

	var cc *vgi.CacheControl
	if state.CurrentIndex == 0 {
		cc = &vgi.CacheControl{Ttl: vgi.Seconds(cacheDefaultTTL), Scope: vgi.CacheScopeTransaction}
	}
	if err := vgi.Emit(out, batch, vgi.WithCacheControl(cc)); err != nil {
		return err
	}
	state.CurrentIndex += size
	state.Remaining -= size
	return nil
}

func NewCacheScopedTxnFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cacheScopedTxnState](&CacheScopedTxnFunction{})
}

// ---------------------------------------------------------------------------
// cache_big — many small batches
// ---------------------------------------------------------------------------

type cacheBigArgs struct {
	Rows int64 `vgi:"default=5000,ge=0,doc=Number of rows to generate"`
}

// CacheBigFunction emits rows rows across MANY small batches; advertises a ttl.
//
// The small batch size (1000) forces multi-batch capture and multi-batch replay
// on serve, exercising parallel capture / serve and the size ceiling.
type CacheBigFunction struct{}

var _ vgi.TypedTableFunc[cacheCountdownState] = (*CacheBigFunction)(nil)

func (f *CacheBigFunction) Name() string { return "cache_big" }

func (f *CacheBigFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Emits many small batches totaling `rows` rows; cacheable",
		Categories:  []string{"generator", "cache", "testing"},
		Tags:        map[string]string{"category": "cache", "type": "multi_batch"},
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT count(*) FROM cache_big(50000)", Description: "Large multi-batch cacheable result"},
		},
	}
}

func (f *CacheBigFunction) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(cacheBigArgs{}) }

func (f *CacheBigFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(cacheSingleInt64Schema)
}

func (f *CacheBigFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CacheBigFunction) NewState(params *vgi.ProcessParams) (*cacheCountdownState, error) {
	var args cacheBigArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &cacheCountdownState{Remaining: args.Rows}, nil
}

func (f *CacheBigFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cacheCountdownState, out *vgirpc.OutputCollector) error {
	if state.Remaining <= 0 {
		return out.Finish()
	}
	return state.emitRange(params, out, 1000, cacheTTL(state.CurrentIndex == 0, cacheDefaultTTL))
}

func NewCacheBigFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cacheCountdownState](&CacheBigFunction{})
}

// ---------------------------------------------------------------------------
// cache_revalidatable — conditional revalidation (304 / not_modified)
// ---------------------------------------------------------------------------

// cacheRevalidatableETag is this fixture's strong validator. Its data never
// changes, so a matching If-None-Match always answers 304.
const cacheRevalidatableETag = `"rev-v1"`

// CacheRevalidatableFunction emits ONE nonce row and advertises a validated,
// always-revalidate result.
//
// It advertises ttl=0 + etag + revalidatable — the "no-cache" semantic: the
// client stores the payload but marks it immediately stale, so every repeat
// sends a conditional request (vgi.cache.if_none_match). Because this fixture's
// data never changes, Process sees the matching IfNoneMatch and answers with a
// 0-row not_modified batch instead of re-emitting — so the client reuses the
// STORED nonce. A stable nonce across repeats therefore proves the not_modified
// path served cached bytes without re-streaming.
type CacheRevalidatableFunction struct{}

var _ vgi.TypedTableFunc[cacheNonceState] = (*CacheRevalidatableFunction)(nil)

func (f *CacheRevalidatableFunction) Name() string { return "cache_revalidatable" }

func (f *CacheRevalidatableFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Emits one nonce row; always-revalidate (304 not_modified)",
		Categories:  []string{"generator", "cache", "testing"},
		Tags:        map[string]string{"category": "cache", "type": "revalidatable"},
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT * FROM cache_revalidatable()", Description: "Conditionally-revalidated result (304 reuses stored bytes)"},
		},
	}
}

func (f *CacheRevalidatableFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cacheNoArgs{})
}

func (f *CacheRevalidatableFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(cacheNonceSchema)
}

func (f *CacheRevalidatableFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CacheRevalidatableFunction) NewState(params *vgi.ProcessParams) (*cacheNonceState, error) {
	return &cacheNonceState{Nonce: nextCacheNonce()}, nil
}

func (f *CacheRevalidatableFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cacheNonceState, out *vgirpc.OutputCollector) error {
	if state.Done {
		return out.Finish()
	}
	state.Done = true

	if params.IfNoneMatch != nil && *params.IfNoneMatch == cacheRevalidatableETag {
		// 304 Not Modified: the client's stored copy is still valid. Emit a
		// 0-row not_modified batch (fresh validators + ttl=0 so it keeps
		// revalidating) — the client reuses its stored payload.
		empty := vgi.BuildInt64Array(0, func(int64) int64 { return 0 })
		defer empty.Release()
		batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{empty}, 0)
		return vgi.Emit(out, batch, vgi.WithCacheControl(&vgi.CacheControl{
			NotModified:   true,
			Ttl:           vgi.Seconds(0),
			ETag:          cacheRevalidatableETag,
			Revalidatable: true,
		}))
	}

	// Fresh result: emit the nonce + advertise the always-revalidate contract.
	nonce := state.Nonce
	arr := vgi.BuildInt64Array(1, func(int64) int64 { return nonce })
	defer arr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, 1)
	return vgi.Emit(out, batch, vgi.WithCacheControl(&vgi.CacheControl{
		Ttl:           vgi.Seconds(0),
		ETag:          cacheRevalidatableETag,
		Revalidatable: true,
	}))
}

func NewCacheRevalidatableFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cacheNonceState](&CacheRevalidatableFunction{})
}

// ---------------------------------------------------------------------------
// cache_multicol — projection-coverage reuse
// ---------------------------------------------------------------------------

type cacheMultiColArgs struct {
	N   int64 `vgi:"default=4,ge=0,doc=Number of rows to generate"`
	Ttl int64 `vgi:"default=300,ge=0,doc=Cache TTL in seconds"`
}

// CacheMultiColFunction emits n rows of three columns (a, b, c) = (i, i*10, i*100).
//
// A multi-column cacheable result: SELECT b reuses the SELECT * cache entry (the
// generator doesn't push projection, so both scans share the same key and DuckDB
// projects locally — projection-coverage reuse).
type CacheMultiColFunction struct{}

var _ vgi.TypedTableFunc[cacheCountdownState] = (*CacheMultiColFunction)(nil)

func (f *CacheMultiColFunction) Name() string { return "cache_multicol" }

func (f *CacheMultiColFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Emits n rows of (a, b, c); cacheable, multi-column",
		Categories:  []string{"generator", "cache", "testing"},
		Tags:        map[string]string{"category": "cache", "type": "multicol"},
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT b FROM cache_multicol()", Description: "Subset projection reuses the full-result cache entry"},
		},
	}
}

func (f *CacheMultiColFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cacheMultiColArgs{})
}

var cacheMultiColSchema = arrow.NewSchema([]arrow.Field{
	{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "c", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
}, nil)

func (f *CacheMultiColFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(cacheMultiColSchema)
}

func (f *CacheMultiColFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CacheMultiColFunction) NewState(params *vgi.ProcessParams) (*cacheCountdownState, error) {
	var args cacheMultiColArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &cacheCountdownState{Remaining: args.N}, nil
}

func (f *CacheMultiColFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cacheCountdownState, out *vgirpc.OutputCollector) error {
	if state.Remaining <= 0 {
		return out.Finish()
	}
	rows := state.Remaining
	a := vgi.BuildInt64Array(rows, func(i int64) int64 { return i })
	defer a.Release()
	b := vgi.BuildInt64Array(rows, func(i int64) int64 { return i * 10 })
	defer b.Release()
	c := vgi.BuildInt64Array(rows, func(i int64) int64 { return i * 100 })
	defer c.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{a, b, c}, rows)
	if err := vgi.Emit(out, batch, vgi.WithCacheControl(&vgi.CacheControl{Ttl: vgi.Seconds(cacheDefaultTTL)})); err != nil {
		return err
	}
	state.Remaining = 0
	return nil
}

func NewCacheMultiColFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cacheCountdownState](&CacheMultiColFunction{})
}

// ---------------------------------------------------------------------------
// cache_whoami — identity-scoped cacheable result
// ---------------------------------------------------------------------------

// CacheWhoamiFunction emits ONE row = the caller's auth principal ("anonymous"
// if none); cacheable.
//
// The linchpin of the cache token-isolation test: two attaches of the same
// worker with different bearer tokens map to different principals, so their
// results MUST land under different (identity-scoped) cache keys and never
// cross-serve. Bearer/OAuth identity is HTTP-only; over subprocess every caller
// is "anonymous".
type CacheWhoamiFunction struct{}

var _ vgi.TypedTableFunc[cacheNonceState] = (*CacheWhoamiFunction)(nil)

func (f *CacheWhoamiFunction) Name() string { return "cache_whoami" }

func (f *CacheWhoamiFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Emits the caller's auth principal; cacheable (identity-scoped)",
		Categories:  []string{"generator", "cache", "testing"},
		Tags:        map[string]string{"category": "cache", "type": "identity"},
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT who FROM cache_whoami()", Description: "One-row cacheable result echoing the caller's principal"},
		},
	}
}

func (f *CacheWhoamiFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cacheNoArgs{})
}

func (f *CacheWhoamiFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "who", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil))
}

func (f *CacheWhoamiFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CacheWhoamiFunction) NewState(params *vgi.ProcessParams) (*cacheNonceState, error) {
	return &cacheNonceState{}, nil
}

func (f *CacheWhoamiFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cacheNonceState, out *vgirpc.OutputCollector) error {
	if state.Done {
		return out.Finish()
	}
	who := "anonymous"
	if params.Auth != nil && params.Auth.Principal != "" {
		who = params.Auth.Principal
	}
	arr := vgi.BuildStringArray(1, func(int64) string { return who })
	defer arr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, 1)
	if err := vgi.Emit(out, batch, vgi.WithCacheControl(&vgi.CacheControl{Ttl: vgi.Seconds(cacheDefaultTTL)})); err != nil {
		return err
	}
	state.Done = true
	return nil
}

func NewCacheWhoamiFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cacheNonceState](&CacheWhoamiFunction{})
}

// ---------------------------------------------------------------------------
// cache_versioned_scan — AT-keyed cache isolation
// ---------------------------------------------------------------------------

// cacheVersionedData maps a data version to its rows. The schema is fixed
// across versions, so table_get needs no per-version override — only the
// scan-function argument changes. The catalog maps AT -> the version argument.
var cacheVersionedData = map[int64][]int64{
	1: {101, 102, 103},
	2: {201, 202},
	3: {301, 302, 303, 304},
}

// cacheVersionedCurrent is the version a scan without an AT clause resolves to.
const cacheVersionedCurrent = 3

type cacheVersionedArgs struct {
	Version int64 `vgi:"pos=0,doc=Data version, resolved from the AT clause by the catalog"`
}

// CacheVersionedFunction emits version-specific rows (fixed schema); cacheable.
//
// For AT cache-isolation: AT (VERSION => 1) / AT (VERSION => 2) / live must
// produce distinct cache entries whose bytes never cross-serve — the cache key
// folds at_unit/at_value. An AT-pinned scan is an immutable snapshot (the client
// marks it never-expires); live uses the TTL.
type CacheVersionedFunction struct{}

var _ vgi.TypedTableFunc[cacheNonceState] = (*CacheVersionedFunction)(nil)

func (f *CacheVersionedFunction) Name() string { return "cache_versioned_scan" }

func (f *CacheVersionedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Version-specific rows; cacheable (AT-keyed)",
		Categories:  []string{"generator", "cache", "testing"},
		Tags:        map[string]string{"category": "cache", "type": "time_travel"},
	}
}

func (f *CacheVersionedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cacheVersionedArgs{})
}

func (f *CacheVersionedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil))
}

func (f *CacheVersionedFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CacheVersionedFunction) NewState(params *vgi.ProcessParams) (*cacheNonceState, error) {
	return &cacheNonceState{}, nil
}

func (f *CacheVersionedFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cacheNonceState, out *vgirpc.OutputCollector) error {
	if state.Done {
		return out.Finish()
	}
	var args cacheVersionedArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return err
	}
	data, ok := cacheVersionedData[args.Version]
	if !ok {
		data = cacheVersionedData[cacheVersionedCurrent]
	}
	rows := int64(len(data))
	arr := vgi.BuildInt64Array(rows, func(i int64) int64 { return data[i] })
	defer arr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, rows)
	if err := vgi.Emit(out, batch, vgi.WithCacheControl(&vgi.CacheControl{Ttl: vgi.Seconds(cacheDefaultTTL)})); err != nil {
		return err
	}
	state.Done = true
	return nil
}

func NewCacheVersionedFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cacheNonceState](&CacheVersionedFunction{})
}

// ---------------------------------------------------------------------------
// cache_projection — projection pushdown keys distinct cache entries
// ---------------------------------------------------------------------------

var cacheProjectionData = map[string][]int64{
	"a": {1, 2, 3},
	"b": {10, 20, 30},
	"c": {100, 200, 300},
}

// CacheProjectionFunction is a 3-column generator that PUSHES projection;
// cacheable.
//
// For the projection-pushdown cross-serve check: because projection_pushdown is
// on, SELECT a and SELECT b push distinct projection_ids that are part of the
// cache key — so each column's scan caches only its own bytes under a distinct
// key, and one column's result can never be served for another's.
type CacheProjectionFunction struct{}

var _ vgi.TypedTableFunc[cacheNonceState] = (*CacheProjectionFunction)(nil)

func (f *CacheProjectionFunction) Name() string { return "cache_projection" }

func (f *CacheProjectionFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "3-column projection-pushdown generator; cacheable",
		Categories:         []string{"generator", "cache", "testing"},
		Tags:               map[string]string{"category": "cache", "type": "projection"},
		ProjectionPushdown: true,
	}
}

func (f *CacheProjectionFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cacheNoArgs{})
}

func (f *CacheProjectionFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(cacheMultiColSchema)
}

func (f *CacheProjectionFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CacheProjectionFunction) NewState(params *vgi.ProcessParams) (*cacheNonceState, error) {
	return &cacheNonceState{}, nil
}

func (f *CacheProjectionFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cacheNonceState, out *vgirpc.OutputCollector) error {
	if state.Done {
		return out.Finish()
	}
	fields := params.OutputSchema.Fields()
	cols := make([]arrow.Array, len(fields))
	for i, field := range fields {
		values, ok := cacheProjectionData[field.Name]
		if !ok {
			return fmt.Errorf("cache_projection: unknown projected column %q", field.Name)
		}
		arr := vgi.BuildInt64Array(3, func(j int64) int64 { return values[j] })
		defer arr.Release()
		cols[i] = arr
	}
	batch := array.NewRecordBatch(params.OutputSchema, cols, 3)
	if err := vgi.Emit(out, batch, vgi.WithCacheControl(&vgi.CacheControl{Ttl: vgi.Seconds(cacheDefaultTTL)})); err != nil {
		return err
	}
	state.Done = true
	return nil
}

func NewCacheProjectionFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cacheNonceState](&CacheProjectionFunction{})
}

// ---------------------------------------------------------------------------
// cache_poison — cacheable first batch then a mid-stream worker error
// ---------------------------------------------------------------------------

// cachePoisonState tracks the two-tick poison sequence: the cacheable batch,
// then the failure.
type cachePoisonState struct {
	Emitted  bool
	Poisoned bool
}

// emitPoisonPrefix emits the fixed cacheable [0,1,2] batch both poison fixtures
// stream before they fail.
func emitPoisonPrefix(params *vgi.ProcessParams, out *vgirpc.OutputCollector) error {
	arr := vgi.BuildInt64Array(3, func(i int64) int64 { return i })
	defer arr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, 3)
	return vgi.Emit(out, batch, vgi.WithCacheControl(&vgi.CacheControl{Ttl: vgi.Seconds(cacheDefaultTTL)}))
}

// CachePoisonFunction emits a cacheable first batch, then FAILS on the next tick.
//
// Adversarial check of the never-partial invariant: a worker error AFTER a
// cacheable batch has streamed must commit NOTHING to the cache (the failing
// thread never reaches EOS, so eos < launched and no entry is stored).
type CachePoisonFunction struct{}

var _ vgi.TypedTableFunc[cachePoisonState] = (*CachePoisonFunction)(nil)

func (f *CachePoisonFunction) Name() string { return "cache_poison" }

func (f *CachePoisonFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Cacheable first batch then a mid-stream error (never-partial check)",
		Categories:  []string{"generator", "cache", "testing"},
		Tags:        map[string]string{"category": "cache", "type": "poison"},
	}
}

func (f *CachePoisonFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cacheNoArgs{})
}

func (f *CachePoisonFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(cacheSingleInt64Schema)
}

func (f *CachePoisonFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CachePoisonFunction) NewState(params *vgi.ProcessParams) (*cachePoisonState, error) {
	return &cachePoisonState{}, nil
}

func (f *CachePoisonFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cachePoisonState, out *vgirpc.OutputCollector) error {
	if !state.Emitted {
		if err := emitPoisonPrefix(params, out); err != nil {
			return err
		}
		state.Emitted = true
		return nil
	}
	return fmt.Errorf("cache_poison: intentional mid-stream failure after a cacheable batch")
}

func NewCachePoisonFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cachePoisonState](&CachePoisonFunction{})
}

// ---------------------------------------------------------------------------
// cache_external_fail — cacheable first batch then an unresolvable pointer batch
// ---------------------------------------------------------------------------

// cacheUnresolvableLocation is an unreachable loopback URL (http, no TLS
// handshake). Port 9 (discard) is closed, so resolution fails fast with
// connection-refused. The poison test also lowers http_retries/http_timeout so
// the failure is bounded and quick.
const cacheUnresolvableLocation = "http://127.0.0.1:9/vgi-cache-poison-nonexistent"

// CacheExternalFailFunction emits a cacheable batch, then a pointer batch to an
// unreachable location.
//
// The client's resolution of the EXTERNAL_LOCATION pointer throws mid-stream.
// Second adversarial never-partial check: an external-location resolution
// failure after a cacheable batch must also commit nothing. The 0-row pointer
// batch carries vgi_rpc.location metadata (the same key the transport uses for
// externalized batches); the client fetches the URL, fails, and aborts the scan.
type CacheExternalFailFunction struct{}

var _ vgi.TypedTableFunc[cachePoisonState] = (*CacheExternalFailFunction)(nil)

func (f *CacheExternalFailFunction) Name() string { return "cache_external_fail" }

func (f *CacheExternalFailFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Cacheable first batch then an unresolvable external-location pointer",
		Categories:  []string{"generator", "cache", "testing"},
		Tags:        map[string]string{"category": "cache", "type": "poison_external"},
	}
}

func (f *CacheExternalFailFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cacheNoArgs{})
}

func (f *CacheExternalFailFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(cacheSingleInt64Schema)
}

func (f *CacheExternalFailFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CacheExternalFailFunction) NewState(params *vgi.ProcessParams) (*cachePoisonState, error) {
	return &cachePoisonState{}, nil
}

// Process emits one cacheable batch, then an unresolvable external-location
// pointer. Over HTTP the client resolves the pointer, fails, and aborts the scan
// before this method is ticked again. The terminal Finish (reached only if
// resolution were to somehow succeed) keeps the producer from looping forever on
// transports that don't resolve external locations.
func (f *CacheExternalFailFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cachePoisonState, out *vgirpc.OutputCollector) error {
	if !state.Emitted {
		if err := emitPoisonPrefix(params, out); err != nil {
			return err
		}
		state.Emitted = true
		return nil
	}
	if !state.Poisoned {
		empty := vgi.BuildInt64Array(0, func(int64) int64 { return 0 })
		defer empty.Release()
		batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{empty}, 0)
		if err := vgi.Emit(out, batch, vgi.WithMetadata("vgi_rpc.location", cacheUnresolvableLocation)); err != nil {
			return err
		}
		state.Poisoned = true
		return nil
	}
	return out.Finish()
}

func NewCacheExternalFailFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cachePoisonState](&CacheExternalFailFunction{})
}
