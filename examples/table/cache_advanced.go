// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Result-cache fixtures, continued: the multi-worker, order-sensitive,
// nested-type, filtered and partitioned generators. See cache.go for the
// baseline fixtures and the shared cache-control helpers.

package table

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ---------------------------------------------------------------------------
// Work-queue item packing, shared by the fan-out cache fixtures.
// ---------------------------------------------------------------------------

// packRange encodes a (start, end) chunk as two big-endian uint64.
func packRange(start, end int64) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], uint64(start))
	binary.BigEndian.PutUint64(b[8:16], uint64(end))
	return b
}

func unpackRange(b []byte) (start, end int64) {
	return int64(binary.BigEndian.Uint64(b[0:8])), int64(binary.BigEndian.Uint64(b[8:16]))
}

// packPartition encodes a (partition_id, start, end) chunk as three big-endian uint64.
func packPartition(pid, start, end int64) []byte {
	b := make([]byte, 24)
	binary.BigEndian.PutUint64(b[0:8], uint64(pid))
	binary.BigEndian.PutUint64(b[8:16], uint64(start))
	binary.BigEndian.PutUint64(b[16:24], uint64(end))
	return b
}

func unpackPartition(b []byte) (pid, start, end int64) {
	return int64(binary.BigEndian.Uint64(b[0:8])),
		int64(binary.BigEndian.Uint64(b[8:16])),
		int64(binary.BigEndian.Uint64(b[16:24]))
}

// ---------------------------------------------------------------------------
// cache_bench — parametrizable large cacheable result (scaling bench + S8 guard)
// ---------------------------------------------------------------------------

type cacheBenchArgs struct {
	// Positional (unlike the other cache fixtures' named-with-default args) so
	// a direct call — `ex.main.cache_bench(rows)` — actually honors the
	// requested row count; the scaling bench and the flat-RAM guard need a
	// result whose size they control.
	Rows int64 `vgi:"pos=0,ge=0,doc=Number of rows to generate"`
}

// CacheBenchFunction emits rows int64 rows across many small batches; cacheable.
//
// Purpose-built for the scaling work: a caller-controlled result size lets the
// C++ concurrency bench build a ~max_entry_bytes result (in-flight RAM) and lets
// the disk-streaming guard build a result larger than memory_limit. Advertises a
// ttl so it is cached like any other result.
type CacheBenchFunction struct{}

var _ vgi.TypedTableFunc[cacheCountdownState] = (*CacheBenchFunction)(nil)

func (f *CacheBenchFunction) Name() string { return "cache_bench" }

func (f *CacheBenchFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Emits `rows` int64 rows (positional arg); cacheable — scaling bench fixture",
		Categories:  []string{"generator", "cache", "testing"},
		Tags:        map[string]string{"category": "cache", "type": "bench"},
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT count(*) FROM cache_bench(1000000)", Description: "Million-row cacheable result for scaling tests"},
		},
	}
}

func (f *CacheBenchFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cacheBenchArgs{})
}

var cacheSingleVSchema = arrow.NewSchema([]arrow.Field{
	{Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
}, nil)

func (f *CacheBenchFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(cacheSingleVSchema)
}

func (f *CacheBenchFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CacheBenchFunction) NewState(params *vgi.ProcessParams) (*cacheCountdownState, error) {
	var args cacheBenchArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &cacheCountdownState{Remaining: args.Rows}, nil
}

func (f *CacheBenchFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cacheCountdownState, out *vgirpc.OutputCollector) error {
	if state.Remaining <= 0 {
		return out.Finish()
	}
	return state.emitRange(params, out, 2048, cacheTTL(state.CurrentIndex == 0, cacheDefaultTTL))
}

func NewCacheBenchFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cacheCountdownState](&CacheBenchFunction{})
}

// ---------------------------------------------------------------------------
// cache_parallel — MULTI-WORKER cacheable result (parallel capture)
// ---------------------------------------------------------------------------

// cacheParallelMaxChunks caps the fan-out at ~24 chunks regardless of size (like
// partitioned_sequence) so remote cost scales with fan-out, not row count.
const cacheParallelMaxChunks = 24

type cacheParallelArgs struct {
	Rows      int64 `vgi:"pos=0,ge=0,doc=Total number of rows to generate"`
	BatchSize int64 `vgi:"default=24000,ge=1,doc=Rows per output batch"`
}

// cacheParallelState is a per-worker cursor plus a one-shot advertise flag.
type cacheParallelState struct {
	Advertised   bool
	HasChunk     bool
	CurrentStart int64
	CurrentEnd   int64
	CurrentIdx   int64
	BatchSize    int64
}

// CacheParallelFunction is a multi-worker cacheable sequence — one capture
// substream per worker.
//
// Purpose-built to prove parallel capture and correct single-thread serve
// reassembly of N substreams: run under SET threads=8 and the cached entry holds
// >1 substream, yet a serve returns the complete union. Values are the plain
// sequence [0..rows) so COUNT and SUM hold regardless of how the chunks were
// distributed across workers. Advertises a ttl on each worker's first batch so
// the cache-control latches regardless of which worker emits first.
type CacheParallelFunction struct{}

var _ vgi.TypedTableFunc[cacheParallelState] = (*CacheParallelFunction)(nil)

func (f *CacheParallelFunction) Name() string { return "cache_parallel" }

func (f *CacheParallelFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Multi-worker cacheable sequence (one substream per worker); parallel-capture fixture",
		Categories:  []string{"generator", "cache", "testing"},
		Tags:        map[string]string{"category": "cache", "type": "parallel"},
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT count(*) FROM cache_parallel(1000000)", Description: "Parallel-captured cacheable result across workers"},
		},
	}
}

func (f *CacheParallelFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cacheParallelArgs{})
}

func (f *CacheParallelFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(cacheSingleVSchema)
}

// OnInit has the primary worker enqueue (start, end) chunks covering [0, rows).
func (f *CacheParallelFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	var args cacheParallelArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	chunk := max(int64(1), (args.Rows+cacheParallelMaxChunks-1)/cacheParallelMaxChunks)
	var items [][]byte
	for start := int64(0); start < args.Rows; start += chunk {
		items = append(items, packRange(start, min(start+chunk, args.Rows)))
	}
	if params.Storage != nil {
		if err := params.Storage.QueuePush(items); err != nil {
			return nil, err
		}
	}
	// Left at the framework default so the framework advertises multi-worker;
	// this is the only cache fixture that exercises parallel capture.
	return &vgi.GlobalInitResponse{}, nil
}

func (f *CacheParallelFunction) NewState(params *vgi.ProcessParams) (*cacheParallelState, error) {
	var args cacheParallelArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &cacheParallelState{BatchSize: args.BatchSize}, nil
}

func (f *CacheParallelFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cacheParallelState, out *vgirpc.OutputCollector) error {
	if !state.HasChunk || state.CurrentIdx >= state.CurrentEnd {
		if params.Storage == nil {
			return out.Finish()
		}
		work, err := params.Storage.QueuePop()
		if err != nil {
			return err
		}
		if work == nil {
			return out.Finish()
		}
		state.CurrentStart, state.CurrentEnd = unpackRange(work)
		state.CurrentIdx = state.CurrentStart
		state.HasChunk = true
	}

	batchEnd := min(state.CurrentIdx+state.BatchSize, state.CurrentEnd)
	start := state.CurrentIdx
	size := batchEnd - start
	arr := vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
	defer arr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, size)
	if err := vgi.Emit(out, batch, vgi.WithCacheControl(cacheTTL(!state.Advertised, cacheDefaultTTL))); err != nil {
		return err
	}
	state.Advertised = true
	state.CurrentIdx = batchEnd
	return nil
}

func NewCacheParallelFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cacheParallelState](&CacheParallelFunction{})
}

// ---------------------------------------------------------------------------
// cache_ordered — batch_index reassembly on serve
// ---------------------------------------------------------------------------

// cacheOrderedState is a per-worker cursor, a one-shot advertise flag, and the
// current partition id (emitted as the batch's batch_index).
type cacheOrderedState struct {
	Advertised   bool
	HasChunk     bool
	PartitionID  int64
	CurrentStart int64
	CurrentEnd   int64
	CurrentIdx   int64
}

// emitOrderedBatch pops a partition when the cursor is spent, then emits one
// batch_index-tagged batch of the global row indices.
func emitOrderedBatch(params *vgi.ProcessParams, state *cacheOrderedState, out *vgirpc.OutputCollector, batchSize int64) error {
	if !state.HasChunk || state.CurrentIdx >= state.CurrentEnd {
		if params.Storage == nil {
			return out.Finish()
		}
		work, err := params.Storage.QueuePop()
		if err != nil {
			return err
		}
		if work == nil {
			return out.Finish()
		}
		state.PartitionID, state.CurrentStart, state.CurrentEnd = unpackPartition(work)
		state.CurrentIdx = state.CurrentStart
		state.HasChunk = true
	}

	batchEnd := min(state.CurrentIdx+batchSize, state.CurrentEnd)
	start := state.CurrentIdx
	size := batchEnd - start
	arr := vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
	defer arr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, size)
	if err := vgi.EmitBatchIndex(out, batch, state.PartitionID, vgi.WithCacheControl(cacheTTL(!state.Advertised, cacheDefaultTTL))); err != nil {
		return err
	}
	state.Advertised = true
	state.CurrentIdx = batchEnd
	return nil
}

// pushPartitionChunks enqueues (partition_id, start, end) chunks covering
// [0, rows), in ascending partition order.
func pushPartitionChunks(params *vgi.InitParams, rows, chunk int64) error {
	var items [][]byte
	pid := int64(0)
	for start := int64(0); start < rows; start += chunk {
		items = append(items, packPartition(pid, start, min(start+chunk, rows)))
		pid++
	}
	if params.Storage == nil {
		return nil
	}
	return params.Storage.QueuePush(items)
}

const cacheOrderedBatchSize = 256

type cacheOrderedArgs struct {
	// Named-with-default (not positional) so this can back a catalog data Table
	// — the parallel + order-sensitive capture path only exists on the catalog
	// table scan.
	Rows      int64 `vgi:"default=200000,ge=0,doc=Total number of rows to generate"`
	ChunkSize int64 `vgi:"default=1000,ge=1,doc=Rows per partition"`
}

// CacheOrderedFunction is a multi-worker, order-sensitive cacheable sequence
// (batch_index tagged).
//
// Parallel capture (>1 substream) of a FIXED_ORDER / supports_batch_index result
// whose correct output is strictly 0,1,…,rows-1. A cache HIT must replay in
// batch_index order (exercising the C++ CachedReplayConnection's stable sort),
// so tests assert row ORDER — not merely the row set. The FIXED_ORDER
// MaxThreads=1 clamp is dropped for supports_batch_index functions, so capture
// still fans out across workers.
type CacheOrderedFunction struct{}

var _ vgi.TypedTableFunc[cacheOrderedState] = (*CacheOrderedFunction)(nil)

func (f *CacheOrderedFunction) Name() string { return "cache_ordered" }

func (f *CacheOrderedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Multi-worker order-sensitive cacheable sequence (batch_index); order-preservation cache fixture",
		Categories:         []string{"generator", "cache", "testing"},
		Tags:               map[string]string{"category": "cache", "type": "ordered"},
		OrderPreservation:  vgi.OrderPreservationFixedOrder,
		SupportsBatchIndex: true,
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT * FROM cache_ordered(1000)", Description: "Order-preserving cacheable result across workers"},
		},
	}
}

func (f *CacheOrderedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cacheOrderedArgs{})
}

func (f *CacheOrderedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(cacheSingleInt64Schema)
}

func (f *CacheOrderedFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	var args cacheOrderedArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	if err := pushPartitionChunks(params, args.Rows, args.ChunkSize); err != nil {
		return nil, err
	}
	return &vgi.GlobalInitResponse{}, nil
}

func (f *CacheOrderedFunction) NewState(params *vgi.ProcessParams) (*cacheOrderedState, error) {
	return &cacheOrderedState{}, nil
}

func (f *CacheOrderedFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cacheOrderedState, out *vgirpc.OutputCollector) error {
	return emitOrderedBatch(params, state, out, cacheOrderedBatchSize)
}

func NewCacheOrderedFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cacheOrderedState](&CacheOrderedFunction{})
}

// ---------------------------------------------------------------------------
// cache_types — nested / wide / NULL columns through the spill + disk blob
// ---------------------------------------------------------------------------

// cacheTypesSchema is STRUCT / LIST / DECIMAL / TIMESTAMP / string with
// interleaved NULLs. Every other cacheable fixture emits flat int64/string, so
// the disk blob + streaming TOC (seek-past-payload) path would otherwise only be
// exercised on fixed-width int64.
var cacheTypesSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "tags", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
	{Name: "attrs", Type: arrow.StructOf(
		arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "y", Type: arrow.BinaryTypes.String, Nullable: true},
	), Nullable: true},
	{Name: "amt", Type: &arrow.Decimal128Type{Precision: 18, Scale: 2}, Nullable: true},
	{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
	{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

const cacheTypesBatchSize = 2048

type cacheTypesArgs struct {
	Rows int64 `vgi:"pos=0,ge=0,doc=Total number of rows to generate"`
}

// CacheTypesFunction emits a nested/wide/NULL cacheable result, exercising the
// disk blob on rich types.
//
// Row i is deterministic: id=i; every 5th row is NULL in the nullable columns
// (tags/attrs/amt/ts/label) so validity bitmaps must round-trip. Purpose-built
// for the spill+streaming byte-identity test.
type CacheTypesFunction struct{}

var _ vgi.TypedTableFunc[cacheCountdownState] = (*CacheTypesFunction)(nil)

func (f *CacheTypesFunction) Name() string { return "cache_types" }

func (f *CacheTypesFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Nested/wide/NULL cacheable result (STRUCT/LIST/DECIMAL/TIMESTAMP + NULLs)",
		Categories:  []string{"generator", "cache", "testing"},
		Tags:        map[string]string{"category": "cache", "type": "types"},
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT count(*) FROM cache_types(10000)", Description: "Nested/NULL cacheable result"},
		},
	}
}

func (f *CacheTypesFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cacheTypesArgs{})
}

func (f *CacheTypesFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(cacheTypesSchema)
}

func (f *CacheTypesFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{}, nil
}

func (f *CacheTypesFunction) NewState(params *vgi.ProcessParams) (*cacheCountdownState, error) {
	var args cacheTypesArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &cacheCountdownState{Remaining: args.Rows}, nil
}

// Process emits one batch per tick; every 5th row is NULL in every nullable column.
func (f *CacheTypesFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cacheCountdownState, out *vgirpc.OutputCollector) error {
	if state.Remaining <= 0 {
		return out.Finish()
	}
	size := min(state.Remaining, cacheTypesBatchSize)
	start := state.CurrentIndex
	mem := memory.NewGoAllocator()

	idB := array.NewInt64Builder(mem)
	defer idB.Release()
	tagsB := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int64)
	defer tagsB.Release()
	tagsValB := tagsB.ValueBuilder().(*array.Int64Builder)
	attrsType := cacheTypesSchema.Field(2).Type.(*arrow.StructType)
	attrsB := array.NewStructBuilder(mem, attrsType)
	defer attrsB.Release()
	attrsXB := attrsB.FieldBuilder(0).(*array.Int64Builder)
	attrsYB := attrsB.FieldBuilder(1).(*array.StringBuilder)
	amtB := array.NewDecimal128Builder(mem, cacheTypesSchema.Field(3).Type.(*arrow.Decimal128Type))
	defer amtB.Release()
	tsB := array.NewTimestampBuilder(mem, cacheTypesSchema.Field(4).Type.(*arrow.TimestampType))
	defer tsB.Release()
	labelB := array.NewStringBuilder(mem)
	defer labelB.Release()

	for j := start; j < start+size; j++ {
		idB.Append(j)
		if j%5 == 0 {
			tagsB.AppendNull()
			attrsB.AppendNull()
			amtB.AppendNull()
			tsB.AppendNull()
			labelB.AppendNull()
			continue
		}
		tagsB.Append(true)
		tagsValB.AppendValues([]int64{j, j + 1, j + 2}, nil)
		attrsB.Append(true)
		attrsXB.Append(j)
		attrsYB.Append(fmt.Sprintf("y%d", j))
		// Decimal(18, 2) of "<j>.<j%100 zero-padded>" — i.e. unscaled j*100 + j%100.
		amtB.Append(decimal128.FromI64(j*100 + j%100))
		tsB.Append(arrow.Timestamp(j))
		labelB.Append(fmt.Sprintf("label-%d", j))
	}

	cols := []arrow.Array{idB.NewArray(), tagsB.NewArray(), attrsB.NewArray(), amtB.NewArray(), tsB.NewArray(), labelB.NewArray()}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()
	batch := array.NewRecordBatch(params.OutputSchema, cols, size)
	if err := vgi.Emit(out, batch, vgi.WithCacheControl(cacheTTL(state.CurrentIndex == 0, cacheDefaultTTL))); err != nil {
		return err
	}
	state.CurrentIndex += size
	state.Remaining -= size
	return nil
}

func NewCacheTypesFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cacheCountdownState](&CacheTypesFunction{})
}

// ---------------------------------------------------------------------------
// cache_filtered — cacheable + STATIC filter pushdown (filter_bytes in the key)
// ---------------------------------------------------------------------------

type cacheFilteredArgs struct {
	// Named-with-default so it can back a catalog data Table: filter pushdown is
	// wired on the catalog table-scan path.
	Rows int64 `vgi:"default=100,ge=0,doc=Total number of rows to generate"`
}

// CacheFilteredFunction is a cacheable sequence with static filter pushdown.
//
// The cache key includes filter_bytes, but no other cacheable fixture pushes
// filters — so the "a pushed WHERE n>=5 must never cross-serve a pushed WHERE
// n>=7" boundary (the filter analog of the tested projection cross-serve) would
// otherwise be uncovered. filter_pushdown + auto_apply_filters means the
// framework applies the pushed predicate to the emitted rows, so distinct static
// filters return distinct rows AND key on distinct filter_bytes.
type CacheFilteredFunction struct{}

var _ vgi.TypedTableFunc[cacheCountdownState] = (*CacheFilteredFunction)(nil)

func (f *CacheFilteredFunction) Name() string { return "cache_filtered" }

func (f *CacheFilteredFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:      "Cacheable sequence with static filter pushdown (filter_bytes keying)",
		Categories:       []string{"generator", "cache", "testing"},
		Tags:             map[string]string{"category": "cache", "type": "filtered"},
		FilterPushdown:   true,
		AutoApplyFilters: true,
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT count(*) FROM cache_filtered(100) WHERE n >= 50", Description: "Cacheable filtered result; WHERE keys the entry"},
		},
	}
}

func (f *CacheFilteredFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cacheFilteredArgs{})
}

func (f *CacheFilteredFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(cacheSingleInt64Schema)
}

func (f *CacheFilteredFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CacheFilteredFunction) NewState(params *vgi.ProcessParams) (*cacheCountdownState, error) {
	var args cacheFilteredArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &cacheCountdownState{Remaining: args.Rows}, nil
}

// Process emits one batch per tick; the framework auto-applies the pushed filter.
func (f *CacheFilteredFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cacheCountdownState, out *vgirpc.OutputCollector) error {
	if state.Remaining <= 0 {
		return out.Finish()
	}
	return state.emitRange(params, out, 2048, cacheTTL(state.CurrentIndex == 0, cacheDefaultTTL))
}

func NewCacheFilteredFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cacheCountdownState](&CacheFilteredFunction{})
}

// ---------------------------------------------------------------------------
// cache_partitioned — partition_values (min/max hints) through the spill blob
// ---------------------------------------------------------------------------

// cachePartitionedCountries is the fixed partition list; one batch per country.
var cachePartitionedCountries = []string{"AU", "BR", "CA", "FR", "US"}

var cachePartitionedCountryField = vgi.PartitionField("country", arrow.BinaryTypes.String, false)

type cachePartitionedArgs struct {
	RowsPerCountry int64 `vgi:"pos=0,ge=1,doc=Rows per country partition"`
}

// cachePartitionedState is a cursor over the fixed country list.
type cachePartitionedState struct {
	CountryIdx     int64
	Advertised     bool
	RowsPerCountry int64
}

// CachePartitionedFunction emits a cacheable single-value-partitioned result
// (country + sales), with partition_values on every batch.
//
// No other cacheable fixture emits partition_values, so the non-empty pv_bytes
// framing in the disk blob (AppendBatch writes pv_len+pv; LoadFromDiskStreaming
// reads pv_len then SEEKS past it) would be untested. Forced to spill and served
// back, any misframed pv_len would misalign the streaming TOC seek and the GROUP
// BY would return wrong rows.
type CachePartitionedFunction struct{}

var _ vgi.TypedTableFunc[cachePartitionedState] = (*CachePartitionedFunction)(nil)

func (f *CachePartitionedFunction) Name() string { return "cache_partitioned" }

func (f *CachePartitionedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:   "Cacheable single-value-partitioned result (partition_values through the spill blob)",
		Categories:    []string{"generator", "cache", "testing", "partitioning"},
		Tags:          map[string]string{"category": "cache", "type": "partitioned"},
		PartitionKind: vgi.PartitionKindSingleValuePartitions,
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT country, SUM(sales) FROM cache_partitioned(100) GROUP BY country", Description: "Partitioned cacheable aggregate over country"},
		},
	}
}

func (f *CachePartitionedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cachePartitionedArgs{})
}

func (f *CachePartitionedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		cachePartitionedCountryField,
		{Name: "sales", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil))
}

func (f *CachePartitionedFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CachePartitionedFunction) NewState(params *vgi.ProcessParams) (*cachePartitionedState, error) {
	var args cachePartitionedArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &cachePartitionedState{RowsPerCountry: args.RowsPerCountry}, nil
}

// Process emits one single-country batch per tick; pv is auto-extracted from the
// country column.
func (f *CachePartitionedFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cachePartitionedState, out *vgirpc.OutputCollector) error {
	if state.CountryIdx >= int64(len(cachePartitionedCountries)) {
		return out.Finish()
	}
	rows := state.RowsPerCountry
	country := cachePartitionedCountries[state.CountryIdx]
	base := state.CountryIdx * 1_000_000
	countryArr := buildStringConst(country, rows)
	defer countryArr.Release()
	salesArr := vgi.BuildInt64Array(rows, func(i int64) int64 { return base + i })
	defer salesArr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{countryArr, salesArr}, rows)

	if err := vgi.EmitPartitioned(out, batch, []arrow.Field{cachePartitionedCountryField},
		vgi.PartitionKindSingleValuePartitions, nil,
		vgi.WithCacheControl(cacheTTL(!state.Advertised, cacheDefaultTTL))); err != nil {
		return err
	}
	state.Advertised = true
	state.CountryIdx++
	return nil
}

func NewCachePartitionedFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cachePartitionedState](&CachePartitionedFunction{})
}
