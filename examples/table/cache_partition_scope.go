// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Per-PARTITION result-cache fixtures (vgi.cache.partition_scope).
//
// A SINGLE_VALUE_PARTITIONS function that advertises partition_scope has its
// result ADDITIONALLY stored split by partition value (one entry per distinct
// partition tuple), so a later =/IN-filtered scan on the partition column(s)
// serves the requested partitions from cache without calling the worker. The
// whole-scan entry is still stored, so the opt-in is purely additive.
//
// See cache.go / cache_advanced.go for the whole-scan cacheable fixtures; these
// four back ../vgi/test/sql/integration/cache/partition_scope*.test.

package table

import (
	"context"
	"encoding/binary"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// partitionScopeCacheControl is what every per-partition fixture advertises.
//
// Unlike the whole-scan fixtures (which advertise on the first batch only) this
// goes on EVERY batch: the extension latches the first cache-control it sees,
// and on a fall-through scan the leading partition can be filtered to zero rows,
// which would drop a first-batch-only advertisement.
func partitionScopeCacheControl() *vgi.CacheControl {
	return &vgi.CacheControl{Ttl: vgi.Seconds(cacheDefaultTTL), PartitionScope: true}
}

// buildNullableStringConst builds an n-row string array of one repeated value,
// or of nulls when v is nil.
func buildNullableStringConst(v *string, n int64) arrow.Array {
	b := array.NewStringBuilder(memory.NewGoAllocator())
	defer b.Release()
	for i := int64(0); i < n; i++ {
		if v == nil {
			b.AppendNull()
		} else {
			b.Append(*v)
		}
	}
	return b.NewArray()
}

// ---------------------------------------------------------------------------
// cache_partition_scope — the baseline per-partition opt-in
// ---------------------------------------------------------------------------

// cachePartitionScopeCountries is the fixed partition list; one batch per country.
var cachePartitionScopeCountries = []string{"AU", "BR", "CA", "FR", "US"}

type cachePartitionScopeArgs struct {
	RowsPerCountry int64 `vgi:"pos=0,ge=1,doc=Rows per country partition"`
}

// cachePartitionCursorState is a cursor over a fixed partition list.
type cachePartitionCursorState struct {
	Idx  int64
	Rows int64
}

// CachePartitionScopeFunction emits a per-partition cacheable single-value
// partitioned result (country + sales).
//
// filter_pushdown + auto_apply_filters means a WHERE country=... predicate
// reaches the worker as a real filter (so the client can enumerate the requested
// set) and the framework prunes emitted batches to it — required for row
// correctness on a fall-through scan, because DuckDB does NOT re-apply a pushed
// predicate above the scan.
type CachePartitionScopeFunction struct{}

var _ vgi.TypedTableFunc[cachePartitionCursorState] = (*CachePartitionScopeFunction)(nil)

func (f *CachePartitionScopeFunction) Name() string { return "cache_partition_scope" }

func (f *CachePartitionScopeFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:      "Per-partition cacheable single-value-partitioned result (vgi.cache.partition_scope)",
		Categories:       []string{"generator", "cache", "testing", "partitioning"},
		Tags:             map[string]string{"category": "cache", "type": "partitioned"},
		PartitionKind:    vgi.PartitionKindSingleValuePartitions,
		FilterPushdown:   true,
		AutoApplyFilters: true,
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT * FROM cache_partition_scope(10) WHERE country = 'US'", Description: "Per-partition cache serve for one country"},
		},
	}
}

func (f *CachePartitionScopeFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cachePartitionScopeArgs{})
}

func (f *CachePartitionScopeFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		cachePartitionedCountryField,
		{Name: "sales", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil))
}

func (f *CachePartitionScopeFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CachePartitionScopeFunction) NewState(params *vgi.ProcessParams) (*cachePartitionCursorState, error) {
	var args cachePartitionScopeArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &cachePartitionCursorState{Rows: args.RowsPerCountry}, nil
}

// Process emits one single-country batch per tick; pv is auto-extracted from the
// country column.
func (f *CachePartitionScopeFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cachePartitionCursorState, out *vgirpc.OutputCollector) error {
	if state.Idx >= int64(len(cachePartitionScopeCountries)) {
		return out.Finish()
	}
	rows := state.Rows
	country := cachePartitionScopeCountries[state.Idx]
	base := state.Idx * 1_000_000
	countryArr := buildStringConst(country, rows)
	defer countryArr.Release()
	salesArr := vgi.BuildInt64Array(rows, func(i int64) int64 { return base + i })
	defer salesArr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{countryArr, salesArr}, rows)

	if err := vgi.EmitPartitioned(out, batch, []arrow.Field{cachePartitionedCountryField},
		vgi.PartitionKindSingleValuePartitions, nil,
		vgi.WithCacheControl(partitionScopeCacheControl())); err != nil {
		return err
	}
	state.Idx++
	return nil
}

func NewCachePartitionScopeFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cachePartitionCursorState](&CachePartitionScopeFunction{})
}

// ---------------------------------------------------------------------------
// cache_partition_parallel — work-queue fan-out (PARALLEL capture) + a NULL partition
// ---------------------------------------------------------------------------

// cachePartitionParallelCountries includes a nil entry: SINGLE_VALUE permits a
// NULL partition, and `IS NULL` is deliberately NOT enumerable (only =/IN), so
// the fixture also pins the correct non-serve behaviour.
var cachePartitionParallelCountries = []*string{
	strPtr("AU"), strPtr("CA"), strPtr("US"), nil,
}

func strPtr(s string) *string { return &s }

type cachePartitionParallelArgs struct {
	RowsPerCountry int64 `vgi:"pos=0,ge=1,doc=Rows per country partition"`
}

// cachePartitionParallelState is a per-worker cursor over the work queue.
type cachePartitionParallelState struct {
	Rows     int64
	Idx      int64
	Emitted  int64
	HasChunk bool
}

// CachePartitionParallelFunction is the multi-worker partner of
// cache_partition_scope: partitions are handed out through the shared work queue,
// so a `threads=N` + `pool false` scan fans them across N workers and the
// per-partition split at commit must bucket batches drawn from MULTIPLE capture
// substreams.
type CachePartitionParallelFunction struct{}

var _ vgi.TypedTableFunc[cachePartitionParallelState] = (*CachePartitionParallelFunction)(nil)

func (f *CachePartitionParallelFunction) Name() string { return "cache_partition_parallel" }

func (f *CachePartitionParallelFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:      "Per-partition cacheable; work-queue fan-out (parallel capture); one NULL partition",
		Categories:       []string{"generator", "cache", "testing", "partitioning"},
		Tags:             map[string]string{"category": "cache", "type": "partitioned"},
		PartitionKind:    vgi.PartitionKindSingleValuePartitions,
		FilterPushdown:   true,
		AutoApplyFilters: true,
	}
}

func (f *CachePartitionParallelFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cachePartitionParallelArgs{})
}

func (f *CachePartitionParallelFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		cachePartitionedCountryField,
		{Name: "sales", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil))
}

// OnInit has the primary worker enqueue one item per partition. MaxWorkers is
// left at the framework default so the scan actually fans out.
func (f *CachePartitionParallelFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	items := make([][]byte, 0, len(cachePartitionParallelCountries))
	for i := range cachePartitionParallelCountries {
		item := make([]byte, 8)
		binary.BigEndian.PutUint64(item, uint64(i))
		items = append(items, item)
	}
	if params.Storage != nil {
		if err := params.Storage.QueuePush(items); err != nil {
			return nil, err
		}
	}
	return &vgi.GlobalInitResponse{}, nil
}

func (f *CachePartitionParallelFunction) NewState(params *vgi.ProcessParams) (*cachePartitionParallelState, error) {
	var args cachePartitionParallelArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &cachePartitionParallelState{Rows: args.RowsPerCountry}, nil
}

// Process pops one country from the queue and emits its single-valued batch.
// The partition value is supplied explicitly so the NULL partition is
// unambiguous — auto-extraction from an all-NULL column cannot distinguish
// "no rows" from "the NULL partition".
func (f *CachePartitionParallelFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cachePartitionParallelState, out *vgirpc.OutputCollector) error {
	if !state.HasChunk || state.Emitted >= state.Rows {
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
		state.Idx = int64(binary.BigEndian.Uint64(work))
		state.Emitted = 0
		state.HasChunk = true
	}

	rows := state.Rows
	country := cachePartitionParallelCountries[state.Idx]
	base := state.Idx * 1_000_000
	countryArr := buildNullableStringConst(country, rows)
	defer countryArr.Release()
	salesArr := vgi.BuildInt64Array(rows, func(i int64) int64 { return base + i })
	defer salesArr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{countryArr, salesArr}, rows)

	var pv vgi.PartitionValue
	if country != nil {
		pv = vgi.PartitionValue{Min: *country, Max: *country}
	}
	if err := vgi.EmitPartitioned(out, batch, []arrow.Field{cachePartitionedCountryField},
		vgi.PartitionKindSingleValuePartitions,
		map[string]vgi.PartitionValue{"country": pv},
		vgi.WithCacheControl(partitionScopeCacheControl())); err != nil {
		return err
	}
	state.Emitted = rows
	return nil
}

func NewCachePartitionParallelFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cachePartitionParallelState](&CachePartitionParallelFunction{})
}

// ---------------------------------------------------------------------------
// cache_partition_multicol — MULTI-COLUMN (region, year) SINGLE_VALUE partitions
// ---------------------------------------------------------------------------

// cachePartitionRegionField / cachePartitionYearField are the two partition
// columns; the fixture exercises cross-product enumeration (region IN x year IN),
// 2-column tuple canonicalization, and the partial-constraint fall-through
// (region constrained, year free -> not enumerable).
var (
	cachePartitionRegionField = vgi.PartitionField("region", arrow.BinaryTypes.String, false)
	cachePartitionYearField   = vgi.PartitionField("year", arrow.PrimitiveTypes.Int64, false)
)

// cachePartitionRegionYear is the fixed (region, year) partition list. The years
// are NON-contiguous on purpose: DuckDB rewrites `year IN (2020, 2021)`
// (contiguous ints) into a BETWEEN range, which is not enumerable, so a gap keeps
// the pushed filter an IN filter and the cross-product path is actually taken.
var cachePartitionRegionYear = []struct {
	Region string
	Year   int64
}{
	{"EU", 2020}, {"EU", 2022}, {"US", 2020}, {"US", 2022},
}

type cachePartitionMultiColArgs struct {
	RowsPerPartition int64 `vgi:"pos=0,ge=1,doc=Rows per (region, year) partition"`
}

// CachePartitionMultiColFunction is per-partition cacheable over TWO
// SINGLE_VALUE partition columns.
type CachePartitionMultiColFunction struct{}

var _ vgi.TypedTableFunc[cachePartitionCursorState] = (*CachePartitionMultiColFunction)(nil)

func (f *CachePartitionMultiColFunction) Name() string { return "cache_partition_multicol" }

func (f *CachePartitionMultiColFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:      "Per-partition cacheable over (region, year) SINGLE_VALUE partition columns",
		Categories:       []string{"generator", "cache", "testing", "partitioning"},
		Tags:             map[string]string{"category": "cache", "type": "partitioned"},
		PartitionKind:    vgi.PartitionKindSingleValuePartitions,
		FilterPushdown:   true,
		AutoApplyFilters: true,
	}
}

func (f *CachePartitionMultiColFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cachePartitionMultiColArgs{})
}

func (f *CachePartitionMultiColFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		cachePartitionRegionField,
		cachePartitionYearField,
		{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil))
}

func (f *CachePartitionMultiColFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CachePartitionMultiColFunction) NewState(params *vgi.ProcessParams) (*cachePartitionCursorState, error) {
	var args cachePartitionMultiColArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &cachePartitionCursorState{Rows: args.RowsPerPartition}, nil
}

// Process emits one single-valued (region, year) batch per tick; both partition
// values are auto-extracted from the emitted columns.
func (f *CachePartitionMultiColFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cachePartitionCursorState, out *vgirpc.OutputCollector) error {
	if state.Idx >= int64(len(cachePartitionRegionYear)) {
		return out.Finish()
	}
	rows := state.Rows
	ry := cachePartitionRegionYear[state.Idx]
	base := state.Idx * 1000
	regionArr := buildStringConst(ry.Region, rows)
	defer regionArr.Release()
	yearArr := vgi.BuildInt64Array(rows, func(int64) int64 { return ry.Year })
	defer yearArr.Release()
	amountArr := vgi.BuildInt64Array(rows, func(i int64) int64 { return base + i })
	defer amountArr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{regionArr, yearArr, amountArr}, rows)

	if err := vgi.EmitPartitioned(out, batch,
		[]arrow.Field{cachePartitionRegionField, cachePartitionYearField},
		vgi.PartitionKindSingleValuePartitions, nil,
		vgi.WithCacheControl(partitionScopeCacheControl())); err != nil {
		return err
	}
	state.Idx++
	return nil
}

func NewCachePartitionMultiColFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cachePartitionCursorState](&CachePartitionMultiColFunction{})
}

// ---------------------------------------------------------------------------
// cache_partition_proj — projection pushdown + per-partition cache
// ---------------------------------------------------------------------------

// cachePartitionProjCountries is the fixed partition list for the projection
// fixture.
var cachePartitionProjCountries = []string{"CA", "US"}

// cachePartitionProjSchema is the FULL (pre-projection) schema; `extra` is a
// non-partition column to project away while keeping `country` (so a
// WHERE country=X can still push).
var cachePartitionProjSchema = arrow.NewSchema([]arrow.Field{
	cachePartitionedCountryField,
	{Name: "sales", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "extra", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
}, nil)

type cachePartitionProjArgs struct {
	RowsPerCountry int64 `vgi:"pos=0,ge=1,doc=Rows per country partition"`
}

// CachePartitionProjFunction is per-partition cacheable with projection
// pushdown: projection becomes part of the cache key, and the explicit partition
// value keeps the split working even when the partition column itself is
// projected OUT of the emitted batch (the auto-extract-impossible case).
type CachePartitionProjFunction struct{}

var _ vgi.TypedTableFunc[cachePartitionCursorState] = (*CachePartitionProjFunction)(nil)

func (f *CachePartitionProjFunction) Name() string { return "cache_partition_proj" }

func (f *CachePartitionProjFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Per-partition cacheable with projection pushdown + explicit partition_values",
		Categories:         []string{"generator", "cache", "testing", "partitioning"},
		Tags:               map[string]string{"category": "cache", "type": "partitioned"},
		PartitionKind:      vgi.PartitionKindSingleValuePartitions,
		ProjectionPushdown: true,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
	}
}

func (f *CachePartitionProjFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cachePartitionProjArgs{})
}

func (f *CachePartitionProjFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(cachePartitionProjSchema)
}

func (f *CachePartitionProjFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *CachePartitionProjFunction) NewState(params *vgi.ProcessParams) (*cachePartitionCursorState, error) {
	var args cachePartitionProjArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &cachePartitionCursorState{Rows: args.RowsPerCountry}, nil
}

// Process emits only the projected columns (params.OutputSchema reflects the
// pushdown) and always supplies the partition value explicitly.
func (f *CachePartitionProjFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cachePartitionCursorState, out *vgirpc.OutputCollector) error {
	if state.Idx >= int64(len(cachePartitionProjCountries)) {
		return out.Finish()
	}
	rows := state.Rows
	country := cachePartitionProjCountries[state.Idx]
	base := state.Idx * 1_000_000

	projected := vgi.ProjectedColumns(params.ProjectionIDs, cachePartitionProjSchema)
	colMap := make(map[string]arrow.Array, 3)
	if projected.Contains("country") {
		colMap["country"] = buildStringConst(country, rows)
	}
	if projected.Contains("sales") {
		colMap["sales"] = vgi.BuildInt64Array(rows, func(i int64) int64 { return base + i })
	}
	if projected.Contains("extra") {
		colMap["extra"] = vgi.BuildInt64Array(rows, func(i int64) int64 { return base + 500 + i })
	}
	arrs := make([]arrow.Array, 0, params.OutputSchema.NumFields())
	for _, f := range params.OutputSchema.Fields() {
		if a, ok := colMap[f.Name]; ok {
			arrs = append(arrs, a)
		}
	}
	defer func() {
		for _, a := range arrs {
			a.Release()
		}
	}()
	batch := array.NewRecordBatch(params.OutputSchema, arrs, rows)

	if err := vgi.EmitPartitioned(out, batch, []arrow.Field{cachePartitionedCountryField},
		vgi.PartitionKindSingleValuePartitions,
		map[string]vgi.PartitionValue{"country": {Min: country, Max: country}},
		vgi.WithCacheControl(partitionScopeCacheControl())); err != nil {
		return err
	}
	state.Idx++
	return nil
}

func NewCachePartitionProjFunction() vgi.TableFunction {
	return vgi.AsTableFunction[cachePartitionCursorState](&CachePartitionProjFunction{})
}
