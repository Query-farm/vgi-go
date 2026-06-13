// Copyright 2025, 2026 Query Farm LLC - https://query.farm

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

var (
	pcCountries    = []string{"AU", "BR", "CA", "FR", "US"}
	pcRegionsYears = []struct {
		region string
		year   int64
	}{{"AMER", 2023}, {"AMER", 2024}, {"EMEA", 2023}, {"EMEA", 2024}, {"APAC", 2023}, {"APAC", 2024}}
	pcCategories = []string{"books", "music", "video"}
)

// pushIndexQueue enqueues n single-int64 work items (partition indices).
func pushIndexQueue(params *vgi.InitParams, n int) error {
	var items [][]byte
	for i := 0; i < n; i++ {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(i))
		items = append(items, b)
	}
	if params.Storage != nil {
		return params.Storage.QueuePush(items)
	}
	return nil
}

// popIndex pops the next partition index, or (-1, false) when the queue is drained.
func popIndex(params *vgi.ProcessParams) (int64, bool, error) {
	if params.Storage == nil {
		return 0, false, nil
	}
	work, err := params.Storage.QueuePop()
	if err != nil {
		return 0, false, err
	}
	if work == nil {
		return 0, false, nil
	}
	return int64(binary.BigEndian.Uint64(work)), true, nil
}

func argInt0(params *vgi.ProcessParams) int64 {
	v, _ := params.Args.GetScalarInt64(0)
	return v
}

type pcArgs struct {
	Count int64 `vgi:"pos=0,doc=Rows per partition"`
}

type pcState struct{}

// ---------------------------------------------------------------------------
// country_partitioned_sales — SINGLE_VALUE, one column.
// ---------------------------------------------------------------------------

var pcCountryField = vgi.PartitionField("country", arrow.BinaryTypes.String, false)

type CountryPartitionedSalesFunction struct{}

var _ vgi.TypedTableFunc[pcState] = (*CountryPartitionedSalesFunction)(nil)

func (f *CountryPartitionedSalesFunction) Name() string { return "country_partitioned_sales" }
func (f *CountryPartitionedSalesFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "Per-country sales rows, one batch per country (SINGLE_VALUE partition).", Categories: []string{"generator", "partitioning"}, PartitionKind: vgi.PartitionKindSingleValuePartitions}
}
func (f *CountryPartitionedSalesFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(pcArgs{})
}
func (f *CountryPartitionedSalesFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{pcCountryField, {Name: "sales", Type: arrow.PrimitiveTypes.Int64}}, nil))
}
func (f *CountryPartitionedSalesFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	if err := pushIndexQueue(params, len(pcCountries)); err != nil {
		return nil, err
	}
	return &vgi.GlobalInitResponse{MaxWorkers: 4}, nil
}
func (f *CountryPartitionedSalesFunction) NewState(params *vgi.ProcessParams) (*pcState, error) {
	return &pcState{}, nil
}
func (f *CountryPartitionedSalesFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *pcState, out *vgirpc.OutputCollector) error {
	idx, ok, err := popIndex(params)
	if err != nil || !ok {
		return finishOr(out, err)
	}
	rows := argInt0(params)
	base := idx * 1_000_000
	countryArr := buildStringConst(pcCountries[idx], rows)
	defer countryArr.Release()
	salesArr := vgi.BuildInt64Array(rows, func(i int64) int64 { return base + i })
	defer salesArr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{countryArr, salesArr}, rows)
	return vgi.EmitPartitioned(out, batch, []arrow.Field{pcCountryField}, vgi.PartitionKindSingleValuePartitions, nil)
}
func NewCountryPartitionedSalesFunction() vgi.TableFunction {
	return vgi.AsTableFunction[pcState](&CountryPartitionedSalesFunction{})
}

// ---------------------------------------------------------------------------
// region_year_partitioned — SINGLE_VALUE, two columns.
// ---------------------------------------------------------------------------

var (
	pcRegionField = vgi.PartitionField("region", arrow.BinaryTypes.String, false)
	pcYearField   = vgi.PartitionField("year", arrow.PrimitiveTypes.Int64, false)
)

type RegionYearPartitionedFunction struct{}

var _ vgi.TypedTableFunc[pcState] = (*RegionYearPartitionedFunction)(nil)

func (f *RegionYearPartitionedFunction) Name() string { return "region_year_partitioned" }
func (f *RegionYearPartitionedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "Per-(region, year) rows; both columns SINGLE_VALUE partitions.", Categories: []string{"generator", "partitioning"}, PartitionKind: vgi.PartitionKindSingleValuePartitions}
}
func (f *RegionYearPartitionedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(pcArgs{})
}
func (f *RegionYearPartitionedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{pcRegionField, pcYearField, {Name: "value", Type: arrow.PrimitiveTypes.Float64}}, nil))
}
func (f *RegionYearPartitionedFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	if err := pushIndexQueue(params, len(pcRegionsYears)); err != nil {
		return nil, err
	}
	return &vgi.GlobalInitResponse{MaxWorkers: 4}, nil
}
func (f *RegionYearPartitionedFunction) NewState(params *vgi.ProcessParams) (*pcState, error) {
	return &pcState{}, nil
}
func (f *RegionYearPartitionedFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *pcState, out *vgirpc.OutputCollector) error {
	idx, ok, err := popIndex(params)
	if err != nil || !ok {
		return finishOr(out, err)
	}
	ry := pcRegionsYears[idx]
	rows := argInt0(params)
	base := float64(idx * 1000)
	regionArr := buildStringConst(ry.region, rows)
	defer regionArr.Release()
	yearArr := vgi.BuildInt64Array(rows, func(i int64) int64 { return ry.year })
	defer yearArr.Release()
	valArr := buildFloat64(rows, func(i int64) float64 { return base + float64(i) })
	defer valArr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{regionArr, yearArr, valArr}, rows)
	return vgi.EmitPartitioned(out, batch, []arrow.Field{pcRegionField, pcYearField}, vgi.PartitionKindSingleValuePartitions, nil)
}
func NewRegionYearPartitionedFunction() vgi.TableFunction {
	return vgi.AsTableFunction[pcState](&RegionYearPartitionedFunction{})
}

// ---------------------------------------------------------------------------
// partitioned_with_explicit_override — SINGLE_VALUE, explicit partition_values.
// ---------------------------------------------------------------------------

var pcCategoryField = vgi.PartitionField("category", arrow.BinaryTypes.String, false)

type PartitionedWithExplicitOverrideFunction struct{}

var _ vgi.TypedTableFunc[pcState] = (*PartitionedWithExplicitOverrideFunction)(nil)

func (f *PartitionedWithExplicitOverrideFunction) Name() string {
	return "partitioned_with_explicit_override"
}
func (f *PartitionedWithExplicitOverrideFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "SINGLE_VALUE partition using the explicit partition_values override.", Categories: []string{"generator", "partitioning"}, PartitionKind: vgi.PartitionKindSingleValuePartitions}
}
func (f *PartitionedWithExplicitOverrideFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(pcArgs{})
}
func (f *PartitionedWithExplicitOverrideFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{pcCategoryField, {Name: "revenue", Type: arrow.PrimitiveTypes.Int64}}, nil))
}
func (f *PartitionedWithExplicitOverrideFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	if err := pushIndexQueue(params, len(pcCategories)); err != nil {
		return nil, err
	}
	return &vgi.GlobalInitResponse{MaxWorkers: 4}, nil
}
func (f *PartitionedWithExplicitOverrideFunction) NewState(params *vgi.ProcessParams) (*pcState, error) {
	return &pcState{}, nil
}
func (f *PartitionedWithExplicitOverrideFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *pcState, out *vgirpc.OutputCollector) error {
	idx, ok, err := popIndex(params)
	if err != nil || !ok {
		return finishOr(out, err)
	}
	category := pcCategories[idx]
	rows := argInt0(params)
	catArr := buildStringConst(category, rows)
	defer catArr.Release()
	revArr := vgi.BuildInt64Array(rows, func(i int64) int64 { return (idx+1)*100 + i })
	defer revArr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{catArr, revArr}, rows)
	return vgi.EmitPartitioned(out, batch, []arrow.Field{pcCategoryField}, vgi.PartitionKindSingleValuePartitions,
		map[string]vgi.PartitionValue{"category": {Min: category, Max: category}})
}
func NewPartitionedWithExplicitOverrideFunction() vgi.TableFunction {
	return vgi.AsTableFunction[pcState](&PartitionedWithExplicitOverrideFunction{})
}

// ---------------------------------------------------------------------------
// disjoint_range_partitioned — DISJOINT, one column.
// ---------------------------------------------------------------------------

var pcKeyField = vgi.PartitionField("key", arrow.PrimitiveTypes.Int64, false)

type disjointArgs struct {
	Partitions       int64 `vgi:"pos=0,doc=Number of disjoint partitions"`
	RowsPerPartition int64 `vgi:"default=10,doc=Rows per partition"`
}

type DisjointRangePartitionedFunction struct{}

var _ vgi.TypedTableFunc[pcState] = (*DisjointRangePartitionedFunction)(nil)

func (f *DisjointRangePartitionedFunction) Name() string { return "disjoint_range_partitioned" }
func (f *DisjointRangePartitionedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "Disjoint per-chunk integer ranges on key (DISJOINT partition).", Categories: []string{"generator", "partitioning"}, PartitionKind: vgi.PartitionKindDisjointPartitions}
}
func (f *DisjointRangePartitionedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(disjointArgs{})
}
func (f *DisjointRangePartitionedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{pcKeyField, {Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil))
}
func (f *DisjointRangePartitionedFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	var args disjointArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	if err := pushIndexQueue(params, int(args.Partitions)); err != nil {
		return nil, err
	}
	return &vgi.GlobalInitResponse{MaxWorkers: 4}, nil
}
func (f *DisjointRangePartitionedFunction) NewState(params *vgi.ProcessParams) (*pcState, error) {
	return &pcState{}, nil
}
func (f *DisjointRangePartitionedFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *pcState, out *vgirpc.OutputCollector) error {
	idx, ok, err := popIndex(params)
	if err != nil || !ok {
		return finishOr(out, err)
	}
	var args disjointArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return err
	}
	rpp := args.RowsPerPartition
	if rpp <= 0 {
		rpp = 10
	}
	base := idx * 1000
	keyArr := vgi.BuildInt64Array(rpp, func(i int64) int64 { return base + i })
	defer keyArr.Release()
	valArr := vgi.BuildInt64Array(rpp, func(i int64) int64 { return idx*10 + i })
	defer valArr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{keyArr, valArr}, rpp)
	return vgi.EmitPartitioned(out, batch, []arrow.Field{pcKeyField}, vgi.PartitionKindDisjointPartitions, nil)
}
func NewDisjointRangePartitionedFunction() vgi.TableFunction {
	return vgi.AsTableFunction[pcState](&DisjointRangePartitionedFunction{})
}

// ---------------------------------------------------------------------------
// Deliberately-broken partition_columns fixtures (partition_columns_contract.test).
// ---------------------------------------------------------------------------

type brokenPCState struct{ Emitted bool }

// BrokenMissingPartitionValuesFunction declares a partition column but emits
// with no partition metadata — the C++ extension raises.
type BrokenMissingPartitionValuesFunction struct{}

var _ vgi.TypedTableFunc[brokenPCState] = (*BrokenMissingPartitionValuesFunction)(nil)

func (f *BrokenMissingPartitionValuesFunction) Name() string {
	return "broken_missing_partition_values"
}
func (f *BrokenMissingPartitionValuesFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "BROKEN: partition column declared but no vgi_partition_values emitted", Categories: []string{"testing", "broken"}, PartitionKind: vgi.PartitionKindSingleValuePartitions}
}
func (f *BrokenMissingPartitionValuesFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(pcArgs{})
}
func (f *BrokenMissingPartitionValuesFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{pcCountryField, {Name: "sales", Type: arrow.PrimitiveTypes.Int64}}, nil))
}
func (f *BrokenMissingPartitionValuesFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}
func (f *BrokenMissingPartitionValuesFunction) NewState(params *vgi.ProcessParams) (*brokenPCState, error) {
	return &brokenPCState{}, nil
}
func (f *BrokenMissingPartitionValuesFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *brokenPCState, out *vgirpc.OutputCollector) error {
	if state.Emitted {
		return out.Finish()
	}
	state.Emitted = true
	rows := argInt0(params)
	catArr := buildStringConst("US", rows)
	defer catArr.Release()
	salesArr := vgi.BuildInt64Array(rows, func(i int64) int64 { return i })
	defer salesArr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{catArr, salesArr}, rows)
	return out.Emit(batch) // no partition metadata — C++ raises
}
func NewBrokenMissingPartitionValuesFunction() vgi.TableFunction {
	return vgi.AsTableFunction[brokenPCState](&BrokenMissingPartitionValuesFunction{})
}

// BrokenPartitionMinNeqMaxFunction supplies an explicit (min != max) override
// for a SINGLE_VALUE partition — the C++ extension's release-build check raises.
type BrokenPartitionMinNeqMaxFunction struct{}

var _ vgi.TypedTableFunc[brokenPCState] = (*BrokenPartitionMinNeqMaxFunction)(nil)

func (f *BrokenPartitionMinNeqMaxFunction) Name() string { return "broken_partition_min_neq_max" }
func (f *BrokenPartitionMinNeqMaxFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "BROKEN: SINGLE_VALUE partition with min != max", Categories: []string{"testing", "broken"}, PartitionKind: vgi.PartitionKindSingleValuePartitions}
}
func (f *BrokenPartitionMinNeqMaxFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(pcArgs{})
}
func (f *BrokenPartitionMinNeqMaxFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{pcCountryField, {Name: "sales", Type: arrow.PrimitiveTypes.Int64}}, nil))
}
func (f *BrokenPartitionMinNeqMaxFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}
func (f *BrokenPartitionMinNeqMaxFunction) NewState(params *vgi.ProcessParams) (*brokenPCState, error) {
	return &brokenPCState{}, nil
}
func (f *BrokenPartitionMinNeqMaxFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *brokenPCState, out *vgirpc.OutputCollector) error {
	if state.Emitted {
		return out.Finish()
	}
	state.Emitted = true
	rows := argInt0(params)
	catArr := buildStringConst("US", rows)
	defer catArr.Release()
	salesArr := vgi.BuildInt64Array(rows, func(i int64) int64 { return i })
	defer salesArr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{catArr, salesArr}, rows)
	return vgi.EmitPartitioned(out, batch, []arrow.Field{pcCountryField}, vgi.PartitionKindSingleValuePartitions,
		map[string]vgi.PartitionValue{"country": {Min: "US", Max: "BR"}})
}
func NewBrokenPartitionMinNeqMaxFunction() vgi.TableFunction {
	return vgi.AsTableFunction[brokenPCState](&BrokenPartitionMinNeqMaxFunction{})
}

// BrokenPartitionValuesNoAnnotationFunction passes partition_values with no
// declared partition field — the worker-side validator raises.
type BrokenPartitionValuesNoAnnotationFunction struct{}

var _ vgi.TypedTableFunc[brokenPCState] = (*BrokenPartitionValuesNoAnnotationFunction)(nil)

func (f *BrokenPartitionValuesNoAnnotationFunction) Name() string {
	return "broken_partition_values_no_annotation"
}
func (f *BrokenPartitionValuesNoAnnotationFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "BROKEN: partition_values passed without annotated fields", Categories: []string{"testing", "broken"}}
}
func (f *BrokenPartitionValuesNoAnnotationFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(pcArgs{})
}
func (f *BrokenPartitionValuesNoAnnotationFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{{Name: "country", Type: arrow.BinaryTypes.String}, {Name: "sales", Type: arrow.PrimitiveTypes.Int64}}, nil))
}
func (f *BrokenPartitionValuesNoAnnotationFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}
func (f *BrokenPartitionValuesNoAnnotationFunction) NewState(params *vgi.ProcessParams) (*brokenPCState, error) {
	return &brokenPCState{}, nil
}
func (f *BrokenPartitionValuesNoAnnotationFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *brokenPCState, out *vgirpc.OutputCollector) error {
	if state.Emitted {
		return out.Finish()
	}
	state.Emitted = true
	rows := argInt0(params)
	catArr := buildStringConst("US", rows)
	defer catArr.Release()
	salesArr := vgi.BuildInt64Array(rows, func(i int64) int64 { return i })
	defer salesArr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{catArr, salesArr}, rows)
	// No declared partition fields, but supply partition_values anyway.
	return vgi.EmitPartitioned(out, batch, nil, vgi.PartitionKindNotPartitioned,
		map[string]vgi.PartitionValue{"country": {Min: "US", Max: "US"}})
}
func NewBrokenPartitionValuesNoAnnotationFunction() vgi.TableFunction {
	return vgi.AsTableFunction[brokenPCState](&BrokenPartitionValuesNoAnnotationFunction{})
}

// BrokenPartitionColumnAbsentFromBatchFunction declares a partition column but
// emits a batch missing it (with no explicit override) — the worker raises.
type BrokenPartitionColumnAbsentFromBatchFunction struct{}

var _ vgi.TypedTableFunc[brokenPCState] = (*BrokenPartitionColumnAbsentFromBatchFunction)(nil)

func (f *BrokenPartitionColumnAbsentFromBatchFunction) Name() string {
	return "broken_partition_column_absent_from_batch"
}
func (f *BrokenPartitionColumnAbsentFromBatchFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "BROKEN: partition column declared but absent from emitted batch", Categories: []string{"testing", "broken"}, PartitionKind: vgi.PartitionKindSingleValuePartitions}
}
func (f *BrokenPartitionColumnAbsentFromBatchFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(pcArgs{})
}
func (f *BrokenPartitionColumnAbsentFromBatchFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	// The output schema declares category as a partition column, but Process
	// emits a batch that omits it.
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{pcCategoryField, {Name: "revenue", Type: arrow.PrimitiveTypes.Int64}}, nil))
}
func (f *BrokenPartitionColumnAbsentFromBatchFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}
func (f *BrokenPartitionColumnAbsentFromBatchFunction) NewState(params *vgi.ProcessParams) (*brokenPCState, error) {
	return &brokenPCState{}, nil
}
func (f *BrokenPartitionColumnAbsentFromBatchFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *brokenPCState, out *vgirpc.OutputCollector) error {
	if state.Emitted {
		return out.Finish()
	}
	state.Emitted = true
	rows := argInt0(params)
	revArr := vgi.BuildInt64Array(rows, func(i int64) int64 { return i })
	defer revArr.Release()
	// Emit a batch that omits the declared "category" partition column.
	revenueOnly := arrow.NewSchema([]arrow.Field{{Name: "revenue", Type: arrow.PrimitiveTypes.Int64}}, nil)
	batch := array.NewRecordBatch(revenueOnly, []arrow.Array{revArr}, rows)
	return vgi.EmitPartitioned(out, batch, []arrow.Field{pcCategoryField}, vgi.PartitionKindSingleValuePartitions, nil)
}
func NewBrokenPartitionColumnAbsentFromBatchFunction() vgi.TableFunction {
	return vgi.AsTableFunction[brokenPCState](&BrokenPartitionColumnAbsentFromBatchFunction{})
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func finishOr(out *vgirpc.OutputCollector, err error) error {
	if err != nil {
		return err
	}
	return out.Finish()
}

func buildStringConst(v string, n int64) arrow.Array {
	mem := memory.NewGoAllocator()
	b := array.NewStringBuilder(mem)
	defer b.Release()
	for i := int64(0); i < n; i++ {
		b.Append(v)
	}
	return b.NewArray()
}

func buildFloat64(n int64, fn func(i int64) float64) arrow.Array {
	mem := memory.NewGoAllocator()
	b := array.NewFloat64Builder(mem)
	defer b.Release()
	for i := int64(0); i < n; i++ {
		b.Append(fn(i))
	}
	return b.NewArray()
}
