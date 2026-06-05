// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Scan functions backing the rff_* Tables exercised by the
// required_field_filter_paths_*.test sqllogictest matrix. They mirror
// vgi-python's vgi/_test_fixtures/table/required_filters.py.
//
// The C++ VGI optimizer extension enforces Table.required_field_filter_paths
// at bind/optimize time — rejecting any scan that lacks the declared WHERE
// filters before a single worker byte is read. Once a scan passes that check,
// DuckDB applies the actual filter itself, so these functions simply emit the
// full static dataset; they declare no filter pushdown.

// ---------------------------------------------------------------------------
// rff_simple_scan — flat columns (a, b).
// ---------------------------------------------------------------------------

var RffSimpleSchema = arrow.NewSchema([]arrow.Field{
	{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	{Name: "b", Type: arrow.PrimitiveTypes.Int64},
}, nil)

type RffSimpleScanFunction struct{}

var _ vgi.TypedTableFunc[staticDone] = (*RffSimpleScanFunction)(nil)

func (f *RffSimpleScanFunction) Name() string { return "rff_simple_scan" }

func (f *RffSimpleScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "rff_simple — flat columns (a, b) for required_field_filter_paths tests",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"generator", "testing"},
	}
}

func (f *RffSimpleScanFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

func (f *RffSimpleScanFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(RffSimpleSchema)
}

func (f *RffSimpleScanFunction) NewState(params *vgi.ProcessParams) (*staticDone, error) {
	return &staticDone{}, nil
}

func (f *RffSimpleScanFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *staticDone, out *vgirpc.OutputCollector) error {
	if state.Done {
		out.Finish()
		return nil
	}
	state.Done = true

	a := vgi.BuildInt64Array(3, func(i int64) int64 { return []int64{1, 2, 3}[i] })
	b := vgi.BuildInt64Array(3, func(i int64) int64 { return []int64{10, 20, 30}[i] })
	out.Emit(array.NewRecordBatch(RffSimpleSchema, []arrow.Array{a, b}, 3))
	return nil
}

func NewRffSimpleScanFunction() vgi.TableFunction {
	return vgi.AsTableFunction[staticDone](&RffSimpleScanFunction{})
}

// ---------------------------------------------------------------------------
// rff_none_scan — control table (no requirements). Same shape as rff_simple.
// ---------------------------------------------------------------------------

var RffNoneSchema = arrow.NewSchema([]arrow.Field{
	{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	{Name: "b", Type: arrow.PrimitiveTypes.Int64},
}, nil)

type RffNoneScanFunction struct{}

var _ vgi.TypedTableFunc[staticDone] = (*RffNoneScanFunction)(nil)

func (f *RffNoneScanFunction) Name() string { return "rff_none_scan" }

func (f *RffNoneScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "rff_none — control table with no required_field_filter_paths",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"generator", "testing"},
	}
}

func (f *RffNoneScanFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

func (f *RffNoneScanFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(RffNoneSchema)
}

func (f *RffNoneScanFunction) NewState(params *vgi.ProcessParams) (*staticDone, error) {
	return &staticDone{}, nil
}

func (f *RffNoneScanFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *staticDone, out *vgirpc.OutputCollector) error {
	if state.Done {
		out.Finish()
		return nil
	}
	state.Done = true

	a := vgi.BuildInt64Array(3, func(i int64) int64 { return []int64{1, 2, 3}[i] })
	b := vgi.BuildInt64Array(3, func(i int64) int64 { return []int64{10, 20, 30}[i] })
	out.Emit(array.NewRecordBatch(RffNoneSchema, []arrow.Array{a, b}, 3))
	return nil
}

func NewRffNoneScanFunction() vgi.TableFunction {
	return vgi.AsTableFunction[staticDone](&RffNoneScanFunction{})
}

// ---------------------------------------------------------------------------
// rff_struct_scan — STRUCT(s.a, s.b) + other.
// ---------------------------------------------------------------------------

var rffStructStructType = arrow.StructOf(
	arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int64},
)

var RffStructSchema = arrow.NewSchema([]arrow.Field{
	{Name: "s", Type: rffStructStructType},
	{Name: "other", Type: arrow.PrimitiveTypes.Int64},
}, nil)

type RffStructScanFunction struct{}

var _ vgi.TypedTableFunc[staticDone] = (*RffStructScanFunction)(nil)

func (f *RffStructScanFunction) Name() string { return "rff_struct_scan" }

func (f *RffStructScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "rff_struct — STRUCT(s.a, s.b) + other for required_field_filter_paths tests",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"generator", "testing"},
	}
}

func (f *RffStructScanFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

func (f *RffStructScanFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(RffStructSchema)
}

func (f *RffStructScanFunction) NewState(params *vgi.ProcessParams) (*staticDone, error) {
	return &staticDone{}, nil
}

func (f *RffStructScanFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *staticDone, out *vgirpc.OutputCollector) error {
	if state.Done {
		out.Finish()
		return nil
	}
	state.Done = true

	mem := memory.NewGoAllocator()
	as := []int64{1, 2, 3}
	bs := []int64{10, 20, 30}

	sb := array.NewStructBuilder(mem, rffStructStructType)
	aB := sb.FieldBuilder(0).(*array.Int64Builder)
	bB := sb.FieldBuilder(1).(*array.Int64Builder)
	for i := 0; i < 3; i++ {
		sb.Append(true)
		aB.Append(as[i])
		bB.Append(bs[i])
	}
	sArr := sb.NewArray()
	sb.Release()

	other := vgi.BuildInt64Array(3, func(i int64) int64 { return []int64{100, 200, 300}[i] })
	out.Emit(array.NewRecordBatch(RffStructSchema, []arrow.Array{sArr, other}, 3))
	return nil
}

func NewRffStructScanFunction() vgi.TableFunction {
	return vgi.AsTableFunction[staticDone](&RffStructScanFunction{})
}

// ---------------------------------------------------------------------------
// rff_nested_scan — nested STRUCT(wrapper.mid.leaf).
// ---------------------------------------------------------------------------

var rffNestedMidType = arrow.StructOf(
	arrow.Field{Name: "leaf", Type: arrow.PrimitiveTypes.Int64},
)

var rffNestedWrapperType = arrow.StructOf(
	arrow.Field{Name: "mid", Type: rffNestedMidType},
)

var RffNestedSchema = arrow.NewSchema([]arrow.Field{
	{Name: "wrapper", Type: rffNestedWrapperType},
}, nil)

type RffNestedScanFunction struct{}

var _ vgi.TypedTableFunc[staticDone] = (*RffNestedScanFunction)(nil)

func (f *RffNestedScanFunction) Name() string { return "rff_nested_scan" }

func (f *RffNestedScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "rff_nested — nested STRUCT(wrapper.mid.leaf) for required_field_filter_paths tests",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"generator", "testing"},
	}
}

func (f *RffNestedScanFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

func (f *RffNestedScanFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(RffNestedSchema)
}

func (f *RffNestedScanFunction) NewState(params *vgi.ProcessParams) (*staticDone, error) {
	return &staticDone{}, nil
}

func (f *RffNestedScanFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *staticDone, out *vgirpc.OutputCollector) error {
	if state.Done {
		out.Finish()
		return nil
	}
	state.Done = true

	mem := memory.NewGoAllocator()
	leaves := []int64{1, 2, 3}

	wb := array.NewStructBuilder(mem, rffNestedWrapperType)
	midB := wb.FieldBuilder(0).(*array.StructBuilder)
	leafB := midB.FieldBuilder(0).(*array.Int64Builder)
	for i := 0; i < 3; i++ {
		wb.Append(true)
		midB.Append(true)
		leafB.Append(leaves[i])
	}
	wArr := wb.NewArray()
	wb.Release()

	out.Emit(array.NewRecordBatch(RffNestedSchema, []arrow.Array{wArr}, 3))
	return nil
}

func NewRffNestedScanFunction() vgi.TableFunction {
	return vgi.AsTableFunction[staticDone](&RffNestedScanFunction{})
}

// ---------------------------------------------------------------------------
// rff_multi_scan — top-level + struct subfield required paths.
// ---------------------------------------------------------------------------

var rffMultiStructType = arrow.StructOf(
	arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int64},
)

var RffMultiSchema = arrow.NewSchema([]arrow.Field{
	{Name: "s", Type: rffMultiStructType},
	{Name: "top", Type: arrow.PrimitiveTypes.Int64},
}, nil)

type RffMultiScanFunction struct{}

var _ vgi.TypedTableFunc[staticDone] = (*RffMultiScanFunction)(nil)

func (f *RffMultiScanFunction) Name() string { return "rff_multi_scan" }

func (f *RffMultiScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "rff_multi — top-level + struct subfield required paths",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"generator", "testing"},
	}
}

func (f *RffMultiScanFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

func (f *RffMultiScanFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(RffMultiSchema)
}

func (f *RffMultiScanFunction) NewState(params *vgi.ProcessParams) (*staticDone, error) {
	return &staticDone{}, nil
}

func (f *RffMultiScanFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *staticDone, out *vgirpc.OutputCollector) error {
	if state.Done {
		out.Finish()
		return nil
	}
	state.Done = true

	mem := memory.NewGoAllocator()
	as := []int64{1, 2}
	bs := []int64{10, 20}

	sb := array.NewStructBuilder(mem, rffMultiStructType)
	aB := sb.FieldBuilder(0).(*array.Int64Builder)
	bB := sb.FieldBuilder(1).(*array.Int64Builder)
	for i := 0; i < 2; i++ {
		sb.Append(true)
		aB.Append(as[i])
		bB.Append(bs[i])
	}
	sArr := sb.NewArray()
	sb.Release()

	top := vgi.BuildInt64Array(2, func(i int64) int64 { return []int64{100, 200}[i] })
	out.Emit(array.NewRecordBatch(RffMultiSchema, []arrow.Array{sArr, top}, 2))
	return nil
}

func NewRffMultiScanFunction() vgi.TableFunction {
	return vgi.AsTableFunction[staticDone](&RffMultiScanFunction{})
}

// ---------------------------------------------------------------------------
// rff_rowid_scan — row_id virtual column + bbox.* required filters.
//
// A `WHERE rowid = N` predicate pushes a table_filter keyed by the
// COLUMN_IDENTIFIER_ROW_ID sentinel (>> column count), which the C++
// optimizer's required-filter check must skip rather than index out of bounds.
// Because of the virtual rowid column this fixture needs projection_pushdown:
// under projection the emitted batch must match the *projected* output schema,
// so it builds only the requested columns. See
// required_field_filter_paths_rowid.test.
// ---------------------------------------------------------------------------

// rffRowidMetadata marks the row_id virtual column (hidden from SELECT *).
var rffRowidMetadata = arrow.NewMetadata([]string{"is_row_id"}, []string{""})

var rffRowidBboxType = arrow.StructOf(
	arrow.Field{Name: "xmin", Type: arrow.PrimitiveTypes.Float32},
	arrow.Field{Name: "ymin", Type: arrow.PrimitiveTypes.Float32},
	arrow.Field{Name: "xmax", Type: arrow.PrimitiveTypes.Float32},
	arrow.Field{Name: "ymax", Type: arrow.PrimitiveTypes.Float32},
)

var RffRowidSchema = arrow.NewSchema([]arrow.Field{
	{Name: "row_id", Type: arrow.PrimitiveTypes.Int64, Metadata: rffRowidMetadata},
	{Name: "bbox", Type: rffRowidBboxType},
	{Name: "other", Type: arrow.PrimitiveTypes.Int64},
}, nil)

type RffRowidScanFunction struct{}

var _ vgi.TypedTableFunc[staticDone] = (*RffRowidScanFunction)(nil)

func (f *RffRowidScanFunction) Name() string { return "rff_rowid_scan" }

func (f *RffRowidScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "rff_rowid — row_id virtual column + bbox.* required filters",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
		Categories:         []string{"generator", "testing"},
	}
}

func (f *RffRowidScanFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

func (f *RffRowidScanFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(RffRowidSchema)
}

func (f *RffRowidScanFunction) NewState(params *vgi.ProcessParams) (*staticDone, error) {
	return &staticDone{}, nil
}

func (f *RffRowidScanFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *staticDone, out *vgirpc.OutputCollector) error {
	if state.Done {
		out.Finish()
		return nil
	}
	state.Done = true

	const n = int64(10)
	mem := memory.NewGoAllocator()
	cols := make([]arrow.Array, 0, params.OutputSchema.NumFields())
	for _, field := range params.OutputSchema.Fields() {
		switch field.Name {
		case "row_id":
			cols = append(cols, vgi.BuildInt64Array(n, func(i int64) int64 { return i }))
		case "other":
			cols = append(cols, vgi.BuildInt64Array(n, func(i int64) int64 { return i * 10 }))
		case "bbox":
			bb := array.NewStructBuilder(mem, rffRowidBboxType)
			xmin := bb.FieldBuilder(0).(*array.Float32Builder)
			ymin := bb.FieldBuilder(1).(*array.Float32Builder)
			xmax := bb.FieldBuilder(2).(*array.Float32Builder)
			ymax := bb.FieldBuilder(3).(*array.Float32Builder)
			for i := int64(0); i < n; i++ {
				bb.Append(true)
				xmin.Append(float32(i))
				ymin.Append(2.0)
				xmax.Append(3.0)
				ymax.Append(4.0)
			}
			cols = append(cols, bb.NewArray())
			bb.Release()
		default:
			return fmt.Errorf("rff_rowid: unexpected projected column %q", field.Name)
		}
	}
	out.Emit(array.NewRecordBatch(params.OutputSchema, cols, n))
	return nil
}

func NewRffRowidScanFunction() vgi.TableFunction {
	return vgi.AsTableFunction[staticDone](&RffRowidScanFunction{})
}
