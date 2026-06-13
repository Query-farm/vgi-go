// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table_in_out

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/examples/scalar"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// UnnestTensorRowsFunction is the table-in-out variant of unnest_tensor.
// Accepts a one-column input table whose column is a nest_tensor-shaped
// struct and emits one output row per cell of the Cartesian product for
// every input row. Unlike the scalar unnest_tensor, this streams output
// without materialising a full list column, and composes with DuckDB's
// LATERAL joins on correlated columns.
type UnnestTensorRowsFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*UnnestTensorRowsFunction)(nil)

func (f *UnnestTensorRowsFunction) Name() string { return "unnest_tensor_rows" }

func (f *UnnestTensorRowsFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Invert nest_tensor, streaming one row per cell (LATERAL-friendly)",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"tensor", "utility"},
	}
}

func (f *UnnestTensorRowsFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table: one column of nest_tensor structs"},
	}
}

func (f *UnnestTensorRowsFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	in := params.InputSchema
	if in == nil || in.NumFields() != 1 {
		return nil, fmt.Errorf("unnest_tensor_rows: input table must have exactly one column (the nest_tensor struct)")
	}
	st, ok := in.Field(0).Type.(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("unnest_tensor_rows: input column must be a struct, got %s", in.Field(0).Type)
	}
	_, hasTensor := st.FieldIdx("tensor")
	_, hasAxes := st.FieldIdx("axes")
	if !hasTensor || !hasAxes {
		return nil, fmt.Errorf("unnest_tensor_rows: struct must have 'tensor' and 'axes' fields")
	}
	axesIdx, _ := st.FieldIdx("axes")
	axesType, ok := st.Field(axesIdx).Type.(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("unnest_tensor_rows: 'axes' field must be a struct, got %s", st.Field(axesIdx).Type)
	}
	// Walk tensor nesting depth.
	tensorIdx, _ := st.FieldIdx("tensor")
	tensorType := st.Field(tensorIdx).Type
	depth := 0
	inner := tensorType
	for {
		lt, ok := inner.(*arrow.ListType)
		if !ok {
			break
		}
		depth++
		inner = lt.Elem()
	}
	if depth != axesType.NumFields() {
		return nil, fmt.Errorf("unnest_tensor_rows: tensor nesting depth %d does not match number of axes %d", depth, axesType.NumFields())
	}
	// Build the output schema: (value: inner, axes: struct<T1, ..., TN>)
	axesRowFields := make([]arrow.Field, axesType.NumFields())
	for i, af := range axesType.Fields() {
		lt, ok := af.Type.(*arrow.ListType)
		if !ok {
			return nil, fmt.Errorf("unnest_tensor_rows: axis '%s' must be a list, got %s", af.Name, af.Type)
		}
		axesRowFields[i] = arrow.Field{Name: af.Name, Type: lt.Elem(), Nullable: true}
	}
	out := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: inner, Nullable: true},
		{Name: "axes", Type: arrow.StructOf(axesRowFields...), Nullable: true},
	}, nil)
	return vgi.BindSchema(out)
}

func (f *UnnestTensorRowsFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

func (f *UnnestTensorRowsFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	outSchema := params.OutputSchema
	mem := memory.NewGoAllocator()
	valueType := outSchema.Field(0).Type
	axesOutType := outSchema.Field(1).Type.(*arrow.StructType)
	axisNames := make([]string, axesOutType.NumFields())
	for i, af := range axesOutType.Fields() {
		axisNames[i] = af.Name
	}

	// Always emit exactly one batch per input batch — empty if the input has
	// no cells to expand (honoring the tio exchange contract).
	if batch.NumRows() == 0 {
		fields := make([]arrow.Array, outSchema.NumFields())
		for i, f := range outSchema.Fields() {
			b := array.NewBuilder(mem, f.Type)
			fields[i] = b.NewArray()
			b.Release()
		}
		return out.Emit(array.NewRecordBatch(outSchema, fields, 0))
	}

	valueBuilder := array.NewBuilder(mem, valueType)
	defer valueBuilder.Release()
	axesBuilder := array.NewStructBuilder(mem, axesOutType)
	defer axesBuilder.Release()

	st := batch.Column(0).(*array.Struct)
	stType := st.DataType().(*arrow.StructType)
	tensorIdx, _ := stType.FieldIdx("tensor")
	axesIdx, _ := stType.FieldIdx("axes")
	tensorCol := st.Field(tensorIdx)
	axesCol := st.Field(axesIdx).(*array.Struct)
	axesInType := axesCol.DataType().(*arrow.StructType)

	rowCount := int64(0)
	for i := 0; i < st.Len(); i++ {
		if st.IsNull(i) {
			continue
		}
		// Harvest per-axis coord slices for this input row.
		n := axesInType.NumFields()
		coordArrays := make([]arrow.Array, n)
		for a := 0; a < n; a++ {
			field := axesCol.Field(a).(*array.List)
			start, end := field.ValueOffsets(i)
			coordArrays[a] = array.NewSlice(field.ListValues(), start, end)
		}
		shape := make([]int, n)
		empty := false
		for a := 0; a < n; a++ {
			shape[a] = coordArrays[a].Len()
			if shape[a] == 0 {
				empty = true
			}
		}
		if empty {
			for _, a := range coordArrays {
				a.Release()
			}
			continue
		}

		// Recursively walk tensor for this row, emitting one cell per leaf.
		err := walkTensor(tensorCol, i, nil, shape, func(coords []int, leaf arrow.Array, leafIdx int) error {
			if err := scalar.AppendScalarExported(valueBuilder, leaf, leafIdx); err != nil {
				return err
			}
			axesBuilder.Append(true)
			for a, c := range coords {
				// Find the output field index matching axis name.
				axisName := axesInType.Field(a).Name
				// Map axis name to output field index (order must match, but be safe).
				outIdx := a
				for k, af := range axesOutType.Fields() {
					if af.Name == axisName {
						outIdx = k
						break
					}
				}
				if err := scalar.AppendScalarExported(axesBuilder.FieldBuilder(outIdx), coordArrays[a], c); err != nil {
					return err
				}
			}
			rowCount++
			return nil
		})
		for _, a := range coordArrays {
			a.Release()
		}
		if err != nil {
			return err
		}
	}

	valuesArr := valueBuilder.NewArray()
	defer valuesArr.Release()
	axesArr := axesBuilder.NewArray()
	defer axesArr.Release()
	rb := array.NewRecordBatch(outSchema, []arrow.Array{valuesArr, axesArr}, rowCount)
	return out.Emit(rb)
}

func (f *UnnestTensorRowsFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// walkTensor walks a nested list<list<...<T>>> value at position `row` in
// `outer`, invoking visit at each leaf with the path of indices and the leaf
// array + offset.
func walkTensor(outer arrow.Array, row int, path []int, shape []int, visit func(coords []int, leaf arrow.Array, leafIdx int) error) error {
	listArr, ok := outer.(*array.List)
	if !ok {
		return visit(path, outer, row)
	}
	start, _ := listArr.ValueOffsets(row)
	inner := listArr.ListValues()
	expected := shape[len(path)]
	for i := 0; i < expected; i++ {
		np := append(path, i)
		if err := walkTensor(inner, int(start)+i, np, shape, visit); err != nil {
			return err
		}
	}
	return nil
}

// NewUnnestTensorRowsFunction creates the registration wrapper.
func NewUnnestTensorRowsFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&UnnestTensorRowsFunction{})
}
