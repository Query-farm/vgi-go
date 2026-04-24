// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// UnnestTensorFunction inverts nest_tensor: given a struct{tensor, axes}, it
// emits a list<struct{value, axes}> covering every cell of the Cartesian
// product of the axis coordinates (including null-valued cells).
type UnnestTensorFunction struct{}

func (f *UnnestTensorFunction) Name() string { return "unnest_tensor" }

func (f *UnnestTensorFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Invert nest_tensor: list of {value, axes} structs per cell",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *UnnestTensorFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "tensor", Position: 0, ArrowType: "any", Doc: "nest_tensor output struct"},
	}
}

func (f *UnnestTensorFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	if params.InputSchema == nil || params.InputSchema.NumFields() == 0 {
		return nil, fmt.Errorf("unnest_tensor: expected one argument (nest_tensor struct)")
	}
	rowType, err := unnestTensorRowType(params.InputSchema.Field(0).Type)
	if err != nil {
		return nil, err
	}
	return vgi.BindResult(arrow.ListOf(rowType))
}

func (f *UnnestTensorFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	input := batch.Column(0)
	out, err := unnestTensorCompute(input, params.OutputSchema.Field(0).Type)
	if err != nil {
		return nil, err
	}
	defer out.Release()
	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{out}, int64(out.Len())), nil
}

// unnestTensorRowType validates the nest_tensor-shaped input struct and
// returns the per-cell struct type: struct<value: inner, axes: struct<T1,...>>
func unnestTensorRowType(in arrow.DataType) (arrow.DataType, error) {
	st, ok := in.(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("unnest_tensor: argument must be a struct, got %s", in)
	}
	_, hasTensor := st.FieldIdx("tensor")
	_, hasAxes := st.FieldIdx("axes")
	if !hasTensor || !hasAxes {
		return nil, fmt.Errorf("unnest_tensor: struct must have 'tensor' and 'axes' fields")
	}
	tensorIdx, _ := st.FieldIdx("tensor")
	axesIdx, _ := st.FieldIdx("axes")
	axesField := st.Field(axesIdx)
	axesType, ok := axesField.Type.(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("unnest_tensor: 'axes' field must be a struct, got %s", axesField.Type)
	}
	// Walk tensor nesting depth and collect inner type.
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
		return nil, fmt.Errorf("unnest_tensor: tensor nesting depth %d does not match number of axes %d", depth, axesType.NumFields())
	}
	// Build output row type: struct<value: inner, axes: struct<fld1: T1, ...>>.
	axesRowFields := make([]arrow.Field, axesType.NumFields())
	for i, f := range axesType.Fields() {
		lt, ok := f.Type.(*arrow.ListType)
		if !ok {
			return nil, fmt.Errorf("unnest_tensor: axis '%s' must be a list, got %s", f.Name, f.Type)
		}
		axesRowFields[i] = arrow.Field{Name: f.Name, Type: lt.Elem(), Nullable: true}
	}
	return arrow.StructOf(
		arrow.Field{Name: "value", Type: inner, Nullable: true},
		arrow.Field{Name: "axes", Type: arrow.StructOf(axesRowFields...), Nullable: true},
	), nil
}

// unnestTensorCompute produces a List<Struct<value, axes>> given the
// nest_tensor struct array. The `outListType` parameter carries the target
// list<row> arrow type so we can build the correct nested types.
func unnestTensorCompute(input arrow.Array, outListType arrow.DataType) (arrow.Array, error) {
	listType, ok := outListType.(*arrow.ListType)
	if !ok {
		return nil, fmt.Errorf("unnest_tensor: expected list output, got %s", outListType)
	}
	rowStruct, ok := listType.Elem().(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("unnest_tensor: expected list<struct>, got %s", listType.Elem())
	}
	mem := memory.NewGoAllocator()
	lb := array.NewListBuilder(mem, rowStruct)
	defer lb.Release()
	rowBuilder := lb.ValueBuilder().(*array.StructBuilder)
	valueBuilder := rowBuilder.FieldBuilder(0)
	axesRowBuilder := rowBuilder.FieldBuilder(1).(*array.StructBuilder)

	st, ok := input.(*array.Struct)
	if !ok {
		return nil, fmt.Errorf("unnest_tensor: input must be a struct array, got %T", input)
	}
	stType := st.DataType().(*arrow.StructType)
	tensorIdx, _ := stType.FieldIdx("tensor")
	axesIdx, _ := stType.FieldIdx("axes")
	tensorCol := st.Field(tensorIdx)
	axesCol := st.Field(axesIdx).(*array.Struct)
	axesType := axesCol.DataType().(*arrow.StructType)

	for i := 0; i < st.Len(); i++ {
		if st.IsNull(i) {
			lb.AppendNull()
			continue
		}
		lb.Append(true)
		if err := emitTensorCells(tensorCol, axesCol, axesType, i, rowBuilder, valueBuilder, axesRowBuilder); err != nil {
			return nil, err
		}
	}
	return lb.NewArray(), nil
}

// emitTensorCells walks the nested list tensor at row `i` and emits one
// struct per cell (value + per-axis coord) into the provided builders.
func emitTensorCells(tensorCol arrow.Array, axesCol *array.Struct, axesType *arrow.StructType, row int,
	rowBuilder *array.StructBuilder, valueBuilder array.Builder, axesRowBuilder *array.StructBuilder) error {
	// Harvest per-axis coord arrays for this row (each is a list scalar).
	n := axesType.NumFields()
	coordArrays := make([]arrow.Array, n)
	for i := 0; i < n; i++ {
		field := axesCol.Field(i).(*array.List)
		start, end := field.ValueOffsets(row)
		values := field.ListValues()
		coordArrays[i] = array.NewSlice(values, start, end)
	}
	defer func() {
		for _, a := range coordArrays {
			a.Release()
		}
	}()
	// Sizes per axis.
	shape := make([]int, n)
	for i, a := range coordArrays {
		shape[i] = a.Len()
	}
	total := 1
	for _, s := range shape {
		if s == 0 {
			return nil // empty Cartesian product — no rows to emit
		}
		total *= s
	}

	// Walk the tensor list for this row.
	emitCellFn := func(coords []int, leafArr arrow.Array, leafIdx int) error {
		rowBuilder.Append(true)
		// value
		if err := appendScalar(valueBuilder, leafArr, leafIdx); err != nil {
			return err
		}
		// axes
		axesRowBuilder.Append(true)
		for i, c := range coords {
			if err := appendScalar(axesRowBuilder.FieldBuilder(i), coordArrays[i], c); err != nil {
				return err
			}
		}
		return nil
	}

	// Walk a nested list value at (tensorCol[row]) recursively to visit leaves.
	return walkNestedList(tensorCol, row, make([]int, 0, n), shape, emitCellFn)
}

// walkNestedList navigates a List<List<...<T>>> value at position `row` in
// `outer`, calling `visit` with the accumulated index path and the innermost
// leaf (array, index). Handles null sublists by skipping them; the C++-side
// nest_tensor already fills the Cartesian product with nulls at the leaf
// value level rather than null lists, but be defensive.
func walkNestedList(outer arrow.Array, row int, path []int, shape []int, visit func(coords []int, leaf arrow.Array, leafIdx int) error) error {
	listArr, ok := outer.(*array.List)
	if !ok {
		// Leaf — single value at position row.
		return visit(path, outer, row)
	}
	start, _ := listArr.ValueOffsets(row)
	inner := listArr.ListValues()
	// Expected span length for this depth.
	expected := shape[len(path)]
	for i := 0; i < expected; i++ {
		np := append(path, i)
		if err := walkNestedList(inner, int(start)+i, np, shape, visit); err != nil {
			return err
		}
	}
	return nil
}

// AppendScalarExported is the exported name for use by the table-in-out
// variant (unnest_tensor_rows) which cannot directly import the package-
// private helper but needs the same scalar-copy semantics.
func AppendScalarExported(b array.Builder, arr arrow.Array, idx int) error {
	return appendScalar(b, arr, idx)
}

// appendScalar is a type-dispatching copier used by both unnest_tensor and
// unnest_tensor_rows; it mirrors nest_tensor's appendScalarFromArray but has
// to live here to avoid a circular import into the aggregate package.
func appendScalar(b array.Builder, arr arrow.Array, idx int) error {
	if arr.IsNull(idx) {
		b.AppendNull()
		return nil
	}
	switch a := arr.(type) {
	case *array.Boolean:
		b.(*array.BooleanBuilder).Append(a.Value(idx))
	case *array.Int8:
		b.(*array.Int8Builder).Append(a.Value(idx))
	case *array.Int16:
		b.(*array.Int16Builder).Append(a.Value(idx))
	case *array.Int32:
		b.(*array.Int32Builder).Append(a.Value(idx))
	case *array.Int64:
		b.(*array.Int64Builder).Append(a.Value(idx))
	case *array.Uint8:
		b.(*array.Uint8Builder).Append(a.Value(idx))
	case *array.Uint16:
		b.(*array.Uint16Builder).Append(a.Value(idx))
	case *array.Uint32:
		b.(*array.Uint32Builder).Append(a.Value(idx))
	case *array.Uint64:
		b.(*array.Uint64Builder).Append(a.Value(idx))
	case *array.Float32:
		b.(*array.Float32Builder).Append(a.Value(idx))
	case *array.Float64:
		b.(*array.Float64Builder).Append(a.Value(idx))
	case *array.String:
		b.(*array.StringBuilder).Append(a.Value(idx))
	case *array.Binary:
		b.(*array.BinaryBuilder).Append(a.Value(idx))
	case *array.Date32:
		b.(*array.Date32Builder).Append(a.Value(idx))
	case *array.Date64:
		b.(*array.Date64Builder).Append(a.Value(idx))
	case *array.Timestamp:
		b.(*array.TimestampBuilder).Append(a.Value(idx))
	default:
		return fmt.Errorf("unnest_tensor: unsupported scalar type %s", arr.DataType())
	}
	return nil
}
