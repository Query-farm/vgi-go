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

// ConstantColumnsFunction generates a table with constant values from varargs.
type ConstantColumnsFunction struct{}

var _ vgi.TypedTableFunc[constantColumnsState] = (*ConstantColumnsFunction)(nil)

func (f *ConstantColumnsFunction) Name() string { return "constant_columns" }

func (f *ConstantColumnsFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generates rows with constant values from varargs",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *ConstantColumnsFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of rows to generate", IsConst: true},
		{Name: "values", Position: 1, ArrowType: "any", Doc: "Values to fill each column", IsVarargs: true, IsConst: true},
	}
}

func (f *ConstantColumnsFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	numValues := params.Args.NumArgs() - 1
	fields := make([]arrow.Field, numValues)
	for i := 0; i < numValues; i++ {
		col := params.Args.Positional[i+1] // skip count
		fields[i] = arrow.Field{
			Name: fmt.Sprintf("col_%d", i),
			Type: col.DataType(),
		}
	}
	return vgi.BindSchema(arrow.NewSchema(fields, nil))
}

func (f *ConstantColumnsFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type constantColumnsState struct {
	vgi.BatchState
}

const constantColumnsBatchSize = 2048

func (f *ConstantColumnsFunction) NewState(params *vgi.ProcessParams) (*constantColumnsState, error) {
	count, _ := params.Args.GetScalarInt64(0)
	return &constantColumnsState{
		BatchState: vgi.NewBatchState(count, constantColumnsBatchSize),
	}, nil
}

func (f *ConstantColumnsFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *constantColumnsState, out *vgirpc.OutputCollector) error {
	if state.Remaining <= 0 {
		return out.Finish()
	}

	size := state.BatchSize
	if state.Remaining < size {
		size = state.Remaining
	}

	mem := memory.NewGoAllocator()
	numCols := params.OutputSchema.NumFields()
	cols := make([]arrow.Array, numCols)

	for c := 0; c < numCols; c++ {
		valueCol := params.Args.Positional[c+1] // skip count
		cols[c] = repeatScalar(mem, valueCol, int(size))
	}

	batch := array.NewRecordBatch(params.OutputSchema, cols, size)
	for _, c := range cols {
		c.Release()
	}

	state.Remaining -= size
	state.Index += size
	return out.Emit(batch)
}

func NewConstantColumnsFunction() vgi.TableFunction {
	return vgi.AsTableFunction[constantColumnsState](&ConstantColumnsFunction{})
}

// repeatScalar creates an array by repeating the scalar value at index 0 of src.
func repeatScalar(mem memory.Allocator, src arrow.Array, n int) arrow.Array {
	single := array.NewSlice(src, 0, 1)
	defer single.Release()

	arrs := make([]arrow.Array, n)
	for i := range arrs {
		arrs[i] = single
	}

	result, err := array.Concatenate(arrs, mem)
	if err != nil {
		b := array.NewBuilder(mem, src.DataType())
		defer b.Release()
		for i := 0; i < n; i++ {
			b.AppendNull()
		}
		return b.NewArray()
	}
	return result
}
