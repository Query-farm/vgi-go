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
	// Output schema: one column per vararg, named col_0, col_1, etc.
	// Skip the first argument (count).
	numValues := params.Args.NumArgs() - 1
	fields := make([]arrow.Field, numValues)
	for i := 0; i < numValues; i++ {
		col := params.Args.Positional[i+1] // skip count
		fields[i] = arrow.Field{
			Name: fmt.Sprintf("col_%d", i),
			Type: col.DataType(),
		}
	}
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema(fields, nil),
	}, nil
}

func (f *ConstantColumnsFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}

func (f *ConstantColumnsFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type constantColumnsState struct {
	remaining int64
}

func (f *ConstantColumnsFunction) NewState(params *vgi.ProcessParams) (interface{}, error) {
	count, _ := params.Args.GetScalarInt64(0)
	return &constantColumnsState{remaining: count}, nil
}

const constantColumnsBatchSize = 2048

func (f *ConstantColumnsFunction) Process(ctx context.Context, params *vgi.ProcessParams, state interface{}, out *vgirpc.OutputCollector) error {
	s := state.(*constantColumnsState)
	if s.remaining <= 0 {
		return out.Finish()
	}

	size := int64(constantColumnsBatchSize)
	if s.remaining < size {
		size = s.remaining
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

	s.remaining -= size
	return out.Emit(batch)
}

// repeatScalar creates an array by repeating the scalar value at index 0 of src.
// Uses array.Concatenate for a generic implementation that works for any Arrow type
// (Decimal128, Struct, List, Map, etc.).
func repeatScalar(mem memory.Allocator, src arrow.Array, n int) arrow.Array {
	// Create a single-element slice from the source
	single := array.NewSlice(src, 0, 1)
	defer single.Release()

	// Build an array of N references to the single-element array
	arrs := make([]arrow.Array, n)
	for i := range arrs {
		arrs[i] = single
	}

	result, err := array.Concatenate(arrs, mem)
	if err != nil {
		// Fallback: null array
		b := array.NewBuilder(mem, src.DataType())
		defer b.Release()
		for i := 0; i < n; i++ {
			b.AppendNull()
		}
		return b.NewArray()
	}
	return result
}
