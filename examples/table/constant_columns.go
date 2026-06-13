// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
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

// constantColumnsArgs is the typed argument schema for constant_columns().
// Values is declared as []any varargs so the spec advertises the right shape;
// the function reads the raw arrow.Array values from params.Args.Positional
// directly (BindArgs leaves []any varargs slices nil).
type constantColumnsArgs struct {
	Count  int64 `vgi:"pos=0,doc=Number of rows to generate"`
	Values []any `vgi:"pos=1,varargs,doc=Values to fill each column"`
}

func (f *ConstantColumnsFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(constantColumnsArgs{})
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

// constantColumnsAllocator is the shared allocator for repeated-scalar columns.
var constantColumnsAllocator = memory.NewGoAllocator()

func (f *ConstantColumnsFunction) NewState(params *vgi.ProcessParams) (*constantColumnsState, error) {
	var args constantColumnsArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &constantColumnsState{
		BatchState: vgi.NewBatchState(args.Count, constantColumnsBatchSize),
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

	numCols := params.OutputSchema.NumFields()
	cols := make([]arrow.Array, numCols)

	for c := 0; c < numCols; c++ {
		valueCol := params.Args.Positional[c+1] // skip count
		col, err := repeatScalar(constantColumnsAllocator, valueCol, int(size))
		if err != nil {
			for _, made := range cols[:c] {
				made.Release()
			}
			return err
		}
		cols[c] = col
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

// repeatScalar builds an n-row array by repeating the value at index 0 of src.
// It grows the result by repeated doubling — O(log n) concatenations instead of
// concatenating n single-element slices — while still handling every Arrow type
// array.Concatenate supports (including DuckDB extension types such as HUGEINT).
// Errors are propagated so a failure surfaces to DuckDB rather than being masked
// as NULL data.
func repeatScalar(mem memory.Allocator, src arrow.Array, n int) (arrow.Array, error) {
	if n <= 0 {
		return array.NewSlice(src, 0, 0), nil
	}
	acc := array.NewSlice(src, 0, 1) // single element, sharing src's buffers
	for acc.Len() < n {
		add := acc.Len()
		if want := n - acc.Len(); add > want {
			add = want
		}
		tail := array.NewSlice(acc, 0, int64(add))
		next, err := array.Concatenate([]arrow.Array{acc, tail}, mem)
		tail.Release()
		acc.Release()
		if err != nil {
			return nil, err
		}
		acc = next
	}
	return acc, nil
}
