// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table_in_out

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// RepeatInputsFunction duplicates each input batch N times.
type RepeatInputsFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*RepeatInputsFunction)(nil)

func (f *RepeatInputsFunction) Name() string { return "repeat_inputs" }

func (f *RepeatInputsFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Duplicates each input batch N times",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *RepeatInputsFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "repeat_count", Position: 0, ArrowType: "int64", Doc: "Number of times to repeat each input batch", IsConst: true},
		{Name: "data", Position: 1, ArrowType: "table", Doc: "Input table to repeat"},
	}
}

func (f *RepeatInputsFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindInputSchema(params)
}

func (f *RepeatInputsFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

func (f *RepeatInputsFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	repeatCount, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return err
	}

	if repeatCount <= 1 {
		return out.Emit(batch)
	}

	// Concatenate batch repeatCount times
	mem := memory.NewGoAllocator()
	numCols := int(batch.NumCols())
	rowsPerBatch := batch.NumRows()
	totalRows := rowsPerBatch * repeatCount

	cols := make([]arrow.Array, numCols)
	for c := 0; c < numCols; c++ {
		srcCol := batch.Column(c)
		b := array.NewBuilder(mem, srcCol.DataType())
		for r := int64(0); r < repeatCount; r++ {
			for i := 0; i < int(rowsPerBatch); i++ {
				if srcCol.IsNull(i) {
					b.AppendNull()
				} else {
					appendValue(b, srcCol, i)
				}
			}
		}
		cols[c] = b.NewArray()
		b.Release()
	}

	result := array.NewRecordBatch(params.OutputSchema, cols, totalRows)
	for _, c := range cols {
		c.Release()
	}

	return out.Emit(result)
}

func (f *RepeatInputsFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewRepeatInputsFunction creates a RepeatInputsFunction wrapped for registration.
func NewRepeatInputsFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&RepeatInputsFunction{})
}

// appendValue appends a single value from src at index i to builder.
func appendValue(b array.Builder, src arrow.Array, i int) {
	switch c := src.(type) {
	case *array.Int64:
		b.(*array.Int64Builder).Append(c.Value(i))
	case *array.Int32:
		b.(*array.Int32Builder).Append(c.Value(i))
	case *array.Int16:
		b.(*array.Int16Builder).Append(c.Value(i))
	case *array.Int8:
		b.(*array.Int8Builder).Append(c.Value(i))
	case *array.Uint64:
		b.(*array.Uint64Builder).Append(c.Value(i))
	case *array.Uint32:
		b.(*array.Uint32Builder).Append(c.Value(i))
	case *array.Float64:
		b.(*array.Float64Builder).Append(c.Value(i))
	case *array.Float32:
		b.(*array.Float32Builder).Append(c.Value(i))
	case *array.String:
		b.(*array.StringBuilder).Append(c.Value(i))
	case *array.Boolean:
		b.(*array.BooleanBuilder).Append(c.Value(i))
	case *array.Binary:
		b.(*array.BinaryBuilder).Append(c.Value(i))
	default:
		b.AppendNull()
	}
}
