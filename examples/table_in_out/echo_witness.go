// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table_in_out

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// EchoWitnessFunction emits len(observed output schema) in every column, to
// prove projection pushdown actually narrows the schema reaching the worker.
// Mirrors vgi-python's EchoWitnessFunction.
type EchoWitnessFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*EchoWitnessFunction)(nil)

func (f *EchoWitnessFunction) Name() string { return "echo_witness" }

func (f *EchoWitnessFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Emits len(observed_output_schema) per column — projection probe",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
		Categories:         []string{"test", "pushdown"},
	}
}

func (f *EchoWitnessFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"}}
}

func (f *EchoWitnessFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindInputSchema(params)
}

func (f *EchoWitnessFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

func (f *EchoWitnessFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	observed := int64(params.OutputSchema.NumFields())
	n := int(batch.NumRows())
	mem := memory.NewGoAllocator()
	cols := make([]arrow.Array, params.OutputSchema.NumFields())
	for i, field := range params.OutputSchema.Fields() {
		arr, err := constIntArray(mem, field.Type, n, observed)
		if err != nil {
			return err
		}
		cols[i] = arr
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()
	return out.Emit(array.NewRecordBatch(params.OutputSchema, cols, int64(n)))
}

func (f *EchoWitnessFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// constIntArray builds an integer array of length n with every value = v.
func constIntArray(mem memory.Allocator, typ arrow.DataType, n int, v int64) (arrow.Array, error) {
	switch typ.ID() {
	case arrow.INT64:
		b := array.NewInt64Builder(mem)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(v)
		}
		return b.NewArray(), nil
	case arrow.INT32:
		b := array.NewInt32Builder(mem)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(int32(v))
		}
		return b.NewArray(), nil
	case arrow.INT16:
		b := array.NewInt16Builder(mem)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(int16(v))
		}
		return b.NewArray(), nil
	default:
		return nil, fmt.Errorf("echo_witness: column type %s is not an integer", typ)
	}
}

// NewEchoWitnessFunction wraps EchoWitnessFunction for registration.
func NewEchoWitnessFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&EchoWitnessFunction{})
}
