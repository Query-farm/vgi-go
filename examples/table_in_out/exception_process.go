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

// ExceptionProcessFunction raises an exception on the second batch.
type ExceptionProcessFunction struct{}

var _ vgi.TypedTableInOutFunc[exceptionProcessState] = (*ExceptionProcessFunction)(nil)

func (f *ExceptionProcessFunction) Name() string { return "exception_process" }

func (f *ExceptionProcessFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Test function that raises exception during process",
		Stability:   vgi.StabilityConsistent,
		HasFinalize: true,
	}
}

func (f *ExceptionProcessFunction) ArgumentSpecs() []vgi.ArgSpec {
	return sumColumnsArgSpecs
}

func (f *ExceptionProcessFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return sumColumnsOnBind(params)
}

type exceptionProcessState struct {
	BatchCount int
}

func (f *ExceptionProcessFunction) NewState(params *vgi.ProcessParams) (*exceptionProcessState, error) {
	return &exceptionProcessState{BatchCount: 0}, nil
}

func (f *ExceptionProcessFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *exceptionProcessState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	state.BatchCount++
	if state.BatchCount%2 == 0 {
		return fmt.Errorf("Intentional exception on batch %d", state.BatchCount)
	}
	return out.Emit(vgi.EmptyBatch(params.OutputSchema))
}

func (f *ExceptionProcessFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *exceptionProcessState) ([]arrow.RecordBatch, error) {
	// Mirror vgi-python's SumAllColumnsFunction.finalize(): even though
	// process() never accumulated (this fixture deliberately skips the
	// parent's process body), emit a single zero-sum row keyed on the
	// output schema so consumers get a clean (0, 0, ...) result when no
	// exception was triggered.
	mem := memory.NewGoAllocator()
	arrs := make([]arrow.Array, params.OutputSchema.NumFields())
	for i := 0; i < params.OutputSchema.NumFields(); i++ {
		ft := params.OutputSchema.Field(i).Type
		switch ft.ID() {
		case arrow.INT64:
			b := array.NewInt64Builder(mem)
			b.Append(0)
			arrs[i] = b.NewArray()
			b.Release()
		case arrow.FLOAT64:
			b := array.NewFloat64Builder(mem)
			b.Append(0)
			arrs[i] = b.NewArray()
			b.Release()
		default:
			// Skip if neither numeric type — the test only exercises numeric inputs.
			arrs[i] = nil
		}
	}
	for _, a := range arrs {
		if a == nil {
			return nil, nil
		}
	}
	defer func() {
		for _, a := range arrs {
			a.Release()
		}
	}()
	batch := array.NewRecordBatch(params.OutputSchema, arrs, 1)
	return []arrow.RecordBatch{batch}, nil
}

// NewExceptionProcessFunction creates an ExceptionProcessFunction wrapped for registration.
func NewExceptionProcessFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[exceptionProcessState](&ExceptionProcessFunction{})
}
