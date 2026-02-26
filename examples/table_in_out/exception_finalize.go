// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table_in_out

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// ExceptionFinalizeFunction raises an exception during finalize.
type ExceptionFinalizeFunction struct{}

var _ vgi.TypedTableInOutFunc[sumAllColumnsState] = (*ExceptionFinalizeFunction)(nil)

func (f *ExceptionFinalizeFunction) Name() string { return "exception_finalize" }

func (f *ExceptionFinalizeFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Test function that raises exception during finalize",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *ExceptionFinalizeFunction) ArgumentSpecs() []vgi.ArgSpec {
	return sumColumnsArgSpecs
}

func (f *ExceptionFinalizeFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return sumColumnsOnBind(params)
}

func (f *ExceptionFinalizeFunction) NewState(params *vgi.ProcessParams) (*sumAllColumnsState, error) {
	return (&SumAllColumnsFunction{}).NewState(params)
}

func (f *ExceptionFinalizeFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *sumAllColumnsState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	return (&SumAllColumnsFunction{}).Process(ctx, params, state, batch, out)
}

func (f *ExceptionFinalizeFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *sumAllColumnsState) ([]arrow.RecordBatch, error) {
	return nil, fmt.Errorf("Intentional exception during finalize()")
}

// NewExceptionFinalizeFunction creates an ExceptionFinalizeFunction wrapped for registration.
func NewExceptionFinalizeFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[sumAllColumnsState](&ExceptionFinalizeFunction{})
}
