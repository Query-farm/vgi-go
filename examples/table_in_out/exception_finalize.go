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

var _ vgi.TypedTableInOutFunc[struct{}] = (*ExceptionFinalizeFunction)(nil)

func (f *ExceptionFinalizeFunction) Name() string { return "exception_finalize" }

func (f *ExceptionFinalizeFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Test function that raises exception during finalize",
		Stability:   vgi.StabilityConsistent,
		HasFinalize: true,
	}
}

func (f *ExceptionFinalizeFunction) ArgumentSpecs() []vgi.ArgSpec {
	return sumColumnsArgSpecs
}

func (f *ExceptionFinalizeFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return sumColumnsOnBind(params)
}

func (f *ExceptionFinalizeFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

func (f *ExceptionFinalizeFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	return out.Emit(vgi.EmptyBatch(params.OutputSchema))
}

func (f *ExceptionFinalizeFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, fmt.Errorf("Intentional exception during finalize()")
}

// NewExceptionFinalizeFunction creates an ExceptionFinalizeFunction wrapped for registration.
func NewExceptionFinalizeFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&ExceptionFinalizeFunction{})
}
