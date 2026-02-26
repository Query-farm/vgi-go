// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table_in_out

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// EchoFunction is a passthrough function that emits each input batch unchanged.
type EchoFunction struct{}

func (f *EchoFunction) Name() string { return "echo" }

func (f *EchoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Passthrough function that emits each input batch unchanged",
		Stability:         vgi.StabilityConsistent,
		ProjectionPushdown: true,
	}
}

func (f *EchoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"},
	}
}

func (f *EchoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: params.InputSchema}, nil
}

func (f *EchoFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}

func (f *EchoFunction) NewState(params *vgi.ProcessParams) (interface{}, error) {
	return nil, nil
}

func (f *EchoFunction) Process(ctx context.Context, params *vgi.ProcessParams, state interface{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	return out.Emit(batch)
}

func (f *EchoFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state interface{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}
