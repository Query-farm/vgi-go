// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table_in_out

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// BufferInputFunction collects all input batches and emits during finalization.
type BufferInputFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*BufferInputFunction)(nil)

func (f *BufferInputFunction) Name() string { return "buffer_input" }

func (f *BufferInputFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Collects all input batches and emits during finalization",
		Stability:   vgi.StabilityConsistent,
		HasFinalize: true,
	}
}

func (f *BufferInputFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"},
	}
}

func (f *BufferInputFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindInputSchema(params)
}

func (f *BufferInputFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

func (f *BufferInputFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	// Buffer the batch in storage
	if params.Storage != nil {
		if err := params.Storage.QueuePushBatches([]arrow.RecordBatch{batch}); err != nil {
			return err
		}
	}
	// Emit empty batch
	return out.Emit(vgi.EmptyBatch(params.OutputSchema))
}

func (f *BufferInputFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	if params.Storage == nil {
		return nil, nil
	}
	var batches []arrow.RecordBatch
	for {
		batch, err := params.Storage.QueuePopBatch()
		if err != nil {
			return nil, err
		}
		if batch == nil {
			break
		}
		batches = append(batches, batch)
	}
	return batches, nil
}

// NewBufferInputFunction creates a BufferInputFunction wrapped for registration.
func NewBufferInputFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&BufferInputFunction{})
}
