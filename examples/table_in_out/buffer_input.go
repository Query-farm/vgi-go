// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table_in_out

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// bufKey is the state-log key BufferInputFunction stores buffered batches under.
var bufKey = []byte("buf")

// BufferInputFunction is a table-buffering function that collects every input
// batch during the sink phase and re-emits them during finalize. One bucket
// per execution: Process appends to a single shared log and returns the
// execution_id; Combine collapses all (identical) state_ids to one
// finalize_state_id; Finalize drains the buffered batches. Mirrors
// vgi-python's BufferInputFunction.
type BufferInputFunction struct{}

var _ vgi.TableBufferingFunction = (*BufferInputFunction)(nil)

func (f *BufferInputFunction) Name() string { return "buffer_input" }

func (f *BufferInputFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Collects all input batches and emits during finalization",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"utility", "buffer"},
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

func (f *BufferInputFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) ([]byte, error) {
	data, err := vgi.SerializeRecordBatch(batch)
	if err != nil {
		return nil, err
	}
	if _, err := params.Storage.StateAppend(bufKey, data); err != nil {
		return nil, err
	}
	return params.ExecutionID, nil
}

func (f *BufferInputFunction) Combine(ctx context.Context, params *vgi.ProcessParams, stateIDs [][]byte) ([][]byte, error) {
	return [][]byte{params.ExecutionID}, nil
}

func (f *BufferInputFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, finalizeStateID []byte) ([]arrow.RecordBatch, error) {
	entries, err := params.Storage.StateLogScan(bufKey, -1, 0)
	if err != nil {
		return nil, err
	}
	batches := make([]arrow.RecordBatch, 0, len(entries))
	for _, e := range entries {
		b, err := vgi.DeserializeRecordBatch(e.Value)
		if err != nil {
			return nil, err
		}
		batches = append(batches, b)
	}
	return batches, nil
}
