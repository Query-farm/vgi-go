// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table_in_out

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// EchoBufferingFunction is a buffered passthrough with projection + filter
// pushdown. Process buffers the full-width input; the SDK applies filters and
// projection to each Finalize batch. Mirrors vgi-python's EchoBufferingFunction.
type EchoBufferingFunction struct{}

var _ vgi.TableBufferingFunction = (*EchoBufferingFunction)(nil)

func (f *EchoBufferingFunction) Name() string { return "echo_buffering" }

func (f *EchoBufferingFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Buffered passthrough with projection + filter pushdown",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
		Categories:         []string{"test", "pushdown", "buffer"},
	}
}

func (f *EchoBufferingFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"}}
}

func (f *EchoBufferingFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindInputSchema(params)
}

func (f *EchoBufferingFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) ([]byte, error) {
	data, err := vgi.SerializeRecordBatch(batch)
	if err != nil {
		return nil, err
	}
	if _, err := params.Storage.StateAppend(bufKey, data); err != nil {
		return nil, err
	}
	return params.ExecutionID, nil
}

func (f *EchoBufferingFunction) Combine(ctx context.Context, params *vgi.ProcessParams, stateIDs [][]byte) ([][]byte, error) {
	return [][]byte{params.ExecutionID}, nil
}

func (f *EchoBufferingFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, finalizeStateID []byte) ([]arrow.RecordBatch, error) {
	entries, err := params.Storage.StateLogScan(bufKey, -1, 0)
	if err != nil {
		return nil, err
	}

	out := make([]arrow.RecordBatch, 0, len(entries))
	for _, e := range entries {
		full, err := vgi.DeserializeRecordBatch(e.Value)
		if err != nil {
			return nil, err
		}
		out = append(out, full)
	}
	return out, nil
}
