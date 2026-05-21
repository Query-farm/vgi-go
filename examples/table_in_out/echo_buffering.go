// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table_in_out

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// EchoBufferingFunction is a buffered passthrough with projection + filter
// pushdown. Process buffers the full-width input; Finalize narrows each batch
// to the (projected) output schema and applies pushdown filters. Mirrors
// vgi-python's EchoBufferingFunction.
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

	// Parse pushdown filters once (if any).
	var filters *vgi.PushdownFilters
	if params.PushdownFilters != nil {
		filters, _ = vgi.DeserializeFilters(params.PushdownFilters, params.JoinKeys)
	}

	out := make([]arrow.RecordBatch, 0, len(entries))
	for _, e := range entries {
		full, err := vgi.DeserializeRecordBatch(e.Value)
		if err != nil {
			return nil, err
		}
		// Apply filters on the full-width batch (predicates may reference
		// columns that projection would drop), then narrow to the output
		// schema.
		filtered := full
		if filters != nil {
			filtered, err = filters.Apply(ctx, full)
			if err != nil {
				full.Release()
				return nil, err
			}
		}
		projected, err := projectByName(filtered, params.OutputSchema)
		if err != nil {
			return nil, err
		}
		out = append(out, projected)
	}
	return out, nil
}

// projectByName builds a batch containing only the columns named in schema,
// in schema order, taken from src by field name.
func projectByName(src arrow.RecordBatch, schema *arrow.Schema) (arrow.RecordBatch, error) {
	srcSchema := src.Schema()
	cols := make([]arrow.Array, schema.NumFields())
	for i, field := range schema.Fields() {
		idx := srcSchema.FieldIndices(field.Name)
		if len(idx) == 0 {
			return nil, fmt.Errorf("echo_buffering: projected column %q absent from buffered batch", field.Name)
		}
		cols[i] = src.Column(idx[0])
	}
	return array.NewRecordBatch(schema, cols, src.NumRows()), nil
}
