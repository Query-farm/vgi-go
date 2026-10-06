// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The DuckDB extension (vgi 63eb257) runs a table-buffering function's whole
// lifecycle even when its input is empty at runtime: the TABLE_BUFFERING sink
// init, then combine with an EMPTY state_ids list, then finalize. No process
// RPC precedes the combine. These pin that the framework carries that sequence
// through, so a whole-input reduction answers for empty input -- what
// test/sql/integration/table_in_out/table_buffering_empty_input.test asserts
// end to end against sum_all_columns.

var emptyInputCountKey = []byte("count")

// emptyInputCounter counts input rows; Combine always records a total, so an
// empty input finalizes to a single zero row.
type emptyInputCounter struct{}

func (emptyInputCounter) Name() string               { return "empty_input_counter" }
func (emptyInputCounter) Metadata() FunctionMetadata { return FunctionMetadata{} }
func (emptyInputCounter) ArgumentSpecs() []ArgSpec   { return nil }

var emptyInputCounterSchema = arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)

func (emptyInputCounter) OnBind(*BindParams) (*BindResponse, error) {
	return BindSchema(emptyInputCounterSchema)
}

func emptyInputCountBatch(n int64) arrow.RecordBatch {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.Append(n)
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(emptyInputCounterSchema, []arrow.Array{col}, 1)
}

func (emptyInputCounter) Process(ctx context.Context, params *ProcessParams, batch arrow.RecordBatch) ([]byte, error) {
	panic("process must not run for empty input")
}

func (emptyInputCounter) Combine(ctx context.Context, params *ProcessParams, stateIDs [][]byte) ([][]byte, error) {
	if len(stateIDs) != 0 {
		panic("this fixture only sees empty input")
	}
	data, err := SerializeRecordBatch(emptyInputCountBatch(0))
	if err != nil {
		return nil, err
	}
	if _, err := params.Storage.StateAppend(emptyInputCountKey, data); err != nil {
		return nil, err
	}
	return [][]byte{params.ExecutionID}, nil
}

func (emptyInputCounter) Finalize(ctx context.Context, params *ProcessParams, finalizeStateID []byte) ([]arrow.RecordBatch, error) {
	entries, err := params.Storage.StateLogScan(emptyInputCountKey, -1, 0)
	if err != nil {
		return nil, err
	}
	out := make([]arrow.RecordBatch, 0, len(entries))
	for _, e := range entries {
		b, err := DeserializeRecordBatch(e.Value)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

func TestTableBufferingEmptyInputCombinesAndFinalizes(t *testing.T) {
	ctx := context.Background()
	w := NewWorker()
	w.RegisterTableBuffering(emptyInputCounter{})

	inputSchema := shapeTestInputSchema(t)
	outSchema, err := SerializeSchema(emptyInputCounterSchema)
	if err != nil {
		t.Fatal(err)
	}
	bind := BindRequestWire{
		FunctionName: "empty_input_counter",
		FunctionType: string(FunctionTypeTable),
		InputSchema:  &inputSchema,
	}

	// 1. The extension's own TABLE_BUFFERING init (no Sink thread received a row).
	sinkPhase := string(PhaseTableBuffering)
	sink, err := w.handleInit(ctx, shapeTestCallCtx(), InitRequestWire{
		BindCall: bind, OutputSchema: outSchema, Phase: &sinkPhase,
	})
	if err != nil {
		t.Fatalf("sink init: %v", err)
	}
	header, ok := sink.Header.(*GlobalInitResponseWire)
	if !ok || len(header.ExecutionID) == 0 {
		t.Fatalf("sink init returned no execution id: %#v", sink.Header)
	}
	execID := header.ExecutionID

	// 2. combine with an empty state_ids list, and no process RPC before it.
	combined, err := w.handleTableBufferingCombine(ctx, shapeTestCallCtx(), TableBufferingCombineRequestWire{
		FunctionName: "empty_input_counter",
		ExecutionID:  execID,
		StateIDs:     [][]byte{},
	})
	if err != nil {
		t.Fatalf("combine with empty state_ids: %v", err)
	}
	if len(combined.FinalizeStateIDs) != 1 {
		t.Fatalf("combine returned %d finalize state ids, want 1", len(combined.FinalizeStateIDs))
	}

	// 3. finalize answers with the reduction's zero row.
	finalPhase := string(PhaseTableBufferingFinalize)
	fsid := combined.FinalizeStateIDs[0]
	fin, err := w.handleInit(ctx, shapeTestCallCtx(), InitRequestWire{
		BindCall: bind, OutputSchema: outSchema, Phase: &finalPhase,
		FinalizeStateID: &fsid, ExecutionID: &execID,
	})
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	state, ok := fin.State.(*FinalizeProducerState)
	if !ok {
		t.Fatalf("finalize state is %T", fin.State)
	}
	if len(state.BatchIPC) != 1 {
		t.Fatalf("finalize produced %d batches, want the one zero row", len(state.BatchIPC))
	}
	batch, err := DeserializeRecordBatch(state.BatchIPC[0])
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()
	if batch.NumRows() != 1 || batch.Column(0).(*array.Int64).Value(0) != 0 {
		t.Fatalf("finalize row = %v, want n=0", batch)
	}
}
