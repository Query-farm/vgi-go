// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table_in_out

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// MultiBatchFinishFunction is a streaming FINALIZE that emits MANY batches.
//
// Every other finalize fixture, in every SDK, emits exactly ONE batch — and one
// batch is the easy case: over HTTP a producer is strictly lock-step, so a
// single-batch flush completes inside its one turn and never needs a
// continuation. Two or more do, and that path was broken in two independent
// places (the Rust worker's flush producer and the DuckDB client's finalize
// drain) for as long as no fixture emitted a second batch.
//
// It emits one batch per input row the substream saw: the first carries that
// substream's total, the rest carry 0. The split makes the two failure modes
// tell themselves apart —
//
//	wrong SUM   → a batch's CONTENTS were lost, duplicated or reordered.
//	wrong COUNT → a whole BATCH was lost or repeated. This is the one that
//	              catches a truncated flush, because the rows that DO arrive are
//	              correct and only the count betrays the missing ones.
//
// Both invariants hold at any substream fan-out (each substream's partials sum
// to the whole, and its batch count equals its row count), so the SQL test needs
// no assumption about thread count. Mirrors vgi-python's
// MultiBatchFinishFunction. Backs
// vgi/test/sql/integration/table_in_out/multi_batch_finalize.test.
type MultiBatchFinishFunction struct{}

var _ vgi.TypedTableInOutFunc[multiBatchFinishState] = (*MultiBatchFinishFunction)(nil)

// multiBatchFinishState is this substream's running total and the number of
// rows it has seen.
type multiBatchFinishState struct {
	Total int64
	Rows  int64
}

func (f *MultiBatchFinishFunction) Name() string { return "multi_batch_finish" }

func (f *MultiBatchFinishFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Streaming finalize that emits one batch per input row (multi-batch flush)",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"testing", "aggregation"},
		HasFinalize: true,
	}
}

func (f *MultiBatchFinishFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"},
	}
}

func (f *MultiBatchFinishFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	field := params.InputSchema.Field(0)
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: field.Name, Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}, nil
}

func (f *MultiBatchFinishFunction) NewState(params *vgi.ProcessParams) (*multiBatchFinishState, error) {
	return &multiBatchFinishState{}, nil
}

func (f *MultiBatchFinishFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *multiBatchFinishState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	state.Total += sumInt64Column(batch.Column(0))
	state.Rows += batch.NumRows()
	// Persist (total, rows) so the FINALIZE-phase init — a separate stream with
	// a fresh state — can read this substream's accumulation back from
	// execution-scoped storage.
	if params.Storage != nil {
		blob := append(encodeInt64(state.Total), encodeInt64(state.Rows)...)
		if err := params.Storage.Put(blob); err != nil {
			return err
		}
	}
	// Accumulate only; emit nothing during processing (the framework requires
	// one emitted batch per exchange, so send an empty one).
	return out.Emit(vgi.EmptyBatch(params.OutputSchema))
}

func (f *MultiBatchFinishFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *multiBatchFinishState) ([]arrow.RecordBatch, error) {
	// One stored entry per worker pid that handled this substream's batches,
	// each holding that pid's own running (total, rows) — so summing across
	// them gives this substream's accumulation, exactly as
	// SubstreamPartialSumFunction does.
	var total, rows int64
	if params.Storage != nil {
		workerData, err := params.Storage.Collect()
		if err != nil {
			return nil, err
		}
		for _, data := range workerData {
			if len(data) < 16 {
				continue
			}
			total += decodeInt64(data[0:8])
			rows += decodeInt64(data[8:16])
		}
	}
	name := params.OutputSchema.Field(0).Name
	batches := make([]arrow.RecordBatch, 0, rows)
	for i := int64(0); i < rows; i++ {
		v := int64(0)
		if i == 0 {
			v = total
		}
		col := vgi.BuildInt64Array(1, func(_ int64) int64 { return v })
		batch, err := vgi.BatchFromMap(params.OutputSchema, map[string]arrow.Array{name: col}, 1)
		if err != nil {
			return nil, err
		}
		batches = append(batches, batch)
	}
	return batches, nil
}

// NewMultiBatchFinishFunction creates a MultiBatchFinishFunction wrapped for
// registration.
func NewMultiBatchFinishFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[multiBatchFinishState](&MultiBatchFinishFunction{})
}
