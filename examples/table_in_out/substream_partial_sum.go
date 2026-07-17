// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table_in_out

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// SubstreamPartialSumFunction is a per-substream partial sum emitted at
// finalize — proves parallel streaming FINALIZE (Phase A / A4).
//
// A streaming table-in-out WITH a finalize is still a per-substream operation
// under per-substream worker fan-out: Process accumulates only THIS
// substream's rows (emitting nothing but the mandatory empty batch), and
// Finalize emits ONE row = this substream's partial sum. DuckDB fans the input
// across N workers and unions their finalize outputs, so the caller
// re-aggregates with an outer SELECT sum(...) to get the global total —
// correct no matter how the rows were partitioned across substreams. Each
// substream's Finalize reads only its OWN accumulated state (keyed by the
// substream's execution_id in execution-scoped storage; params.SubstreamID is
// the stable client-owned key available for workers that manage cross-backend
// state themselves). This is NOT a global cross-substream combine (that is a
// TableBufferingFunction; see DistributedSumFunction). Mirrors vgi-python's
// SubstreamPartialSumFunction.
//
// Invariant the tests assert (deterministic regardless of thread/substream
// count): SELECT sum(n) FROM substream_partial_sum((SELECT ... AS n)) equals
// the sum of the input column, because the per-substream partials sum to the
// whole.
type SubstreamPartialSumFunction struct{}

var _ vgi.TypedTableInOutFunc[substreamPartialSumState] = (*SubstreamPartialSumFunction)(nil)

// substreamPartialSumState is the running sum for ONE substream's worker
// (never merged across substreams).
type substreamPartialSumState struct {
	Total int64
}

func (f *SubstreamPartialSumFunction) Name() string { return "substream_partial_sum" }

func (f *SubstreamPartialSumFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Per-substream partial sum emitted at finalize (parallel streaming finalize)",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"aggregation", "numeric"},
		HasFinalize: true,
	}
}

func (f *SubstreamPartialSumFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"},
	}
}

func (f *SubstreamPartialSumFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	field := params.InputSchema.Field(0)
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: field.Name, Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}, nil
}

func (f *SubstreamPartialSumFunction) NewState(params *vgi.ProcessParams) (*substreamPartialSumState, error) {
	return &substreamPartialSumState{}, nil
}

func (f *SubstreamPartialSumFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *substreamPartialSumState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	state.Total += sumInt64Column(batch.Column(0))
	// Persist the running total (per worker pid) so the FINALIZE-phase init —
	// a separate stream with a fresh state — can read this substream's
	// accumulated partial from execution-scoped storage.
	if params.Storage != nil {
		if err := params.Storage.Put(encodeInt64(state.Total)); err != nil {
			return err
		}
	}
	// Accumulate only; emit nothing during processing (the framework requires
	// one emitted batch per exchange, so send an empty one).
	return out.Emit(vgi.EmptyBatch(params.OutputSchema))
}

func (f *SubstreamPartialSumFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *substreamPartialSumState) ([]arrow.RecordBatch, error) {
	// The stored values are THIS substream's accumulated totals (one per worker
	// pid that handled this substream's batches); their sum is this substream's
	// partial.
	var total int64
	if params.Storage != nil {
		workerData, err := params.Storage.Collect()
		if err != nil {
			return nil, err
		}
		for _, data := range workerData {
			total += decodeInt64(data)
		}
	}
	name := params.OutputSchema.Field(0).Name
	col := vgi.BuildInt64Array(1, func(_ int64) int64 { return total })
	batch, err := vgi.BatchFromMap(params.OutputSchema, map[string]arrow.Array{name: col}, 1)
	if err != nil {
		return nil, err
	}
	return []arrow.RecordBatch{batch}, nil
}

// encodeInt64 renders v as 8 little-endian bytes.
func encodeInt64(v int64) []byte {
	b := make([]byte, 8)
	for i := 0; i < 8; i++ {
		b[i] = byte(uint64(v) >> (8 * i))
	}
	return b
}

// decodeInt64 reads 8 little-endian bytes back into an int64.
func decodeInt64(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	var v uint64
	for i := 0; i < 8; i++ {
		v |= uint64(b[i]) << (8 * i)
	}
	return int64(v)
}

// NewSubstreamPartialSumFunction creates a SubstreamPartialSumFunction wrapped
// for registration.
func NewSubstreamPartialSumFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[substreamPartialSumState](&SubstreamPartialSumFunction{})
}
