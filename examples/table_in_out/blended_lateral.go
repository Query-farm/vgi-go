// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Fixtures backing the batched correlated-LATERAL operator (see
// table_in_out/lateral_batch.test): blended_explode carries per-output-row
// provenance for a 1->N fan-out; projectable_blended exercises the operator's
// projection fallback; hostile_provenance emits MALFORMED provenance the
// extension must reject. Mirrors vgi-python's BlendedExplodeFunction /
// ProjectableBlendedFunction / HostileProvenanceFunction.
package table_in_out

import (
	"context"
	"encoding/base64"
	"encoding/binary"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ---------------------------------------------------------------------------
// blended_explode(n) — 1->N fan-out with per-output-row provenance
// ---------------------------------------------------------------------------

// BlendedExplodeFunction is a blended 1->N fan-out map carrying per-output-row
// provenance. For each input row with count n, it emits n output rows (the
// integers 0..n-1). Because the output row count differs from the input row
// count, the worker declares per-output-row provenance via
// vgi.EmitParentRows — parentRows[i] is the index (into this call's input
// batch) of the row that produced output row i. That lets the batched
// correlated LATERAL operator ship a whole input chunk in ONE exchange and
// still stamp each output row's outer/correlated columns from the right input
// row, instead of DuckDB driving the call row-by-row.
//
// One fixture covers all three cardinalities by input value: n=0 -> 1->0
// (filter), n=1 -> 1->1, n=3 -> 1->N. Deterministic output (the emitted
// integers are 0..n-1) so tests assert exact values, and the result must match
// the row-by-row path (SET vgi_batch_lateral=false) exactly.
type BlendedExplodeFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*BlendedExplodeFunction)(nil)

func (f *BlendedExplodeFunction) Name() string { return "blended_explode" }

func (f *BlendedExplodeFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:   "Blended 1->N fan-out (emit 0..n-1 per input row) with row provenance",
		Stability:     vgi.StabilityConsistent,
		Categories:    []string{"blended", "test"},
		InputFromArgs: true,
	}
}

func (f *BlendedExplodeFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "n", Position: 0, ArrowType: "int64", Doc: "Fan-out count: emit rows 0..n-1 for this input row"},
	}
}

func (f *BlendedExplodeFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "i", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}, nil
}

func (f *BlendedExplodeFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

func (f *BlendedExplodeFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	col := batch.Column(0)
	n := int(batch.NumRows())
	var outVals []int64
	var parentRows []int32
	for row := 0; row < n; row++ {
		if col.IsNull(row) {
			continue
		}
		fan := vgi.GetInt64Value(col, row)
		if fan < 0 {
			fan = 0
		}
		for i := int64(0); i < fan; i++ {
			outVals = append(outVals, i)
			parentRows = append(parentRows, int32(row))
		}
	}
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.AppendValues(outVals, nil)
	arr := b.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int64}}, nil)
	outBatch := array.NewRecordBatch(schema, []arrow.Array{arr}, int64(len(outVals)))
	// Whole-chunk fan-out: one emit for the whole input batch, carrying the
	// per-output-row parent index so the batched-LATERAL operator can stamp
	// the correlated columns. (Identity provenance is omitted for 1->1 maps —
	// the extension assumes it — but here the row count changes, so it's
	// required.)
	return vgi.EmitParentRows(out, outBatch, parentRows)
}

func (f *BlendedExplodeFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewBlendedExplodeFunction creates a BlendedExplodeFunction wrapped for registration.
func NewBlendedExplodeFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&BlendedExplodeFunction{})
}

// ---------------------------------------------------------------------------
// projectable_blended(x) — projection_pushdown + two output columns
// ---------------------------------------------------------------------------

// ProjectableBlendedFunction is a blended 1->1 map advertising
// projection_pushdown, with TWO output columns. Regression fixture for the
// batched correlated-LATERAL operator vs projection pushdown: when a
// correlated LATERAL query projects only a SUBSET of the worker's output
// columns, DuckDB's UNUSED_COLUMNS optimizer can narrow the LogicalGet's
// output types before the batched-lateral rewriter runs. The batched operator
// does not support projection pushdown, so it must NOT batch such a get — it
// falls back to the row-by-row path. This fixture + a subset-projection
// LATERAL query proves the fallback keeps results correct (the regression was
// SELECT x, b silently returning column a's value).
//
// For input x emits {a: x*10, b: x*100} (deterministic).
type ProjectableBlendedFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*ProjectableBlendedFunction)(nil)

func (f *ProjectableBlendedFunction) Name() string { return "projectable_blended" }

func (f *ProjectableBlendedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Blended 1->1 map with projection_pushdown + two output columns",
		Stability:          vgi.StabilityConsistent,
		Categories:         []string{"blended", "test"},
		InputFromArgs:      true,
		ProjectionPushdown: true,
	}
}

func (f *ProjectableBlendedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "x", Position: 0, ArrowType: "int64", Doc: "Input column"},
	}
}

func (f *ProjectableBlendedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "a", Type: arrow.PrimitiveTypes.Int64},
			{Name: "b", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}, nil
}

func (f *ProjectableBlendedFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

func (f *ProjectableBlendedFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	col := batch.Column(0)
	n := int(batch.NumRows())
	mem := memory.NewGoAllocator()
	ab := array.NewInt64Builder(mem)
	defer ab.Release()
	bb := array.NewInt64Builder(mem)
	defer bb.Release()
	for row := 0; row < n; row++ {
		if col.IsNull(row) {
			ab.AppendNull()
			bb.AppendNull()
			continue
		}
		x := vgi.GetInt64Value(col, row)
		ab.Append(x * 10)
		bb.Append(x * 100)
	}
	aArr := ab.NewArray()
	defer aArr.Release()
	bArr := bb.NewArray()
	defer bArr.Release()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	outBatch := array.NewRecordBatch(schema, []arrow.Array{aArr, bArr}, int64(n))
	// 1->1 identity map: no provenance needed (the operator assumes identity).
	// The framework narrows the emitted batch to the projected columns.
	return out.Emit(outBatch)
}

func (f *ProjectableBlendedFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewProjectableBlendedFunction creates a ProjectableBlendedFunction wrapped for registration.
func NewProjectableBlendedFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&ProjectableBlendedFunction{})
}

// ---------------------------------------------------------------------------
// hostile_provenance(x, mode := ...) — adversarial malformed provenance
// ---------------------------------------------------------------------------

// HostileProvenanceFunction is an adversarial blended fixture: it emits a
// MALFORMED vgi_rpc.parent_row payload. Simulates a buggy or hostile worker
// (especially a remote HTTP one) that sends provenance the batched
// correlated-LATERAL operator must reject rather than use as an unchecked
// array index. Emits one output row per input row (so the row count matches —
// the metadata is present, not the identity path), but attaches a poisoned
// vgi_rpc.parent_row#b64 according to mode:
//
//   - range  — a well-formed int32[] of the right length whose values are all
//     num_rows (one past the last valid index). The C++ range check must throw.
//   - length — a valid-base64 int32[] blob that is one element TOO LONG. The
//     length check must throw.
//   - base64 — a value that is not valid base64 at all. The base64 decode must
//     throw.
//
// Each is asserted on BOTH transports so the subprocess and HTTP parse paths
// stay symmetric. The payload is set via the raw metadata key (not
// EmitParentRows) so it bypasses the framework's length-only check and reaches
// the C++ validation unfiltered.
type HostileProvenanceFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*HostileProvenanceFunction)(nil)

func (f *HostileProvenanceFunction) Name() string { return "hostile_provenance" }

func (f *HostileProvenanceFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:   "Adversarial blended fixture emitting malformed vgi_rpc.parent_row",
		Stability:     vgi.StabilityConsistent,
		Categories:    []string{"blended", "test", "adversarial"},
		InputFromArgs: true,
	}
}

func (f *HostileProvenanceFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "x", Position: 0, ArrowType: "int64", Doc: "Input column (echoed as output)"},
		{Name: "mode", Position: -1, ArrowType: "varchar", Doc: "range | length | base64",
			HasDefault: true, DefaultValue: "range"},
	}
}

func (f *HostileProvenanceFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "hv", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}, nil
}

func (f *HostileProvenanceFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

// packInt32LE renders vals as raw little-endian int32 bytes.
func packInt32LE(vals []int32) []byte {
	raw := make([]byte, 4*len(vals))
	for i, v := range vals {
		binary.LittleEndian.PutUint32(raw[i*4:], uint32(v))
	}
	return raw
}

func (f *HostileProvenanceFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	col := batch.Column(0)
	n := int(batch.NumRows())
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for row := 0; row < n; row++ {
		if col.IsNull(row) {
			b.AppendNull()
		} else {
			b.Append(vgi.GetInt64Value(col, row))
		}
	}
	arr := b.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "hv", Type: arrow.PrimitiveTypes.Int64}}, nil)
	outBatch := array.NewRecordBatch(schema, []arrow.Array{arr}, int64(n))

	var payload string
	switch vgi.OptionalString(params.Args, "mode", "range") {
	case "base64":
		payload = "@@@ this is not base64 @@@"
	case "length":
		// One int32 too many for the emitted row count.
		payload = base64.StdEncoding.EncodeToString(packInt32LE(make([]int32, n+1)))
	default: // "range" — every parent index == n (one past the last valid index n-1)
		vals := make([]int32, n)
		for i := range vals {
			vals[i] = int32(n)
		}
		payload = base64.StdEncoding.EncodeToString(packInt32LE(vals))
	}
	return vgi.Emit(out, outBatch, vgi.WithMetadata("vgi_rpc.parent_row#b64", payload))
}

func (f *HostileProvenanceFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewHostileProvenanceFunction creates a HostileProvenanceFunction wrapped for registration.
func NewHostileProvenanceFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&HostileProvenanceFunction{})
}
