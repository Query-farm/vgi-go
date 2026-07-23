// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table_in_out

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Same-name-in-two-schemas *exchange-mode* fixtures.
//
// The scalar analogue lives in examples/scalar/same_name.go. This file covers
// the two exchange-mode shapes, which reach the worker through different bind
// call sites in the DuckDB extension than scalars do:
//
//   - table-in-out — VgiTableInOutBind builds its bind-time connection directly
//     rather than going through AcquireAndBindConnection.
//   - table-buffering — shares that bind site, but its runtime connections come
//     from the buffering operator's own BuildAcquireParams, and the unary
//     process/combine RPCs re-resolve the function outside the bind entirely.
//
// That distinction is the point. The extension originally threaded the owning
// schema onto the runtime exchange connections but not onto the bind-time one,
// so an exchange-mode call reached the worker with no BindRequest.schema_name
// and could not be resolved when one name was declared in two schemas. The
// scalar fixture cannot catch it — scalars bind through a separate call site.
//
// Each implementation tags its rows with its own schema, so a mis-routed bind
// reads as the wrong tag rather than a plausible answer. Driven by
// ../vgi/test/sql/integration/table_in_out/same_name_schemas.test.

// Deliberately shared across the two schemas — the collision is the point.
const (
	sameNameTransformName = "test_same_name_transform"
	sameNameBufferedName  = "test_same_name_buffered"
)

// sameNameOutputSchema is the single column every implementation here emits.
var sameNameOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "tag", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

// sameNameTagBatch renders "<schemaName>:<value>" for every row of the first
// input column, preserving nulls.
func sameNameTagBatch(schemaName string, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	col := batch.Column(0)
	n := int(batch.NumRows())
	b := array.NewStringBuilder(memory.NewGoAllocator())
	defer b.Release()
	b.Reserve(n)
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(fmt.Sprintf("%s:%d", schemaName, vgi.GetInt64Value(col, i)))
	}
	arr := b.NewArray()
	defer arr.Release()
	return array.NewRecordBatch(sameNameOutputSchema, []arrow.Array{arr}, int64(n)), nil
}

// sameNameArgSpecs is the single TABLE argument both shapes take.
func sameNameArgSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table whose first column is tagged"},
	}
}

// ---------------------------------------------------------------------------
// test_same_name_transform — table-in-out (streaming exchange)
// ---------------------------------------------------------------------------

// SameNameTransformFunction tags each input row with the schema it was declared
// in. Two instances register under one name, in `main` and in `data`.
type SameNameTransformFunction struct {
	// schema is the catalog schema this instance is registered into — the tag
	// it stamps, so a mis-route is visible in the query result.
	schema string
}

var _ vgi.TypedTableInOutFunc[struct{}] = (*SameNameTransformFunction)(nil)

func (f *SameNameTransformFunction) Name() string { return sameNameTransformName }

func (f *SameNameTransformFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Schema-disambiguation probe; the " + f.schema + "-schema table-in-out",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"test", "schema"},
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT * FROM example." + f.schema + ".test_same_name_transform((SELECT 1 AS n))",
				Description: "Returns '" + f.schema + ":1'",
			},
		},
	}
}

func (f *SameNameTransformFunction) ArgumentSpecs() []vgi.ArgSpec { return sameNameArgSpecs() }

func (f *SameNameTransformFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(sameNameOutputSchema)
}

func (f *SameNameTransformFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

func (f *SameNameTransformFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	tagged, err := sameNameTagBatch(f.schema, batch)
	if err != nil {
		return err
	}
	defer tagged.Release()
	return out.Emit(tagged)
}

func (f *SameNameTransformFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewSameNameTransformFunction wraps the transform for registration into
// schemaName, which is also the tag it stamps.
func NewSameNameTransformFunction(schemaName string) vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&SameNameTransformFunction{schema: schemaName})
}

// ---------------------------------------------------------------------------
// test_same_name_buffered — table-buffering (Sink+Source exchange)
// ---------------------------------------------------------------------------

// sameNameBufKey is the state-log slot the Sink phase buffers tagged batches in.
var sameNameBufKey = []byte("same_name_buffered")

// SameNameBufferedFunction buffers tagged rows in the Sink phase and drains
// them in the Source phase.
//
// Tagging in Process (rather than in Finalize) is deliberate: it proves the
// SINK-side worker resolved the right implementation, and the Sink phase is
// driven by the unary table_buffering_process RPC — a different connection from
// the one the Source phase acquires.
type SameNameBufferedFunction struct {
	// schema is the catalog schema this instance is registered into.
	schema string
}

var _ vgi.TableBufferingFunction = (*SameNameBufferedFunction)(nil)

func (f *SameNameBufferedFunction) Name() string { return sameNameBufferedName }

func (f *SameNameBufferedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Schema-disambiguation probe; the " + f.schema + "-schema buffered function",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"test", "schema", "buffer"},
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT * FROM example." + f.schema + ".test_same_name_buffered((SELECT 1 AS n))",
				Description: "Returns '" + f.schema + ":1'",
			},
		},
	}
}

func (f *SameNameBufferedFunction) ArgumentSpecs() []vgi.ArgSpec { return sameNameArgSpecs() }

func (f *SameNameBufferedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(sameNameOutputSchema)
}

func (f *SameNameBufferedFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) ([]byte, error) {
	tagged, err := sameNameTagBatch(f.schema, batch)
	if err != nil {
		return nil, err
	}
	defer tagged.Release()
	data, err := vgi.SerializeRecordBatch(tagged)
	if err != nil {
		return nil, err
	}
	if _, err := params.Storage.StateAppend(sameNameBufKey, data); err != nil {
		return nil, err
	}
	return params.ExecutionID, nil
}

func (f *SameNameBufferedFunction) Combine(ctx context.Context, params *vgi.ProcessParams, stateIDs [][]byte) ([][]byte, error) {
	// Collapse every Sink bucket into one finalize stream.
	return [][]byte{params.ExecutionID}, nil
}

func (f *SameNameBufferedFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, finalizeStateID []byte) ([]arrow.RecordBatch, error) {
	entries, err := params.Storage.StateLogScan(sameNameBufKey, -1, 0)
	if err != nil {
		return nil, err
	}
	out := make([]arrow.RecordBatch, 0, len(entries))
	for _, e := range entries {
		batch, err := vgi.DeserializeRecordBatch(e.Value)
		if err != nil {
			return nil, err
		}
		out = append(out, batch)
	}
	return out, nil
}

// NewSameNameBufferedFunction builds the buffered probe for registration into
// schemaName, which is also the tag it stamps.
func NewSameNameBufferedFunction(schemaName string) vgi.TableBufferingFunction {
	return &SameNameBufferedFunction{schema: schemaName}
}
