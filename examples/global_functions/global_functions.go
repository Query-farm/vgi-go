// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package global_functions holds the probe functions for global (system.main)
// registration.
//
// WARNING: EXAMPLE/TEST FUNCTIONS ONLY. These exist purely so a client can be
// observed publishing a worker's functions into its *global* function
// namespace, and are deliberately separate from every other fixture:
//
//   - They are additive for other language implementations. The example
//     catalog is a cross-language contract — Python, TypeScript, and Java
//     workers mirror it. If global registration reused existing fixtures
//     (double, ten_thousand, vgi_sum, echo_buffering), every implementation
//     would have to make the same semantic change to functions it already
//     ships. New functions cost each implementation only what it chooses to
//     add, when it chooses to add it.
//   - They document their own purpose. Nothing else depends on them, so
//     changing one cannot break an unrelated test, and a reader at the
//     definition site can see what they are for.
//
// One per catalog function type, so the client's registration path is
// exercised for every function kind:
//
//	Kind              Registered name    Published as (vgi_example)
//	scalar            global_scalar      vgi_example_global_scalar
//	table             global_table       vgi_example_global_table
//	aggregate         global_agg         vgi_example_global_agg
//	table-buffering   global_buffered    vgi_example_global_buffered
//
// Each returns a value tagged with its own name so a test can assert that the
// globally-published name reached the function it was supposed to, rather than
// some same-named function belonging to another catalog.
//
// Mirrors vgi-python's vgi/_test_fixtures/global_functions.py.
package global_functions

import (
	"context"
	"encoding/gob"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// probeCategories is the category list every probe carries, so the four are
// selectable as a group through the catalog.
var probeCategories = []string{"test", "global"}

func init() {
	// gob requires concrete types be registered so the encoder can persist
	// them through interface{} state slots (aggregate states cross the wire
	// between update/combine/finalize).
	gob.Register(&globalAggState{})
}

// RegisterAll registers the four global-registration probes on w, one per
// catalog function type. They live in the worker's default schema (main) —
// mirroring vgi-python, where the same four classes are listed both in the
// example catalog's `main` schema and in its `global_functions`.
func RegisterAll(w *vgi.Worker) {
	w.RegisterScalar(&GlobalScalarFunction{})
	w.RegisterTable(NewGlobalTableFunction())
	w.RegisterAggregate(&GlobalAggFunction{})
	w.RegisterTableBuffering(&GlobalBufferedFunction{})
}

// ---------------------------------------------------------------------------
// global_scalar
// ---------------------------------------------------------------------------

// GlobalScalarFunction is the scalar probe — it labels each input so the caller
// can prove which implementation ran.
//
// SQL: SELECT vgi_example_global_scalar(7) -> 'global_scalar:7'
type GlobalScalarFunction struct{}

var _ vgi.ScalarFunction = (*GlobalScalarFunction)(nil)

// Name returns the registered function name.
func (f *GlobalScalarFunction) Name() string { return "global_scalar" }

// Metadata returns descriptive metadata for global_scalar.
func (f *GlobalScalarFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Global-registration probe (scalar)",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
		Categories:  probeCategories,
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT vgi_example_global_scalar(7)",
				Description: "Scalar probe published into system.main",
			},
		},
	}
}

// ArgumentSpecs declares the single int64 column argument.
func (f *GlobalScalarFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "int64", Doc: "Value to label"},
	}
}

// OnBind resolves the output type (always VARCHAR).
func (f *GlobalScalarFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

// Process returns "global_scalar:<value>" for each row; NULL in, NULL out.
func (f *GlobalScalarFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	get := vgi.Int64Accessor(batch.Column(0)) // hoist the type switch out of the row loop
	return vgi.MapColumn(params, batch, 0, array.NewStringBuilder,
		func(_ arrow.Array, i int) string {
			return fmt.Sprintf("global_scalar:%d", get(i))
		})
}

// ---------------------------------------------------------------------------
// global_table
// ---------------------------------------------------------------------------

// GlobalTableSchema is the fixed output schema global_table always produces.
var GlobalTableSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	{Name: "label", Type: arrow.BinaryTypes.String},
}, nil)

// GlobalTableFunction is the table probe — three labelled rows, no arguments.
//
// SQL: SELECT * FROM vgi_example_global_table()
type GlobalTableFunction struct{}

var (
	_ vgi.TypedTableFunc[globalTableState] = (*GlobalTableFunction)(nil)
	_ vgi.CardinalityEstimator             = (*GlobalTableFunction)(nil)
)

// globalTableState records whether the single output batch has been emitted.
type globalTableState struct{ Emitted bool }

// Name returns the registered function name.
func (f *GlobalTableFunction) Name() string { return "global_table" }

// Metadata returns descriptive metadata for global_table.
func (f *GlobalTableFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Global-registration probe (table)",
		Stability:   vgi.StabilityConsistent,
		Categories:  probeCategories,
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT * FROM vgi_example_global_table()",
				Description: "Table probe published into system.main",
			},
		},
	}
}

// ArgumentSpecs declares no arguments — global_table is a fixed generator.
func (f *GlobalTableFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

// OnBind resolves the fixed output schema.
func (f *GlobalTableFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(GlobalTableSchema)
}

// Cardinality reports the exact row count: three rows, always.
func (f *GlobalTableFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	return &vgi.TableCardinality{Estimate: 3, Max: 3}, nil
}

// NewState creates a fresh per-scan cursor.
func (f *GlobalTableFunction) NewState(params *vgi.ProcessParams) (*globalTableState, error) {
	return &globalTableState{}, nil
}

// Process emits the three probe rows once, then finishes.
func (f *GlobalTableFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *globalTableState, out *vgirpc.OutputCollector) error {
	if state.Emitted {
		return out.Finish()
	}
	state.Emitted = true

	ns := vgi.BuildInt64Array(3, func(i int64) int64 { return i })
	labels := vgi.BuildStringArray(3, func(i int64) string {
		return fmt.Sprintf("global_table:%d", i)
	})
	return out.Emit(array.NewRecordBatch(params.OutputSchema, []arrow.Array{ns, labels}, 3))
}

// NewGlobalTableFunction adapts GlobalTableFunction for Worker.RegisterTable.
func NewGlobalTableFunction() vgi.TableFunction {
	return vgi.AsTableFunction[globalTableState](&GlobalTableFunction{})
}

// ---------------------------------------------------------------------------
// global_agg
// ---------------------------------------------------------------------------

// GlobalAggFunction is the aggregate probe — it sums int64 input.
//
// SQL: SELECT vgi_example_global_agg(v) FROM t
type GlobalAggFunction struct{}

var _ vgi.AggregateFunction = (*GlobalAggFunction)(nil)

// globalAggState is one group's running total.
type globalAggState struct{ Total int64 }

// globalAggArgs is the typed argument schema for global_agg().
type globalAggArgs struct {
	Value int64 `vgi:"pos=0,const=false,doc=Column to sum"`
}

// Name returns the registered function name.
func (f *GlobalAggFunction) Name() string { return "global_agg" }

// Metadata returns descriptive metadata for global_agg.
func (f *GlobalAggFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Global-registration probe (aggregate)",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingDefault,
		ReturnType:        arrow.PrimitiveTypes.Int64,
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
		Categories:        probeCategories,
	}
}

// ArgumentSpecs declares the single int64 column argument.
func (f *GlobalAggFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(globalAggArgs{})
}

// OnBind resolves the output type (always BIGINT).
func (f *GlobalAggFunction) OnBind(p *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

// NewState creates a fresh per-group state.
func (f *GlobalAggFunction) NewState(*vgi.AggregateProcessParams) interface{} {
	return &globalAggState{}
}

// Update accumulates each group's values.
func (f *GlobalAggFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, _ *vgi.AggregateProcessParams) error {
	if len(columns) == 0 {
		return fmt.Errorf("global_agg: missing value column")
	}
	col, ok := columns[0].(*array.Int64)
	if !ok {
		return fmt.Errorf("global_agg: value column is %T, expected int64", columns[0])
	}
	for i := 0; i < gids.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		s := vgi.EnsureState(states, gids.At(i), func() *globalAggState { return &globalAggState{} })
		s.Total += col.Value(i)
	}
	return nil
}

// Combine merges two partial states.
func (f *GlobalAggFunction) Combine(source, target interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	s := source.(*globalAggState)
	t := target.(*globalAggState)
	return &globalAggState{Total: s.Total + t.Total}, nil
}

// Finalize emits one total per group; a group with no state yields NULL.
func (f *GlobalAggFunction) Finalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	b := array.NewInt64Builder(memory.NewGoAllocator())
	defer b.Release()
	for _, gid := range gids {
		if s, ok := states[gid].(*globalAggState); ok {
			b.Append(s.Total)
		} else {
			b.AppendNull()
		}
	}
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(p.OutputSchema, []arrow.Array{col}, int64(len(gids))), nil
}

// ---------------------------------------------------------------------------
// global_buffered
// ---------------------------------------------------------------------------

// globalBufKey is the state-log key GlobalBufferedFunction buffers batches
// under. Its own key, so the probe cannot be perturbed by another fixture.
var globalBufKey = []byte("global_buf")

// GlobalBufferedFunction is the table-buffering probe — it buffers all input
// and replays it on finalize.
//
// SQL: SELECT * FROM vgi_example_global_buffered((SELECT * FROM t))
type GlobalBufferedFunction struct{}

var _ vgi.TableBufferingFunction = (*GlobalBufferedFunction)(nil)

// Name returns the registered function name.
func (f *GlobalBufferedFunction) Name() string { return "global_buffered" }

// Metadata returns descriptive metadata for global_buffered.
func (f *GlobalBufferedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Global-registration probe (table-buffering)",
		Stability:   vgi.StabilityConsistent,
		Categories:  probeCategories,
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT * FROM vgi_example_global_buffered((SELECT 1 AS x))",
				Description: "Buffering probe published into system.main",
			},
		},
	}
}

// ArgumentSpecs declares the single TABLE-typed input argument.
func (f *GlobalBufferedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"}}
}

// OnBind sets the output schema to the input schema (passthrough).
func (f *GlobalBufferedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindInputSchema(params)
}

// Process appends the batch to the shared state log and returns the execution ID.
func (f *GlobalBufferedFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) ([]byte, error) {
	data, err := vgi.SerializeRecordBatch(batch)
	if err != nil {
		return nil, err
	}
	if _, err := params.Storage.StateAppend(globalBufKey, data); err != nil {
		return nil, err
	}
	return params.ExecutionID, nil
}

// Combine collapses every state ID — they all name one log — to one stream.
func (f *GlobalBufferedFunction) Combine(ctx context.Context, params *vgi.ProcessParams, stateIDs [][]byte) ([][]byte, error) {
	return [][]byte{params.ExecutionID}, nil
}

// Finalize replays every buffered batch in the order it was appended.
func (f *GlobalBufferedFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, finalizeStateID []byte) ([]arrow.RecordBatch, error) {
	entries, err := params.Storage.StateLogScan(globalBufKey, -1, 0)
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
