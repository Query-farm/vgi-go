// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// projection-pushdown reproducer table functions. Mirrors vgi-python's
// _test_fixtures/projection_repro/worker.py — a 12-column wide schema
// (kafka_consume shape) with three projection-aware variants and one
// multi-worker variant. Used by projection_pushdown_repro.test to verify
// that DuckDB's column-id mapping reads the correct wire positions when
// a worker emits the full FIXED_SCHEMA under projection.
var projReproWideSchema = arrow.NewSchema([]arrow.Field{
	{Name: "topic", Type: arrow.BinaryTypes.String},
	{Name: "partition", Type: arrow.PrimitiveTypes.Int32},
	{Name: "offset", Type: arrow.PrimitiveTypes.Int64},
	{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, Nullable: true},
	{Name: "timestamp_type", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "key", Type: arrow.BinaryTypes.Binary, Nullable: true},
	{Name: "key_string", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "key_schema_id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
	{Name: "value_string", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "value_schema_id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "headers", Type: arrow.ListOf(arrow.StructOf(
		arrow.Field{Name: "k", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "v", Type: arrow.BinaryTypes.Binary},
	))},
}, nil)

// buildProjReproColumn returns one column of the wide schema for rows
// [start, end). value_schema_id and key_schema_id are intentionally all-NULL
// so the projection-pushdown test can verify the column-id wire mapping.
func buildProjReproColumn(name string, start, end int64) arrow.Array {
	n := end - start
	switch name {
	case "topic":
		return vgi.BuildStringArray(n, func(_ int64) string { return "demo_topic" })
	case "partition":
		return vgi.BuildInt32Array(n, func(i int64) int32 { return int32((start + i) % 4) })
	case "offset":
		return vgi.BuildInt64Array(n, func(i int64) int64 { return start + i })
	case "key":
		return vgi.BuildBinaryArray(n, func(i int64) []byte { return []byte(fmt.Sprintf("k%d", start+i)) })
	case "key_string":
		return vgi.BuildStringArray(n, func(i int64) string { return fmt.Sprintf("k%d", start+i) })
	case "value":
		return vgi.BuildBinaryArray(n, func(i int64) []byte { return []byte(fmt.Sprintf("v%d", start+i)) })
	case "value_string":
		return vgi.BuildStringArray(n, func(i int64) string { return fmt.Sprintf("v%d", start+i) })
	case "timestamp", "timestamp_type", "key_schema_id", "value_schema_id":
		idx := projReproWideSchema.FieldIndices(name)[0]
		return vgi.BuildAllNullArray(projReproWideSchema.Field(idx).Type, n)
	case "headers":
		return buildEmptyHeadersList(n)
	}
	return nil
}

// buildEmptyHeadersList returns an n-row list<struct{k:string,v:binary}>
// where every row is an empty list (length 0). Matches Python's
// “"headers": []“ shape for the kafka-fixture wide schema.
func buildEmptyHeadersList(n int64) arrow.Array {
	mem := memory.NewGoAllocator()
	innerStruct := arrow.StructOf(
		arrow.Field{Name: "k", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "v", Type: arrow.BinaryTypes.Binary},
	)
	listBuilder := array.NewListBuilder(mem, innerStruct)
	defer listBuilder.Release()
	for i := int64(0); i < n; i++ {
		listBuilder.Append(true) // valid, empty list
	}
	return listBuilder.NewArray()
}

// projReproArgs is the shared typed argument schema for all proj_repro variants.
type projReproArgs struct {
	N int64 `vgi:"pos=0,doc=Number of rows"`
}

// ProjReproStrictFunction emits batches built from params.OutputSchema only.
type ProjReproStrictFunction struct{}

var _ vgi.TypedTableFunc[projReproState] = (*ProjReproStrictFunction)(nil)

type projReproState struct {
	vgi.BatchState
}

func (f *ProjReproStrictFunction) Name() string { return "proj_repro_strict" }
func (f *ProjReproStrictFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "projection-pushdown reproducer (strict params.output_schema)",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
	}
}
func (f *ProjReproStrictFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(projReproArgs{})
}
func (f *ProjReproStrictFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(projReproWideSchema)
}
func (f *ProjReproStrictFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	var args projReproArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: args.N, Max: args.N}, nil
}
func (f *ProjReproStrictFunction) NewState(params *vgi.ProcessParams) (*projReproState, error) {
	var args projReproArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &projReproState{BatchState: vgi.NewBatchState(args.N, args.N)}, nil
}
func (f *ProjReproStrictFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *projReproState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Index
		// Empty projection (count(*)): build a 0-column N-row batch by
		// returning no arrays — EmitArrays handles row_count via the size param.
		if params.OutputSchema.NumFields() == 0 {
			return nil, nil
		}
		arrs := make([]arrow.Array, params.OutputSchema.NumFields())
		for i := 0; i < params.OutputSchema.NumFields(); i++ {
			arrs[i] = buildProjReproColumn(params.OutputSchema.Field(i).Name, start, start+size)
		}
		return arrs, nil
	})
}
func NewProjReproStrictFunction() vgi.TableFunction {
	return vgi.AsTableFunction[projReproState](&ProjReproStrictFunction{})
}

// ProjReproFullSchemaFunction always emits the full FIXED_SCHEMA regardless
// of projection. The framework projects the result to OutputSchema; this is
// the "naive worker" pattern that exercised the column-id mapping bug.
type ProjReproFullSchemaFunction struct{}

var _ vgi.TypedTableFunc[projReproState] = (*ProjReproFullSchemaFunction)(nil)

func (f *ProjReproFullSchemaFunction) Name() string { return "proj_repro_full_schema" }
func (f *ProjReproFullSchemaFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "projection-pushdown reproducer (emits full FIXED_SCHEMA)",
		Stability:   vgi.StabilityConsistent,
		// Intentionally NOT declaring ProjectionPushdown=true — the test
		// exercises the framework's auto-project path: worker emits the
		// full FIXED_SCHEMA batch and the framework projects down to
		// whatever DuckDB requested, instead of trusting the worker to
		// observe params.OutputSchema itself.
	}
}
func (f *ProjReproFullSchemaFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(projReproArgs{})
}
func (f *ProjReproFullSchemaFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(projReproWideSchema)
}
func (f *ProjReproFullSchemaFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	var args projReproArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: args.N, Max: args.N}, nil
}
func (f *ProjReproFullSchemaFunction) NewState(params *vgi.ProcessParams) (*projReproState, error) {
	var args projReproArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &projReproState{BatchState: vgi.NewBatchState(args.N, args.N)}, nil
}
func (f *ProjReproFullSchemaFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *projReproState, out *vgirpc.OutputCollector) error {
	// Always emit all 12 columns (full FIXED_SCHEMA), then let the
	// framework project them to params.OutputSchema.
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Index
		arrs := make([]arrow.Array, projReproWideSchema.NumFields())
		for i := 0; i < projReproWideSchema.NumFields(); i++ {
			arrs[i] = buildProjReproColumn(projReproWideSchema.Field(i).Name, start, start+size)
		}
		return arrs, nil
	})
}
func NewProjReproFullSchemaFunction() vgi.TableFunction {
	return vgi.AsTableFunction[projReproState](&ProjReproFullSchemaFunction{})
}

// ProjReproChunkedFunction emits 2 rows per process() tick, mirroring
// kafka_consume's shard-queue pattern where each tick yields control.
type ProjReproChunkedFunction struct{}

var _ vgi.TypedTableFunc[projReproState] = (*ProjReproChunkedFunction)(nil)

func (f *ProjReproChunkedFunction) Name() string { return "proj_repro_chunked" }
func (f *ProjReproChunkedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "projection-pushdown reproducer (multi-tick, full FIXED_SCHEMA)",
		Stability:   vgi.StabilityConsistent,
		// See ProjReproFullSchemaFunction — same auto-project rationale.
	}
}
func (f *ProjReproChunkedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(projReproArgs{})
}
func (f *ProjReproChunkedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(projReproWideSchema)
}
func (f *ProjReproChunkedFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	var args projReproArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: args.N, Max: args.N}, nil
}
func (f *ProjReproChunkedFunction) NewState(params *vgi.ProcessParams) (*projReproState, error) {
	var args projReproArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &projReproState{BatchState: vgi.NewBatchState(args.N, 2)}, nil
}
func (f *ProjReproChunkedFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *projReproState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Index
		arrs := make([]arrow.Array, projReproWideSchema.NumFields())
		for i := 0; i < projReproWideSchema.NumFields(); i++ {
			arrs[i] = buildProjReproColumn(projReproWideSchema.Field(i).Name, start, start+size)
		}
		return arrs, nil
	})
}
func NewProjReproChunkedFunction() vgi.TableFunction {
	return vgi.AsTableFunction[projReproState](&ProjReproChunkedFunction{})
}

// ProjReproMultiWorkerFunction is the chunked variant declaring max_workers=4
// so DuckDB schedules multiple parallel scan threads (mirrors kafka_consume's
// 4-partition layout where the bug originally surfaced).
type ProjReproMultiWorkerFunction struct{}

var _ vgi.TypedTableFunc[projReproState] = (*ProjReproMultiWorkerFunction)(nil)

func (f *ProjReproMultiWorkerFunction) Name() string { return "proj_repro_multi_worker" }
func (f *ProjReproMultiWorkerFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "projection-pushdown reproducer (4 workers, multi-tick, full FIXED_SCHEMA)",
		Stability:   vgi.StabilityConsistent,
		// See ProjReproFullSchemaFunction — same auto-project rationale.
	}
}
func (f *ProjReproMultiWorkerFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(projReproArgs{})
}
func (f *ProjReproMultiWorkerFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(projReproWideSchema)
}
func (f *ProjReproMultiWorkerFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	var args projReproArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: args.N, Max: args.N}, nil
}
func (f *ProjReproMultiWorkerFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 4}, nil
}
func (f *ProjReproMultiWorkerFunction) NewState(params *vgi.ProcessParams) (*projReproState, error) {
	var args projReproArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &projReproState{BatchState: vgi.NewBatchState(args.N, 2)}, nil
}
func (f *ProjReproMultiWorkerFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *projReproState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Index
		arrs := make([]arrow.Array, projReproWideSchema.NumFields())
		for i := 0; i < projReproWideSchema.NumFields(); i++ {
			arrs[i] = buildProjReproColumn(projReproWideSchema.Field(i).Name, start, start+size)
		}
		return arrs, nil
	})
}
func NewProjReproMultiWorkerFunction() vgi.TableFunction {
	return vgi.AsTableFunction[projReproState](&ProjReproMultiWorkerFunction{})
}
