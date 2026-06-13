// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package accumulate is a Go port of vgi-python's `accumulate` test fixture: a
// per-ATTACH, name-keyed row accumulator backed by attach-scoped FunctionStorage.
//
//   - accumulate(name, <rows>, ttl, max_row_size, result) — append rows to a
//     named collection (stamping one call-time _timestamp) and optionally
//     return its contents. A table-buffering (sink→combine→source) function.
//   - accumulate_read(name) — read a collection's rows without modifying it.
//   - accumulate_clear(name) — drop a collection; returns rows removed.
//
// Collections persist across queries via vgi.AttachStore (scoped to the random
// per-ATTACH id), so they survive the fresh worker a subprocess-transport query
// spawns, and two independent ATTACH sessions never share a collection.
package accumulate

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// CatalogName is the catalog the accumulate functions live under.
const CatalogName = "accumulate"

// DataVersion is advertised by vgi_catalogs() for the accumulate catalog.
const DataVersion = "2.0.0"

// tsColumn is appended to every output row, holding the per-call ingest time.
// A tz-naive microsecond timestamp surfaces as DuckDB TIMESTAMP (not TIMESTAMP
// WITH TIME ZONE). Underscore-prefixed to avoid colliding with user columns.
const tsColumn = "_timestamp"

const maxNameBytes = 255

// resultAll/New/None are the choices for the `result` named parameter.
const (
	resultAll  = "all"
	resultNew  = "new"
	resultNone = "none"
)

// Execution-scoped staging keys (transient per query) for the buffering
// operator's sink→combine→source handoff and for accumulate_read.
var (
	nsIn   = []byte("acc.stage.in")
	nsOut  = []byte("acc.stage.out")
	nsRead = []byte("acc.stage.read")
)

func tsType() *arrow.TimestampType { return &arrow.TimestampType{Unit: arrow.Microsecond} }

func validateName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("collection name must be a non-empty string")
	}
	if len(name) > maxNameBytes {
		return fmt.Errorf("collection name must be at most %d bytes", maxNameBytes)
	}
	return nil
}

// outputSchemaOf returns input schema + the appended _timestamp field.
func outputSchemaOf(input *arrow.Schema) *arrow.Schema {
	fields := make([]arrow.Field, 0, input.NumFields()+1)
	fields = append(fields, input.Fields()...)
	fields = append(fields, arrow.Field{Name: tsColumn, Type: tsType()})
	return arrow.NewSchema(fields, nil)
}

// inputFieldsOf returns the input portion (everything but _timestamp) of a
// pinned output schema.
func inputFieldsOf(output *arrow.Schema) []arrow.Field {
	out := make([]arrow.Field, 0, output.NumFields())
	for _, f := range output.Fields() {
		if f.Name != tsColumn {
			out = append(out, f)
		}
	}
	return out
}

// inputFieldsMatch reports whether the pinned input fields equal the incoming
// input schema (names + types, ignoring metadata).
func inputFieldsMatch(pinned []arrow.Field, incoming *arrow.Schema) bool {
	if len(pinned) != incoming.NumFields() {
		return false
	}
	for i, f := range pinned {
		g := incoming.Field(i)
		if f.Name != g.Name || !arrow.TypeEqual(f.Type, g.Type) {
			return false
		}
	}
	return true
}

// timestampArray builds an n-row tz-naive microsecond timestamp array.
func timestampArray(n int64, micros int64) arrow.Array {
	b := array.NewTimestampBuilder(memory.DefaultAllocator, tsType())
	defer b.Release()
	for i := int64(0); i < n; i++ {
		b.Append(arrow.Timestamp(micros))
	}
	return b.NewArray()
}

// stampBatch returns input's columns plus a _timestamp column of `micros`.
func stampBatch(input arrow.RecordBatch, outSchema *arrow.Schema, micros int64) arrow.RecordBatch {
	cols := make([]arrow.Array, 0, input.NumCols()+1)
	for i := 0; i < int(input.NumCols()); i++ {
		cols = append(cols, input.Column(i))
	}
	ts := timestampArray(input.NumRows(), micros)
	cols = append(cols, ts)
	out := array.NewRecordBatch(outSchema, cols, input.NumRows())
	ts.Release() // NewRecordBatch retains its columns
	return out
}

func namedString(args *vgi.Arguments, key, def string) string {
	if args == nil {
		return def
	}
	if _, err := args.GetColumn(key); err != nil {
		return def
	}
	if args.IsNull(key) {
		return def
	}
	if v, err := args.GetScalarString(key); err == nil {
		return v
	}
	return def
}

func namedInt64(args *vgi.Arguments, key string, def int64) int64 {
	if args == nil {
		return def
	}
	if _, err := args.GetColumn(key); err != nil {
		return def
	}
	if args.IsNull(key) {
		return def
	}
	if v, err := args.GetScalarInt64(key); err == nil {
		return v
	}
	return def
}

// namedInterval returns the ttl duration and whether it was supplied.
func namedInterval(args *vgi.Arguments, key string) (time.Duration, bool) {
	if args == nil {
		return 0, false
	}
	if _, err := args.GetColumn(key); err != nil {
		return 0, false
	}
	if args.IsNull(key) {
		return 0, false
	}
	if d, err := args.GetScalarDuration(key); err == nil {
		return d, true
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// accumulate(name, <rows>, ttl, max_row_size, result)
// ---------------------------------------------------------------------------

// AccumulateFunction appends input rows to a named collection and returns the
// collection (or just the new rows, or nothing). It is a table-buffering
// function: input is staged across the parallel sink, Combine runs once to
// stamp the rows with a single timestamp, append them to the persistent
// collection, apply ttl/max_row_size, and stage the result; Finalize streams it.
type AccumulateFunction struct{}

var _ vgi.TableBufferingFunction = (*AccumulateFunction)(nil)

func (*AccumulateFunction) Name() string { return "accumulate" }

func (*AccumulateFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Append rows to a named collection; return all/new/no rows with a _timestamp column",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"stateful", "utility"},
	}
}

func (*AccumulateFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "name", Position: 0, ArrowType: "varchar", IsConst: true, ArrowDataType: arrow.BinaryTypes.String, Doc: "Name of the collection to accumulate into"},
		{Name: "data", Position: 1, ArrowType: "table", Doc: "Rows to accumulate (any table expression)"},
		{Name: "ttl", Position: -1, IsConst: true, HasDefault: true, ArrowType: "interval", ArrowDataType: arrow.FixedWidthTypes.MonthDayNanoInterval, Doc: "Evict rows older than this INTERVAL before returning (months treated as 30 days)"},
		{Name: "max_row_size", Position: -1, IsConst: true, HasDefault: true, DefaultValue: "0", ArrowType: "int64", ArrowDataType: arrow.PrimitiveTypes.Int64, Doc: "Maximum rows retained per name; oldest dropped first (0 = unlimited)"},
		{Name: "result", Position: -1, IsConst: true, HasDefault: true, DefaultValue: resultAll, ArrowType: "varchar", ArrowDataType: arrow.BinaryTypes.String, Doc: "What to return: 'all' (default), 'new', or 'none'"},
	}
}

func (*AccumulateFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	name, err := params.Args.GetScalarString(0)
	if err != nil {
		return nil, fmt.Errorf("collection name must be a non-empty string")
	}
	if err := validateName(name); err != nil {
		return nil, err
	}
	input := params.InputSchema
	if input == nil {
		return nil, fmt.Errorf("accumulate requires a table input")
	}
	for _, f := range input.Fields() {
		if f.Name == tsColumn {
			return nil, fmt.Errorf("input may not contain a reserved '%s' column; accumulate adds this column to its output", tsColumn)
		}
	}
	out := outputSchemaOf(input)

	st, err := params.AttachStore()
	if err != nil {
		return nil, err
	}
	// Lock-free schema pin: read the pinned schema, write it if absent, or
	// reject a mismatch against the input portion.
	existing, err := getSchema(st, name)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		if err := putSchema(st, name, out); err != nil {
			return nil, err
		}
	} else if !inputFieldsMatch(inputFieldsOf(existing), input) {
		return nil, fmt.Errorf("input schema for accumulate('%s', ...) does not match the schema already accumulated under that name", name)
	}
	return &vgi.BindResponse{OutputSchema: out}, nil
}

func (*AccumulateFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) ([]byte, error) {
	data, err := vgi.SerializeRecordBatch(batch)
	if err != nil {
		return nil, err
	}
	if _, err := params.Storage.StateAppend(nsIn, data); err != nil {
		return nil, err
	}
	return params.ExecutionID, nil
}

func (*AccumulateFunction) Combine(ctx context.Context, params *vgi.ProcessParams, stateIDs [][]byte) ([][]byte, error) {
	name, err := params.Args.GetScalarString(0)
	if err != nil {
		return nil, err
	}
	st, err := params.Storage.AttachStore(params.AttachScope)
	if err != nil {
		return nil, err
	}

	// Reassemble this call's input from the execution-scoped staging log.
	staged, err := params.Storage.StateLogScan(nsIn, -1, 0)
	if err != nil {
		return nil, err
	}
	callMicros := time.Now().UTC().UnixMicro()

	// Stamp each staged input batch with the single call timestamp, store it as
	// a segment, and (for result='new') keep the stamped batches to stage.
	var newBatches []arrow.RecordBatch
	defer func() {
		for _, b := range newBatches {
			b.Release()
		}
	}()
	for _, e := range staged {
		in, err := vgi.DeserializeRecordBatch(e.Value)
		if err != nil {
			return nil, err
		}
		if in.NumRows() == 0 {
			in.Release()
			continue
		}
		outSchema := outputSchemaOf(in.Schema())
		stamped := stampBatch(in, outSchema, callMicros)
		in.Release()
		if err := appendSegment(st, name, stamped, callMicros); err != nil {
			stamped.Release()
			return nil, err
		}
		newBatches = append(newBatches, stamped)
	}

	// Eviction: ttl first (drop rows older than call_time - ttl), then the row cap.
	if ttl, ok := namedInterval(params.Args, "ttl"); ok {
		cutoff := callMicros - ttl.Microseconds()
		if err := evictTTL(st, name, cutoff); err != nil {
			return nil, err
		}
	}
	if maxRows := namedInt64(params.Args, "max_row_size", 0); maxRows > 0 {
		if err := evictMaxRows(st, name, maxRows); err != nil {
			return nil, err
		}
	}

	// Stage the requested result for the source phase.
	switch namedString(params.Args, "result", resultAll) {
	case resultNone:
		// nothing
	case resultNew:
		for _, b := range newBatches {
			if err := stageTo(params.Storage, nsOut, b); err != nil {
				return nil, err
			}
		}
	default: // resultAll
		all, err := readCollection(st, name)
		if err != nil {
			return nil, err
		}
		for _, b := range all {
			if err := stageTo(params.Storage, nsOut, b); err != nil {
				for _, rb := range all {
					rb.Release()
				}
				return nil, err
			}
		}
		for _, b := range all {
			b.Release()
		}
	}
	return [][]byte{params.ExecutionID}, nil
}

// stageTo appends one batch to an execution-scoped staging log under key.
func stageTo(storage *vgi.ExecutionStorage, key []byte, b arrow.RecordBatch) error {
	data, err := vgi.SerializeRecordBatch(b)
	if err != nil {
		return err
	}
	_, err = storage.StateAppend(key, data)
	return err
}

func (*AccumulateFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, finalizeStateID []byte) ([]arrow.RecordBatch, error) {
	entries, err := params.Storage.StateLogScan(nsOut, -1, 0)
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

// ---------------------------------------------------------------------------
// accumulate_read(name)
// ---------------------------------------------------------------------------

// AccumulateReadFunction returns a collection's rows without modifying it.
type AccumulateReadFunction struct{}

var _ vgi.TypedTableFunc[accReadState] = (*AccumulateReadFunction)(nil)

func (*AccumulateReadFunction) Name() string { return "accumulate_read" }

func (*AccumulateReadFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Read an accumulated collection's rows without modifying it",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"stateful", "utility"},
	}
}

func (*AccumulateReadFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "name", Position: 0, ArrowType: "varchar", IsConst: true, ArrowDataType: arrow.BinaryTypes.String, Doc: "Name of the collection to read"},
	}
}

func (*AccumulateReadFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	name, err := params.Args.GetScalarString(0)
	if err != nil {
		return nil, fmt.Errorf("collection name must be a non-empty string")
	}
	if err := validateName(name); err != nil {
		return nil, err
	}
	st, err := params.AttachStore()
	if err != nil {
		return nil, err
	}
	schema, err := getSchema(st, name)
	if err != nil {
		return nil, err
	}
	if schema == nil {
		return nil, fmt.Errorf("no accumulation named '%s' in this session", name)
	}
	return vgi.BindSchema(schema)
}

// accReadState is gob-encoded between ticks (HTTP/cross-process), so it holds
// only the drain cursor — never live Arrow batches. The snapshot is staged into
// the execution-scoped log on the first tick and drained one batch per tick.
type accReadState struct {
	Staged  bool
	AfterID int64
}

func (*AccumulateReadFunction) NewState(params *vgi.ProcessParams) (*accReadState, error) {
	return &accReadState{AfterID: -1}, nil
}

func (*AccumulateReadFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *accReadState, out *vgirpc.OutputCollector) error {
	if !state.Staged {
		name, err := params.Args.GetScalarString(0)
		if err != nil {
			return err
		}
		st, err := params.Storage.AttachStore(params.AttachScope)
		if err != nil {
			return err
		}
		batches, err := readCollection(st, name)
		if err != nil {
			return err
		}
		for _, b := range batches {
			if err := stageTo(params.Storage, nsRead, b); err != nil {
				b.Release()
				return err
			}
			b.Release()
		}
		state.Staged = true
	}
	rows, err := params.Storage.StateLogScan(nsRead, state.AfterID, 1)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		out.Finish()
		return nil
	}
	b, err := vgi.DeserializeRecordBatch(rows[0].Value)
	if err != nil {
		return err
	}
	state.AfterID = rows[0].ID
	return out.Emit(b)
}

// NewAccumulateReadFunction returns the registerable table function.
func NewAccumulateReadFunction() vgi.TableFunction {
	return vgi.AsTableFunction[accReadState](&AccumulateReadFunction{})
}

// ---------------------------------------------------------------------------
// accumulate_clear(name)
// ---------------------------------------------------------------------------

var clearSchema = arrow.NewSchema([]arrow.Field{
	{Name: "name", Type: arrow.BinaryTypes.String},
	{Name: "rows_cleared", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// AccumulateClearFunction removes a collection by name and reports rows removed.
type AccumulateClearFunction struct{}

var _ vgi.TypedTableFunc[accClearState] = (*AccumulateClearFunction)(nil)

func (*AccumulateClearFunction) Name() string { return "accumulate_clear" }

func (*AccumulateClearFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Remove an accumulated collection by name; returns rows cleared",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"stateful", "utility"},
	}
}

func (*AccumulateClearFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "name", Position: 0, ArrowType: "varchar", IsConst: true, ArrowDataType: arrow.BinaryTypes.String, Doc: "Name of the collection to clear"},
	}
}

func (*AccumulateClearFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	name, err := params.Args.GetScalarString(0)
	if err != nil {
		return nil, fmt.Errorf("collection name must be a non-empty string")
	}
	if err := validateName(name); err != nil {
		return nil, err
	}
	return vgi.BindSchema(clearSchema)
}

type accClearState struct {
	Done bool
}

func (*AccumulateClearFunction) NewState(params *vgi.ProcessParams) (*accClearState, error) {
	return &accClearState{}, nil
}

func (*AccumulateClearFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *accClearState, out *vgirpc.OutputCollector) error {
	if state.Done {
		out.Finish()
		return nil
	}
	name, err := params.Args.GetScalarString(0)
	if err != nil {
		return err
	}
	st, err := params.Storage.AttachStore(params.AttachScope)
	if err != nil {
		return err
	}
	rowsCleared, err := clearCollection(st, name)
	if err != nil {
		return err
	}
	names := vgi.BuildStringArray(1, func(int64) string { return name })
	counts := vgi.BuildInt64Array(1, func(int64) int64 { return rowsCleared })
	batch := array.NewRecordBatch(clearSchema, []arrow.Array{names, counts}, 1)
	defer batch.Release()
	state.Done = true
	return out.Emit(batch)
}

// NewAccumulateClearFunction returns the registerable table function.
func NewAccumulateClearFunction() vgi.TableFunction {
	return vgi.AsTableFunction[accClearState](&AccumulateClearFunction{})
}
