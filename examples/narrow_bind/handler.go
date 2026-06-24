// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package narrow_bind is the cross-language reproducer for a VGI worker whose
// bind output_schema disagrees with the table's advertised columns. Two
// virtual tables live in the narrow_bind catalog:
//
//   - mismatch   — advertises columns {id, val} in its catalog listing but its
//     scan function narrow_scan binds to {id} only. This is the inconsistency
//     that used to walk off the end of the worker's 1-column batch in
//     ArrowTableFunction::ArrowToDuckDB (a hard client SIGSEGV). The client
//     must now refuse it at bind with a clear BinderException.
//
//   - consistent — advertises {id, val} and its scan function wide_scan binds
//     to {id, val}. Positive control: this must keep working unchanged.
//
// Backs test/sql/integration/narrow_bind_mismatch.test in the vgi extension.
package narrow_bind

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// CatalogName is the SQL catalog name this fixture publishes.
const CatalogName = "narrow_bind"

// SchemaName is the only schema this fixture publishes.
const SchemaName = "main"

// tableSchema is what the catalog advertises for both tables: two columns.
var tableSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "val", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
}, nil)

// narrowBindSchema is what the narrow scan function actually binds to: one
// column. The deliberate disagreement with tableSchema is the bug under test.
var narrowBindSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
}, nil)

// tableFunctions maps each advertised table to the scan function that backs it.
// Both tables advertise tableSchema (2 cols); narrow_scan binds to 1 col.
var tableFunctions = map[string]string{
	"mismatch":   "narrow_scan",
	"consistent": "wide_scan",
}

// scanState tracks whether the single output batch has been emitted.
type scanState struct {
	Emitted bool
}

// ============================================================================
// narrow_scan — binds to a NARROWER schema than the catalog advertises (bug).
// ============================================================================

type narrowScan struct{}

var _ vgi.TypedTableFunc[scanState] = (*narrowScan)(nil)

func (narrowScan) Name() string { return "narrow_scan" }
func (narrowScan) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "bind reports a narrower schema than the table advertises",
		Stability:   vgi.StabilityConsistent,
	}
}
func (narrowScan) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "rows", IsConst: true},
	}
}
func (narrowScan) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: narrowBindSchema}, nil
}
func (narrowScan) Cardinality(_ *vgi.BindParams) (*vgi.TableCardinality, error) {
	return &vgi.TableCardinality{Estimate: -1, Max: -1}, nil
}
func (narrowScan) OnInit(_ *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}
func (narrowScan) NewState(_ *vgi.ProcessParams) (*scanState, error) { return &scanState{}, nil }
func (narrowScan) Process(_ context.Context, params *vgi.ProcessParams, state *scanState, out *vgirpc.OutputCollector) error {
	// In practice the client refuses this scan at bind (1 col vs 2 advertised),
	// so Process is never reached. Emit a single {id} batch defensively.
	if state.Emitted {
		return out.Finish()
	}
	state.Emitted = true
	// Emit takes ownership of the batch reference; do not release it here.
	if err := out.Emit(buildBatch(params.OutputSchema, []int64{0, 1, 2}, nil)); err != nil {
		return err
	}
	return out.Finish()
}

// ============================================================================
// wide_scan — binds to the full advertised schema (positive control).
// ============================================================================

type wideScan struct{}

var _ vgi.TypedTableFunc[scanState] = (*wideScan)(nil)

func (wideScan) Name() string { return "wide_scan" }
func (wideScan) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "bind matches the table's advertised schema",
		Stability:   vgi.StabilityConsistent,
	}
}
func (wideScan) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "rows", IsConst: true},
	}
}
func (wideScan) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: tableSchema}, nil
}
func (wideScan) Cardinality(_ *vgi.BindParams) (*vgi.TableCardinality, error) {
	return &vgi.TableCardinality{Estimate: -1, Max: -1}, nil
}
func (wideScan) OnInit(_ *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}
func (wideScan) NewState(_ *vgi.ProcessParams) (*scanState, error) { return &scanState{}, nil }
func (wideScan) Process(_ context.Context, params *vgi.ProcessParams, state *scanState, out *vgirpc.OutputCollector) error {
	if state.Emitted {
		return out.Finish()
	}
	state.Emitted = true
	// Emit takes ownership of the batch reference; do not release it here.
	if err := out.Emit(buildBatch(params.OutputSchema, []int64{0, 1, 2}, []int64{10, 20, 30})); err != nil {
		return err
	}
	return out.Finish()
}

// buildBatch assembles a batch matching schema. It carries an "id" column
// always and a "val" column when vals is non-nil, projecting to whatever
// columns the (possibly pruned) output schema requests.
func buildBatch(schema *arrow.Schema, ids, vals []int64) arrow.RecordBatch {
	mem := memory.NewGoAllocator()
	cols := make([]arrow.Array, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		b := array.NewInt64Builder(mem)
		switch schema.Field(i).Name {
		case "id":
			b.AppendValues(ids, nil)
		case "val":
			if vals != nil {
				b.AppendValues(vals, nil)
			} else {
				for range ids {
					b.AppendNull()
				}
			}
		default:
			for range ids {
				b.AppendNull()
			}
		}
		cols[i] = b.NewArray()
		b.Release()
	}
	batch := array.NewRecordBatch(schema, cols, int64(len(ids)))
	for _, c := range cols {
		c.Release()
	}
	return batch
}

// ============================================================================
// Catalog wiring — the narrow_bind catalog advertises mismatch/consistent and
// routes each table's SELECT to its scan function via the attach handlers.
// ============================================================================

func serializeTableInfo(name string) ([]byte, error) {
	info := &vgi.TableInfo{
		Name:       name,
		SchemaName: SchemaName,
		Comment:    "narrow-bind reproducer table -> " + tableFunctions[name],
		Columns:    tableSchema,
	}
	return vgi.SerializeTableInfo(info)
}

// SchemaContentsHandler returns the table list for the narrow_bind catalog.
// Wired via vgi.WithSchemaContentsHandler (composed with other fixtures).
func SchemaContentsHandler(attachOpaqueData []byte, schemaName string) ([]vgi.SerializedSchemaItem, bool) {
	if string(attachOpaqueData) != CatalogName || schemaName != SchemaName {
		return nil, false
	}
	items := make([]vgi.SerializedSchemaItem, 0, len(tableFunctions))
	for _, name := range []string{"consistent", "mismatch"} {
		data, err := serializeTableInfo(name)
		if err != nil {
			continue
		}
		items = append(items, data)
	}
	return items, true
}

// AttachTableGetHandler answers single-table catalog_table_get RPCs for the
// narrow_bind catalog. Wired via vgi.WithAttachTableGetHandler.
func AttachTableGetHandler(attachOpaqueData []byte, schemaName, name string, _, _ *string) ([]byte, bool, error) {
	if string(attachOpaqueData) != CatalogName || schemaName != SchemaName {
		return nil, false, nil
	}
	if _, ok := tableFunctions[name]; !ok {
		return nil, false, nil
	}
	data, err := serializeTableInfo(name)
	if err != nil {
		return nil, true, err
	}
	return data, true, nil
}

// AttachScanFunctionGetHandler routes SELECT-time scan-function lookups to the
// table's backing scan function (mismatch->narrow_scan, consistent->wide_scan).
// Wired via vgi.WithAttachScanFunctionGetHandler.
func AttachScanFunctionGetHandler(attachOpaqueData []byte, schemaName, name string, _, _ *string) (*vgi.ScanFunctionResult, bool, error) {
	if string(attachOpaqueData) != CatalogName || schemaName != SchemaName {
		return nil, false, nil
	}
	fn, ok := tableFunctions[name]
	if !ok {
		return nil, false, nil
	}
	return &vgi.ScanFunctionResult{
		FunctionName: fn,
		PositionalArguments: []vgi.ScanArg{
			{Value: int64(3), Type: arrow.PrimitiveTypes.Int64},
		},
	}, true, nil
}

// RegisterAll registers the two scan functions on the worker, scoped to the
// narrow_bind catalog so they don't clutter the example catalog's listing.
func RegisterAll(w *vgi.Worker) {
	w.RegisterTableForCatalog(CatalogName, vgi.AsTableFunction[scanState](narrowScan{}))
	w.RegisterTableForCatalog(CatalogName, vgi.AsTableFunction[scanState](wideScan{}))
}
