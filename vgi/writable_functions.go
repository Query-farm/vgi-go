// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Function names registered with DuckDB so the catalog_table_*_function_get
// RPCs can return them as the table-function indirection target.
const (
	writableScanFunctionName   = "vgi_writable_scan"
	writableInsertFunctionName = "vgi_writable_insert"
	writableUpdateFunctionName = "vgi_writable_update"
	writableDeleteFunctionName = "vgi_writable_delete"
)

// registerWritableFunctions registers the four generic writable table
// functions on the worker. Called automatically when the first writable
// catalog is registered.
func (w *Worker) registerWritableFunctions() {
	if _, ok := w.tables[writableScanFunctionName]; ok {
		return
	}
	w.RegisterTable(AsTableFunction[writableScanState](&writableScanFn{w: w}))
	w.RegisterTableInOut(&writableInsertFn{w: w})
	w.RegisterTableInOut(&writableUpdateFn{w: w})
	w.RegisterTableInOut(&writableDeleteFn{w: w})
}

// findWritableTable looks up a (schema, table) across all writable catalogs.
// Reads from the SQLite store so DuckDB-spawned worker subprocesses see the
// same state as the process that ran CREATE TABLE.
func (w *Worker) findWritableTable(schemaName, tableName string) (*WritableCatalog, *writableTable, error) {
	for _, c := range w.extraCatalogs {
		t, err := c.store.tableLoad(c.Name, schemaName, tableName)
		if err != nil {
			return nil, nil, err
		}
		if t != nil {
			return c, t, nil
		}
	}
	return nil, nil, fmt.Errorf("writable table %q.%q not found", schemaName, tableName)
}

// ---------------------------------------------------------------------------
// vgi_writable_scan — emit all rows from a writable table.
// ---------------------------------------------------------------------------

type writableScanFn struct{ w *Worker }

var _ TypedTableFunc[writableScanState] = (*writableScanFn)(nil)

// Name returns the registered function name for the writable scan function.
func (f *writableScanFn) Name() string { return writableScanFunctionName }

// Metadata reports the function as a volatile, internal writable table function
// with projection pushdown enabled.
func (f *writableScanFn) Metadata() FunctionMetadata {
	return FunctionMetadata{
		Description:        "Generic scan over a writable VGI table",
		Stability:          StabilityVolatile,
		ProjectionPushdown: true,
		Categories:         []string{"writable", "internal"},
	}
}

// ArgumentSpecs declares the constant schema_name and table_name arguments
// identifying the writable table to scan.
func (f *writableScanFn) ArgumentSpecs() []ArgSpec {
	return []ArgSpec{
		{Name: "schema_name", Position: 0, ArrowType: "varchar", Doc: "Schema name", IsConst: true},
		{Name: "table_name", Position: 1, ArrowType: "varchar", Doc: "Table name", IsConst: true},
	}
}

// OnBind looks up the target table and binds the output to its schema plus a
// synthesized row-ID column.
func (f *writableScanFn) OnBind(params *BindParams) (*BindResponse, error) {
	schemaName, _ := params.Args.GetScalarString(0)
	tableName, _ := params.Args.GetScalarString(1)
	_, t, err := f.w.findWritableTable(schemaName, tableName)
	if err != nil {
		return nil, err
	}
	return BindSchema(withSynthesizedRowID(t.schema))
}

// Cardinality estimates the result size from the table's stored row count,
// returning zero if the table cannot be found.
func (f *writableScanFn) Cardinality(params *BindParams) (*TableCardinality, error) {
	schemaName, _ := params.Args.GetScalarString(0)
	tableName, _ := params.Args.GetScalarString(1)
	c, _, err := f.w.findWritableTable(schemaName, tableName)
	if err != nil {
		return &TableCardinality{Estimate: 0}, nil
	}
	n, err := c.store.rowsCount(c.Name, schemaName, tableName)
	if err != nil {
		return &TableCardinality{Estimate: 0}, nil
	}
	return &TableCardinality{Estimate: n, Max: n}, nil
}

type writableScanState struct {
	Emitted bool // exported for gob round-trip via the framework
}

// NewState creates the per-scan state tracking whether rows have been emitted.
func (f *writableScanFn) NewState(params *ProcessParams) (*writableScanState, error) {
	return &writableScanState{}, nil
}

// Process emits all stored rows once as a single batch, honoring projection
// pushdown, then finishes the stream.
func (f *writableScanFn) Process(ctx context.Context, params *ProcessParams, state *writableScanState, out *vgirpc.OutputCollector) error {
	if state.Emitted {
		out.Finish()
		return nil
	}
	schemaName, _ := params.Args.GetScalarString(0)
	tableName, _ := params.Args.GetScalarString(1)
	c, t, err := f.w.findWritableTable(schemaName, tableName)
	if err != nil {
		return err
	}
	rows, err := c.store.rowsScan(c.Name, schemaName, tableName)
	if err != nil {
		return err
	}
	state.Emitted = true
	if len(rows) == 0 {
		out.Finish()
		return nil
	}
	// Honor projection pushdown: emit only the columns DuckDB asked for.
	outSchema := params.OutputSchema
	if outSchema == nil {
		outSchema = withSynthesizedRowID(t.schema)
	}
	batch, err := rowsToBatch(outSchema, rows)
	if err != nil {
		return err
	}
	defer batch.Release()
	if err := out.Emit(batch); err != nil {
		return err
	}
	out.Finish()
	return nil
}

// ---------------------------------------------------------------------------
// Shared helpers for writable insert/update/delete.
// ---------------------------------------------------------------------------

func writableCountSchema(name string) *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: name, Type: arrow.PrimitiveTypes.Int64}}, nil)
}

func writableCountBatch(name string, n int64) arrow.RecordBatch {
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.Append(n)
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(writableCountSchema(name), []arrow.Array{col}, 1)
}

type writableMutateState struct {
	SchemaName string
	TableName  string
	Count      int64
}

func writableArgs(args *Arguments) (string, string) {
	schemaName, _ := args.GetScalarString(0)
	tableName, _ := args.GetScalarString(1)
	return schemaName, tableName
}

func writableMetadata(desc string) FunctionMetadata {
	return FunctionMetadata{
		Description: desc,
		Stability:   StabilityVolatile,
		Categories:  []string{"writable", "internal"},
	}
}

func writableArgumentSpecs() []ArgSpec {
	return []ArgSpec{
		{Name: "schema_name", Position: 0, ArrowType: "varchar", IsConst: true},
		{Name: "table_name", Position: 1, ArrowType: "varchar", IsConst: true},
	}
}

// ---------------------------------------------------------------------------
// vgi_writable_insert
// ---------------------------------------------------------------------------

type writableInsertFn struct{ w *Worker }

// Name returns the registered function name for the writable insert function.
func (f *writableInsertFn) Name() string { return writableInsertFunctionName }

// Metadata reports the function as a volatile, internal writable table function.
func (f *writableInsertFn) Metadata() FunctionMetadata {
	return writableMetadata("Generic INSERT into writable VGI table")
}

// ArgumentSpecs declares the constant schema_name and table_name arguments.
func (f *writableInsertFn) ArgumentSpecs() []ArgSpec { return writableArgumentSpecs() }

// OnBind binds the output to a single-column "rows_inserted" count schema.
func (f *writableInsertFn) OnBind(p *BindParams) (*BindResponse, error) {
	return BindSchema(writableCountSchema("rows_inserted"))
}

// OnInit limits processing to a single worker to serialize mutations.
func (f *writableInsertFn) OnInit(p *InitParams) (*GlobalInitResponse, error) {
	return &GlobalInitResponse{MaxWorkers: 1}, nil
}

// NewState creates the per-call state holding the target schema and table names.
func (f *writableInsertFn) NewState(p *ProcessParams) (interface{}, error) {
	schemaName, tableName := writableArgs(p.Args)
	return &writableMutateState{SchemaName: schemaName, TableName: tableName}, nil
}

// Process appends the incoming batch rows to the table and emits the inserted
// row count.
func (f *writableInsertFn) Process(ctx context.Context, p *ProcessParams, state interface{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	st := state.(*writableMutateState)
	c, _, err := f.w.findWritableTable(st.SchemaName, st.TableName)
	if err != nil {
		return err
	}
	rows, err := batchToRows(batch)
	if err != nil {
		return err
	}
	if _, err := c.store.rowsAppend(c.Name, st.SchemaName, st.TableName, rows); err != nil {
		return err
	}
	st.Count += int64(len(rows))
	result := writableCountBatch("rows_inserted", int64(len(rows)))
	defer result.Release()
	return out.Emit(result)
}

// Finalize emits no additional batches; per-batch counts are returned inline.
func (f *writableInsertFn) Finalize(ctx context.Context, p *ProcessParams, state interface{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// vgi_writable_update
// ---------------------------------------------------------------------------

type writableUpdateFn struct{ w *Worker }

// Name returns the registered function name for the writable update function.
func (f *writableUpdateFn) Name() string { return writableUpdateFunctionName }

// Metadata reports the function as a volatile, internal writable table function.
func (f *writableUpdateFn) Metadata() FunctionMetadata {
	return writableMetadata("Generic UPDATE on writable VGI table")
}

// ArgumentSpecs declares the constant schema_name and table_name arguments.
func (f *writableUpdateFn) ArgumentSpecs() []ArgSpec { return writableArgumentSpecs() }

// OnBind binds the output to a single-column "rows_updated" count schema.
func (f *writableUpdateFn) OnBind(p *BindParams) (*BindResponse, error) {
	return BindSchema(writableCountSchema("rows_updated"))
}

// OnInit limits processing to a single worker to serialize mutations.
func (f *writableUpdateFn) OnInit(p *InitParams) (*GlobalInitResponse, error) {
	return &GlobalInitResponse{MaxWorkers: 1}, nil
}

// NewState creates the per-call state holding the target schema and table names.
func (f *writableUpdateFn) NewState(p *ProcessParams) (interface{}, error) {
	schemaName, tableName := writableArgs(p.Args)
	return &writableMutateState{SchemaName: schemaName, TableName: tableName}, nil
}

// Process applies the incoming batch rows as updates to the table and emits the
// updated row count.
func (f *writableUpdateFn) Process(ctx context.Context, p *ProcessParams, state interface{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	st := state.(*writableMutateState)
	c, _, err := f.w.findWritableTable(st.SchemaName, st.TableName)
	if err != nil {
		return err
	}
	updates, err := batchToRows(batch)
	if err != nil {
		return err
	}
	updated, err := c.store.rowsUpdate(c.Name, st.SchemaName, st.TableName, updates)
	if err != nil {
		return err
	}
	st.Count += updated
	result := writableCountBatch("rows_updated", updated)
	defer result.Release()
	return out.Emit(result)
}

// Finalize emits no additional batches; per-batch counts are returned inline.
func (f *writableUpdateFn) Finalize(ctx context.Context, p *ProcessParams, state interface{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// vgi_writable_delete
// ---------------------------------------------------------------------------

type writableDeleteFn struct{ w *Worker }

// Name returns the registered function name for the writable delete function.
func (f *writableDeleteFn) Name() string { return writableDeleteFunctionName }

// Metadata reports the function as a volatile, internal writable table function.
func (f *writableDeleteFn) Metadata() FunctionMetadata {
	return writableMetadata("Generic DELETE on writable VGI table")
}

// ArgumentSpecs declares the constant schema_name and table_name arguments.
func (f *writableDeleteFn) ArgumentSpecs() []ArgSpec { return writableArgumentSpecs() }

// OnBind binds the output to a single-column "rows_deleted" count schema.
func (f *writableDeleteFn) OnBind(p *BindParams) (*BindResponse, error) {
	return BindSchema(writableCountSchema("rows_deleted"))
}

// OnInit limits processing to a single worker to serialize mutations.
func (f *writableDeleteFn) OnInit(p *InitParams) (*GlobalInitResponse, error) {
	return &GlobalInitResponse{MaxWorkers: 1}, nil
}

// NewState creates the per-call state holding the target schema and table names.
func (f *writableDeleteFn) NewState(p *ProcessParams) (interface{}, error) {
	schemaName, tableName := writableArgs(p.Args)
	return &writableMutateState{SchemaName: schemaName, TableName: tableName}, nil
}

// Process deletes rows identified by their synthesized row IDs in the incoming
// batch and emits the deleted row count.
func (f *writableDeleteFn) Process(ctx context.Context, p *ProcessParams, state interface{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	st := state.(*writableMutateState)
	c, _, err := f.w.findWritableTable(st.SchemaName, st.TableName)
	if err != nil {
		return err
	}
	rows, err := batchToRows(batch)
	if err != nil {
		return err
	}
	rids := make([]int64, 0, len(rows))
	for _, r := range rows {
		if v, ok := r[rowIDFieldName]; ok {
			rids = append(rids, toInt64(v))
		}
	}
	deleted, err := c.store.rowsDelete(c.Name, st.SchemaName, st.TableName, rids)
	if err != nil {
		return err
	}
	st.Count += deleted
	result := writableCountBatch("rows_deleted", deleted)
	defer result.Release()
	return out.Emit(result)
}

// Finalize emits no additional batches; per-batch counts are returned inline.
func (f *writableDeleteFn) Finalize(ctx context.Context, p *ProcessParams, state interface{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}
