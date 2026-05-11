// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-rpc/vgirpc"
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

func (f *writableScanFn) Name() string { return writableScanFunctionName }

func (f *writableScanFn) Metadata() FunctionMetadata {
	return FunctionMetadata{
		Description:        "Generic scan over a writable VGI table",
		Stability:          StabilityVolatile,
		ProjectionPushdown: true,
		Categories:         []string{"writable", "internal"},
	}
}

func (f *writableScanFn) ArgumentSpecs() []ArgSpec {
	return []ArgSpec{
		{Name: "schema_name", Position: 0, ArrowType: "varchar", Doc: "Schema name", IsConst: true},
		{Name: "table_name", Position: 1, ArrowType: "varchar", Doc: "Table name", IsConst: true},
	}
}

func (f *writableScanFn) OnBind(params *BindParams) (*BindResponse, error) {
	schemaName, _ := params.Args.GetScalarString(0)
	tableName, _ := params.Args.GetScalarString(1)
	_, t, err := f.w.findWritableTable(schemaName, tableName)
	if err != nil {
		return nil, err
	}
	return BindSchema(withSynthesizedRowID(t.schema))
}

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

func (f *writableScanFn) NewState(params *ProcessParams) (*writableScanState, error) {
	return &writableScanState{}, nil
}

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

func (f *writableInsertFn) Name() string { return writableInsertFunctionName }
func (f *writableInsertFn) Metadata() FunctionMetadata {
	return writableMetadata("Generic INSERT into writable VGI table")
}
func (f *writableInsertFn) ArgumentSpecs() []ArgSpec { return writableArgumentSpecs() }
func (f *writableInsertFn) OnBind(p *BindParams) (*BindResponse, error) {
	return BindSchema(writableCountSchema("rows_inserted"))
}
func (f *writableInsertFn) OnInit(p *InitParams) (*GlobalInitResponse, error) {
	return &GlobalInitResponse{MaxWorkers: 1}, nil
}
func (f *writableInsertFn) NewState(p *ProcessParams) (interface{}, error) {
	schemaName, tableName := writableArgs(p.Args)
	return &writableMutateState{SchemaName: schemaName, TableName: tableName}, nil
}
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
func (f *writableInsertFn) Finalize(ctx context.Context, p *ProcessParams, state interface{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// vgi_writable_update
// ---------------------------------------------------------------------------

type writableUpdateFn struct{ w *Worker }

func (f *writableUpdateFn) Name() string { return writableUpdateFunctionName }
func (f *writableUpdateFn) Metadata() FunctionMetadata {
	return writableMetadata("Generic UPDATE on writable VGI table")
}
func (f *writableUpdateFn) ArgumentSpecs() []ArgSpec { return writableArgumentSpecs() }
func (f *writableUpdateFn) OnBind(p *BindParams) (*BindResponse, error) {
	return BindSchema(writableCountSchema("rows_updated"))
}
func (f *writableUpdateFn) OnInit(p *InitParams) (*GlobalInitResponse, error) {
	return &GlobalInitResponse{MaxWorkers: 1}, nil
}
func (f *writableUpdateFn) NewState(p *ProcessParams) (interface{}, error) {
	schemaName, tableName := writableArgs(p.Args)
	return &writableMutateState{SchemaName: schemaName, TableName: tableName}, nil
}
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
func (f *writableUpdateFn) Finalize(ctx context.Context, p *ProcessParams, state interface{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// vgi_writable_delete
// ---------------------------------------------------------------------------

type writableDeleteFn struct{ w *Worker }

func (f *writableDeleteFn) Name() string { return writableDeleteFunctionName }
func (f *writableDeleteFn) Metadata() FunctionMetadata {
	return writableMetadata("Generic DELETE on writable VGI table")
}
func (f *writableDeleteFn) ArgumentSpecs() []ArgSpec { return writableArgumentSpecs() }
func (f *writableDeleteFn) OnBind(p *BindParams) (*BindResponse, error) {
	return BindSchema(writableCountSchema("rows_deleted"))
}
func (f *writableDeleteFn) OnInit(p *InitParams) (*GlobalInitResponse, error) {
	return &GlobalInitResponse{MaxWorkers: 1}, nil
}
func (f *writableDeleteFn) NewState(p *ProcessParams) (interface{}, error) {
	schemaName, tableName := writableArgs(p.Args)
	return &writableMutateState{SchemaName: schemaName, TableName: tableName}, nil
}
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
func (f *writableDeleteFn) Finalize(ctx context.Context, p *ProcessParams, state interface{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}
