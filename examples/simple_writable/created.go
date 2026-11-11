// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package simple_writable

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Created tables (CREATE TABLE / CREATE TABLE ... AS SELECT).
//
// catalog_table_create in schema main creates a table with the request's
// columns next to the four fixed tables, in the attach's AttachStore. A
// created table is scannable (simple_writable_created_scan, table_only) and
// insert-only (simple_writable_created_insert, result modes up to rows); it
// has no row id. The reply's `created` (protocol 3.3) is true when the call
// created the table (REPLACE drops the old one and its rows first) and false
// when IGNORE (IF NOT EXISTS) found it; the extension's CTAS inserts nothing
// then. This is the vgi repo's docs/writable_tx_fixtures.md "simple_writable:
// created tables" contract.
//
// Storage: the column schema of each created table under ("sw:__created",
// name); its rows as serialized Arrow batches under ("swc:<name>", seq) with a
// per-table batch counter. Types are stored and returned as DuckDB sends them,
// so any column type round-trips.

var (
	createdNS     = []byte("sw:__created")
	createdSeqKey = []byte("batch_seq")
)

func createdRowsNS(table string) []byte { return []byte("swc:" + table) }

// backend is the FunctionStorage the catalog handlers open attach stores on
// (functions get theirs through ProcessParams/BindParams). Options sets it.
var backend struct {
	mu sync.RWMutex
	fs vgi.FunctionStorage
}

// attachStore opens the attach's store for a catalog handler.
func attachStore(attachOpaqueData []byte) (*vgi.AttachStore, error) {
	backend.mu.RLock()
	fs := backend.fs
	backend.mu.RUnlock()
	if fs == nil {
		return nil, fmt.Errorf("simple_writable: no function storage (use Options(storage))")
	}
	es := vgi.NewExecutionStorage()
	es.SetBackend(fs)
	return es.AttachStore(attachOpaqueData)
}

// createdSchema is a created table's columns, or nil when there is none.
func createdSchema(st *vgi.AttachStore, table string) (*arrow.Schema, error) {
	data, err := st.Get(createdNS, []byte(table))
	if err != nil || data == nil {
		return nil, err
	}
	return vgi.DeserializeSchema(data)
}

// createdTables lists the attach's created tables, in name order.
func createdTables(st *vgi.AttachStore) ([]string, error) {
	kvs, err := st.Scan(createdNS)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(kvs))
	for i, kv := range kvs {
		out[i] = string(kv.Key)
	}
	return out, nil
}

// dropCreated removes a created table and its rows.
func dropCreated(st *vgi.AttachStore, table string) error {
	if err := st.DeleteNS(createdRowsNS(table)); err != nil {
		return err
	}
	if err := st.CounterDelete(createdRowsNS(table), createdSeqKey); err != nil {
		return err
	}
	return st.DeleteKey(createdNS, []byte(table))
}

// createdTableInfo is a created table's TableInfo: its columns, no row id,
// INSERT with up to rows.
func createdTableInfo(table string, columns *arrow.Schema) ([]byte, error) {
	return vgi.SerializeTableInfo(&vgi.TableInfo{
		Name:             table,
		SchemaPath:       vgi.SchemaPath{SchemaPath},
		Columns:          columns,
		WriteResultModes: map[string]string{"insert": "rows"},
	})
}

// TableCreateHandler answers catalog_table_create (vgi.WithAttachTableCreateHandler).
func TableCreateHandler(req *vgi.TableCreateRequestWire) (created, handled bool, err error) {
	if !isOurs(req.AttachOpaqueData) {
		return false, false, nil
	}
	if !isSchema(req.SchemaPath) {
		return false, true, fmt.Errorf("simple_writable: tables can only be created in schema %q", SchemaPath)
	}
	if len(req.ConstraintDefinitions) > 0 {
		return false, true, fmt.Errorf("simple_writable: created tables do not support constraints")
	}
	columns, err := vgi.DeserializeSchema(req.Columns)
	if err != nil {
		return false, true, fmt.Errorf("simple_writable: catalog_table_create columns: %w", err)
	}
	st, err := attachStore(req.AttachOpaqueData)
	if err != nil {
		return false, true, err
	}
	existing, err := createdSchema(st, req.Name)
	if err != nil {
		return false, true, err
	}
	_, fixed := userSchemas[req.Name]
	if existing != nil || fixed {
		switch req.OnConflict {
		case "IGNORE":
			return false, true, nil
		case "REPLACE":
			if fixed {
				return false, true, fmt.Errorf("simple_writable: table %q is fixed and cannot be replaced", req.Name)
			}
			if err := dropCreated(st, req.Name); err != nil {
				return false, true, err
			}
		default:
			return false, true, fmt.Errorf("simple_writable: table %q already exists", req.Name)
		}
	}
	data, err := vgi.SerializeSchema(columns)
	if err != nil {
		return false, true, err
	}
	if err := st.Put(createdNS, []byte(req.Name), data); err != nil {
		return false, true, err
	}
	return true, true, nil
}

// TableDropHandler answers catalog_table_drop for created tables
// (vgi.WithAttachTableDropHandler). The fixed tables cannot be dropped.
func TableDropHandler(req *vgi.CatalogTableDropParams) (handled bool, err error) {
	if !isOurs(req.AttachOpaqueData) {
		return false, nil
	}
	if _, fixed := userSchemas[req.Name]; fixed && isSchema(req.SchemaPath) {
		return true, fmt.Errorf("simple_writable: table %q is fixed and cannot be dropped", req.Name)
	}
	st, err := attachStore(req.AttachOpaqueData)
	if err != nil {
		return true, err
	}
	var existing *arrow.Schema
	if isSchema(req.SchemaPath) {
		if existing, err = createdSchema(st, req.Name); err != nil {
			return true, err
		}
	}
	if existing == nil {
		if req.IgnoreNotFound {
			return true, nil
		}
		return true, fmt.Errorf("simple_writable: table %q does not exist", req.Name)
	}
	return true, dropCreated(st, req.Name)
}

// createdBindSchema resolves a created table's columns at bind.
func createdBindSchema(params *vgi.BindParams) (string, *arrow.Schema, error) {
	table, err := tableArg(params.Args)
	if err != nil {
		return "", nil, err
	}
	st, err := params.AttachStore()
	if err != nil {
		return "", nil, err
	}
	columns, err := createdSchema(st, table)
	if err != nil {
		return "", nil, err
	}
	if columns == nil {
		return "", nil, fmt.Errorf("simple_writable: unknown table %q", table)
	}
	return table, columns, nil
}

// ---------------------------------------------------------------------------
// simple_writable_created_scan(table)
// ---------------------------------------------------------------------------

type createdScanFn struct{}

var _ vgi.TypedTableFunc[scanState] = createdScanFn{}

func (createdScanFn) Name() string { return "simple_writable_created_scan" }
func (createdScanFn) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "Scan a created simple_writable table", Stability: vgi.StabilityConsistent}
}
func (createdScanFn) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "table_name", Position: 0, ArrowType: "varchar", IsConst: true, ArrowDataType: arrow.BinaryTypes.String, Doc: "Table to scan"},
	}
}
func (createdScanFn) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	_, columns, err := createdBindSchema(params)
	if err != nil {
		return nil, err
	}
	return vgi.BindSchema(columns)
}
func (createdScanFn) NewState(*vgi.ProcessParams) (*scanState, error) { return &scanState{}, nil }

// Process reads every stored batch once (a snapshot) and emits them as one
// batch in the bound schema.
func (createdScanFn) Process(_ context.Context, params *vgi.ProcessParams, state *scanState, out *vgirpc.OutputCollector) error {
	if state.Done {
		return out.Finish()
	}
	state.Done = true
	table, err := tableArg(params.Args)
	if err != nil {
		return err
	}
	st, err := params.Storage.AttachStore(params.AttachScope)
	if err != nil {
		return err
	}
	kvs, err := st.Scan(createdRowsNS(table))
	if err != nil {
		return err
	}
	schema := params.OutputSchema
	parts := make([][]arrow.Array, schema.NumFields())
	var rows int64
	for _, kv := range kvs {
		b, err := vgi.DeserializeRecordBatch(kv.Value)
		if err != nil {
			return err
		}
		defer b.Release()
		if int(b.NumCols()) != schema.NumFields() {
			return fmt.Errorf("simple_writable: stored batch of %q has %d columns, want %d", table, b.NumCols(), schema.NumFields())
		}
		for i := range parts {
			parts[i] = append(parts[i], b.Column(i))
		}
		rows += b.NumRows()
	}
	cols := make([]arrow.Array, schema.NumFields())
	for i, f := range schema.Fields() {
		if len(parts[i]) == 0 {
			cols[i] = array.MakeArrayOfNull(memory.DefaultAllocator, f.Type, 0)
			continue
		}
		c, err := array.Concatenate(parts[i], memory.DefaultAllocator)
		if err != nil {
			return fmt.Errorf("simple_writable: scan %q column %q: %w", table, f.Name, err)
		}
		cols[i] = c
	}
	batch := array.NewRecordBatch(schema, cols, rows)
	for _, c := range cols {
		c.Release()
	}
	return out.Emit(batch)
}

// ---------------------------------------------------------------------------
// simple_writable_created_insert(table, data)
// ---------------------------------------------------------------------------

type createdInsertFn struct{}

var _ vgi.TypedTableInOutFunc[noState] = createdInsertFn{}

func (createdInsertFn) Name() string { return "simple_writable_created_insert" }
func (createdInsertFn) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "INSERT into a created simple_writable table", Stability: vgi.StabilityConsistent}
}
func (createdInsertFn) ArgumentSpecs() []vgi.ArgSpec { return writeArgSpecs() }
func (createdInsertFn) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	_, columns, err := createdBindSchema(params)
	if err != nil {
		return nil, err
	}
	opts, err := vgi.ParseWriteOptions(params.Args)
	if err != nil {
		return nil, err
	}
	if opts.ResultMode == vgi.WriteResultChanges {
		return nil, fmt.Errorf("simple_writable: created tables return at most rows")
	}
	schema, err := vgi.WriteResultSchema(opts.ResultMode, columns)
	if err != nil {
		return nil, err
	}
	return vgi.BindSchema(schema)
}
func (createdInsertFn) NewState(*vgi.ProcessParams) (*noState, error) { return &noState{}, nil }

// Process stores the input batch (in the table's columns) and returns its
// count or its rows.
func (createdInsertFn) Process(_ context.Context, params *vgi.ProcessParams, _ *noState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	table, err := tableArg(params.Args)
	if err != nil {
		return err
	}
	st, err := params.Storage.AttachStore(params.AttachScope)
	if err != nil {
		return err
	}
	columns, err := createdSchema(st, table)
	if err != nil {
		return err
	}
	if columns == nil {
		return fmt.Errorf("simple_writable: unknown table %q", table)
	}
	if int(batch.NumCols()) != columns.NumFields() {
		return fmt.Errorf("simple_writable: insert into %q sent %d columns, want %d", table, batch.NumCols(), columns.NumFields())
	}
	for i, f := range columns.Fields() {
		if !arrow.TypeEqual(batch.Column(i).DataType(), f.Type) {
			return fmt.Errorf("simple_writable: insert into %q column %q is %s, want %s", table, f.Name, batch.Column(i).DataType(), f.Type)
		}
	}
	rows := array.NewRecordBatch(columns, batch.Columns(), batch.NumRows())
	defer rows.Release()
	if rows.NumRows() > 0 {
		data, err := vgi.SerializeRecordBatch(rows)
		if err != nil {
			return err
		}
		seq, err := st.CounterAdd(createdRowsNS(table), createdSeqKey, 1)
		if err != nil {
			return err
		}
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(seq))
		if err := st.Put(createdRowsNS(table), key, data); err != nil {
			return err
		}
	}
	opts, err := vgi.ParseWriteOptions(params.Args)
	if err != nil {
		return err
	}
	if opts.ResultMode == vgi.WriteResultRows {
		rows.Retain()
		return out.Emit(rows)
	}
	return out.Emit(vgi.WriteCountBatch(rows.NumRows()))
}
func (createdInsertFn) Finalize(context.Context, *vgi.ProcessParams, *noState) ([]arrow.RecordBatch, error) {
	return nil, nil
}
