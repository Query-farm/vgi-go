// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package schema_reconcile is the cross-language reproducer fixture for
// the C++ extension's ReconcileBatchToSchema helper. Three writable tables
// (demo, ts_demo, struct_demo) share a common user-schema (id int64,
// ts timestamptz, nested struct, tags list-of-struct) but differ in their
// rowid type to exercise every reshape code path:
//
//   - demo        : rowid int64 NOT NULL
//   - ts_demo     : rowid timestamp[ms, tz=UTC] NOT NULL
//   - struct_demo : rowid struct{a int64 NOT NULL, b string nullable} NOT NULL
//
// Each table is exposed via the schema_reconcile catalog (alias of the
// example worker). Routing for SELECT/INSERT/UPDATE/DELETE goes through
// the schema_reconcile_{scan,insert,update,delete} table functions, with
// the table name passed as a positional argument.
//
// Row state is persisted in a per-process-group SQLite file so that the
// multiple subprocess workers DuckDB spawns under VGI_SYNC_INIT_GLOBAL=1
// see the same data without explicit synchronization.
package schema_reconcile

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	_ "modernc.org/sqlite"
)

// CatalogName is the SQL catalog name this fixture publishes.
const CatalogName = "schema_reconcile"

// SchemaName is the only schema this fixture publishes.
const SchemaName = "main"

// userFields is the user-facing column set shared by every table. The
// awkward types (NOT NULL primitives, ms+UTC timestamps, NOT NULL leaves
// inside structs and list-of-struct items) are precisely what DuckDB's
// Arrow round-trip cannot preserve — they exist to verify that the C++
// ReconcileBatchToSchema helper restores the worker-declared shape on
// every batch flowing through INSERT/UPDATE/DELETE/SELECT.
var userFields = []arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, Nullable: false},
	{Name: "nested", Type: arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "ts2", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, Nullable: true},
	), Nullable: false},
	{Name: "tags", Type: arrow.ListOfField(arrow.Field{
		Name: "item",
		Type: arrow.StructOf(
			arrow.Field{Name: "k", Type: arrow.BinaryTypes.String, Nullable: false},
			arrow.Field{Name: "v", Type: arrow.BinaryTypes.Binary, Nullable: true},
		),
		Nullable: false,
	}), Nullable: false},
}

// userSchema is the shared user-facing column set as a Schema.
var userSchema = arrow.NewSchema(userFields, nil)

// rowIDMeta marks the worker-declared rowid pseudocolumn; the C++
// extension keys on this metadata to build CreateTableInfo.row_id.
var rowIDMeta = arrow.NewMetadata([]string{"is_row_id"}, []string{""})

// tableSpec is one (table_name → rowid_type) pin. Three specs together
// cover every rowid reshape code path.
type tableSpec struct {
	name        string
	storageName string // SQLite table name; lowercase, underscore-friendly.
	rowidField  arrow.Field
}

// rowidField builds a "rowid" field declared NOT NULL with the is_row_id
// metadata the C++ extension looks for.
func makeRowidField(t arrow.DataType) arrow.Field {
	return arrow.Field{Name: "rowid", Type: t, Nullable: false, Metadata: rowIDMeta}
}

var (
	int64RowID  = makeRowidField(arrow.PrimitiveTypes.Int64)
	tsRowID     = makeRowidField(&arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"})
	structRowID = makeRowidField(arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	))
)

var tableSpecs = map[string]tableSpec{
	"demo":        {name: "demo", storageName: "demo_rows", rowidField: int64RowID},
	"ts_demo":     {name: "ts_demo", storageName: "ts_demo_rows", rowidField: tsRowID},
	"struct_demo": {name: "struct_demo", storageName: "struct_demo_rows", rowidField: structRowID},
}

// tableSchema returns the full table schema (user columns + rowid).
func (s tableSpec) tableSchema() *arrow.Schema {
	fields := make([]arrow.Field, 0, len(userFields)+1)
	fields = append(fields, userFields...)
	fields = append(fields, s.rowidField)
	return arrow.NewSchema(fields, nil)
}

// countSchema is the result schema for INSERT/UPDATE/DELETE handlers —
// they return one row carrying the affected-row count.
var countSchema = arrow.NewSchema([]arrow.Field{
	{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
}, nil)

// ============================================================================
// Storage — SQLite, one row-store table per logical table. The DB filename
// is keyed on the parent process group so all worker subprocesses spawned
// for one DuckDB process share state, while distinct test runs land on
// fresh files.
// ============================================================================

var storageMu sync.Mutex

func dbPath() string {
	if v := os.Getenv("VGI_SCHEMA_RECONCILE_DB"); v != "" {
		return v
	}
	// Use the parent PID (= the DuckDB process spawning every worker
	// subprocess) so all subprocesses for one test invocation share one
	// SQLite file. PGID would also work, but PPID is more direct: the
	// fork+exec chain preserves it for every direct child of DuckDB.
	return filepath.Join(os.TempDir(), fmt.Sprintf("vgi_schema_reconcile.%d.sqlite", os.Getppid()))
}

func openDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite", dbPath())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	// DELETE journal — the simplest mode that gives every reader a
	// consistent snapshot at open time, no checkpoint required. WAL would
	// be faster but every process needs the WAL file open at the same
	// time, which is fragile across short-lived subprocesses.
	if _, err := db.Exec("PRAGMA journal_mode=DELETE"); err != nil {
		// Best-effort; not fatal for correctness.
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		// Best-effort.
	}
	if _, err := db.Exec("PRAGMA synchronous=FULL"); err != nil {
		// Best-effort.
	}
	for _, spec := range tableSpecs {
		if _, err := db.Exec(fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s (rid_blob BLOB PRIMARY KEY, payload_blob BLOB NOT NULL)",
			spec.storageName,
		)); err != nil {
			db.Close()
			return nil, err
		}
	}
	return db, nil
}

// rowidKey returns a canonical lookup key for a rowid value at row i in the
// given column. We hash the IPC-encoded rowid array slice for primitive
// types; for struct rowids we hash each child column at i.
func rowidKey(col arrow.Array, i int) ([]byte, error) {
	h := fnv.New64a()
	hashScalar(h, col, i)
	var out [8]byte
	binary.LittleEndian.PutUint64(out[:], h.Sum64())
	return out[:], nil
}

func hashScalar(h hash.Hash64, col arrow.Array, i int) {
	if col.IsNull(i) {
		_, _ = h.Write([]byte{0xff, 0x00})
		return
	}
	switch a := col.(type) {
	case *array.Int64:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(a.Value(i)))
		_, _ = h.Write(b[:])
	case *array.Int32:
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], uint32(a.Value(i)))
		_, _ = h.Write(b[:])
	case *array.Timestamp:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(a.Value(i)))
		_, _ = h.Write(b[:])
	case *array.String:
		_, _ = h.Write([]byte(a.Value(i)))
	case *array.Binary:
		_, _ = h.Write(a.Value(i))
	case *array.Struct:
		for c := 0; c < a.NumField(); c++ {
			hashScalar(h, a.Field(c), i)
			_, _ = h.Write([]byte{0x00})
		}
	default:
		// Fallback: stringify
		_, _ = h.Write([]byte(fmt.Sprintf("%v", a)))
	}
	_, _ = h.Write([]byte{0xfe})
}

// ============================================================================
// Wire helpers — serialize/deserialize a single batch row through IPC.
// ============================================================================

func serializeBatch(b arrow.RecordBatch) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(b.Schema()))
	if err := w.Write(b); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func deserializeBatch(data []byte) (arrow.RecordBatch, error) {
	r, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Release()
	if !r.Next() {
		return nil, fmt.Errorf("schema_reconcile: empty payload")
	}
	b := r.RecordBatch()
	b.Retain()
	return b, nil
}

// ============================================================================
// Insert handler
// ============================================================================

// insertFn is the table-in-out behind catalog_table_insert_function_get. It
// receives the user-column batch (no rowid) and writes one storage row per
// input row, generating a rowid from the user columns according to the
// table spec.
type insertFn struct{}

var _ vgi.TypedTableInOutFunc[noState] = (*insertFn)(nil)

func (insertFn) Name() string { return "schema_reconcile_insert" }
func (insertFn) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "INSERT handler for the schema_reconcile fixture", Stability: vgi.StabilityVolatile}
}
func (insertFn) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "table_name", Position: 0, ArrowType: "varchar", Doc: "Logical table name", IsConst: true},
		{Name: "data", Position: 1, ArrowType: "table", Doc: "Rows to insert"},
	}
}
func (insertFn) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: countSchema}, nil
}
func (insertFn) NewState(params *vgi.ProcessParams) (*noState, error) { return &noState{}, nil }
func (insertFn) Process(ctx context.Context, params *vgi.ProcessParams, _ *noState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	spec, err := specFromArgs(params)
	if err != nil {
		return err
	}
	storageMu.Lock()
	defer storageMu.Unlock()
	db, err := openDB()
	if err != nil {
		return err
	}
	defer db.Close()

	// Build the rowid column up-front so the stored row payload includes
	// the rowid alongside the user columns. SCAN re-emits the stored
	// payload verbatim (rowid included), and UPDATE/DELETE hash the rowid
	// column from their input batches with the same scheme.
	rowidArr, err := buildInsertRowidColumn(db, spec, batch)
	if err != nil {
		return err
	}
	defer rowidArr.Release()

	n := batch.NumRows()
	for i := int64(0); i < n; i++ {
		// Build a single-row batch matching userSchema + rowid (the full
		// table schema), serialize it as the stored payload.
		row := buildRowWithRowid(batch, rowidArr, i, spec)
		payload, err := serializeBatch(row)
		row.Release()
		if err != nil {
			return err
		}
		ridKey, err := rowidKey(rowidArr, int(i))
		if err != nil {
			return err
		}
		if _, err := db.Exec(
			fmt.Sprintf("INSERT OR REPLACE INTO %s (rid_blob, payload_blob) VALUES (?, ?)", spec.storageName),
			ridKey, payload,
		); err != nil {
			return err
		}
	}
	return out.Emit(buildCountBatch(n))
}

// buildInsertRowidColumn synthesizes the rowid column for an INSERT batch.
// For demo (int64 rowid): assign next sequential ids based on existing rows.
// For ts_demo: project from the ts column.
// For struct_demo: project struct{a:id, b:nested.b}.
func buildInsertRowidColumn(db *sql.DB, spec tableSpec, batch arrow.RecordBatch) (arrow.Array, error) {
	mem := memory.NewGoAllocator()
	n := int(batch.NumRows())
	switch spec.name {
	case "demo":
		// Find max existing int rowid.
		// We store rid_blob as the 8-byte little-endian int64 of the rowid value
		// for fast equality matching by UPDATE/DELETE. To find max, scan rows.
		var maxRid int64
		rows, err := db.Query(fmt.Sprintf("SELECT rid_blob FROM %s", spec.storageName))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var blob []byte
			if err := rows.Scan(&blob); err != nil {
				rows.Close()
				return nil, err
			}
			if len(blob) == 8 {
				v := int64(binary.LittleEndian.Uint64(blob))
				if v > maxRid {
					maxRid = v
				}
			}
		}
		rows.Close()
		b := array.NewInt64Builder(mem)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(maxRid + int64(i+1))
		}
		return b.NewArray(), nil
	case "ts_demo":
		idx := batch.Schema().FieldIndices("ts")
		if len(idx) == 0 {
			return nil, fmt.Errorf("ts_demo: missing ts column on insert")
		}
		col := batch.Column(idx[0])
		col.Retain()
		return col, nil
	case "struct_demo":
		idIdx := batch.Schema().FieldIndices("id")
		nestedIdx := batch.Schema().FieldIndices("nested")
		if len(idIdx) == 0 || len(nestedIdx) == 0 {
			return nil, fmt.Errorf("struct_demo: missing id or nested column on insert")
		}
		nested, ok := batch.Column(nestedIdx[0]).(*array.Struct)
		if !ok {
			return nil, fmt.Errorf("struct_demo: nested column is %T", batch.Column(nestedIdx[0]))
		}
		nestedType := batch.Schema().Field(nestedIdx[0]).Type.(*arrow.StructType)
		bIdx := -1
		for c := 0; c < nestedType.NumFields(); c++ {
			if nestedType.Field(c).Name == "b" {
				bIdx = c
				break
			}
		}
		if bIdx < 0 {
			return nil, fmt.Errorf("struct_demo: nested.b not found")
		}
		structType := spec.rowidField.Type.(*arrow.StructType)
		structBuilder := array.NewStructBuilder(mem, structType)
		defer structBuilder.Release()
		aBuilder := structBuilder.FieldBuilder(0).(*array.Int64Builder)
		bBuilder := structBuilder.FieldBuilder(1).(*array.StringBuilder)
		idCol := batch.Column(idIdx[0])
		bCol := nested.Field(bIdx)
		for i := 0; i < n; i++ {
			structBuilder.Append(true)
			if a, ok := idCol.(*array.Int64); ok {
				aBuilder.Append(a.Value(i))
			} else {
				aBuilder.AppendNull()
			}
			if bCol.IsNull(i) {
				bBuilder.AppendNull()
			} else if s, ok := bCol.(*array.String); ok {
				bBuilder.Append(s.Value(i))
			} else {
				bBuilder.AppendNull()
			}
		}
		return structBuilder.NewArray(), nil
	}
	return nil, fmt.Errorf("buildInsertRowidColumn: unknown spec %s", spec.name)
}

// buildRowWithRowid returns a 1-row batch with userSchema + rowid for storage.
func buildRowWithRowid(batch arrow.RecordBatch, rowidArr arrow.Array, i int64, spec tableSpec) arrow.RecordBatch {
	cols := make([]arrow.Array, batch.NumCols()+1)
	for c := 0; c < int(batch.NumCols()); c++ {
		cols[c] = array.NewSlice(batch.Column(c), i, i+1)
	}
	cols[batch.NumCols()] = array.NewSlice(rowidArr, i, i+1)
	out := array.NewRecordBatch(spec.tableSchema(), cols, 1)
	for _, c := range cols {
		c.Release()
	}
	return out
}
func (insertFn) Finalize(ctx context.Context, params *vgi.ProcessParams, _ *noState) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// ============================================================================
// Update handler
// ============================================================================

type updateFn struct{}

var _ vgi.TypedTableInOutFunc[noState] = (*updateFn)(nil)

func (updateFn) Name() string { return "schema_reconcile_update" }
func (updateFn) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "UPDATE handler for the schema_reconcile fixture", Stability: vgi.StabilityVolatile}
}
func (updateFn) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "table_name", Position: 0, ArrowType: "varchar", Doc: "Logical table name", IsConst: true},
		{Name: "data", Position: 1, ArrowType: "table", Doc: "rowid + selected columns"},
	}
}
func (updateFn) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: countSchema}, nil
}
func (updateFn) NewState(params *vgi.ProcessParams) (*noState, error) { return &noState{}, nil }
func (updateFn) Process(ctx context.Context, params *vgi.ProcessParams, _ *noState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	spec, err := specFromArgs(params)
	if err != nil {
		return err
	}
	rowidIdx := batch.Schema().FieldIndices("rowid")
	if len(rowidIdx) == 0 {
		return fmt.Errorf("schema_reconcile_update: missing rowid column")
	}
	rowidCol := batch.Column(rowidIdx[0])

	storageMu.Lock()
	defer storageMu.Unlock()
	db, err := openDB()
	if err != nil {
		return err
	}
	defer db.Close()
	var updated int64
	for i := int64(0); i < batch.NumRows(); i++ {
		ridKey, err := rowidKey(rowidCol, int(i))
		if err != nil {
			return err
		}
		// Load existing payload.
		var existingBytes []byte
		if err := db.QueryRow(
			fmt.Sprintf("SELECT payload_blob FROM %s WHERE rid_blob = ?", spec.storageName),
			ridKey,
		).Scan(&existingBytes); err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			return err
		}
		existing, err := deserializeBatch(existingBytes)
		if err != nil {
			return err
		}
		// Build a merged row: take the existing row, override columns
		// present in the update batch (excluding rowid which we keep from
		// existing).
		merged, err := mergeRow(existing, batch, i, spec)
		existing.Release()
		if err != nil {
			return err
		}
		mergedBytes, err := serializeBatch(merged)
		merged.Release()
		if err != nil {
			return err
		}
		if _, err := db.Exec(
			fmt.Sprintf("UPDATE %s SET payload_blob = ? WHERE rid_blob = ?", spec.storageName),
			mergedBytes, ridKey,
		); err != nil {
			return err
		}
		updated++
	}
	return out.Emit(buildCountBatch(updated))
}
func (updateFn) Finalize(ctx context.Context, params *vgi.ProcessParams, _ *noState) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// mergeRow takes the existing stored payload (full table schema, including
// rowid) and overlays columns from the update batch at row updateRow.
// Returns a one-row RecordBatch matching spec.tableSchema(). The rowid is
// always carried over from the existing payload — even when the UPDATE
// batch carries a rowid column (DuckDB sends it for the WHERE-rowid
// match), the stored row's rowid identity is what we keep.
func mergeRow(existing, update arrow.RecordBatch, updateRow int64, spec tableSpec) (arrow.RecordBatch, error) {
	full := spec.tableSchema()
	cols := make([]arrow.Array, full.NumFields())
	for i := 0; i < full.NumFields(); i++ {
		name := full.Field(i).Name
		// Always take rowid from existing — we don't re-key on UPDATE.
		if name == "rowid" {
			eIdx := existing.Schema().FieldIndices("rowid")
			if len(eIdx) == 0 {
				return nil, fmt.Errorf("mergeRow: existing row missing rowid column")
			}
			cols[i] = array.NewSlice(existing.Column(eIdx[0]), 0, 1)
			continue
		}
		// Override if the update batch supplies this column.
		if idx := update.Schema().FieldIndices(name); len(idx) > 0 {
			cols[i] = array.NewSlice(update.Column(idx[0]), updateRow, updateRow+1)
		} else {
			eIdx := existing.Schema().FieldIndices(name)
			if len(eIdx) == 0 {
				return nil, fmt.Errorf("mergeRow: missing column %q in both existing and update batches", name)
			}
			cols[i] = array.NewSlice(existing.Column(eIdx[0]), 0, 1)
		}
	}
	out := array.NewRecordBatch(full, cols, 1)
	for _, c := range cols {
		c.Release()
	}
	return out, nil
}

// ============================================================================
// Delete handler
// ============================================================================

type deleteFn struct{}

var _ vgi.TypedTableInOutFunc[noState] = (*deleteFn)(nil)

func (deleteFn) Name() string { return "schema_reconcile_delete" }
func (deleteFn) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "DELETE handler for the schema_reconcile fixture", Stability: vgi.StabilityVolatile}
}
func (deleteFn) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "table_name", Position: 0, ArrowType: "varchar", Doc: "Logical table name", IsConst: true},
		{Name: "data", Position: 1, ArrowType: "table", Doc: "rowid only"},
	}
}
func (deleteFn) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: countSchema}, nil
}
func (deleteFn) NewState(params *vgi.ProcessParams) (*noState, error) { return &noState{}, nil }
func (deleteFn) Process(ctx context.Context, params *vgi.ProcessParams, _ *noState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	spec, err := specFromArgs(params)
	if err != nil {
		return err
	}
	rowidIdx := batch.Schema().FieldIndices("rowid")
	if len(rowidIdx) == 0 {
		return fmt.Errorf("schema_reconcile_delete: missing rowid column")
	}
	rowidCol := batch.Column(rowidIdx[0])

	storageMu.Lock()
	defer storageMu.Unlock()
	db, err := openDB()
	if err != nil {
		return err
	}
	defer db.Close()
	var deleted int64
	for i := int64(0); i < batch.NumRows(); i++ {
		ridKey, err := rowidKey(rowidCol, int(i))
		if err != nil {
			return err
		}
		res, err := db.Exec(
			fmt.Sprintf("DELETE FROM %s WHERE rid_blob = ?", spec.storageName),
			ridKey,
		)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		deleted += n
	}
	return out.Emit(buildCountBatch(deleted))
}
func (deleteFn) Finalize(ctx context.Context, params *vgi.ProcessParams, _ *noState) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// ============================================================================
// Scan handler
// ============================================================================

type scanState struct {
	Emitted bool
}

type scanFn struct{}

var _ vgi.TypedTableFunc[scanState] = (*scanFn)(nil)

func (scanFn) Name() string { return "schema_reconcile_scan" }
func (scanFn) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "SCAN handler for the schema_reconcile fixture", Stability: vgi.StabilityVolatile, ProjectionPushdown: true}
}
func (scanFn) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "table_name", Position: 0, ArrowType: "varchar", Doc: "Logical table name", IsConst: true},
	}
}
func (scanFn) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	spec, err := specFromBindArgs(params)
	if err != nil {
		return nil, err
	}
	return &vgi.BindResponse{OutputSchema: spec.tableSchema()}, nil
}
func (scanFn) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	return &vgi.TableCardinality{Estimate: -1, Max: -1}, nil
}
func (scanFn) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	// Single worker — otherwise every secondary subprocess would re-emit
	// the full row set, duplicating each row.
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}
func (scanFn) NewState(params *vgi.ProcessParams) (*scanState, error) { return &scanState{}, nil }
func (scanFn) Process(ctx context.Context, params *vgi.ProcessParams, state *scanState, out *vgirpc.OutputCollector) error {
	if state.Emitted {
		return out.Finish()
	}
	state.Emitted = true
	spec, err := specFromArgs(params)
	if err != nil {
		return err
	}
	storageMu.Lock()
	defer storageMu.Unlock()
	db, err := openDB()
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.Query(fmt.Sprintf("SELECT payload_blob FROM %s", spec.storageName))
	if err != nil {
		return err
	}
	defer rows.Close()
	var batches []arrow.RecordBatch
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()
	for rows.Next() {
		var blob []byte
		if err := rows.Scan(&blob); err != nil {
			return err
		}
		b, err := deserializeBatch(blob)
		if err != nil {
			return err
		}
		batches = append(batches, b)
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	if len(batches) == 0 {
		// Emit an empty batch matching the projected schema.
		return emitEmpty(out, params.OutputSchema)
	}
	// The stored payload already includes the rowid column (built at
	// INSERT time). Concatenate the per-row stored batches and project to
	// the C++ side's requested output schema, stripping any field metadata
	// (the rowid pseudocolumn flag confuses the C++ data_reader if echoed
	// back on the wire).
	full, err := concatBatches(batches)
	if err != nil {
		return err
	}
	defer full.Release()
	projected := projectBatchStrippedMetadata(full, params.OutputSchema)
	defer projected.Release()
	if err := out.Emit(projected); err != nil {
		return err
	}
	return out.Finish()
}

// projectBatchStrippedMetadata projects src to the named columns of schema
// and rebuilds the output schema without per-field metadata. The rowid
// pseudocolumn metadata, in particular, must be stripped or the C++
// data_reader silently drops the batch.
func projectBatchStrippedMetadata(src arrow.RecordBatch, schema *arrow.Schema) arrow.RecordBatch {
	cols := make([]arrow.Array, schema.NumFields())
	stripped := make([]arrow.Field, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		f := schema.Field(i)
		stripped[i] = arrow.Field{Name: f.Name, Type: f.Type, Nullable: f.Nullable}
		idx := src.Schema().FieldIndices(f.Name)
		if len(idx) == 0 {
			continue
		}
		cols[i] = src.Column(idx[0])
		cols[i].Retain()
	}
	out := array.NewRecordBatch(arrow.NewSchema(stripped, nil), cols, src.NumRows())
	for _, c := range cols {
		if c != nil {
			c.Release()
		}
	}
	return out
}

// concatBatches stacks N one-row batches with userSchema into a single
// N-row batch. Each row was stored separately at INSERT time so we can
// dispatch UPDATE/DELETE by rowid; SELECT pulls them all and concatenates.
// We Concatenate per column rather than going through a TableReader so the
// result is a single batch even when the input has many tiny rows.
func concatBatches(batches []arrow.RecordBatch) (arrow.RecordBatch, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("concatBatches: no batches")
	}
	if len(batches) == 1 {
		batches[0].Retain()
		return batches[0], nil
	}
	mem := memory.NewGoAllocator()
	schema := batches[0].Schema()
	cols := make([]arrow.Array, schema.NumFields())
	defer func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}()
	for c := 0; c < schema.NumFields(); c++ {
		parts := make([]arrow.Array, len(batches))
		for i, b := range batches {
			parts[i] = b.Column(c)
		}
		merged, err := array.Concatenate(parts, mem)
		if err != nil {
			return nil, fmt.Errorf("concatBatches: column %d: %w", c, err)
		}
		cols[c] = merged
	}
	var totalRows int64
	for _, b := range batches {
		totalRows += b.NumRows()
	}
	return array.NewRecordBatch(schema, cols, totalRows), nil
}

func emitEmpty(out *vgirpc.OutputCollector, schema *arrow.Schema) error {
	mem := memory.NewGoAllocator()
	cols := make([]arrow.Array, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		b := array.NewBuilder(mem, schema.Field(i).Type)
		cols[i] = b.NewArray()
		b.Release()
	}
	batch := array.NewRecordBatch(schema, cols, 0)
	defer batch.Release()
	for _, c := range cols {
		c.Release()
	}
	if err := out.Emit(batch); err != nil {
		return err
	}
	return out.Finish()
}

// ============================================================================
// Helpers
// ============================================================================

// noState is a stateless placeholder for the table-in-out handlers. The
// padding field gives gob something to encode (it rejects struct types
// with no exported fields).
type noState struct {
	Pad bool
}

func specFromArgs(params *vgi.ProcessParams) (tableSpec, error) {
	if params.Args == nil {
		return tableSpec{}, fmt.Errorf("schema_reconcile: missing arguments")
	}
	name, err := params.Args.GetScalarString(0)
	if err != nil {
		return tableSpec{}, fmt.Errorf("schema_reconcile: missing table_name arg: %w", err)
	}
	spec, ok := tableSpecs[name]
	if !ok {
		return tableSpec{}, fmt.Errorf("schema_reconcile: unknown table %q", name)
	}
	return spec, nil
}

func specFromBindArgs(params *vgi.BindParams) (tableSpec, error) {
	if params.Args == nil {
		return tableSpec{}, fmt.Errorf("schema_reconcile: missing arguments")
	}
	name, err := params.Args.GetScalarString(0)
	if err != nil {
		return tableSpec{}, fmt.Errorf("schema_reconcile: missing table_name arg: %w", err)
	}
	spec, ok := tableSpecs[name]
	if !ok {
		return tableSpec{}, fmt.Errorf("schema_reconcile: unknown table %q", name)
	}
	return spec, nil
}

func buildCountBatch(n int64) arrow.RecordBatch {
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.Append(n)
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(countSchema, []arrow.Array{col}, 1)
}

// ============================================================================
// Catalog wiring
// ============================================================================

// serializeTableInfo builds a TableInfo IPC payload for a given spec —
// used by both the schema_contents handler (returns all tables) and the
// table_get handler (returns one).
func serializeTableInfo(spec tableSpec) ([]byte, error) {
	info := &vgi.TableInfo{
		Name:           spec.name,
		SchemaName:     SchemaName,
		Comment:        fmt.Sprintf("schema_reconcile %s (rowid type %s)", spec.name, spec.rowidField.Type),
		Columns:        spec.tableSchema(),
		SupportsInsert: true,
		SupportsUpdate: true,
		SupportsDelete: true,
	}
	return vgi.SerializeTableInfo(info)
}

// SchemaContentsHandler returns the per-(attach, schema) table list. Wired
// via vgi.WithSchemaContentsHandler.
func SchemaContentsHandler(attachOpaqueData []byte, schemaName string) ([]vgi.SerializedSchemaItem, bool) {
	if string(attachOpaqueData) != CatalogName {
		return nil, false
	}
	if schemaName != SchemaName {
		return nil, false
	}
	items := make([]vgi.SerializedSchemaItem, 0, len(tableSpecs))
	// Stable order — the SQL test sees this ordering on count(*) etc.
	for _, name := range []string{"demo", "ts_demo", "struct_demo"} {
		spec := tableSpecs[name]
		data, err := serializeTableInfo(spec)
		if err != nil {
			continue
		}
		items = append(items, data)
	}
	return items, true
}

// AttachTableGetHandler answers single-table catalog_table_get RPCs for the
// schema_reconcile catalog. Wired via vgi.WithAttachTableGetHandler.
func AttachTableGetHandler(attachOpaqueData []byte, schemaName, name string, atUnit, atValue *string) ([]byte, bool, error) {
	if string(attachOpaqueData) != CatalogName {
		return nil, false, nil
	}
	if schemaName != SchemaName {
		return nil, false, nil
	}
	spec, ok := tableSpecs[name]
	if !ok {
		return nil, false, nil
	}
	data, err := serializeTableInfo(spec)
	if err != nil {
		return nil, true, err
	}
	return data, true, nil
}

// AttachScanFunctionGetHandler routes SELECT-time scan-function lookups to
// schema_reconcile_scan(<table_name>). Wired via
// vgi.WithAttachScanFunctionGetHandler.
func AttachScanFunctionGetHandler(attachOpaqueData []byte, schemaName, name string, atUnit, atValue *string) (*vgi.ScanFunctionResult, bool, error) {
	if string(attachOpaqueData) != CatalogName {
		return nil, false, nil
	}
	if schemaName != SchemaName {
		return nil, false, nil
	}
	if _, ok := tableSpecs[name]; !ok {
		return nil, false, nil
	}
	return &vgi.ScanFunctionResult{
		FunctionName: "schema_reconcile_scan",
		PositionalArguments: []vgi.ScanArg{
			{Value: name, Type: arrow.BinaryTypes.String},
		},
	}, true, nil
}

// AttachWriteFunctionGetHandler routes INSERT/UPDATE/DELETE-time function
// lookups to schema_reconcile_{insert,update,delete}(<table_name>). Wired
// via vgi.WithAttachWriteFunctionGetHandler.
func AttachWriteFunctionGetHandler(op vgi.WriteOp, attachOpaqueData []byte, schemaName, name string) (*vgi.ScanFunctionResult, bool, error) {
	if string(attachOpaqueData) != CatalogName {
		return nil, false, nil
	}
	if schemaName != SchemaName {
		return nil, false, nil
	}
	if _, ok := tableSpecs[name]; !ok {
		return nil, false, nil
	}
	var fn string
	switch op {
	case vgi.WriteOpInsert:
		fn = "schema_reconcile_insert"
	case vgi.WriteOpUpdate:
		fn = "schema_reconcile_update"
	case vgi.WriteOpDelete:
		fn = "schema_reconcile_delete"
	default:
		return nil, false, nil
	}
	return &vgi.ScanFunctionResult{
		FunctionName: fn,
		PositionalArguments: []vgi.ScanArg{
			{Value: name, Type: arrow.BinaryTypes.String},
		},
	}, true, nil
}

// RegisterAll registers the four handler functions on the worker, scoped
// to the schema_reconcile catalog so they don't clutter the example
// catalog's function listing.
func RegisterAll(w *vgi.Worker) {
	w.RegisterTableInOutForCatalog(CatalogName, vgi.AsTableInOutFunction[noState](insertFn{}))
	w.RegisterTableInOutForCatalog(CatalogName, vgi.AsTableInOutFunction[noState](updateFn{}))
	w.RegisterTableInOutForCatalog(CatalogName, vgi.AsTableInOutFunction[noState](deleteFn{}))
	w.RegisterTableForCatalog(CatalogName, vgi.AsTableFunction[scanState](scanFn{}))
}
