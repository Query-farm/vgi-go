// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ---------------------------------------------------------------------------
// Writable-table result modes and helpers for write functions.
//
// A writable table (CatalogTable.InsertFunction / UpdateFunction /
// DeleteFunction) advertises, per operation, the richest result mode its write
// function can produce (CatalogTable.WriteResultModes). Modes are ordered
// count < rows < changes, and a table that supports one mode supports every
// lower one. DuckDB asks for one mode per statement in the write_options named
// argument of the write function's bind (ParseWriteOptions):
//
//	count    plain INSERT/UPDATE/DELETE: one row, `count BIGINT NOT NULL`
//	rows     ... RETURNING: the written rows, in the table's user columns
//	changes  old/new pairs: `old STRUCT(<table>)`, `new STRUCT(<table>)`
//
// What a write function receives (its input table):
//
//	insert   the new rows, in the table's user columns
//	update   the row-id column plus the SET columns, per updated row
//	delete   the row-id column of each deleted row
//
// UPDATE and DELETE identify rows by the table's row-id column (RowIDField),
// which the table's scan function must emit. Port of vgi-python's
// write_results.py; mirrors the TypeScript SDK's write-results.ts.
// ---------------------------------------------------------------------------

// WriteResultMode is what a write function returns for one statement.
type WriteResultMode string

const (
	// WriteResultCount is one row: `count BIGINT NOT NULL`.
	WriteResultCount WriteResultMode = "count"
	// WriteResultRows is the written rows in the table's user columns
	// (INSERT/UPDATE/DELETE ... RETURNING).
	WriteResultRows WriteResultMode = "rows"
	// WriteResultChanges is one OLD/NEW struct pair per written row.
	WriteResultChanges WriteResultMode = "changes"
)

var writeResultModeRank = map[WriteResultMode]int{WriteResultCount: 0, WriteResultRows: 1, WriteResultChanges: 2}

// writeOps lists the write operations in wire order.
var writeOps = []WriteOp{WriteOpInsert, WriteOpUpdate, WriteOpDelete}

// Valid reports whether m is one of count, rows, changes.
func (m WriteResultMode) Valid() bool {
	_, ok := writeResultModeRank[m]
	return ok
}

// SupportsWriteResultMode reports whether a table whose maximum mode is
// maximum can answer a statement that requested requested.
func SupportsWriteResultMode(maximum, requested WriteResultMode) bool {
	mx, ok1 := writeResultModeRank[maximum]
	rq, ok2 := writeResultModeRank[requested]
	return ok1 && ok2 && mx >= rq
}

// RowIDMetadataKey is the field-metadata key that marks a table's row-id
// column.
const RowIDMetadataKey = "is_row_id"

// RowIDField returns a row-id field for a writable table's columns and its
// scan's output. RowIDField("", nil) is DuckDB's conventional `rowid BIGINT`.
//
// DuckDB reads the row id as a virtual column, which it only requests from a
// scan that supports projection pushdown. So a table with UPDATE or DELETE
// needs a scan Function that (1) emits this field, (2) declares
// FunctionMetadata.ProjectionPushdown and honours ProcessParams.OutputSchema,
// and (3) is listed in the catalog (CatalogTable.Function registers it). Row
// ids must be unique and stable for the life of a statement; DuckDB hides the
// column from SELECT *.
func RowIDField(name string, typ arrow.DataType) arrow.Field {
	if name == "" {
		name = "rowid"
	}
	if typ == nil {
		typ = arrow.PrimitiveTypes.Int64
	}
	return arrow.Field{Name: name, Type: typ, Nullable: false,
		Metadata: arrow.NewMetadata([]string{RowIDMetadataKey}, []string{""})}
}

// IsRowIDField reports whether f carries the row-id marker.
func IsRowIDField(f arrow.Field) bool {
	return f.HasMetadata() && f.Metadata.FindKey(RowIDMetadataKey) >= 0
}

// UserColumns returns columns without its row-id field: the schema of write
// results (rows, and both halves of changes) and of INSERT input.
func UserColumns(columns *arrow.Schema) *arrow.Schema {
	if columns == nil {
		return nil
	}
	fields := make([]arrow.Field, 0, columns.NumFields())
	for _, f := range columns.Fields() {
		if !IsRowIDField(f) {
			fields = append(fields, f)
		}
	}
	md := columns.Metadata()
	return arrow.NewSchema(fields, &md)
}

// WriteResultSchema is the exact output schema a write function must bind
// (BindResponse.OutputSchema) for mode. tableSchema is the table's user
// columns (UserColumns strips a row-id field if present).
func WriteResultSchema(mode WriteResultMode, tableSchema *arrow.Schema) (*arrow.Schema, error) {
	switch mode {
	case WriteResultCount:
		return arrow.NewSchema([]arrow.Field{{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: false}}, nil), nil
	case WriteResultRows:
		return UserColumns(tableSchema), nil
	case WriteResultChanges:
		row := arrow.StructOf(UserColumns(tableSchema).Fields()...)
		return arrow.NewSchema([]arrow.Field{
			{Name: "old", Type: row, Nullable: true},
			{Name: "new", Type: row, Nullable: true},
		}, nil), nil
	}
	return nil, fmt.Errorf("vgi: unknown write result mode %q", mode)
}

// WriteCountBatch is a count result batch.
func WriteCountBatch(count int64) arrow.RecordBatch {
	schema, _ := WriteResultSchema(WriteResultCount, nil)
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.Append(count)
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(schema, []arrow.Array{col}, 1)
}

// WriteRowsBatch is a rows result batch: rows (name-keyed, missing keys NULL)
// in tableSchema's user columns.
func WriteRowsBatch(tableSchema *arrow.Schema, rows []map[string]any) (arrow.RecordBatch, error) {
	bb := NewBatchBuilder(UserColumns(tableSchema))
	defer bb.Release()
	for i, r := range rows {
		if err := bb.AppendRow(r); err != nil {
			return nil, fmt.Errorf("vgi: write result row %d: %w", i, err)
		}
	}
	return bb.Build()
}

// WriteChangesBatch is a changes result batch, one OLD/NEW pair per written
// row: oldRows[i] is nil for an insert, newRows[i] is nil for a delete.
func WriteChangesBatch(tableSchema *arrow.Schema, oldRows, newRows []map[string]any) (arrow.RecordBatch, error) {
	if len(oldRows) != len(newRows) {
		return nil, fmt.Errorf("vgi: WriteChangesBatch: %d old rows but %d new rows", len(oldRows), len(newRows))
	}
	schema, err := WriteResultSchema(WriteResultChanges, tableSchema)
	if err != nil {
		return nil, err
	}
	bb := NewBatchBuilder(schema)
	defer bb.Release()
	for i := range oldRows {
		row := map[string]any{}
		if oldRows[i] != nil {
			row["old"] = oldRows[i]
		}
		if newRows[i] != nil {
			row["new"] = newRows[i]
		}
		if err := bb.AppendRow(row); err != nil {
			return nil, fmt.Errorf("vgi: write result change %d: %w", i, err)
		}
	}
	return bb.Build()
}

// WriteResultBatch is the result batch for mode, from the written rows'
// before/after images (nil entries for the side that does not exist:
// old for an insert, new for a delete). For rows (RETURNING) it emits each
// row as written: the new image, or the old one for a delete.
func WriteResultBatch(mode WriteResultMode, tableSchema *arrow.Schema, oldRows, newRows []map[string]any) (arrow.RecordBatch, error) {
	switch mode {
	case WriteResultCount:
		n := len(newRows)
		if len(oldRows) > n {
			n = len(oldRows)
		}
		return WriteCountBatch(int64(n)), nil
	case WriteResultChanges:
		return WriteChangesBatch(tableSchema, oldRows, newRows)
	case WriteResultRows:
		n := len(newRows)
		if len(oldRows) > n {
			n = len(oldRows)
		}
		rows := make([]map[string]any, n)
		for i := range rows {
			if i < len(newRows) && newRows[i] != nil {
				rows[i] = newRows[i]
			} else if i < len(oldRows) {
				rows[i] = oldRows[i]
			}
		}
		return WriteRowsBatch(tableSchema, rows)
	}
	return nil, fmt.Errorf("vgi: unknown write result mode %q", mode)
}

// WriteOptions is what DuckDB asked a write function for, decoded from the
// write_options named argument (ParseWriteOptions).
type WriteOptions struct {
	// ResultMode is the result the statement needs (count, or rows for
	// RETURNING).
	ResultMode WriteResultMode
	// OnConflict is always "throw".
	//
	// Deprecated: protocol 3.3 removed on_conflict from write_options.
	// DuckDB rewrites every INSERT ... ON CONFLICT and INSERT OR REPLACE
	// into a MERGE before the catalog sees it, so a write function never
	// received anything but "throw". Kept so existing code compiles.
	OnConflict string
	// OnConflictColumns is always empty.
	//
	// Deprecated: protocol 3.3 removed on_conflict_columns from
	// write_options (see OnConflict).
	OnConflictColumns []string
}

// WriteOptionsArgName is the named argument DuckDB passes write options in.
const WriteOptionsArgName = "write_options"

// WriteOptionsArgSpec is the ArgSpec a write function declares for the
// write_options named argument (a serialized one-row batch), alongside its
// input table argument:
//
//	func (f *itemsUpdate) ArgumentSpecs() []vgi.ArgSpec {
//		return []vgi.ArgSpec{
//			{Name: "data", Position: 0, ArrowType: "table"},
//			vgi.WriteOptionsArgSpec(),
//		}
//	}
func WriteOptionsArgSpec() ArgSpec {
	return ArgSpec{Name: WriteOptionsArgName, Position: -1, IsConst: true, HasDefault: true,
		ArrowType: "blob", ArrowDataType: arrow.BinaryTypes.Binary,
		Doc: "Serialized write options (result_mode)"}
}

// ParseWriteOptions decodes the write_options named argument of a write
// function's bind (BindParams.Args / InitParams.Args / ProcessParams.Args).
// Missing options mean a plain count write. Only result_mode is read:
// protocol 3.3 write_options carries nothing else.
func ParseWriteOptions(args *Arguments) (WriteOptions, error) {
	out := WriteOptions{ResultMode: WriteResultCount, OnConflict: "throw"}
	if args == nil || args.IsNull(WriteOptionsArgName) {
		return out, nil
	}
	if _, err := args.GetColumn(WriteOptionsArgName); err != nil {
		return out, nil
	}
	blob, err := args.GetScalarBytes(WriteOptionsArgName)
	if err != nil {
		return out, fmt.Errorf("vgi: write_options: %w", err)
	}
	if len(blob) == 0 {
		return out, nil
	}
	b, err := DeserializeRecordBatch(blob)
	if err != nil {
		return out, fmt.Errorf("vgi: write_options: %w", err)
	}
	defer b.Release()
	if b.NumRows() == 0 {
		return out, nil
	}
	rows, err := batchToRows(b)
	if err != nil {
		return out, err
	}
	row := rows[0]
	if v, ok := row["result_mode"].(string); ok {
		m := WriteResultMode(v)
		if !m.Valid() {
			return out, fmt.Errorf("vgi: write_options: unknown result_mode %q", v)
		}
		out.ResultMode = m
	}
	return out, nil
}

// validateWrites checks a table's write declaration as vgi-python's
// Table.__post_init__ does: known modes only, a mode only for an operation
// with a function, and UPDATE/DELETE only with a scan function (row ids).
func (ct *CatalogTable) validateWrites() error {
	for op, mode := range ct.WriteResultModes {
		if !mode.Valid() {
			return fmt.Errorf("table %q: unknown write result mode %q for %s", ct.Name, mode, op)
		}
		if ct.writeFunction(op) == nil {
			return fmt.Errorf("table %q: WriteResultModes declares %q without a corresponding %sFunction", ct.Name, op, capitalizeOp(op))
		}
	}
	if (ct.UpdateFunction != nil || ct.DeleteFunction != nil) && ct.Function == nil {
		return fmt.Errorf("table %q: UpdateFunction and DeleteFunction require a scan Function that emits the row-id column (RowIDField)", ct.Name)
	}
	return nil
}

// writeFunction returns the table's write function for op, or nil.
func (ct *CatalogTable) writeFunction(op WriteOp) TableInOutFunction {
	switch op {
	case WriteOpInsert:
		return ct.InsertFunction
	case WriteOpUpdate:
		return ct.UpdateFunction
	case WriteOpDelete:
		return ct.DeleteFunction
	}
	return nil
}

// writable reports whether the table has any write function.
func (ct *CatalogTable) writable() bool {
	return ct.InsertFunction != nil || ct.UpdateFunction != nil || ct.DeleteFunction != nil
}

// wireWriteResultModes is TableInfo.write_result_modes: each operation with a
// function, at its declared maximum mode (default count).
func (ct *CatalogTable) wireWriteResultModes() map[string]string {
	if !ct.writable() {
		return nil
	}
	out := map[string]string{}
	for _, op := range writeOps {
		if ct.writeFunction(op) == nil {
			continue
		}
		mode := ct.WriteResultModes[op]
		if mode == "" {
			mode = WriteResultCount
		}
		out[string(op)] = string(mode)
	}
	return out
}

func capitalizeOp(op WriteOp) string {
	switch op {
	case WriteOpInsert:
		return "Insert"
	case WriteOpUpdate:
		return "Update"
	case WriteOpDelete:
		return "Delete"
	}
	return string(op)
}
