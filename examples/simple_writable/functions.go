// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package simple_writable

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type noState struct{}

// ---------------------------------------------------------------------------
// Arg + value helpers
// ---------------------------------------------------------------------------

func tableArg(args *vgi.Arguments) (string, error) {
	name, err := args.GetScalarString(0)
	if err != nil {
		return "", fmt.Errorf("simple_writable: table_name positional argument is required")
	}
	return name, nil
}

// wantReturning reports whether the bound output schema is the user-row schema
// (RETURNING requested) rather than the (count) schema.
func wantReturning(out *arrow.Schema) bool {
	return out == nil || out.NumFields() != 1 || out.Field(0).Name != "count"
}

// returnChunks decodes write_options.return_chunks (a serialized 1-row batch
// passed as a named blob argument, mirroring vgi-python). Absent/false → count.
func returnChunks(args *vgi.Arguments) bool {
	if args == nil {
		return false
	}
	if _, err := args.GetColumn("write_options"); err != nil {
		return false
	}
	blob, err := args.GetScalarBytes("write_options")
	if err != nil || len(blob) == 0 {
		return false
	}
	b, err := vgi.DeserializeRecordBatch(blob)
	if err != nil {
		return false
	}
	defer b.Release()
	idxs := b.Schema().FieldIndices("return_chunks")
	if len(idxs) == 0 || b.NumRows() == 0 {
		return false
	}
	col, ok := b.Column(idxs[0]).(*array.Boolean)
	if !ok || col.IsNull(0) {
		return false
	}
	return col.Value(0)
}

// cellValue reads the Go value at row i of a (int64 | string) Arrow array.
func cellValue(arr arrow.Array, i int) (any, error) {
	if arr.IsNull(i) {
		return nil, nil
	}
	switch a := arr.(type) {
	case *array.Int64:
		return a.Value(i), nil
	case *array.String:
		return a.Value(i), nil
	default:
		return nil, fmt.Errorf("simple_writable: unsupported column type %s", arr.DataType())
	}
}

// buildColumn builds an (int64 | string) Arrow array from Go values (nil = NULL).
func buildColumn(field arrow.Field, vals []any) (arrow.Array, error) {
	switch field.Type.ID() {
	case arrow.INT64:
		b := array.NewInt64Builder(memory.DefaultAllocator)
		defer b.Release()
		for _, v := range vals {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(v.(int64))
			}
		}
		return b.NewArray(), nil
	case arrow.STRING:
		b := array.NewStringBuilder(memory.DefaultAllocator)
		defer b.Release()
		for _, v := range vals {
			if v == nil {
				b.AppendNull()
			} else {
				b.Append(v.(string))
			}
		}
		return b.NewArray(), nil
	default:
		return nil, fmt.Errorf("simple_writable: unsupported field type %s", field.Type)
	}
}

// rowMapFromBatch builds a row map from the named columns of batch at row i.
func rowMapFromBatch(batch arrow.RecordBatch, cols []string, i int) (rowMap, error) {
	r := make(rowMap, len(cols))
	sc := batch.Schema()
	for _, c := range cols {
		idxs := sc.FieldIndices(c)
		if len(idxs) == 0 {
			return nil, fmt.Errorf("simple_writable: column %q missing from input batch", c)
		}
		v, err := cellValue(batch.Column(idxs[0]), i)
		if err != nil {
			return nil, err
		}
		r[c] = v
	}
	return r, nil
}

// emitRowsOrCount emits the affected rows (user schema) when returning, else a
// 1-row (count) batch. Releases the batch it emits.
func emitRowsOrCount(out *vgirpc.OutputCollector, us *arrow.Schema, rows []rowMap, returning bool) error {
	if !returning {
		counts, err := buildColumn(countSchema.Field(0), []any{int64(len(rows))})
		if err != nil {
			return err
		}
		batch := array.NewRecordBatch(countSchema, []arrow.Array{counts}, 1)
		counts.Release()
		return out.Emit(batch)
	}
	cols := make([]arrow.Array, us.NumFields())
	for fi, f := range us.Fields() {
		vals := make([]any, len(rows))
		for ri, r := range rows {
			vals[ri] = r[f.Name]
		}
		arr, err := buildColumn(f, vals)
		if err != nil {
			return err
		}
		cols[fi] = arr
	}
	batch := array.NewRecordBatch(us, cols, int64(len(rows)))
	for _, c := range cols {
		c.Release()
	}
	return out.Emit(batch)
}

func writeArgSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "table_name", Position: 0, ArrowType: "varchar", IsConst: true, ArrowDataType: arrow.BinaryTypes.String, Doc: "Target table"},
		{Name: "data", Position: 1, ArrowType: "table", Doc: "Rows to write"},
		{Name: "write_options", Position: -1, IsConst: true, HasDefault: true, ArrowType: "blob", ArrowDataType: arrow.BinaryTypes.Binary, Doc: "Serialized write options (return_chunks, ...)"},
	}
}

// ---------------------------------------------------------------------------
// scan
// ---------------------------------------------------------------------------

type scanState struct{ Done bool }

type scanFn struct{}

var _ vgi.TypedTableFunc[scanState] = scanFn{}

func (scanFn) Name() string { return "simple_writable_scan" }
func (scanFn) Metadata() vgi.FunctionMetadata {
	// Projection pushdown so DuckDB controls the output columns: a plain SELECT
	// gets only its columns (no rowid), while UPDATE/DELETE request rowid in the
	// projection. Process builds exactly params.OutputSchema. Without this, the
	// scan always emits the is_row_id rowid column, which DuckDB mishandles.
	return vgi.FunctionMetadata{Description: "Scan a simple_writable table", Stability: vgi.StabilityConsistent, ProjectionPushdown: true}
}
func (scanFn) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "table_name", Position: 0, ArrowType: "varchar", IsConst: true, ArrowDataType: arrow.BinaryTypes.String, Doc: "Table to scan"},
	}
}

func (scanFn) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	table, err := tableArg(params.Args)
	if err != nil {
		return nil, err
	}
	us, ok := userSchema(table)
	if !ok {
		return nil, fmt.Errorf("simple_writable: unknown table %q", table)
	}
	fields := append(append([]arrow.Field{}, us.Fields()...), rowIDField())
	return vgi.BindSchema(arrow.NewSchema(fields, nil))
}

func (scanFn) NewState(params *vgi.ProcessParams) (*scanState, error) { return &scanState{}, nil }

func (scanFn) Process(ctx context.Context, params *vgi.ProcessParams, state *scanState, out *vgirpc.OutputCollector) error {
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
	rows, err := scanRows(st, table)
	if err != nil {
		return err
	}
	outSchema := params.OutputSchema
	cols := make([]arrow.Array, outSchema.NumFields())
	for fi, f := range outSchema.Fields() {
		vals := make([]any, len(rows))
		for ri, r := range rows {
			if f.Name == "rowid" {
				vals[ri] = r.rid
			} else {
				vals[ri] = r.cols[f.Name]
			}
		}
		arr, err := buildColumn(f, vals)
		if err != nil {
			return err
		}
		cols[fi] = arr
	}
	// NewRecordBatch retains cols, so release our refs; do NOT release batch —
	// the framework's producer loop reads then releases the emitted batch.
	batch := array.NewRecordBatch(outSchema, cols, int64(len(rows)))
	for _, c := range cols {
		c.Release()
	}
	return out.Emit(batch)
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

type insertFn struct{}

var _ vgi.TypedTableInOutFunc[noState] = insertFn{}

func (insertFn) Name() string { return "simple_writable_insert" }
func (insertFn) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "INSERT into a simple_writable table", Stability: vgi.StabilityConsistent}
}
func (insertFn) ArgumentSpecs() []vgi.ArgSpec { return writeArgSpecs() }

func (insertFn) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	table, err := tableArg(params.Args)
	if err != nil {
		return nil, err
	}
	us, ok := userSchema(table)
	if !ok {
		return nil, fmt.Errorf("simple_writable: unknown table %q", table)
	}
	if returnChunks(params.Args) {
		return &vgi.BindResponse{OutputSchema: us}, nil
	}
	return &vgi.BindResponse{OutputSchema: countSchema}, nil
}

func (insertFn) NewState(params *vgi.ProcessParams) (*noState, error) { return &noState{}, nil }

func (insertFn) Process(ctx context.Context, params *vgi.ProcessParams, _ *noState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	inserted, err := insertRows(params, batch)
	if err != nil {
		return err
	}
	us, _ := userSchema(mustTable(params))
	return emitRowsOrCount(out, us, inserted, wantReturning(params.OutputSchema))
}

func (insertFn) Finalize(ctx context.Context, params *vgi.ProcessParams, _ *noState) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// insertRows appends the input batch's rows to the table and returns the
// inserted row maps (for RETURNING).
func insertRows(params *vgi.ProcessParams, batch arrow.RecordBatch) ([]rowMap, error) {
	table, err := tableArg(params.Args)
	if err != nil {
		return nil, err
	}
	us, ok := userSchema(table)
	if !ok {
		return nil, fmt.Errorf("simple_writable: unknown table %q", table)
	}
	st, err := params.Storage.AttachStore(params.AttachScope)
	if err != nil {
		return nil, err
	}
	colNames := make([]string, us.NumFields())
	for i, f := range us.Fields() {
		colNames[i] = f.Name
	}
	base, err := allocRowids(st, table, batch.NumRows())
	if err != nil {
		return nil, err
	}
	inserted := make([]rowMap, 0, batch.NumRows())
	for i := 0; i < int(batch.NumRows()); i++ {
		r, err := rowMapFromBatch(batch, colNames, i)
		if err != nil {
			return nil, err
		}
		if err := putRow(st, table, base+int64(i), r); err != nil {
			return nil, err
		}
		inserted = append(inserted, r)
	}
	return inserted, nil
}

func mustTable(params *vgi.ProcessParams) string {
	t, _ := tableArg(params.Args)
	return t
}

// ---------------------------------------------------------------------------
// update
// ---------------------------------------------------------------------------

type updateFn struct{}

var _ vgi.TypedTableInOutFunc[noState] = updateFn{}

func (updateFn) Name() string { return "simple_writable_update" }
func (updateFn) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "UPDATE a simple_writable table", Stability: vgi.StabilityConsistent}
}
func (updateFn) ArgumentSpecs() []vgi.ArgSpec                         { return writeArgSpecs() }
func (updateFn) NewState(params *vgi.ProcessParams) (*noState, error) { return &noState{}, nil }
func (updateFn) Finalize(ctx context.Context, params *vgi.ProcessParams, _ *noState) ([]arrow.RecordBatch, error) {
	return nil, nil
}

func (updateFn) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return writeOnBind(params)
}

func (updateFn) Process(ctx context.Context, params *vgi.ProcessParams, _ *noState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	table, err := tableArg(params.Args)
	if err != nil {
		return err
	}
	us, ok := userSchema(table)
	if !ok {
		return fmt.Errorf("simple_writable: unknown table %q", table)
	}
	st, err := params.Storage.AttachStore(params.AttachScope)
	if err != nil {
		return err
	}
	ridIdx := batch.Schema().FieldIndices("rowid")
	if len(ridIdx) == 0 {
		return fmt.Errorf("simple_writable: update input missing rowid")
	}
	ridCol := batch.Column(ridIdx[0])
	var updateCols []string
	for _, f := range batch.Schema().Fields() {
		if f.Name != "rowid" {
			updateCols = append(updateCols, f.Name)
		}
	}
	updated := make([]rowMap, 0, batch.NumRows())
	for i := 0; i < int(batch.NumRows()); i++ {
		rv, err := cellValue(ridCol, i)
		if err != nil {
			return err
		}
		rid, ok := rv.(int64)
		if !ok {
			return fmt.Errorf("simple_writable: rowid is not int64")
		}
		existing, err := getRow(st, table, rid)
		if err != nil {
			return err
		}
		if existing == nil {
			return fmt.Errorf("simple_writable: update target rowid %d not in table %s", rid, table)
		}
		patch, err := rowMapFromBatch(batch, updateCols, i)
		if err != nil {
			return err
		}
		for k, v := range patch {
			existing[k] = v
		}
		if err := putRow(st, table, rid, existing); err != nil {
			return err
		}
		updated = append(updated, existing)
	}
	return emitRowsOrCount(out, us, updated, wantReturning(params.OutputSchema))
}

// ---------------------------------------------------------------------------
// delete
// ---------------------------------------------------------------------------

type deleteFn struct{}

var _ vgi.TypedTableInOutFunc[noState] = deleteFn{}

func (deleteFn) Name() string { return "simple_writable_delete" }
func (deleteFn) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "DELETE from a simple_writable table", Stability: vgi.StabilityConsistent}
}
func (deleteFn) ArgumentSpecs() []vgi.ArgSpec                         { return writeArgSpecs() }
func (deleteFn) NewState(params *vgi.ProcessParams) (*noState, error) { return &noState{}, nil }
func (deleteFn) Finalize(ctx context.Context, params *vgi.ProcessParams, _ *noState) ([]arrow.RecordBatch, error) {
	return nil, nil
}

func (deleteFn) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return writeOnBind(params)
}

func (deleteFn) Process(ctx context.Context, params *vgi.ProcessParams, _ *noState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	table, err := tableArg(params.Args)
	if err != nil {
		return err
	}
	us, ok := userSchema(table)
	if !ok {
		return fmt.Errorf("simple_writable: unknown table %q", table)
	}
	st, err := params.Storage.AttachStore(params.AttachScope)
	if err != nil {
		return err
	}
	ridIdx := batch.Schema().FieldIndices("rowid")
	if len(ridIdx) == 0 {
		return fmt.Errorf("simple_writable: delete input missing rowid")
	}
	ridCol := batch.Column(ridIdx[0])
	deleted := make([]rowMap, 0, batch.NumRows())
	for i := 0; i < int(batch.NumRows()); i++ {
		rv, err := cellValue(ridCol, i)
		if err != nil {
			return err
		}
		rid, ok := rv.(int64)
		if !ok {
			return fmt.Errorf("simple_writable: rowid is not int64")
		}
		existing, err := getRow(st, table, rid)
		if err != nil {
			return err
		}
		if existing == nil {
			return fmt.Errorf("simple_writable: delete target rowid %d not in table %s", rid, table)
		}
		if err := deleteRow(st, table, rid); err != nil {
			return err
		}
		deleted = append(deleted, existing)
	}
	return emitRowsOrCount(out, us, deleted, wantReturning(params.OutputSchema))
}

// writeOnBind is the shared OnBind for update/delete: user schema when
// returning, else the count schema.
func writeOnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	table, err := tableArg(params.Args)
	if err != nil {
		return nil, err
	}
	us, ok := userSchema(table)
	if !ok {
		return nil, fmt.Errorf("simple_writable: unknown table %q", table)
	}
	if returnChunks(params.Args) {
		return &vgi.BindResponse{OutputSchema: us}, nil
	}
	return &vgi.BindResponse{OutputSchema: countSchema}, nil
}

// ---------------------------------------------------------------------------
// broken-returning insert
// ---------------------------------------------------------------------------

type brokenInsertFn struct{}

var _ vgi.TypedTableInOutFunc[noState] = brokenInsertFn{}

func (brokenInsertFn) Name() string { return "simple_writable_broken_returning_insert" }
func (brokenInsertFn) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "Misbehaving INSERT that always emits a count batch", Stability: vgi.StabilityConsistent}
}
func (brokenInsertFn) ArgumentSpecs() []vgi.ArgSpec                         { return writeArgSpecs() }
func (brokenInsertFn) NewState(params *vgi.ProcessParams) (*noState, error) { return &noState{}, nil }
func (brokenInsertFn) Finalize(ctx context.Context, params *vgi.ProcessParams, _ *noState) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// OnBind always advertises the count surface, even when return_chunks=true —
// the mismatch is what exercises the extension's runtime RETURNING validation.
func (brokenInsertFn) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: countSchema}, nil
}

func (brokenInsertFn) Process(ctx context.Context, params *vgi.ProcessParams, _ *noState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	if _, err := insertRows(params, batch); err != nil {
		return err
	}
	// Always emit count regardless of what was requested — that's the bug.
	return emitRowsOrCount(out, nil, make([]rowMap, batch.NumRows()), false)
}
