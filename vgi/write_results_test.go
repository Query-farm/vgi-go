// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var writeTestColumns = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	RowIDField("", nil),
}, nil)

func TestRowIDFieldAndUserColumns(t *testing.T) {
	f := RowIDField("", nil)
	if f.Name != "rowid" || !arrow.TypeEqual(f.Type, arrow.PrimitiveTypes.Int64) || f.Nullable || !IsRowIDField(f) {
		t.Fatalf("RowIDField default: %+v", f)
	}
	if g := RowIDField("rid", arrow.PrimitiveTypes.Int32); g.Name != "rid" || !arrow.TypeEqual(g.Type, arrow.PrimitiveTypes.Int32) {
		t.Fatalf("RowIDField custom: %+v", g)
	}
	user := UserColumns(writeTestColumns)
	if user.NumFields() != 2 || user.Field(1).Name != "name" {
		t.Fatalf("UserColumns: %v", user)
	}
}

func TestWriteResultModesOrder(t *testing.T) {
	cases := []struct {
		max, req WriteResultMode
		want     bool
	}{
		{WriteResultChanges, WriteResultCount, true},
		{WriteResultChanges, WriteResultRows, true},
		{WriteResultRows, WriteResultChanges, false},
		{WriteResultCount, WriteResultRows, false},
		{WriteResultCount, "bogus", false},
	}
	for _, c := range cases {
		if got := SupportsWriteResultMode(c.max, c.req); got != c.want {
			t.Errorf("SupportsWriteResultMode(%s, %s) = %v", c.max, c.req, got)
		}
	}
}

func TestWriteResultSchemasAndBatches(t *testing.T) {
	count, _ := WriteResultSchema(WriteResultCount, writeTestColumns)
	if count.NumFields() != 1 || count.Field(0).Name != "count" || count.Field(0).Nullable {
		t.Fatalf("count schema %v", count)
	}
	rows, _ := WriteResultSchema(WriteResultRows, writeTestColumns)
	if rows.NumFields() != 2 {
		t.Fatalf("rows schema keeps the row id: %v", rows)
	}
	changes, _ := WriteResultSchema(WriteResultChanges, writeTestColumns)
	if changes.NumFields() != 2 || changes.Field(0).Name != "old" || changes.Field(0).Type.ID() != arrow.STRUCT {
		t.Fatalf("changes schema %v", changes)
	}
	if _, err := WriteResultSchema("bogus", writeTestColumns); err == nil {
		t.Fatal("unknown mode accepted")
	}

	olds := []map[string]any{{"id": int32(1), "name": "a"}, {"id": int32(2), "name": "b"}}
	news := []map[string]any{{"id": int32(1), "name": "A"}, nil}

	b, err := WriteResultBatch(WriteResultCount, writeTestColumns, olds, news)
	if err != nil || b.NumRows() != 1 || b.Column(0).(*array.Int64).Value(0) != 2 {
		t.Fatalf("count batch %v %v", b, err)
	}
	b, err = WriteResultBatch(WriteResultRows, writeTestColumns, olds, news)
	if err != nil {
		t.Fatal(err)
	}
	// RETURNING shows the new image, or the old one where there is none.
	if names := b.Column(1).(*array.String); names.Value(0) != "A" || names.Value(1) != "b" {
		t.Fatalf("rows batch %v", b)
	}
	b, err = WriteResultBatch(WriteResultChanges, writeTestColumns, olds, news)
	if err != nil {
		t.Fatal(err)
	}
	newCol := b.Column(1).(*array.Struct)
	if b.NumRows() != 2 || newCol.IsNull(0) || !newCol.IsNull(1) || newCol.Field(1).(*array.String).Value(0) != "A" {
		t.Fatalf("changes batch %v", b)
	}
	if _, err := WriteChangesBatch(writeTestColumns, olds, news[:1]); err == nil {
		t.Fatal("unequal old/new accepted")
	}
}

// writeOptionsArgs builds Arguments carrying a write_options blob as DuckDB
// sends it.
func writeOptionsArgs(t *testing.T, mode string, onConflict string, cols []string) *Arguments {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "result_mode", Type: arrow.BinaryTypes.String},
		{Name: "on_conflict", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "on_conflict_columns", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
	}, nil)
	bb := NewBatchBuilder(schema)
	anyCols := make([]any, len(cols))
	for i, c := range cols {
		anyCols[i] = c
	}
	if err := bb.AppendRow(map[string]any{"result_mode": mode, "on_conflict": onConflict, "on_conflict_columns": anyCols}); err != nil {
		t.Fatal(err)
	}
	batch, err := bb.Build()
	if err != nil {
		t.Fatal(err)
	}
	blob, err := SerializeRecordBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	b := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	b.Append(blob)
	return &Arguments{Named: map[string]arrow.Array{WriteOptionsArgName: b.NewArray()}}
}

func TestParseWriteOptions(t *testing.T) {
	opts, err := ParseWriteOptions(nil)
	if err != nil || opts.ResultMode != WriteResultCount || opts.OnConflict != "throw" {
		t.Fatalf("absent options: %+v %v", opts, err)
	}
	opts, err = ParseWriteOptions(writeOptionsArgs(t, "rows", "nothing", []string{"id"}))
	if err != nil || opts.ResultMode != WriteResultRows || opts.OnConflict != "nothing" || len(opts.OnConflictColumns) != 1 || opts.OnConflictColumns[0] != "id" {
		t.Fatalf("options: %+v %v", opts, err)
	}
	if _, err := ParseWriteOptions(writeOptionsArgs(t, "bogus", "throw", nil)); err == nil {
		t.Fatal("unknown result_mode accepted")
	}
	spec := WriteOptionsArgSpec()
	if spec.Name != WriteOptionsArgName || spec.Position != -1 || !spec.IsConst {
		t.Fatalf("WriteOptionsArgSpec %+v", spec)
	}
}

// stubInOut is a minimal table in-out function for write-declaration tests.
type stubInOut struct{ name string }

func (s stubInOut) Name() string                                  { return s.name }
func (stubInOut) Metadata() FunctionMetadata                      { return FunctionMetadata{} }
func (stubInOut) ArgumentSpecs() []ArgSpec                        { return []ArgSpec{WriteOptionsArgSpec()} }
func (stubInOut) OnBind(*BindParams) (*BindResponse, error)       { return BindSchema(writeTestColumns) }
func (stubInOut) OnInit(*InitParams) (*GlobalInitResponse, error) { return &GlobalInitResponse{}, nil }
func (stubInOut) NewState(*ProcessParams) (interface{}, error)    { return nil, nil }
func (stubInOut) Process(context.Context, *ProcessParams, interface{}, arrow.RecordBatch, *vgirpc.OutputCollector) error {
	return nil
}
func (stubInOut) Finalize(context.Context, *ProcessParams, interface{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

type stubScan struct{}

func (stubScan) Name() string                               { return "stub_scan" }
func (stubScan) Metadata() FunctionMetadata                 { return FunctionMetadata{ProjectionPushdown: true} }
func (stubScan) ArgumentSpecs() []ArgSpec                   { return nil }
func (stubScan) OnBind(*BindParams) (*BindResponse, error)  { return BindSchema(writeTestColumns) }
func (stubScan) NewState(*ProcessParams) (*struct{}, error) { return &struct{}{}, nil }
func (stubScan) Process(context.Context, *ProcessParams, *struct{}, *vgirpc.OutputCollector) error {
	return nil
}

func TestCatalogTableWriteDeclarationValidation(t *testing.T) {
	cases := map[string]CatalogTable{
		"mode without function": {Name: "t", Columns: writeTestColumns, InsertFunction: stubInOut{"ins"},
			WriteResultModes: map[WriteOp]WriteResultMode{WriteOpDelete: WriteResultRows}},
		"unknown mode": {Name: "t", Columns: writeTestColumns, InsertFunction: stubInOut{"ins"},
			WriteResultModes: map[WriteOp]WriteResultMode{WriteOpInsert: "everything"}},
		"update without scan": {Name: "t", Columns: writeTestColumns, UpdateFunction: stubInOut{"upd"}},
	}
	for name, ct := range cases {
		if err := ct.validateWrites(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	defer func() {
		if recover() == nil {
			t.Fatal("RegisterCatalogTable accepted an invalid write declaration")
		}
	}()
	NewWorker().RegisterCatalogTable("main", cases["update without scan"])
}

func writableTestWorker(t *testing.T, opts ...WorkerOption) *Worker {
	t.Helper()
	pk, err := ConstraintRecord(ConstraintRecordSpec{SchemaPath: []string{"main"}, Table: "items", ID: "items:pk",
		Kind: ConstraintPrimaryKey, Columns: [][]string{{"id"}}, Enforcement: ConstraintEnforced, Validation: ConstraintAllRows})
	if err != nil {
		t.Fatal(err)
	}
	w := NewWorker(append([]WorkerOption{WithCatalogName("wt")}, opts...)...)
	w.RegisterCatalogTable("main", CatalogTable{
		Name: "items", Columns: writeTestColumns, Function: AsTableFunction[struct{}](stubScan{}),
		UpdateFunction:   stubInOut{"items_update"},
		InsertFunction:   stubInOut{"items_insert"},
		WriteResultModes: map[WriteOp]WriteResultMode{WriteOpUpdate: WriteResultChanges},
		Constraints:      [][]byte{pk},
	})
	w.RegisterCatalogTable("main", CatalogTable{Name: "labels", Columns: writeTestColumns, Function: AsTableFunction[struct{}](stubScan{})})
	return w
}

func TestCatalogTableWritesServed(t *testing.T) {
	w := writableTestWorker(t)
	info, err := w.catalogTableInfo(SchemaPath{"main"}, w.findCatalogTable(SchemaPath{"main"}, "items"))
	if err != nil {
		t.Fatal(err)
	}
	if info.WriteResultModes["update"] != "changes" || info.WriteResultModes["insert"] != "count" || info.WriteResultModes["delete"] != "" {
		t.Fatalf("write_result_modes %v", info.WriteResultModes)
	}
	if info.UpdateFunction == nil || info.InsertFunction == nil || info.DeleteFunction != nil {
		t.Fatal("write functions not inlined as declared")
	}
	if _, ok := w.tableInOuts["items_update"]; !ok {
		t.Fatal("UpdateFunction not registered for dispatch")
	}
	labels, err := w.catalogTableInfo(SchemaPath{"main"}, w.findCatalogTable(SchemaPath{"main"}, "labels"))
	if err != nil || len(labels.WriteResultModes) != 0 || labels.UpdateFunction != nil {
		t.Fatalf("read-only table advertised writes: %v %v", labels.WriteResultModes, err)
	}

	resp, err := w.declarativeWriteFunctionGet(WriteOpUpdate, SchemaPath{"main"}, "items")
	if err != nil || resp.FunctionName != "items_update" {
		t.Fatalf("update function get: %+v %v", resp, err)
	}
	_, err = w.declarativeWriteFunctionGet(WriteOpUpdate, SchemaPath{"main"}, "labels")
	if err == nil || !strings.Contains(err.Error(), "does not support UPDATE") || strings.Contains(err.Error(), "constraint") {
		t.Fatalf("read-only table: %v", err)
	}
	_, err = w.declarativeWriteFunctionGet(WriteOpDelete, SchemaPath{"main"}, "items")
	if err == nil || !strings.Contains(err.Error(), "does not support DELETE") {
		t.Fatalf("no delete function: %v", err)
	}
}

func TestWritableConstrainedTableNegotiatesEnforce(t *testing.T) {
	w := writableTestWorker(t)
	if got := w.declarativeWriteProfiles("wt"); len(got) != 1 || got[0] != ConstraintEnforceProfile {
		t.Fatalf("write profiles %v", got)
	}
	ro := NewWorker(WithCatalogName("ro"))
	ro.RegisterCatalogTable("main", CatalogTable{Name: "labels", Columns: writeTestColumns, Function: AsTableFunction[struct{}](stubScan{}),
		Constraints: w.findCatalogTable(SchemaPath{"main"}, "items").Constraints})
	if got := ro.declarativeWriteProfiles("ro"); got != nil {
		t.Fatalf("read-only constrained catalog advertised %v", got)
	}
	if got := NewWorker(WithConstraintEnforceProfile()).declarativeWriteProfiles("x"); len(got) != 1 {
		t.Fatalf("WithConstraintEnforceProfile: %v", got)
	}
}

func TestPerAttachSupportsTransactions(t *testing.T) {
	w := NewWorker(WithCatalogName("c"), WithAttachValidator(func(req *CatalogAttachRequestWire, _ *vgirpc.CallContext) (*AttachDecision, error) {
		on := req.DataVersionSpec != nil && *req.DataVersionSpec == "tx"
		return &AttachDecision{SupportsTransactions: &on}, nil
	}))
	svc := workerService{Worker: w}
	tx := "tx"
	on, err := svc.CatalogAttach(context.Background(), nil, CatalogAttachRequestWire{Name: "c", DataVersionSpec: &tx})
	if err != nil || !on.SupportsTransactions {
		t.Fatalf("transactional attach: %v %v", on.SupportsTransactions, err)
	}
	off, err := svc.CatalogAttach(context.Background(), nil, CatalogAttachRequestWire{Name: "c"})
	if err != nil || off.SupportsTransactions {
		t.Fatalf("plain attach: %v %v", off.SupportsTransactions, err)
	}
	// Worker-wide default still applies when the decision leaves it nil.
	ww := workerService{Worker: NewWorker(WithCatalogName("c"), WithSupportsTransactions(true))}
	if r, _ := ww.CatalogAttach(context.Background(), nil, CatalogAttachRequestWire{Name: "c"}); !r.SupportsTransactions {
		t.Fatal("WithSupportsTransactions ignored")
	}
}

// Concurrent ATTACHes (HTTP serves them in parallel) must not share state:
// run with -race.
func TestConcurrentAttachesDoNotRace(t *testing.T) {
	w := NewWorker(WithCatalogName("c"), WithAttachValidator(func(req *CatalogAttachRequestWire, _ *vgirpc.CallContext) (*AttachDecision, error) {
		return &AttachDecision{AttachOpaqueData: []byte("c\x00" + *req.DataVersionSpec)}, nil
	}))
	w.RegisterCatalogTable("main", CatalogTable{Name: "labels", Columns: writeTestColumns, Function: AsTableFunction[struct{}](stubScan{})})
	w.catalog = NewDefaultReadOnlyCatalog(w.catalogName, w) // as a served worker has
	svc := workerService{Worker: w}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := strings.Repeat("x", i+1)
			res, err := svc.CatalogAttach(context.Background(), nil, CatalogAttachRequestWire{Name: "c", DataVersionSpec: &v})
			if err != nil {
				t.Error(err)
				return
			}
			if !strings.HasSuffix(string(res.AttachOpaqueData), v) {
				t.Errorf("attach %d got another attach's value", i)
			}
		}(i)
	}
	wg.Wait()
}

func TestTypedArgsPointerFieldsAreOptional(t *testing.T) {
	type args struct {
		Power      *bool    `vgi:"name=power"`
		Brightness *float64 `vgi:"name=brightness"`
		Color      *string  `vgi:"name=color,default=red"`
	}
	specs := DeriveArgSpecs(args{})
	if specs[0].ArrowType != "bool" || specs[1].ArrowType != "double" || specs[0].HasDefault {
		t.Fatalf("specs %+v", specs)
	}
	bb := array.NewBooleanBuilder(memory.DefaultAllocator)
	bb.Append(false)
	nb := array.NewFloat64Builder(memory.DefaultAllocator)
	nb.AppendNull()
	a := &Arguments{Named: map[string]arrow.Array{"power": bb.NewArray(), "brightness": nb.NewArray()}}
	var got args
	if err := BindArgs(a, &got); err != nil {
		t.Fatal(err)
	}
	if got.Power == nil || *got.Power {
		t.Fatalf("power: %v", got.Power)
	}
	if got.Brightness != nil {
		t.Fatalf("NULL brightness bound to %v", *got.Brightness)
	}
	if got.Color == nil || *got.Color != "red" {
		t.Fatalf("color default: %v", got.Color)
	}
}
