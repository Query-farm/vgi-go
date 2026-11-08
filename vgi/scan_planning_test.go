// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

var plannedColumns = arrow.NewSchema([]arrow.Field{
	{Name: "event_date", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
	{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
}, nil)

// plannedFilters is the pushdown_filters payload for WHERE n >= 10.
func plannedFilters(t *testing.T) []byte {
	t.Helper()
	b := array.NewInt64Builder(memory.NewGoAllocator())
	b.Append(10)
	literal := b.NewArray()
	b.Release()
	defer literal.Release()
	document := `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"query:0","revision":0,"mode":"advisory","source":"query","expression":{"node":"comparison","op":"ge","left":{"node":"column_ref","column_index":1,"column_name":"n"},"right":{"node":"literal","value_ref":0}}}]}`
	batch := filterV2Batch(t, document, []arrow.Field{{Name: "value_0", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, []arrow.Array{literal})
	defer batch.Release()
	raw, err := SerializeRecordBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestScanPlanningValidation(t *testing.T) {
	if mode, err := validateScanPlanning("t", "", false); err != nil || mode != ScanPlanningBind {
		t.Fatalf("default = %q, %v", mode, err)
	}
	if _, err := validateScanPlanning("t", "eager", false); err == nil {
		t.Fatal("unknown scan_planning accepted")
	}
	if _, err := SerializeTableInfo(&TableInfo{Name: "t", SchemaPath: []string{"main"}, Columns: plannedColumns,
		ScanPlanning: ScanPlanningFiltered, ScanFunction: []byte{1}}); err == nil {
		t.Fatal("filtered table with an inlined scan_function accepted")
	}
	if _, err := SerializeTableInfo(&TableInfo{Name: "t", SchemaPath: []string{"main"}, Columns: plannedColumns,
		ScanPlanning: "Filtered"}); err == nil {
		t.Fatal("unknown scan_planning serialized")
	}

	w := NewWorker(WithCatalogName("c"))
	if _, err := w.catalogTableInfo(SchemaPath{"main"}, &CatalogTable{Name: "f", Function: AsTableFunction[struct{}](stubScan{}),
		Columns: writeTestColumns, ScanPlanning: ScanPlanningFiltered}); err == nil {
		t.Fatal("filtered CatalogTable with a Function accepted")
	}
	info, err := w.catalogTableInfo(SchemaPath{"main"}, &CatalogTable{Name: "f", Columns: plannedColumns, ScanPlanning: ScanPlanningFiltered})
	if err != nil || info.ScanPlanning != ScanPlanningFiltered || info.ScanFunction != nil {
		t.Fatalf("filtered CatalogTable: %+v, %v", info, err)
	}
	if info, err := w.catalogTableInfo(SchemaPath{"main"}, &CatalogTable{Name: "b", Columns: plannedColumns}); err != nil || info.ScanPlanning != ScanPlanningBind {
		t.Fatalf("default CatalogTable scan planning: %v", err)
	}
}

func TestScanBranchStatisticsValidation(t *testing.T) {
	neg := int64(-1)
	if _, err := SerializeScanBranch(&ScanBranch{FunctionName: "f", RowCount: &neg}); err == nil {
		t.Fatal("negative RowCount accepted")
	}
	stat := ColumnStatistics{ColumnName: "n", Type: arrow.PrimitiveTypes.Int64, Min: int64(0), Max: int64(1), HasNotNull: true}
	if _, err := SerializeScanBranch(&ScanBranch{FunctionName: "f", ColumnStatistics: []ColumnStatistics{stat, stat}}); err == nil {
		t.Fatal("duplicate column statistics accepted")
	}
	contradictory := stat
	contradictory.HasNotNull = false
	if _, err := SerializeScanBranch(&ScanBranch{FunctionName: "f", ColumnStatistics: []ColumnStatistics{contradictory}}); err == nil ||
		!strings.Contains(err.Error(), "HasNotNull") {
		t.Fatalf("Min/Max with HasNotNull=false accepted: %v", err)
	}
	// Absent statistics stay NULL on the wire.
	data, err := SerializeScanBranch(&ScanBranch{FunctionName: "f"})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := DeserializeRecordBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()
	for _, name := range []string{"row_count", "column_statistics"} {
		if !batch.Column(batch.Schema().FieldIndices(name)[0]).IsNull(0) {
			t.Fatalf("%s should be NULL when unset", name)
		}
	}
}

func TestColumnStatisticsDateAndTimestamp(t *testing.T) {
	day := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	ts := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	data, err := SerializeColumnStatistics([]ColumnStatistics{
		{ColumnName: "d", Type: arrow.FixedWidthTypes.Date32, Min: day, Max: arrow.Date32(20730), HasNotNull: true},
		{ColumnName: "t", Type: ts, Min: day, Max: arrow.Timestamp(1), HasNotNull: true},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := DeserializeRecordBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()
	minimum := batch.Column(1).(*array.SparseUnion)
	if got := minimum.Field(0).(*array.Date32).Value(0); got != 20729 {
		t.Fatalf("date32 min = %d, want 20729", got)
	}
	if got := minimum.Field(1).(*array.Timestamp).Value(1); got != arrow.Timestamp(day.UnixMicro()) {
		t.Fatalf("timestamp min = %d", got)
	}
}

func TestPushdownFiltersMayMatch(t *testing.T) {
	i64 := func(v int64) scalar.Scalar { return scalar.NewInt64Scalar(v) }
	cmp := func(col string, op ComparisonOp, v scalar.Scalar) *ConstantFilter {
		return &ConstantFilter{columnName: col, Op: op, Value: v}
	}
	stat := func(lo, hi int64) []ColumnStatistics {
		return []ColumnStatistics{{ColumnName: "n", Type: arrow.PrimitiveTypes.Int64, Min: lo, Max: hi, HasNull: true, HasNotNull: true}}
	}
	inValues := func(values ...int64) arrow.Array {
		b := array.NewInt64Builder(memory.NewGoAllocator())
		defer b.Release()
		b.AppendValues(values, nil)
		return b.NewArray()
	}
	cases := []struct {
		name   string
		filter Filter
		stats  []ColumnStatistics
		want   bool
	}{
		{"eq inside", cmp("n", OpEQ, i64(5)), stat(0, 9), true},
		{"eq below", cmp("n", OpEQ, i64(-1)), stat(0, 9), false},
		{"eq above", cmp("n", OpEQ, i64(10)), stat(0, 9), false},
		{"lt at min", cmp("n", OpLT, i64(0)), stat(0, 9), false},
		{"le at min", cmp("n", OpLE, i64(0)), stat(0, 9), true},
		{"gt at max", cmp("n", OpGT, i64(9)), stat(0, 9), false},
		{"ge at max", cmp("n", OpGE, i64(9)), stat(0, 9), true},
		{"ne against a single value", cmp("n", OpNE, i64(5)), stat(5, 5), false},
		{"ne against a range", cmp("n", OpNE, i64(5)), stat(5, 6), true},
		{"float literal vs int stats", cmp("n", OpGT, scalar.NewFloat64Scalar(9.5)), stat(0, 9), false},
		{"string literal vs int stats is incomparable", cmp("n", OpEQ, scalar.NewStringScalar("x")), stat(0, 9), true},
		{"in all outside", &InFilter{columnName: "n", Values: inValues(-5, 20)}, stat(0, 9), false},
		{"in one inside", &InFilter{columnName: "n", Values: inValues(-5, 3)}, stat(0, 9), true},
		{"and excludes when one child does", &AndFilter{columnName: "n", Children: []Filter{cmp("n", OpGE, i64(0)), cmp("n", OpGT, i64(100))}}, stat(0, 9), false},
		{"or of other columns", &OrFilter{columnName: "n", Children: []Filter{cmp("n", OpGT, i64(100)), cmp("m", OpEQ, i64(3))}}, stat(0, 9), true},
		{"or keeps when one child may match", &OrFilter{columnName: "n", Children: []Filter{cmp("n", OpGT, i64(100)), cmp("n", OpEQ, i64(3))}}, stat(0, 9), true},
		{"or excludes when every child does", &OrFilter{columnName: "n", Children: []Filter{cmp("n", OpGT, i64(100)), cmp("n", OpLT, i64(0))}}, stat(0, 9), false},
		{"all-null column fails a comparison", cmp("n", OpEQ, i64(5)), []ColumnStatistics{{ColumnName: "n", HasNull: true}}, false},
		{"is null with nulls", &IsNullFilter{columnName: "n"}, stat(0, 9), true},
		{"is null without nulls", &IsNullFilter{columnName: "n"}, []ColumnStatistics{{ColumnName: "n", Type: arrow.PrimitiveTypes.Int64, Min: int64(0), Max: int64(9), HasNotNull: true}}, false},
		{"is not null on an all-null column", &IsNotNullFilter{columnName: "n"}, []ColumnStatistics{{ColumnName: "n", HasNull: true}}, false},
		{"is not null with values", &IsNotNullFilter{columnName: "n"}, stat(0, 9), true},
		{"contradictory stats trust the bounds", cmp("n", OpEQ, i64(5)), []ColumnStatistics{{ColumnName: "n", Type: arrow.PrimitiveTypes.Int64, Min: int64(0), Max: int64(9)}}, true},
		{"in with only nulls", &InFilter{columnName: "n", Values: inValues()}, stat(0, 9), false},
		{"other column", cmp("m", OpEQ, i64(100)), stat(0, 9), true},
		{"open lower bound", cmp("n", OpLT, i64(0)), []ColumnStatistics{{ColumnName: "n", Type: arrow.PrimitiveTypes.Int64, Max: int64(9), HasNotNull: true}}, true},
		{"string range", cmp("s", OpLT, scalar.NewStringScalar("apple")),
			[]ColumnStatistics{{ColumnName: "s", Type: arrow.BinaryTypes.String, Min: "banana", Max: "cherry", HasNotNull: true}}, false},
		{"date range", cmp("d", OpGE, scalar.NewDate32Scalar(20729)),
			[]ColumnStatistics{{ColumnName: "d", Type: arrow.FixedWidthTypes.Date32, Min: arrow.Date32(20700), Max: arrow.Date32(20728), HasNotNull: true}}, false},
		{"timestamp range", cmp("t", OpLT, scalar.NewTimestampScalar(arrow.Timestamp(1_000_000), &arrow.TimestampType{Unit: arrow.Microsecond})),
			[]ColumnStatistics{{ColumnName: "t", Type: &arrow.TimestampType{Unit: arrow.Second}, Min: int64(1), Max: int64(5), HasNotNull: true}}, false},
		{"date literal vs timestamp stats is incomparable", cmp("t", OpLT, scalar.NewDate32Scalar(0)),
			[]ColumnStatistics{{ColumnName: "t", Type: &arrow.TimestampType{Unit: arrow.Second}, Min: int64(1), Max: int64(5), HasNotNull: true}}, true},
	}
	for _, tc := range cases {
		pf := &PushdownFilters{Filters: []Filter{tc.filter}}
		if got := pf.MayMatch(tc.stats); got != tc.want {
			t.Errorf("%s: MayMatch = %v, want %v", tc.name, got, tc.want)
		}
	}
	var none *PushdownFilters
	if !none.MayMatch(stat(0, 1)) {
		t.Fatal("nil filters must match")
	}
	if !(&ScanPlanningFilters{Raw: []byte("not arrow")}).MayMatch(stat(0, 1)) {
		t.Fatal("an undecodable payload must keep the candidate")
	}
}

func TestScanBranchesPlanHandlerDispatch(t *testing.T) {
	type call struct {
		filters *ScanPlanningFilters
	}
	var calls []call
	handled := true
	w := NewWorker(WithCatalogName("c"),
		WithAttachScanBranchesPlanHandler(func(_ []byte, _ SchemaPath, name string, _, _ *string, filters *ScanPlanningFilters) (*ScanBranchesResult, bool, error) {
			calls = append(calls, call{filters})
			if !handled {
				return nil, false, nil
			}
			var branches []ScanBranch
			for _, day := range []struct {
				file   string
				lo, hi int64
			}{{"a.parquet", 0, 9}, {"b.parquet", 10, 19}} {
				stats := []ColumnStatistics{{ColumnName: "n", Type: arrow.PrimitiveTypes.Int64, Min: day.lo, Max: day.hi, HasNotNull: true}}
				if filters.MayMatch(stats) {
					rows := day.hi - day.lo + 1
					branches = append(branches, ScanBranch{FunctionName: "read_parquet",
						PositionalArguments: []ScanArg{{Value: day.file, Type: arrow.BinaryTypes.String}},
						RowCount:            &rows, ColumnStatistics: stats})
				}
			}
			return &ScanBranchesResult{Branches: branches}, true, nil
		}),
		WithAttachScanBranchesGetHandler(func(_ []byte, _ SchemaPath, _ string, _, _ *string) (*ScanBranchesResult, bool, error) {
			return &ScanBranchesResult{Branches: []ScanBranch{{FunctionName: "fallback"}}}, true, nil
		}),
	)
	w.RegisterCatalogTable("main", CatalogTable{Name: "planned", Columns: plannedColumns, ScanPlanning: ScanPlanningFiltered})
	w.catalog = NewDefaultReadOnlyCatalog(w.catalogName, w)
	svc := workerService{Worker: w}

	raw := plannedFilters(t)
	res, err := svc.CatalogTableScanBranchesGet(context.Background(), nil, CatalogTableScanBranchesGetParams{
		SchemaPath: []string{"main"}, Name: "planned", PushdownFilters: &raw,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Branches) != 1 || calls[0].filters == nil {
		t.Fatalf("WHERE n >= 10 kept %d branches, want 1", len(res.Branches))
	}
	pf, err := calls[0].filters.Decode()
	if err != nil || len(pf.Filters) != 1 || pf.Filters[0].ColumnName() != "n" {
		t.Fatalf("decoded against the declared columns: %v, %v", pf, err)
	}
	branch, err := DeserializeRecordBatch(res.Branches[0])
	if err != nil {
		t.Fatal(err)
	}
	defer branch.Release()
	if got := branch.Column(branch.Schema().FieldIndices("row_count")[0]).(*array.Int64).Value(0); got != 10 {
		t.Fatalf("row_count = %d", got)
	}
	if branch.Column(branch.Schema().FieldIndices("column_statistics")[0]).IsNull(0) {
		t.Fatal("column_statistics dropped")
	}

	// No predicates: the hook gets nil filters and returns every branch.
	res, err = svc.CatalogTableScanBranchesGet(context.Background(), nil, CatalogTableScanBranchesGetParams{SchemaPath: []string{"main"}, Name: "planned"})
	if err != nil || len(res.Branches) != 2 || calls[1].filters != nil {
		t.Fatalf("unfiltered plan: %d branches, %v", len(res.Branches), err)
	}

	// Falling through reaches the 3.1 handler, which ignores the filters.
	handled = false
	res, err = svc.CatalogTableScanBranchesGet(context.Background(), nil, CatalogTableScanBranchesGetParams{
		SchemaPath: []string{"main"}, Name: "planned", PushdownFilters: &raw,
	})
	if err != nil || len(res.Branches) != 1 {
		t.Fatalf("fallthrough: %d branches, %v", len(res.Branches), err)
	}
}

func TestScanBranchesZeroBranchesIsLegal(t *testing.T) {
	w := NewWorker(WithCatalogName("c"),
		WithAttachScanBranchesPlanHandler(func([]byte, SchemaPath, string, *string, *string, *ScanPlanningFilters) (*ScanBranchesResult, bool, error) {
			return &ScanBranchesResult{}, true, nil
		}))
	res, err := workerService{Worker: w}.CatalogTableScanBranchesGet(context.Background(), nil, CatalogTableScanBranchesGetParams{Name: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Branches == nil || len(res.Branches) != 0 {
		t.Fatalf("zero branches must encode as an empty, non-null list: %#v", res.Branches)
	}
}
