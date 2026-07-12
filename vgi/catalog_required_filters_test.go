// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// readRequiredFilters round-trips the serialized TableInfo IPC bytes and
// extracts the trailing required_filters column (list<list<utf8>>) as [][]string.
func readRequiredFilters(t *testing.T, info *TableInfo) [][]string {
	t.Helper()
	data, err := SerializeTableInfo(info)
	if err != nil {
		t.Fatalf("SerializeTableInfo: %v", err)
	}
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ipc.NewReader: %v", err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("expected one record batch")
	}
	rec := reader.RecordBatch()

	field := rec.Schema().Field(rec.Schema().NumFields() - 1)
	if field.Name != "required_filters" {
		t.Fatalf("trailing field = %q, want required_filters", field.Name)
	}
	if !arrow.TypeEqual(field.Type, arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.String))) {
		t.Fatalf("required_filters type = %s, want list<list<utf8>>", field.Type)
	}

	outer := rec.Column(rec.Schema().NumFields() - 1).(*array.List)
	if outer.IsNull(0) {
		return nil
	}
	start, end := outer.ValueOffsets(0)
	inner := outer.ListValues().(*array.List)
	strs := inner.ListValues().(*array.String)

	var groups [][]string
	for g := start; g < end; g++ {
		gs, ge := inner.ValueOffsets(int(g))
		group := make([]string, 0, ge-gs)
		for i := gs; i < ge; i++ {
			group = append(group, strs.Value(int(i)))
		}
		groups = append(groups, group)
	}
	return groups
}

func TestSerializeTableInfoRequiredFiltersCNF(t *testing.T) {
	// A singleton group AND a genuine OR-group:
	// accession_number AND one of (ticker, cik).
	info := &TableInfo{
		Name:       "filings",
		SchemaName: "company",
		RequiredFilters: [][]string{
			{"accession_number"},
			{"ticker", "cik"},
		},
	}
	got := readRequiredFilters(t, info)
	want := [][]string{{"accession_number"}, {"ticker", "cik"}}
	if len(got) != len(want) {
		t.Fatalf("got %d groups, want %d: %v", len(got), len(want), got)
	}
	for gi := range want {
		if len(got[gi]) != len(want[gi]) {
			t.Fatalf("group %d = %v, want %v", gi, got[gi], want[gi])
		}
		for pi := range want[gi] {
			if got[gi][pi] != want[gi][pi] {
				t.Fatalf("group %d path %d = %q, want %q", gi, pi, got[gi][pi], want[gi][pi])
			}
		}
	}
}

func TestSerializeTableInfoRequiredFiltersEmpty(t *testing.T) {
	// Empty (default) => an empty, non-null outer list.
	got := readRequiredFilters(t, &TableInfo{Name: "t", SchemaName: "s"})
	if len(got) != 0 {
		t.Fatalf("expected no groups, got %v", got)
	}
}

func TestValidateRequiredFilters(t *testing.T) {
	columns := arrow.NewSchema([]arrow.Field{
		{Name: "accession_number", Type: arrow.BinaryTypes.String},
		{Name: "ticker", Type: arrow.BinaryTypes.String},
		{Name: "cik", Type: arrow.PrimitiveTypes.Int64},
		{Name: "bbox", Type: arrow.StructOf(
			arrow.Field{Name: "xmin", Type: arrow.PrimitiveTypes.Float32},
		)},
	}, nil)

	// Valid: singleton group, OR-group, and a struct-subfield path.
	if err := validateRequiredFilters("t", columns, [][]string{
		{"accession_number"},
		{"ticker", "cik"},
		{"bbox.xmin"},
	}); err != nil {
		t.Fatalf("valid CNF rejected: %v", err)
	}

	// Empty (default) => no enforcement, no error.
	if err := validateRequiredFilters("t", columns, nil); err != nil {
		t.Fatalf("empty required_filters rejected: %v", err)
	}

	// Empty group is rejected.
	if err := validateRequiredFilters("t", columns, [][]string{{}}); err == nil {
		t.Fatal("expected error for empty group")
	}

	// Empty string path is rejected.
	if err := validateRequiredFilters("t", columns, [][]string{{""}}); err == nil {
		t.Fatal("expected error for empty string path")
	}

	// Unknown leading column is rejected.
	if err := validateRequiredFilters("t", columns, [][]string{{"nope.field"}}); err == nil {
		t.Fatal("expected error for unknown column")
	}
}
