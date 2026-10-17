// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// TestCatalogItemsCarryEnumMemberNames drives catalog_contents over HTTP
// against the shipped example functions (scalar, aggregate, table,
// table-in-out, table-buffering) plus scalar and table macros, and pins the
// function_type / macro_type each FunctionInfo / MacroInfo item carries to the
// protocol enum member names. The strict client rejects the lower-case values
// this SDK used to send.
func TestCatalogItemsCarryEnumMemberNames(t *testing.T) {
	c := newContentsClient(t, contentsWorker())
	resp := c.contents(c.attach("example").AttachOpaqueData, nil)

	seen := map[string]map[string]int{"function_type": {}, "macro_type": {}}
	check := func(items [][]byte, column string, allowed ...string) {
		t.Helper()
		for _, item := range items {
			rec, err := vgi.DeserializeRecordBatch(item)
			if err != nil {
				t.Fatalf("decoding a catalog item: %v", err)
			}
			got := dictString(t, rec, column)
			rec.Release()
			ok := false
			for _, a := range allowed {
				ok = ok || got == a
			}
			if !ok {
				t.Errorf("%s = %q, want one of %v", column, got, allowed)
			}
			seen[column][got]++
		}
	}
	for _, sc := range resp.Schemas {
		check(sc.ScalarFunctions, "function_type", "SCALAR")
		check(sc.AggregateFunctions, "function_type", "AGGREGATE")
		check(sc.TableFunctions, "function_type", "TABLE", "TABLE_BUFFERING")
		check(sc.ScalarMacros, "macro_type", "SCALAR")
		check(sc.TableMacros, "macro_type", "TABLE")
	}
	for column, values := range map[string][]string{
		"function_type": {"SCALAR", "AGGREGATE", "TABLE", "TABLE_BUFFERING"},
		"macro_type":    {"SCALAR", "TABLE"},
	} {
		for _, v := range values {
			if seen[column][v] == 0 {
				t.Errorf("fixture lists no item with %s = %s; the check proves nothing for it", column, v)
			}
		}
	}
}

func dictString(t *testing.T, rec arrow.RecordBatch, name string) string {
	t.Helper()
	dict := column[*array.Dictionary](t, rec, name)
	return dict.Dictionary().(*array.String).Value(dict.GetValueIndex(0))
}
