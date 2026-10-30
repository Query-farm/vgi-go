// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// The VGI client decodes protocol records with generated, strict codecs. Two
// properties the schema-shape test (wire_schema_completeness_test.go) does not
// pin are checked here:
//
//   - A dictionary-encoded enum column carries the Python enum member NAME
//     (vgi-python is the wire's source of truth): function_type is SCALAR /
//     TABLE / AGGREGATE / TABLE_BUFFERING, macro_type is SCALAR / TABLE. This
//     SDK used to send the lower-case values ("scalar", "table"), which the
//     old client tolerated case-insensitively and the strict one rejects.
//   - No null lands in a non-nullable column, list item, struct child or map
//     key, at any depth — for a fully populated record AND for a record built
//     from a near-zero-value struct (the shape where a builder is most likely
//     to reach for AppendNull).

// wireEnumMemberNames is, per dictionary-encoded column of a record this SDK
// builds, the set of Python enum member names the protocol allows.
var wireEnumMemberNames = map[string]map[string][]string{
	"FunctionInfo": {
		"function_type":      {"SCALAR", "AGGREGATE", "TABLE", "TABLE_BUFFERING"},
		"stability":          {"CONSISTENT", "VOLATILE", "CONSISTENT_WITHIN_QUERY"},
		"null_handling":      {"DEFAULT", "SPECIAL"},
		"order_preservation": {"PRESERVES_ORDER", "NO_ORDER_GUARANTEE", "FIXED_ORDER"},
		"partition_kind":     {"NOT_PARTITIONED", "SINGLE_VALUE_PARTITIONS", "OVERLAPPING_PARTITIONS", "DISJOINT_PARTITIONS"},
		"order_dependent":    {"ORDER_DEPENDENT", "NOT_ORDER_DEPENDENT"},
		"distinct_dependent": {"DISTINCT_DEPENDENT", "NOT_DISTINCT_DEPENDENT"},
	},
	"MacroInfo": {
		"macro_type": {"SCALAR", "TABLE"},
	},
}

// readSingleRecord decodes a one-record IPC stream.
func readSingleRecord(t *testing.T, origin string, data []byte) arrow.RecordBatch {
	t.Helper()
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("%s: reading IPC: %v", origin, err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatalf("%s: IPC stream carried no record batch", origin)
	}
	rec := reader.RecordBatch()
	rec.Retain()
	return rec
}

// assertNoIllegalNulls walks every column of rec against schema and reports a
// null anywhere the schema says non-nullable.
func assertNoIllegalNulls(t *testing.T, origin string, schema *arrow.Schema, rec arrow.RecordBatch) {
	t.Helper()
	for i, f := range schema.Fields() {
		for _, msg := range illegalNulls(f.Name, f, rec.Column(i)) {
			t.Errorf("%s: %s", origin, msg)
		}
	}
}

func illegalNulls(path string, f arrow.Field, arr arrow.Array) []string {
	var out []string
	if !f.Nullable && arr.NullN() > 0 {
		out = append(out, fmt.Sprintf("%s is non-nullable but has %d null(s)", path, arr.NullN()))
	}
	switch a := arr.(type) {
	case *array.Map:
		mt := f.Type.(*arrow.MapType)
		keyField := mt.KeyField()
		itemField := mt.ItemField()
		out = append(out, illegalNulls(path+".key", keyField, a.Keys())...)
		out = append(out, illegalNulls(path+".value", itemField, a.Items())...)
	case array.ListLike:
		elem := f.Type.(arrow.ListLikeType).ElemField()
		out = append(out, illegalNulls(path+"[]", elem, a.ListValues())...)
	case *array.Struct:
		st := f.Type.(*arrow.StructType)
		for j := 0; j < st.NumFields(); j++ {
			cf := st.Field(j)
			out = append(out, illegalNulls(path+"."+cf.Name, cf, a.Field(j))...)
		}
	}
	return out
}

// assertEnumMemberNames checks every string dictionary column of rec carries
// only values from the record's allowed member-name sets. Binary metadata
// dictionaries are tested separately for byte equality and required values.
func assertEnumMemberNames(t *testing.T, origin string, rec arrow.RecordBatch) {
	t.Helper()
	allowed := wireEnumMemberNames[origin]
	for i, f := range rec.Schema().Fields() {
		dict, ok := rec.Column(i).(*array.Dictionary)
		if !ok || dict.Dictionary().DataType().ID() != arrow.STRING {
			continue
		}
		names, known := allowed[f.Name]
		if !known {
			t.Errorf("%s: dictionary column %q has no entry in wireEnumMemberNames", origin, f.Name)
			continue
		}
		values := dict.Dictionary().(*array.String)
		for row := 0; row < dict.Len(); row++ {
			if dict.IsNull(row) {
				continue
			}
			got := values.Value(dict.GetValueIndex(row))
			found := false
			for _, n := range names {
				if got == n {
					found = true
				}
			}
			if !found {
				t.Errorf("%s: %s = %q, not a protocol enum member name (want one of %v)", origin, f.Name, got, names)
			}
		}
	}
}

// dictValue reads row 0 of a dictionary-encoded string column.
func dictValue(t *testing.T, rec arrow.RecordBatch, name string) string {
	t.Helper()
	idx := rec.Schema().FieldIndices(name)
	if len(idx) != 1 {
		t.Fatalf("no column %q", name)
	}
	dict := rec.Column(idx[0]).(*array.Dictionary)
	return dict.Dictionary().(*array.String).Value(dict.GetValueIndex(0))
}

// TestFunctionTypeWireValues pins function_type to the member names, for the
// constants and for the legacy lower-case spellings a caller may still put in a
// FunctionInfo by hand.
func TestFunctionTypeWireValues(t *testing.T) {
	cases := []struct {
		in   FunctionType
		want string
	}{
		{FunctionTypeScalar, "SCALAR"},
		{FunctionTypeTable, "TABLE"},
		{FunctionTypeAggregate, "AGGREGATE"},
		{FunctionTypeTableBuffering, "TABLE_BUFFERING"},
		{"scalar", "SCALAR"},
		{"table", "TABLE"},
		{"aggregate", "AGGREGATE"},
		{"table_buffering", "TABLE_BUFFERING"},
	}
	for _, tc := range cases {
		data, err := SerializeFunctionInfo(&FunctionInfo{Name: "f", FunctionType: tc.in})
		if err != nil {
			t.Fatalf("SerializeFunctionInfo(%q): %v", tc.in, err)
		}
		rec := readSingleRecord(t, "FunctionInfo", data)
		if got := dictValue(t, rec, "function_type"); got != tc.want {
			t.Errorf("function_type for %q: got %q, want %q", tc.in, got, tc.want)
		}
		rec.Release()
	}
	for _, bad := range []FunctionType{"", "table_in_out", "TABLE_IN_OUT", "bogus"} {
		if _, err := SerializeFunctionInfo(&FunctionInfo{Name: "f", FunctionType: bad}); err == nil {
			t.Errorf("SerializeFunctionInfo accepted function_type %q; the strict client would reject it", bad)
		}
	}
}

// TestMacroTypeWireValues pins macro_type to the member names.
func TestMacroTypeWireValues(t *testing.T) {
	cases := []struct {
		in   MacroType
		want string
	}{
		{MacroTypeScalar, "SCALAR"},
		{MacroTypeTable, "TABLE"},
		{"scalar", "SCALAR"},
		{"table", "TABLE"},
	}
	for _, tc := range cases {
		info, err := macroInfoFromCatalogMacro(CatalogMacro{Name: "m", MacroType: tc.in, Definition: "1"}, SchemaPath{"main"})
		if err != nil {
			t.Fatal(err)
		}
		data, err := SerializeMacroInfo(info)
		if err != nil {
			t.Fatalf("SerializeMacroInfo(%q): %v", tc.in, err)
		}
		rec := readSingleRecord(t, "MacroInfo", data)
		if got := dictValue(t, rec, "macro_type"); got != tc.want {
			t.Errorf("macro_type for %q: got %q, want %q", tc.in, got, tc.want)
		}
		rec.Release()
	}
	for _, bad := range []MacroType{"", "scalar_macro", "bogus"} {
		if _, err := SerializeMacroInfo(&MacroInfo{Name: "m", MacroType: bad}); err == nil {
			t.Errorf("SerializeMacroInfo accepted macro_type %q", bad)
		}
	}
}

// TestDecodedFunctionTypeIsCanonical: whatever spelling the client sends in
// BindRequest.function_type (the member name, DuckDB's *_FUNCTION catalog type,
// or a legacy lower-case value), the SDK hands functions the constant.
func TestDecodedFunctionTypeIsCanonical(t *testing.T) {
	for in, want := range map[string]FunctionType{
		"SCALAR": FunctionTypeScalar, "SCALAR_FUNCTION": FunctionTypeScalar, "scalar": FunctionTypeScalar,
		"TABLE": FunctionTypeTable, "TABLE_FUNCTION": FunctionTypeTable, "table": FunctionTypeTable,
		"AGGREGATE": FunctionTypeAggregate, "AGGREGATE_FUNCTION": FunctionTypeAggregate, "aggregate": FunctionTypeAggregate,
		"TABLE_BUFFERING": FunctionTypeTableBuffering, "table_buffering": FunctionTypeTableBuffering,
	} {
		if got := normalizeFunctionType(FunctionType(in)); got != want {
			t.Errorf("normalizeFunctionType(%q) = %q, want %q", in, got, want)
		}
	}
	w := NewWorker()
	params, err := w.parseBindRequest(BindRequestWire{FunctionName: "f", FunctionType: "SCALAR", Arguments: mustEmptyArguments(t)}, nil)
	if err != nil {
		t.Fatalf("parseBindRequest: %v", err)
	}
	if params.FunctionType != FunctionTypeScalar {
		t.Errorf("BindParams.FunctionType = %q, want %q", params.FunctionType, FunctionTypeScalar)
	}
}

func mustEmptyArguments(t *testing.T) []byte {
	t.Helper()
	data, err := SerializeRecordBatch(array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 1))
	if err != nil {
		t.Fatalf("serializing empty arguments: %v", err)
	}
	return data
}

// minimalWireRecordCases builds each hand-assembled record from a struct with
// only its identifying fields set.
func minimalWireRecordCases() []wireRecordCase {
	return []wireRecordCase{
		{"CatalogInfo", generated.CatalogInfoSchema, func() ([]byte, error) {
			return SerializeCatalogInfo(&CatalogInfo{Name: "c"})
		}, nil},
		{"SchemaInfo", generated.SchemaInfoSchema, func() ([]byte, error) {
			return SerializeSchemaInfo(&SchemaInfo{Path: SchemaPath{"main"}})
		}, nil},
		{"TableInfo", generated.TableInfoSchema, func() ([]byte, error) {
			return SerializeTableInfo(&TableInfo{Name: "t", SchemaPath: []string{"main"},
				Columns: arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)})
		}, nil},
		{"ViewInfo", generated.ViewInfoSchema, func() ([]byte, error) {
			return SerializeViewInfo(&ViewInfo{Name: "v", Definition: "SELECT 1"})
		}, nil},
		{"FunctionInfo", generated.FunctionInfoSchema, func() ([]byte, error) {
			return SerializeFunctionInfo(&FunctionInfo{Name: "f", FunctionType: FunctionTypeTable})
		}, nil},
		{"MacroInfo", generated.MacroInfoSchema, func() ([]byte, error) {
			info, err := macroInfoFromCatalogMacro(CatalogMacro{Name: "m", MacroType: MacroTypeTable, Definition: "SELECT 1"}, SchemaPath{"main"})
			if err != nil {
				return nil, err
			}
			return SerializeMacroInfo(info)
		}, nil},
		{"CopyFromFormatInfo", generated.CopyFromFormatInfoSchema, func() ([]byte, error) {
			return SerializeCopyFromFormatInfo(copyFromFormatRecord{formatName: "x", handler: "h", direction: "from"})
		}, nil},
		{"ScanFunctionResult", generated.ScanFunctionResultSchema, func() ([]byte, error) {
			return SerializeScanFunctionResult(&ScanFunctionResult{FunctionName: "read_parquet"})
		}, nil},
		{"ScanBranch", generated.ScanBranchSchema, func() ([]byte, error) {
			return SerializeScanBranch(&ScanBranch{FunctionName: "read_parquet"})
		}, nil},
		{"ScanSplit", generated.ScanSplitSchema, func() ([]byte, error) {
			return SerializeScanSplit(&ScanSplit{})
		}, nil},
		{"ForeignKeyInfo", generated.ForeignKeyInfoSchema, func() ([]byte, error) {
			return serializeForeignKey(SchemaPath{"main"}, &ForeignKeyConstraint{ReferencedTable: "t"})
		}, nil},
		{"AttachCatalogInfo", generated.AttachCatalogInfoSchema, func() ([]byte, error) {
			return SerializeAttachCatalogInfo(AttachCatalogInfo{Alias: "a", Target: "x", DBType: "duckdb"})
		}, nil},
	}
}

func TestWireRecordsHaveNoIllegalNullsOrEnumValues(t *testing.T) {
	for _, set := range []struct {
		name  string
		cases []wireRecordCase
	}{
		{"populated", wireRecordCases(t)},
		{"minimal", minimalWireRecordCases()},
	} {
		for _, tc := range set.cases {
			t.Run(set.name+"/"+tc.origin, func(t *testing.T) {
				data, err := tc.build()
				if err != nil {
					t.Fatalf("build: %v", err)
				}
				rec := readSingleRecord(t, tc.origin, data)
				defer rec.Release()
				assertSchemaMatches(t, tc.origin, rec.Schema(), tc.schema)
				assertNoIllegalNulls(t, tc.origin, tc.schema, rec)
				assertEnumMemberNames(t, tc.origin, rec)
			})
		}
	}
}
