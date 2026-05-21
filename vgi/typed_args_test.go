// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ---------------------------------------------------------------------------
// DeriveArgSpecs
// ---------------------------------------------------------------------------

func TestDeriveArgSpecs_PrimitivesAndDefaults(t *testing.T) {
	type args struct {
		Count     int64   `vgi:"pos=0,doc=Row count"`
		BatchSize int64   `vgi:"name=batch_size,default=2048,doc=Batch size"`
		Name      string  `vgi:"pos=1"`
		Scale     float64 `vgi:"default=1.5"`
		Enabled   bool    `vgi:"default=true"`
		Skipped   int     `vgi:"-"`
		Untagged  int     // no tag = not included
	}
	specs := DeriveArgSpecs(args{})
	if len(specs) != 5 {
		t.Fatalf("expected 5 specs, got %d: %+v", len(specs), specs)
	}

	want := []ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Row count", IsConst: true, ArrowDataType: arrow.PrimitiveTypes.Int64},
		{Name: "batch_size", Position: -1, ArrowType: "int64", Doc: "Batch size", IsConst: true, HasDefault: true, DefaultValue: "2048", ArrowDataType: arrow.PrimitiveTypes.Int64},
		{Name: "name", Position: 1, ArrowType: "varchar", IsConst: true, ArrowDataType: arrow.BinaryTypes.String},
		{Name: "scale", Position: -1, ArrowType: "double", IsConst: true, HasDefault: true, DefaultValue: "1.5", ArrowDataType: arrow.PrimitiveTypes.Float64},
		{Name: "enabled", Position: -1, ArrowType: "bool", IsConst: true, HasDefault: true, DefaultValue: "true", ArrowDataType: arrow.FixedWidthTypes.Boolean},
	}
	for i, w := range want {
		got := specs[i]
		if got.Name != w.Name || got.Position != w.Position || got.ArrowType != w.ArrowType ||
			got.Doc != w.Doc || got.IsConst != w.IsConst || got.HasDefault != w.HasDefault ||
			got.DefaultValue != w.DefaultValue {
			t.Errorf("spec[%d]: got %+v want %+v", i, got, w)
		}
		if got.ArrowDataType == nil || got.ArrowDataType.String() != w.ArrowDataType.String() {
			t.Errorf("spec[%d] ArrowDataType: got %v want %v", i, got.ArrowDataType, w.ArrowDataType)
		}
	}
}

func TestDeriveArgSpecs_ComplexTypes(t *testing.T) {
	type point struct {
		X float64
		Y float64
	}
	type args struct {
		P1      point      `vgi:"pos=0,doc=First point"`
		P2      [2]float64 `vgi:"pos=1,doc=Coords"`
		Tags    []string   `vgi:"name=tags"`
		Blob    []byte     `vgi:"name=blob"`
		Generic any        `vgi:"name=gen"`
	}
	specs := DeriveArgSpecs(args{})
	if len(specs) != 5 {
		t.Fatalf("expected 5 specs, got %d", len(specs))
	}

	if specs[0].ArrowType != "struct" {
		t.Errorf("p1 ArrowType: got %q", specs[0].ArrowType)
	}
	if specs[0].ArrowDataType == nil || specs[0].ArrowDataType.ID() != arrow.STRUCT {
		t.Errorf("p1 ArrowDataType: got %v", specs[0].ArrowDataType)
	}
	st := specs[0].ArrowDataType.(*arrow.StructType)
	if st.NumFields() != 2 || st.Field(0).Name != "x" || st.Field(1).Name != "y" {
		t.Errorf("p1 struct fields: got %+v", st.Fields())
	}

	if specs[1].ArrowType != "fixed_list" {
		t.Errorf("p2 ArrowType: got %q", specs[1].ArrowType)
	}
	fsl, ok := specs[1].ArrowDataType.(*arrow.FixedSizeListType)
	if !ok || fsl.Len() != 2 || fsl.Elem().ID() != arrow.FLOAT64 {
		t.Errorf("p2 ArrowDataType: got %v", specs[1].ArrowDataType)
	}

	if specs[2].ArrowType != "list" {
		t.Errorf("tags ArrowType: got %q", specs[2].ArrowType)
	}
	lt, ok := specs[2].ArrowDataType.(*arrow.ListType)
	if !ok || lt.Elem().ID() != arrow.STRING {
		t.Errorf("tags ArrowDataType: got %v", specs[2].ArrowDataType)
	}

	if specs[3].ArrowType != "blob" || specs[3].ArrowDataType.ID() != arrow.BINARY {
		t.Errorf("blob: got %q / %v", specs[3].ArrowType, specs[3].ArrowDataType)
	}

	if specs[4].ArrowType != "any" || specs[4].ArrowDataType != nil {
		t.Errorf("gen: got %q / %v", specs[4].ArrowType, specs[4].ArrowDataType)
	}
}

func TestDeriveArgSpecs_VarargsAndConstToggle(t *testing.T) {
	type args struct {
		Count   int64    `vgi:"pos=0"`
		IntVals []int64  `vgi:"pos=1,varargs"`
		StrVals []string `vgi:"pos=1,varargs"`
		Col     any      `vgi:"pos=0,const=false,bound=addable"`
	}
	specs := DeriveArgSpecs(args{})
	if len(specs) != 4 {
		t.Fatalf("expected 4 specs, got %d", len(specs))
	}
	// Varargs advertise the element type on the wire, not the slice.
	if !specs[1].IsVarargs || specs[1].ArrowType != "int64" {
		t.Errorf("int64 varargs spec: got %+v", specs[1])
	}
	if specs[1].ArrowDataType == nil || specs[1].ArrowDataType.ID() != arrow.INT64 {
		t.Errorf("int64 varargs ArrowDataType: got %v", specs[1].ArrowDataType)
	}
	if !specs[2].IsVarargs || specs[2].ArrowType != "varchar" {
		t.Errorf("string varargs spec: got %+v", specs[2])
	}
	if specs[3].IsConst {
		t.Errorf("expected const=false on col, got %+v", specs[3])
	}
	if len(specs[3].TypeBound) != 1 {
		t.Errorf("expected 1 bound on col, got %d", len(specs[3].TypeBound))
	}
}

func TestDeriveArgSpecs_DocWithCommas(t *testing.T) {
	type args struct {
		Quoted   string `vgi:"pos=0,doc='hello, world'"`
		Unquoted string `vgi:"pos=1,doc=Station code (e.g. asd, ut, rd)"`
	}
	specs := DeriveArgSpecs(args{})
	if specs[0].Doc != "hello, world" {
		t.Errorf("quoted doc with comma: got %q", specs[0].Doc)
	}
	if specs[1].Doc != "Station code (e.g. asd, ut, rd)" {
		t.Errorf("greedy unquoted doc: got %q", specs[1].Doc)
	}
}

func TestDeriveArgSpecs_TypeOverride(t *testing.T) {
	type args struct {
		// Go type says int64, but mark it as int32 on the wire.
		Limit int64 `vgi:"pos=0,type=int32"`
	}
	specs := DeriveArgSpecs(args{})
	if specs[0].ArrowType != "int32" {
		t.Errorf("type override: got %q", specs[0].ArrowType)
	}
	if specs[0].ArrowDataType != nil {
		t.Errorf("type override should clear ArrowDataType, got %v", specs[0].ArrowDataType)
	}
}

func TestDeriveArgSpecs_UnknownTagPanics(t *testing.T) {
	type args struct {
		X int64 `vgi:"pos=0,whatever=nope"`
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on unknown tag key")
		}
	}()
	DeriveArgSpecs(args{})
}

func TestDeriveArgSpecs_UnknownBoundPanics(t *testing.T) {
	type args struct {
		X any `vgi:"pos=0,bound=hypotenuse"`
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on unknown bound")
		}
	}()
	DeriveArgSpecs(args{})
}

// ---------------------------------------------------------------------------
// BindArgs
// ---------------------------------------------------------------------------

// makeArgs builds an *Arguments from a column map. Each column is a single-row
// array; the helper wraps them in the DuckDB-style "args" struct column so the
// existing ParseArguments path is exercised end-to-end.
func makeArgs(t *testing.T, cols map[string]arrow.Array) *Arguments {
	t.Helper()
	mem := memory.NewGoAllocator()
	// Sort field names so positional_0 stays at index 0, etc. We use the
	// caller's order — Go map iteration is random, so we copy to a slice
	// caller built. Here, easier: build separate columns and let the wrapper
	// see them all.
	fields := make([]arrow.Field, 0, len(cols))
	values := make([]arrow.Array, 0, len(cols))
	for name, arr := range cols {
		fields = append(fields, arrow.Field{Name: name, Type: arr.DataType()})
		values = append(values, arr)
	}
	// Wrap in struct.
	structType := arrow.StructOf(fields...)
	structData := array.NewData(structType, 1, []*memory.Buffer{nil}, makeChildData(values), 0, 0)
	defer structData.Release()
	structArr := array.NewStructData(structData)
	defer structArr.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "args", Type: structType}}, nil)
	batch := array.NewRecordBatch(schema, []arrow.Array{structArr}, 1)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err := w.Write(batch); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	args, err := ParseArguments(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return args
}

func makeChildData(arrs []arrow.Array) []arrow.ArrayData {
	out := make([]arrow.ArrayData, len(arrs))
	for i, a := range arrs {
		a.Data().Retain()
		out[i] = a.Data()
	}
	return out
}

func int64Arr(t *testing.T, v int64) arrow.Array {
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.Append(v)
	return b.NewArray()
}

func stringArr(t *testing.T, v string) arrow.Array {
	mem := memory.NewGoAllocator()
	b := array.NewStringBuilder(mem)
	defer b.Release()
	b.Append(v)
	return b.NewArray()
}

func boolArr(t *testing.T, v bool) arrow.Array {
	mem := memory.NewGoAllocator()
	b := array.NewBooleanBuilder(mem)
	defer b.Release()
	b.Append(v)
	return b.NewArray()
}

func floatArr(t *testing.T, v float64) arrow.Array {
	mem := memory.NewGoAllocator()
	b := array.NewFloat64Builder(mem)
	defer b.Release()
	b.Append(v)
	return b.NewArray()
}

func nullArr(t *testing.T, dt arrow.DataType) arrow.Array {
	mem := memory.NewGoAllocator()
	b := array.NewBuilder(mem, dt)
	defer b.Release()
	b.AppendNull()
	return b.NewArray()
}

func TestBindArgs_PositionalAndNamed(t *testing.T) {
	type myArgs struct {
		Count     int64   `vgi:"pos=0"`
		BatchSize int64   `vgi:"name=batch_size,default=2048"`
		Scale     float64 `vgi:"default=1.0"`
	}
	args := makeArgs(t, map[string]arrow.Array{
		"positional_0":     int64Arr(t, 100),
		"named_batch_size": int64Arr(t, 512),
		"named_scale":      nullArr(t, arrow.PrimitiveTypes.Float64), // null → default
	})
	defer args.Release()

	var a myArgs
	if err := BindArgs(args, &a); err != nil {
		t.Fatal(err)
	}
	if a.Count != 100 {
		t.Errorf("Count: got %d", a.Count)
	}
	if a.BatchSize != 512 {
		t.Errorf("BatchSize: got %d", a.BatchSize)
	}
	if a.Scale != 1.0 {
		t.Errorf("Scale (default): got %v", a.Scale)
	}
}

func TestBindArgs_AllPrimitives(t *testing.T) {
	type myArgs struct {
		S string  `vgi:"pos=0"`
		I int64   `vgi:"pos=1"`
		F float64 `vgi:"pos=2"`
		B bool    `vgi:"pos=3"`
		U uint32  `vgi:"pos=4"`
		N int32   `vgi:"pos=5"`
	}
	args := makeArgs(t, map[string]arrow.Array{
		"positional_0": stringArr(t, "hi"),
		"positional_1": int64Arr(t, 42),
		"positional_2": floatArr(t, 3.14),
		"positional_3": boolArr(t, true),
		"positional_4": int64Arr(t, 7),
		"positional_5": int64Arr(t, -9),
	})
	defer args.Release()

	var a myArgs
	if err := BindArgs(args, &a); err != nil {
		t.Fatal(err)
	}
	if a.S != "hi" || a.I != 42 || a.F != 3.14 || !a.B || a.U != 7 || a.N != -9 {
		t.Errorf("got %+v", a)
	}
}

func TestBindArgs_Varargs(t *testing.T) {
	type myArgs struct {
		Count  int64   `vgi:"pos=0"`
		Values []int64 `vgi:"pos=1,varargs"`
	}
	args := makeArgs(t, map[string]arrow.Array{
		"positional_0": int64Arr(t, 3),
		"positional_1": int64Arr(t, 10),
		"positional_2": int64Arr(t, 20),
		"positional_3": int64Arr(t, 30),
	})
	defer args.Release()

	var a myArgs
	if err := BindArgs(args, &a); err != nil {
		t.Fatal(err)
	}
	if a.Count != 3 {
		t.Errorf("Count: got %d", a.Count)
	}
	if !reflect.DeepEqual(a.Values, []int64{10, 20, 30}) {
		t.Errorf("Values: got %v", a.Values)
	}
}

func TestBindArgs_DefaultsWhenNullOrMissing(t *testing.T) {
	type myArgs struct {
		Greeting   string  `vgi:"name=greeting,default=hi"`
		Multiplier int64   `vgi:"name=multiplier,default=1"`
		Scale      float64 `vgi:"name=scale,default=1.0"`
		Enabled    bool    `vgi:"name=enabled,default=true"`
	}
	// Provide nulls; defaults should fill in.
	args := makeArgs(t, map[string]arrow.Array{
		"named_greeting":   nullArr(t, arrow.BinaryTypes.String),
		"named_multiplier": nullArr(t, arrow.PrimitiveTypes.Int64),
		"named_scale":      nullArr(t, arrow.PrimitiveTypes.Float64),
		"named_enabled":    nullArr(t, arrow.FixedWidthTypes.Boolean),
	})
	defer args.Release()

	var a myArgs
	if err := BindArgs(args, &a); err != nil {
		t.Fatal(err)
	}
	if a.Greeting != "hi" || a.Multiplier != 1 || a.Scale != 1.0 || !a.Enabled {
		t.Errorf("defaults not applied: %+v", a)
	}
}

func TestBindArgs_RejectsBadTarget(t *testing.T) {
	var a struct{}
	if err := BindArgs(nil, a); err == nil {
		t.Error("expected error for non-pointer target")
	}
	var p *struct{}
	if err := BindArgs(nil, p); err == nil {
		t.Error("expected error for nil pointer target")
	}
	var i int
	if err := BindArgs(nil, &i); err == nil {
		t.Error("expected error for non-struct target")
	}
}

func TestBindArgs_SkipsNonConstFields(t *testing.T) {
	// Non-const fields are column arguments; BindArgs must leave them at
	// zero rather than attempting scalar extraction (the value column might
	// be a placeholder).
	type myArgs struct {
		Path    string `vgi:"pos=0,const=false"` // column arg
		SleepMs int64  `vgi:"name=sleep_ms,default=50"`
	}
	args := makeArgs(t, map[string]arrow.Array{
		"positional_0":   nullArr(t, arrow.BinaryTypes.String),
		"named_sleep_ms": int64Arr(t, 100),
	})
	defer args.Release()

	var a myArgs
	if err := BindArgs(args, &a); err != nil {
		t.Fatal(err)
	}
	if a.Path != "" {
		t.Errorf("Path should remain zero for non-const field, got %q", a.Path)
	}
	if a.SleepMs != 100 {
		t.Errorf("SleepMs: got %d", a.SleepMs)
	}
}

func TestBindArgs_NilArgsAppliesDefaults(t *testing.T) {
	type myArgs struct {
		Greeting string `vgi:"name=greeting,default=hi"`
		Count    int64  `vgi:"pos=0"`
	}
	var a myArgs
	if err := BindArgs(nil, &a); err != nil {
		t.Fatal(err)
	}
	if a.Greeting != "hi" {
		t.Errorf("Greeting: got %q", a.Greeting)
	}
	if a.Count != 0 {
		t.Errorf("Count should remain zero, got %d", a.Count)
	}
}

func TestRegisterTypeBound_RoundTrip(t *testing.T) {
	RegisterTypeBound("custom_bound", IsNumericType)
	if LookupTypeBound("CUSTOM_BOUND") == nil {
		t.Error("case-insensitive lookup failed")
	}
}

func TestSnakeCase(t *testing.T) {
	cases := map[string]string{
		"":             "",
		"Count":        "count",
		"BatchSize":    "batch_size",
		"foo":          "foo",
		"HTMLPath":     "html_path",
		"MyURL":        "my_url",
		"URLPath":      "url_path",
		"MyHTMLParser": "my_html_parser",
		"HTTPSPort":    "https_port",
		"User2FA":      "user2_fa", // digit-uppercase transition still splits
		"ID":           "id",
		"A":            "a",
	}
	for in, want := range cases {
		if got := snakeCase(in); got != want {
			t.Errorf("snakeCase(%q): got %q want %q", in, got, want)
		}
	}
}
