// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// TestMacroArgumentsSchema_VgiDocRoundTrip verifies that documented macro
// parameters carry a vgi_doc field-metadata entry (presence-only), undocumented
// parameters carry none, parameter order is preserved, and the descriptions
// round-trip back out via MacroParameterDocsFromSchema. Mirrors how functions
// carry per-argument docs.
func TestMacroArgumentsSchema_VgiDocRoundTrip(t *testing.T) {
	const docA = "lower bound — inclusive (µ ≥ value)"
	const docC = "the value to clamp"

	parameters := []string{"lo", "hi", "value"}
	docs := map[string]string{
		"lo":    docA,
		"value": docC,
		// "hi" is intentionally undocumented.
	}

	// Give "value" a typed default so its field type is non-null.
	defaults, err := BuildMacroDefaultValues([]MacroDefault{
		{Name: "value", Value: int64(5), Type: arrow.PrimitiveTypes.Int64},
	})
	if err != nil {
		t.Fatalf("BuildMacroDefaultValues: %v", err)
	}

	argsBytes, err := BuildMacroArgumentsSchema(parameters, defaults, docs)
	if err != nil {
		t.Fatalf("BuildMacroArgumentsSchema: %v", err)
	}
	if argsBytes == nil {
		t.Fatal("expected non-nil arguments schema for non-empty parameters")
	}

	schema, err := DeserializeSchema(argsBytes)
	if err != nil {
		t.Fatalf("DeserializeSchema: %v", err)
	}

	// Field order must match parameters order, all nullable.
	if schema.NumFields() != len(parameters) {
		t.Fatalf("field count: got %d want %d", schema.NumFields(), len(parameters))
	}
	for i, name := range parameters {
		f := schema.Field(i)
		if f.Name != name {
			t.Errorf("field %d name: got %q want %q", i, f.Name, name)
		}
		if !f.Nullable {
			t.Errorf("field %q should be nullable", f.Name)
		}
	}

	// "lo" (documented) carries vgi_doc; type is null (no default).
	lo := schema.Field(0)
	if idx := lo.Metadata.FindKey("vgi_doc"); idx < 0 {
		t.Fatalf("lo missing vgi_doc; keys=%v", lo.Metadata.Keys())
	} else if got := lo.Metadata.Values()[idx]; got != docA {
		t.Errorf("lo vgi_doc: got %q want %q", got, docA)
	}
	if lo.Type.ID() != arrow.NULL {
		t.Errorf("lo type: got %s want null", lo.Type)
	}

	// "hi" (undocumented) carries no vgi_doc (presence-only).
	hi := schema.Field(1)
	if hi.Metadata.FindKey("vgi_doc") >= 0 {
		t.Errorf("hi should not carry vgi_doc; keys=%v", hi.Metadata.Keys())
	}

	// "value" (documented, with default) carries vgi_doc and the default's type.
	value := schema.Field(2)
	if idx := value.Metadata.FindKey("vgi_doc"); idx < 0 {
		t.Fatalf("value missing vgi_doc; keys=%v", value.Metadata.Keys())
	} else if got := value.Metadata.Values()[idx]; got != docC {
		t.Errorf("value vgi_doc: got %q want %q", got, docC)
	}
	if value.Type.ID() != arrow.INT64 {
		t.Errorf("value type: got %s want int64", value.Type)
	}

	// Round-trip the docs back out: only documented params present.
	got, err := MacroParameterDocsFromSchema(argsBytes)
	if err != nil {
		t.Fatalf("MacroParameterDocsFromSchema: %v", err)
	}
	if len(got) != 2 || got["lo"] != docA || got["value"] != docC {
		t.Errorf("round-tripped docs: got %v want {lo:%q value:%q}", got, docA, docC)
	}
	if _, ok := got["hi"]; ok {
		t.Errorf("undocumented param hi should be absent from round-tripped docs")
	}
}

// TestMacroArgumentsSchema_NoParameters verifies that a parameterless macro
// yields no arguments schema (nil), and that decoding empty bytes is a no-op.
func TestMacroArgumentsSchema_NoParameters(t *testing.T) {
	argsBytes, err := BuildMacroArgumentsSchema(nil, nil, nil)
	if err != nil {
		t.Fatalf("BuildMacroArgumentsSchema: %v", err)
	}
	if argsBytes != nil {
		t.Errorf("expected nil arguments schema for no parameters, got %d bytes", len(argsBytes))
	}

	docs, err := MacroParameterDocsFromSchema(nil)
	if err != nil {
		t.Fatalf("MacroParameterDocsFromSchema(nil): %v", err)
	}
	if len(docs) != 0 {
		t.Errorf("expected empty docs, got %v", docs)
	}
}

// TestMacroInfoFromCatalogMacro_PopulatesCanonicalDocumentation verifies the listing/get
// path populates canonical parameters and serializes the required family.
func TestMacroInfoFromCatalogMacro_PopulatesCanonicalDocumentation(t *testing.T) {
	cm := CatalogMacro{
		Name:       "clamp",
		MacroType:  MacroTypeScalar,
		Parameters: []string{"x", "lo"},
		Definition: "least(greatest(x, lo), lo)",
		ParameterDocs: map[string]string{
			"x": "the input value",
		},
	}

	info, err := macroInfoFromCatalogMacro(cm, SchemaPath{"main"})
	if err != nil {
		t.Fatalf("macroInfoFromCatalogMacro: %v", err)
	}
	if len(info.Overloads) != 1 || info.Overloads[0].Parameters[0].Documentation == nil || *info.Overloads[0].Parameters[0].Documentation != "the input value" {
		t.Fatal("canonical family lost parameter documentation")
	}
	if info.Overloads[0].Parameters[0].DeclaredType != nil {
		t.Fatal("untyped parameter acquired a declaration")
	}

	// The full MacroInfo must serialize into the generated schema slot.
	if _, err := SerializeMacroInfo(info); err != nil {
		t.Fatalf("SerializeMacroInfo: %v", err)
	}
}

func TestMacroCanonicalDefaultPreservesNullMetadataAndEmptyDocumentation(t *testing.T) {
	fieldMetadata := arrow.MetadataFrom(map[string]string{"logical": "field annotation"})
	schemaMetadata := arrow.MetadataFrom(map[string]string{"logical": "schema annotation"})
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: fieldMetadata}}, &schemaMetadata)
	values := array.NewInt64Builder(memory.DefaultAllocator)
	values.AppendNull()
	column := values.NewArray()
	values.Release()
	batch := array.NewRecordBatch(schema, []arrow.Array{column}, 1)
	column.Release()
	defer batch.Release()
	defaults, err := SerializeRecordBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	info, err := macroInfoFromCatalogMacro(CatalogMacro{Name: "m", MacroType: MacroTypeScalar,
		Parameters: []string{"x"}, Definition: "x", ParameterDefaultValues: defaults,
		ParameterDocs: map[string]string{"x": ""}}, SchemaPath{"sales.archive"})
	if err != nil {
		t.Fatal(err)
	}
	p := info.Overloads[0].Parameters[0]
	if p.DeclaredType != nil || p.DefaultExpression != nil || p.DefaultValue == nil || p.Documentation == nil || *p.Documentation != "" {
		t.Fatal("default, type or documentation presence changed")
	}
	reader, err := ipc.NewReader(bytes.NewReader(*p.DefaultValue))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("missing default")
	}
	got := reader.RecordBatch()
	if !got.Schema().Equal(schema) || !got.Schema().Metadata().Equal(schemaMetadata) || !got.Column(0).IsNull(0) {
		t.Fatalf("typed NULL or metadata lost: %v", got)
	}
	// Direct families preserve all overloads and embedded payloads, never the first only.
	second := info.Overloads[0]
	second.Parameters = nil
	second.Definition = "1"
	family := append(info.Overloads, second)
	direct, err := macroInfoFromCatalogMacro(CatalogMacro{Name: "m", MacroType: MacroTypeScalar, Overloads: family}, SchemaPath{"sales.archive"})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := SerializeMacroInfo(direct)
	if err != nil {
		t.Fatal(err)
	}
	outer, err := ipc.NewReader(bytes.NewReader(wire))
	if err != nil {
		t.Fatal(err)
	}
	defer outer.Release()
	if !outer.Next() {
		t.Fatal("missing macro info")
	}
	record := outer.RecordBatch()
	start, end := record.Column(5).(*array.List).ValueOffsets(0)
	if record.NumCols() != 6 || end-start != 2 {
		t.Fatal("family projected or legacy fields emitted")
	}
}
