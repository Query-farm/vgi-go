// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

type exampleArgs struct {
	Value int64 `vgi:"pos=0,doc=Numeric value to double"`
}

type exampleImpl struct{}

func (exampleImpl) Name() string { return "double_example" }

func (exampleImpl) Metadata() FunctionMetadata {
	expected := "4"
	return FunctionMetadata{
		Description: "Doubles numeric values",
		Examples: []CatalogExample{
			{
				SQL:            "SELECT double_example(2)",
				Description:    "Doubles an integer literal",
				ExpectedOutput: &expected,
			},
			{
				SQL:         "SELECT double_example(value) FROM numbers",
				Description: "Doubles each value in a column",
			},
		},
	}
}

func (exampleImpl) OnBindTyped(*exampleArgs, *BindParams) (*BindResponse, error) {
	return BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

func (exampleImpl) ProcessTyped(context.Context, *exampleArgs, *ProcessParams, arrow.RecordBatch) (arrow.RecordBatch, error) {
	return nil, nil
}

// TestFunctionExamplesSurfacedInCatalog verifies that Examples declared in a
// scalar's FunctionMetadata are threaded through buildFunctionInfo into the
// catalog's FunctionInfo (and therefore onto the wire).
func TestFunctionExamplesSurfacedInCatalog(t *testing.T) {
	w := NewWorker()
	w.RegisterScalar(AsScalarFunction[exampleArgs](exampleImpl{}))

	cat := NewDefaultReadOnlyCatalog("example", w)

	si, ok := cat.schemas["main"]
	if !ok {
		t.Fatalf("main schema not found in catalog")
	}

	var fi *FunctionInfo
	for i := range si.functions {
		if si.functions[i].Name == "double_example" {
			fi = &si.functions[i]
			break
		}
	}
	if fi == nil {
		t.Fatalf("double_example function not found in catalog")
	}

	if len(fi.Examples) != 2 {
		t.Fatalf("expected 2 examples, got %d", len(fi.Examples))
	}

	ex0 := fi.Examples[0]
	if ex0.SQL != "SELECT double_example(2)" {
		t.Errorf("example[0].SQL = %q", ex0.SQL)
	}
	if ex0.Description != "Doubles an integer literal" {
		t.Errorf("example[0].Description = %q", ex0.Description)
	}
	if ex0.ExpectedOutput == nil || *ex0.ExpectedOutput != "4" {
		t.Errorf("example[0].ExpectedOutput = %v", ex0.ExpectedOutput)
	}

	ex1 := fi.Examples[1]
	if ex1.SQL != "SELECT double_example(value) FROM numbers" {
		t.Errorf("example[1].SQL = %q", ex1.SQL)
	}
	if ex1.Description != "Doubles each value in a column" {
		t.Errorf("example[1].Description = %q", ex1.Description)
	}
	if ex1.ExpectedOutput != nil {
		t.Errorf("example[1].ExpectedOutput = %v, want nil", ex1.ExpectedOutput)
	}

	// Ensure the FunctionInfo serializes cleanly with examples populated.
	if _, err := SerializeFunctionInfo(fi); err != nil {
		t.Fatalf("SerializeFunctionInfo: %v", err)
	}
}

// TestSchemaTagsSurfacedInCatalog verifies that tags configured via
// WithSchemaTags are threaded onto SchemaInfo.Tags (duckdb_schemas().tags) and
// serialize cleanly.
func TestSchemaTagsSurfacedInCatalog(t *testing.T) {
	w := NewWorker(WithSchemaTags(map[string]map[string]string{
		"main": {
			"vgi.description_llm": "LLM description",
			"vgi.description_md":  "MD description",
		},
	}))
	w.RegisterScalar(AsScalarFunction[exampleArgs](exampleImpl{}))

	cat := NewDefaultReadOnlyCatalog("example", w)
	si, ok := cat.schemas["main"]
	if !ok {
		t.Fatalf("main schema not found in catalog")
	}
	if got := si.info.Tags["vgi.description_llm"]; got != "LLM description" {
		t.Errorf("tags[vgi.description_llm] = %q", got)
	}
	if got := si.info.Tags["vgi.description_md"]; got != "MD description" {
		t.Errorf("tags[vgi.description_md] = %q", got)
	}
	if _, err := SerializeSchemaInfo(si.info); err != nil {
		t.Fatalf("SerializeSchemaInfo: %v", err)
	}
}

// TestCatalogSourceURLSerialized verifies that a worker-set CatalogInfo.SourceURL
// serializes into the catalog_catalogs discovery record (source_url column).
func TestCatalogSourceURLSerialized(t *testing.T) {
	url := "https://github.com/Query-farm/vgi-go"
	if _, err := SerializeCatalogInfo(&CatalogInfo{Name: "example", SourceURL: &url}); err != nil {
		t.Fatalf("SerializeCatalogInfo: %v", err)
	}
}
