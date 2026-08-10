// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func stringDefaultBatch(t *testing.T, val string) arrow.RecordBatch {
	t.Helper()
	sc := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)
	b, err := buildDefaultValueBatch(memory.NewGoAllocator(), sc, arrow.BinaryTypes.String, val)
	if err != nil {
		t.Fatalf("buildDefaultValueBatch: %v", err)
	}
	return b
}

// An option that falls back to a value is always satisfiable without the
// caller, so declaring it required as well is a declaration bug.
func TestSerializeRejectsRequiredWithDefault(t *testing.T) {
	batch := stringDefaultBatch(t, "us-east-1")
	defer batch.Release()
	_, err := serializeAttachOptionSpec(AttachOptionSpec{
		Name: "region", Type: arrow.BinaryTypes.String, Required: true, DefaultBatch: batch,
	})
	if err == nil {
		t.Fatal("expected required + default to be rejected")
	}
}

func TestSerializeRequiredRoundTrips(t *testing.T) {
	data, err := serializeAttachOptionSpec(AttachOptionSpec{
		Name: "api_key", Description: "API key", Type: arrow.BinaryTypes.String, Required: true,
	})
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("empty spec bytes")
	}
	// The column is written explicitly so a reader sees false, not NULL, for an
	// option that simply isn't required.
	names, err := suppliedAttachOptionNames(data)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	if _, ok := names["required"]; !ok {
		t.Fatalf("serialized spec has no `required` column: %v", names)
	}
}

func TestValidateRequiredAttachOptions(t *testing.T) {
	specs := []AttachOptionSpec{
		{Name: "api_key", Type: arrow.BinaryTypes.String, Required: true},
		{Name: "region", Type: arrow.BinaryTypes.String},
	}

	// Nothing supplied: the required one is reported, by name.
	err := validateRequiredAttachOptions("gated", specs, nil)
	var missing *MissingAttachOptionsError
	if !errors.As(err, &missing) {
		t.Fatalf("expected MissingAttachOptionsError, got %v", err)
	}
	if len(missing.Missing) != 1 || missing.Missing[0] != "api_key" {
		t.Fatalf("unexpected missing set: %v", missing.Missing)
	}

	// No required specs at all: an empty mapping is fine.
	if err := validateRequiredAttachOptions("gated", specs[1:], nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
