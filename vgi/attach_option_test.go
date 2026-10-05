// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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

func TestAttachOptionsForCatalog(t *testing.T) {
	gated := AttachOptionSpec{Name: "api_key", Type: arrow.BinaryTypes.String, Required: true}
	shared := AttachOptionSpec{Name: "opt_string", Type: arrow.BinaryTypes.String}

	w := NewWorker(
		WithCatalogName("primary"),
		WithAttachOptions(shared),
		WithCatalogAliasInfo("gated", CatalogInfo{Name: "gated"}),
		WithAttachOptionsForCatalog("gated", gated),
	)

	// The gated catalog is governed by its own specs...
	got := w.attachOptionsFor("gated")
	if len(got) != 1 || got[0].Name != "api_key" || !got[0].Required {
		t.Fatalf("gated catalog got the wrong specs: %v", got)
	}

	// ...and the required option there must not gate the primary catalog too.
	got = w.attachOptionsFor("primary")
	if len(got) != 1 || got[0].Name != "opt_string" {
		t.Fatalf("primary catalog got the wrong specs: %v", got)
	}
	if err := validateRequiredAttachOptions("primary", got, nil); err != nil {
		t.Fatalf("primary attach should not require anything: %v", err)
	}
}

func TestAttachOptionSpecSecretRoundTrips(t *testing.T) {
	for _, tc := range []struct {
		name             string
		required, secret bool
	}{
		{"plain", false, false},
		{"secret", false, true},
		{"required", true, false},
		{"required secret", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := serializeAttachOptionSpec(AttachOptionSpec{
				Name: "api_key", Description: "API key", Type: arrow.BinaryTypes.String,
				Required: tc.required, Secret: tc.secret,
			})
			if err != nil {
				t.Fatalf("serialize: %v", err)
			}
			names, err := suppliedAttachOptionNames(data)
			if err != nil {
				t.Fatalf("read schema: %v", err)
			}
			if _, ok := names["secret"]; !ok {
				t.Fatalf("serialized spec has no `secret` column: %v", names)
			}
			got, err := DeserializeAttachOptionSpec(data)
			if err != nil {
				t.Fatalf("deserialize: %v", err)
			}
			if got.Name != "api_key" || got.Description != "API key" || !arrow.TypeEqual(got.Type, arrow.BinaryTypes.String) {
				t.Fatalf("unexpected spec: %+v", got)
			}
			if got.Required != tc.required || got.Secret != tc.secret {
				t.Fatalf("required/secret = %v/%v, want %v/%v", got.Required, got.Secret, tc.required, tc.secret)
			}
			if got.DefaultBatch != nil {
				t.Fatal("unexpected default")
			}
		})
	}
}

// The secret column follows required, so a peer reading by position up to
// required still sees the columns it knows where it expects them.
func TestAttachOptionSpecSecretColumnOrder(t *testing.T) {
	data, err := serializeAttachOptionSpec(AttachOptionSpec{Name: "k", Type: arrow.BinaryTypes.String, Secret: true})
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	sc, err := DeserializeSchema(data)
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	want := []string{"name", "description", "type", "default_value", "required", "secret"}
	if sc.NumFields() != len(want) {
		t.Fatalf("got %d fields, want %d", sc.NumFields(), len(want))
	}
	for i, n := range want {
		if sc.Field(i).Name != n {
			t.Fatalf("field %d = %q, want %q", i, sc.Field(i).Name, n)
		}
		if (n == "required" || n == "secret") && !sc.Field(i).Nullable {
			t.Fatalf("field %q must be nullable", n)
		}
	}
}

// A secret option may carry a default (unlike a required one), even though it
// normally has none.
func TestAttachOptionSpecSecretWithDefault(t *testing.T) {
	batch := stringDefaultBatch(t, "dev-token")
	defer batch.Release()
	data, err := serializeAttachOptionSpec(AttachOptionSpec{
		Name: "token", Type: arrow.BinaryTypes.String, Secret: true, DefaultBatch: batch,
	})
	if err != nil {
		t.Fatalf("secret + default should be allowed: %v", err)
	}
	got, err := DeserializeAttachOptionSpec(data)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	if !got.Secret || got.Required || got.DefaultBatch == nil {
		t.Fatalf("unexpected spec: %+v", got)
	}
	defer got.DefaultBatch.Release()
	if v := got.DefaultBatch.Column(0).(*array.String).Value(0); v != "dev-token" {
		t.Fatalf("default = %q", v)
	}

	// Required + secret + default is still rejected, by the required rule.
	if _, err := serializeAttachOptionSpec(AttachOptionSpec{
		Name: "token", Type: arrow.BinaryTypes.String, Secret: true, Required: true, DefaultBatch: batch,
	}); err == nil {
		t.Fatal("expected required + default to be rejected even when secret")
	}
}

// legacySpecBatch builds a spec batch as an older peer would: no `secret`
// column, and optionally no `required` column, or a null in either.
func legacySpecBatch(t *testing.T, withRequired bool, nullFlags bool) []byte {
	t.Helper()
	mem := memory.NewGoAllocator()
	typeBytes, err := SerializeSchema(arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil))
	if err != nil {
		t.Fatal(err)
	}
	fields := []arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "description", Type: arrow.BinaryTypes.String},
		{Name: "type", Type: arrow.BinaryTypes.Binary},
		{Name: "default_value", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}
	nameB := array.NewStringBuilder(mem)
	nameB.Append("api_key")
	descB := array.NewStringBuilder(mem)
	descB.Append("API key")
	typeB := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	typeB.Append(typeBytes)
	defB := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defB.AppendNull()
	cols := []arrow.Array{nameB.NewArray(), descB.NewArray(), typeB.NewArray(), defB.NewArray()}
	if withRequired {
		fields = append(fields, arrow.Field{Name: "required", Type: arrow.FixedWidthTypes.Boolean, Nullable: true})
		reqB := array.NewBooleanBuilder(mem)
		if nullFlags {
			reqB.AppendNull()
		} else {
			reqB.Append(true)
		}
		cols = append(cols, reqB.NewArray())
	}
	if nullFlags {
		fields = append(fields, arrow.Field{Name: "secret", Type: arrow.FixedWidthTypes.Boolean, Nullable: true})
		secB := array.NewBooleanBuilder(mem)
		secB.AppendNull()
		cols = append(cols, secB.NewArray())
	}
	sc := arrow.NewSchema(fields, nil)
	batch := array.NewRecordBatch(sc, cols, 1)
	defer batch.Release()
	for _, c := range cols {
		c.Release()
	}
	data, err := SerializeRecordBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestDeserializeAttachOptionSpecWithoutSecretColumn(t *testing.T) {
	// A peer that knows `required` but predates `secret`.
	got, err := DeserializeAttachOptionSpec(legacySpecBatch(t, true, false))
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	if !got.Required || got.Secret {
		t.Fatalf("required/secret = %v/%v, want true/false", got.Required, got.Secret)
	}

	// A peer that predates both columns.
	got, err = DeserializeAttachOptionSpec(legacySpecBatch(t, false, false))
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	if got.Required || got.Secret {
		t.Fatalf("required/secret = %v/%v, want false/false", got.Required, got.Secret)
	}

	// Explicit nulls read the same as absent columns.
	got, err = DeserializeAttachOptionSpec(legacySpecBatch(t, true, true))
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	if got.Required || got.Secret {
		t.Fatalf("required/secret = %v/%v, want false/false", got.Required, got.Secret)
	}
}

// Secret does not change required-option validation: a missing required
// secret option is reported by name like any other.
func TestValidateRequiredSecretAttachOption(t *testing.T) {
	specs := []AttachOptionSpec{{Name: "api_key", Type: arrow.BinaryTypes.String, Required: true, Secret: true}}
	err := validateRequiredAttachOptions("gated", specs, nil)
	var missing *MissingAttachOptionsError
	if !errors.As(err, &missing) || len(missing.Missing) != 1 || missing.Missing[0] != "api_key" {
		t.Fatalf("expected api_key missing, got %v", err)
	}
	// A secret, non-required option is never demanded.
	specs[0].Required = false
	if err := validateRequiredAttachOptions("gated", specs, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
