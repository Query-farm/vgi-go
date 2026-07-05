// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// ptr is a tiny helper for building *float64 bound literals in table tests.
func ptr(f float64) *float64 { return &f }

// ---------------------------------------------------------------------------
// formatRange — interval notation for every bound combination
// ---------------------------------------------------------------------------

func TestFormatRange(t *testing.T) {
	cases := []struct {
		name           string
		ge, le, gt, lt *float64
		want           string
	}{
		{"all-absent", nil, nil, nil, nil, ""},
		{"ge-le-inclusive", ptr(0), ptr(10), nil, nil, "[0, 10]"},
		{"gt-open-upper", nil, nil, ptr(0), nil, "(0, +inf)"},
		{"ge-lt-mixed", ptr(1), nil, nil, ptr(10), "[1, 10)"},
		{"le-only-open-lower", nil, ptr(100), nil, nil, "(-inf, 100]"},
		{"lt-only-open-lower", nil, nil, nil, ptr(5), "(-inf, 5)"},
		{"ge-only-open-upper", ptr(-3), nil, nil, nil, "[-3, +inf)"},
		{"gt-lt-both-exclusive", nil, nil, ptr(0), ptr(1), "(0, 1)"},
		{"fractional-bounds", ptr(0.5), ptr(2.25), nil, nil, "[0.5, 2.25]"},
		{"whole-float-no-decimal", ptr(10.0), ptr(20.0), nil, nil, "[10, 20]"},
		{"negative-fractional", nil, nil, ptr(-1.5), ptr(1.5), "(-1.5, 1.5)"},
		// gt wins over ge on the low side; lt wins over le on the high side.
		{"exclusive-wins-both-sides", ptr(0), ptr(10), ptr(1), ptr(9), "(1, 9)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatRange(tc.ge, tc.le, tc.gt, tc.lt); got != tc.want {
				t.Errorf("formatRange = %q, want %q", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// formatBound — integer vs fractional rendering
// ---------------------------------------------------------------------------

func TestFormatBound(t *testing.T) {
	cases := map[float64]string{
		0:        "0",
		10:       "10",
		-3:       "-3",
		1.5:      "1.5",
		0.1:      "0.1",
		1000000:  "1000000", // must NOT become scientific notation
		-2.25:    "-2.25",
		100000.5: "100000.5",
	}
	for in, want := range cases {
		if got := formatBound(in); got != want {
			t.Errorf("formatBound(%v) = %q, want %q", in, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// encodeDefaultJSON — typed JSON per Arrow type
// ---------------------------------------------------------------------------

func TestEncodeDefaultJSON(t *testing.T) {
	cases := []struct {
		name      string
		arrowType string
		def       string
		want      string
	}{
		{"int", "int64", "2048", "2048"},
		{"uint", "uint32", "7", "7"},
		{"float", "double", "1.5", "1.5"},
		{"bool-true", "bool", "true", "true"},
		{"bool-false", "boolean", "false", "false"},
		{"string", "varchar", "x", `"x"`},
		// Unparseable-as-int falls back to the JSON string form.
		{"int-parse-failure", "int64", "not-a-number", `"not-a-number"`},
		// Unknown type falls back to the JSON string form.
		{"unknown-type", "struct", "whatever", `"whatever"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := ArgSpec{ArrowType: tc.arrowType, HasDefault: true, DefaultValue: tc.def}
			if got := encodeDefaultJSON(spec); got != tc.want {
				t.Errorf("encodeDefaultJSON(%s=%q) = %q, want %q", tc.arrowType, tc.def, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// encodeChoicesJSON — typed JSON arrays
// ---------------------------------------------------------------------------

func TestEncodeChoicesJSON(t *testing.T) {
	cases := []struct {
		name    string
		choices []any
		want    string
	}{
		{"ints", []any{int64(1), int64(2), int64(3)}, "[1,2,3]"},
		{"strings", []any{"a", "b"}, `["a","b"]`},
		{"floats", []any{1.5, 2.5}, "[1.5,2.5]"},
		{"bools", []any{true, false}, "[true,false]"},
		{"mixed", []any{int64(1), "two"}, `[1,"two"]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := encodeChoicesJSON(tc.choices); got != tc.want {
				t.Errorf("encodeChoicesJSON(%v) = %q, want %q", tc.choices, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// parseChoiceValue — element typing by Arrow type
// ---------------------------------------------------------------------------

func TestParseChoiceValue(t *testing.T) {
	if v := parseChoiceValue("int64", "3"); v != int64(3) {
		t.Errorf("int64 choice: got %#v, want int64(3)", v)
	}
	if v := parseChoiceValue("double", "1.5"); v != 1.5 {
		t.Errorf("double choice: got %#v, want 1.5", v)
	}
	if v := parseChoiceValue("bool", "true"); v != true {
		t.Errorf("bool choice: got %#v, want true", v)
	}
	if v := parseChoiceValue("varchar", "hello"); v != "hello" {
		t.Errorf("varchar choice: got %#v, want \"hello\"", v)
	}
	// Non-scalar / unknown type falls back to the raw string.
	if v := parseChoiceValue("struct", "raw"); v != "raw" {
		t.Errorf("fallback choice: got %#v, want \"raw\"", v)
	}
}

// ---------------------------------------------------------------------------
// BuildArgSchema — constraint metadata emission (presence-only)
// ---------------------------------------------------------------------------

func metaValue(f arrow.Field, key string) (string, bool) {
	idx := f.Metadata.FindKey(key)
	if idx < 0 {
		return "", false
	}
	return f.Metadata.Values()[idx], true
}

func TestBuildArgSchema_EmitsConstraintMetadata(t *testing.T) {
	le := 10.0
	ge := 0.0
	specs := []ArgSpec{
		{
			Name:          "precision",
			Position:      0,
			ArrowType:     "int64",
			IsConst:       true,
			ArrowDataType: arrow.PrimitiveTypes.Int64,
			HasDefault:    true,
			DefaultValue:  "2",
			Choices:       []any{int64(1), int64(2), int64(3)},
			Ge:            &ge,
			Le:            &le,
			Pattern:       "^[0-9]+$",
		},
	}

	schema := BuildArgSchema(specs)
	f := schema.Field(0)

	if v, ok := metaValue(f, "vgi_default"); !ok || v != "2" {
		t.Errorf("vgi_default: got %q ok=%v, want \"2\"", v, ok)
	}
	if v, ok := metaValue(f, "vgi_choices"); !ok || v != "[1,2,3]" {
		t.Errorf("vgi_choices: got %q ok=%v, want \"[1,2,3]\"", v, ok)
	}
	if v, ok := metaValue(f, "vgi_range"); !ok || v != "[0, 10]" {
		t.Errorf("vgi_range: got %q ok=%v, want \"[0, 10]\"", v, ok)
	}
	if v, ok := metaValue(f, "vgi_pattern"); !ok || v != "^[0-9]+$" {
		t.Errorf("vgi_pattern: got %q ok=%v, want \"^[0-9]+$\"", v, ok)
	}
}

func TestBuildArgSchema_ConstraintsPresenceOnly(t *testing.T) {
	// An argument with no constraints must carry none of the four keys.
	specs := []ArgSpec{
		{Name: "plain", Position: 0, ArrowType: "varchar", IsConst: true, ArrowDataType: arrow.BinaryTypes.String},
	}
	f := BuildArgSchema(specs).Field(0)
	for _, key := range []string{"vgi_default", "vgi_choices", "vgi_range", "vgi_pattern"} {
		if _, ok := metaValue(f, key); ok {
			t.Errorf("unconstrained arg should not carry %s; keys=%v", key, f.Metadata.Keys())
		}
	}
}

// ---------------------------------------------------------------------------
// parseArgTag — tag-declared constraints flow into ArgSpec + schema
// ---------------------------------------------------------------------------

func TestParseArgTag_Constraints(t *testing.T) {
	type argsStruct struct {
		Precision int    `vgi:"const,ge=0,le=10,choices='1,2,3',default=2"`
		Name      string `vgi:"const,pattern='^[a-z]+$'"`
	}
	specs := DeriveArgSpecs(&argsStruct{})
	if len(specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(specs))
	}

	p := specs[0]
	if p.Ge == nil || *p.Ge != 0 {
		t.Errorf("Ge: got %v, want 0", p.Ge)
	}
	if p.Le == nil || *p.Le != 10 {
		t.Errorf("Le: got %v, want 10", p.Le)
	}
	if len(p.Choices) != 3 || p.Choices[0] != int64(1) || p.Choices[2] != int64(3) {
		t.Errorf("Choices: got %#v, want [1 2 3] as int64", p.Choices)
	}
	if !p.HasDefault || p.DefaultValue != "2" {
		t.Errorf("default: got has=%v val=%q, want has=true val=\"2\"", p.HasDefault, p.DefaultValue)
	}

	n := specs[1]
	if n.Pattern != "^[a-z]+$" {
		t.Errorf("Pattern: got %q, want \"^[a-z]+$\"", n.Pattern)
	}

	// End-to-end: the derived specs must serialize the expected metadata.
	schema := BuildArgSchema(specs)
	if v, ok := metaValue(schema.Field(0), "vgi_range"); !ok || v != "[0, 10]" {
		t.Errorf("derived vgi_range: got %q ok=%v", v, ok)
	}
	if v, ok := metaValue(schema.Field(0), "vgi_choices"); !ok || v != "[1,2,3]" {
		t.Errorf("derived vgi_choices: got %q ok=%v", v, ok)
	}
	if v, ok := metaValue(schema.Field(0), "vgi_default"); !ok || v != "2" {
		t.Errorf("derived vgi_default: got %q ok=%v", v, ok)
	}
	if v, ok := metaValue(schema.Field(1), "vgi_pattern"); !ok || v != "^[a-z]+$" {
		t.Errorf("derived vgi_pattern: got %q ok=%v", v, ok)
	}
}
