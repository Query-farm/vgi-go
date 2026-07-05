// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

// Per-argument constraint metadata emitted onto the argument-spec schema for
// agent discovery. These helpers value-encode an ArgSpec's optional
// constraints into the UTF-8 field-metadata values read back by the C++ vgi
// extension's vgi_function_arguments() diagnostic. All are presence-only: the
// caller omits the key entirely when the constraint is absent, so these
// helpers never need to emit an "empty" sentinel.

// formatRange builds interval notation from an argument's numeric bounds.
//
// Inclusive bounds (ge/le) render as square brackets, exclusive bounds (gt/lt)
// as parentheses, and an open side as -inf / +inf. When both an inclusive and
// exclusive bound are present on the same side the exclusive one wins (matching
// the Python reference). Returns "" when the argument has no numeric bound at
// all (the caller treats "" as "omit the vgi_range key").
//
// Examples: ge=0,le=10 -> "[0, 10]"; gt=0 (no upper) -> "(0, +inf)";
// ge=1,lt=10 -> "[1, 10)".
func formatRange(ge, le, gt, lt *float64) string {
	if ge == nil && le == nil && gt == nil && lt == nil {
		return ""
	}

	var low string
	switch {
	case gt != nil:
		low = "(" + formatBound(*gt)
	case ge != nil:
		low = "[" + formatBound(*ge)
	default:
		low = "(-inf"
	}

	var high string
	switch {
	case lt != nil:
		high = formatBound(*lt) + ")"
	case le != nil:
		high = formatBound(*le) + "]"
	default:
		high = "+inf)"
	}

	return low + ", " + high
}

// formatBound renders a numeric bound for interval notation. Whole numbers
// print without a trailing ".0" (0, not 0.0); genuinely fractional bounds keep
// their decimal. Uses the 'f' form so bounds never fall into scientific
// notation.
func formatBound(f float64) string {
	if !math.IsInf(f, 0) && !math.IsNaN(f) && f == math.Trunc(f) && math.Abs(f) < 1e18 {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// encodeDefaultJSON JSON-encodes an ArgSpec's default value for the vgi_default
// field-metadata key. Go stores the default as the raw textual DefaultValue, so
// this parses it against the declared arg type to produce a typed JSON scalar
// (e.g. int64 default "5" -> `5`, string default "x" -> `"x"`, bool "true" ->
// `true`). On any parse/marshal failure it falls back to the JSON string form
// of the raw value rather than dropping the whole registration.
func encodeDefaultJSON(spec ArgSpec) string {
	typed, ok := parseDefaultTyped(spec.ArrowType, spec.DefaultValue)
	if ok {
		if b, err := json.Marshal(typed); err == nil {
			return string(b)
		}
	}
	if b, err := json.Marshal(spec.DefaultValue); err == nil {
		return string(b)
	}
	// Last-resort fallback: JSON of the fmt string form.
	b, _ := json.Marshal(fmt.Sprintf("%v", spec.DefaultValue))
	return string(b)
}

// encodeChoicesJSON JSON-encodes an argument's closed set of allowed values for
// the vgi_choices field-metadata key (a JSON array). On marshal failure it
// falls back to a JSON array of each element's fmt string form rather than
// dropping the whole registration.
func encodeChoicesJSON(choices []any) string {
	if b, err := json.Marshal(choices); err == nil {
		return string(b)
	}
	strs := make([]string, len(choices))
	for i, c := range choices {
		strs[i] = fmt.Sprintf("%v", c)
	}
	if b, err := json.Marshal(strs); err == nil {
		return string(b)
	}
	return "[]"
}

// parseDefaultTyped parses the raw textual default against the argument's
// declared Arrow type name, returning a Go value whose json.Marshal form
// matches the value's natural type. The bool reports whether a typed parse
// succeeded; false means the caller should fall back to the string form.
func parseDefaultTyped(arrowType, raw string) (any, bool) {
	switch arrowType {
	case "int8", "int16", "int32", "int64":
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return n, true
		}
	case "uint8", "uint16", "uint32", "uint64":
		if n, err := strconv.ParseUint(raw, 10, 64); err == nil {
			return n, true
		}
	case "float", "float32", "double", "float64":
		if f, err := strconv.ParseFloat(raw, 64); err == nil {
			return f, true
		}
	case "bool", "boolean":
		if b, err := strconv.ParseBool(raw); err == nil {
			return b, true
		}
	case "varchar", "string":
		return raw, true
	}
	return nil, false
}

// parseChoiceValue parses a single choices= tag element against the argument's
// declared Arrow type name so the emitted vgi_choices JSON is typed (e.g. int
// args yield `[1,2,3]`, not `["1","2","3"]`). Unparseable / non-scalar types
// fall back to the raw string.
func parseChoiceValue(arrowType, raw string) any {
	if v, ok := parseDefaultTyped(arrowType, raw); ok {
		return v
	}
	return raw
}
