// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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

// ---------------------------------------------------------------------------
// Bind-time enforcement of const-argument constraints
// ---------------------------------------------------------------------------

// ValidateArgConstraints enforces the discovery constraints declared on a
// function's const arguments against the actual bound values. Const arguments
// are bind-time scalars, so this runs once at bind (mirroring the Python SDK's
// Arg._validate): a value that violates a declared choices / numeric-range /
// pattern constraint yields an *ArgumentError. Column (non-const) arguments and
// type bounds are not handled here (type bounds are ValidateTypeBounds' job).
// A null const value skips its value constraints, matching the Python SDK.
func ValidateArgConstraints(specs []ArgSpec, args *Arguments) error {
	if args == nil {
		return nil
	}
	for i := range specs {
		spec := specs[i]
		if !spec.IsConst || !specHasValueConstraints(spec) {
			continue
		}
		if spec.Position < 0 || spec.Position >= len(args.Positional) {
			continue
		}
		col := args.Positional[spec.Position]
		if col == nil || col.Len() == 0 || col.IsNull(0) {
			continue
		}
		value, ok := constScalarValue(col)
		if !ok {
			continue // unsupported scalar kind; nothing to compare against
		}
		if err := validateConstValue(spec, value); err != nil {
			return err
		}
	}
	return nil
}

// specHasValueConstraints reports whether a spec declares any value constraint.
func specHasValueConstraints(spec ArgSpec) bool {
	return len(spec.Choices) > 0 || spec.Ge != nil || spec.Le != nil ||
		spec.Gt != nil || spec.Lt != nil || spec.Pattern != ""
}

// constScalarValue extracts a const argument's scalar value as a Go native
// (int64 / float64 / string / bool). The bool result is false for kinds we
// don't compare against constraints.
func constScalarValue(col arrow.Array) (any, bool) {
	switch c := col.(type) {
	case *array.Int64:
		return c.Value(0), true
	case *array.Int32:
		return int64(c.Value(0)), true
	case *array.Int16:
		return int64(c.Value(0)), true
	case *array.Int8:
		return int64(c.Value(0)), true
	case *array.Uint64:
		return int64(c.Value(0)), true
	case *array.Uint32:
		return int64(c.Value(0)), true
	case *array.Uint16:
		return int64(c.Value(0)), true
	case *array.Uint8:
		return int64(c.Value(0)), true
	case *array.Float64:
		return c.Value(0), true
	case *array.Float32:
		return float64(c.Value(0)), true
	case *array.String:
		return c.Value(0), true
	case *array.Boolean:
		return c.Value(0), true
	case *array.Dictionary:
		if d, ok := c.Dictionary().(*array.String); ok {
			return d.Value(c.GetValueIndex(0)), true
		}
	}
	return nil, false
}

// validateConstValue checks one const value against its spec's constraints.
func validateConstValue(spec ArgSpec, value any) error {
	argErr := func(detail string) error {
		return &ArgumentError{ArgName: spec.Name, Position: spec.Position, Detail: detail}
	}
	if spec.Ge != nil || spec.Le != nil || spec.Gt != nil || spec.Lt != nil {
		if f, ok := toFloat(value); ok {
			switch {
			case spec.Ge != nil && f < *spec.Ge:
				return argErr(fmt.Sprintf("must be >= %s", formatBound(*spec.Ge)))
			case spec.Le != nil && f > *spec.Le:
				return argErr(fmt.Sprintf("must be <= %s", formatBound(*spec.Le)))
			case spec.Gt != nil && f <= *spec.Gt:
				return argErr(fmt.Sprintf("must be > %s", formatBound(*spec.Gt)))
			case spec.Lt != nil && f >= *spec.Lt:
				return argErr(fmt.Sprintf("must be < %s", formatBound(*spec.Lt)))
			}
		}
	}
	if len(spec.Choices) > 0 && !choicesContain(spec.Choices, value) {
		return argErr(fmt.Sprintf("must be one of %s", encodeChoicesJSON(spec.Choices)))
	}
	if spec.Pattern != "" {
		if s, ok := value.(string); ok {
			if re, err := regexp.Compile(spec.Pattern); err == nil && !re.MatchString(s) {
				return argErr(fmt.Sprintf("must match pattern %q", spec.Pattern))
			}
		}
	}
	return nil
}

// toFloat coerces a numeric Go native to float64 for range/choice comparison.
func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case int:
		return float64(n), true
	case float32:
		return float64(n), true
	}
	return 0, false
}

// choicesContain reports whether value equals one of the declared choices,
// comparing numerically when both are numbers and by identity otherwise.
func choicesContain(choices []any, value any) bool {
	for _, c := range choices {
		if af, aok := toFloat(c); aok {
			if bf, bok := toFloat(value); bok && af == bf {
				return true
			}
			continue
		}
		if c == value {
			return true
		}
	}
	return false
}
