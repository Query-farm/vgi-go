// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ---------------------------------------------------------------------------
// Typed argument declarations
//
// Declare arguments once as a struct with `vgi:"..."` tags; the framework
// derives []ArgSpec at registration time and populates the struct at runtime
// from an *Arguments. Replaces the dual-declaration pattern of writing
// ArgumentSpecs() AND extracting values with GetScalar... in Process/NewState.
//
// Tag keys (case-insensitive, comma-separated):
//
//   pos=N           Position (default -1, i.e. named-only)
//   name=X          Argument name (default = snake_case of field name)
//   default=V       Has a default value; V is the string form, parsed at bind
//   doc=...         Doc string (single-quote when V contains commas)
//   type=X          Override the ArrowType string
//   const           IsConst (default true). const=false marks column args.
//   varargs         IsVarargs (slice field consumes Positional[pos:])
//   bound=a+b       TypeBound predicates joined with '+' (OR), looked up by name
//   -               Skip this field
//
// Go field type → Arrow type inference:
//
//   string                → varchar (BinaryTypes.String)
//   int, int64            → int64
//   int32, int16, int8    → matching ints
//   uint64, uint32, ...   → matching uints
//   float64               → double (PrimitiveTypes.Float64)
//   float32               → float
//   bool                  → bool
//   []byte                → blob
//   []T                   → list of inferred T
//   [N]T (N>0, T!=byte)   → fixed_list of N inferred T
//   struct{ ... }         → struct of inferred field types
//   time.Time             → timestamp[us, UTC]
//   interface{} / any     → "any" (ArrowDataType nil; pair with bound=)
//
// The tag may override inference via type= or by attaching ArrowDataType
// through field type alone (a struct field whose Go type is a Go struct will
// produce arrow.StructOf with field names matching the Go field names).
// ---------------------------------------------------------------------------

const argTagKey = "vgi"

// fieldBinding caches the parsed tag plus reflection info for one field.
type fieldBinding struct {
	FieldIndex int
	Field      reflect.StructField
	Spec       ArgSpec
}

// DeriveArgSpecs returns []ArgSpec derived from the struct's `vgi:"..."` tags.
// args may be a struct value or pointer to one. Panics on malformed tags;
// these are developer errors that should fail at startup.
func DeriveArgSpecs(args any) []ArgSpec {
	bindings, err := parseArgBindings(reflect.TypeOf(args))
	if err != nil {
		panic(fmt.Errorf("vgi.DeriveArgSpecs: %w", err))
	}
	out := make([]ArgSpec, len(bindings))
	for i, b := range bindings {
		out[i] = b.Spec
	}
	return out
}

// BindArgs populates target (a non-nil pointer to a struct) from args using
// the same `vgi:"..."` tag conventions as DeriveArgSpecs. Missing or null
// arguments fall back to the declared default; otherwise the field keeps its
// zero value.
func BindArgs(args *Arguments, target any) error {
	v := reflect.ValueOf(target)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("vgi.BindArgs: target must be a non-nil pointer to a struct")
	}
	if v.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("vgi.BindArgs: target must point to a struct, got %v", v.Elem().Kind())
	}
	bindings, err := parseArgBindings(reflect.TypeOf(target))
	if err != nil {
		return err
	}
	if args == nil {
		// Apply defaults only; everything else stays zero.
		s := v.Elem()
		for _, b := range bindings {
			if b.Spec.HasDefault {
				if err := assignDefault(s.Field(b.FieldIndex), b.Spec.DefaultValue); err != nil {
					return fmt.Errorf("vgi.BindArgs: field %q: %w", b.Field.Name, err)
				}
			}
		}
		return nil
	}
	s := v.Elem()
	for _, b := range bindings {
		// Non-const fields are column arguments (scalar functions, or column
		// inputs to table functions); they don't carry a per-call scalar
		// value, so BindArgs leaves them at zero. Callers access column args
		// through the input batch / args.GetColumn directly.
		if !b.Spec.IsConst {
			continue
		}
		if err := bindOneField(s.Field(b.FieldIndex), b, args); err != nil {
			return fmt.Errorf("vgi.BindArgs: field %q: %w", b.Field.Name, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

var bindingCache sync.Map // reflect.Type -> []fieldBinding

func parseArgBindings(t reflect.Type) ([]fieldBinding, error) {
	if t == nil {
		return nil, fmt.Errorf("nil type")
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("args type must be a struct, got %v", t.Kind())
	}
	if cached, ok := bindingCache.Load(t); ok {
		return cached.([]fieldBinding), nil
	}
	var out []fieldBinding
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		raw, ok := f.Tag.Lookup(argTagKey)
		if !ok {
			continue
		}
		if strings.TrimSpace(raw) == "-" {
			continue
		}
		spec, err := parseArgTag(f, raw)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", f.Name, err)
		}
		out = append(out, fieldBinding{FieldIndex: i, Field: f, Spec: spec})
	}
	bindingCache.Store(t, out)
	return out, nil
}

// parseArgTag parses one `vgi:"..."` tag, applying Go-type-based inference for
// unspecified fields (name, ArrowType, ArrowDataType).
func parseArgTag(f reflect.StructField, tag string) (ArgSpec, error) {
	spec := ArgSpec{
		Name:     snakeCase(f.Name),
		Position: -1,
		IsConst:  true,
	}

	inferred := inferArrowType(f.Type)
	spec.ArrowType = inferred.ArrowTypeName
	spec.ArrowDataType = inferred.DataType

	for _, raw := range splitTag(tag) {
		key, val, hasVal := strings.Cut(raw, "=")
		key = strings.ToLower(strings.TrimSpace(key))
		val = strings.TrimSpace(val)
		val = strings.Trim(val, "'") // permit doc='...' for commas
		switch key {
		case "":
			continue
		case "pos":
			n, err := strconv.Atoi(val)
			if err != nil {
				return spec, fmt.Errorf("invalid pos=%q: %w", val, err)
			}
			spec.Position = n
		case "name":
			if val == "" {
				return spec, fmt.Errorf("name= must not be empty")
			}
			spec.Name = val
		case "default":
			spec.HasDefault = true
			spec.DefaultValue = val
		case "doc":
			spec.Doc = val
		case "type":
			if val == "" {
				return spec, fmt.Errorf("type= must not be empty")
			}
			spec.ArrowType = val
			// If the override is one of the canonical names we recognise
			// (duration_ms, timestamp_ms_utc, ...) attach its concrete
			// Arrow type so the spec serializer doesn't need string→type
			// resolution. Otherwise leave ArrowDataType nil and let the
			// schema layer fall back to ArrowType name resolution.
			if dt, ok := typeOverrides[strings.ToLower(val)]; ok {
				spec.ArrowDataType = dt
			} else {
				spec.ArrowDataType = nil
			}
		case "const":
			if !hasVal || val == "" {
				spec.IsConst = true
			} else {
				b, err := strconv.ParseBool(val)
				if err != nil {
					return spec, fmt.Errorf("invalid const=%q: %w", val, err)
				}
				spec.IsConst = b
			}
		case "varargs":
			if !hasVal || val == "" {
				spec.IsVarargs = true
			} else {
				b, err := strconv.ParseBool(val)
				if err != nil {
					return spec, fmt.Errorf("invalid varargs=%q: %w", val, err)
				}
				spec.IsVarargs = b
			}
			// Varargs slice fields advertise the *element* type on the wire,
			// not the slice. Re-infer from the element kind so the spec
			// matches the existing `IsVarargs: true, ArrowType: "<elem>"`
			// convention used by repeat_value, sum_values, etc.
			if spec.IsVarargs && f.Type.Kind() == reflect.Slice && f.Type.Elem() != byteType {
				elemInfo := inferArrowType(f.Type.Elem())
				spec.ArrowType = elemInfo.ArrowTypeName
				spec.ArrowDataType = elemInfo.DataType
			}
		case "bound":
			preds, err := resolveBounds(val)
			if err != nil {
				return spec, err
			}
			spec.TypeBound = preds
		case "choices":
			// Comma-separated allowed values; quote to keep commas
			// (e.g. choices='a,b,c'). Elements are parsed against the
			// argument's declared type so the JSON metadata is typed.
			for _, raw := range strings.Split(val, ",") {
				raw = strings.TrimSpace(raw)
				if raw == "" {
					continue
				}
				spec.Choices = append(spec.Choices, parseChoiceValue(spec.ArrowType, raw))
			}
		case "ge", "le", "gt", "lt":
			f, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return spec, fmt.Errorf("invalid %s=%q: %w", key, val, err)
			}
			switch key {
			case "ge":
				spec.Ge = &f
			case "le":
				spec.Le = &f
			case "gt":
				spec.Gt = &f
			case "lt":
				spec.Lt = &f
			}
		case "pattern":
			spec.Pattern = val
		default:
			return spec, fmt.Errorf("unknown vgi tag key %q", key)
		}
	}
	return spec, nil
}

// splitTag splits a vgi tag into key=value parts. Two ergonomic exceptions:
//
//   - Single-quoted values keep their commas: `doc='hello, world'`.
//   - `doc=` (case-insensitive) consumes the rest of the tag if unquoted,
//     because doc strings naturally contain commas and quoting every one is
//     noisy. The trade-off: doc= must be the last entry in the tag, otherwise
//     keys that follow it become part of the doc string.
func splitTag(tag string) []string {
	var out []string
	var cur strings.Builder
	inQuote := false
	inDoc := false
	for i := 0; i < len(tag); i++ {
		c := tag[i]
		// Detect start of an unquoted `doc=` value once we've seen the '='.
		if !inQuote && !inDoc && c == '=' && cur.Len() >= 3 {
			head := strings.ToLower(strings.TrimSpace(cur.String()))
			if head == "doc" {
				// Consume the rest of the tag as the doc value (unless it
				// begins with a quote, handled below).
				cur.WriteByte(c)
				i++
				rest := strings.TrimLeft(tag[i:], " ")
				if strings.HasPrefix(rest, "'") {
					// Quoted form — fall back to normal scanning for this segment.
					inQuote = true
					continue
				}
				cur.WriteString(rest)
				i = len(tag)
				break
			}
		}
		switch {
		case c == '\'':
			inQuote = !inQuote
			cur.WriteByte(c)
		case c == ',' && !inQuote:
			out = append(out, cur.String())
			cur.Reset()
			inDoc = false
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

// snakeCase converts a Go identifier to snake_case. Handles acronyms cleanly:
//
//	Count        → count
//	BatchSize    → batch_size
//	HTMLPath     → html_path
//	MyURL        → my_url
//	URLPath      → url_path
//	MyHTMLParser → my_html_parser
//	HTTPSPort    → https_port
//
// Rule: insert "_" before an uppercase letter if (a) the previous rune is
// lowercase or a digit, or (b) the previous rune is uppercase AND the next
// rune is lowercase (end-of-acronym).
func snakeCase(s string) string {
	if s == "" {
		return s
	}
	runes := []rune(s)
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i, r := range runes {
		isUpper := r >= 'A' && r <= 'Z'
		if i > 0 && isUpper {
			prev := runes[i-1]
			prevLowerOrDigit := (prev >= 'a' && prev <= 'z') || (prev >= '0' && prev <= '9')
			next := rune(0)
			if i+1 < len(runes) {
				next = runes[i+1]
			}
			prevUpper := prev >= 'A' && prev <= 'Z'
			nextLower := next >= 'a' && next <= 'z'
			if prevLowerOrDigit || (prevUpper && nextLower) {
				b.WriteByte('_')
			}
		}
		if isUpper {
			b.WriteRune(r - 'A' + 'a')
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// Go type → Arrow type inference
// ---------------------------------------------------------------------------

// arrowTypeInfo carries both the canonical wire-protocol name (used by
// ArgumentSpecs for catalog serialization) and the concrete Arrow DataType
// (used when the spec is a struct/list/fixed_list and the name alone is
// insufficient).
type arrowTypeInfo struct {
	ArrowTypeName string
	DataType      arrow.DataType
}

var byteType = reflect.TypeOf(byte(0))
var timeType = reflect.TypeOf(time.Time{})
var durationType = reflect.TypeOf(time.Duration(0))
var anyType = reflect.TypeOf((*any)(nil)).Elem()

// arrowArrayConcreteTypes maps a concrete *array.X pointer type to the Arrow
// type it represents. Used by inferArrowType so a typed scalar can declare
// a column-arg field with the precise array type — e.g.
//
//	type multiplyArgs struct {
//	    Value  *array.Int64 `vgi:"pos=0,const=false"`
//	    Factor int64        `vgi:"pos=1"`
//	}
//
// will produce ArgumentSpecs with ArrowType="int64" + the matching
// ArrowDataType, and the scalar adapter will set the field to
// batch.Column(N).(*array.Int64) at Process time.
//
// Only fixed (non-parametric) Arrow types live here. Nested or parametric
// types — Struct, List, FixedSizeList, Timestamp, Duration, Decimal — are
// intentionally absent because the Go pointer type doesn't capture their
// inner shape (struct fields, list element type, decimal precision/scale).
// For those, declare the column-arg field as arrow.Array and validate the
// shape in OnBindTyped — same approach vgi-python uses.
var arrowArrayConcreteTypes = map[reflect.Type]arrowTypeInfo{
	reflect.TypeOf((*array.Int8)(nil)):    {ArrowTypeName: "int8", DataType: arrow.PrimitiveTypes.Int8},
	reflect.TypeOf((*array.Int16)(nil)):   {ArrowTypeName: "int16", DataType: arrow.PrimitiveTypes.Int16},
	reflect.TypeOf((*array.Int32)(nil)):   {ArrowTypeName: "int32", DataType: arrow.PrimitiveTypes.Int32},
	reflect.TypeOf((*array.Int64)(nil)):   {ArrowTypeName: "int64", DataType: arrow.PrimitiveTypes.Int64},
	reflect.TypeOf((*array.Uint8)(nil)):   {ArrowTypeName: "uint8", DataType: arrow.PrimitiveTypes.Uint8},
	reflect.TypeOf((*array.Uint16)(nil)):  {ArrowTypeName: "uint16", DataType: arrow.PrimitiveTypes.Uint16},
	reflect.TypeOf((*array.Uint32)(nil)):  {ArrowTypeName: "uint32", DataType: arrow.PrimitiveTypes.Uint32},
	reflect.TypeOf((*array.Uint64)(nil)):  {ArrowTypeName: "uint64", DataType: arrow.PrimitiveTypes.Uint64},
	reflect.TypeOf((*array.Float32)(nil)): {ArrowTypeName: "float", DataType: arrow.PrimitiveTypes.Float32},
	reflect.TypeOf((*array.Float64)(nil)): {ArrowTypeName: "double", DataType: arrow.PrimitiveTypes.Float64},
	reflect.TypeOf((*array.Boolean)(nil)): {ArrowTypeName: "bool", DataType: arrow.FixedWidthTypes.Boolean},
	reflect.TypeOf((*array.String)(nil)):  {ArrowTypeName: "varchar", DataType: arrow.BinaryTypes.String},
	reflect.TypeOf((*array.Binary)(nil)):  {ArrowTypeName: "blob", DataType: arrow.BinaryTypes.Binary},
	reflect.TypeOf((*array.Date32)(nil)):  {ArrowTypeName: "date32", DataType: arrow.FixedWidthTypes.Date32},
	reflect.TypeOf((*array.Date64)(nil)):  {ArrowTypeName: "date64", DataType: arrow.FixedWidthTypes.Date64},
}

// typeOverrides maps the string accepted in a `vgi:"type=..."` tag to a
// concrete Arrow type. Used when the Go field type would otherwise infer
// to a different (or no) Arrow type — e.g. a Go int64 field that the
// caller wants surfaced to DuckDB as INTERVAL ('duration_ms') or
// TIMESTAMPTZ ('timestamp_ms_utc').
//
// Keys are case-insensitive at lookup time; entries here are the canonical
// lowercase forms.
var typeOverrides = map[string]arrow.DataType{
	// Durations
	"duration_s":  arrow.FixedWidthTypes.Duration_s,
	"duration_ms": arrow.FixedWidthTypes.Duration_ms,
	"duration_us": arrow.FixedWidthTypes.Duration_us,
	"duration_ns": arrow.FixedWidthTypes.Duration_ns,
	"duration":    arrow.FixedWidthTypes.Duration_ns, // shorthand, native Go precision

	// Naive timestamps (no tz)
	"timestamp_s":  arrow.FixedWidthTypes.Timestamp_s,
	"timestamp_ms": arrow.FixedWidthTypes.Timestamp_ms,
	"timestamp_us": arrow.FixedWidthTypes.Timestamp_us,
	"timestamp_ns": arrow.FixedWidthTypes.Timestamp_ns,

	// UTC-tagged timestamps
	"timestamp_s_utc":  &arrow.TimestampType{Unit: arrow.Second, TimeZone: "UTC"},
	"timestamp_ms_utc": &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"},
	"timestamp_us_utc": &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
	"timestamp_ns_utc": &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"},

	// Date / time-of-day
	"date32":    arrow.FixedWidthTypes.Date32,
	"date64":    arrow.FixedWidthTypes.Date64,
	"time32_s":  arrow.FixedWidthTypes.Time32s,
	"time32_ms": arrow.FixedWidthTypes.Time32ms,
	"time64_us": arrow.FixedWidthTypes.Time64us,
	"time64_ns": arrow.FixedWidthTypes.Time64ns,
}

func inferArrowType(t reflect.Type) arrowTypeInfo {
	if t == timeType {
		return arrowTypeInfo{ArrowTypeName: "timestamp", DataType: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}}
	}
	if t == durationType {
		return arrowTypeInfo{ArrowTypeName: "duration", DataType: arrow.FixedWidthTypes.Duration_ns}
	}
	if t == anyType {
		return arrowTypeInfo{ArrowTypeName: "any"}
	}
	// Concrete *array.X column types — e.g. `*array.Int64`. Used by typed
	// scalar functions to declare column-arg fields with the precise array
	// type instead of the generic arrow.Array interface.
	if info, ok := arrowArrayConcreteTypes[t]; ok {
		return info
	}
	switch t.Kind() {
	case reflect.String:
		return arrowTypeInfo{ArrowTypeName: "varchar", DataType: arrow.BinaryTypes.String}
	case reflect.Bool:
		return arrowTypeInfo{ArrowTypeName: "bool", DataType: arrow.FixedWidthTypes.Boolean}
	case reflect.Int, reflect.Int64:
		return arrowTypeInfo{ArrowTypeName: "int64", DataType: arrow.PrimitiveTypes.Int64}
	case reflect.Int32:
		return arrowTypeInfo{ArrowTypeName: "int32", DataType: arrow.PrimitiveTypes.Int32}
	case reflect.Int16:
		return arrowTypeInfo{ArrowTypeName: "int16", DataType: arrow.PrimitiveTypes.Int16}
	case reflect.Int8:
		return arrowTypeInfo{ArrowTypeName: "int8", DataType: arrow.PrimitiveTypes.Int8}
	case reflect.Uint, reflect.Uint64:
		return arrowTypeInfo{ArrowTypeName: "uint64", DataType: arrow.PrimitiveTypes.Uint64}
	case reflect.Uint32:
		return arrowTypeInfo{ArrowTypeName: "uint32", DataType: arrow.PrimitiveTypes.Uint32}
	case reflect.Uint16:
		return arrowTypeInfo{ArrowTypeName: "uint16", DataType: arrow.PrimitiveTypes.Uint16}
	case reflect.Uint8:
		return arrowTypeInfo{ArrowTypeName: "uint8", DataType: arrow.PrimitiveTypes.Uint8}
	case reflect.Float64:
		return arrowTypeInfo{ArrowTypeName: "double", DataType: arrow.PrimitiveTypes.Float64}
	case reflect.Float32:
		return arrowTypeInfo{ArrowTypeName: "float", DataType: arrow.PrimitiveTypes.Float32}
	case reflect.Slice:
		if t.Elem() == byteType {
			return arrowTypeInfo{ArrowTypeName: "blob", DataType: arrow.BinaryTypes.Binary}
		}
		inner := inferArrowType(t.Elem())
		var dt arrow.DataType
		if inner.DataType != nil {
			dt = arrow.ListOf(inner.DataType)
		}
		return arrowTypeInfo{ArrowTypeName: "list", DataType: dt}
	case reflect.Array:
		if t.Elem() == byteType {
			return arrowTypeInfo{ArrowTypeName: "blob", DataType: arrow.BinaryTypes.Binary}
		}
		inner := inferArrowType(t.Elem())
		var dt arrow.DataType
		if inner.DataType != nil {
			dt = arrow.FixedSizeListOf(int32(t.Len()), inner.DataType)
		}
		return arrowTypeInfo{ArrowTypeName: "fixed_list", DataType: dt}
	case reflect.Struct:
		fields := make([]arrow.Field, 0, t.NumField())
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			inner := inferArrowType(f.Type)
			name := snakeCase(f.Name)
			// Allow an inner `vgi:"name=..."` to rename a struct field.
			if tag, ok := f.Tag.Lookup(argTagKey); ok {
				for _, part := range splitTag(tag) {
					k, v, _ := strings.Cut(part, "=")
					if strings.ToLower(strings.TrimSpace(k)) == "name" {
						name = strings.Trim(strings.TrimSpace(v), "'")
					}
				}
			}
			if inner.DataType == nil {
				// Can't form a complete arrow.StructOf; leave DataType nil.
				return arrowTypeInfo{ArrowTypeName: "struct"}
			}
			fields = append(fields, arrow.Field{Name: name, Type: inner.DataType})
		}
		return arrowTypeInfo{ArrowTypeName: "struct", DataType: arrow.StructOf(fields...)}
	}
	return arrowTypeInfo{ArrowTypeName: "any"}
}

// ---------------------------------------------------------------------------
// TypeBound predicate registry
// ---------------------------------------------------------------------------

var (
	typeBoundMu       sync.RWMutex
	typeBoundRegistry = map[string]TypeBoundPredicate{}
)

// RegisterTypeBound registers a named TypeBoundPredicate so it can be
// referenced via `bound=name` in argument tags. Names are case-insensitive.
// Re-registering a name replaces the previous predicate.
func RegisterTypeBound(name string, pred TypeBoundPredicate) {
	if pred == nil {
		return
	}
	typeBoundMu.Lock()
	defer typeBoundMu.Unlock()
	typeBoundRegistry[strings.ToLower(name)] = pred
}

// LookupTypeBound returns the predicate registered under name, or nil.
func LookupTypeBound(name string) TypeBoundPredicate {
	typeBoundMu.RLock()
	defer typeBoundMu.RUnlock()
	return typeBoundRegistry[strings.ToLower(name)]
}

func resolveBounds(spec string) ([]TypeBoundPredicate, error) {
	if spec == "" {
		return nil, fmt.Errorf("bound= must not be empty")
	}
	parts := strings.Split(spec, "+")
	out := make([]TypeBoundPredicate, 0, len(parts))
	for _, p := range parts {
		name := strings.TrimSpace(p)
		if name == "" {
			continue
		}
		pred := LookupTypeBound(name)
		if pred == nil {
			return nil, fmt.Errorf("unknown TypeBound predicate %q (register with vgi.RegisterTypeBound)", name)
		}
		out = append(out, pred)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("bound= produced no predicates")
	}
	return out, nil
}

func init() {
	RegisterTypeBound("numeric", IsNumericType)
	RegisterTypeBound("integer", IsIntegerType)
	RegisterTypeBound("floating", IsFloatingType)
	RegisterTypeBound("decimal", IsDecimalType)
	RegisterTypeBound("temporal", IsTemporalType)
	RegisterTypeBound("addable", IsAddableType)
	RegisterTypeBound("multipliable", IsMultipliableType)
}

// ---------------------------------------------------------------------------
// Runtime extraction
// ---------------------------------------------------------------------------

// bindOneField populates a single struct field from the matching argument.
func bindOneField(field reflect.Value, b fieldBinding, args *Arguments) error {
	// Varargs: collect all positional args from Position onward into a slice.
	if b.Spec.IsVarargs {
		if field.Kind() != reflect.Slice {
			return fmt.Errorf("varargs field must be a slice, got %v", field.Kind())
		}
		// []any / []interface{} varargs aren't bound to scalar Go values —
		// the function should access the raw arrow.Array values from
		// params.Args.Positional directly. Leave the slice nil.
		if field.Type().Elem().Kind() == reflect.Interface {
			return nil
		}
		start := b.Spec.Position
		if start < 0 {
			start = 0
		}
		positional := args.Positional
		if start >= len(positional) {
			return nil // empty varargs is fine
		}
		elemKind := field.Type().Elem()
		out := reflect.MakeSlice(field.Type(), 0, len(positional)-start)
		for i := start; i < len(positional); i++ {
			elem := reflect.New(elemKind).Elem()
			if err := assignScalar(elem, args, i); err != nil {
				return fmt.Errorf("varargs[%d]: %w", i-start, err)
			}
			out = reflect.Append(out, elem)
		}
		field.Set(out)
		return nil
	}

	// Locate the argument by position (preferred) or name.
	key := lookupKey(b.Spec)
	if key == nil {
		// No positional and no name — shouldn't happen since parseArgTag
		// always assigns a name. Defensive.
		if b.Spec.HasDefault {
			return assignDefault(field, b.Spec.DefaultValue)
		}
		return nil
	}

	if args.IsNull(key) {
		if b.Spec.HasDefault {
			return assignDefault(field, b.Spec.DefaultValue)
		}
		return nil
	}
	return assignScalar(field, args, key)
}

func lookupKey(spec ArgSpec) any {
	if spec.Position >= 0 {
		return spec.Position
	}
	if spec.Name != "" {
		return spec.Name
	}
	return nil
}

// assignScalar extracts the Arrow scalar at key and writes it into field.
// Supports primitives, []byte, and time.Time. Returns a typed error for
// unsupported field kinds so callers can extend Arguments to handle them.
func assignScalar(field reflect.Value, args *Arguments, key any) error {
	ft := field.Type()
	switch {
	case ft == timeType:
		t, err := args.GetScalarTime(key)
		if err != nil {
			return err
		}
		field.Set(reflect.ValueOf(t))
		return nil
	case ft == durationType:
		d, err := args.GetScalarDuration(key)
		if err != nil {
			return err
		}
		field.Set(reflect.ValueOf(d))
		return nil
	case ft.Kind() == reflect.Slice && ft.Elem() == byteType:
		v, err := args.GetScalarBytes(key)
		if err != nil {
			return err
		}
		field.SetBytes(v)
		return nil
	}
	switch field.Kind() {
	case reflect.String:
		v, err := args.GetScalarString(key)
		if err != nil {
			return err
		}
		field.SetString(v)
	case reflect.Bool:
		v, err := args.GetScalarBool(key)
		if err != nil {
			return err
		}
		field.SetBool(v)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, err := args.GetScalarInt64(key)
		if err != nil {
			return err
		}
		if field.OverflowInt(v) {
			return fmt.Errorf("value %d overflows %v", v, field.Kind())
		}
		field.SetInt(v)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v, err := args.GetScalarInt64(key)
		if err != nil {
			return err
		}
		if v < 0 {
			return fmt.Errorf("negative value %d into unsigned field", v)
		}
		uv := uint64(v)
		if field.OverflowUint(uv) {
			return fmt.Errorf("value %d overflows %v", uv, field.Kind())
		}
		field.SetUint(uv)
	case reflect.Float32, reflect.Float64:
		v, err := args.GetScalarFloat64(key)
		if err != nil {
			return err
		}
		field.SetFloat(v)
	case reflect.Slice, reflect.Array, reflect.Struct, reflect.Interface:
		return fmt.Errorf("BindArgs does not yet support runtime extraction of %v scalars; use args.GetColumn(key) directly", field.Kind())
	default:
		return fmt.Errorf("unsupported field kind %v", field.Kind())
	}
	return nil
}

// assignDefault parses the textual DefaultValue and writes it into field.
func assignDefault(field reflect.Value, def string) error {
	if field.Type() == timeType {
		t, err := time.Parse(time.RFC3339Nano, def)
		if err != nil {
			return fmt.Errorf("invalid time.Time default %q: %w", def, err)
		}
		field.Set(reflect.ValueOf(t.UTC()))
		return nil
	}
	if field.Type() == durationType {
		// Empty default = zero (no duration). Otherwise parse via
		// time.ParseDuration so users can write `default=5s` /
		// `default=200ms` etc.
		var d time.Duration
		if def != "" {
			parsed, err := time.ParseDuration(def)
			if err != nil {
				return fmt.Errorf("invalid time.Duration default %q: %w", def, err)
			}
			d = parsed
		}
		field.Set(reflect.ValueOf(d))
		return nil
	}
	switch field.Kind() {
	case reflect.String:
		field.SetString(def)
	case reflect.Bool:
		b, err := strconv.ParseBool(def)
		if err != nil {
			return fmt.Errorf("invalid bool default %q: %w", def, err)
		}
		field.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(def, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid int default %q: %w", def, err)
		}
		if field.OverflowInt(n) {
			return fmt.Errorf("default %d overflows %v", n, field.Kind())
		}
		field.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(def, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid uint default %q: %w", def, err)
		}
		if field.OverflowUint(n) {
			return fmt.Errorf("default %d overflows %v", n, field.Kind())
		}
		field.SetUint(n)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(def, 64)
		if err != nil {
			return fmt.Errorf("invalid float default %q: %w", def, err)
		}
		field.SetFloat(f)
	case reflect.Slice:
		if field.Type().Elem() == byteType {
			field.SetBytes([]byte(def))
			return nil
		}
		return fmt.Errorf("no default-value parser for slice of %v", field.Type().Elem().Kind())
	default:
		return fmt.Errorf("no default-value parser for kind %v", field.Kind())
	}
	return nil
}
