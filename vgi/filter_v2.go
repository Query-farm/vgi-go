// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

const filterV2Semantics = "vgi.duckdb.standard.v1"

var payloadNameV2 = regexp.MustCompile(`^(value|type|artifact)_(0|[1-9][0-9]*)$`)

type evaluationContext struct{ Profile string }
type v2Predicate struct {
	ID       string
	Revision uint64
	Mode     string
	Expr     *v2Expr
}
type v2Expr struct {
	Node                    string
	ColumnIndex             int
	ColumnName              string
	FieldIndex              int
	FieldName               string
	ValueRef                int
	TypeRef                 int
	ArtifactRef             int
	Op                      string
	Negated                 bool
	Left, Right, Expression *v2Expr
	Children                []*v2Expr
	Set                     *v2Set
	Function                string
	Arguments               []*v2Expr
	Value                   arrow.Array
	ValueField              arrow.Field
	Target                  arrow.DataType
	DataType                arrow.DataType
	Bound                   bool
}
type v2Set struct {
	Values   arrow.Array
	DataType arrow.DataType
}

type v2Document struct {
	Encoding   string            `json:"encoding"`
	Semantics  string            `json:"semantics"`
	Kind       string            `json:"kind"`
	Predicates []json.RawMessage `json:"predicates"`
	Updates    []json.RawMessage `json:"updates"`
}

func supportsSpatialFilter(capabilities []FilterFunctionCapability) bool {
	for _, capability := range capabilities {
		if capability.Namespace == "duckdb.spatial" && capability.Name == "intersects_extent" && capability.Version == 1 {
			return true
		}
	}
	return false
}

func deserializeFiltersForMetadata(batch arrow.RecordBatch, joinKeys map[string]arrow.Array, joinKeyBatches []arrow.RecordBatch, meta FunctionMetadata, outputSchema *arrow.Schema) (*PushdownFilters, error) {
	return deserializeFiltersV2(batch, joinKeys, joinKeyBatches, supportsSpatialFilter(meta.AdditionalFilterFunctions), outputSchema)
}

func deserializeProcessFiltersForMetadata(params *ProcessParams, meta FunctionMetadata) (*PushdownFilters, error) {
	return deserializeFiltersForMetadata(
		params.PushdownFilters,
		params.JoinKeys,
		params.JoinKeyBatches,
		meta,
		params.BindOutputSchema,
	)
}

func deserializeFiltersV2(batch arrow.RecordBatch, joinKeys map[string]arrow.Array, joinKeyBatches []arrow.RecordBatch, allowSpatial bool, outputSchema *arrow.Schema) (*PushdownFilters, error) {
	context, raw, err := validateFilterV2Batch(batch)
	if err != nil {
		return nil, err
	}
	var doc v2Document
	if err := decodeStrict(raw, &doc); err != nil {
		return nil, fmt.Errorf("invalid filter JSON: %w", err)
	}
	if doc.Encoding != "vgi.filters.v2" || doc.Semantics != filterV2Semantics || doc.Kind != "snapshot" {
		return nil, fmt.Errorf("initial filter document must be a vgi.filters.v2 snapshot using %s", filterV2Semantics)
	}
	if doc.Updates != nil || len(doc.Predicates) > 1024 {
		return nil, fmt.Errorf("invalid snapshot members or predicate limit exceeded")
	}
	if len(doc.Predicates) != 0 && outputSchema == nil {
		return nil, fmt.Errorf("Filter Encoding v2 requires the authoritative unprojected bind output schema")
	}
	pf := &PushdownFilters{Version: "2", v2Revisions: map[string]uint64{}, v2Required: map[string]struct{}{}, v2Context: context, v2JoinKeyBatches: joinKeyBatches, v2Spatial: allowSpatial, v2OutputSchema: outputSchema}
	for i, value := range doc.Predicates {
		predicate, err := parseV2Predicate(value, batch, joinKeys, joinKeyBatches, allowSpatial, outputSchema, false)
		if err != nil {
			return nil, fmt.Errorf("predicate %d: %w", i, err)
		}
		if predicate.Revision != 0 {
			return nil, fmt.Errorf("snapshot predicate revisions must be zero")
		}
		if _, exists := pf.v2Revisions[predicate.ID]; exists {
			return nil, fmt.Errorf("duplicate predicate ID %q", predicate.ID)
		}
		pf.v2Revisions[predicate.ID] = 0
		if predicate.Mode == "required" {
			pf.v2Required[predicate.ID] = struct{}{}
		}
		pf.v2Predicates = append(pf.v2Predicates, predicate)
		pf.Filters = append(pf.Filters, publicV2Filter(predicate.Expr))
	}
	return pf, nil
}

func validateFilterV2Batch(batch arrow.RecordBatch) (evaluationContext, []byte, error) {
	if batch.NumRows() != 1 || batch.NumCols() == 0 {
		return evaluationContext{}, nil, fmt.Errorf("filter batch must contain exactly one row")
	}
	field := batch.Schema().Field(0)
	if field.Name != "filter_spec" || field.Type.ID() != arrow.STRING || field.Nullable {
		return evaluationContext{}, nil, fmt.Errorf("first field must be filter_spec: utf8 not null")
	}
	column, ok := batch.Column(0).(*array.String)
	if !ok || column.IsNull(0) {
		return evaluationContext{}, nil, fmt.Errorf("filter_spec must be non-null utf8")
	}
	raw := []byte(column.Value(0))
	if len(raw) > 1<<20 {
		return evaluationContext{}, nil, fmt.Errorf("filter JSON exceeds 1 MiB")
	}
	seen := map[string]bool{"filter_spec": true}
	for i := 1; i < int(batch.NumCols()); i++ {
		f := batch.Schema().Field(i)
		if seen[f.Name] || !payloadNameV2.MatchString(f.Name) {
			return evaluationContext{}, nil, fmt.Errorf("noncanonical or duplicate payload field %q", f.Name)
		}
		seen[f.Name] = true
		if strings.HasPrefix(f.Name, "type_") && !batch.Column(i).IsNull(0) {
			return evaluationContext{}, nil, fmt.Errorf("type payload must contain NULL")
		}
	}
	md := batch.Schema().Metadata()
	get := func(key string) string {
		idx := md.FindKey(key)
		if idx < 0 {
			return ""
		}
		return md.Values()[idx]
	}
	if get("vgi_filter_encoding") != "vgi.filters.v2" || get("vgi_filter_version") != "2" {
		return evaluationContext{}, nil, fmt.Errorf("unsupported filter encoding/version")
	}
	profile := get("vgi_evaluation_context")
	if profile != "vgi.none.v1" {
		return evaluationContext{}, nil, fmt.Errorf("evaluation context %q was not advertised", profile)
	}
	for _, key := range []string{"vgi_time_zone", "vgi_calendar", "vgi_default_collation", "vgi_ieee_floating_point_ops", "vgi_integer_division", "vgi_context_provider_fingerprint"} {
		if get(key) != "" {
			return evaluationContext{}, nil, fmt.Errorf("vgi.none.v1 forbids session context metadata")
		}
	}
	return evaluationContext{Profile: profile}, raw, nil
}

func parseV2Predicate(raw []byte, batch arrow.RecordBatch, joinKeys map[string]arrow.Array, joinKeyBatches []arrow.RecordBatch, allowSpatial bool, outputSchema *arrow.Schema, delta bool) (v2Predicate, error) {
	var object map[string]json.RawMessage
	if err := decodeStrict(raw, &object); err != nil {
		return v2Predicate{}, err
	}
	required := []string{"id", "revision", "mode", "source", "expression"}
	if delta {
		required = append(required, "operation")
	}
	if err := exactKeys(object, required...); err != nil {
		return v2Predicate{}, err
	}
	var id, mode, source string
	var revision uint64
	if err := json.Unmarshal(object["id"], &id); err != nil || id == "" || len(id) > 128 {
		return v2Predicate{}, fmt.Errorf("invalid predicate ID")
	}
	if err := json.Unmarshal(object["revision"], &revision); err != nil {
		return v2Predicate{}, fmt.Errorf("invalid revision")
	}
	_ = json.Unmarshal(object["mode"], &mode)
	_ = json.Unmarshal(object["source"], &source)
	if mode != "required" && mode != "advisory" {
		return v2Predicate{}, fmt.Errorf("unknown predicate mode")
	}
	if delta && mode != "advisory" {
		return v2Predicate{}, fmt.Errorf("delta upserts must be advisory")
	}
	if !containsString([]string{"query", "join", "top_n", "split_refinement", "other"}, source) {
		return v2Predicate{}, fmt.Errorf("unknown predicate source")
	}
	nodes := 0
	expr, err := parseV2Expr(object["expression"], batch, joinKeys, joinKeyBatches, allowSpatial, outputSchema, 1, true, &nodes)
	if err != nil {
		return v2Predicate{}, err
	}
	if expr.Node == "runtime_filter" && mode != "advisory" {
		return v2Predicate{}, fmt.Errorf("runtime_filter must be advisory")
	}
	if expr.Node != "runtime_filter" && (expr.DataType == nil || expr.DataType.ID() != arrow.BOOL) {
		return v2Predicate{}, fmt.Errorf("predicate root must resolve to BOOLEAN")
	}
	return v2Predicate{ID: id, Revision: revision, Mode: mode, Expr: expr}, nil
}

func parseV2Expr(raw []byte, batch arrow.RecordBatch, joinKeys map[string]arrow.Array, joinKeyBatches []arrow.RecordBatch, allowSpatial bool, outputSchema *arrow.Schema, depth int, root bool, nodes *int) (*v2Expr, error) {
	if depth > 64 {
		return nil, fmt.Errorf("expression exceeds depth limit")
	}
	*nodes++
	if *nodes > 10000 {
		return nil, fmt.Errorf("expression exceeds node limit")
	}
	var object map[string]json.RawMessage
	if err := decodeStrict(raw, &object); err != nil {
		return nil, err
	}
	var node string
	if err := json.Unmarshal(object["node"], &node); err != nil {
		return nil, fmt.Errorf("expression node missing")
	}
	result := &v2Expr{Node: node, ValueRef: -1, TypeRef: -1, ArtifactRef: -1}
	child := func(key string) (*v2Expr, error) {
		return parseV2Expr(object[key], batch, joinKeys, joinKeyBatches, allowSpatial, outputSchema, depth+1, false, nodes)
	}
	switch node {
	case "column_ref":
		if err := exactKeys(object, "node", "column_index", "column_name"); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(object["column_index"], &result.ColumnIndex); err != nil || result.ColumnIndex < 0 {
			return nil, fmt.Errorf("invalid column index")
		}
		if err := json.Unmarshal(object["column_name"], &result.ColumnName); err != nil || result.ColumnName == "" {
			return nil, fmt.Errorf("invalid column name")
		}
		if outputSchema != nil {
			if result.ColumnIndex >= outputSchema.NumFields() || outputSchema.Field(result.ColumnIndex).Name != result.ColumnName {
				return nil, fmt.Errorf("column_ref name/index does not match authoritative output schema")
			}
			result.DataType = outputSchema.Field(result.ColumnIndex).Type
			result.Bound = true
		}
	case "field_ref":
		if err := exactKeys(object, "node", "expression", "field_index", "field_name"); err != nil {
			return nil, err
		}
		var err error
		result.Expression, err = child("expression")
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(object["field_index"], &result.FieldIndex); err != nil || result.FieldIndex < 0 {
			return nil, fmt.Errorf("invalid field index")
		}
		if err := json.Unmarshal(object["field_name"], &result.FieldName); err != nil || result.FieldName == "" {
			return nil, fmt.Errorf("invalid field name")
		}
		if result.Expression.Bound {
			parent, ok := result.Expression.DataType.(*arrow.StructType)
			if !ok || result.FieldIndex >= parent.NumFields() || parent.Field(result.FieldIndex).Name != result.FieldName {
				return nil, fmt.Errorf("field_ref name/index does not match authoritative struct type")
			}
			result.DataType = parent.Field(result.FieldIndex).Type
			result.Bound = true
		}
	case "literal":
		if err := exactKeys(object, "node", "value_ref"); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(object["value_ref"], &result.ValueRef); err != nil || result.ValueRef < 0 {
			return nil, fmt.Errorf("invalid value_ref")
		}
		value, err := payloadColumn(batch, "value", result.ValueRef)
		if err != nil {
			return nil, err
		}
		result.Value = value
		result.ValueField = batch.Schema().Field(batch.Schema().FieldIndices(fmt.Sprintf("value_%d", result.ValueRef))[0])
		result.DataType = logicalV2Type(result.ValueField.Type)
	case "comparison", "arithmetic":
		if err := exactKeys(object, "node", "op", "left", "right"); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(object["op"], &result.Op)
		ops := []string{"eq", "ne", "lt", "le", "gt", "ge", "distinct_from", "not_distinct_from"}
		if node == "arithmetic" {
			ops = []string{"add", "subtract", "multiply", "divide", "modulo"}
		}
		if !containsString(ops, result.Op) {
			return nil, fmt.Errorf("unknown %s operator", node)
		}
		var err error
		result.Left, err = child("left")
		if err != nil {
			return nil, err
		}
		result.Right, err = child("right")
		if err != nil {
			return nil, err
		}
		if node == "comparison" {
			if !compatibleV2Types(result.Left.DataType, result.Right.DataType) {
				return nil, fmt.Errorf("comparison operands have incompatible types")
			}
			result.DataType = arrow.FixedWidthTypes.Boolean
		} else {
			if !numericV2Type(result.Left.DataType) || !arrow.TypeEqual(result.Left.DataType, result.Right.DataType) {
				return nil, fmt.Errorf("arithmetic operands require one exact numeric type")
			}
			if result.Op == "divide" || result.Op == "modulo" {
				return nil, fmt.Errorf("context-dependent arithmetic requires vgi.duckdb.session.v1")
			}
			result.DataType = result.Left.DataType
		}
	case "and", "or":
		if err := exactKeys(object, "node", "children"); err != nil {
			return nil, err
		}
		var children []json.RawMessage
		if err := json.Unmarshal(object["children"], &children); err != nil || len(children) < 2 {
			return nil, fmt.Errorf("and/or requires two children")
		}
		for _, value := range children {
			parsed, err := parseV2Expr(value, batch, joinKeys, joinKeyBatches, allowSpatial, outputSchema, depth+1, false, nodes)
			if err != nil {
				return nil, err
			}
			result.Children = append(result.Children, parsed)
			if parsed.DataType == nil || parsed.DataType.ID() != arrow.BOOL {
				return nil, fmt.Errorf("and/or children must resolve to BOOLEAN")
			}
		}
		result.DataType = arrow.FixedWidthTypes.Boolean
	case "not", "negate":
		if err := exactKeys(object, "node", "expression"); err != nil {
			return nil, err
		}
		var err error
		result.Expression, err = child("expression")
		if err != nil {
			return nil, err
		}
		if node == "not" {
			if result.Expression.DataType == nil || result.Expression.DataType.ID() != arrow.BOOL {
				return nil, fmt.Errorf("not operand must resolve to BOOLEAN")
			}
			result.DataType = arrow.FixedWidthTypes.Boolean
		} else {
			if !numericV2Type(result.Expression.DataType) {
				return nil, fmt.Errorf("negate operand must be numeric")
			}
			result.DataType = result.Expression.DataType
		}
	case "is_null":
		if err := exactKeys(object, "node", "expression", "negated"); err != nil {
			return nil, err
		}
		var err error
		result.Expression, err = child("expression")
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(object["negated"], &result.Negated); err != nil {
			return nil, err
		}
		result.DataType = arrow.FixedWidthTypes.Boolean
	case "in":
		if err := exactKeys(object, "node", "expression", "set", "negated"); err != nil {
			return nil, err
		}
		var err error
		result.Expression, err = child("expression")
		if err != nil {
			return nil, err
		}
		_ = json.Unmarshal(object["negated"], &result.Negated)
		result.Set, err = parseV2Set(object["set"], batch, joinKeys, joinKeyBatches)
		if err != nil {
			return nil, err
		}
		if !compatibleV2Types(result.Expression.DataType, result.Set.DataType) {
			return nil, fmt.Errorf("IN expression and set have incompatible types")
		}
		result.DataType = arrow.FixedWidthTypes.Boolean
	case "cast":
		if err := exactKeys(object, "node", "expression", "type_ref"); err != nil {
			return nil, err
		}
		var err error
		result.Expression, err = child("expression")
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(object["type_ref"], &result.TypeRef); err != nil || result.TypeRef < 0 {
			return nil, fmt.Errorf("invalid type_ref")
		}
		column, err := payloadColumn(batch, "type", result.TypeRef)
		if err != nil {
			return nil, err
		}
		if !column.IsNull(0) {
			return nil, fmt.Errorf("type payload must be NULL")
		}
		indices := batch.Schema().FieldIndices(fmt.Sprintf("type_%d", result.TypeRef))
		result.Target = batch.Schema().Field(indices[0]).Type
		result.DataType = logicalV2Type(result.Target)
		if contextualV2Type(result.Expression.DataType) && contextualV2Type(result.DataType) {
			return nil, fmt.Errorf("context-dependent cast requires vgi.duckdb.session.v1")
		}
	case "call":
		if err := allowedKeys(object, []string{"node", "function", "arguments"}, []string{"options"}); err != nil {
			return nil, err
		}
		_, hasOptions := object["options"]
		if object["function"][0] == '"' {
			_ = json.Unmarshal(object["function"], &result.Function)
			if !containsString([]string{"starts_with", "ends_with", "contains", "list_contains"}, result.Function) {
				return nil, fmt.Errorf("unknown standard filter function")
			}
			if hasOptions {
				return nil, fmt.Errorf("standard filter functions do not accept options")
			}
		} else {
			var identity struct {
				Namespace string `json:"namespace"`
				Name      string `json:"name"`
				Version   uint64 `json:"version"`
			}
			if err := decodeStrict(object["function"], &identity); err != nil {
				return nil, err
			}
			if identity.Namespace != "duckdb.spatial" || identity.Name != "intersects_extent" || identity.Version != 1 {
				return nil, fmt.Errorf("unknown extension filter function")
			}
			if !allowSpatial {
				return nil, fmt.Errorf("extension filter function was not advertised")
			}
			if hasOptions && string(object["options"]) != "{}" {
				return nil, fmt.Errorf("duckdb.spatial/intersects_extent@1 does not accept options")
			}
			result.Function = "duckdb.spatial/intersects_extent@1"
		}
		var arguments []json.RawMessage
		_ = json.Unmarshal(object["arguments"], &arguments)
		if len(arguments) > 256 {
			return nil, fmt.Errorf("too many call arguments")
		}
		for _, value := range arguments {
			parsed, err := parseV2Expr(value, batch, joinKeys, joinKeyBatches, allowSpatial, outputSchema, depth+1, false, nodes)
			if err != nil {
				return nil, err
			}
			result.Arguments = append(result.Arguments, parsed)
		}
		if result.Function == "duckdb.spatial/intersects_extent@1" && len(result.Arguments) != 2 {
			return nil, fmt.Errorf("duckdb.spatial/intersects_extent@1 requires exactly two arguments")
		}
		if result.Function != "duckdb.spatial/intersects_extent@1" && len(result.Arguments) != 2 {
			return nil, fmt.Errorf("standard filter functions require exactly two arguments")
		}
		if containsString([]string{"starts_with", "ends_with", "contains"}, result.Function) &&
			(!stringV2Type(result.Arguments[0].DataType) || !stringV2Type(result.Arguments[1].DataType)) {
			return nil, fmt.Errorf("string filter function arguments must be VARCHAR")
		}
		if result.Function == "list_contains" {
			list, ok := logicalV2Type(result.Arguments[0].DataType).(*arrow.ListType)
			if !ok || !compatibleV2Types(list.Elem(), result.Arguments[1].DataType) {
				return nil, fmt.Errorf("list_contains arguments have incompatible types")
			}
		}
		result.DataType = arrow.FixedWidthTypes.Boolean
	case "runtime_filter":
		if !root {
			return nil, fmt.Errorf("runtime_filter may appear only at root")
		}
		if err := exactKeys(object, "node", "algorithm", "input", "artifact_ref", "null_handling"); err != nil {
			return nil, err
		}
		var identity struct {
			Namespace string `json:"namespace"`
			Name      string `json:"name"`
			Version   uint64 `json:"version"`
		}
		if err := decodeStrict(object["algorithm"], &identity); err != nil {
			return nil, err
		}
		if identity.Namespace != "duckdb.runtime_filter" || identity.Version != 1 || (identity.Name != "bloom" && identity.Name != "prefix_range") {
			return nil, fmt.Errorf("unknown runtime filter")
		}
		if err := json.Unmarshal(object["artifact_ref"], &result.ArtifactRef); err != nil || result.ArtifactRef < 0 {
			return nil, fmt.Errorf("invalid artifact_ref")
		}
		if _, err := payloadColumn(batch, "artifact", result.ArtifactRef); err != nil {
			return nil, err
		}
		var nullHandling string
		if err := json.Unmarshal(object["null_handling"], &nullHandling); err != nil ||
			(nullHandling != "pass" && nullHandling != "reject") {
			return nil, fmt.Errorf("runtime_filter.null_handling must be pass or reject")
		}
		var err error
		result.Expression, err = child("input")
		if err != nil {
			return nil, err
		}
		result.DataType = arrow.FixedWidthTypes.Boolean
	default:
		return nil, fmt.Errorf("unknown expression node %q", node)
	}
	return result, nil
}

func parseV2Set(raw []byte, batch arrow.RecordBatch, joinKeys map[string]arrow.Array, joinKeyBatches []arrow.RecordBatch) (*v2Set, error) {
	var object map[string]json.RawMessage
	if err := decodeStrict(raw, &object); err != nil {
		return nil, err
	}
	var kind string
	if err := json.Unmarshal(object["kind"], &kind); err != nil {
		return nil, fmt.Errorf("invalid IN set kind")
	}
	if kind == "literal" {
		if err := exactKeys(object, "kind", "value_ref"); err != nil {
			return nil, err
		}
		var ref int
		if err := json.Unmarshal(object["value_ref"], &ref); err != nil || ref < 0 {
			return nil, fmt.Errorf("invalid literal IN value_ref")
		}
		column, err := payloadColumn(batch, "value", ref)
		if err != nil {
			return nil, err
		}
		list, ok := column.(*array.List)
		if !ok || list.IsNull(0) {
			return nil, fmt.Errorf("literal IN payload must be a non-null list")
		}
		start, end := list.ValueOffsets(0)
		values := array.NewSlice(list.ListValues(), start, end)
		return &v2Set{Values: values, DataType: logicalV2Type(values.DataType())}, nil
	}
	if kind == "external" {
		if err := exactKeys(object, "kind", "batch_index", "column_index", "column_name"); err != nil {
			return nil, err
		}
		var bi, ci int
		var name string
		if json.Unmarshal(object["batch_index"], &bi) != nil ||
			json.Unmarshal(object["column_index"], &ci) != nil ||
			json.Unmarshal(object["column_name"], &name) != nil || bi < 0 || ci < 0 || name == "" {
			return nil, fmt.Errorf("invalid external IN identity")
		}
		if len(joinKeyBatches) > 0 {
			if bi >= len(joinKeyBatches) {
				return nil, fmt.Errorf("external IN batch index unavailable")
			}
			keys := joinKeyBatches[bi]
			if ci >= int(keys.NumCols()) {
				return nil, fmt.Errorf("external IN column index unavailable")
			}
			if keys.Schema().Field(ci).Name != name {
				return nil, fmt.Errorf("external IN name does not match authoritative index")
			}
			values := keys.Column(ci)
			values.Retain()
			return &v2Set{Values: values, DataType: logicalV2Type(values.DataType())}, nil
		}
		if bi != 0 || ci != 0 {
			return nil, fmt.Errorf("external IN batch/column unavailable")
		}
		values := joinKeys[name]
		if values == nil {
			return nil, fmt.Errorf("external IN column unavailable")
		}
		values.Retain()
		return &v2Set{Values: values, DataType: logicalV2Type(values.DataType())}, nil
	}
	return nil, fmt.Errorf("unknown IN set kind")
}

func payloadColumn(batch arrow.RecordBatch, prefix string, ref int) (arrow.Array, error) {
	name := fmt.Sprintf("%s_%d", prefix, ref)
	indices := batch.Schema().FieldIndices(name)
	if len(indices) != 1 {
		return nil, fmt.Errorf("missing or duplicate payload %q", name)
	}
	return batch.Column(indices[0]), nil
}

func logicalV2Type(value arrow.DataType) arrow.DataType {
	if dictionary, ok := value.(*arrow.DictionaryType); ok {
		return dictionary.ValueType
	}
	return value
}

func compatibleV2Types(left, right arrow.DataType) bool {
	if left == nil || right == nil {
		return false
	}
	return arrow.TypeEqual(logicalV2Type(left), logicalV2Type(right))
}

func numericV2Type(value arrow.DataType) bool {
	if value == nil {
		return false
	}
	switch logicalV2Type(value).ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64, arrow.DECIMAL128, arrow.DECIMAL256:
		return true
	}
	return false
}

func stringV2Type(value arrow.DataType) bool {
	if value == nil {
		return false
	}
	id := logicalV2Type(value).ID()
	return id == arrow.STRING || id == arrow.LARGE_STRING
}

func contextualV2Type(value arrow.DataType) bool {
	if value == nil {
		return false
	}
	switch logicalV2Type(value).ID() {
	case arrow.STRING, arrow.LARGE_STRING, arrow.DATE32, arrow.DATE64,
		arrow.TIME32, arrow.TIME64, arrow.TIMESTAMP:
		return true
	}
	return false
}

// ApplyDelta validates every applicable update before committing any of them.
func (pf *PushdownFilters) ApplyDelta(batch arrow.RecordBatch, joinKeys ...map[string]arrow.Array) error {
	context, raw, err := validateFilterV2Batch(batch)
	if err != nil {
		return err
	}
	if context != pf.v2Context {
		return fmt.Errorf("evaluation context changed within scan")
	}
	var doc v2Document
	if err := decodeStrict(raw, &doc); err != nil {
		return err
	}
	if doc.Encoding != "vgi.filters.v2" || doc.Semantics != filterV2Semantics || doc.Kind != "delta" || doc.Predicates != nil {
		return fmt.Errorf("invalid v2 delta document")
	}
	keys := map[string]arrow.Array(nil)
	if len(joinKeys) > 0 {
		keys = joinKeys[0]
	}
	type update struct {
		Operation, ID string
		Revision      uint64
		Mode, Source  string
		Expression    json.RawMessage
	}
	nextPredicates := append([]v2Predicate(nil), pf.v2Predicates...)
	nextRevisions := make(map[string]uint64, len(pf.v2Revisions))
	for k, v := range pf.v2Revisions {
		nextRevisions[k] = v
	}
	seen := map[string]bool{}
	for _, rawUpdate := range doc.Updates {
		var object map[string]json.RawMessage
		if err := decodeStrict(rawUpdate, &object); err != nil {
			return err
		}
		var operation, id string
		var revision uint64
		if err := json.Unmarshal(object["operation"], &operation); err != nil ||
			json.Unmarshal(object["id"], &id) != nil || id == "" || len(id) > 128 ||
			json.Unmarshal(object["revision"], &revision) != nil {
			return fmt.Errorf("invalid delta update")
		}
		switch operation {
		case "remove":
			if err := exactKeys(object, "operation", "id", "revision"); err != nil {
				return err
			}
		case "upsert":
			if err := exactKeys(object, "operation", "id", "revision", "mode", "source", "expression"); err != nil {
				return err
			}
			var mode, source string
			var expression map[string]json.RawMessage
			if json.Unmarshal(object["mode"], &mode) != nil || mode != "advisory" ||
				json.Unmarshal(object["source"], &source) != nil || !containsString([]string{"query", "join", "top_n", "split_refinement", "other"}, source) ||
				json.Unmarshal(object["expression"], &expression) != nil || expression == nil {
				return fmt.Errorf("invalid delta upsert")
			}
		default:
			return fmt.Errorf("unknown delta operation")
		}
		if seen[id] {
			return fmt.Errorf("duplicate delta predicate ID")
		}
		seen[id] = true
		if _, required := pf.v2Required[id]; required {
			return fmt.Errorf("delta targets required predicate")
		}
		if old, ok := nextRevisions[id]; ok && revision <= old {
			continue
		}
		switch operation {
		case "remove":
			nextPredicates = removeV2Predicate(nextPredicates, id)
		case "upsert":
			predicate, err := parseV2Predicate(rawUpdate, batch, keys, pf.v2JoinKeyBatches, pf.v2Spatial, pf.v2OutputSchema, true)
			if err != nil {
				return err
			}
			nextPredicates = removeV2Predicate(nextPredicates, id)
			nextPredicates = append(nextPredicates, predicate)
		}
		nextRevisions[id] = revision
	}
	if len(nextRevisions) > 4096 {
		return fmt.Errorf("predicate ID limit exceeded")
	}
	pf.v2Predicates = nextPredicates
	pf.v2Revisions = nextRevisions
	pf.Filters = nil
	for i := range nextPredicates {
		pf.Filters = append(pf.Filters, publicV2Filter(nextPredicates[i].Expr))
	}
	return nil
}

func removeV2Predicate(values []v2Predicate, id string) []v2Predicate {
	out := values[:0]
	for _, v := range values {
		if v.ID != id {
			out = append(out, v)
		}
	}
	return out
}

func (pf *PushdownFilters) evaluateV2(ctx context.Context, batch arrow.RecordBatch) (arrow.Array, error) {
	result := makeBoolArray(true, int(batch.NumRows()))
	for _, predicate := range pf.v2Predicates {
		if predicate.Expr.Node == "runtime_filter" {
			continue
		}
		sql, err := predicate.Expr.sql(batch)
		if err == nil {
			var current arrow.Array
			current, err = evalExpressionAgainstBatch(ctx, batch, sql)
			if err == nil {
				lhs := compute.NewDatum(result)
				rhs := compute.NewDatum(current)
				combined, e := compute.CallFunction(ctx, "and_kleene", nil, lhs, rhs)
				lhs.Release()
				rhs.Release()
				result.Release()
				current.Release()
				if e != nil {
					return nil, e
				}
				result = combined.(*compute.ArrayDatum).MakeArray()
				combined.Release()
				continue
			}
		}
		if predicate.Mode == "advisory" {
			continue
		}
		result.Release()
		return nil, err
	}
	return result, nil
}

func (expression *v2Expr) sql(batch arrow.RecordBatch) (string, error) {
	switch expression.Node {
	case "column_ref":
		index := expression.ColumnIndex
		if index < 0 || index >= int(batch.NumCols()) || batch.Schema().Field(index).Name != expression.ColumnName {
			if !expression.Bound {
				return "", fmt.Errorf("unbound column_ref cannot be remapped in a projected evaluation batch")
			}
			indices := batch.Schema().FieldIndices(expression.ColumnName)
			if len(indices) != 1 {
				return "", fmt.Errorf("authoritative column_ref is absent or ambiguous in evaluation batch")
			}
			index = indices[0]
		}
		field := batch.Schema().Field(index)
		if expression.Bound && !arrow.TypeEqual(field.Type, expression.DataType) {
			return "", fmt.Errorf("column_ref type changed in projected evaluation batch")
		}
		column := `"` + escapeIdent(field.Name) + `"`
		if isWKBField(field) {
			return "ST_GeomFromWKB(" + column + ")", nil
		}
		return column, nil
	case "field_ref":
		parent, err := expression.Expression.sql(batch)
		if err != nil {
			return "", err
		}
		return "struct_extract(" + parent + ", '" + strings.ReplaceAll(expression.FieldName, "'", "''") + "')", nil
	case "literal":
		return renderArrayValueField(expression.Value, 0, expression.ValueField)
	case "comparison":
		left, err := expression.Left.sql(batch)
		if err != nil {
			return "", err
		}
		right, err := expression.Right.sql(batch)
		if err != nil {
			return "", err
		}
		operators := map[string]string{"eq": "=", "ne": "!=", "lt": "<", "le": "<=", "gt": ">", "ge": ">=", "distinct_from": "IS DISTINCT FROM", "not_distinct_from": "IS NOT DISTINCT FROM"}
		return "(" + left + " " + operators[expression.Op] + " " + right + ")", nil
	case "and", "or":
		parts := make([]string, len(expression.Children))
		for i, child := range expression.Children {
			value, err := child.sql(batch)
			if err != nil {
				return "", err
			}
			parts[i] = value
		}
		return "(" + strings.Join(parts, " "+strings.ToUpper(expression.Node)+" ") + ")", nil
	case "not":
		value, err := expression.Expression.sql(batch)
		return "(NOT " + value + ")", err
	case "is_null":
		value, err := expression.Expression.sql(batch)
		if err != nil {
			return "", err
		}
		suffix := " IS NULL"
		if expression.Negated {
			suffix = " IS NOT NULL"
		}
		return "(" + value + suffix + ")", nil
	case "in":
		value, err := expression.Expression.sql(batch)
		if err != nil {
			return "", err
		}
		items := make([]string, expression.Set.Values.Len())
		for i := range items {
			items[i], err = renderArrayValue(expression.Set.Values, i)
			if err != nil {
				return "", err
			}
		}
		op := " IN "
		if expression.Negated {
			op = " NOT IN "
		}
		return "(" + value + op + "(" + strings.Join(items, ",") + "))", nil
	case "cast":
		value, err := expression.Expression.sql(batch)
		if err != nil {
			return "", err
		}
		target, err := duckDBTypeFor(arrow.Field{Type: expression.Target})
		if err != nil {
			return "", err
		}
		return "CAST(" + value + " AS " + target + ")", nil
	case "arithmetic":
		left, err := expression.Left.sql(batch)
		if err != nil {
			return "", err
		}
		right, err := expression.Right.sql(batch)
		if err != nil {
			return "", err
		}
		op := map[string]string{"add": "+", "subtract": "-", "multiply": "*", "divide": "/", "modulo": "%"}[expression.Op]
		return "(" + left + op + right + ")", nil
	case "negate":
		value, err := expression.Expression.sql(batch)
		return "(-" + value + ")", err
	case "call":
		parts := make([]string, len(expression.Arguments))
		for i, arg := range expression.Arguments {
			value, err := arg.sql(batch)
			if err != nil {
				return "", err
			}
			parts[i] = value
		}
		if expression.Function == "duckdb.spatial/intersects_extent@1" {
			return "st_intersects_extent(" + strings.Join(parts, ",") + ")", nil
		}
		return expression.Function + "(" + strings.Join(parts, ",") + ")", nil
	}
	return "", fmt.Errorf("runtime filter has no evaluator")
}

// v2FilterView preserves the ergonomic Filter helper API over the v2 AST.
type v2FilterView struct{ expr *v2Expr }

// publicV2Filter projects the v2 AST onto the original ergonomic filter
// helpers when the expression has an equivalent representation. Complex v2
// expressions retain a view backed directly by the v2 evaluator.
func publicV2Filter(expr *v2Expr) Filter {
	if expr == nil {
		return &v2FilterView{expr: expr}
	}
	switch expr.Node {
	case "comparison":
		ref, literal, op := expr.Left, expr.Right, ComparisonOp(expr.Op)
		if ref == nil || ref.Node != "column_ref" || literal == nil || literal.Node != "literal" {
			ref, literal = expr.Right, expr.Left
			if inverse, ok := map[ComparisonOp]ComparisonOp{OpLT: OpGT, OpLE: OpGE, OpGT: OpLT, OpGE: OpLE}[op]; ok {
				op = inverse
			}
		}
		if ref != nil && ref.Node == "column_ref" && literal != nil && literal.Node == "literal" &&
			containsString([]string{string(OpEQ), string(OpNE), string(OpLT), string(OpLE), string(OpGT), string(OpGE)}, string(op)) && literal.Value.Len() == 1 {
			value, err := scalar.GetScalar(literal.Value, 0)
			if err == nil {
				return &ConstantFilter{columnName: ref.ColumnName, columnIndex: ref.ColumnIndex, Op: op, Value: value}
			}
		}
	case "is_null":
		if expr.Expression != nil && expr.Expression.Node == "column_ref" {
			if expr.Negated {
				return &IsNotNullFilter{columnName: expr.Expression.ColumnName, columnIndex: expr.Expression.ColumnIndex}
			}
			return &IsNullFilter{columnName: expr.Expression.ColumnName, columnIndex: expr.Expression.ColumnIndex}
		}
	case "in":
		if !expr.Negated && expr.Expression != nil && expr.Expression.Node == "column_ref" && expr.Set != nil {
			return &InFilter{columnName: expr.Expression.ColumnName, columnIndex: expr.Expression.ColumnIndex, Values: expr.Set.Values}
		}
	case "and", "or":
		children := make([]Filter, len(expr.Children))
		for i, child := range expr.Children {
			children[i] = publicV2Filter(child)
		}
		name, index := v2RootColumn(expr), (&v2FilterView{expr: expr}).ColumnIndex()
		if expr.Node == "and" {
			return &AndFilter{columnName: name, columnIndex: index, Children: children}
		}
		return &OrFilter{columnName: name, columnIndex: index, Children: children}
	}
	return &v2FilterView{expr: expr}
}

func (f *v2FilterView) ColumnName() string { return v2RootColumn(f.expr) }
func (f *v2FilterView) ColumnIndex() int {
	e := f.expr
	for e != nil && e.Node == "field_ref" {
		e = e.Expression
	}
	if e != nil && e.Node == "column_ref" {
		return e.ColumnIndex
	}
	return -1
}
func (f *v2FilterView) Type() FilterType {
	switch f.expr.Node {
	case "comparison":
		return FilterConstant
	case "in":
		return FilterIn
	case "is_null":
		if f.expr.Negated {
			return FilterIsNotNull
		}
		return FilterIsNull
	case "and":
		return FilterAnd
	case "or":
		return FilterOr
	case "field_ref":
		return FilterStruct
	}
	return FilterExpression
}
func (f *v2FilterView) Evaluate(ctx context.Context, batch arrow.RecordBatch) (arrow.Array, error) {
	sql, err := f.expr.sql(batch)
	if err != nil {
		return nil, err
	}
	return evalExpressionAgainstBatch(ctx, batch, sql)
}
func v2RootColumn(e *v2Expr) string {
	if e == nil {
		return ""
	}
	if e.Node == "column_ref" {
		return e.ColumnName
	}
	if e.Expression != nil {
		return v2RootColumn(e.Expression)
	}
	if e.Left != nil {
		return v2RootColumn(e.Left)
	}
	for _, child := range e.Children {
		if name := v2RootColumn(child); name != "" {
			return name
		}
	}
	return ""
}

func renderArrayValue(column arrow.Array, index int) (string, error) {
	return renderArrayValueField(column, index, arrow.Field{})
}

func renderArrayValueField(column arrow.Array, index int, field arrow.Field) (string, error) {
	if column.IsNull(index) {
		return "NULL", nil
	}
	switch value := column.(type) {
	case *array.Dictionary:
		field.Type = value.Dictionary().DataType()
		return renderArrayValueField(value.Dictionary(), value.GetValueIndex(index), field)
	case *array.Int64:
		return fmt.Sprintf("%d", value.Value(index)), nil
	case *array.Int32:
		return fmt.Sprintf("%d", value.Value(index)), nil
	case *array.Float64:
		return fmt.Sprintf("%g", value.Value(index)), nil
	case *array.Float32:
		return fmt.Sprintf("%g", value.Value(index)), nil
	case *array.Boolean:
		if value.Value(index) {
			return "TRUE", nil
		}
		return "FALSE", nil
	case *array.String:
		return "'" + strings.ReplaceAll(value.Value(index), "'", "''") + "'", nil
	case *array.Binary:
		hexValue := fmt.Sprintf("%x", value.Value(index))
		if isWKBField(field) {
			return "ST_GeomFromHEXWKB('" + hexValue + "')", nil
		}
		return "'\\x" + hexValue + "'::BLOB", nil
	}
	return "", fmt.Errorf("unsupported literal type %s", column.DataType())
}

func decodeStrict(raw []byte, target any) error {
	if err := rejectDuplicateJSON(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.More() {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}
func rejectDuplicateJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		switch token := token.(type) {
		case json.Delim:
			if token == '{' {
				seen := map[string]bool{}
				for decoder.More() {
					keyToken, err := decoder.Token()
					if err != nil {
						return err
					}
					key := keyToken.(string)
					if seen[key] {
						return fmt.Errorf("duplicate JSON key %q", key)
					}
					seen[key] = true
					if err := walk(); err != nil {
						return err
					}
				}
				_, err = decoder.Token()
				return err
			}
			if token == '[' {
				for decoder.More() {
					if err := walk(); err != nil {
						return err
					}
				}
				_, err = decoder.Token()
				return err
			}
		}
		return nil
	}
	return walk()
}
func exactKeys(object map[string]json.RawMessage, keys ...string) error {
	return allowedKeys(object, keys, nil)
}
func allowedKeys(object map[string]json.RawMessage, required, optional []string) error {
	allowed := map[string]bool{}
	for _, key := range required {
		allowed[key] = true
		if _, ok := object[key]; !ok {
			return fmt.Errorf("missing property %q", key)
		}
	}
	for _, key := range optional {
		allowed[key] = true
	}
	for key := range object {
		if !allowed[key] {
			return fmt.Errorf("unknown property %q", key)
		}
	}
	return nil
}
func containsString(values []string, target string) bool {
	index := sort.SearchStrings(appendSorted(values), target)
	sorted := appendSorted(values)
	return index < len(sorted) && sorted[index] == target
}
func appendSorted(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}
