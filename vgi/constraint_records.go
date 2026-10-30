// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"fmt"
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// Canonical constraint records (vgi.v3 TableInfo.constraints).
//
// A table's constraints are facts its provider attests: the SDK never derives
// them from schema shape or legacy descriptor fields. The DuckDB extension
// reads them only when the attach negotiated ConstraintRecordsReadProfile,
// accepts only ENFORCED + ALL_ROWS records, and requires every table's Arrow
// nullability to match its NOT_NULL/PRIMARY_KEY column paths exactly.

// ConstraintRecordsReadProfile is the catalog read profile under which a
// catalog serves canonical ConstraintInfo records.
const ConstraintRecordsReadProfile = "vgi.constraint.records.read.v1"

// ScalarExpressionReadProfile is the read profile CHECK records need: their
// expressions are DuckDB scalar SQL. It is selected only alongside
// ConstraintRecordsReadProfile.
const ScalarExpressionReadProfile = scalarExpressionProfile

// ConstraintKind is a ConstraintDefinition kind.
type ConstraintKind string

// Constraint kinds.
const (
	ConstraintNotNull    ConstraintKind = "NOT_NULL"
	ConstraintPrimaryKey ConstraintKind = "PRIMARY_KEY"
	ConstraintUnique     ConstraintKind = "UNIQUE"
	ConstraintCheck      ConstraintKind = "CHECK"
	ConstraintForeignKey ConstraintKind = "FOREIGN_KEY"
)

// ConstraintEnforcement states whether the provider enforces the constraint
// on every write path.
type ConstraintEnforcement string

// Enforcement states.
const (
	ConstraintEnforced     ConstraintEnforcement = "ENFORCED"
	ConstraintMetadataOnly ConstraintEnforcement = "METADATA_ONLY"
)

// ConstraintValidation states which existing rows the constraint covers. Only
// ALL_ROWS justifies engine facts.
type ConstraintValidation string

// Validation states.
const (
	ConstraintAllRows      ConstraintValidation = "ALL_ROWS"
	ConstraintFutureWrites ConstraintValidation = "FUTURE_WRITES"
	ConstraintUnvalidated  ConstraintValidation = "UNVALIDATED"
)

// ConstraintReference is a FOREIGN_KEY record's referenced key.
type ConstraintReference struct {
	// SchemaPath and Table name the referenced table.
	SchemaPath []string
	Table      string
	// Columns are the referenced raw column paths, in local-column order.
	Columns [][]string
	// ConstraintID is the ID of the referenced table's PRIMARY_KEY or UNIQUE record.
	ConstraintID string
}

// ConstraintRecordSpec describes one canonical ConstraintInfo record.
type ConstraintRecordSpec struct {
	// SchemaPath and Table name the owning table.
	SchemaPath []string
	Table      string
	// ID is the provider's stable identity for the constraint, unique within
	// the table. The SDK never derives it.
	ID   string
	Kind ConstraintKind
	// Columns are raw component paths, e.g. {{"id"}} or {{"bbox", "xmin"}};
	// empty for CHECK.
	Columns [][]string
	// Enforcement and Validation are the provider's attestation and must be
	// set explicitly. The DuckDB extension reads only ENFORCED + ALL_ROWS
	// records: state that only when every row the table can ever return
	// satisfies the constraint.
	Enforcement ConstraintEnforcement
	Validation  ConstraintValidation
	// DeclaredName is an optional SQL label (CONSTRAINT <name>).
	DeclaredName string
	// CheckSQL is required for CHECK (and only CHECK): a DuckDB scalar
	// expression over the table's columns.
	CheckSQL string
	// References is required for FOREIGN_KEY (and only FOREIGN_KEY).
	References *ConstraintReference
}

// ConstraintRecord encodes one canonical ConstraintInfo record for
// CatalogTable.Constraints / TableInfo.Constraints.
func ConstraintRecord(spec ConstraintRecordSpec) ([]byte, error) {
	if spec.ID == "" || strings.ContainsRune(spec.ID, 0) {
		return nil, fmt.Errorf("constraint id must be nonempty without NUL")
	}
	if spec.Table == "" {
		return nil, fmt.Errorf("constraint %q: owning table is required", spec.ID)
	}
	switch spec.Kind {
	case ConstraintNotNull, ConstraintPrimaryKey, ConstraintUnique, ConstraintCheck, ConstraintForeignKey:
	default:
		return nil, fmt.Errorf("constraint %q: unknown kind %q", spec.ID, spec.Kind)
	}
	switch spec.Enforcement {
	case ConstraintEnforced, ConstraintMetadataOnly:
	default:
		return nil, fmt.Errorf("constraint %q: enforcement must be ENFORCED or METADATA_ONLY, got %q", spec.ID, spec.Enforcement)
	}
	switch spec.Validation {
	case ConstraintAllRows, ConstraintFutureWrites, ConstraintUnvalidated:
	default:
		return nil, fmt.Errorf("constraint %q: validation must be ALL_ROWS, FUTURE_WRITES or UNVALIDATED, got %q", spec.ID, spec.Validation)
	}
	isCheck := spec.Kind == ConstraintCheck
	if isCheck != (spec.CheckSQL != "") {
		return nil, fmt.Errorf("constraint %q: CheckSQL is required for CHECK and only CHECK", spec.ID)
	}
	if isCheck && len(spec.Columns) != 0 {
		return nil, fmt.Errorf("constraint %q: CHECK takes no column paths", spec.ID)
	}
	if !isCheck && len(spec.Columns) == 0 {
		return nil, fmt.Errorf("constraint %q: %s requires column paths", spec.ID, spec.Kind)
	}
	if spec.Kind == ConstraintNotNull && len(spec.Columns) != 1 {
		return nil, fmt.Errorf("constraint %q: NOT_NULL requires exactly one column path", spec.ID)
	}
	if err := checkColumnPaths(spec.ID, spec.Columns); err != nil {
		return nil, err
	}
	if (spec.Kind == ConstraintForeignKey) != (spec.References != nil) {
		return nil, fmt.Errorf("constraint %q: References is required for FOREIGN_KEY and only FOREIGN_KEY", spec.ID)
	}

	var check, foreign, referenced any
	if isCheck {
		encoded, err := encodeCanonicalRow(generated.SqlExpressionSchema, map[string]any{
			"sql": spec.CheckSQL, "profile": scalarExpressionProfile, "schema_path": nonNilStrings(spec.SchemaPath),
			"evaluation_context": map[string]any{
				"timezone": "UTC", "collation": "binary", "default_order": "asc",
				"default_null_order": "nulls_last", "preserve_insertion_order": true,
			},
		})
		if err != nil {
			return nil, err
		}
		check = encoded
	}
	if ref := spec.References; ref != nil {
		if len(ref.Columns) != len(spec.Columns) {
			return nil, fmt.Errorf("constraint %q: foreign key local and referenced arity must match", spec.ID)
		}
		if err := checkColumnPaths(spec.ID, ref.Columns); err != nil {
			return nil, err
		}
		if ref.Table == "" || ref.ConstraintID == "" {
			return nil, fmt.Errorf("constraint %q: foreign key needs the referenced table and its key's constraint id", spec.ID)
		}
		table, err := catalogObjectRef("table", ref.SchemaPath, ref.Table, nil)
		if err != nil {
			return nil, err
		}
		if foreign, err = encodeCanonicalRow(generated.ForeignKeyDefinitionSchema, map[string]any{
			"referenced_table": table, "referenced_column_paths": ref.Columns,
			"match": "SIMPLE", "on_update": "NO_ACTION", "on_delete": "NO_ACTION",
		}); err != nil {
			return nil, err
		}
		owner := ref.Table
		if referenced, err = catalogObjectRef("constraint", ref.SchemaPath, ref.ConstraintID, &owner); err != nil {
			return nil, err
		}
	}
	var declared any
	if spec.DeclaredName != "" {
		declared = spec.DeclaredName
	}
	definition, err := encodeCanonicalRow(generated.ConstraintDefinitionSchema, map[string]any{
		"kind": string(spec.Kind), "column_paths": nonNilPaths(spec.Columns), "declared_name": declared,
		"check_expression": check, "foreign_key": foreign,
	})
	if err != nil {
		return nil, err
	}
	owner := spec.Table
	ref, err := catalogObjectRef("constraint", spec.SchemaPath, spec.ID, &owner)
	if err != nil {
		return nil, err
	}
	return encodeCanonicalRow(generated.ConstraintInfoSchema, map[string]any{
		"ref": ref, "definition": definition, "enforcement": string(spec.Enforcement),
		"validation": string(spec.Validation), "referenced_constraint": referenced,
	})
}

// ReadConstraintRecord decodes a TableInfo.constraints entry; it is the
// inverse of ConstraintRecord.
func ReadConstraintRecord(data []byte) (ConstraintRecordSpec, error) {
	var spec ConstraintRecordSpec
	info, err := readCanonicalRow(data, generated.ConstraintInfoSchema)
	if err != nil {
		return spec, fmt.Errorf("ConstraintInfo: %w", err)
	}
	defer info.Release()
	ref, err := readObjectRef(rowBinary(info, "ref"))
	if err != nil {
		return spec, err
	}
	spec.SchemaPath, spec.ID = ref.SchemaPath, ref.Name
	if ref.OwnerTable != nil {
		spec.Table = *ref.OwnerTable
	}
	spec.Enforcement = ConstraintEnforcement(rowEnum(info, "enforcement"))
	spec.Validation = ConstraintValidation(rowEnum(info, "validation"))

	definition, err := readCanonicalRow(rowBinary(info, "definition"), generated.ConstraintDefinitionSchema)
	if err != nil {
		return spec, fmt.Errorf("ConstraintDefinition: %w", err)
	}
	defer definition.Release()
	spec.Kind = ConstraintKind(rowEnum(definition, "kind"))
	spec.Columns = rowPaths(definition, "column_paths")
	if name := rowColumn(definition, "declared_name"); !name.IsNull(0) {
		spec.DeclaredName = name.(*array.String).Value(0)
	}
	if expr := rowColumn(definition, "check_expression"); !expr.IsNull(0) {
		row, err := readCanonicalRow(expr.(*array.Binary).Value(0), generated.SqlExpressionSchema)
		if err != nil {
			return spec, fmt.Errorf("SqlExpression: %w", err)
		}
		spec.CheckSQL = rowColumn(row, "sql").(*array.String).Value(0)
		row.Release()
	}
	if fk := rowColumn(definition, "foreign_key"); !fk.IsNull(0) {
		row, err := readCanonicalRow(fk.(*array.Binary).Value(0), generated.ForeignKeyDefinitionSchema)
		if err != nil {
			return spec, fmt.Errorf("ForeignKeyDefinition: %w", err)
		}
		defer row.Release()
		table, err := readObjectRef(rowBinary(row, "referenced_table"))
		if err != nil {
			return spec, err
		}
		spec.References = &ConstraintReference{SchemaPath: table.SchemaPath, Table: table.Name, Columns: rowPaths(row, "referenced_column_paths")}
		if target := rowColumn(info, "referenced_constraint"); !target.IsNull(0) {
			key, err := readObjectRef(target.(*array.Binary).Value(0))
			if err != nil {
				return spec, err
			}
			spec.References.ConstraintID = key.Name
		}
	}
	return spec, nil
}

// SelectReadProfiles returns the profiles in available that the client
// offered in its ClientCapabilities.catalog.read, sorted. The scalar
// expression profile is selected only alongside constraint records. A nil
// clientCapabilities (an older client) selects nothing.
func SelectReadProfiles(available []string, clientCapabilities []byte) ([]string, error) {
	if len(clientCapabilities) == 0 || len(available) == 0 {
		return []string{}, nil
	}
	client, err := readCanonicalRow(clientCapabilities, generated.ClientCapabilitiesSchema)
	if err != nil {
		return nil, fmt.Errorf("ClientCapabilities: %w", err)
	}
	defer client.Release()
	catalog := rowColumn(client, "catalog")
	if catalog.IsNull(0) {
		return []string{}, nil
	}
	offer, err := readCanonicalRow(catalog.(*array.Binary).Value(0), generated.CatalogCapabilitiesSchema)
	if err != nil {
		return nil, fmt.Errorf("CatalogCapabilities: %w", err)
	}
	defer offer.Release()
	offered := rowStrings(offer, "read")
	selected := []string{}
	for _, profile := range available {
		if slices.Contains(offered, profile) && !slices.Contains(selected, profile) {
			selected = append(selected, profile)
		}
	}
	return finishReadSelection(selected), nil
}

// finishReadSelection drops a scalar-expression read without constraint
// records and sorts the result.
func finishReadSelection(selected []string) []string {
	if !slices.Contains(selected, ConstraintRecordsReadProfile) {
		selected = slices.DeleteFunc(selected, func(p string) bool { return p == ScalarExpressionReadProfile })
	}
	slices.Sort(selected)
	return slices.Compact(selected)
}

// ValidateConstraintRecords checks a table's records the way the DuckDB
// extension does at ATTACH: every record is ENFORCED + ALL_ROWS, and the
// Arrow nullability of columns agrees with the NOT_NULL/PRIMARY_KEY records.
// A field (descending through ordinary STRUCTs, i.e. ones without an
// ARROW:extension:name or vgi.logical_type.* metadata key) must be
// non-nullable exactly when a record names its path. The is_row_id column is
// exempt. It applies to every table, including one with no records.
func ValidateConstraintRecords(table string, columns *arrow.Schema, constraints [][]byte) error {
	required := map[string]bool{}
	for _, data := range constraints {
		record, err := ReadConstraintRecord(data)
		if err != nil {
			return fmt.Errorf("table %q: %w", table, err)
		}
		// DuckDB reads only facts that hold for every row; anything else fails ATTACH.
		if record.Enforcement != ConstraintEnforced || record.Validation != ConstraintAllRows {
			return fmt.Errorf("table %q: constraint %q is %s/%s; DuckDB accepts only ENFORCED + ALL_ROWS records",
				table, record.ID, record.Enforcement, record.Validation)
		}
		if record.Kind == ConstraintNotNull || record.Kind == ConstraintPrimaryKey {
			for _, path := range record.Columns {
				required[pathKey(path)] = true
			}
		}
	}
	if columns == nil {
		return nil
	}
	var walk func(fields []arrow.Field, prefix []string, top bool) error
	walk = func(fields []arrow.Field, prefix []string, top bool) error {
		for _, field := range fields {
			if top && field.Metadata.FindKey("is_row_id") >= 0 {
				continue
			}
			path := append(slices.Clone(prefix), field.Name)
			if field.Nullable == required[pathKey(path)] {
				if field.Nullable {
					return fmt.Errorf("table %q: column %s is in a NOT_NULL/PRIMARY_KEY record but its Arrow field is nullable",
						table, strings.Join(path, "."))
				}
				return fmt.Errorf("table %q: column %s is non-nullable in Arrow but no NOT_NULL/PRIMARY_KEY record names it; "+
					"declare one with vgi.ConstraintRecord or make the field nullable", table, strings.Join(path, "."))
			}
			if children, ok := ordinaryStructChildren(field); ok {
				if err := walk(children, path, false); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(columns.Fields(), nil, true)
}

func ordinaryStructChildren(field arrow.Field) ([]arrow.Field, bool) {
	st, ok := field.Type.(*arrow.StructType)
	if !ok {
		return nil, false
	}
	for _, key := range field.Metadata.Keys() {
		if key == "ARROW:extension:name" || strings.HasPrefix(key, "vgi.logical_type.") {
			return nil, false
		}
	}
	return st.Fields(), true
}

// pathKey is an unambiguous key for a raw component path.
func pathKey(path []string) string {
	var b strings.Builder
	for _, part := range path {
		fmt.Fprintf(&b, "%d:%s", len(part), part)
	}
	return b.String()
}

func checkColumnPaths(id string, paths [][]string) error {
	for _, path := range paths {
		if len(path) == 0 {
			return fmt.Errorf("constraint %q: empty column path", id)
		}
		for _, part := range path {
			if part == "" || strings.ContainsRune(part, 0) {
				return fmt.Errorf("constraint %q: column path components must be nonempty without NUL", id)
			}
		}
	}
	return nil
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

func nonNilPaths(values [][]string) [][]string {
	if values == nil {
		return [][]string{}
	}
	return values
}

func catalogObjectRef(kind string, schemaPath []string, name string, owner *string) ([]byte, error) {
	var ownerValue any
	if owner != nil {
		ownerValue = *owner
	}
	return encodeCanonicalRow(generated.CatalogObjectRefSchema, map[string]any{
		"kind": kind, "schema_path": nonNilStrings(schemaPath), "name": name, "owner_table": ownerValue,
	})
}

func readObjectRef(data []byte) (generated.CatalogObjectRef, error) {
	row, err := readCanonicalRow(data, generated.CatalogObjectRefSchema)
	if err != nil {
		return generated.CatalogObjectRef{}, fmt.Errorf("CatalogObjectRef: %w", err)
	}
	defer row.Release()
	ref := generated.CatalogObjectRef{
		Kind:       rowColumn(row, "kind").(*array.String).Value(0),
		SchemaPath: rowStrings(row, "schema_path"),
		Name:       rowColumn(row, "name").(*array.String).Value(0),
	}
	if owner := rowColumn(row, "owner_table"); !owner.IsNull(0) {
		value := owner.(*array.String).Value(0)
		ref.OwnerTable = &value
	}
	return ref, nil
}

// encodeCanonicalRow builds one row of a generated protocol schema and
// serializes it as an IPC stream. Values: string, []byte, bool, []string,
// [][]string, map[string]any (struct) or nil (NULL).
func encodeCanonicalRow(schema *arrow.Schema, values map[string]any) ([]byte, error) {
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()
	for i, field := range schema.Fields() {
		value, ok := values[field.Name]
		if !ok || (value == nil && !field.Nullable) {
			return nil, fmt.Errorf("%s: missing required field", field.Name)
		}
		if err := appendCanonicalValue(builder.Field(i), value); err != nil {
			return nil, fmt.Errorf("%s: %w", field.Name, err)
		}
	}
	row := builder.NewRecordBatch()
	defer row.Release()
	return SerializeRecordBatch(row)
}

func appendCanonicalValue(builder array.Builder, value any) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}
	switch b := builder.(type) {
	case *array.StringBuilder:
		s, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
		b.Append(s)
	case *array.BinaryBuilder:
		v, ok := value.([]byte)
		if !ok {
			return fmt.Errorf("expected bytes, got %T", value)
		}
		b.Append(v)
	case *array.BooleanBuilder:
		v, ok := value.(bool)
		if !ok {
			return fmt.Errorf("expected bool, got %T", value)
		}
		b.Append(v)
	case *array.BinaryDictionaryBuilder:
		s, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected enum string, got %T", value)
		}
		return b.AppendString(s)
	case *array.ListBuilder:
		b.Append(true)
		switch items := value.(type) {
		case []string:
			for _, item := range items {
				if err := appendCanonicalValue(b.ValueBuilder(), item); err != nil {
					return err
				}
			}
		case [][]string:
			for _, item := range items {
				if err := appendCanonicalValue(b.ValueBuilder(), item); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unsupported list value %T", value)
		}
	case *array.StructBuilder:
		fields, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("expected struct map, got %T", value)
		}
		b.Append(true)
		for i, field := range b.Type().(*arrow.StructType).Fields() {
			if err := appendCanonicalValue(b.FieldBuilder(i), fields[field.Name]); err != nil {
				return fmt.Errorf("%s: %w", field.Name, err)
			}
		}
	default:
		return fmt.Errorf("unsupported builder %T", builder)
	}
	return nil
}

// readCanonicalRow reads exactly one complete single-row IPC record of schema.
func readCanonicalRow(data []byte, schema *arrow.Schema) (arrow.RecordBatch, error) {
	if len(data) == 0 || len(data) > 8*1024*1024 {
		return nil, fmt.Errorf("invalid record size")
	}
	source := bytes.NewReader(data)
	reader, err := ipc.NewReader(source)
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	if !reader.Next() {
		return nil, fmt.Errorf("record missing")
	}
	row := reader.RecordBatch()
	if row.NumRows() != 1 || !row.Schema().Equal(schema) {
		return nil, fmt.Errorf("record does not match the canonical schema")
	}
	for i, field := range schema.Fields() {
		if !field.Nullable && row.Column(i).IsNull(0) {
			return nil, fmt.Errorf("required field %s is NULL", field.Name)
		}
	}
	row.Retain()
	if reader.Next() || reader.Err() != nil || source.Len() != 0 {
		row.Release()
		return nil, fmt.Errorf("record requires exactly one complete IPC batch")
	}
	return row, nil
}

func rowColumn(row arrow.RecordBatch, name string) arrow.Array {
	return row.Column(row.Schema().FieldIndices(name)[0])
}

func rowBinary(row arrow.RecordBatch, name string) []byte {
	return rowColumn(row, name).(*array.Binary).Value(0)
}

func rowEnum(row arrow.RecordBatch, name string) string {
	values := rowColumn(row, name).(*array.Dictionary)
	return values.Dictionary().(*array.String).Value(values.GetValueIndex(0))
}

func rowStrings(row arrow.RecordBatch, name string) []string {
	list := rowColumn(row, name).(*array.List)
	start, end := list.ValueOffsets(0)
	values := list.ListValues().(*array.String)
	out := make([]string, 0, end-start)
	for i := start; i < end; i++ {
		out = append(out, values.Value(int(i)))
	}
	return out
}

func rowPaths(row arrow.RecordBatch, name string) [][]string {
	list := rowColumn(row, name).(*array.List)
	start, end := list.ValueOffsets(0)
	inner := list.ListValues().(*array.List)
	values := inner.ListValues().(*array.String)
	out := make([][]string, 0, end-start)
	for i := start; i < end; i++ {
		a, b := inner.ValueOffsets(int(i))
		path := make([]string, 0, b-a)
		for j := a; j < b; j++ {
			path = append(path, values.Value(int(j)))
		}
		out = append(out, path)
	}
	return out
}
