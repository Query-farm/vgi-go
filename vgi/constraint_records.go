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
// nullability to match its NOT_NULL/PRIMARY_KEY column paths exactly. The SDK
// serves nullability from the records (see ServedTableColumns), so authors
// never have to flip Arrow fields by hand.

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
//
// It rejects what the DuckDB extension refuses at ATTACH whenever the record
// alone shows it: anything but ENFORCED + ALL_ROWS, a schema path that is not
// exactly one component, nested column paths outside NOT_NULL, duplicate
// paths, a cross-schema FOREIGN KEY, and CHECK SQL outside the numeric
// comparison grammar. Checks that need the table's columns (path resolution,
// operand types, nullability) run when the table is served.
func ConstraintRecord(spec ConstraintRecordSpec) ([]byte, error) {
	if !validIdentifier(spec.ID) {
		return nil, fmt.Errorf("constraint id must be nonempty, at most 4096 bytes, without NUL")
	}
	if !validIdentifier(spec.Table) {
		return nil, fmt.Errorf("constraint %q: owning table is required", spec.ID)
	}
	if len(spec.SchemaPath) != 1 || !validIdentifier(spec.SchemaPath[0]) {
		return nil, fmt.Errorf("constraint %q: SchemaPath must be the owning table's one-component schema path; DuckDB reads no other", spec.ID)
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
	if spec.Enforcement != ConstraintEnforced || spec.Validation != ConstraintAllRows {
		return nil, fmt.Errorf("constraint %q is %s/%s; DuckDB accepts only ENFORCED + ALL_ROWS records, "+
			"so state a constraint only when every row the table can return satisfies it", spec.ID, spec.Enforcement, spec.Validation)
	}
	if spec.DeclaredName != "" && !validIdentifier(spec.DeclaredName) {
		return nil, fmt.Errorf("constraint %q: declared name must be at most 4096 bytes without NUL", spec.ID)
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
	if err := checkColumnPaths(spec.ID, spec.Columns, spec.Kind == ConstraintNotNull); err != nil {
		return nil, err
	}
	if isCheck {
		if err := validateNumericCheckSQL(spec.CheckSQL, nil); err != nil {
			return nil, fmt.Errorf("constraint %q: CHECK %q: %w", spec.ID, spec.CheckSQL, err)
		}
	}
	if (spec.Kind == ConstraintForeignKey) != (spec.References != nil) {
		return nil, fmt.Errorf("constraint %q: References is required for FOREIGN_KEY and only FOREIGN_KEY", spec.ID)
	}
	if ref := spec.References; ref != nil {
		if len(ref.SchemaPath) != 0 && !slices.Equal(ref.SchemaPath, spec.SchemaPath) {
			return nil, fmt.Errorf("constraint %q: foreign key must reference a table in its own schema %v; DuckDB refuses cross-schema keys", spec.ID, spec.SchemaPath)
		}
		if len(ref.Columns) != len(spec.Columns) {
			return nil, fmt.Errorf("constraint %q: foreign key local and referenced arity must match", spec.ID)
		}
		if err := checkColumnPaths(spec.ID, ref.Columns, false); err != nil {
			return nil, err
		}
		if !validIdentifier(ref.Table) || !validIdentifier(ref.ConstraintID) {
			return nil, fmt.Errorf("constraint %q: foreign key needs the referenced table and its key's constraint id", spec.ID)
		}
	}
	return encodeConstraintRecord(spec)
}

// encodeConstraintRecord encodes spec without ConstraintRecord's checks. A
// FOREIGN KEY with no referenced schema path references the owner's schema.
func encodeConstraintRecord(spec ConstraintRecordSpec) ([]byte, error) {
	var check, foreign, referenced any
	isCheck := spec.Kind == ConstraintCheck
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
		refSchema := ref.SchemaPath
		if len(refSchema) == 0 {
			refSchema = spec.SchemaPath
		}
		table, err := catalogObjectRef("table", refSchema, ref.Table, nil)
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
		if referenced, err = catalogObjectRef("constraint", refSchema, ref.ConstraintID, &owner); err != nil {
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

// decodedConstraint is a record plus the wire details ATTACH checks that
// ConstraintRecordSpec does not carry.
type decodedConstraint struct {
	spec ConstraintRecordSpec
	// CHECK expression envelope.
	checkProfile        string
	checkSchemaPath     []string
	checkDefaultContext bool
	// FOREIGN KEY definition and referenced key identity.
	fkMatch, fkOnUpdate, fkOnDelete string
	targetSchemaPath                []string
	targetOwner                     string
	hasForeignKey, hasTarget        bool
}

// ReadConstraintRecord decodes a TableInfo.constraints entry; it is the
// inverse of ConstraintRecord.
func ReadConstraintRecord(data []byte) (ConstraintRecordSpec, error) {
	decoded, err := decodeConstraintRecord(data)
	return decoded.spec, err
}

func decodeConstraintRecord(data []byte) (decodedConstraint, error) {
	var out decodedConstraint
	spec := &out.spec
	info, err := readCanonicalRow(data, generated.ConstraintInfoSchema)
	if err != nil {
		return out, fmt.Errorf("ConstraintInfo: %w", err)
	}
	defer info.Release()
	ref, err := readObjectRef(rowBinary(info, "ref"))
	if err != nil {
		return out, err
	}
	if ref.Kind != "constraint" {
		return out, fmt.Errorf("ConstraintInfo.ref has kind %q, want constraint", ref.Kind)
	}
	spec.SchemaPath, spec.ID = ref.SchemaPath, ref.Name
	if ref.OwnerTable != nil {
		spec.Table = *ref.OwnerTable
	}
	spec.Enforcement = ConstraintEnforcement(rowEnum(info, "enforcement"))
	spec.Validation = ConstraintValidation(rowEnum(info, "validation"))

	definition, err := readCanonicalRow(rowBinary(info, "definition"), generated.ConstraintDefinitionSchema)
	if err != nil {
		return out, fmt.Errorf("ConstraintDefinition: %w", err)
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
			return out, fmt.Errorf("SqlExpression: %w", err)
		}
		spec.CheckSQL = rowColumn(row, "sql").(*array.String).Value(0)
		out.checkProfile = rowColumn(row, "profile").(*array.String).Value(0)
		out.checkSchemaPath = rowStrings(row, "schema_path")
		out.checkDefaultContext = defaultEvaluationContext(rowColumn(row, "evaluation_context").(*array.Struct))
		row.Release()
	}
	if fk := rowColumn(definition, "foreign_key"); !fk.IsNull(0) {
		out.hasForeignKey = true
		row, err := readCanonicalRow(fk.(*array.Binary).Value(0), generated.ForeignKeyDefinitionSchema)
		if err != nil {
			return out, fmt.Errorf("ForeignKeyDefinition: %w", err)
		}
		defer row.Release()
		table, err := readObjectRef(rowBinary(row, "referenced_table"))
		if err != nil {
			return out, err
		}
		if table.Kind != "table" || table.OwnerTable != nil {
			return out, fmt.Errorf("ForeignKeyDefinition.referenced_table must be a table reference")
		}
		out.fkMatch, out.fkOnUpdate, out.fkOnDelete = rowEnum(row, "match"), rowEnum(row, "on_update"), rowEnum(row, "on_delete")
		spec.References = &ConstraintReference{SchemaPath: table.SchemaPath, Table: table.Name, Columns: rowPaths(row, "referenced_column_paths")}
	}
	if target := rowColumn(info, "referenced_constraint"); !target.IsNull(0) {
		key, err := readObjectRef(target.(*array.Binary).Value(0))
		if err != nil {
			return out, err
		}
		if key.Kind != "constraint" || key.OwnerTable == nil {
			return out, fmt.Errorf("ConstraintInfo.referenced_constraint must be an owned constraint reference")
		}
		out.hasTarget = true
		out.targetSchemaPath, out.targetOwner = key.SchemaPath, *key.OwnerTable
		if spec.References != nil {
			spec.References.ConstraintID = key.Name
		}
	}
	return out, nil
}

func defaultEvaluationContext(context *array.Struct) bool {
	values := map[string]any{}
	fields := context.DataType().(*arrow.StructType).Fields()
	for i, field := range fields {
		column := context.Field(i)
		if column.IsNull(0) {
			return false
		}
		switch v := column.(type) {
		case *array.String:
			values[field.Name] = v.Value(0)
		case *array.Boolean:
			values[field.Name] = v.Value(0)
		}
	}
	return values["timezone"] == "UTC" && values["collation"] == "binary" && values["default_order"] == "asc" &&
		values["default_null_order"] == "nulls_last" && values["preserve_insertion_order"] == true
}

// SelectReadProfiles returns the profiles in available that the client
// offered in its ClientCapabilities.catalog.read, sorted. The scalar
// expression profile is selected only alongside constraint records. A nil
// clientCapabilities (an older client) selects nothing.
func SelectReadProfiles(available []string, clientCapabilities []byte) ([]string, error) {
	selected, err := SelectCatalogCapabilities(CatalogCapabilities{Read: available}, clientCapabilities)
	if err != nil {
		return nil, err
	}
	return selected.Read, nil
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

// ServedTableColumns returns the Arrow schema a table is served with, after
// checking its constraint records the way the DuckDB extension does at ATTACH.
//
// Nullability follows the records, never the other way round: a field
// (descending through ordinary STRUCTs, i.e. ones without an
// ARROW:extension:name or vgi.logical_type.* metadata key) is served
// non-nullable exactly when a NOT_NULL or PRIMARY_KEY record names its path,
// and nullable otherwise. A non-nullable Arrow field nothing attests is
// therefore relaxed rather than refused, and a record-named nullable field is
// served non-nullable because every record is ENFORCED + ALL_ROWS. The
// top-level is_row_id column is left untouched. SerializeTableInfo applies
// this to every table it serializes.
//
// The records are rejected, naming the table, constraint id and path, when
// the extension would refuse them: an owner other than (schemaPath, table),
// duplicate constraint ids, duplicate declared names (case-insensitive), more
// than one PRIMARY KEY, column paths that do not resolve, a FOREIGN KEY to
// another schema or with an action other than NO ACTION/RESTRICT, CHECK SQL
// outside the numeric comparison grammar, anything but ENFORCED + ALL_ROWS,
// and a NOT_NULL/PRIMARY KEY on a generated column (which must be nullable).
func ServedTableColumns(schemaPath []string, table string, columns *arrow.Schema, constraints [][]byte) (*arrow.Schema, error) {
	if len(schemaPath) != 1 && len(constraints) > 0 {
		return nil, fmt.Errorf("table %q: constraint records need a one-component schema path, got %v", table, schemaPath)
	}
	required, err := checkTableConstraints(schemaPath, true, table, columns, constraints)
	if err != nil {
		return nil, err
	}
	if columns == nil {
		return nil, nil
	}
	return servedNullability(table, columns, required)
}

// ValidateConstraintRecords checks a table's records the way the DuckDB
// extension does at ATTACH (see ServedTableColumns), except for the owner
// schema, which it cannot see. Arrow nullability is no longer checked: the
// SDK serves it from the records.
//
// Deprecated: use ServedTableColumns, which also checks the owner schema and
// returns the schema the table is served with.
func ValidateConstraintRecords(table string, columns *arrow.Schema, constraints [][]byte) error {
	required, err := checkTableConstraints(nil, false, table, columns, constraints)
	if err != nil || columns == nil {
		return err
	}
	_, err = servedNullability(table, columns, required)
	return err
}

// checkTableConstraints mirrors DecodeNativeConstraints and returns the set
// of column paths NOT_NULL/PRIMARY_KEY records require to be non-NULL.
func checkTableConstraints(schemaPath []string, checkSchema bool, table string, columns *arrow.Schema, constraints [][]byte) (map[string]bool, error) {
	required := map[string]bool{}
	if len(constraints) > 1024 {
		return nil, fmt.Errorf("table %q: too many constraints (%d > 1024)", table, len(constraints))
	}
	total := 0
	ids := map[string]bool{}
	labels := map[string]string{}
	var primaryKey string
	rowID := -1
	if columns != nil {
		for i, field := range columns.Fields() {
			if isRowIDField(field) {
				rowID = i
			}
		}
	}
	for _, data := range constraints {
		total += len(data)
		if len(data) == 0 || len(data) > 8*1024*1024 || total > 8*1024*1024 {
			return nil, fmt.Errorf("table %q: constraint records exceed the 8 MiB limit", table)
		}
		decoded, err := decodeConstraintRecord(data)
		if err != nil {
			return nil, fmt.Errorf("table %q: %w", table, err)
		}
		record := decoded.spec
		fail := func(format string, args ...any) error {
			return fmt.Errorf("table %q: constraint %q: %s", table, record.ID, fmt.Sprintf(format, args...))
		}
		if !validIdentifier(record.ID) || len(record.SchemaPath) != 1 || !validIdentifier(record.SchemaPath[0]) {
			return nil, fail("identity must be a nonempty id with a one-component schema path, got schema %v", record.SchemaPath)
		}
		if record.Table != table || (checkSchema && !slices.Equal(record.SchemaPath, schemaPath)) {
			return nil, fail("belongs to %v.%q, not the serving table %v.%q", record.SchemaPath, record.Table, schemaPath, table)
		}
		if ids[record.ID] {
			return nil, fail("duplicate constraint id")
		}
		ids[record.ID] = true
		if record.Enforcement != ConstraintEnforced || record.Validation != ConstraintAllRows {
			return nil, fail("is %s/%s; DuckDB accepts only ENFORCED + ALL_ROWS records", record.Enforcement, record.Validation)
		}
		if record.DeclaredName != "" {
			if !validIdentifier(record.DeclaredName) {
				return nil, fail("invalid declared name")
			}
			key := strings.ToLower(record.DeclaredName)
			if other, dup := labels[key]; dup {
				return nil, fail("declared name %q duplicates constraint %q (names compare case-insensitively)", record.DeclaredName, other)
			}
			labels[key] = record.ID
		}
		if err := checkColumnPaths(record.ID, record.Columns, true); err != nil {
			return nil, fmt.Errorf("table %q: %w", table, err)
		}
		isCheck := record.Kind == ConstraintCheck
		if isCheck != (record.CheckSQL != "") || isCheck == (len(record.Columns) > 0) {
			return nil, fail("inconsistent CHECK payload or column paths")
		}
		isFK := record.Kind == ConstraintForeignKey
		if isFK != decoded.hasForeignKey || isFK != decoded.hasTarget {
			return nil, fail("inconsistent FOREIGN KEY payload")
		}
		switch record.Kind {
		case ConstraintCheck:
			if decoded.checkProfile != ScalarExpressionReadProfile {
				return nil, fail("unsupported CHECK expression profile %q", decoded.checkProfile)
			}
			if !slices.Equal(decoded.checkSchemaPath, record.SchemaPath) {
				return nil, fail("CHECK expression belongs to schema %v", decoded.checkSchemaPath)
			}
			if !decoded.checkDefaultContext {
				return nil, fail("unsupported CHECK evaluation context; DuckDB reads only UTC/binary/asc/nulls_last/preserve_insertion_order")
			}
			if err := validateNumericCheckSQL(record.CheckSQL, columns); err != nil {
				return nil, fail("CHECK %q: %v", record.CheckSQL, err)
			}
			continue
		case ConstraintNotNull:
			if len(record.Columns) != 1 {
				return nil, fail("NOT_NULL requires exactly one column path")
			}
			if columns != nil {
				if err := resolveNotNullPath(columns, rowID, record.Columns[0]); err != nil {
					return nil, fail("column %s: %v", strings.Join(record.Columns[0], "."), err)
				}
			}
			required[pathKey(record.Columns[0])] = true
			continue
		case ConstraintPrimaryKey, ConstraintUnique, ConstraintForeignKey:
		default:
			return nil, fail("unknown constraint kind %q", record.Kind)
		}
		for _, path := range record.Columns {
			if len(path) != 1 {
				return nil, fail("column %s: DuckDB supports nested constraint paths only for NOT_NULL", strings.Join(path, "."))
			}
			if columns != nil {
				if index := topLevelIndex(columns, path[0]); index < 0 || index == rowID {
					return nil, fail("column %s is missing, ambiguous or the row-id column", path[0])
				}
			}
		}
		switch record.Kind {
		case ConstraintPrimaryKey:
			if primaryKey != "" {
				return nil, fail("a table has at most one PRIMARY KEY (already %q)", primaryKey)
			}
			primaryKey = record.ID
			for _, path := range record.Columns {
				required[pathKey(path)] = true
			}
		case ConstraintForeignKey:
			ref := record.References
			if !slices.Equal(ref.SchemaPath, record.SchemaPath) || !slices.Equal(decoded.targetSchemaPath, record.SchemaPath) || decoded.targetOwner != ref.Table {
				return nil, fail("FOREIGN KEY must reference an identified key of a table in the same schema %v", record.SchemaPath)
			}
			if len(ref.Columns) != len(record.Columns) {
				return nil, fail("FOREIGN KEY arity mismatch")
			}
			for _, path := range ref.Columns {
				if len(path) != 1 {
					return nil, fail("referenced column %s: DuckDB supports only top-level FOREIGN KEY columns", strings.Join(path, "."))
				}
			}
			if decoded.fkMatch != "SIMPLE" {
				return nil, fail("DuckDB supports only MATCH SIMPLE foreign keys, got %s", decoded.fkMatch)
			}
			for _, action := range []string{decoded.fkOnUpdate, decoded.fkOnDelete} {
				if action != "NO_ACTION" && action != "RESTRICT" {
					return nil, fail("DuckDB supports only NO ACTION/RESTRICT referential actions, got %s", action)
				}
			}
		}
	}
	return required, nil
}

// resolveNotNullPath mirrors ResolveNotNullPath: an exact, unambiguous
// top-level field (not the row id), then ordinary STRUCT children.
func resolveNotNullPath(columns *arrow.Schema, rowID int, path []string) error {
	index := topLevelIndex(columns, path[0])
	if index < 0 || index == rowID {
		return fmt.Errorf("references a missing, ambiguous or row-id column")
	}
	field := columns.Field(index)
	for _, part := range path[1:] {
		children, ok := ordinaryStructChildren(field)
		if !ok {
			return fmt.Errorf("path must traverse ordinary STRUCT fields, not %s", field.Type)
		}
		found := -1
		for i, child := range children {
			if child.Name == part {
				if found >= 0 {
					return fmt.Errorf("ambiguous nested field %q", part)
				}
				found = i
			}
		}
		if found < 0 {
			return fmt.Errorf("missing nested field %q", part)
		}
		field = children[found]
	}
	return nil
}

func topLevelIndex(columns *arrow.Schema, name string) int {
	indices := columns.FieldIndices(name)
	if len(indices) != 1 {
		return -1
	}
	return indices[0]
}

// servedNullability rewrites Arrow nullability to match the required paths.
func servedNullability(table string, columns *arrow.Schema, required map[string]bool) (*arrow.Schema, error) {
	var rewrite func(fields []arrow.Field, prefix []string, top bool) ([]arrow.Field, error)
	rewrite = func(fields []arrow.Field, prefix []string, top bool) ([]arrow.Field, error) {
		out := make([]arrow.Field, len(fields))
		for i, field := range fields {
			out[i] = field
			if top && isRowIDField(field) {
				continue
			}
			path := append(slices.Clone(prefix), field.Name)
			needed := required[pathKey(path)]
			if needed && top && field.Metadata.FindKey(GeneratedExpressionKey) >= 0 {
				return nil, fmt.Errorf("table %q: column %s is generated, and DuckDB requires generated columns to be nullable; "+
					"no NOT_NULL/PRIMARY_KEY record may name it", table, strings.Join(path, "."))
			}
			out[i].Nullable = !needed
			if children, ok := ordinaryStructChildren(field); ok {
				served, err := rewrite(children, path, false)
				if err != nil {
					return nil, err
				}
				out[i].Type = arrow.StructOf(served...)
			}
		}
		return out, nil
	}
	fields, err := rewrite(columns.Fields(), nil, true)
	if err != nil {
		return nil, err
	}
	metadata := columns.Metadata()
	return arrow.NewSchema(fields, &metadata), nil
}

func validIdentifier(value string) bool {
	return value != "" && len(value) <= 4096 && !strings.ContainsRune(value, 0)
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

func checkColumnPaths(id string, paths [][]string, nested bool) error {
	if len(paths) > 1024 {
		return fmt.Errorf("constraint %q: too many column paths", id)
	}
	seen := map[string]bool{}
	for _, path := range paths {
		if len(path) == 0 || len(path) > 64 {
			return fmt.Errorf("constraint %q: empty or oversized column path", id)
		}
		if !nested && len(path) != 1 {
			return fmt.Errorf("constraint %q: column %s: DuckDB supports nested constraint paths only for NOT_NULL", id, strings.Join(path, "."))
		}
		for _, part := range path {
			if !validIdentifier(part) {
				return fmt.Errorf("constraint %q: column path components must be nonempty, at most 4096 bytes, without NUL", id)
			}
		}
		key := pathKey(path)
		if seen[key] {
			return fmt.Errorf("constraint %q: duplicate column path %s", id, strings.Join(path, "."))
		}
		seen[key] = true
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
		case [][]byte:
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
