// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// Writable-catalog constraints.
//
// The writable catalog enforces exactly one constraint kind: NOT NULL on a
// top-level column, checked on every write path it has (INSERT through
// rowsAppend, UPDATE through rowsUpdate; it has no ALTER). CREATE TABLE ...
// NOT NULL records a canonical ENFORCED + ALL_ROWS NOT_NULL record per
// column, and ATTACH negotiates the read, create and enforce profiles that
// lets DuckDB send and read them. PRIMARY KEY, UNIQUE, CHECK and FOREIGN KEY
// are not enforced, so CREATE TABLE with them is refused.
//
// The extension sends constrained CREATE TABLE inside a worker transaction
// only under vgi.transaction.ddl.v1, which this catalog does not claim (its
// DDL is not rolled back). On a worker built WithTransactions, DuckDB
// therefore refuses CREATE TABLE ... NOT NULL itself.

// writableCapabilities are the profiles a writable catalog advertises.
var writableCapabilities = CatalogCapabilities{
	Read:  []string{ConstraintRecordsReadProfile},
	Ddl:   []string{ConstraintCreateProfile},
	Write: []string{ConstraintEnforceProfile},
}

// notNullConstraintID is the identity this catalog gives the NOT_NULL record
// of a column.
func notNullConstraintID(column string) string { return "not_null:" + column }

// notNullRecords validates CREATE TABLE constraint definitions and returns
// the canonical NOT_NULL records for them, plus the set of required columns.
// Anything but a top-level NOT NULL is refused.
func notNullRecords(schemaPath []string, table string, schema *arrow.Schema, definitions [][]byte) ([][]byte, map[string]bool, error) {
	required := map[string]bool{}
	if len(definitions) == 0 {
		return [][]byte{}, required, nil
	}
	if len(schemaPath) != 1 {
		return nil, nil, methodNotImplemented("catalog_table_create: NOT NULL constraints need a one-component schema path")
	}
	records := make([][]byte, 0, len(definitions))
	for _, data := range definitions {
		kind, paths, declared, err := decodeConstraintDefinition(data)
		if err != nil {
			return nil, nil, fmt.Errorf("catalog_table_create: %w", err)
		}
		if kind != ConstraintNotNull {
			return nil, nil, methodNotImplemented(fmt.Sprintf("catalog_table_create: %s constraints are not enforced by this writable catalog; "+
				"it enforces only NOT NULL on top-level columns", strings.ReplaceAll(string(kind), "_", " ")))
		}
		if len(paths) != 1 || len(paths[0]) != 1 {
			return nil, nil, methodNotImplemented("catalog_table_create: this writable catalog enforces NOT NULL only on top-level columns")
		}
		column := paths[0][0]
		if topLevelIndex(schema, column) < 0 {
			return nil, nil, fmt.Errorf("catalog_table_create: NOT NULL names unknown column %q", column)
		}
		if required[column] {
			continue
		}
		required[column] = true
		record, err := ConstraintRecord(ConstraintRecordSpec{
			SchemaPath: schemaPath, Table: table, ID: notNullConstraintID(column), Kind: ConstraintNotNull,
			Columns: [][]string{{column}}, DeclaredName: declared,
			Enforcement: ConstraintEnforced, Validation: ConstraintAllRows,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("catalog_table_create: %w", err)
		}
		records = append(records, record)
	}
	return records, required, nil
}

// decodeConstraintDefinition reads the parts of a ConstraintDefinition this
// catalog acts on.
func decodeConstraintDefinition(data []byte) (ConstraintKind, [][]string, string, error) {
	row, err := readCanonicalRow(data, generated.ConstraintDefinitionSchema)
	if err != nil {
		return "", nil, "", fmt.Errorf("ConstraintDefinition: %w", err)
	}
	defer row.Release()
	declared := ""
	if name := rowColumn(row, "declared_name"); !name.IsNull(0) {
		declared = name.(*array.String).Value(0)
	}
	return ConstraintKind(rowEnum(row, "kind")), rowPaths(row, "column_paths"), declared, nil
}

// requiredColumns returns the top-level columns a table's NOT_NULL and
// PRIMARY_KEY records require to be non-NULL.
func requiredColumns(constraints [][]byte) ([]string, error) {
	var out []string
	for _, data := range constraints {
		record, err := ReadConstraintRecord(data)
		if err != nil {
			return nil, err
		}
		if record.Kind != ConstraintNotNull && record.Kind != ConstraintPrimaryKey {
			continue
		}
		for _, path := range record.Columns {
			if len(path) == 1 && !slices.Contains(out, path[0]) {
				out = append(out, path[0])
			}
		}
	}
	return out, nil
}

// checkNotNullRow refuses a row whose required column is NULL or absent.
func checkNotNullRow(table string, required []string, row map[string]interface{}) error {
	for _, column := range required {
		if value, ok := row[column]; !ok || value == nil {
			return fmt.Errorf("NOT NULL constraint failed: %s.%s", table, column)
		}
	}
	return nil
}

// rebindRecords re-issues this catalog's records for the table name and
// schema the client asked for: the store keys tables case-insensitively, but
// a record's owner must equal the served table exactly.
func rebindRecords(records [][]byte, schemaPath []string, table string) ([][]byte, error) {
	out := make([][]byte, 0, len(records))
	for _, data := range records {
		spec, err := ReadConstraintRecord(data)
		if err != nil {
			return nil, err
		}
		if spec.Table == table && slices.Equal(spec.SchemaPath, schemaPath) {
			out = append(out, data)
			continue
		}
		spec.Table, spec.SchemaPath = table, schemaPath
		rebound, err := ConstraintRecord(spec)
		if err != nil {
			return nil, err
		}
		out = append(out, rebound)
	}
	return out, nil
}

// migrateLegacyConstraints upgrades tables stored before vgi.v3. A stored
// NOT NULL becomes a canonical record when every stored row satisfies it
// (this catalog now enforces NOT NULL); a NOT NULL that existing rows
// violate, and every PRIMARY KEY/UNIQUE/CHECK/FOREIGN KEY (never enforced
// here), is dropped with a warning. The upgraded metadata is persisted, so
// this runs once per table; it runs at ATTACH so the warnings surface there.
func (c *WritableCatalog) migrateLegacyConstraints() error {
	schemas, err := c.store.schemaList(c.Name)
	if err != nil {
		return err
	}
	for _, schema := range schemas {
		names, err := c.store.tableList(c.Name, schema.Name)
		if err != nil {
			return err
		}
		for _, name := range names {
			t, err := c.store.tableLoad(c.Name, schema.Name, name)
			if err != nil {
				return err
			}
			if t == nil || t.legacy == nil {
				continue
			}
			if err := c.migrateLegacyTable(strings.Split(schema.Name, "\x00"), schema.Name, t); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *WritableCatalog) migrateLegacyTable(schemaPath []string, schemaKey string, t *writableTable) error {
	legacy := t.legacy
	dropped := len(legacy.PrimaryKey) + len(legacy.Unique) + len(legacy.Check) + len(legacy.ForeignKey)
	if dropped > 0 {
		LogCatalog.Warn("writable catalog: dropping stored PRIMARY KEY/UNIQUE/CHECK/FOREIGN KEY metadata from before vgi.v3; "+
			"this catalog never enforced it and does not serve it", "catalog", c.Name, "table", t.name, "constraints", dropped)
	}
	if len(legacy.NotNull) > 0 {
		rows, err := c.store.rowsScan(c.Name, schemaKey, t.name)
		if err != nil {
			return err
		}
		existing, err := requiredColumns(t.constraints)
		if err != nil {
			return err
		}
		for _, column := range legacy.NotNull {
			if slices.Contains(existing, column) {
				continue
			}
			violated := topLevelIndex(t.schema, column) < 0 || len(schemaPath) != 1
			for _, row := range rows {
				if violated || checkNotNullRow(t.name, []string{column}, row) != nil {
					violated = true
					break
				}
			}
			if violated {
				LogCatalog.Warn("writable catalog: dropping stored NOT NULL from before vgi.v3; existing rows violate it",
					"catalog", c.Name, "table", t.name, "column", column)
				continue
			}
			record, err := ConstraintRecord(ConstraintRecordSpec{SchemaPath: schemaPath, Table: t.name,
				ID: notNullConstraintID(column), Kind: ConstraintNotNull, Columns: [][]string{{column}},
				Enforcement: ConstraintEnforced, Validation: ConstraintAllRows})
			if err != nil {
				return err
			}
			t.constraints = append(t.constraints, record)
			LogCatalog.Info("writable catalog: stored NOT NULL from before vgi.v3 is now an enforced canonical record",
				"catalog", c.Name, "table", t.name, "column", column)
		}
	}
	t.legacy = nil
	return c.store.tableUpsert(c.Name, schemaKey, t)
}
