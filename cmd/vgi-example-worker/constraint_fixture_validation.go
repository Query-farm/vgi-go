// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package main

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/Query-farm/vgi-go/examples/table"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func fixtureValue(batch arrow.RecordBatch, name string) arrow.Array {
	indices := batch.Schema().FieldIndices(name)
	if len(indices) != 1 {
		panic("missing/ambiguous fixture column: " + name)
	}
	return batch.Column(indices[0])
}
func fixturePathNames(paths [][]string) []string {
	var names []string
	for _, path := range paths {
		if len(path) != 1 {
			panic("fixture constraint needs top-level key")
		}
		names = append(names, path[0])
	}
	return names
}
func fixtureKey(batch arrow.RecordBatch, names []string, row int) (string, bool) {
	var key strings.Builder
	for _, name := range names {
		column := fixtureValue(batch, name)
		if column.IsNull(row) {
			return "", false
		}
		switch values := column.(type) {
		case *array.Int64:
			fmt.Fprintf(&key, "i%d;", values.Value(row))
		case *array.String:
			value := values.Value(row)
			fmt.Fprintf(&key, "s%d:%s", len(value), value)
		default:
			panic("unsupported fixture key type")
		}
	}
	return key.String(), true
}

func validateFixtureRows(batch arrow.RecordBatch, definitions [][]byte) error {
	for _, encoded := range definitions {
		record, err := vgi.ReadConstraintRecord(encoded)
		if err != nil {
			return err
		}
		if err := validateFixtureDefinition(batch, record); err != nil {
			return err
		}
	}
	return nil
}
func validateFixtureDefinition(batch arrow.RecordBatch, record vgi.ConstraintRecordSpec) error {
	names := fixturePathNames(record.Columns)
	kind := string(record.Kind)
	switch record.Kind {
	case vgi.ConstraintNotNull:
		for _, name := range names {
			if fixtureValue(batch, name).NullN() != 0 {
				return fmt.Errorf("fixture NOT NULL violated: %s", name)
			}
		}
	case vgi.ConstraintPrimaryKey, vgi.ConstraintUnique:
		keys := map[string]bool{}
		for row := 0; row < int(batch.NumRows()); row++ {
			key, valid := fixtureKey(batch, names, row)
			if !valid && record.Kind == vgi.ConstraintPrimaryKey {
				return fmt.Errorf("fixture PRIMARY KEY contains NULL")
			}
			if valid && keys[key] {
				return fmt.Errorf("fixture %s contains duplicate key", kind)
			}
			if valid {
				keys[key] = true
			}
		}
	case vgi.ConstraintForeignKey:
		// This fixture qualifies exactly the declared immutable departments key.
		if !reflect.DeepEqual(*record.References, fixtureDepartmentKey) {
			return fmt.Errorf("unsupported fixture foreign key")
		}
		target := table.ConstraintFixtureRows("departments", 0)
		defer target.Release()
		if err := validateFixtureRows(target, fixtureDefinitions("departments", 0)); err != nil {
			return err
		}
		targetNames := fixturePathNames(record.References.Columns)
		keys := map[string]bool{}
		for row := 0; row < int(target.NumRows()); row++ {
			key, _ := fixtureKey(target, targetNames, row)
			keys[key] = true
		}
		for row := 0; row < int(batch.NumRows()); row++ {
			key, valid := fixtureKey(batch, names, row)
			if valid && !keys[key] {
				return fmt.Errorf("fixture FOREIGN KEY missing referenced row")
			}
		}
	case vgi.ConstraintCheck:
		if record.CheckSQL != "budget >= 0" {
			return fmt.Errorf("unsupported fixture CHECK expression")
		}
		budget := fixtureValue(batch, "budget").(*array.Float64)
		for row := 0; row < int(batch.NumRows()); row++ {
			if !budget.IsNull(row) && !(budget.Value(row) >= 0) {
				return fmt.Errorf("fixture CHECK budget >= 0 violated")
			}
		}
	default:
		return fmt.Errorf("unsupported fixture constraint kind: %s", kind)
	}
	return nil
}
