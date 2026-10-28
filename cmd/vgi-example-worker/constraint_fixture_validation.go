// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package main

import (
	"bytes"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/Query-farm/vgi-go/examples/schema_reconcile"
	"github.com/Query-farm/vgi-go/examples/table"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func fixtureDecode(data []byte) arrow.RecordBatch {
	batch, err := vgi.DeserializeRecordBatch(data)
	if err != nil {
		panic(err)
	}
	return batch
}
func fixtureValue(batch arrow.RecordBatch, name string) arrow.Array {
	indices := batch.Schema().FieldIndices(name)
	if len(indices) != 1 {
		panic("missing/ambiguous fixture column: " + name)
	}
	return batch.Column(indices[0])
}
func fixtureEnumValue(batch arrow.RecordBatch, name string) string {
	values := fixtureValue(batch, name).(*array.Dictionary)
	return values.Dictionary().(*array.String).Value(values.GetValueIndex(0))
}
func fixturePathNames(batch arrow.RecordBatch, name string) []string {
	paths := fixtureValue(batch, name).(*array.List)
	start, end := paths.ValueOffsets(0)
	components := paths.ListValues().(*array.List)
	values := components.ListValues().(*array.String)
	var names []string
	for i := start; i < end; i++ {
		a, b := components.ValueOffsets(int(i))
		if b-a != 1 {
			panic("fixture constraint needs top-level key")
		}
		names = append(names, values.Value(int(a)))
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
		info := fixtureDecode(encoded)
		definition := fixtureDecode(fixtureValue(info, "definition").(*array.Binary).Value(0))
		err := validateFixtureDefinition(batch, definition)
		definition.Release()
		info.Release()
		if err != nil {
			return err
		}
	}
	return nil
}
func validateFixtureDefinition(batch, definition arrow.RecordBatch) error {
	names := fixturePathNames(definition, "column_paths")
	kind := fixtureEnumValue(definition, "kind")
	switch kind {
	case "NOT_NULL":
		for _, name := range names {
			if fixtureValue(batch, name).NullN() != 0 {
				return fmt.Errorf("fixture NOT NULL violated: %s", name)
			}
		}
	case "PRIMARY_KEY", "UNIQUE":
		keys := map[string]bool{}
		for row := 0; row < int(batch.NumRows()); row++ {
			key, valid := fixtureKey(batch, names, row)
			if !valid && kind == "PRIMARY_KEY" {
				return fmt.Errorf("fixture PRIMARY KEY contains NULL")
			}
			if valid && keys[key] {
				return fmt.Errorf("fixture %s contains duplicate key", kind)
			}
			if valid {
				keys[key] = true
			}
		}
	case "FOREIGN_KEY":
		foreign := fixtureDecode(fixtureValue(definition, "foreign_key").(*array.Binary).Value(0))
		defer foreign.Release()
		// This fixture qualifies exactly the declared immutable departments key.
		if !bytes.Equal(fixtureValue(definition, "foreign_key").(*array.Binary).Value(0), fixtureDepartmentForeignKey()) {
			return fmt.Errorf("unsupported fixture foreign key")
		}
		target := table.ConstraintFixtureRows("departments", 0)
		defer target.Release()
		if err := validateFixtureRows(target, fixtureDefinitions("departments", 0)); err != nil {
			return err
		}
		targetNames := fixturePathNames(foreign, "referenced_column_paths")
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
	case "CHECK":
		expression := fixtureDecode(fixtureValue(definition, "check_expression").(*array.Binary).Value(0))
		defer expression.Release()
		if fixtureValue(expression, "sql").(*array.String).Value(0) != "budget >= 0" {
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

func fixtureCapabilityRecord(data []byte, schema *arrow.Schema) (arrow.RecordBatch, error) {
	if len(data) == 0 || len(data) > 8*1024*1024 {
		return nil, fmt.Errorf("invalid fixture capability record size")
	}
	source := bytes.NewReader(data)
	reader, err := ipc.NewReader(source)
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	if !reader.Schema().Equal(schema) || !reader.Next() || reader.RecordBatch().NumRows() != 1 {
		return nil, fmt.Errorf("invalid fixture capability record")
	}
	tail := data[len(data)-source.Len():]
	if len(tail) != 0 && !bytes.Equal(tail, []byte{0, 0, 0, 0}) && !bytes.Equal(tail, []byte{255, 255, 255, 255, 0, 0, 0, 0}) {
		return nil, fmt.Errorf("trailing fixture capability data")
	}
	batch := reader.RecordBatch()
	batch.Retain()
	return batch, nil
}

func fixtureAttachValidator(req *vgi.CatalogAttachRequestWire, ctx *vgirpc.CallContext) (*vgi.AttachDecision, error) {
	if req.Name != "example" {
		return schema_reconcile.AttachValidator(req, ctx)
	}
	supported := []string{"vgi.constraint.records.read.v1", "vgi.expression.duckdb.scalar.v1"}
	selected := []string{}
	if req.ClientCapabilities != nil {
		client, err := fixtureCapabilityRecord(*req.ClientCapabilities, generated.ClientCapabilitiesSchema)
		if err != nil {
			return nil, err
		}
		defer client.Release()
		catalog := fixtureValue(client, "catalog").(*array.Binary)
		if !catalog.IsNull(0) {
			offer, err := fixtureCapabilityRecord(catalog.Value(0), generated.CatalogCapabilitiesSchema)
			if err != nil {
				return nil, err
			}
			defer offer.Release()
			pattern := regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*\.v[1-9][0-9]*$`)
			for i, field := range offer.Schema().Fields() {
				list := offer.Column(i).(*array.List)
				if list.IsNull(0) {
					return nil, fmt.Errorf("null fixture capability list")
				}
				start, end := list.ValueOffsets(0)
				if end-start > 256 {
					return nil, fmt.Errorf("too many fixture capability profiles")
				}
				values := list.ListValues().(*array.String)
				seen := map[string]bool{}
				for j := start; j < end; j++ {
					value := values.Value(int(j))
					if values.IsNull(int(j)) || len(value) > 256 || !pattern.MatchString(value) || seen[value] {
						return nil, fmt.Errorf("invalid or duplicate fixture capability profile")
					}
					seen[value] = true
					if field.Name == "read" && slices.Contains(supported, value) {
						selected = append(selected, value)
					}
				}
			}
		}
	}
	if !slices.Contains(selected, "vgi.constraint.records.read.v1") {
		selected = []string{}
	}
	encode := func(read []string) []byte {
		return fixtureRecord(generated.CatalogCapabilitiesSchema,
			map[string]any{"read": read, "ddl": []string{}, "write": []string{}, "transaction": []string{}})
	}
	return &vgi.AttachDecision{CatalogCapabilities: encode(supported), SelectedCapabilities: encode(selected)}, nil
}
