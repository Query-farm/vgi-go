// Copyright 2026 Query Farm LLC - https://query.farm

package schema_reconcile

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const constraintRead = "vgi.constraint.records.read.v1"
const constraintEnforce = "vgi.constraint.enforce.v1"

// AttachValidator publishes only this fixture's bounded NOT_NULL guarantees.
// Other example catalogs keep their existing absent capability agreement.
func AttachValidator(req *vgi.CatalogAttachRequestWire, _ *vgirpc.CallContext) (*vgi.AttachDecision, error) {
	if req.Name != CatalogName {
		return nil, nil
	}
	selected := map[string]any{"read": []string{}, "ddl": []string{}, "write": []string{}, "transaction": []string{}}
	if req.ClientCapabilities != nil {
		client, err := readRecord(*req.ClientCapabilities, generated.ClientCapabilitiesSchema)
		if err != nil {
			return nil, err
		}
		defer client.Release()
		catalog := client.Column(client.Schema().FieldIndices("catalog")[0]).(*array.Binary)
		if !catalog.IsNull(0) {
			offer, err := readRecord(catalog.Value(0), generated.CatalogCapabilitiesSchema)
			if err != nil {
				return nil, err
			}
			defer offer.Release()
			names := map[string][]string{}
			profileName := regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*\.v[1-9][0-9]*$`)
			for i, field := range offer.Schema().Fields() {
				list := offer.Column(i).(*array.List)
				if list.IsNull(0) {
					return nil, fmt.Errorf("schema_reconcile: null capability list %s", field.Name)
				}
				start, end := list.ValueOffsets(0)
				values := list.ListValues().(*array.String)
				for j := start; j < end; j++ {
					value := values.Value(int(j))
					if values.IsNull(int(j)) || !profileName.MatchString(value) || slices.Contains(names[field.Name], value) {
						return nil, fmt.Errorf("schema_reconcile: invalid capability offer in %s", field.Name)
					}
					names[field.Name] = append(names[field.Name], value)
				}
			}
			if slices.Contains(names["read"], constraintRead) {
				selected["read"] = []string{constraintRead}
				if slices.Contains(names["write"], constraintEnforce) {
					selected["write"] = []string{constraintEnforce}
				}
			}
		}
	}
	// An existing database may predate enforcement. Do not claim ALL_ROWS
	// until its actual stored arrays have been checked. Every write and scan
	// also validates rows; external SQLite mutation is not a supported writer.
	for _, spec := range tableSpecs {
		if err := validateStoredRows(spec); err != nil {
			return nil, err
		}
	}
	available, err := encodeRecord(generated.CatalogCapabilitiesSchema, map[string]any{
		"read": []string{constraintRead}, "ddl": []string{},
		"write": []string{constraintEnforce}, "transaction": []string{},
	})
	if err != nil {
		return nil, err
	}
	agreement, err := encodeRecord(generated.CatalogCapabilitiesSchema, selected)
	if err != nil {
		return nil, err
	}
	return &vgi.AttachDecision{CatalogCapabilities: available, SelectedCapabilities: agreement}, nil
}

func encodeRecord(schema *arrow.Schema, values map[string]any) ([]byte, error) {
	data, err := json.Marshal([]map[string]any{values})
	if err != nil {
		return nil, err
	}
	batch, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer batch.Release()
	return vgi.SerializeRecordBatch(batch)
}

func readRecord(data []byte, schema *arrow.Schema) (arrow.RecordBatch, error) {
	source := bytes.NewReader(data)
	reader, err := ipc.NewReader(source)
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	if !reader.Schema().Equal(schema) || !reader.Next() || reader.RecordBatch().NumRows() != 1 {
		return nil, fmt.Errorf("schema_reconcile: expected one canonical capability record")
	}
	batch := reader.RecordBatch()
	batch.Retain()
	remaining := data[len(data)-source.Len():]
	if len(remaining) != 0 && !bytes.Equal(remaining, []byte{0, 0, 0, 0}) && !bytes.Equal(remaining, []byte{255, 255, 255, 255, 0, 0, 0, 0}) {
		batch.Release()
		return nil, fmt.Errorf("schema_reconcile: malformed capability IPC stream")
	}
	if reader.Next() || reader.Err() != nil || source.Len() != 0 {
		batch.Release()
		return nil, fmt.Errorf("schema_reconcile: malformed capability IPC stream")
	}
	return batch, nil
}

func notNullConstraints(spec tableSpec) ([][]byte, error) {
	var constraints [][]byte
	var visit func(arrow.Field, []string) error
	visit = func(field arrow.Field, parent []string) error {
		path := append(slices.Clone(parent), field.Name)
		if !field.Nullable {
			// JSON encodes raw components unambiguously, including literal dots.
			identity, err := json.Marshal(path)
			if err != nil {
				return err
			}
			ref, err := encodeRecord(generated.CatalogObjectRefSchema, map[string]any{
				"kind": "constraint", "schema_path": []string{SchemaPath},
				"name": "schema_reconcile:" + spec.name + ":not_null:" + string(identity), "owner_table": spec.name,
			})
			if err != nil {
				return err
			}
			definition, err := encodeRecord(generated.ConstraintDefinitionSchema, map[string]any{
				"kind": "NOT_NULL", "column_paths": [][]string{path}, "declared_name": nil,
				"check_expression": nil, "foreign_key": nil,
			})
			if err != nil {
				return err
			}
			info, err := encodeRecord(generated.ConstraintInfoSchema, map[string]any{
				"ref": ref, "definition": definition, "enforcement": "ENFORCED", "validation": "ALL_ROWS",
				"referenced_constraint": nil,
			})
			if err != nil {
				return err
			}
			constraints = append(constraints, info)
		}
		// List items remain physical Arrow required-value rules, not SQL
		// column paths. The rowid pseudocolumn is excluded by the caller.
		if nested, ok := field.Type.(*arrow.StructType); ok {
			for _, child := range nested.Fields() {
				if err := visit(child, path); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for _, field := range userFields {
		if err := visit(field, nil); err != nil {
			return nil, err
		}
	}
	return constraints, nil
}

// validateRows checks the actual values, rather than trusting Arrow's nullable
// flags. Partial update batches may omit columns but cannot change their shape.
// A null nullable parent masks its required children, including list elements.
func validateRows(batch arrow.RecordBatch, expected *arrow.Schema, partial bool) error {
	if !partial && !batch.Schema().Equal(expected) {
		return fmt.Errorf("schema_reconcile: input schema differs from declared schema")
	}
	seen := map[string]bool{}
	for i, field := range batch.Schema().Fields() {
		indices := expected.FieldIndices(field.Name)
		if len(indices) != 1 || seen[field.Name] || !field.Equal(expected.Field(indices[0])) {
			return fmt.Errorf("schema_reconcile: unexpected input field %q", field.Name)
		}
		seen[field.Name] = true
		for row := 0; row < int(batch.NumRows()); row++ {
			if err := validateValue(field, batch.Column(i), row); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateValue(field arrow.Field, values arrow.Array, row int) error {
	if values.IsNull(row) {
		if !field.Nullable {
			return fmt.Errorf("schema_reconcile: NULL supplied for non-nullable field '%s'", field.Name)
		}
		return nil
	}
	switch values := values.(type) {
	case *array.Struct:
		for i, child := range field.Type.(*arrow.StructType).Fields() {
			if err := validateValue(child, values.Field(i), row); err != nil {
				return err
			}
		}
	case *array.List:
		start, end := values.ValueOffsets(row)
		child := field.Type.(*arrow.ListType).ElemField()
		for i := start; i < end; i++ {
			if err := validateValue(child, values.ListValues(), int(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateStoredRows(spec tableSpec) error {
	storageMu.Lock()
	defer storageMu.Unlock()
	db, err := openDB()
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.Query("SELECT payload_blob FROM " + spec.storageName)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return err
		}
		batch, err := deserializeBatch(payload)
		if err != nil {
			return err
		}
		err = validateRows(batch, spec.tableSchema(), false)
		batch.Release()
		if err != nil {
			return err
		}
	}
	return rows.Err()
}
