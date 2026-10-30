// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package vgi_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
)

func TestColumnMetadataHTTPCreateAndDiscovery(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	data, err := os.ReadFile("../conformance/catalog-contracts.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Schemas map[string]map[string]string `json:"column_metadata_schemas"`
	}
	if err = json.Unmarshal(data, &corpus); err != nil {
		t.Fatal(err)
	}
	w := vgi.NewWorker(vgi.WithCatalogName("metadata"))
	w.RegisterWritableCatalog(vgi.NewWritableCatalog("mem"))
	c := newContentsClient(t, w)
	attach := c.attach("mem").AttachOpaqueData
	create := func(name string, columns []byte) error {
		request := jsonRow(t, generated.TableCreateRequestSchema, map[string]any{"attach_opaque_data": attach, "schema_path": []string{"main"}, "name": name, "columns": columns, "on_conflict": "ERROR", "constraint_definitions": [][]byte{}})
		defer request.Release()
		payload, err := vgi.SerializeRecordBatch(request)
		if err != nil {
			return err
		}
		params := jsonRow(t, generated.CatalogTableCreateParamsSchema, map[string]any{"request": payload})
		defer params.Release()
		result, err := c.client.CallUnary(context.Background(), "catalog_table_create", params, nil)
		if result != nil {
			result.Release()
		}
		return err
	}
	for group, values := range corpus.Schemas {
		for name, encoded := range values {
			t.Run(group+"/"+name, func(t *testing.T) {
				raw, err := base64.StdEncoding.DecodeString(encoded)
				if err != nil {
					t.Fatal(err)
				}
				if group == "invalid" {
					if err = create(name, raw); err == nil {
						t.Fatal("malformed metadata reached CREATE")
					}
					return
				}
				if err = create(name, raw); err != nil {
					t.Fatal(err)
				}
				items := c.items("catalog_table_get", generated.CatalogTableGetParamsSchema, map[string]any{"attach_opaque_data": attach, "schema_path": []string{"main"}, "name": name})
				if len(items) != 1 {
					t.Fatal("missing created table")
				}
				info, err := vgi.DeserializeRecordBatch(items[0])
				if err != nil {
					t.Fatal(err)
				}
				defer info.Release()
				source, err := vgi.DeserializeSchema(raw)
				if err != nil {
					t.Fatal(err)
				}
				canonical, err := vgi.SerializeSchema(source)
				if err != nil {
					t.Fatal(err)
				}
				got := column[*array.Binary](t, info, "columns").Value(0)
				readback, err := vgi.DeserializeSchema(got)
				if err != nil {
					t.Fatal(err)
				}
				var userFields []arrow.Field
				for _, field := range readback.Fields() {
					if field.Metadata.FindKey("is_row_id") < 0 {
						userFields = append(userFields, field)
					}
				}
				md := readback.Metadata()
				actual, err := vgi.SerializeSchema(arrow.NewSchema(userFields, &md))
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(actual, canonical) {
					t.Fatal("column metadata changed in CREATE/discovery")
				}
			})
		}
	}
	items := c.items("catalog_schema_contents_tables", generated.CatalogSchemaContentsTablesParamsSchema, map[string]any{"attach_opaque_data": attach, "path": []string{"main"}})
	if len(items) != 4 {
		t.Fatalf("denied CREATE changed inventory: %d", len(items))
	}
	// Static providers enforce the same contract on the actual discovery encoder.
	for name, encoded := range corpus.Schemas["invalid"] {
		raw, _ := base64.StdEncoding.DecodeString(encoded)
		schema, err := vgi.DeserializeSchema(raw)
		if err != nil {
			continue
		}
		if _, err = vgi.SerializeTableInfo(&vgi.TableInfo{Name: name, SchemaPath: []string{"main"}, Columns: schema}); err == nil {
			t.Fatalf("discovery accepted %s", name)
		}
	}
}

func TestColumnMetadataRegisteredExtensionRoundTrip(t *testing.T) {
	dataType, err := extensions.NewJSONType(arrow.BinaryTypes.String)
	if err != nil {
		t.Fatal(err)
	}
	metadata := arrow.MetadataFrom(map[string]string{"comment": "", "tags": "{}", "provider.note": "keep"})
	schema := arrow.NewSchema([]arrow.Field{{Name: "json", Type: dataType, Nullable: true, Metadata: metadata}}, nil)
	for i := 0; i < 2; i++ {
		raw, err := vgi.SerializeSchema(schema)
		if err != nil {
			t.Fatal(err)
		}
		schema, err = vgi.DeserializeSchema(raw)
		if err != nil {
			t.Fatal(err)
		}
		if !arrow.TypeEqual(schema.Field(0).Type, dataType) {
			t.Fatal("extension type changed")
		}
		if !schema.Field(0).Metadata.Equal(metadata) {
			t.Fatal("extension serialization keys leaked into field metadata")
		}
		if err = vgi.ValidateColumnMetadata(schema); err != nil {
			t.Fatal(err)
		}
	}
}

func TestColumnMetadataEmptyCommentOverrideDiscovery(t *testing.T) {
	metadata := arrow.MetadataFrom(map[string]string{"comment": "old", "tags": "{}", "provider.note": "keep"})
	schemaMetadata := arrow.MetadataFrom(map[string]string{"provider": "retained"})
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Metadata: metadata}}, &schemaMetadata)
	worker := vgi.NewWorker(vgi.WithCatalogName("metadata"))
	worker.RegisterCatalogTable("main", vgi.CatalogTable{Name: "comments", Columns: schema, ColumnComments: map[string]string{"id": ""}})
	client := newContentsClient(t, worker)
	attach := client.attach("metadata").AttachOpaqueData
	items := client.items("catalog_table_get", generated.CatalogTableGetParamsSchema, map[string]any{"attach_opaque_data": attach, "schema_path": []string{"main"}, "name": "comments"})
	if len(items) != 1 {
		t.Fatal("missing table")
	}
	info, err := vgi.DeserializeRecordBatch(items[0])
	if err != nil {
		t.Fatal(err)
	}
	defer info.Release()
	readback, err := vgi.DeserializeSchema(column[*array.Binary](t, info, "columns").Value(0))
	if err != nil {
		t.Fatal(err)
	}
	expected := arrow.MetadataFrom(map[string]string{"comment": "", "tags": "{}", "provider.note": "keep"})
	if !readback.Field(0).Metadata.Equal(expected) || !readback.Metadata().Equal(schemaMetadata) {
		t.Fatal("comment override changed unrelated metadata")
	}
}
