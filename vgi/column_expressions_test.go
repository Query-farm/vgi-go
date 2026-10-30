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
)

func TestGeneratedColumnDiscoveryAndUnsupportedCreate(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	data, err := os.ReadFile("../conformance/catalog-contracts.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Schemas struct {
			Owner          []string `json:"owner_schema_path"`
			Valid, Invalid map[string]string
		} `json:"generated_column_schemas"`
	}
	if err = json.Unmarshal(data, &corpus); err != nil {
		t.Fatal(err)
	}
	worker := vgi.NewWorker(vgi.WithCatalogName("generated"))
	worker.RegisterWritableCatalog(vgi.NewWritableCatalog("mem"))
	client := newContentsClient(t, worker)
	attach := client.attach("mem").AttachOpaqueData
	for group, values := range map[string]map[string]string{"valid": corpus.Schemas.Valid, "invalid": corpus.Schemas.Invalid} {
		for name, encoded := range values {
			t.Run(group+"/"+name, func(t *testing.T) {
				raw, err := base64.StdEncoding.DecodeString(encoded)
				if err != nil {
					t.Fatal(err)
				}
				schema, err := vgi.DeserializeSchema(raw)
				if err != nil {
					t.Fatal(err)
				}
				info, discoveryError := vgi.SerializeTableInfo(&vgi.TableInfo{Name: name, SchemaPath: corpus.Schemas.Owner, Columns: schema})
				if group == "invalid" {
					if discoveryError == nil {
						t.Fatal("invalid generated metadata published")
					}
				} else {
					if discoveryError != nil {
						t.Fatal(discoveryError)
					}
					record, err := vgi.DeserializeRecordBatch(info)
					if err != nil {
						t.Fatal(err)
					}
					defer record.Release()
					canonical, err := vgi.SerializeSchema(schema)
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(column[*array.Binary](t, record, "columns").Value(0), canonical) {
						t.Fatal("expression IPC/context changed at discovery")
					}
				}
				request := jsonRow(t, generated.TableCreateRequestSchema, map[string]any{"attach_opaque_data": attach, "schema_path": corpus.Schemas.Owner, "name": name, "columns": raw, "on_conflict": "ERROR", "constraint_definitions": [][]byte{}})
				defer request.Release()
				payload, err := vgi.SerializeRecordBatch(request)
				if err != nil {
					t.Fatal(err)
				}
				params := jsonRow(t, generated.CatalogTableCreateParamsSchema, map[string]any{"request": payload})
				defer params.Release()
				result, err := client.client.CallUnary(context.Background(), "catalog_table_create", params, nil)
				if result != nil {
					result.Release()
				}
				if group == "valid" && name == "ordinary" {
					if err != nil {
						t.Fatal(err)
					}
				} else if err == nil {
					t.Fatal("unsupported or malformed generated CREATE reached storage")
				}
			})
		}
	}
	if items := client.items("catalog_schema_contents_tables", generated.CatalogSchemaContentsTablesParamsSchema, map[string]any{"attach_opaque_data": attach, "path": corpus.Schemas.Owner}); len(items) != 1 {
		t.Fatal("denied CREATE changed inventory")
	}
}

func TestGeneratedConvenienceUsesCanonicalOwnerAndPreservesMetadata(t *testing.T) {
	md := arrow.MetadataFrom(map[string]string{"comment": "", "tags": "{}", "provider.note": "keep"})
	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}, {Name: "doubled", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: md}}, &md)
	worker := vgi.NewWorker(vgi.WithCatalogName("generated"))
	worker.RegisterCatalogTable("raw.schema", vgi.CatalogTable{Name: "t", Columns: schema, Generated: map[string]string{"doubled": "n * 2"}})
	client := newContentsClient(t, worker)
	attach := client.attach("generated").AttachOpaqueData
	items := client.items("catalog_table_get", generated.CatalogTableGetParamsSchema, map[string]any{"attach_opaque_data": attach, "schema_path": []string{"raw.schema"}, "name": "t"})
	record, err := vgi.DeserializeRecordBatch(items[0])
	if err != nil {
		t.Fatal(err)
	}
	defer record.Release()
	output, err := vgi.DeserializeSchema(column[*array.Binary](t, record, "columns").Value(0))
	if err != nil {
		t.Fatal(err)
	}
	if err = vgi.ValidateColumnExpressions(output, []string{"raw.schema"}, true); err != nil {
		t.Fatal(err)
	}
	values := output.Field(1).Metadata.ToMap()
	if values["comment"] != "" || values["tags"] != "{}" || values["provider.note"] != "keep" || !output.Metadata().Equal(md) {
		t.Fatal("unrelated metadata changed")
	}
	if _, exists := values["generated_expression"]; exists {
		t.Fatal("legacy generated key emitted")
	}
}
