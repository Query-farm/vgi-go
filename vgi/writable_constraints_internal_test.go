// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// A writable table stored with pre-vgi.v3 constraint metadata used to fill the
// legacy TableInfo fields, which SerializeTableInfo rejects, so listing the
// table failed. Those facts were never enforced by this catalog; the table is
// now served without them, carrying only canonical records.
func TestWritableTableWithLegacyConstraintMetaLists(t *testing.T) {
	var legacy bytes.Buffer
	if err := gob.NewEncoder(&legacy).Encode(writableTableMeta{
		NotNull: []string{"id"}, PrimaryKey: [][]string{{"id"}}, Unique: [][]string{{"name"}},
		Check: []string{"id > 0"}, ForeignKey: []ForeignKeyConstraint{{Columns: []string{"id"}, ReferencedTable: "u", ReferencedColumns: []string{"id"}}},
	}); err != nil {
		t.Fatal(err)
	}
	meta, err := decodeTableMeta(legacy.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	info, err := tableInfoFromWritable(writableTableFromMeta("legacy", schema, "", meta), SchemaPath{"main"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SerializeTableInfo(info); err != nil {
		t.Fatalf("listing a writable table with stored constraints failed: %v", err)
	}
	if len(info.Constraints) != 0 {
		t.Fatalf("legacy facts were translated into %d records", len(info.Constraints))
	}
}

func TestWritableTableCarriesCanonicalRecords(t *testing.T) {
	record, err := ConstraintRecord(ConstraintRecordSpec{SchemaPath: []string{"main"}, Table: "t", ID: "t:nn:id",
		Kind: ConstraintNotNull, Columns: [][]string{{"id"}}, Enforcement: ConstraintEnforced, Validation: ConstraintAllRows})
	if err != nil {
		t.Fatal(err)
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	// Through the persisted meta, as another worker process would load it.
	meta, err := decodeTableMeta(encodeTableMeta(&writableTable{name: "t", schema: schema, constraints: [][]byte{record}}))
	if err != nil {
		t.Fatal(err)
	}
	info, err := tableInfoFromWritable(writableTableFromMeta("t", schema, "", meta), SchemaPath{"main"})
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Constraints) != 1 || !bytes.Equal(info.Constraints[0], record) {
		t.Fatalf("canonical records were not carried: %d", len(info.Constraints))
	}
	// The synthesized __row_id column is exempt from the nullability rule.
	if _, err := SerializeTableInfo(info); err != nil {
		t.Fatal(err)
	}
}
