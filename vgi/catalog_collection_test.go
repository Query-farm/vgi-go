// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package vgi

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestCatalogCollectionAllBatchesAndDictionaryChanges(t *testing.T) {
	const count = 2300
	records := make([]arrow.RecordBatch, 0, count)
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()
	for i := range count {
		kind := FunctionTypeScalar
		args := arrow.NewSchema([]arrow.Field{{Name: "before", Type: arrow.BinaryTypes.Binary}}, nil)
		if i >= 1024 {
			kind = FunctionTypeTable
			args = arrow.NewSchema([]arrow.Field{{Name: "after", Type: arrow.PrimitiveTypes.Int64}}, nil)
		}
		r, err := buildFunctionInfoRecord(&FunctionInfo{Name: fmt.Sprintf("f%d", i), SchemaPath: []string{"main"}, FunctionType: kind,
			ArgSchema: args, OutputSchema: arrow.NewSchema(nil, nil), Categories: []string{fmt.Sprintf("category-%d", i)}})
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, r)
	}
	data, err := encodeCatalogRecords(generated.FunctionInfoSchema, records)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	rows, batches := 0, 0
	for reader.Next() {
		batch := reader.RecordBatch()
		batches++
		if batch.NumRows() > catalogBatchRows {
			t.Fatal("collection batch exceeded row bound")
		}
		names := batch.Column(batch.Schema().FieldIndices("name")[0]).(*array.String)
		arguments := batch.Column(batch.Schema().FieldIndices("arguments")[0]).(*array.Dictionary)
		if arguments.Dictionary().Len() != 1 {
			t.Fatal("equal schema bytes were not deduplicated")
		}
		for i := range int(batch.NumRows()) {
			if names.Value(i) != fmt.Sprintf("f%d", rows) {
				t.Fatal("collection dropped or reordered rows")
			}
			original := records[rows].Column(batch.Schema().FieldIndices("arguments")[0]).(*array.Dictionary)
			actualBytes := arguments.Dictionary().(*array.Binary).Value(arguments.GetValueIndex(i))
			originalBytes := original.Dictionary().(*array.Binary).Value(original.GetValueIndex(0))
			if !bytes.Equal(actualBytes, originalBytes) {
				t.Fatal("replacement dictionary changed schema bytes")
			}
			rows++
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatal(err)
	}
	if rows != count || batches != 3 {
		t.Fatalf("rows=%d batches=%d", rows, batches)
	}
	schema, err := SerializeSchemaInfo(&SchemaInfo{Path: []string{"main"}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := encodeCatalogStreams(1, nil, []generated.SchemaContents{{Path: []string{"main"}, Schema: schema, ScalarFunctions: [][]byte{data}}})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCatalogStreams(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 1 || len(decoded[0].ScalarFunctions) != 1024 || len(decoded[0].TableFunctions) != count-1024 {
		t.Fatal("legacy adapter lost rows or function subtypes")
	}
}

func TestFunctionInfoBinaryDictionaries(t *testing.T) {
	args := arrow.NewSchema([]arrow.Field{{Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true}}, nil)
	defaults := array.NewRecordBuilder(memory.DefaultAllocator, args)
	defer defaults.Release()
	defaults.Field(0).(*array.BinaryBuilder).Append([]byte{255, 0, 128})
	defaultRecord := defaults.NewRecordBatch()
	defer defaultRecord.Release()
	expectedArgs, err := SerializeSchema(args)
	if err != nil {
		t.Fatal(err)
	}
	expectedDefaults, err := SerializeRecordBatch(defaultRecord)
	if err != nil {
		t.Fatal(err)
	}
	for _, withDefaults := range []bool{false, true} {
		info := &FunctionInfo{Name: "binary", FunctionType: FunctionTypeScalar, ArgSchema: args}
		if withDefaults {
			info.ParameterDefaultValues = defaultRecord
		}
		data, err := SerializeFunctionInfo(info)
		if err != nil {
			t.Fatal(err)
		}
		reader, err := ipc.NewReader(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Release()
		if !reader.Next() {
			t.Fatal("missing function info")
		}
		batch := reader.RecordBatch()
		for name, expected := range map[string][]byte{"arguments": expectedArgs, "output_schema": {}, "parameter_default_values": expectedDefaults} {
			col := batch.Column(batch.Schema().FieldIndices(name)[0]).(*array.Dictionary)
			dt := col.DataType().(*arrow.DictionaryType)
			if dt.IndexType.ID() != arrow.INT32 || dt.ValueType.ID() != arrow.BINARY {
				t.Fatal("incorrect dictionary type")
			}
			if name == "parameter_default_values" && !withDefaults {
				if !col.IsNull(0) {
					t.Fatal("missing defaults must remain null")
				}
				continue
			}
			if col.IsNull(0) || !bytes.Equal(col.Dictionary().(*array.Binary).Value(col.GetValueIndex(0)), expected) {
				t.Fatalf("%s changed bytes or confused empty with null", name)
			}
		}
	}
}

func TestFunctionInfoDictionaryNullAndEmptyValidation(t *testing.T) {
	base, err := buildFunctionInfoRecord(&FunctionInfo{Name: "bytes", FunctionType: FunctionTypeScalar})
	if err != nil {
		t.Fatal(err)
	}
	defer base.Release()
	schemaInfo, err := SerializeSchemaInfo(&SchemaInfo{Path: []string{"main"}})
	if err != nil {
		t.Fatal(err)
	}
	index := generated.FunctionInfoSchema.FieldIndices("arguments")[0]
	dt := generated.FunctionInfoSchema.Field(index).Type.(*arrow.DictionaryType)
	values := [][]byte{{255, 0, 128}, {255, 0, 128}, {}, {254, 0, 128}}
	records := []arrow.RecordBatch{}
	defer func() {
		for _, record := range records {
			record.Release()
		}
	}()
	for _, value := range values {
		builder := array.NewDictionaryBuilder(memory.DefaultAllocator, dt).(*array.BinaryDictionaryBuilder)
		if err := builder.Append(bytes.Clone(value)); err != nil {
			t.Fatal(err)
		}
		column := builder.NewArray()
		builder.Release()
		columns := append([]arrow.Array(nil), base.Columns()...)
		columns[index] = column
		records = append(records, array.NewRecordBatch(generated.FunctionInfoSchema, columns, 1))
		column.Release()
	}
	data, err := encodeCatalogRecords(generated.FunctionInfoSchema, records)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("missing function records")
	}
	dictionary := reader.RecordBatch().Column(index).(*array.Dictionary)
	if dictionary.Dictionary().Len() != 3 {
		t.Fatal("binary values were not deduplicated by content")
	}
	for row, expected := range values {
		if dictionary.IsNull(row) || !bytes.Equal(dictionary.Dictionary().(*array.Binary).Value(dictionary.GetValueIndex(row)), expected) {
			t.Fatal("binary value changed or empty became null")
		}
	}

	// A valid index can refer to a null dictionary entry. Required fields must
	// reject this too, even though the index column itself has no nulls.
	indices := array.NewInt32Builder(memory.DefaultAllocator)
	defer indices.Release()
	indices.Append(0)
	i := indices.NewArray()
	defer i.Release()
	binary := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer binary.Release()
	binary.AppendNull()
	b := binary.NewArray()
	defer b.Release()
	nullDictionary := array.NewDictionaryArray(dt, i, b)
	defer nullDictionary.Release()
	columns := append([]arrow.Array(nil), base.Columns()...)
	columns[index] = nullDictionary
	badRecord := array.NewRecordBatch(generated.FunctionInfoSchema, columns, 1)
	defer badRecord.Release()
	badBytes, err := SerializeRecordBatch(badRecord)
	if err != nil {
		t.Fatal(err)
	}
	response, err := encodeCatalogStreams(1, nil, []generated.SchemaContents{{Path: []string{"main"}, Schema: schemaInfo, ScalarFunctions: [][]byte{badBytes}}})
	if err == nil {
		_, err = DecodeCatalogStreams(response)
	}
	if err == nil {
		t.Fatal("null dictionary value accepted in required arguments")
	}
}

func TestCatalogCollectionIndexesAcrossSchemas(t *testing.T) {
	var inventory []generated.SchemaContents
	for _, path := range []string{"left", "right"} {
		info, err := SerializeSchemaInfo(&SchemaInfo{Path: []string{path}})
		if err != nil {
			t.Fatal(err)
		}
		builder := array.NewRecordBuilder(memory.DefaultAllocator, generated.IndexInfoSchema)
		builder.Field(0).(*array.StringBuilder).AppendNull()
		builder.Field(1).(*array.MapBuilder).Append(true)
		builder.Field(2).(*array.StringBuilder).Append("idx_" + path)
		builder.Field(3).(*array.ListBuilder).Append(true)
		builder.Field(3).(*array.ListBuilder).ValueBuilder().(*array.StringBuilder).Append(path)
		builder.Field(4).(*array.StringBuilder).Append("items")
		builder.Field(5).(*array.StringBuilder).Append("ART")
		if err := builder.Field(6).(*array.BinaryDictionaryBuilder).AppendString("NONE"); err != nil {
			t.Fatal(err)
		}
		builder.Field(7).(*array.ListBuilder).Append(true)
		builder.Field(7).(*array.ListBuilder).ValueBuilder().(*array.StringBuilder).Append("id")
		builder.Field(8).(*array.MapBuilder).Append(true)
		record := builder.NewRecordBatch()
		data, err := SerializeRecordBatch(record)
		record.Release()
		builder.Release()
		if err != nil {
			t.Fatal(err)
		}
		inventory = append(inventory, generated.SchemaContents{Path: []string{path}, Schema: info, Indexes: [][]byte{data}})
	}
	wire, err := encodeCatalogStreams(1, nil, inventory)
	if err != nil {
		t.Fatal(err)
	}
	if wire.Indexes == nil {
		t.Fatal("index stream omitted")
	}
	decoded, err := DecodeCatalogStreams(wire)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 2 {
		t.Fatal("schema collection incomplete")
	}
	for i, schema := range decoded {
		if len(schema.Indexes) != 1 || schema.Path[0] != inventory[i].Path[0] {
			t.Fatal("index assigned to the wrong schema")
		}
		reader, err := ipc.NewReader(bytes.NewReader(schema.Indexes[0]))
		if err != nil {
			t.Fatal(err)
		}
		if !reader.Next() {
			reader.Release()
			t.Fatal("index row missing")
		}
		name := reader.RecordBatch().Column(2).(*array.String).Value(0)
		reader.Release()
		if name != "idx_"+schema.Path[0] {
			t.Fatal("index fields changed")
		}
	}
}
