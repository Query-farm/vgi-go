// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"strings"
)

const catalogBatchRows = 1024

// JSON preserves component boundaries even when names contain delimiters.
func catalogCollectionPathKey(path []string) string {
	parts := make([]string, len(path))
	for i, part := range path {
		parts[i] = strings.ToLower(part)
	}
	key, _ := json.Marshal(parts)
	return string(key)
}

// CombineCatalogItems adapts legacy serialized metadata to one category stream.
// Arrow concatenation unifies dictionaries, including nested dictionary fields;
// concatenating IPC byte strings would lose every item after the first EOS.
func CombineCatalogItems(schema *arrow.Schema, items [][]byte) ([]byte, error) {
	if len(items) == 0 {
		return nil, nil
	}
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	defer func() { _ = writer.Close() }()
	records := []arrow.RecordBatch{}
	pendingRows := int64(0)
	totalRows := int64(0)
	release := func() {
		for _, r := range records {
			r.Release()
		}
		records = nil
		pendingRows = 0
	}
	defer release()
	flush := func() error {
		if len(records) == 0 {
			return nil
		}
		columns := make([]arrow.Array, schema.NumFields())
		defer func() {
			for _, c := range columns {
				if c != nil {
					c.Release()
				}
			}
		}()
		var rows int64
		for _, r := range records {
			rows += r.NumRows()
		}
		for i := range columns {
			parts := make([]arrow.Array, len(records))
			for j, r := range records {
				parts[j] = r.Column(i)
			}
			combined, err := array.Concatenate(parts, memory.DefaultAllocator)
			if err != nil {
				return err
			}
			columns[i] = combined
		}
		batch := array.NewRecordBatch(schema, columns, rows)
		defer batch.Release()
		if err := writer.Write(batch); err != nil {
			return err
		}
		release()
		return nil
	}
	for _, item := range items {
		reader, err := ipc.NewReader(bytes.NewReader(item))
		if err != nil {
			return nil, err
		}
		if !reader.Schema().Equal(schema) {
			reader.Release()
			return nil, fmt.Errorf("catalog collection metadata schema mismatch")
		}
		for reader.Next() {
			r := reader.RecordBatch()
			totalRows += r.NumRows()
			for offset := int64(0); offset < r.NumRows(); {
				end := min(offset+catalogBatchRows-pendingRows, r.NumRows())
				records = append(records, r.NewSlice(offset, end))
				pendingRows += end - offset
				offset = end
				if pendingRows >= catalogBatchRows {
					if err := flush(); err != nil {
						reader.Release()
						return nil, err
					}
				}
			}
		}
		err = reader.Err()
		reader.Release()
		if err != nil {
			return nil, err
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	if totalRows == 0 {
		return nil, nil
	}
	return buf.Bytes(), nil
}

func encodeCatalogStreams(version int64, etag *string, schemas []generated.SchemaContents) (generated.CatalogContentsResponse, error) {
	items := make([][][]byte, 6)
	for _, s := range schemas {
		items[0] = append(items[0], s.Schema)
		items[1] = append(items[1], s.Tables...)
		items[2] = append(items[2], s.Views...)
		items[3] = append(items[3], s.ScalarFunctions...)
		items[3] = append(items[3], s.AggregateFunctions...)
		items[3] = append(items[3], s.TableFunctions...)
		items[4] = append(items[4], s.ScalarMacros...)
		items[4] = append(items[4], s.TableMacros...)
		items[5] = append(items[5], s.Indexes...)
	}
	out := generated.CatalogContentsResponse{CatalogVersion: version, Etag: etag}
	fields := []**[]byte{&out.Schemas, &out.Tables, &out.Views, &out.Functions, &out.Macros, &out.Indexes}
	types := []*arrow.Schema{generated.SchemaInfoSchema, generated.TableInfoSchema, generated.ViewInfoSchema, generated.FunctionInfoSchema, generated.MacroInfoSchema, generated.IndexInfoSchema}
	for i := range fields {
		data, err := CombineCatalogItems(types[i], items[i])
		if err != nil {
			return out, fmt.Errorf("catalog category %d: %w", i, err)
		}
		if data != nil {
			*fields[i] = &data
		}
	}
	_, err := decodeCatalogStreams(out, false)
	return out, err
}

// DecodeCatalogStreams validates and adapts the complete inventory to the
// existing grouped API. It reads all rows/batches and retains overloads.
func DecodeCatalogStreams(response generated.CatalogContentsResponse) ([]generated.SchemaContents, error) {
	return decodeCatalogStreams(response, true)
}

func decodeCatalogStreams(response generated.CatalogContentsResponse, materialize bool) ([]generated.SchemaContents, error) {
	streams := []*[]byte{response.Schemas, response.Tables, response.Views, response.Functions, response.Macros, response.Indexes}
	if response.NotModified {
		if response.Etag == nil {
			return nil, fmt.Errorf("not_modified requires an etag")
		}
		for _, data := range streams {
			if data != nil {
				return nil, fmt.Errorf("not_modified contains metadata")
			}
		}
		return []generated.SchemaContents{}, nil
	}
	types := []*arrow.Schema{generated.SchemaInfoSchema, generated.TableInfoSchema, generated.ViewInfoSchema, generated.FunctionInfoSchema, generated.MacroInfoSchema, generated.IndexInfoSchema}
	out := []generated.SchemaContents{}
	index := map[string]int{}
	for kind, data := range streams {
		if data == nil {
			continue
		}
		reader, err := ipc.NewReader(bytes.NewReader(*data))
		if err != nil {
			return nil, err
		}
		if !reader.Schema().Equal(types[kind]) {
			reader.Release()
			return nil, fmt.Errorf("catalog category %d has incompatible schema", kind)
		}
		for reader.Next() {
			record := reader.RecordBatch()
			for col, field := range record.Schema().Fields() {
				values := record.Column(col)
				invalid := !field.Nullable && values.NullN() != 0
				if dict, ok := values.(*array.Dictionary); ok && !field.Nullable && !invalid {
					for row := 0; row < dict.Len(); row++ {
						index := dict.GetValueIndex(row)
						if index < 0 || index >= dict.Dictionary().Len() || dict.Dictionary().IsNull(index) {
							invalid = true
							break
						}
					}
				}
				if invalid {
					reader.Release()
					return nil, fmt.Errorf("catalog metadata has null required field %s", field.Name)
				}
			}
			for row := int64(0); row < record.NumRows(); row++ {
				pathName := "schema_path"
				if kind == 0 {
					pathName = "path"
				}
				pathCol := record.Column(record.Schema().FieldIndices(pathName)[0]).(*array.List)
				pathValues := pathCol.ListValues().(*array.String)
				start, end := pathCol.ValueOffsets(int(row))
				path := []string{}
				for j := start; j < end; j++ {
					if pathValues.IsNull(int(j)) {
						reader.Release()
						return nil, fmt.Errorf("null catalog schema path component")
					}
					path = append(path, pathValues.Value(int(j)))
				}
				if len(path) == 0 {
					reader.Release()
					return nil, fmt.Errorf("empty catalog schema path")
				}
				key := catalogCollectionPathKey(path)
				if kind == 0 {
					if _, exists := index[key]; exists {
						reader.Release()
						return nil, fmt.Errorf("duplicate catalog schema")
					}
					if len(path) > 1 {
						if _, exists := index[catalogCollectionPathKey(path[:len(path)-1])]; !exists {
							reader.Release()
							return nil, fmt.Errorf("catalog schema missing preceding parent")
						}
					}
					index[key] = len(out)
					out = append(out, generated.SchemaContents{Path: path, Tables: [][]byte{}, Views: [][]byte{}, ScalarFunctions: [][]byte{}, AggregateFunctions: [][]byte{}, TableFunctions: [][]byte{}, ScalarMacros: [][]byte{}, TableMacros: [][]byte{}, Indexes: [][]byte{}})
				}
				pos, exists := index[key]
				if !exists {
					reader.Release()
					return nil, fmt.Errorf("catalog object has unknown schema")
				}
				var item []byte
				if materialize {
					one := record.NewSlice(row, row+1)
					item, err = SerializeRecordBatch(one)
					one.Release()
					if err != nil {
						reader.Release()
						return nil, err
					}
				}
				entry := &out[pos]
				switch kind {
				case 0:
					entry.Schema = item
				case 1:
					entry.Tables = append(entry.Tables, item)
				case 2:
					entry.Views = append(entry.Views, item)
				case 3, 4:
					name := "function_type"
					if kind == 4 {
						name = "macro_type"
					}
					dict := record.Column(record.Schema().FieldIndices(name)[0]).(*array.Dictionary)
					value := strings.ToUpper(dict.Dictionary().(*array.String).Value(dict.GetValueIndex(int(row))))
					switch {
					case kind == 3 && value == "SCALAR":
						entry.ScalarFunctions = append(entry.ScalarFunctions, item)
					case kind == 3 && value == "AGGREGATE":
						entry.AggregateFunctions = append(entry.AggregateFunctions, item)
					case kind == 3 && (value == "TABLE" || value == "TABLE_BUFFERING"):
						entry.TableFunctions = append(entry.TableFunctions, item)
					case kind == 4 && value == "SCALAR":
						entry.ScalarMacros = append(entry.ScalarMacros, item)
					case kind == 4 && value == "TABLE":
						entry.TableMacros = append(entry.TableMacros, item)
					default:
						reader.Release()
						return nil, fmt.Errorf("unknown catalog subtype %q", value)
					}
				case 5:
					entry.Indexes = append(entry.Indexes, item)
				}
			}
		}
		err = reader.Err()
		reader.Release()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
