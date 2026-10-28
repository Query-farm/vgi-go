// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// encodeCatalogRecords shares existing metadata field builders with singleton
// RPCs, without serializing or reopening an IPC stream per metadata object.
func encodeCatalogRecords(schema *arrow.Schema, records []arrow.RecordBatch) ([]byte, error) {
	if len(records) == 0 {
		return nil, nil
	}
	var output bytes.Buffer
	writer := ipc.NewWriter(&output, ipc.WithSchema(schema))
	defer func() { _ = writer.Close() }()
	for offset := 0; offset < len(records); offset += catalogBatchRows {
		chunk := records[offset:min(offset+catalogBatchRows, len(records))]
		columns := make([]arrow.Array, schema.NumFields())
		release := func() {
			for _, c := range columns {
				if c != nil {
					c.Release()
				}
			}
		}
		for i := range columns {
			parts := make([]arrow.Array, len(chunk))
			for j, r := range chunk {
				parts[j] = r.Column(i)
			}
			value, err := array.Concatenate(parts, memory.DefaultAllocator)
			if err != nil {
				release()
				return nil, err
			}
			columns[i] = value
		}
		batch := array.NewRecordBatch(schema, columns, int64(len(chunk)))
		release()
		err := writer.Write(batch)
		batch.Release()
		if err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func (w *Worker) directCatalogContents(attach []byte, version int64, ifNoneMatch *string) (generated.CatalogContentsResponse, error) {
	out := generated.CatalogContentsResponse{CatalogVersion: version}
	infos, err := w.listSchemaInfos(attach)
	if err != nil {
		return out, err
	}
	records := make([][]arrow.RecordBatch, 6)
	defer func() {
		for _, kind := range records {
			for _, r := range kind {
				r.Release()
			}
		}
	}()
	add := func(kind int, record arrow.RecordBatch, err error) error {
		if err == nil {
			records[kind] = append(records[kind], record)
		}
		return err
	}
	catalogName := w.catalogOfAttach(attach)
	for _, info := range infos {
		record, err := buildSchemaInfoRecord(info)
		if err = add(0, record, err); err != nil {
			return out, err
		}
		si := w.catalog.schemas[schemaPathKey(info.Path)]
		if si == nil {
			continue
		}
		enabled := func(kind string) bool { count, known := info.EstimatedObjectCount[kind]; return !known || count != 0 }
		if enabled("table") {
			for i := range si.tables {
				table, err := w.catalogTableInfo(info.Path, &si.tables[i])
				if err != nil {
					return out, err
				}
				record, err := buildTableInfoRecord(table)
				if err = add(1, record, err); err != nil {
					return out, err
				}
			}
		}
		if enabled("view") {
			for _, view := range si.views {
				record, err := buildViewInfoRecord(&ViewInfo{Name: view.Name, SchemaPath: info.Path, Comment: descriptorComment(view.Comment), Tags: view.Tags, Definition: view.Definition, ColumnComments: view.ColumnComments, Aliases: view.Aliases, Security: view.Security})
				if err = add(2, record, err); err != nil {
					return out, err
				}
			}
		}
		for _, kind := range []struct {
			count string
			kind  FunctionType
		}{{"scalar_function", FunctionTypeScalar}, {"aggregate_function", FunctionTypeAggregate}, {"table_function", FunctionTypeTable}} {
			if !enabled(kind.count) {
				continue
			}
			for i := range si.functions {
				function := &si.functions[i]
				if function.unlisted || !listsFunction(kind.kind, function.FunctionType) || (catalogName != "" && catalogName != function.catalogHome) {
					continue
				}
				record, err := buildFunctionInfoRecord(function)
				if err = add(3, record, err); err != nil {
					return out, err
				}
			}
		}
		if enabled("macro") {
			for _, kind := range []MacroType{MacroTypeScalar, MacroTypeTable} {
				for _, macro := range si.macros {
					if canonicalMacroType(macro.MacroType) != kind {
						continue
					}
					value, err := macroInfoFromCatalogMacro(macro, info.Path)
					if err != nil {
						return out, err
					}
					record, err := buildMacroInfoRecord(value)
					if err = add(4, record, err); err != nil {
						return out, err
					}
				}
			}
		}
	}
	fields := []**[]byte{&out.Schemas, &out.Tables, &out.Views, &out.Functions, &out.Macros, &out.Indexes}
	types := []*arrow.Schema{generated.SchemaInfoSchema, generated.TableInfoSchema, generated.ViewInfoSchema, generated.FunctionInfoSchema, generated.MacroInfoSchema, generated.IndexInfoSchema}
	for i := range fields {
		data, err := encodeCatalogRecords(types[i], records[i])
		if err != nil {
			return out, err
		}
		if data != nil {
			*fields[i] = &data
		}
	}
	if _, err = decodeCatalogStreams(out, false); err != nil {
		return out, err
	}
	if w.catalogContentsEtag == CatalogContentsEtagContentHash {
		etag := CatalogStreamsDigest(out)
		out.Etag = &etag
		if ifNoneMatch != nil && *ifNoneMatch == etag {
			return notModifiedResponse(version, etag), nil
		}
	}
	return out, nil
}
