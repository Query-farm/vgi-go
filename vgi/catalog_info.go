// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// CatalogInfo is the discovery record returned by catalog_catalogs.
type CatalogInfo struct {
	Name                  string
	ImplementationVersion *string
	DataVersionSpec       *string
}

var catalogInfoSchema = generated.CatalogInfoSchema

// SerializeCatalogInfo serializes a CatalogInfo to IPC bytes.
func SerializeCatalogInfo(info *CatalogInfo) ([]byte, error) {
	mem := memory.NewGoAllocator()

	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release()
	nameBuilder.Append(info.Name)

	implBuilder := array.NewStringBuilder(mem)
	defer implBuilder.Release()
	if info.ImplementationVersion != nil {
		implBuilder.Append(*info.ImplementationVersion)
	} else {
		implBuilder.AppendNull()
	}

	dvsBuilder := array.NewStringBuilder(mem)
	defer dvsBuilder.Release()
	if info.DataVersionSpec != nil {
		dvsBuilder.Append(*info.DataVersionSpec)
	} else {
		dvsBuilder.AppendNull()
	}

	cols := []arrow.Array{
		nameBuilder.NewArray(),
		implBuilder.NewArray(),
		dvsBuilder.NewArray(),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(catalogInfoSchema, cols, 1)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(catalogInfoSchema))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
