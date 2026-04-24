// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// AttachOptionSpec describes an ATTACH-time option the worker accepts.
// Mirrors vgi-python's AttachOptionSpec: wire format is the same as
// SettingSpec so DuckDB can parse both with shared code.
//
// DefaultBatch, when non-nil, must be a single-row RecordBatch whose only
// column (name "value") has type Type. Use this to carry defaults for types
// like list/struct/decimal/date/time/timestamp that are awkward to express
// as Go scalars. Callers that only need primitive defaults can use
// BuildDefaultValueBatch.
type AttachOptionSpec struct {
	Name         string
	Description  string
	Type         arrow.DataType
	DefaultBatch arrow.RecordBatch
}

var attachOptionSpecSchema = arrow.NewSchema([]arrow.Field{
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "description", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "type", Type: arrow.BinaryTypes.Binary, Nullable: false},
	{Name: "default_value", Type: arrow.BinaryTypes.Binary, Nullable: true},
}, nil)

// serializeAttachOptionSpec serializes an AttachOptionSpec to Arrow IPC bytes.
func serializeAttachOptionSpec(spec AttachOptionSpec) ([]byte, error) {
	if spec.Type == nil {
		return nil, fmt.Errorf("attach option %q: Type must not be nil", spec.Name)
	}
	mem := memory.NewGoAllocator()

	typeSchema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: spec.Type}}, nil)
	typeBytes, err := SerializeSchema(typeSchema)
	if err != nil {
		return nil, fmt.Errorf("serializing attach option type: %w", err)
	}

	var defaultBytes []byte
	if spec.DefaultBatch != nil {
		defaultBytes, err = SerializeRecordBatch(spec.DefaultBatch)
		if err != nil {
			return nil, fmt.Errorf("serializing attach option default: %w", err)
		}
	}

	nameB := array.NewStringBuilder(mem)
	defer nameB.Release()
	nameB.Append(spec.Name)

	descB := array.NewStringBuilder(mem)
	defer descB.Release()
	descB.Append(spec.Description)

	typeB := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer typeB.Release()
	typeB.Append(typeBytes)

	defB := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer defB.Release()
	if defaultBytes != nil {
		defB.Append(defaultBytes)
	} else {
		defB.AppendNull()
	}

	cols := []arrow.Array{nameB.NewArray(), descB.NewArray(), typeB.NewArray(), defB.NewArray()}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(attachOptionSpecSchema, cols, 1)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(attachOptionSpecSchema))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
