// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BinaryPacketFunction builds binary packets with header, payload, and config metadata.
type BinaryPacketFunction struct{}

func (f *BinaryPacketFunction) Name() string { return "binary_packet" }

func (f *BinaryPacketFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Build binary packets with header, payload, and config",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *BinaryPacketFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "header", Position: 0, ArrowType: "blob", Doc: "Header bytes to prepend", IsConst: true},
		{Name: "payload", Position: 1, ArrowType: "blob", Doc: "Binary payload data"},
		{Name: "config", Position: 2, ArrowType: "struct", Doc: "Config {label, version}", IsConst: true},
	}
}

func (f *BinaryPacketFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "result", Type: arrow.BinaryTypes.Binary},
		}, nil),
	}, nil
}

func (f *BinaryPacketFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	// Get header bytes (const param)
	header, err := params.Args.GetScalarBytes(0)
	if err != nil {
		return nil, err
	}

	// Get config struct (const param)
	configCol, err := params.Args.GetColumn(2)
	if err != nil {
		return nil, err
	}
	structArr := configCol.(*array.Struct)
	structType := structArr.DataType().(*arrow.StructType)

	var label string
	var version int64
	for fi := 0; fi < structType.NumFields(); fi++ {
		fieldName := structType.Field(fi).Name
		fieldArr := structArr.Field(fi)
		switch fieldName {
		case "label":
			if s, ok := fieldArr.(*array.String); ok {
				label = s.Value(0)
			}
		case "version":
			switch v := fieldArr.(type) {
			case *array.Int64:
				version = v.Value(0)
			case *array.Int32:
				version = int64(v.Value(0))
			case *array.Int16:
				version = int64(v.Value(0))
			case *array.Int8:
				version = int64(v.Value(0))
			}
		}
	}

	// Build suffix from config: label bytes + version as single byte
	suffix := append([]byte(label), byte(version&0xFF))

	mem := memory.NewGoAllocator()
	payloadCol := batch.Column(0).(*array.Binary)
	n := int(batch.NumRows())

	builder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer builder.Release()

	for i := 0; i < n; i++ {
		if payloadCol.IsNull(i) {
			// Empty payload for nulls
			result := make([]byte, 0, len(header)+len(suffix))
			result = append(result, header...)
			result = append(result, suffix...)
			builder.Append(result)
		} else {
			payload := payloadCol.Value(i)
			result := make([]byte, 0, len(header)+len(payload)+len(suffix))
			result = append(result, header...)
			result = append(result, payload...)
			result = append(result, suffix...)
			builder.Append(result)
		}
	}

	resultArr := builder.NewArray()
	defer resultArr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, int64(n)), nil
}
