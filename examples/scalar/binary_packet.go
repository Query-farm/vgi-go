// Copyright 2025, 2026 Query Farm LLC - https://query.farm

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
		ReturnType:  arrow.BinaryTypes.Binary,
	}
}

func (f *BinaryPacketFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "header", Position: 0, ArrowType: "blob", Doc: "Header bytes to prepend", IsConst: true},
		{Name: "payload", Position: 1, ArrowType: "blob", Doc: "Binary payload data"},
		{Name: "config", Position: 2, ArrowType: "struct", Doc: "Config {label, version}", IsConst: true,
			ArrowDataType: arrow.StructOf(
				arrow.Field{Name: "label", Type: arrow.BinaryTypes.String},
				arrow.Field{Name: "version", Type: arrow.PrimitiveTypes.Int64},
			)},
	}
}

func (f *BinaryPacketFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.Binary)
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
	structArr, err := vgi.MustTyped[*array.Struct](configCol)
	if err != nil {
		return nil, err
	}
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
			version = vgi.GetInt64Value(fieldArr, 0)
		}
	}

	// Build suffix from config: label bytes + version as single byte
	suffix := append([]byte(label), byte(version&0xFF))

	return vgi.MapColumnCustomNulls(params, batch, 0,
		func(mem memory.Allocator) *array.BinaryBuilder {
			return array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		},
		func(col arrow.Array, i int) []byte {
			if col.IsNull(i) {
				result := make([]byte, 0, len(header)+len(suffix))
				result = append(result, header...)
				result = append(result, suffix...)
				return result
			}
			payload := col.(*array.Binary).Value(i)
			result := make([]byte, 0, len(header)+len(payload)+len(suffix))
			result = append(result, header...)
			result = append(result, payload...)
			result = append(result, suffix...)
			return result
		})
}
