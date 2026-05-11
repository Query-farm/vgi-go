// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var rowIDMetadata = arrow.NewMetadata([]string{"is_row_id"}, []string{""})

var rowIDStructType = arrow.StructOf(
	arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	arrow.Field{Name: "b", Type: arrow.BinaryTypes.String},
)

// RowIdSequenceFunction generates a sequence with a row_id virtual column.
type RowIdSequenceFunction struct{}

var _ vgi.TypedTableFunc[rowIDState] = (*RowIdSequenceFunction)(nil)

func (f *RowIdSequenceFunction) Name() string { return "rowid_sequence" }

func (f *RowIdSequenceFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Sequence with row_id column",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
	}
}

func (f *RowIdSequenceFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of rows to generate", IsConst: true},
		{Name: "layout", Position: -1, ArrowType: "varchar", Doc: "Row ID column position: first, middle, last", HasDefault: true, DefaultValue: "first", IsConst: true},
		{Name: "row_id_type", Position: -1, ArrowType: "varchar", Doc: "Row ID type: int64, string, struct", HasDefault: true, DefaultValue: "int64", IsConst: true},
	}
}

func rowIDField(ridType string) arrow.Field {
	var dt arrow.DataType
	switch ridType {
	case "string":
		dt = arrow.BinaryTypes.String
	case "struct":
		dt = rowIDStructType
	default:
		dt = arrow.PrimitiveTypes.Int64
	}
	return arrow.Field{Name: "row_id", Type: dt, Metadata: rowIDMetadata}
}

func buildRowIDSchema(layout, ridType string) *arrow.Schema {
	rid := rowIDField(ridType)
	name := arrow.Field{Name: "name", Type: arrow.BinaryTypes.String}
	value := arrow.Field{Name: "value", Type: arrow.BinaryTypes.String}

	var fields []arrow.Field
	switch layout {
	case "middle":
		fields = []arrow.Field{name, rid, value}
	case "last":
		fields = []arrow.Field{name, value, rid}
	default: // "first"
		fields = []arrow.Field{rid, name, value}
	}
	return arrow.NewSchema(fields, nil)
}

func (f *RowIdSequenceFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	layout := "first"
	if v, err := params.Args.GetScalarString("layout"); err == nil && v != "" {
		layout = v
	}
	if layout != "first" && layout != "middle" && layout != "last" {
		return nil, fmt.Errorf("layout must be one of the allowed choices: first, middle, last (got %q)", layout)
	}
	ridType := "int64"
	if v, err := params.Args.GetScalarString("row_id_type"); err == nil && v != "" {
		ridType = v
	}
	if ridType != "int64" && ridType != "string" && ridType != "struct" {
		return nil, fmt.Errorf("row_id_type must be one of the allowed choices: int64, string, struct (got %q)", ridType)
	}
	schema := buildRowIDSchema(layout, ridType)
	return &vgi.BindResponse{OutputSchema: schema}, nil
}

type rowIDState struct {
	vgi.BatchState
	Layout    string
	RowIDType string
}

func (f *RowIdSequenceFunction) NewState(params *vgi.ProcessParams) (*rowIDState, error) {
	count, _ := params.Args.GetScalarInt64(0)
	if count < 0 {
		count = 0
	}
	layout := "first"
	if v, err := params.Args.GetScalarString("layout"); err == nil && v != "" {
		layout = v
	}
	ridType := "int64"
	if v, err := params.Args.GetScalarString("row_id_type"); err == nil && v != "" {
		ridType = v
	}
	return &rowIDState{
		BatchState: vgi.NewBatchState(count, 1024),
		Layout:     layout,
		RowIDType:  ridType,
	}, nil
}

func buildRowIDArray(mem memory.Allocator, ridType string, offset, size int64) arrow.Array {
	switch ridType {
	case "string":
		return vgi.BuildStringArray(size, func(i int64) string {
			return fmt.Sprintf("rid_%d", offset+i)
		})
	case "struct":
		sb := array.NewStructBuilder(mem, rowIDStructType)
		defer sb.Release()
		aBuilder := sb.FieldBuilder(0).(*array.Int64Builder)
		bBuilder := sb.FieldBuilder(1).(*array.StringBuilder)
		for i := int64(0); i < size; i++ {
			sb.Append(true)
			aBuilder.Append(offset + i)
			bBuilder.Append(fmt.Sprintf("s_%d", offset+i))
		}
		return sb.NewStructArray()
	default: // int64
		return vgi.BuildInt64Array(size, func(i int64) int64 { return offset + i })
	}
}

func (f *RowIdSequenceFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *rowIDState, out *vgirpc.OutputCollector) error {
	mem := memory.NewGoAllocator()
	fullSchema := buildRowIDSchema(state.Layout, state.RowIDType)
	projected := vgi.ProjectedColumns(params.ProjectionIDs, fullSchema)
	return vgi.GenerateBatchMap(&state.BatchState, out, params.OutputSchema, func(size int64) (map[string]arrow.Array, error) {
		offset := state.Index
		colMap := make(map[string]arrow.Array)
		if projected.Contains("row_id") {
			colMap["row_id"] = buildRowIDArray(mem, state.RowIDType, offset, size)
		}
		if projected.Contains("name") {
			colMap["name"] = vgi.BuildStringArray(size, func(i int64) string {
				return fmt.Sprintf("item_%d", offset+i)
			})
		}
		if projected.Contains("value") {
			colMap["value"] = vgi.BuildStringArray(size, func(i int64) string {
				return fmt.Sprintf("val_%d", offset+i)
			})
		}
		return colMap, nil
	})
}

func NewRowIdSequenceFunction() vgi.TableFunction {
	return vgi.AsTableFunction[rowIDState](&RowIdSequenceFunction{})
}
