// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// typeInfoBase provides shared logic for all type_info overloads.
type typeInfoBase struct {
	typeName  string
	arrowType string
}

func (f *typeInfoBase) Name() string { return "type_info" }

func (f *typeInfoBase) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Returns the type name of the column",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (f *typeInfoBase) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *typeInfoBase) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	typeName := f.typeName
	return vgi.MapColumn(params, batch, 0, array.NewStringBuilder,
		func(col arrow.Array, i int) string {
			return typeName
		})
}

// TypeInfoInt32Function — type_info for int32 columns.
type TypeInfoInt32Function struct{ typeInfoBase }

func NewTypeInfoInt32Function() *TypeInfoInt32Function {
	return &TypeInfoInt32Function{typeInfoBase{typeName: "int32", arrowType: "int32"}}
}

func (f *TypeInfoInt32Function) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "int32", Doc: "Input value"},
	}
}

// TypeInfoInt64Function — type_info for int64 columns.
type TypeInfoInt64Function struct{ typeInfoBase }

func NewTypeInfoInt64Function() *TypeInfoInt64Function {
	return &TypeInfoInt64Function{typeInfoBase{typeName: "int64", arrowType: "int64"}}
}

func (f *TypeInfoInt64Function) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "int64", Doc: "Input value"},
	}
}

// TypeInfoUint32Function — type_info for uint32 columns.
type TypeInfoUint32Function struct{ typeInfoBase }

func NewTypeInfoUint32Function() *TypeInfoUint32Function {
	return &TypeInfoUint32Function{typeInfoBase{typeName: "uint32", arrowType: "uint32"}}
}

func (f *TypeInfoUint32Function) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "uint32", Doc: "Input value"},
	}
}

// TypeInfoUint64Function — type_info for uint64 columns.
type TypeInfoUint64Function struct{ typeInfoBase }

func NewTypeInfoUint64Function() *TypeInfoUint64Function {
	return &TypeInfoUint64Function{typeInfoBase{typeName: "uint64", arrowType: "uint64"}}
}

func (f *TypeInfoUint64Function) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "uint64", Doc: "Input value"},
	}
}

// TypeInfoVarcharFunction — type_info for varchar columns.
type TypeInfoVarcharFunction struct{ typeInfoBase }

func NewTypeInfoVarcharFunction() *TypeInfoVarcharFunction {
	return &TypeInfoVarcharFunction{typeInfoBase{typeName: "varchar", arrowType: "varchar"}}
}

func (f *TypeInfoVarcharFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "varchar", Doc: "Input value"},
	}
}
