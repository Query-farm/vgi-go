// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"context"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// UpperCaseFunction converts string values to uppercase.
type UpperCaseFunction struct{}

func (f *UpperCaseFunction) Name() string { return "upper_case" }

func (f *UpperCaseFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Converts string values to uppercase",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (f *UpperCaseFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "varchar", Doc: "String value to uppercase"},
	}
}

func (f *UpperCaseFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *UpperCaseFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapColumn(params, batch, 0, array.NewStringBuilder,
		func(col arrow.Array, i int) string {
			return strings.ToUpper(vgi.GetStringValue(col, i))
		})
}
