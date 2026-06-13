// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// FormatNumberDefaultFunction formats a number with default precision (0 decimals).
type FormatNumberDefaultFunction struct{}

func (f *FormatNumberDefaultFunction) Name() string { return "format_number" }

func (f *FormatNumberDefaultFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Format number with default precision (0 decimals)",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (f *FormatNumberDefaultFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "double", Doc: "Number to format"},
	}
}

func (f *FormatNumberDefaultFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *FormatNumberDefaultFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapColumn(params, batch, 0, array.NewStringBuilder,
		func(col arrow.Array, i int) string {
			return fmt.Sprintf("%.0f", vgi.GetFloat64Value(col, i))
		})
}

// FormatNumberPrecisionFunction formats a number with specified precision.
type FormatNumberPrecisionFunction struct{}

func (f *FormatNumberPrecisionFunction) Name() string { return "format_number" }

func (f *FormatNumberPrecisionFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Format number with specified precision",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (f *FormatNumberPrecisionFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "precision", Position: 0, ArrowType: "int64", Doc: "Decimal places", IsConst: true},
		{Name: "value", Position: 1, ArrowType: "double", Doc: "Number to format"},
	}
}

func (f *FormatNumberPrecisionFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *FormatNumberPrecisionFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	precision, _ := params.Args.GetScalarInt64(0)
	return vgi.MapColumn(params, batch, 0, array.NewStringBuilder,
		func(col arrow.Array, i int) string {
			return fmt.Sprintf("%.*f", int(precision), vgi.GetFloat64Value(col, i))
		})
}

// FormatNumberFullFunction formats a number with precision and prefix.
type FormatNumberFullFunction struct{}

func (f *FormatNumberFullFunction) Name() string { return "format_number" }

func (f *FormatNumberFullFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Format number with precision and prefix",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (f *FormatNumberFullFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "precision", Position: 0, ArrowType: "int64", Doc: "Decimal places", IsConst: true},
		{Name: "prefix", Position: 1, ArrowType: "varchar", Doc: "Prefix string", IsConst: true},
		{Name: "value", Position: 2, ArrowType: "double", Doc: "Number to format"},
	}
}

func (f *FormatNumberFullFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *FormatNumberFullFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	precision, _ := params.Args.GetScalarInt64(0)
	prefix, _ := params.Args.GetScalarString(1)
	return vgi.MapColumn(params, batch, 0, array.NewStringBuilder,
		func(col arrow.Array, i int) string {
			return prefix + fmt.Sprintf("%.*f", int(precision), vgi.GetFloat64Value(col, i))
		})
}
