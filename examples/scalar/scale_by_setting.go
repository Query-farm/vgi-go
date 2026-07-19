// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"context"
	"strconv"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ScaleBySettingFunction scales the input value by the float setting
// `scale_factor`. It reads a DOUBLE session setting (distinct from
// multiply_by_setting's integer `multiplier`).
type ScaleBySettingFunction struct{}

func (f *ScaleBySettingFunction) Name() string { return "scale_by_setting" }

func (f *ScaleBySettingFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Scale the input value by the float setting `scale_factor`",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.PrimitiveTypes.Float64,
	}
}

func (f *ScaleBySettingFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "float64", Doc: "Value to scale"},
	}
}

func (f *ScaleBySettingFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.PrimitiveTypes.Float64)
}

func (f *ScaleBySettingFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	scale := 1.0
	if params.Settings != nil {
		if v, ok := params.Settings["scale_factor"]; ok {
			scale = scaleFactorValue(v)
		}
	}

	get := vgi.Float64Accessor(batch.Column(0)) // hoist the type switch out of the row loop
	return vgi.MapColumn(params, batch, 0, array.NewFloat64Builder,
		func(_ arrow.Array, i int) float64 {
			return get(i) * scale
		})
}

// scaleFactorValue coerces a setting value (which may arrive as a float, an
// integer, or a string) to a float64, defaulting to 1.0 when it cannot.
func scaleFactorValue(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case int:
		return float64(val)
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return 1.0
}
