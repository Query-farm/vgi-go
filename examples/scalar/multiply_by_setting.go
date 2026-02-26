// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"
	"fmt"
	"strconv"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// MultiplyBySettingFunction multiplies the input value by a setting value.
type MultiplyBySettingFunction struct{}

func (f *MultiplyBySettingFunction) Name() string { return "multiply_by_setting" }

func (f *MultiplyBySettingFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Multiply the input value by a setting value",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.PrimitiveTypes.Int64,
	}
}

func (f *MultiplyBySettingFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "int64", Doc: "Integer value to multiply"},
	}
}

func (f *MultiplyBySettingFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.PrimitiveTypes.Int64)
}

func (f *MultiplyBySettingFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	// Get multiplier from settings
	var multiplier int64 = 1
	if params.Settings != nil {
		if v, ok := params.Settings["multiplier"]; ok {
			switch val := v.(type) {
			case int64:
				multiplier = val
			case string:
				if n, err := strconv.ParseInt(val, 10, 64); err == nil {
					multiplier = n
				}
			default:
				return nil, fmt.Errorf("unsupported multiplier setting type: %T", v)
			}
		}
	}

	return vgi.MapColumn(params, batch, 0, array.NewInt64Builder,
		func(col arrow.Array, i int) int64 {
			return vgi.GetInt64Value(col, i) * multiplier
		})
}
