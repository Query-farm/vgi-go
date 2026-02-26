// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"
	"math/rand"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// RandomIntFunction generates random integers (demonstrates VOLATILE stability).
type RandomIntFunction struct{}

func (f *RandomIntFunction) Name() string { return "random_int" }

func (f *RandomIntFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generate random integers (demonstrates VOLATILE stability)",
		Stability:   vgi.StabilityVolatile,
		ReturnType:  arrow.PrimitiveTypes.Int64,
	}
}

func (f *RandomIntFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "min_val", Position: 0, ArrowType: "int64", Doc: "Minimum value (inclusive)"},
		{Name: "max_val", Position: 1, ArrowType: "int64", Doc: "Maximum value (inclusive)"},
	}
}

func (f *RandomIntFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.PrimitiveTypes.Int64)
}

func (f *RandomIntFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapColumns(params, batch, []int{0, 1}, array.NewInt64Builder,
		func(cols []arrow.Array, i int) int64 {
			minVal := vgi.GetInt64Value(cols[0], i)
			maxVal := vgi.GetInt64Value(cols[1], i)
			if maxVal <= minVal {
				return minVal
			}
			return minVal + rand.Int63n(maxVal-minVal+1)
		})
}
