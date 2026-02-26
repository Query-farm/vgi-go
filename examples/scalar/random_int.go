// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"
	"math/rand"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// RandomIntFunction generates random integers (demonstrates VOLATILE stability).
type RandomIntFunction struct{}

func (f *RandomIntFunction) Name() string { return "random_int" }

func (f *RandomIntFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generate random integers (demonstrates VOLATILE stability)",
		Stability:   vgi.StabilityVolatile,
	}
}

func (f *RandomIntFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "min_val", Position: 0, ArrowType: "int64", Doc: "Minimum value (inclusive)"},
		{Name: "max_val", Position: 1, ArrowType: "int64", Doc: "Maximum value (inclusive)"},
	}
}

func (f *RandomIntFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "result", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}, nil
}

func (f *RandomIntFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	minCol := batch.Column(0)
	maxCol := batch.Column(1)
	n := int(batch.NumRows())

	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	for i := 0; i < n; i++ {
		if minCol.IsNull(i) || maxCol.IsNull(i) {
			builder.AppendNull()
		} else {
			minVal := getInt64Value(minCol, i)
			maxVal := getInt64Value(maxCol, i)
			if maxVal <= minVal {
				builder.Append(minVal)
			} else {
				builder.Append(minVal + rand.Int63n(maxVal-minVal+1))
			}
		}
	}

	resultArr := builder.NewArray()
	defer resultArr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, int64(n)), nil
}
