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

// BernoulliFunction generates random booleans (no input columns, VOLATILE).
type BernoulliFunction struct{}

func (f *BernoulliFunction) Name() string { return "bernoulli" }

func (f *BernoulliFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generate random booleans (demonstrates VOLATILE stability)",
		Stability:   vgi.StabilityVolatile,
	}
}

func (f *BernoulliFunction) ArgumentSpecs() []vgi.ArgSpec {
	return nil
}

func (f *BernoulliFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "result", Type: arrow.FixedWidthTypes.Boolean},
		}, nil),
	}, nil
}

func (f *BernoulliFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	n := int(batch.NumRows())

	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()

	for i := 0; i < n; i++ {
		builder.Append(rand.Intn(2) == 1)
	}

	resultArr := builder.NewArray()
	defer resultArr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, int64(n)), nil
}
