// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// HashSeedFunction generates deterministic integers from a constant seed.
// Each output value is seed + row_index.
type HashSeedFunction struct{}

func (f *HashSeedFunction) Name() string { return "hash_seed" }

func (f *HashSeedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generate deterministic integers from a constant seed",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.PrimitiveTypes.Int64,
	}
}

func (f *HashSeedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "seed", Position: 0, ArrowType: "int64", Doc: "Seed value", IsConst: true},
	}
}

func (f *HashSeedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.PrimitiveTypes.Int64)
}

func (f *HashSeedFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	seed, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}

	return vgi.GenerateColumn(params, batch, array.NewInt64Builder,
		func(i int) int64 {
			return seed + int64(i)
		})
}
