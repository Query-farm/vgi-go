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

// RandomBytesFunction generates deterministic pseudo-random binary blobs from a seed.
type RandomBytesFunction struct{}

func (f *RandomBytesFunction) Name() string { return "random_bytes" }

func (f *RandomBytesFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generate pseudo-random binary blobs from seed and length",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *RandomBytesFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "seed", Position: 0, ArrowType: "int64", Doc: "Seed for pseudo-random byte generation", IsConst: true},
		{Name: "byte_length", Position: 1, ArrowType: "int64", Doc: "Output blob length in bytes", IsConst: true},
	}
}

func (f *RandomBytesFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "result", Type: arrow.BinaryTypes.Binary},
		}, nil),
	}, nil
}

func (f *RandomBytesFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	seed, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	byteLength, err := params.Args.GetScalarInt64(1)
	if err != nil {
		return nil, err
	}

	mem := memory.NewGoAllocator()
	n := int(batch.NumRows())

	builder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer builder.Release()

	rng := rand.New(rand.NewSource(seed))

	for i := 0; i < n; i++ {
		blob := make([]byte, byteLength)
		for j := range blob {
			blob[j] = byte(rng.Intn(256))
		}
		builder.Append(blob)
	}

	resultArr := builder.NewArray()
	defer resultArr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, int64(n)), nil
}
