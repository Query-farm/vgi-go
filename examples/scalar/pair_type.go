// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// PairTypeIntIntFunction — pair_type(int64, int64) → "int+int"
type PairTypeIntIntFunction struct{}

func (f *PairTypeIntIntFunction) Name() string { return "pair_type" }

func (f *PairTypeIntIntFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Returns type pair description for two int64 columns",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (f *PairTypeIntIntFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "a", Position: 0, ArrowType: "int64", Doc: "First int value"},
		{Name: "b", Position: 1, ArrowType: "int64", Doc: "Second int value"},
	}
}

func (f *PairTypeIntIntFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *PairTypeIntIntFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapColumns(params, batch, []int{0, 1}, array.NewStringBuilder,
		func(cols []arrow.Array, i int) string {
			return "int+int"
		})
}

// PairTypeStrStrFunction — pair_type(varchar, varchar) → "str+str"
type PairTypeStrStrFunction struct{}

func (f *PairTypeStrStrFunction) Name() string { return "pair_type" }

func (f *PairTypeStrStrFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Returns type pair description for two varchar columns",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (f *PairTypeStrStrFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "a", Position: 0, ArrowType: "varchar", Doc: "First string value"},
		{Name: "b", Position: 1, ArrowType: "varchar", Doc: "Second string value"},
	}
}

func (f *PairTypeStrStrFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *PairTypeStrStrFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapColumns(params, batch, []int{0, 1}, array.NewStringBuilder,
		func(cols []arrow.Array, i int) string {
			return "str+str"
		})
}

// PairTypeIntStrFunction — pair_type(int64, varchar) → "int+str"
type PairTypeIntStrFunction struct{}

func (f *PairTypeIntStrFunction) Name() string { return "pair_type" }

func (f *PairTypeIntStrFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Returns type pair description for int64 + varchar columns",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (f *PairTypeIntStrFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "a", Position: 0, ArrowType: "int64", Doc: "Int value"},
		{Name: "b", Position: 1, ArrowType: "varchar", Doc: "String value"},
	}
}

func (f *PairTypeIntStrFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *PairTypeIntStrFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapColumns(params, batch, []int{0, 1}, array.NewStringBuilder,
		func(cols []arrow.Array, i int) string {
			return "int+str"
		})
}
