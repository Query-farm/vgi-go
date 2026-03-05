// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// AnyMixedIntFunction — any_mixed(any, int64) → "any+int: {val}"
type AnyMixedIntFunction struct{}

func (f *AnyMixedIntFunction) Name() string { return "any_mixed" }

func (f *AnyMixedIntFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Returns description with any + int64 value",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (f *AnyMixedIntFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "a", Position: 0, ArrowType: "any", Doc: "Any value"},
		{Name: "b", Position: 1, ArrowType: "int64", Doc: "Int value"},
	}
}

func (f *AnyMixedIntFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *AnyMixedIntFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapColumns(params, batch, []int{0, 1}, array.NewStringBuilder,
		func(cols []arrow.Array, i int) string {
			return fmt.Sprintf("any+int: %d", vgi.GetInt64Value(cols[1], i))
		})
}

// AnyMixedStrFunction — any_mixed(any, varchar) → "any+str: {val}"
type AnyMixedStrFunction struct{}

func (f *AnyMixedStrFunction) Name() string { return "any_mixed" }

func (f *AnyMixedStrFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Returns description with any + varchar value",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (f *AnyMixedStrFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "a", Position: 0, ArrowType: "any", Doc: "Any value"},
		{Name: "b", Position: 1, ArrowType: "varchar", Doc: "String value"},
	}
}

func (f *AnyMixedStrFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *AnyMixedStrFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapColumns(params, batch, []int{0, 1}, array.NewStringBuilder,
		func(cols []arrow.Array, i int) string {
			return fmt.Sprintf("any+str: %s", vgi.GetStringValue(cols[1], i))
		})
}
