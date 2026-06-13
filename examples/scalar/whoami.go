// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// WhoAmIFunction returns the authenticated principal name.
// Over stdio transport (no auth), it always returns "anonymous".
type WhoAmIFunction struct{}

func (f *WhoAmIFunction) Name() string { return "whoami" }

func (f *WhoAmIFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Return the authenticated principal name",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (f *WhoAmIFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "x", Position: 0, ArrowType: "int64", Doc: "dummy input"},
	}
}

func (f *WhoAmIFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *WhoAmIFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	numRows := int64(batch.NumRows())
	principal := "anonymous"
	if params.Auth != nil && params.Auth.Principal != "" {
		principal = params.Auth.Principal
	}
	result := vgi.BuildStringArray(numRows, func(i int64) string {
		return principal
	})
	return vgi.BuildResultBatch(params, result, numRows), nil
}
