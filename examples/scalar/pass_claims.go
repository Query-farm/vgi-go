// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"context"
	"encoding/json"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// PassClaimsFunction returns the caller's verified Orchard Pass claims as a
// JSON object: {"claims": {...}, "domain": ..., "principal": ...}, where
// claims is the auth context's claim map as is. Claims keep their JSON types
// (email_verified is a boolean, exp an integer), so a test can tell a real
// boolean from the string "true". The contract is test/hosted/README.md in the
// vgi repository ("The fixture function: main.pass_claims").
//
// It is the Orchard hosted-mode fixture: the example worker registers it only
// when VGI_ORCHARD_HOSTED is on, so the ordinary function listing is
// unchanged.
//
// SQL: SELECT pass_claims(1)
type PassClaimsFunction struct{}

func (f *PassClaimsFunction) Name() string { return "pass_claims" }

func (f *PassClaimsFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Return the caller's verified Orchard Pass claims as JSON",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (f *PassClaimsFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "x", Position: 0, ArrowType: "int64", Doc: "dummy input"},
	}
}

func (f *PassClaimsFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *PassClaimsFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	numRows := int64(batch.NumRows())
	payload := map[string]any{"principal": "", "domain": "", "claims": map[string]any{}}
	if params.Auth != nil {
		claims := params.Auth.Claims
		if claims == nil {
			claims = map[string]any{}
		}
		payload = map[string]any{"principal": params.Auth.Principal, "domain": params.Auth.Domain, "claims": claims}
	}
	// encoding/json writes map keys sorted, so the output is deterministic.
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	result := vgi.BuildStringArray(numRows, func(int64) string { return string(encoded) })
	return vgi.BuildResultBatch(params, result, numRows), nil
}
