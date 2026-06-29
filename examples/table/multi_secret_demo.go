// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// MultiSecretDemoFunction resolves TWO same-type scoped secrets in one bind,
// then selects the one matching the path argument via ForScopeOfType.
//
// It requests the vgi_example secret for both s3://bucket-a/ and s3://bucket-b/
// scopes in a single bind. Because resolved secrets are keyed by name, both
// survive; ForScopeOfType then picks the one whose scope matches the path
// argument and returns its api_key.
type MultiSecretDemoFunction struct{}

var _ vgi.TypedTableFunc[multiSecretDemoState] = (*MultiSecretDemoFunction)(nil)

func (f *MultiSecretDemoFunction) Name() string { return "multi_secret_demo" }

func (f *MultiSecretDemoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Demo: two same-type scoped secrets resolved in one bind",
		Stability:   vgi.StabilityVolatile,
		RequiredSecrets: []vgi.SecretRequirement{
			{SecretType: "vgi_example"},
		},
	}
}

func (f *MultiSecretDemoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(scopedSecretDemoArgs{})
}

var multiSecretDemoOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "api_key", Type: arrow.BinaryTypes.String},
}, nil)

func (f *MultiSecretDemoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	var args scopedSecretDemoArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}

	if !params.ResolvedSecretsProvided {
		// Phase 1: request the vgi_example secret for two distinct scopes.
		return &vgi.BindResponse{
			SecretScopeRequest: []vgi.SecretLookup{
				{SecretType: "vgi_example", Scope: "s3://bucket-a/"},
				{SecretType: "vgi_example", Scope: "s3://bucket-b/"},
			},
		}, nil
	}

	// Phase 2: secrets resolved, return output schema.
	return vgi.BindSchema(multiSecretDemoOutputSchema)
}

type multiSecretDemoState struct {
	APIKey string
}

func (f *MultiSecretDemoFunction) NewState(params *vgi.ProcessParams) (*multiSecretDemoState, error) {
	var args scopedSecretDemoArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}

	state := &multiSecretDemoState{}
	if params.Secrets != nil {
		if secret, ok := params.Secrets.ForScopeOfType(args.Path, "vgi_example"); ok {
			state.APIKey = vgi.RenderSecretValue(secret["api_key"])
		}
	}
	return state, nil
}

func (f *MultiSecretDemoFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *multiSecretDemoState, out *vgirpc.OutputCollector) error {
	apiKeys := vgi.BuildStringArray(1, func(i int64) string { return state.APIKey })
	if err := out.EmitArrays([]arrow.Array{apiKeys}, 1); err != nil {
		return err
	}
	return out.Finish()
}

func NewMultiSecretDemoFunction() vgi.TableFunction {
	return vgi.AsTableFunction[multiSecretDemoState](&MultiSecretDemoFunction{})
}
