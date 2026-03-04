// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"sort"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// ScopedSecretDemoFunction demonstrates two-phase bind with scoped secrets.
type ScopedSecretDemoFunction struct{}

var _ vgi.TypedTableFunc[scopedSecretDemoState] = (*ScopedSecretDemoFunction)(nil)

func (f *ScopedSecretDemoFunction) Name() string { return "scoped_secret_demo" }

func (f *ScopedSecretDemoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Demonstrates two-phase bind with scoped secrets",
		Stability:   vgi.StabilityVolatile,
		RequiredSecrets: []vgi.SecretRequirement{
			{SecretType: "vgi_example"},
		},
	}
}

func (f *ScopedSecretDemoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "path", Position: 0, ArrowType: "varchar", Doc: "Path for scoped secret lookup", IsConst: true},
	}
}

var scopedSecretDemoOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "scope", Type: arrow.BinaryTypes.String},
	{Name: "found", Type: &arrow.BooleanType{}},
	{Name: "secret_keys", Type: arrow.BinaryTypes.String},
}, nil)

func (f *ScopedSecretDemoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	path, err := params.Args.GetScalarString(0)
	if err != nil {
		return nil, err
	}

	if !params.ResolvedSecretsProvided {
		// Phase 1: request scoped secret lookup
		return &vgi.BindResponse{
			SecretScopeRequest: []vgi.SecretLookup{
				{SecretType: "vgi_example", Scope: path},
			},
		}, nil
	}

	// Phase 2: secrets resolved, return output schema
	return vgi.BindSchema(scopedSecretDemoOutputSchema)
}

type scopedSecretDemoState struct {
	Scope      string
	Found      bool
	SecretKeys string
}

func (f *ScopedSecretDemoFunction) NewState(params *vgi.ProcessParams) (*scopedSecretDemoState, error) {
	path, _ := params.Args.GetScalarString(0)
	state := &scopedSecretDemoState{
		Scope: path,
	}

	if params.Secrets != nil {
		if secret, ok := params.Secrets["vgi_example"]; ok && len(secret) > 0 {
			state.Found = true
			keys := make([]string, 0, len(secret))
			for k := range secret {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			state.SecretKeys = strings.Join(keys, ",")
		}
	}

	return state, nil
}

func (f *ScopedSecretDemoFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *scopedSecretDemoState, out *vgirpc.OutputCollector) error {
	scopes := vgi.BuildStringArray(1, func(i int64) string { return state.Scope })
	founds := vgi.BuildBooleanArray(1, func(i int64) bool { return state.Found })
	secretKeys := vgi.BuildStringArray(1, func(i int64) string { return state.SecretKeys })

	if err := out.EmitArrays([]arrow.Array{scopes, founds, secretKeys}, 1); err != nil {
		return err
	}
	return out.Finish()
}

func NewScopedSecretDemoFunction() vgi.TableFunction {
	return vgi.AsTableFunction[scopedSecretDemoState](&ScopedSecretDemoFunction{})
}
