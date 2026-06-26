// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"
	"fmt"
	"sort"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// SecretDemoFunction outputs secret key-value pairs as rows.
type SecretDemoFunction struct{}

var _ vgi.TypedTableFunc[secretDemoState] = (*SecretDemoFunction)(nil)

func (f *SecretDemoFunction) Name() string { return "secret_demo" }

func (f *SecretDemoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Outputs secret key-value pairs as rows",
		Stability:   vgi.StabilityVolatile,
		RequiredSecrets: []vgi.SecretRequirement{
			{SecretType: "vgi_example"},
		},
	}
}

func (f *SecretDemoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return nil
}

var secretDemoOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "key", Type: arrow.BinaryTypes.String},
	{Name: "value", Type: arrow.BinaryTypes.String},
	{Name: "arrow_type", Type: arrow.BinaryTypes.String},
}, nil)

func (f *SecretDemoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(secretDemoOutputSchema)
}

type secretDemoState struct {
	Keys       []string
	Values     []string
	ArrowTypes []string
}

func (f *SecretDemoFunction) NewState(params *vgi.ProcessParams) (*secretDemoState, error) {
	state := &secretDemoState{}
	if params.Secrets == nil {
		return state, nil
	}
	// Secrets are keyed by name; select by type via OfType.
	matches := params.Secrets.OfType("vgi_example")
	if len(matches) == 0 || len(matches[0]) == 0 {
		return state, nil
	}
	secret := matches[0]

	// Sort keys for deterministic output
	keys := make([]string, 0, len(secret))
	for k := range secret {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := secret[k]
		state.Keys = append(state.Keys, k)
		state.Values = append(state.Values, fmt.Sprintf("%v", v))
		state.ArrowTypes = append(state.ArrowTypes, arrowTypeName(v))
	}
	return state, nil
}

func (f *SecretDemoFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *secretDemoState, out *vgirpc.OutputCollector) error {
	if len(state.Keys) == 0 {
		return out.Finish()
	}

	n := int64(len(state.Keys))
	keys := vgi.BuildStringArray(n, func(i int64) string { return state.Keys[i] })
	values := vgi.BuildStringArray(n, func(i int64) string { return state.Values[i] })
	arrowTypes := vgi.BuildStringArray(n, func(i int64) string { return state.ArrowTypes[i] })

	if err := out.EmitArrays([]arrow.Array{keys, values, arrowTypes}, n); err != nil {
		return err
	}
	return out.Finish()
}

// arrowTypeName returns a human-readable type name for a secret value.
// Note: the secrets pipeline normalizes smaller integer types (int8/16/32) to int64.
func arrowTypeName(v interface{}) string {
	switch v.(type) {
	case string:
		return "utf8"
	case int64:
		return "int64"
	case float64:
		return "float64"
	case bool:
		return "bool"
	default:
		return fmt.Sprintf("%T", v)
	}
}

func NewSecretDemoFunction() vgi.TableFunction {
	return vgi.AsTableFunction[secretDemoState](&SecretDemoFunction{})
}
