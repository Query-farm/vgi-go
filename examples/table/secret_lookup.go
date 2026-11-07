// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

// Secret-lookup conformance fixtures.
//
// The five table functions below implement the language-neutral contract that
// the shared test/sql/integration/secret/secret_lookup_*.test files run against
// every SDK's example worker (Query-farm/vgi docs/secret_lookup_fixtures.md;
// keep this file in step with it). They read secrets of the vgi_example type
// and select them only through the public vgi.Secrets selectors -- Lookup,
// LookupNamed, ForScopeOfType, Require -- because those are what is under test.

import (
	"context"
	"sort"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	// secretLookupType is the secret type every value fixture reads.
	secretLookupType = "vgi_example"
	// secretLookupScopeA / secretLookupScopeAB are the nested static scopes of
	// secret_value_for_scope, so longest-prefix selection is observable.
	secretLookupScopeA  = "vgi-conformance://a/"
	secretLookupScopeAB = "vgi-conformance://a/b/"
	// secretLookupS3Scope scopes the s3 requirement of secret_types_seen (s3
	// secrets default to scope s3://, so an unscoped lookup never matches one).
	secretLookupS3Scope = "s3://vgi-conformance/"
)

var secretValueSchema = arrow.NewSchema([]arrow.Field{
	{Name: "found", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	{Name: "secret_name", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

var secretTypesSeenSchema = arrow.NewSchema([]arrow.Field{
	{Name: "secret_name", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "secret_type", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "scope", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

type secretFieldArgs struct {
	Field string `vgi:"pos=0,doc=Secret field to return"`
}

type secretNameFieldArgs struct {
	Name  string `vgi:"pos=0,doc=Secret name to look up"`
	Field string `vgi:"pos=1,doc=Secret field to return"`
}

type secretPathFieldArgs struct {
	Path  string `vgi:"pos=0,doc=Path to select a secret for (longest scope prefix)"`
	Field string `vgi:"pos=1,doc=Secret field to return"`
}

// secretValueState is the single (found, secret_name, value) row to emit.
type secretValueState struct {
	Found    bool
	Name     string
	HasName  bool
	Value    string
	HasValue bool
	Done     bool
}

func newSecretValueState(secret map[string]interface{}, ok bool, field string) *secretValueState {
	st := &secretValueState{}
	if !ok || secret == nil {
		return st
	}
	st.Found = true
	if v, has := secret["name"]; has && v != nil {
		st.Name, st.HasName = vgi.RenderSecretValue(v), true
	}
	if v, has := secret[field]; has && v != nil {
		st.Value, st.HasValue = vgi.RenderSecretValue(v), true
	}
	return st
}

func appendOptString(b *array.StringBuilder, v string, has bool) {
	if has {
		b.Append(v)
	} else {
		b.AppendNull()
	}
}

func emitSecretValueRow(st *secretValueState, out *vgirpc.OutputCollector) error {
	if st.Done {
		return out.Finish()
	}
	mem := memory.DefaultAllocator
	found := array.NewBooleanBuilder(mem)
	defer found.Release()
	name := array.NewStringBuilder(mem)
	defer name.Release()
	value := array.NewStringBuilder(mem)
	defer value.Release()
	found.Append(st.Found)
	appendOptString(name, st.Name, st.HasName)
	appendOptString(value, st.Value, st.HasValue)
	st.Done = true
	return out.EmitArrays([]arrow.Array{found.NewArray(), name.NewArray(), value.NewArray()}, 1)
}

// ---------------------------------------------------------------------------
// secret_value_by_type
// ---------------------------------------------------------------------------

// SecretValueByTypeFunction selects the vgi_example secret by TYPE from a
// static requirement.
type SecretValueByTypeFunction struct{}

var _ vgi.TypedTableFunc[secretValueState] = (*SecretValueByTypeFunction)(nil)

func (f *SecretValueByTypeFunction) Name() string { return "secret_value_by_type" }

func (f *SecretValueByTypeFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:     "Conformance: a field of the vgi_example secret, selected by type",
		Stability:       vgi.StabilityVolatile,
		Categories:      []string{"secret", "testing"},
		RequiredSecrets: []vgi.SecretRequirement{{SecretType: secretLookupType}},
	}
}

func (f *SecretValueByTypeFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(secretFieldArgs{})
}

func (f *SecretValueByTypeFunction) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(secretValueSchema)
}

func (f *SecretValueByTypeFunction) NewState(params *vgi.ProcessParams) (*secretValueState, error) {
	var args secretFieldArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	secret, ok := params.Secrets.Lookup(secretLookupType)
	return newSecretValueState(secret, ok, args.Field), nil
}

func (f *SecretValueByTypeFunction) Process(_ context.Context, _ *vgi.ProcessParams, st *secretValueState, out *vgirpc.OutputCollector) error {
	return emitSecretValueRow(st, out)
}

// NewSecretValueByTypeFunction returns secret_value_by_type.
func NewSecretValueByTypeFunction() vgi.TableFunction {
	return vgi.AsTableFunction[secretValueState](&SecretValueByTypeFunction{})
}

// ---------------------------------------------------------------------------
// secret_value_required
// ---------------------------------------------------------------------------

// SecretValueRequiredFunction is secret_value_by_type, but bind fails (via
// Secrets.Require) when no vgi_example secret was resolved.
type SecretValueRequiredFunction struct{}

var _ vgi.TypedTableFunc[secretValueState] = (*SecretValueRequiredFunction)(nil)

func (f *SecretValueRequiredFunction) Name() string { return "secret_value_required" }

func (f *SecretValueRequiredFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:     "Conformance: a field of the required vgi_example secret; errors when absent",
		Stability:       vgi.StabilityVolatile,
		Categories:      []string{"secret", "testing"},
		RequiredSecrets: []vgi.SecretRequirement{{SecretType: secretLookupType}},
	}
}

func (f *SecretValueRequiredFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(secretFieldArgs{})
}

func (f *SecretValueRequiredFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	if _, err := params.Secrets.Require(secretLookupType); err != nil {
		return nil, err
	}
	return vgi.BindSchema(secretValueSchema)
}

func (f *SecretValueRequiredFunction) NewState(params *vgi.ProcessParams) (*secretValueState, error) {
	var args secretFieldArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	secret, ok := params.Secrets.Lookup(secretLookupType)
	return newSecretValueState(secret, ok, args.Field), nil
}

func (f *SecretValueRequiredFunction) Process(_ context.Context, _ *vgi.ProcessParams, st *secretValueState, out *vgirpc.OutputCollector) error {
	return emitSecretValueRow(st, out)
}

// NewSecretValueRequiredFunction returns secret_value_required.
func NewSecretValueRequiredFunction() vgi.TableFunction {
	return vgi.AsTableFunction[secretValueState](&SecretValueRequiredFunction{})
}

// ---------------------------------------------------------------------------
// secret_value_by_name
// ---------------------------------------------------------------------------

// SecretValueByNameFunction requests a vgi_example secret by NAME at bind
// (two-phase) and returns one of its fields.
type SecretValueByNameFunction struct{}

var _ vgi.TypedTableFunc[secretValueState] = (*SecretValueByNameFunction)(nil)

func (f *SecretValueByNameFunction) Name() string { return "secret_value_by_name" }

func (f *SecretValueByNameFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Conformance: a field of the named vgi_example secret",
		Stability:   vgi.StabilityVolatile,
		Categories:  []string{"secret", "testing"},
	}
}

func (f *SecretValueByNameFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(secretNameFieldArgs{})
}

func (f *SecretValueByNameFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	var args secretNameFieldArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	if !params.ResolvedSecretsProvided {
		return &vgi.BindResponse{
			SecretScopeRequest: []vgi.SecretLookup{{SecretType: secretLookupType, SecretName: args.Name}},
		}, nil
	}
	return vgi.BindSchema(secretValueSchema)
}

func (f *SecretValueByNameFunction) NewState(params *vgi.ProcessParams) (*secretValueState, error) {
	var args secretNameFieldArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	secret, ok := params.Secrets.LookupNamed(secretLookupType, args.Name)
	return newSecretValueState(secret, ok, args.Field), nil
}

func (f *SecretValueByNameFunction) Process(_ context.Context, _ *vgi.ProcessParams, st *secretValueState, out *vgirpc.OutputCollector) error {
	return emitSecretValueRow(st, out)
}

// NewSecretValueByNameFunction returns secret_value_by_name.
func NewSecretValueByNameFunction() vgi.TableFunction {
	return vgi.AsTableFunction[secretValueState](&SecretValueByNameFunction{})
}

// ---------------------------------------------------------------------------
// secret_value_for_scope
// ---------------------------------------------------------------------------

// SecretValueForScopeFunction selects among several same-type secrets by the
// longest scope prefix of path, falling back to the unscoped one.
type SecretValueForScopeFunction struct{}

var _ vgi.TypedTableFunc[secretValueState] = (*SecretValueForScopeFunction)(nil)

func (f *SecretValueForScopeFunction) Name() string { return "secret_value_for_scope" }

func (f *SecretValueForScopeFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Conformance: a field of the vgi_example secret whose scope best matches a path",
		Stability:   vgi.StabilityVolatile,
		Categories:  []string{"secret", "testing"},
		RequiredSecrets: []vgi.SecretRequirement{
			{SecretType: secretLookupType},
			{SecretType: secretLookupType, Scope: secretLookupScopeA},
			{SecretType: secretLookupType, Scope: secretLookupScopeAB},
		},
	}
}

func (f *SecretValueForScopeFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(secretPathFieldArgs{})
}

func (f *SecretValueForScopeFunction) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(secretValueSchema)
}

func (f *SecretValueForScopeFunction) NewState(params *vgi.ProcessParams) (*secretValueState, error) {
	var args secretPathFieldArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	secret, ok := params.Secrets.ForScopeOfType(args.Path, secretLookupType)
	return newSecretValueState(secret, ok, args.Field), nil
}

func (f *SecretValueForScopeFunction) Process(_ context.Context, _ *vgi.ProcessParams, st *secretValueState, out *vgirpc.OutputCollector) error {
	return emitSecretValueRow(st, out)
}

// NewSecretValueForScopeFunction returns secret_value_for_scope.
func NewSecretValueForScopeFunction() vgi.TableFunction {
	return vgi.AsTableFunction[secretValueState](&SecretValueForScopeFunction{})
}

// ---------------------------------------------------------------------------
// secret_types_seen
// ---------------------------------------------------------------------------

// SecretTypesSeenFunction lists every secret delivered to a function that
// requires two secret types (vgi_example unscoped, s3 scoped).
type SecretTypesSeenFunction struct{}

type secretTypesSeenState struct {
	Names  []string
	Types  []string
	Scopes []string
	Done   bool
}

var _ vgi.TypedTableFunc[secretTypesSeenState] = (*SecretTypesSeenFunction)(nil)

func (f *SecretTypesSeenFunction) Name() string { return "secret_types_seen" }

func (f *SecretTypesSeenFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Conformance: the secrets delivered for a vgi_example + s3 requirement",
		Stability:   vgi.StabilityVolatile,
		Categories:  []string{"secret", "testing"},
		RequiredSecrets: []vgi.SecretRequirement{
			{SecretType: secretLookupType},
			{SecretType: "s3", Scope: secretLookupS3Scope},
		},
	}
}

func (f *SecretTypesSeenFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

func (f *SecretTypesSeenFunction) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(secretTypesSeenSchema)
}

func (f *SecretTypesSeenFunction) NewState(params *vgi.ProcessParams) (*secretTypesSeenState, error) {
	st := &secretTypesSeenState{}
	for name := range params.Secrets {
		st.Names = append(st.Names, name)
	}
	sort.Strings(st.Names)
	for _, name := range st.Names {
		typ, _ := params.Secrets.SecretType(name)
		st.Types = append(st.Types, typ)
		st.Scopes = append(st.Scopes, vgi.RenderSecretValue(params.Secrets[name]["scope"]))
	}
	return st, nil
}

func (f *SecretTypesSeenFunction) Process(_ context.Context, _ *vgi.ProcessParams, st *secretTypesSeenState, out *vgirpc.OutputCollector) error {
	if st.Done || len(st.Names) == 0 {
		return out.Finish()
	}
	st.Done = true
	n := int64(len(st.Names))
	return out.EmitArrays([]arrow.Array{
		vgi.BuildStringArray(n, func(i int64) string { return st.Names[i] }),
		vgi.BuildStringArray(n, func(i int64) string { return st.Types[i] }),
		vgi.BuildStringArray(n, func(i int64) string { return st.Scopes[i] }),
	}, n)
}

// NewSecretTypesSeenFunction returns secret_types_seen.
func NewSecretTypesSeenFunction() vgi.TableFunction {
	return vgi.AsTableFunction[secretTypesSeenState](&SecretTypesSeenFunction{})
}
