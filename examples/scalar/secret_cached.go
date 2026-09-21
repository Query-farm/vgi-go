// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// secret_cached_scalar(value) — the scalar member of the secret-dependent
// result-cache fixtures (see examples/internal/secretcache for the family and
// why it exists). Backs cache/secret_scope.test. Mirrors vgi-python's
// SecretCachedScalarFunction.

package scalar

import (
	"context"
	"strconv"

	"github.com/Query-farm/vgi-go/examples/internal/secretcache"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// SecretCachedScalarFunction maps every value to '<secret_string>|<nonce>' and
// is memoized per value per secret.
//
// The secret is declared in Metadata().RequiredSecrets (as on
// return_secret_value), so the execute-time bind resolves it and the extension
// keys the per-value memo on its fingerprint. One nonce per Process call,
// shared by the whole batch, so a served value keeps the nonce of the call that
// produced it. With no secret resolved the label is '|<nonce>' — a dropped
// secret is a state the test drives, so it must not be an error. per_value is a
// TEST choice, as on cached_double_scalar: the point is coverage of the tier,
// not economics.
type SecretCachedScalarFunction struct{}

var _ vgi.ScalarFunction = (*SecretCachedScalarFunction)(nil)

func (f *SecretCachedScalarFunction) Name() string { return "secret_cached_scalar" }

func (f *SecretCachedScalarFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Returns '<secret_string>|<nonce>' per value; memoized per value per secret",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
		RequiredSecrets: []vgi.SecretRequirement{
			{SecretType: secretcache.SecretType},
		},
		CacheControl: &vgi.CacheControl{Ttl: vgi.Seconds(secretcache.TTLSeconds), PerValue: true},
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT secret_cached_scalar(1)", Description: "Stable while the vgi_example secret is unchanged"},
		},
	}
}

func (f *SecretCachedScalarFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "int64", Doc: "Any value; the output ignores it"},
	}
}

func (f *SecretCachedScalarFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: varcharOutputSchema}, nil
}

// Process labels every row with the secret's value ("" when none) and this
// call's nonce.
func (f *SecretCachedScalarFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	secret, _ := secretcache.SecretString(params.Secrets)
	label := secret + "|" + strconv.FormatInt(secretcache.Nonce(), 10)
	return vgi.GenerateColumn(params, batch, array.NewStringBuilder,
		func(int) string { return label })
}
