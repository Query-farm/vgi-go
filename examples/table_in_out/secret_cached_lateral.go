// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// secret_cached_lateral(x) — the LATERAL member of the secret-dependent
// result-cache fixtures (see examples/internal/secretcache for the family and
// why it exists). Backs cache/secret_scope.test. Mirrors vgi-python's
// SecretCachedLateralFunction.

package table_in_out

import (
	"context"

	"github.com/Query-farm/vgi-go/examples/internal/secretcache"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// secretCachedLateralSchema is the output: the secret's secret_string (NULL
// when no secret resolves) and the per-call nonce.
var secretCachedLateralSchema = arrow.NewSchema([]arrow.Field{
	{Name: "secret_string", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "nonce", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
}, nil)

// SecretCachedLateralFunction is a blended 1->1 map emitting the vgi_example
// secret's secret_string and a per-call nonce, memoized per input value per
// secret.
//
// Unlike the other two members of the family it does NOT declare the secret in
// metadata: OnBind requests it (the two-phase bind, as scoped_secret_demo does),
// so the secret is discovered at bind time and the fingerprint has to come from
// the second bind pass. It advertises per_value on every output batch (as
// cached_double does) so a correlated LATERAL call is memoized per input value;
// every row of a call carries that call's one nonce.
type SecretCachedLateralFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*SecretCachedLateralFunction)(nil)

func (f *SecretCachedLateralFunction) Name() string { return "secret_cached_lateral" }

func (f *SecretCachedLateralFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:   "Blended map emitting a secret's value and a per-call nonce; memoized per secret",
		Stability:     vgi.StabilityConsistent,
		Categories:    []string{"blended", "cache", "secret", "test"},
		InputFromArgs: true,
	}
}

func (f *SecretCachedLateralFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "x", Position: 0, ArrowType: "int64", Doc: "Input column"},
	}
}

// OnBind requests the secret on the first pass and declares the output columns
// on the second, once the client has resolved it.
func (f *SecretCachedLateralFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	if !params.ResolvedSecretsProvided {
		return &vgi.BindResponse{
			SecretScopeRequest: []vgi.SecretLookup{{SecretType: secretcache.SecretType}},
		}, nil
	}
	return vgi.BindSchema(secretCachedLateralSchema)
}

func (f *SecretCachedLateralFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

// Process emits one row per input row, all carrying this call's nonce.
func (f *SecretCachedLateralFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	secret, ok := secretcache.SecretString(params.Secrets)
	nonce := secretcache.Nonce()
	n := int(batch.NumRows())

	mem := memory.NewGoAllocator()
	sb := array.NewStringBuilder(mem)
	defer sb.Release()
	for i := 0; i < n; i++ {
		if ok {
			sb.Append(secret)
		} else {
			sb.AppendNull()
		}
	}
	secretArr := sb.NewArray()
	defer secretArr.Release()
	nonceArr := vgi.BuildInt64Array(int64(n), func(int64) int64 { return nonce })
	defer nonceArr.Release()

	result := array.NewRecordBatch(params.OutputSchema, []arrow.Array{secretArr, nonceArr}, int64(n))
	return vgi.Emit(out, result, vgi.WithCacheControl(&vgi.CacheControl{
		Ttl: vgi.Seconds(secretcache.TTLSeconds), PerValue: true,
	}))
}

func (f *SecretCachedLateralFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewSecretCachedLateralFunction creates a SecretCachedLateralFunction wrapped
// for registration.
func NewSecretCachedLateralFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&SecretCachedLateralFunction{})
}
