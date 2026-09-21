// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// secret_cache_nonce — the producer member of the secret-dependent result-cache
// fixtures (see examples/internal/secretcache for the family and why it
// exists). Backs cache/secret_scope.test both as a table function and as the
// inline-bound ex.data.secret_cache_nonce table. Mirrors vgi-python's
// SecretCacheNonceFunction.

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/examples/internal/secretcache"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// secretCacheNonceSchema is the fixed output: the secret's secret_string (NULL
// when no secret resolves) and the per-invocation nonce. Static, so the data
// table can inline its bind result.
var secretCacheNonceSchema = arrow.NewSchema([]arrow.Field{
	{Name: "secret_string", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "nonce", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
}, nil)

// secretCacheNonceState is the one row to emit, minted on a real invocation.
type secretCacheNonceState struct {
	SecretString *string
	Nonce        int64
	Done         bool
}

// SecretCacheNonceFunction emits ONE row: the vgi_example secret's
// secret_string and a nonce, advertising a cache TTL.
//
// The secret is declared in Metadata().RequiredSecrets, so the client resolves
// it before bind and folds its fingerprint into the result-cache key. NewState
// runs only on a cache MISS, so the nonce is stable across HITs. A rotated (or
// re-fielded, or dropped) secret must MISS and report the new value; restoring
// the original secret must HIT the entry it produced.
type SecretCacheNonceFunction struct{}

var _ vgi.TypedTableFunc[secretCacheNonceState] = (*SecretCacheNonceFunction)(nil)

func (f *SecretCacheNonceFunction) Name() string { return "secret_cache_nonce" }

func (f *SecretCacheNonceFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "One row with a secret's value and a per-invocation nonce; cacheable per secret",
		Categories:  []string{"generator", "cache", "secret", "testing"},
		RequiredSecrets: []vgi.SecretRequirement{
			{SecretType: secretcache.SecretType},
		},
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT * FROM secret_cache_nonce()", Description: "The nonce is stable while the vgi_example secret is unchanged"},
		},
	}
}

func (f *SecretCacheNonceFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cacheNoArgs{})
}

func (f *SecretCacheNonceFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(secretCacheNonceSchema)
}

func (f *SecretCacheNonceFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

// NewState reads the secret and mints the nonce for this (real) invocation.
func (f *SecretCacheNonceFunction) NewState(params *vgi.ProcessParams) (*secretCacheNonceState, error) {
	state := &secretCacheNonceState{Nonce: secretcache.Nonce()}
	if v, ok := secretcache.SecretString(params.Secrets); ok {
		state.SecretString = &v
	}
	return state, nil
}

func (f *SecretCacheNonceFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *secretCacheNonceState, out *vgirpc.OutputCollector) error {
	if state.Done {
		return out.Finish()
	}
	mem := memory.NewGoAllocator()
	sb := array.NewStringBuilder(mem)
	defer sb.Release()
	if state.SecretString != nil {
		sb.Append(*state.SecretString)
	} else {
		sb.AppendNull()
	}
	secretArr := sb.NewArray()
	defer secretArr.Release()
	nonce := state.Nonce
	nonceArr := vgi.BuildInt64Array(1, func(int64) int64 { return nonce })
	defer nonceArr.Release()

	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{secretArr, nonceArr}, 1)
	if err := vgi.Emit(out, batch, vgi.WithCacheControl(&vgi.CacheControl{Ttl: vgi.Seconds(secretcache.TTLSeconds)})); err != nil {
		return err
	}
	state.Done = true
	return nil
}

// NewSecretCacheNonceFunction creates a SecretCacheNonceFunction wrapped for
// registration.
func NewSecretCacheNonceFunction() vgi.TableFunction {
	return vgi.AsTableFunction[secretCacheNonceState](&SecretCacheNonceFunction{})
}
