// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Cacheable same-name-in-two-schemas producer fixture.
//
// The result-cache member of the schema-disambiguation family (see
// examples/scalar/same_name.go, examples/table_in_out/same_name.go,
// examples/aggregate/same_name.go). Those probe *dispatch*; this one probes the
// *result cache*, a distinct layer.
//
// test_same_name_cached is a one-row producer table function that advertises
// vgi.cache.ttl and registers under one name in BOTH the `main` and `data`
// schemas of the `example` catalog. Each instance emits a single row tagged with
// its own schema name.
//
// The result cache keyed on catalog + auth + function name with no schema
// dimension, so the two implementations produced byte-identical cache keys and
// one schema's memoized rows cross-served the other — the caching-layer twin of
// the (schema, name) dispatch bug. The tag makes a cross-serve visible:
// example.data.test_same_name_cached() would return a `main` row. With the schema
// in the key, each schema gets its own entry (so vgi_result_cache() holds two rows
// for the one function name) and returns its own tag. Driven by
// ../vgi/test/sql/integration/cache/same_name_schemas.test.

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// sameNameCachedName is deliberately shared across the two schemas — the
// collision is the point.
const sameNameCachedName = "test_same_name_cached"

// sameNameCachedSchema is the single VARCHAR column every instance emits.
var sameNameCachedSchema = arrow.NewSchema([]arrow.Field{
	{Name: "tag", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

// SameNameCachedFunction emits one cacheable row tagged with the schema it was
// declared in. Two instances register under one name, in `main` and in `data`.
type SameNameCachedFunction struct {
	// schema is the catalog schema this instance is registered into — the tag it
	// stamps, so a mis-route or cross-serve is visible in the query result.
	schema string
}

var _ vgi.TypedTableFunc[cacheNonceState] = (*SameNameCachedFunction)(nil)

func (f *SameNameCachedFunction) Name() string { return sameNameCachedName }

func (f *SameNameCachedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Schema-disambiguation probe; the " + f.schema + "-schema cacheable producer",
		Categories:  []string{"generator", "cache", "testing"},
		Tags:        map[string]string{"category": "cache", "type": "same_name"},
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT * FROM example." + f.schema + ".test_same_name_cached()",
				Description: "One cacheable row tagged '" + f.schema + "'",
			},
		},
	}
}

func (f *SameNameCachedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(cacheNoArgs{})
}

func (f *SameNameCachedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(sameNameCachedSchema)
}

func (f *SameNameCachedFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *SameNameCachedFunction) NewState(params *vgi.ProcessParams) (*cacheNonceState, error) {
	return &cacheNonceState{}, nil
}

func (f *SameNameCachedFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *cacheNonceState, out *vgirpc.OutputCollector) error {
	if state.Done {
		return out.Finish()
	}
	tag := f.schema
	arr := vgi.BuildStringArray(1, func(int64) string { return tag })
	defer arr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, 1)
	if err := vgi.Emit(out, batch, vgi.WithCacheControl(&vgi.CacheControl{Ttl: vgi.Seconds(cacheDefaultTTL)})); err != nil {
		return err
	}
	state.Done = true
	return nil
}

// NewSameNameCachedFunction wraps the cacheable producer for registration into
// schemaName, which is also the tag it stamps.
func NewSameNameCachedFunction(schemaName string) vgi.TableFunction {
	return vgi.AsTableFunction[cacheNonceState](&SameNameCachedFunction{schema: schemaName})
}
