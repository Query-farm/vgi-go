// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package aggregate

import (
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Same-name-in-two-schemas *aggregate* fixtures.
//
// The third member of the schema-disambiguation family, after
// examples/scalar/same_name.go (scalar) and
// examples/table_in_out/same_name.go (table-in-out + buffering).
//
// Aggregates are the widest surface of the three. Every aggregate RPC —
// update / combine / finalize / destructor, the four window calls and the three
// streaming calls — is a stateless unary request with no bound connection, so
// each one re-resolves the function through the worker's single by-name entry
// point. Before protocol 1.2.0 none of those requests carried a schema, not even
// aggregate_bind, so an aggregate name declared in two schemas resolved to
// whichever the by-name lookup found first.
//
// Both instances register under test_same_name_agg, in the `main` and `data`
// schemas of the `example` catalog. Each returns a VARCHAR tagged with its own
// schema AND the aggregated value, so a mis-routed call reads as the wrong tag
// rather than a plausible answer — and a call that mis-routes only partway
// (bind to one implementation, update/finalize to another) still shows up,
// because the tag is stamped at finalize while accumulation happens in update.
//
// Driven by ../vgi/test/sql/integration/aggregate/same_name_schemas.test.

// sameNameAggName is deliberately shared across the two schemas.
const sameNameAggName = "test_same_name_agg"

// SameNameAggState is the running total for one group.
type SameNameAggState struct {
	Total int64
}

// SameNameAggFunction sums its input and tags the result with the schema it was
// declared in.
type SameNameAggFunction struct {
	// schema is the catalog schema this instance is registered into — the tag
	// it stamps, so a mis-route is visible in the query result.
	schema string
}

var _ vgi.AggregateFunction = (*SameNameAggFunction)(nil)

func (f *SameNameAggFunction) Name() string { return sameNameAggName }

func (f *SameNameAggFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Schema-disambiguation probe; the " + f.schema + "-schema aggregate",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingSpecial,
		ReturnType:        arrow.BinaryTypes.String,
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
		Categories:        []string{"test", "schema"},
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT example." + f.schema + ".test_same_name_agg(n) FROM range(3) t(n)",
				Description: "Returns '" + f.schema + ":3'",
			},
		},
	}
}

// sameNameAggArgs is the typed argument schema for test_same_name_agg().
type sameNameAggArgs struct {
	Value int64 `vgi:"pos=0,const=false,doc=Integer value to accumulate"`
}

func (f *SameNameAggFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(sameNameAggArgs{})
}

func (f *SameNameAggFunction) OnBind(p *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.BinaryTypes.String},
	}, nil))
}

func (f *SameNameAggFunction) NewState(*vgi.AggregateProcessParams) interface{} {
	return &SameNameAggState{}
}

func (f *SameNameAggFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, _ *vgi.AggregateProcessParams) error {
	if len(columns) == 0 {
		return fmt.Errorf("%s: missing value column", sameNameAggName)
	}
	col, ok := columns[0].(*array.Int64)
	if !ok {
		return fmt.Errorf("%s: value column is %T, expected int64", sameNameAggName, columns[0])
	}
	n := gids.Len()
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			continue
		}
		s := vgi.EnsureState(states, gids.At(i), func() *SameNameAggState { return &SameNameAggState{} })
		s.Total += col.Value(i)
	}
	return nil
}

func (f *SameNameAggFunction) Combine(source, target interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	s := source.(*SameNameAggState)
	t := target.(*SameNameAggState)
	return &SameNameAggState{Total: s.Total + t.Total}, nil
}

func (f *SameNameAggFunction) Finalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	b := array.NewStringBuilder(memory.NewGoAllocator())
	defer b.Release()
	b.Reserve(len(gids))
	for _, gid := range gids {
		total := int64(0)
		if s, ok := states[gid].(*SameNameAggState); ok {
			total = s.Total
		}
		b.Append(fmt.Sprintf("%s:%d", f.schema, total))
	}
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(p.OutputSchema, []arrow.Array{col}, int64(len(gids))), nil
}

// NewSameNameAggFunction builds the aggregate probe for registration into
// schemaName, which is also the tag it stamps.
func NewSameNameAggFunction(schemaName string) vgi.AggregateFunction {
	return &SameNameAggFunction{schema: schemaName}
}
