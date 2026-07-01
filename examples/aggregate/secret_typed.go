// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package aggregate

import (
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ============================================================================
// secret_typed_sum — aggregate whose result type is chosen from a secret.
//
// The vgi_example secret is declared as a static RequiredSecret, so the
// extension pre-resolves it and delivers it on the aggregate bind request.
// OnBind reads the secret's use_ssl value: true -> DOUBLE result, false ->
// BIGINT. The sum itself is computed as a normal per-group int64 sum, then cast
// to the bound type in Finalize. ReturnType is omitted from Metadata so the
// function advertises ANY (the concrete type is resolved at bind time).
// ============================================================================

type SecretTypedSumFunction struct{}

var _ vgi.AggregateFunction = (*SecretTypedSumFunction)(nil)

func (SecretTypedSumFunction) Name() string { return "secret_typed_sum" }

func (SecretTypedSumFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Sum an integer column; the result type is chosen from a secret",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingDefault,
		Categories:        []string{"aggregate", "secret"},
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
		RequiredSecrets: []vgi.SecretRequirement{
			{SecretType: "vgi_example"},
		},
	}
}

// secretTypedSumArgs is the typed argument schema for secret_typed_sum().
type secretTypedSumArgs struct {
	Value int64 `vgi:"pos=0,const=false,doc=Integer column to sum"`
}

func (SecretTypedSumFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(secretTypedSumArgs{})
}

func (SecretTypedSumFunction) OnBind(p *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	// Read use_ssl from the statically-resolved vgi_example secret: true selects
	// a DOUBLE result type, false (or absent) selects BIGINT.
	asDouble := false
	if matches := vgi.Secrets(p.Secrets).OfType("vgi_example"); len(matches) > 0 {
		if v, ok := matches[0]["use_ssl"]; ok {
			if b, isBool := v.(bool); isBool {
				asDouble = b
			}
		}
	}
	resultType := arrow.DataType(arrow.PrimitiveTypes.Int64)
	if asDouble {
		resultType = arrow.PrimitiveTypes.Float64
	}
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: resultType},
	}, nil))
}

func (SecretTypedSumFunction) NewState(*vgi.AggregateProcessParams) interface{} { return &SumState{} }

func (SecretTypedSumFunction) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, _ *vgi.AggregateProcessParams) error {
	if len(columns) == 0 {
		return fmt.Errorf("secret_typed_sum: missing value column")
	}
	col, ok := columns[0].(*array.Int64)
	if !ok {
		return fmt.Errorf("secret_typed_sum: value column is %T, expected int64", columns[0])
	}
	n := gids.Len()
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			continue
		}
		s := vgi.EnsureState(states, gids.At(i), func() *SumState { return &SumState{} })
		s.Total += col.Value(i)
	}
	return nil
}

func (SecretTypedSumFunction) Combine(source, target interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	s := source.(*SumState)
	t := target.(*SumState)
	return &SumState{Total: s.Total + t.Total}, nil
}

func (SecretTypedSumFunction) Finalize(gids []int64, states map[int64]interface{}, p *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	dt := p.OutputSchema.Field(0).Type
	switch dt.ID() {
	case arrow.FLOAT64:
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		for _, gid := range gids {
			if s, ok := states[gid].(*SumState); ok {
				b.Append(float64(s.Total))
			} else {
				b.AppendNull()
			}
		}
		col := b.NewArray()
		defer col.Release()
		return array.NewRecordBatch(p.OutputSchema, []arrow.Array{col}, int64(len(gids))), nil
	case arrow.INT64:
		b := array.NewInt64Builder(mem)
		defer b.Release()
		for _, gid := range gids {
			if s, ok := states[gid].(*SumState); ok {
				b.Append(s.Total)
			} else {
				b.AppendNull()
			}
		}
		col := b.NewArray()
		defer col.Release()
		return array.NewRecordBatch(p.OutputSchema, []arrow.Array{col}, int64(len(gids))), nil
	}
	return nil, fmt.Errorf("secret_typed_sum: unsupported output type %s", dt)
}
