// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// QuerySeedFunction adds a per-query-stable seed to each input value.
//
// It is the only example fixture that emits
// vgi.StabilityConsistentWithinQuery, so the wire path for the third
// stability variant stays exercised. Semantically the seed is fixed for the
// duration of a single query but may differ across queries (like now()); the
// offset is a constant here so SQL tests have a stable expected output — the
// stability flag is what is under test, not the numeric result.
type QuerySeedFunction struct{}

type querySeedArgs struct {
	Value *array.Int64 `vgi:"pos=0,const=false,doc=Value to offset"`
}

func (*QuerySeedFunction) Name() string { return "query_seed" }

func (*QuerySeedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Add a per-query-stable seed to each value (demonstrates CONSISTENT_WITHIN_QUERY stability)",
		Stability:   vgi.StabilityConsistentWithinQuery,
		ReturnType:  arrow.PrimitiveTypes.Int64,
	}
}

func (*QuerySeedFunction) OnBindTyped(_ *querySeedArgs, _ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.PrimitiveTypes.Int64)
}

func (*QuerySeedFunction) ProcessTyped(_ context.Context, args *querySeedArgs, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.GenerateColumn(params, batch, array.NewInt64Builder,
		func(i int) int64 {
			return args.Value.Value(i) + 1000
		})
}

// NewQuerySeed returns the registration-ready ScalarFunction.
func NewQuerySeed() vgi.ScalarFunction {
	return vgi.AsScalarFunction[querySeedArgs](&QuerySeedFunction{})
}
