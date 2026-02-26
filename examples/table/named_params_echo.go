// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// NamedParamsEchoFunction echoes named parameter values in output columns.
// Designed for testing that DuckDB correctly passes various parameter types
// (VARCHAR, BIGINT, DOUBLE, BOOLEAN) to the worker.
type NamedParamsEchoFunction struct{}

var _ vgi.TypedTableFunc[namedParamsEchoState] = (*NamedParamsEchoFunction)(nil)

func (f *NamedParamsEchoFunction) Name() string { return "named_params_echo" }

func (f *NamedParamsEchoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Echoes named parameter values in output columns",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"generator", "testing"},
	}
}

func (f *NamedParamsEchoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of rows to generate", IsConst: true},
		{Name: "greeting", Position: -1, ArrowType: "varchar", Doc: "Greeting text echoed in output", HasDefault: true, DefaultValue: "hello", IsConst: true},
		{Name: "multiplier", Position: -1, ArrowType: "int64", Doc: "Multiplier for value column", HasDefault: true, DefaultValue: "1", IsConst: true},
		{Name: "scale", Position: -1, ArrowType: "double", Doc: "Scale factor for float_value column", HasDefault: true, DefaultValue: "1.0", IsConst: true},
		{Name: "enabled", Position: -1, ArrowType: "bool", Doc: "Boolean echoed in output", HasDefault: true, DefaultValue: "true", IsConst: true},
	}
}

var namedParamsEchoOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "greeting", Type: arrow.BinaryTypes.String},
	{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	{Name: "float_value", Type: arrow.PrimitiveTypes.Float64},
	{Name: "enabled", Type: &arrow.BooleanType{}},
}, nil)

func (f *NamedParamsEchoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(namedParamsEchoOutputSchema)
}

func (f *NamedParamsEchoFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type namedParamsEchoState struct {
	vgi.BatchState
	greeting   string
	multiplier int64
	scale      float64
	enabled    bool
}

func (f *NamedParamsEchoFunction) NewState(params *vgi.ProcessParams) (*namedParamsEchoState, error) {
	count, _ := params.Args.GetScalarInt64(0)
	return &namedParamsEchoState{
		BatchState: vgi.NewBatchState(count, 1000),
		greeting:   vgi.OptionalString(params.Args, "greeting", "hello"),
		multiplier: vgi.OptionalInt64(params.Args, "multiplier", 1),
		scale:      vgi.OptionalFloat64(params.Args, "scale", 1.0),
		enabled:    vgi.OptionalBool(params.Args, "enabled", true),
	}, nil
}

func (f *NamedParamsEchoFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *namedParamsEchoState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Index
		return []arrow.Array{
			vgi.BuildInt64Array(size, func(i int64) int64 { return start + i }),
			vgi.BuildStringArray(size, func(i int64) string { return state.greeting }),
			vgi.BuildInt64Array(size, func(i int64) int64 { return (start + i) * state.multiplier }),
			vgi.BuildFloat64Array(size, func(i int64) float64 { return float64(start+i) * state.scale }),
			vgi.BuildBooleanArray(size, func(i int64) bool { return state.enabled }),
		}, nil
	})
}

func NewNamedParamsEchoFunction() vgi.TableFunction {
	return vgi.AsTableFunction[namedParamsEchoState](&NamedParamsEchoFunction{})
}
