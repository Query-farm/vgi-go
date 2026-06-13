// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
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

// namedParamsEchoArgs is the typed argument schema for named_params_echo.
type namedParamsEchoArgs struct {
	Count      int64   `vgi:"pos=0,doc=Number of rows to generate"`
	Greeting   string  `vgi:"default=hello,doc=Greeting text echoed in output"`
	Multiplier int64   `vgi:"default=1,doc=Multiplier for value column"`
	Scale      float64 `vgi:"default=1.0,doc=Scale factor for float_value column"`
	Enabled    bool    `vgi:"default=true,doc=Boolean echoed in output"`
}

func (f *NamedParamsEchoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(namedParamsEchoArgs{})
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
	Greeting   string
	Multiplier int64
	Scale      float64
	Enabled    bool
}

func (f *NamedParamsEchoFunction) NewState(params *vgi.ProcessParams) (*namedParamsEchoState, error) {
	var args namedParamsEchoArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &namedParamsEchoState{
		BatchState: vgi.NewBatchState(args.Count, 1000),
		Greeting:   args.Greeting,
		Multiplier: args.Multiplier,
		Scale:      args.Scale,
		Enabled:    args.Enabled,
	}, nil
}

func (f *NamedParamsEchoFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *namedParamsEchoState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Index
		return []arrow.Array{
			vgi.BuildInt64Array(size, func(i int64) int64 { return start + i }),
			vgi.BuildStringArray(size, func(i int64) string { return state.Greeting }),
			vgi.BuildInt64Array(size, func(i int64) int64 { return (start + i) * state.Multiplier }),
			vgi.BuildFloat64Array(size, func(i int64) float64 { return float64(start+i) * state.Scale }),
			vgi.BuildBooleanArray(size, func(i int64) bool { return state.Enabled }),
		}, nil
	})
}

func NewNamedParamsEchoFunction() vgi.TableFunction {
	return vgi.AsTableFunction[namedParamsEchoState](&NamedParamsEchoFunction{})
}
