// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

var projectedDataFullSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "name", Type: arrow.BinaryTypes.String},
	{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	{Name: "extra", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// ProjectedDataFunction generates data with 4 columns, supporting projection pushdown.
type ProjectedDataFunction struct{}

var _ vgi.TypedTableFunc[projectedDataState] = (*ProjectedDataFunction)(nil)

func (f *ProjectedDataFunction) Name() string { return "projected_data" }

func (f *ProjectedDataFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Generates data with 4 columns, supporting projection pushdown",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
	}
}

// projectedDataArgs is the typed argument schema for projected_data().
type projectedDataArgs struct {
	Count int64 `vgi:"pos=0,doc=Number of rows to generate"`
}

func (f *ProjectedDataFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(projectedDataArgs{})
}

func (f *ProjectedDataFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(projectedDataFullSchema)
}

func (f *ProjectedDataFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	var args projectedDataArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: args.Count, Max: args.Count}, nil
}

type projectedDataState struct {
	vgi.BatchState
}

const projectedDataBatchSize = 1000

func (f *ProjectedDataFunction) NewState(params *vgi.ProcessParams) (*projectedDataState, error) {
	var args projectedDataArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &projectedDataState{
		BatchState: vgi.NewBatchState(args.Count, projectedDataBatchSize),
	}, nil
}

func (f *ProjectedDataFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *projectedDataState, out *vgirpc.OutputCollector) error {
	projected := vgi.ProjectedColumns(params.ProjectionIDs, projectedDataFullSchema)
	return vgi.GenerateBatchMap(&state.BatchState, out, params.OutputSchema, func(size int64) (map[string]arrow.Array, error) {
		start := state.Index
		colMap := make(map[string]arrow.Array)
		if projected.Contains("id") {
			colMap["id"] = vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
		}
		if projected.Contains("name") {
			colMap["name"] = vgi.BuildStringArray(size, func(i int64) string { return fmt.Sprintf("item_%d", start+i) })
		}
		if projected.Contains("value") {
			colMap["value"] = vgi.BuildFloat64Array(size, func(i int64) float64 { return float64(start+i) * 1.5 })
		}
		if projected.Contains("extra") {
			colMap["extra"] = vgi.BuildInt64Array(size, func(i int64) int64 { v := start + i; return v * v })
		}
		return colMap, nil
	})
}

func NewProjectedDataFunction() vgi.TableFunction {
	return vgi.AsTableFunction[projectedDataState](&ProjectedDataFunction{})
}
