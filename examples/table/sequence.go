// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// SequenceFunction generates a sequence of integers from 0 to n-1.
type SequenceFunction struct{}

var _ vgi.TypedTableFunc[sequenceState] = (*SequenceFunction)(nil)

func (f *SequenceFunction) Name() string { return "sequence" }

func (f *SequenceFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Generates a sequence of integers from 0 to n-1",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
		Categories:         []string{"generator", "utility"},
	}
}

func (f *SequenceFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of integers to generate", IsConst: true},
		{Name: "batch_size", Position: -1, ArrowType: "int64", Doc: "Batch size for output", HasDefault: true, DefaultValue: "1000", IsConst: true},
		{Name: "increment", Position: -1, ArrowType: "int64", Doc: "Step between values", HasDefault: true, DefaultValue: "1", IsConst: true},
	}
}

var sequenceOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
}, nil)

func (f *SequenceFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	// Validate at bind time so bad arguments surface as a clean
	// ArgumentValidationError rather than a stream-truncation IO error.
	if params.Args != nil {
		// count: required, non-null
		if len(params.Args.Positional) > 0 {
			if a := params.Args.Positional[0]; a != nil && a.Len() > 0 && a.IsNull(0) {
				return nil, fmt.Errorf("count cannot be NULL")
			}
		}
		if c, err := params.Args.GetColumn("batch_size"); err == nil && c.Len() > 0 {
			if c.IsNull(0) {
				return nil, fmt.Errorf("batch_size cannot be NULL")
			}
			if bs, err := params.Args.GetScalarInt64("batch_size"); err == nil && bs < 1 {
				return nil, fmt.Errorf("batch_size must be >= 1 (got %d)", bs)
			}
		}
		if c, err := params.Args.GetColumn("increment"); err == nil && c.Len() > 0 {
			if c.IsNull(0) {
				return nil, fmt.Errorf("increment cannot be NULL")
			}
			if inc, err := params.Args.GetScalarInt64("increment"); err == nil && inc < 1 {
				return nil, fmt.Errorf("increment must be >= 1 (got %d)", inc)
			}
		}
	}
	return vgi.BindSchema(sequenceOutputSchema)
}

func (f *SequenceFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

func (f *SequenceFunction) Statistics(params *vgi.BindParams) ([]vgi.ColumnStatistics, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil || count <= 0 {
		return nil, nil
	}
	increment := vgi.OptionalInt64(params.Args, "increment", 1)
	maxValue := (count - 1) * increment
	return []vgi.ColumnStatistics{{
		ColumnName:    "n",
		Type:          arrow.PrimitiveTypes.Int64,
		Min:           int64(0),
		Max:           maxValue,
		HasNull:       false,
		HasNotNull:    true,
		DistinctCount: count,
	}}, nil
}

type sequenceState struct {
	vgi.BatchState
	Increment int64
}

func (f *SequenceFunction) NewState(params *vgi.ProcessParams) (*sequenceState, error) {
	count, _ := params.Args.GetScalarInt64(0)
	return &sequenceState{
		BatchState: vgi.NewBatchState(count, vgi.OptionalInt64(params.Args, "batch_size", 1000)),
		Increment:  vgi.OptionalInt64(params.Args, "increment", 1),
	}, nil
}

func (f *SequenceFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *sequenceState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Index
		return []arrow.Array{
			vgi.BuildInt64Array(size, func(i int64) int64 { return (start + i) * state.Increment }),
		}, nil
	})
}

func NewSequenceFunction() vgi.TableFunction {
	return vgi.AsTableFunction[sequenceState](&SequenceFunction{})
}
