// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
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

// sequenceArgs is the typed argument schema for sequence().
type sequenceArgs struct {
	Count     int64 `vgi:"pos=0,doc=Number of integers to generate"`
	BatchSize int64 `vgi:"default=2048,doc=Batch size for output"`
	Increment int64 `vgi:"default=1,doc=Step between values"`
}

func (f *SequenceFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(sequenceArgs{})
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
	var args sequenceArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil || args.Count <= 0 {
		return nil, nil
	}
	maxValue := (args.Count - 1) * args.Increment
	return []vgi.ColumnStatistics{{
		ColumnName:    "n",
		Type:          arrow.PrimitiveTypes.Int64,
		Min:           int64(0),
		Max:           maxValue,
		HasNull:       false,
		HasNotNull:    true,
		DistinctCount: args.Count,
	}}, nil
}

type sequenceState struct {
	vgi.BatchState
	Increment int64
}

func (f *SequenceFunction) NewState(params *vgi.ProcessParams) (*sequenceState, error) {
	var args sequenceArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &sequenceState{
		BatchState: vgi.NewBatchState(args.Count, args.BatchSize),
		Increment:  args.Increment,
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
