// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// TenThousandFunction generates 10000 integers from 0 to 9999.
type TenThousandFunction struct{}

var _ vgi.TypedTableFunc[tenThousandState] = (*TenThousandFunction)(nil)

func (f *TenThousandFunction) Name() string { return "ten_thousand" }

func (f *TenThousandFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generates 10000 integers from 0 to 9999",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *TenThousandFunction) ArgumentSpecs() []vgi.ArgSpec {
	return nil
}

func (f *TenThousandFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

func (f *TenThousandFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	return &vgi.TableCardinality{Estimate: 10000, Max: 10000}, nil
}

type tenThousandState struct {
	vgi.BatchState
}

func (f *TenThousandFunction) NewState(params *vgi.ProcessParams) (*tenThousandState, error) {
	return &tenThousandState{
		BatchState: vgi.NewBatchState(10000, 1000),
	}, nil
}

func (f *TenThousandFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *tenThousandState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Index
		return []arrow.Array{
			vgi.BuildInt64Array(size, func(i int64) int64 { return start + i }),
		}, nil
	})
}

func NewTenThousandFunction() vgi.TableFunction {
	return vgi.AsTableFunction[tenThousandState](&TenThousandFunction{})
}
