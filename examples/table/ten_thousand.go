// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TenThousandFunction generates 10000 integers from 0 to 9999.
type TenThousandFunction struct{}

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
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}, nil
}

func (f *TenThousandFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}

func (f *TenThousandFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	return &vgi.TableCardinality{Estimate: 10000, Max: 10000}, nil
}

type tenThousandState struct {
	start int64
}

func (f *TenThousandFunction) NewState(params *vgi.ProcessParams) (interface{}, error) {
	return &tenThousandState{start: 0}, nil
}

func (f *TenThousandFunction) Process(ctx context.Context, params *vgi.ProcessParams, state interface{}, out *vgirpc.OutputCollector) error {
	s := state.(*tenThousandState)
	if s.start >= 10000 {
		return out.Finish()
	}

	end := s.start + 1000
	if end > 10000 {
		end = 10000
	}

	mem := memory.NewGoAllocator()
	builder := array.NewInt64Builder(mem)
	defer builder.Release()

	for i := s.start; i < end; i++ {
		builder.Append(i)
	}

	arr := builder.NewArray()
	defer arr.Release()

	size := end - s.start
	s.start = end
	return out.EmitArrays([]arrow.Array{arr}, size)
}
