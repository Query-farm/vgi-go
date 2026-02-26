// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table_in_out

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// ExceptionProcessFunction raises an exception on the second batch.
type ExceptionProcessFunction struct {
	SumAllColumnsFunction
}

func (f *ExceptionProcessFunction) Name() string { return "exception_process" }

func (f *ExceptionProcessFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Test function that raises exception during process",
		Stability:   vgi.StabilityConsistent,
	}
}

type exceptionProcessState struct {
	batchCount int
}

func (f *ExceptionProcessFunction) NewState(params *vgi.ProcessParams) (interface{}, error) {
	return &exceptionProcessState{batchCount: 0}, nil
}

func (f *ExceptionProcessFunction) Process(ctx context.Context, params *vgi.ProcessParams, state interface{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	s := state.(*exceptionProcessState)
	s.batchCount++
	if s.batchCount%2 == 0 {
		return fmt.Errorf("Intentional exception on batch %d", s.batchCount)
	}
	return out.Emit(vgi.EmptyBatch(params.OutputSchema))
}
