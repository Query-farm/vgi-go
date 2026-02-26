// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table_in_out

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// ExceptionFinalizeFunction raises an exception during finalize.
type ExceptionFinalizeFunction struct {
	SumAllColumnsFunction
}

func (f *ExceptionFinalizeFunction) Name() string { return "exception_finalize" }

func (f *ExceptionFinalizeFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Test function that raises exception during finalize",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *ExceptionFinalizeFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state interface{}) ([]arrow.RecordBatch, error) {
	return nil, fmt.Errorf("Intentional exception during finalize()")
}
