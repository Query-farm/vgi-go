// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
)

// ScalarFunction is the interface for scalar VGI functions.
// Scalar functions perform 1:1 row mapping: each input batch produces
// one output batch with the same number of rows.
type ScalarFunction interface {
	// Name returns the function name used in SQL.
	Name() string
	// Metadata returns descriptive metadata.
	Metadata() FunctionMetadata
	// ArgumentSpecs returns the function's argument specifications.
	ArgumentSpecs() []ArgSpec
	// OnBind resolves the output schema given the bind parameters.
	OnBind(params *BindParams) (*BindResponse, error)
	// Process transforms an input batch into an output batch.
	// The output batch must have the same number of rows as the input.
	Process(ctx context.Context, params *ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error)
}
