// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Package aggregate registers example aggregate functions on a worker.
package aggregate

import (
	"encoding/gob"

	"github.com/Query-farm/vgi-go/vgi"
)

func init() {
	// gob requires concrete types be registered so the encoder can persist
	// them through interface{} state slots.
	gob.Register(&stubState{})
	gob.Register(&CountState{})
	gob.Register(&SumState{})
	gob.Register(&AvgState{})
	gob.Register(&WeightedSumState{})
	gob.Register(&ListAggState{})
	gob.Register(&PercentileState{})
	gob.Register(&SumAllState{})
	gob.Register(&GenericSumState{})
}

// RegisterAll registers every example aggregate on the worker.
func RegisterAll(w *vgi.Worker) {
	w.RegisterAggregate(&CountFunction{})
	w.RegisterAggregate(&SumFunction{})
	w.RegisterAggregate(&AvgFunction{})
	w.RegisterAggregate(&WeightedSumFunction{})
	w.RegisterAggregate(&ListAggFunction{})
	w.RegisterAggregate(&PercentileFunction{})
	w.RegisterAggregate(&SumAllFunction{})
	w.RegisterAggregate(&GenericSumFunction{})
	w.RegisterAggregate(&DynamicAggFunction{})
	w.RegisterAggregate(&DynamicMLAggFunction{})
	w.RegisterAggregate(&LLMDistillFunction{})
	w.RegisterAggregate(&LLMSummarizeFunction{})
	w.RegisterAggregate(&WindowSumFunction{})
	w.RegisterAggregate(&WindowMedianFunction{})
	w.RegisterAggregate(&WindowListAggFunction{})
}
