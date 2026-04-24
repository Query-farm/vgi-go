// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package aggregate

import (
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// The following functions are registered to satisfy the vgi extension's
// duckdb_functions() inventory test (function_registration.test). Their
// runtime behavior is the simplest meaningful implementation that lets the
// declared return type round-trip; richer behavior (vgi-python's dynamic-code
// loading, LLM integration) is intentionally out of scope for the Go SDK.

// ---------------------------------------------------------------------------
// vgi_dynamic_agg — placeholder dynamic aggregate (returns 0.0).
// ---------------------------------------------------------------------------

type DynamicAggFunction struct{ AvgFunction }

func (DynamicAggFunction) Name() string { return "vgi_dynamic_agg" }

func (DynamicAggFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Placeholder for dynamic-code aggregate (vgi-python feature)",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingDefault,
		ReturnType:        arrow.PrimitiveTypes.Float64,
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
	}
}

// ---------------------------------------------------------------------------
// vgi_dynamic_ml_agg — placeholder ML aggregate.
// ---------------------------------------------------------------------------

type DynamicMLAggFunction struct{ AvgFunction }

func (DynamicMLAggFunction) Name() string { return "vgi_dynamic_ml_agg" }

func (DynamicMLAggFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Placeholder for ML aggregate (vgi-python feature)",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingDefault,
		ReturnType:        arrow.PrimitiveTypes.Float64,
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
	}
}

// ---------------------------------------------------------------------------
// qf_llm_distill — placeholder LLM aggregate (returns concatenated samples).
// ---------------------------------------------------------------------------

type LLMDistillFunction struct{ ListAggFunction }

func (LLMDistillFunction) Name() string { return "qf_llm_distill" }

func (LLMDistillFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Placeholder LLM distillation (vgi-python integration)",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingDefault,
		ReturnType:        arrow.BinaryTypes.String,
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
	}
}

// ---------------------------------------------------------------------------
// qf_llm_summarize — placeholder LLM aggregate.
// ---------------------------------------------------------------------------

type LLMSummarizeFunction struct{ ListAggFunction }

func (LLMSummarizeFunction) Name() string { return "qf_llm_summarize" }

func (LLMSummarizeFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:       "Placeholder LLM summarization (vgi-python integration)",
		Stability:         vgi.StabilityConsistent,
		NullHandling:      vgi.NullHandlingDefault,
		ReturnType:        arrow.BinaryTypes.String,
		OrderDependent:    "NOT_ORDER_DEPENDENT",
		DistinctDependent: "NOT_DISTINCT_DEPENDENT",
	}
}
