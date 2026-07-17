// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table_in_out

import (
	"github.com/Query-farm/vgi-go/vgi"
)

// DistributedSumFunction is a distributed column-wise sum — a *global*
// reduction over all input.
//
// Global combine (every input row contributes to a single output row) is a
// buffered table function, NOT a streaming table-in-out one: a streaming
// table-in-out is a per-substream map, and under per-substream worker fan-out
// each worker sees only its own substream's rows, so a streaming finalize that
// merges across substreams would produce a partial. This fixture used to
// demonstrate that (now-invalid) streaming-distributed finalize API; it has
// been migrated to the buffered Sink+Combine+Source model, which coordinates
// cross-worker state through execution-scoped storage (the correct home for a
// full-stream reduction). Mirrors vgi-python's SumAllColumnsSimpleDistributed.
//
// Behaviourally identical to SumAllColumnsFunction (kept as a distinct named
// fixture so its integration tests keep exercising the buffered path under its
// own name): Process appends this batch's partial sums to an append-only
// per-execution log; Combine reduces the log to one merged row; Finalize emits
// it.
type DistributedSumFunction struct {
	SumAllColumnsFunction
}

var _ vgi.TableBufferingFunction = (*DistributedSumFunction)(nil)
var _ vgi.TableBufferingFunctionWithCardinality = (*DistributedSumFunction)(nil)

func (f *DistributedSumFunction) Name() string { return "sum_all_columns_simple_distributed" }

func (f *DistributedSumFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Distributed sum using the buffered (Sink+Combine+Source) model",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"aggregation", "numeric", "distributed"},
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT * FROM sum_all_columns_simple_distributed((SELECT * FROM input_table))",
				Description: "Sum columns across buffered workers",
			},
		},
	}
}

func (f *DistributedSumFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"},
	}
}
