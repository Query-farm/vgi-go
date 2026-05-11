// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ExpressionFilterTestFunction generates rows for testing non-spatial
// expression-filter pushdown: (id, name='item_<i>', tags=['tag_<i%5>',
// 'tag_<(i+1)%5>'], score=i*1.1). Declares list_contains, starts_with,
// and contains as supported.
type ExpressionFilterTestFunction struct{}

var _ vgi.TypedTableFunc[expressionFilterState] = (*ExpressionFilterTestFunction)(nil)

func (*ExpressionFilterTestFunction) Name() string { return "expression_filter_test" }

func (*ExpressionFilterTestFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:                "Generates rows for non-spatial expression-filter pushdown tests",
		Stability:                  vgi.StabilityConsistent,
		ProjectionPushdown:         true,
		FilterPushdown:             true,
		AutoApplyFilters:           true,
		SupportedExpressionFilters: []string{"list_contains", "starts_with", "contains"},
		Categories:                 []string{"generator", "diagnostic", "testing"},
	}
}

// expressionFilterTestArgs is the typed argument schema for expression_filter_test().
type expressionFilterTestArgs struct {
	Count int64 `vgi:"pos=0,doc=Number of rows to generate"`
}

func (*ExpressionFilterTestFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(expressionFilterTestArgs{})
}

var expressionFilterTestSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "name", Type: arrow.BinaryTypes.String},
	{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
	{Name: "score", Type: arrow.PrimitiveTypes.Float64},
}, nil)

func (*ExpressionFilterTestFunction) OnBind(p *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(expressionFilterTestSchema)
}

func (*ExpressionFilterTestFunction) Cardinality(p *vgi.BindParams) (*vgi.TableCardinality, error) {
	var args expressionFilterTestArgs
	if err := vgi.BindArgs(p.Args, &args); err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: args.Count, Max: args.Count}, nil
}

type expressionFilterState struct {
	vgi.BatchState
}

func (*ExpressionFilterTestFunction) NewState(p *vgi.ProcessParams) (*expressionFilterState, error) {
	var args expressionFilterTestArgs
	if err := vgi.BindArgs(p.Args, &args); err != nil {
		return nil, err
	}
	return &expressionFilterState{BatchState: vgi.NewBatchState(args.Count, 2048)}, nil
}

func (*ExpressionFilterTestFunction) Process(ctx context.Context, p *vgi.ProcessParams, state *expressionFilterState, out *vgirpc.OutputCollector) error {
	projected := vgi.ProjectedColumns(p.ProjectionIDs, expressionFilterTestSchema)
	return vgi.GenerateBatchMap(&state.BatchState, out, p.OutputSchema, func(size int64) (map[string]arrow.Array, error) {
		start := state.Index
		colMap := make(map[string]arrow.Array)
		mem := memory.NewGoAllocator()
		if projected.Contains("id") {
			colMap["id"] = vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
		}
		if projected.Contains("name") {
			colMap["name"] = vgi.BuildStringArray(size, func(i int64) string { return fmt.Sprintf("item_%d", start+i) })
		}
		if projected.Contains("tags") {
			b := array.NewListBuilder(mem, arrow.BinaryTypes.String)
			defer b.Release()
			vb := b.ValueBuilder().(*array.StringBuilder)
			for i := int64(0); i < size; i++ {
				idx := start + i
				b.Append(true)
				vb.Append(fmt.Sprintf("tag_%d", idx%5))
				vb.Append(fmt.Sprintf("tag_%d", (idx+1)%5))
			}
			colMap["tags"] = b.NewArray()
		}
		if projected.Contains("score") {
			colMap["score"] = vgi.BuildFloat64Array(size, func(i int64) float64 { return float64(start+i) * 1.1 })
		}
		return colMap, nil
	})
}

func NewExpressionFilterTestFunction() vgi.TableFunction {
	return vgi.AsTableFunction[expressionFilterState](&ExpressionFilterTestFunction{})
}
