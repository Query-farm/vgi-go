// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// SlowCancellableFunction is a slow producer used by cancellation tests. Each
// process tick emits a single row after sleeping. The pure-table variant of
// SlowCancellableInOutFunction; the function_registration smoke test only
// asserts the function's presence, so the on_cancel probe file is not wired.
type SlowCancellableFunction struct{}

var _ vgi.TypedTableFunc[slowCancellableState] = (*SlowCancellableFunction)(nil)

func (f *SlowCancellableFunction) Name() string { return "slow_cancellable" }

func (f *SlowCancellableFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Slow producer with an on_cancel file-writing probe (test fixture)",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"test"},
	}
}

func (f *SlowCancellableFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "probe_path", Position: 0, ArrowType: "varchar", Doc: "Path to append to when on_cancel fires"},
		{Name: "sleep_ms", Position: -1, ArrowType: "int64", Doc: "Sleep per batch (ms)", HasDefault: true, DefaultValue: "50", IsConst: true},
		{Name: "count", Position: -1, ArrowType: "int64", Doc: "Total rows to produce", HasDefault: true, DefaultValue: "1000000", IsConst: true},
	}
}

func (f *SlowCancellableFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

type slowCancellableState struct {
	Emitted int64
}

func (f *SlowCancellableFunction) NewState(params *vgi.ProcessParams) (*slowCancellableState, error) {
	return &slowCancellableState{}, nil
}

func (f *SlowCancellableFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *slowCancellableState, out *vgirpc.OutputCollector) error {
	count := vgi.OptionalInt64(params.Args, "count", 1_000_000)
	if state.Emitted >= count {
		return out.Finish()
	}
	sleepMs := vgi.OptionalInt64(params.Args, "sleep_ms", 50)
	if sleepMs > 0 {
		select {
		case <-time.After(time.Duration(sleepMs) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	arr := vgi.BuildInt64Array(1, func(i int64) int64 { return state.Emitted })
	defer arr.Release()
	state.Emitted++
	return out.EmitArrays([]arrow.Array{arr}, 1)
}

func NewSlowCancellableFunction() vgi.TableFunction {
	return vgi.AsTableFunction[slowCancellableState](&SlowCancellableFunction{})
}
