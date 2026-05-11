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

// slowCancellableArgs is the typed argument schema for slow_cancellable().
// probe_path is intentionally non-const (matches the pre-typed-args wire
// shape used by cancellation tests).
type slowCancellableArgs struct {
	ProbePath string `vgi:"pos=0,const=false,doc=Path to append to when on_cancel fires"`
	SleepMs   int64  `vgi:"default=50,doc=Sleep per batch (ms)"`
	Count     int64  `vgi:"default=1000000,doc=Total rows to produce"`
}

func (f *SlowCancellableFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(slowCancellableArgs{})
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
	var args slowCancellableArgs
	// probe_path is non-const so BindArgs will leave it zero; SleepMs/Count
	// come from defaults if absent.
	_ = vgi.BindArgs(params.Args, &args)
	if state.Emitted >= args.Count {
		return out.Finish()
	}
	if args.SleepMs > 0 {
		select {
		case <-time.After(time.Duration(args.SleepMs) * time.Millisecond):
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
