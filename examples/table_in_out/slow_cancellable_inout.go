// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table_in_out

import (
	"context"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// SlowCancellableInOutFunction is the table-in-out cancel-probe fixture:
// echoes its input table verbatim after sleeping per batch. Used by
// table_in_out/function_registration.test which only checks for the function's
// presence; the on_cancel probe-file path used by vgi-python is accepted as an
// argument but not exercised here (cancellation observability happens through
// DuckDB's regular pipeline teardown).
type SlowCancellableInOutFunction struct{}

var _ vgi.TypedTableInOutFunc[slowCancellableInOutState] = (*SlowCancellableInOutFunction)(nil)

func (f *SlowCancellableInOutFunction) Name() string { return "slow_cancellable_inout" }

func (f *SlowCancellableInOutFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Slow table-in-out with on_cancel probe (test fixture)",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"test"},
	}
}

func (f *SlowCancellableInOutFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "probe_path", Position: 0, ArrowType: "varchar", Doc: "Path to append to when on_cancel fires"},
		{Name: "data", Position: 1, ArrowType: "table", Doc: "Input table"},
		{Name: "sleep_ms", Position: -1, ArrowType: "int64", Doc: "Sleep per batch (ms)", HasDefault: true, DefaultValue: "50", IsConst: true},
	}
}

func (f *SlowCancellableInOutFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(params.InputSchema)
}

type slowCancellableInOutState struct {
	Processed int
}

func (f *SlowCancellableInOutFunction) NewState(params *vgi.ProcessParams) (*slowCancellableInOutState, error) {
	return &slowCancellableInOutState{}, nil
}

func (f *SlowCancellableInOutFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *slowCancellableInOutState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	sleepMs := vgi.OptionalInt64(params.Args, "sleep_ms", 50)
	if sleepMs > 0 {
		select {
		case <-time.After(time.Duration(sleepMs) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	state.Processed++
	return out.Emit(batch)
}

func (f *SlowCancellableInOutFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *slowCancellableInOutState) ([]arrow.RecordBatch, error) {
	return nil, nil
}

func NewSlowCancellableInOutFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[slowCancellableInOutState](&SlowCancellableInOutFunction{})
}
