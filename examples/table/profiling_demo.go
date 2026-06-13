// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// ProfilingDemoFunction is a sequence(count) clone whose dynamic_to_string
// hook surfaces rows_produced / batches_emitted / elapsed_ms in EXPLAIN
// ANALYZE output. Per-tick snapshots are persisted to the cross-process
// SQLite-backed ExecutionStorage so the C++ extension's per-thread
// dynamic_to_string call (which lands on a different worker subprocess
// under the multi-conn sync-init transport) can read them.
type ProfilingDemoFunction struct{}

var _ vgi.TypedTableFunc[profilingDemoState] = (*ProfilingDemoFunction)(nil)

func (f *ProfilingDemoFunction) Name() string { return "profiling_demo" }

func (f *ProfilingDemoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Sequence generator publishing diagnostics under EXPLAIN ANALYZE",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"generator", "utility"},
	}
}

// profilingDemoArgs is the typed argument schema for profiling_demo().
type profilingDemoArgs struct {
	Count     int64 `vgi:"pos=0,doc=Number of rows to generate"`
	BatchSize int64 `vgi:"default=1000,doc=Rows per batch"`
	Increment int64 `vgi:"default=1,doc=Increment between values"`
}

func (f *ProfilingDemoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(profilingDemoArgs{})
}

func (f *ProfilingDemoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

func (f *ProfilingDemoFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, _ := params.Args.GetScalarInt64(0)
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type profilingDemoState struct {
	vgi.BatchState
	Increment      int64
	StartedNs      int64
	RowsProduced   int64
	BatchesEmitted int64
}

// packSnapshot encodes (rows, batches, started_ns) as 24 bytes for storage.
// DynamicToString decodes one row per worker pid via Snapshot().
func packSnapshot(rows, batches, startedNs int64) []byte {
	b := make([]byte, 24)
	binary.LittleEndian.PutUint64(b[0:8], uint64(rows))
	binary.LittleEndian.PutUint64(b[8:16], uint64(batches))
	binary.LittleEndian.PutUint64(b[16:24], uint64(startedNs))
	return b
}

func unpackSnapshot(b []byte) (rows, batches, startedNs int64) {
	if len(b) < 24 {
		return 0, 0, 0
	}
	rows = int64(binary.LittleEndian.Uint64(b[0:8]))
	batches = int64(binary.LittleEndian.Uint64(b[8:16]))
	startedNs = int64(binary.LittleEndian.Uint64(b[16:24]))
	return
}

func (f *ProfilingDemoFunction) NewState(params *vgi.ProcessParams) (*profilingDemoState, error) {
	var args profilingDemoArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &profilingDemoState{
		BatchState: vgi.NewBatchState(args.Count, args.BatchSize),
		Increment:  args.Increment,
		StartedNs:  time.Now().UnixNano(),
	}, nil
}

func (f *ProfilingDemoFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *profilingDemoState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Index
		state.RowsProduced += size
		state.BatchesEmitted++
		// Persist this worker's per-tick snapshot under the shared execution
		// ID so a different process can read it from dynamic_to_string.
		if params.Storage != nil {
			_ = params.Storage.Put(packSnapshot(state.RowsProduced, state.BatchesEmitted, state.StartedNs))
		}
		return []arrow.Array{
			vgi.BuildInt64Array(size, func(i int64) int64 { return (start + i) * state.Increment }),
		}, nil
	})
}

// DynamicToString aggregates every worker's per-pid snapshot into the final
// Extra Info shown under EXPLAIN ANALYZE.
func (f *ProfilingDemoFunction) DynamicToString(ctx context.Context, params *vgi.DynamicToStringParams) ([]string, []string, error) {
	if params.Storage == nil {
		return nil, nil, nil
	}
	snapshots, err := params.Storage.Snapshot()
	if err != nil {
		return nil, nil, nil
	}
	var rows, batches int64
	var earliestStart int64
	for _, blob := range snapshots {
		r, b, started := unpackSnapshot(blob)
		rows += r
		batches += b
		if earliestStart == 0 || started < earliestStart {
			earliestStart = started
		}
	}
	elapsedMs := 0.0
	if earliestStart > 0 {
		elapsedMs = float64(time.Now().UnixNano()-earliestStart) / 1e6
	}
	keys := []string{"rows_produced", "batches_emitted", "elapsed_ms"}
	values := []string{
		fmt.Sprintf("%d", rows),
		fmt.Sprintf("%d", batches),
		fmt.Sprintf("%.2f", elapsedMs),
	}
	return keys, values, nil
}

func NewProfilingDemoFunction() vgi.TableFunction {
	return vgi.AsTableFunction[profilingDemoState](&ProfilingDemoFunction{})
}
