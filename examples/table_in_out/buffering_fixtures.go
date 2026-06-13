// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table_in_out

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"syscall"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ---------------------------------------------------------------------------
// Crash / hang fixtures — thin overrides of buffer_input. Used by the
// table_buffering_*_crash and worker-crash tests.
// ---------------------------------------------------------------------------

// CrashOnProcessFunction SIGKILLs its own worker during process.
type CrashOnProcessFunction struct{ BufferInputFunction }

func (f *CrashOnProcessFunction) Name() string { return "crash_on_process" }
func (f *CrashOnProcessFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "Worker SIGKILLs itself during process (test)", Categories: []string{"test", "crash"}}
}
func (f *CrashOnProcessFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) ([]byte, error) {
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	return params.ExecutionID, nil // unreachable
}

// CrashOnCombineFunction buffers normally but raises during combine.
type CrashOnCombineFunction struct{ BufferInputFunction }

func (f *CrashOnCombineFunction) Name() string { return "crash_on_combine" }
func (f *CrashOnCombineFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "Worker raises during combine (test)", Categories: []string{"test", "crash"}}
}
func (f *CrashOnCombineFunction) Combine(ctx context.Context, params *vgi.ProcessParams, stateIDs [][]byte) ([][]byte, error) {
	return nil, fmt.Errorf("Intentional exception during combine()")
}

// CrashOnFinalizeFunction raises during finalize.
type CrashOnFinalizeFunction struct{ BufferInputFunction }

func (f *CrashOnFinalizeFunction) Name() string { return "crash_on_finalize" }
func (f *CrashOnFinalizeFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "Worker raises during finalize (test)", Categories: []string{"test", "crash"}}
}
func (f *CrashOnFinalizeFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, finalizeStateID []byte) ([]arrow.RecordBatch, error) {
	return nil, fmt.Errorf("Intentional exception during finalize()")
}

// HangOnProcessFunction sleeps for an hour in process (manual cancel smoke).
type HangOnProcessFunction struct{ BufferInputFunction }

func (f *HangOnProcessFunction) Name() string { return "hang_on_process" }
func (f *HangOnProcessFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "Worker sleeps in process (manual cancel test)", Categories: []string{"test", "hang"}}
}
func (f *HangOnProcessFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// ---------------------------------------------------------------------------
// Ordering knobs.
// ---------------------------------------------------------------------------

// OrderedBufferInputFunction is buffer_input with single-threaded ingest.
type OrderedBufferInputFunction struct{ BufferInputFunction }

func (f *OrderedBufferInputFunction) Name() string { return "ordered_buffer_input" }
func (f *OrderedBufferInputFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "buffer_input variant with sink_order_dependent=True",
		Categories:         []string{"test", "ordering"},
		SinkOrderDependent: true,
	}
}

// ---------------------------------------------------------------------------
// batch_index_buffer_input — reconstructs source order via batch_index.
// ---------------------------------------------------------------------------

var unsortedKey = []byte("unsorted")

// BatchIndexBufferInputFunction demands batch_index per process call, sorts
// globally by it in combine, and drains in order.
type BatchIndexBufferInputFunction struct{}

var _ vgi.TableBufferingFunction = (*BatchIndexBufferInputFunction)(nil)

func (f *BatchIndexBufferInputFunction) Name() string { return "batch_index_buffer_input" }
func (f *BatchIndexBufferInputFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:             "buffer_input variant using batch_index to reconstruct order",
		Categories:              []string{"test", "ordering"},
		RequiresInputBatchIndex: true,
	}
}
func (f *BatchIndexBufferInputFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"}}
}
func (f *BatchIndexBufferInputFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindInputSchema(params)
}
func (f *BatchIndexBufferInputFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) ([]byte, error) {
	if params.BatchIndex == nil {
		return nil, fmt.Errorf("batch_index_buffer_input.process() received batch_index=nil — RequiresInputBatchIndex plumbing is broken")
	}
	data, err := vgi.SerializeRecordBatch(batch)
	if err != nil {
		return nil, err
	}
	if _, err := params.Storage.StateAppend(unsortedKey, packIndexed(*params.BatchIndex, data)); err != nil {
		return nil, err
	}
	return params.ExecutionID, nil
}
func (f *BatchIndexBufferInputFunction) Combine(ctx context.Context, params *vgi.ProcessParams, stateIDs [][]byte) ([][]byte, error) {
	entries, err := params.Storage.StateLogScan(unsortedKey, -1, 0)
	if err != nil {
		return nil, err
	}
	type pair struct {
		idx  int64
		data []byte
	}
	pairs := make([]pair, 0, len(entries))
	for _, e := range entries {
		idx, data := unpackIndexed(e.Value)
		pairs = append(pairs, pair{idx, data})
	}
	sort.SliceStable(pairs, func(i, j int) bool { return pairs[i].idx < pairs[j].idx })
	for _, p := range pairs {
		if _, err := params.Storage.StateAppend(bufKey, p.data); err != nil {
			return nil, err
		}
	}
	return [][]byte{params.ExecutionID}, nil
}
func (f *BatchIndexBufferInputFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, finalizeStateID []byte) ([]arrow.RecordBatch, error) {
	return drainBufBatches(params)
}

func packIndexed(idx int64, data []byte) []byte {
	out := make([]byte, 8+len(data))
	binary.LittleEndian.PutUint64(out[:8], uint64(idx))
	copy(out[8:], data)
	return out
}
func unpackIndexed(blob []byte) (int64, []byte) {
	return int64(binary.LittleEndian.Uint64(blob[:8])), blob[8:]
}

// drainBufBatches returns all batches buffered under bufKey.
func drainBufBatches(params *vgi.ProcessParams) ([]arrow.RecordBatch, error) {
	entries, err := params.Storage.StateLogScan(bufKey, -1, 0)
	if err != nil {
		return nil, err
	}
	batches := make([]arrow.RecordBatch, 0, len(entries))
	for _, e := range entries {
		b, err := vgi.DeserializeRecordBatch(e.Value)
		if err != nil {
			return nil, err
		}
		batches = append(batches, b)
	}
	return batches, nil
}

// ---------------------------------------------------------------------------
// ordered_source — fixed 0..15 sequence via source_order_dependent.
// ---------------------------------------------------------------------------

const orderedSourceNRows = 16

// OrderedSourceFunction emits a fixed 0..15 sequence; input is ignored.
type OrderedSourceFunction struct{}

var _ vgi.TableBufferingFunction = (*OrderedSourceFunction)(nil)

func (f *OrderedSourceFunction) Name() string { return "ordered_source" }
func (f *OrderedSourceFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:          "Emits a fixed 0..15 sequence via source_order_dependent=True; input is ignored",
		Categories:           []string{"test", "ordering"},
		SourceOrderDependent: true,
	}
}
func (f *OrderedSourceFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table (ignored)"}}
}
func (f *OrderedSourceFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)}, nil
}
func (f *OrderedSourceFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) ([]byte, error) {
	return params.ExecutionID, nil
}
func (f *OrderedSourceFunction) Combine(ctx context.Context, params *vgi.ProcessParams, stateIDs [][]byte) ([][]byte, error) {
	ids := make([][]byte, orderedSourceNRows)
	for i := 0; i < orderedSourceNRows; i++ {
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(i))
		ids[i] = b
	}
	return ids, nil
}
func (f *OrderedSourceFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, finalizeStateID []byte) ([]arrow.RecordBatch, error) {
	v := int64(binary.BigEndian.Uint32(finalizeStateID))
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.Append(v)
	arr := b.NewArray()
	defer arr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, 1)
	return []arrow.RecordBatch{batch}, nil
}

// ---------------------------------------------------------------------------
// large_state — buffers ~1 MB per input batch (IPC chunking test).
// ---------------------------------------------------------------------------

var largeKey = []byte("large")

// LargeStateFunction appends 1 MB per process call; combine emits the total
// payload size as a single row.
type LargeStateFunction struct{}

var _ vgi.TableBufferingFunction = (*LargeStateFunction)(nil)

func (f *LargeStateFunction) Name() string { return "large_state" }
func (f *LargeStateFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "Buffers ~1 MB per input batch into state (IPC test)", Categories: []string{"test", "memory"}}
}
func (f *LargeStateFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"}}
}
func (f *LargeStateFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindInputSchema(params)
}
func (f *LargeStateFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) ([]byte, error) {
	if _, err := params.Storage.StateAppend(largeKey, make([]byte, 1024*1024)); err != nil {
		return nil, err
	}
	return params.ExecutionID, nil
}
func (f *LargeStateFunction) Combine(ctx context.Context, params *vgi.ProcessParams, stateIDs [][]byte) ([][]byte, error) {
	entries, err := params.Storage.StateLogScan(largeKey, -1, 0)
	if err != nil {
		return nil, err
	}
	var total int64
	for _, e := range entries {
		total += int64(len(e.Value))
	}
	out, err := singleValueRow(params.OutputSchema, total)
	if err != nil {
		return nil, err
	}
	data, err := vgi.SerializeRecordBatch(out)
	if err != nil {
		return nil, err
	}
	if _, err := params.Storage.StateAppend(bufKey, data); err != nil {
		return nil, err
	}
	return [][]byte{params.ExecutionID}, nil
}
func (f *LargeStateFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, finalizeStateID []byte) ([]arrow.RecordBatch, error) {
	return drainBufBatches(params)
}

// singleValueRow builds a 1-row batch where every (int64) column holds v.
func singleValueRow(schema *arrow.Schema, v int64) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	cols := make([]arrow.Array, schema.NumFields())
	for i, field := range schema.Fields() {
		if field.Type.ID() != arrow.INT64 {
			return nil, fmt.Errorf("large_state: column %q is %s, expected int64", field.Name, field.Type)
		}
		b := array.NewInt64Builder(mem)
		b.Append(v)
		cols[i] = b.NewArray()
		b.Release()
	}
	return array.NewRecordBatch(schema, cols, 1), nil
}

// ---------------------------------------------------------------------------
// slow_cancellable_buffering — slow producer with an on_cancel probe.
// Automated tests only exercise registration; the cancel smoke is manual.
// ---------------------------------------------------------------------------

// SlowCancellableBufferingFunction emits count rows during finalize.
type SlowCancellableBufferingFunction struct{}

var _ vgi.TableBufferingFunction = (*SlowCancellableBufferingFunction)(nil)

func (f *SlowCancellableBufferingFunction) Name() string { return "slow_cancellable_buffering" }
func (f *SlowCancellableBufferingFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "Slow buffered table function with an on_cancel file probe (test fixture)", Categories: []string{"test"}}
}
func (f *SlowCancellableBufferingFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "probe_path", Position: 0, ArrowType: "varchar", Doc: "Path to append to when on_cancel fires"},
		{Name: "data", Position: 1, ArrowType: "table", Doc: "Input table (rows ignored)"},
		{Name: "count", Position: -1, ArrowType: "int64", Doc: "Total rows to emit during finalize"},
		{Name: "sleep_ms", Position: -1, ArrowType: "int64", Doc: "Sleep per emitted row (ms)"},
	}
}
func (f *SlowCancellableBufferingFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)}, nil
}
func (f *SlowCancellableBufferingFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) ([]byte, error) {
	return params.ExecutionID, nil
}
func (f *SlowCancellableBufferingFunction) Combine(ctx context.Context, params *vgi.ProcessParams, stateIDs [][]byte) ([][]byte, error) {
	return [][]byte{params.ExecutionID}, nil
}
func (f *SlowCancellableBufferingFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, finalizeStateID []byte) ([]arrow.RecordBatch, error) {
	count := int64(1000)
	if c, err := params.Args.GetScalarInt64("count"); err == nil && c > 0 {
		count = c
	}
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for i := int64(0); i < count; i++ {
		b.Append(i)
	}
	arr := b.NewArray()
	defer arr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, count)
	return []arrow.RecordBatch{batch}, nil
}
