// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"encoding/gob"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Storage round trips per RPC, counted at the FunctionStorage boundary. On a
// remote backend (cloudflare-do) each is a 30-90 ms network call, so these
// numbers are the latency budget of buffered and aggregate functions.
//
// Set VGI_CALLCOUNT_REPORT_ONLY=1 to log the counts without asserting (how the
// before/after table in the change description was produced against the
// previous release).

func callCountAsserting() bool { return os.Getenv("VGI_CALLCOUNT_REPORT_ONLY") == "" }

// countingWorker is a worker whose shared storage is a counted SQLite file.
func countingWorker(t *testing.T) (*Worker, *countingStorage) {
	t.Helper()
	t.Setenv("VGI_ATTACH_SECRET_KEY_FILE", filepath.Join(t.TempDir(), "key", "host.key"))
	back, err := NewSQLiteStorage(SQLiteStorageOptions{Path: filepath.Join(t.TempDir(), "storage.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = back.Close() })
	counter := &countingStorage{FunctionStorage: back}
	w := NewWorker()
	w.fsOnce.Do(func() { w.fs = counter })
	return w, counter
}

// --- aggregate fixture: sum(x) * scale, scale a constant argument ----------

type ccSumState struct{ Sum int64 }

func init() { gob.Register(&ccSumState{}) }

type ccScaledSum struct{}

func (ccScaledSum) Name() string               { return "cc_scaled_sum" }
func (ccScaledSum) Metadata() FunctionMetadata { return FunctionMetadata{SupportsWindow: true} }
func (ccScaledSum) ArgumentSpecs() []ArgSpec {
	return []ArgSpec{
		{Name: "x", Position: 0, ArrowType: "int64"},
		{Name: "scale", Position: 1, ArrowType: "int64", IsConst: true},
	}
}

var ccOutSchema = arrow.NewSchema([]arrow.Field{{Name: "result", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)

func (ccScaledSum) OnBind(*AggregateBindParams) (*BindResponse, error) {
	return BindSchema(ccOutSchema)
}
func (ccScaledSum) NewState(*AggregateProcessParams) interface{} { return &ccSumState{} }

func ccScale(p *AggregateProcessParams) int64 {
	if p.Args == nil || len(p.Args.Positional) == 0 {
		return -1000 // makes a lost const arg visible in the result
	}
	return p.Args.Positional[0].(*array.Int64).Value(0)
}

func (f ccScaledSum) Update(states map[int64]interface{}, gids *Int64Slice, cols []arrow.Array, p *AggregateProcessParams) error {
	x := cols[0].(*array.Int64)
	for i := 0; i < gids.Len(); i++ {
		s := EnsureState(states, gids.At(i), func() *ccSumState { return &ccSumState{} })
		s.Sum += x.Value(i)
	}
	return nil
}

func (ccScaledSum) Combine(src, tgt interface{}, p *AggregateProcessParams) (interface{}, error) {
	t := tgt.(*ccSumState)
	t.Sum += src.(*ccSumState).Sum
	return t, nil
}

func (ccScaledSum) Finalize(gids []int64, states map[int64]interface{}, p *AggregateProcessParams) (arrow.RecordBatch, error) {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	for _, g := range gids {
		if s, ok := states[g].(*ccSumState); ok {
			b.Append(s.Sum * ccScale(p))
		} else {
			b.AppendNull()
		}
	}
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(ccOutSchema, []arrow.Array{col}, int64(len(gids))), nil
}

func (ccScaledSum) WindowInit(*WindowPartition, *AggregateProcessParams) (interface{}, error) {
	return nil, nil
}

func (ccScaledSum) Window(rid int64, frames [][2]int64, part *WindowPartition, _ interface{}, p *AggregateProcessParams) (interface{}, error) {
	x := part.Inputs.Column(0).(*array.Int64)
	var sum int64
	for _, f := range frames {
		for i := f[0]; i < f[1]; i++ {
			sum += x.Value(int(i))
		}
	}
	return sum * ccScale(p), nil
}

func ccInt64Batch(t *testing.T, names []string, cols ...[]int64) []byte {
	t.Helper()
	fields := make([]arrow.Field, len(names))
	arrs := make([]arrow.Array, len(names))
	for i, n := range names {
		fields[i] = arrow.Field{Name: n, Type: arrow.PrimitiveTypes.Int64}
		b := array.NewInt64Builder(memory.DefaultAllocator)
		b.AppendValues(cols[i], nil)
		arrs[i] = b.NewArray()
		b.Release()
	}
	batch := array.NewRecordBatch(arrow.NewSchema(fields, nil), arrs, int64(len(cols[0])))
	data, err := SerializeRecordBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// ccAttach is a subprocess-transport attach value: uuid(16) || catalog bytes.
func ccAttach(b byte) *[]byte {
	v := append(bytes16(b), []byte("cc-catalog")...)
	return &v
}

func bytes16(b byte) []byte {
	out := make([]byte, 16)
	for i := range out {
		out[i] = b
	}
	return out
}

func TestStorageCallCountsAggregate(t *testing.T) {
	ctx := context.Background()
	w, counter := countingWorker(t)
	w.RegisterAggregate(ccScaledSum{})
	cc := shapeTestCallCtx()
	attach := ccAttach(0xA1)
	name := "cc_scaled_sum"

	bind, err := w.handleAggregateBind(ctx, cc, AggregateBindRequestWire{
		FunctionName: name, Arguments: ccInt64Batch(t, []string{"scale"}, []int64{3}), AttachOpaqueData: attach,
	})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	exec := bind.ExecutionID
	bindCalls := counter.total()
	counter.reset()

	const batches = 3
	for i := range batches {
		if _, err := w.handleAggregateUpdate(ctx, cc, AggregateUpdateRequestWire{
			FunctionName: name, ExecutionID: exec, AttachOpaqueData: attach,
			InputBatch: ccInt64Batch(t, []string{GroupColumnName, "x"}, []int64{0, 1, 0}, []int64{int64(i), 10, 1}),
		}); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}
	updateCalls := counter.total()
	counter.reset()

	if _, err := w.handleAggregateCombine(ctx, cc, AggregateCombineRequestWire{
		FunctionName: name, ExecutionID: exec, AttachOpaqueData: attach,
		MergeBatch: ccInt64Batch(t, []string{"source_group_id", "target_group_id"}, []int64{1}, []int64{0}),
	}); err != nil {
		t.Fatalf("combine: %v", err)
	}
	combineCalls := counter.total()
	counter.reset()

	outSchema, _ := SerializeSchema(ccOutSchema)
	fin, err := w.handleAggregateFinalize(ctx, cc, AggregateFinalizeRequestWire{
		FunctionName: name, ExecutionID: exec, AttachOpaqueData: attach, OutputSchema: outSchema,
		GroupIDsBatch: ccInt64Batch(t, []string{"group_id"}, []int64{0}),
	})
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	finalizeCalls := counter.total()
	res, err := DeserializeRecordBatch(fin.ResultBatch)
	if err != nil {
		t.Fatal(err)
	}
	// group 0: (0+1)+(1+1)+(2+1) = 6, plus group 1's 30 combined in = 36; * 3.
	if got := res.Column(0).(*array.Int64).Value(0); got != 108 {
		t.Fatalf("result = %d, want 108", got)
	}
	t.Logf("aggregate storage calls: bind=%d update=%d (%d batches, %.1f/batch) combine=%d finalize=%d",
		bindCalls, updateCalls, batches, float64(updateCalls)/batches, combineCalls, finalizeCalls)
	if callCountAsserting() {
		if updateCalls != 2*batches || combineCalls != 2 || finalizeCalls != 1 {
			t.Fatalf("aggregate storage calls: update=%d combine=%d finalize=%d, want %d/2/1",
				updateCalls, combineCalls, finalizeCalls, 2*batches)
		}
	}
}

func TestStorageCallCountsWindow(t *testing.T) {
	ctx := context.Background()
	w, counter := countingWorker(t)
	w.RegisterAggregate(ccScaledSum{})
	cc := shapeTestCallCtx()
	attach := ccAttach(0xB2)
	name := "cc_scaled_sum"

	bind, err := w.handleAggregateBind(ctx, cc, AggregateBindRequestWire{
		FunctionName: name, Arguments: ccInt64Batch(t, []string{"scale"}, []int64{2}), AttachOpaqueData: attach,
	})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	exec := bind.ExecutionID
	counter.reset()

	outSchema, _ := SerializeSchema(ccOutSchema)
	const rows = 8
	xs := make([]int64, rows)
	for i := range xs {
		xs[i] = int64(i + 1)
	}
	if _, err := w.handleAggregateWindowInit(ctx, cc, AggregateWindowInitRequestWire{
		FunctionName: name, ExecutionID: exec, PartitionID: 7, RowCount: rows, AttachOpaqueData: attach,
		PartitionBatch: ccInt64Batch(t, []string{"x"}, xs), OutputSchema: outSchema,
	}); err != nil {
		t.Fatalf("window init: %v", err)
	}
	initCalls := counter.total()
	counter.reset()

	const blocks = 4
	for b := range blocks {
		start := int64(b * 2)
		res, err := w.handleAggregateWindowBatch(ctx, cc, AggregateWindowBatchRequestWire{
			FunctionName: name, ExecutionID: exec, PartitionID: 7, RowIdx: start, Count: 2,
			FramesPerRow: []int64{1, 1}, FrameStarts: []int64{0, 0}, FrameEnds: []int64{start + 1, start + 2},
			AttachOpaqueData: attach,
		})
		if err != nil {
			t.Fatalf("window block %d: %v", b, err)
		}
		out, err := DeserializeRecordBatch(res.ResultBatch)
		if err != nil {
			t.Fatal(err)
		}
		// Running sum of 1..n, scaled by 2.
		n := start + 2
		if got := out.Column(0).(*array.Int64).Value(1); got != n*(n+1) {
			t.Fatalf("block %d row 1 = %d, want %d", b, got, n*(n+1))
		}
	}
	blockCalls := counter.total()
	t.Logf("window storage calls: init=%d evaluate=%d (%d blocks)", initCalls, blockCalls, blocks)
	if callCountAsserting() {
		if initCalls != 1 || blockCalls != 0 {
			t.Fatalf("window storage calls: init=%d evaluate=%d, want 1/0", initCalls, blockCalls)
		}
	}
}

// --- buffered fixture --------------------------------------------------------

type ccBuffer struct{}

func (ccBuffer) Name() string               { return "cc_buffer" }
func (ccBuffer) Metadata() FunctionMetadata { return FunctionMetadata{} }
func (ccBuffer) ArgumentSpecs() []ArgSpec   { return nil }
func (ccBuffer) OnBind(*BindParams) (*BindResponse, error) {
	return BindSchema(emptyInputCounterSchema)
}
func (ccBuffer) Process(ctx context.Context, p *ProcessParams, b arrow.RecordBatch) ([]byte, error) {
	return []byte("s"), nil
}
func (ccBuffer) Combine(ctx context.Context, p *ProcessParams, ids [][]byte) ([][]byte, error) {
	return [][]byte{p.ExecutionID}, nil
}
func (ccBuffer) Finalize(ctx context.Context, p *ProcessParams, id []byte) ([]arrow.RecordBatch, error) {
	return []arrow.RecordBatch{emptyInputCountBatch(int64(len(id)))}, nil
}

func TestStorageCallCountsBuffered(t *testing.T) {
	ctx := context.Background()
	w, counter := countingWorker(t)
	w.RegisterTableBuffering(ccBuffer{})
	cc := shapeTestCallCtx()
	attach := ccAttach(0xC3)

	inputSchema := shapeTestInputSchema(t)
	outSchema, _ := SerializeSchema(emptyInputCounterSchema)
	bind := BindRequestWire{FunctionName: "cc_buffer", FunctionType: string(FunctionTypeTable), InputSchema: &inputSchema, AttachOpaqueData: attach}
	phase := string(PhaseTableBuffering)
	sink, err := w.handleInit(ctx, cc, InitRequestWire{BindCall: bind, OutputSchema: outSchema, Phase: &phase})
	if err != nil {
		t.Fatalf("sink init: %v", err)
	}
	exec := sink.Header.(*GlobalInitResponseWire).ExecutionID
	initCalls := counter.total()
	counter.reset()

	const batches = 5
	for i := range batches {
		if _, err := w.handleTableBufferingProcess(ctx, cc, TableBufferingProcessRequestWire{
			FunctionName: "cc_buffer", ExecutionID: exec, AttachOpaqueData: attach,
			InputBatch: ccInt64Batch(t, []string{"x"}, []int64{int64(i)}),
		}); err != nil {
			t.Fatalf("process %d: %v", i, err)
		}
	}
	if _, err := w.handleTableBufferingCombine(ctx, cc, TableBufferingCombineRequestWire{
		FunctionName: "cc_buffer", ExecutionID: exec, AttachOpaqueData: attach, StateIDs: [][]byte{[]byte("s")},
	}); err != nil {
		t.Fatalf("combine: %v", err)
	}
	runCalls := counter.total()
	t.Logf("buffered storage calls: sink init=%d process+combine=%d (%d batches)", initCalls, runCalls, batches)
	if callCountAsserting() {
		if initCalls != 1 || runCalls != 0 {
			t.Fatalf("buffered storage calls: init=%d process+combine=%d, want 1/0", initCalls, runCalls)
		}
	}
}
