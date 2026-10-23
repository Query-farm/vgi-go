// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/examples/table_in_out"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Continued output through the worker's real servers. Over HTTP every turn is
// rehydrated from the state token the previous one minted, so the resume
// position makes the round trip GobEncode -> token -> rehydrate -> Resume; over
// TCP the stream's state stays in memory. The test plays the client: it sends
// the ticks the extension would.

func continuationWorker() *vgi.Worker {
	w := vgi.NewWorker()
	w.RegisterTableInOut(table_in_out.NewPagedExplodeFunction())
	w.RegisterTableInOut(vgi.AsTableInOutFunction[silentState](&silentFunction{}))
	return w
}

func openContinuationInput(t *testing.T, transport tioTransport, function string) tioStream {
	t.Helper()
	conn := transport.serve(t, continuationWorker())()
	inputSchema, err := vgi.SerializeSchema(tioInputSchema)
	if err != nil {
		t.Fatal(err)
	}
	outputSchema, err := vgi.SerializeSchema(arrow.NewSchema([]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int64}}, nil))
	if err != nil {
		t.Fatal(err)
	}
	bindCall := mustIPC(t, jsonRow(t, generated.BindRequestSchema, map[string]any{
		"function_name":             function,
		"arguments":                 []byte{},
		"function_type":             string(vgi.FunctionTypeTable),
		"input_schema":              inputSchema,
		"resolved_secrets_provided": false,
	}))
	request := mustIPC(t, jsonRow(t, generated.InitRequestSchema, map[string]any{
		"bind_call":     bindCall,
		"output_schema": outputSchema,
		"phase":         string(vgi.PhaseInput),
	}))
	params := jsonRow(t, generated.InitParamsSchema, map[string]any{"request": request})
	defer params.Release()
	stream, closeFn, err := conn.open(params, true)
	if err != nil {
		t.Fatalf("opening the INPUT stream: %v", err)
	}
	t.Cleanup(func() { _ = closeFn() })
	if h := stream.Header(); h != nil {
		h.Release()
	}
	return stream
}

func countsBatch(meta map[string]string, counts ...int64) arrow.RecordBatch {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(counts, nil)
	col := b.NewArray()
	defer col.Release()
	var md arrow.Metadata
	if meta != nil {
		keys, vals := make([]string, 0, len(meta)), make([]string, 0, len(meta))
		for k, v := range meta {
			keys, vals = append(keys, k), append(vals, v)
		}
		md = arrow.NewMetadata(keys, vals)
	}
	return array.NewRecordBatchWithMetadata(tioInputSchema, []arrow.Array{col}, int64(len(counts)), md)
}

var continuationTick = map[string]string{"vgi.status": "CONTINUE"}

// reply exchanges one input and returns the reply's values and whether it said
// more.
func reply(t *testing.T, stream tioStream, input arrow.RecordBatch) ([]int64, bool) {
	t.Helper()
	defer input.Release()
	out, err := stream.Exchange(context.Background(), input)
	if err != nil {
		t.Fatalf("exchange: %v", err)
	}
	defer out.Release()
	vals := append([]int64{}, out.Batch.Column(0).(*array.Int64).Int64Values()...)
	return vals, out.Metadata["vgi.status"] == "HAVE_MORE_OUTPUT"
}

// paged_explode(page_size := 2) over counts [3, 2] is 0 1 2 0 1, cut into
// [0 1] [2 0] [1]: the first from Process, the rest from Resume answering
// ticks. The next input starts over from Process.
func TestContinuedOutputAcrossTurns(t *testing.T) {
	for _, transport := range tioTransports {
		t.Run(transport.name, func(t *testing.T) {
			stream := openContinuationInput(t, transport, "paged_explode")
			steps := []struct {
				input arrow.RecordBatch
				want  []int64
				more  bool
			}{
				{countsBatch(nil, 3, 2), []int64{0, 1}, true},
				{countsBatch(continuationTick), []int64{2, 0}, true},
				{countsBatch(continuationTick), []int64{1}, false},
				{countsBatch(nil, 0, 0), []int64{}, false}, // no output: one empty reply
				{countsBatch(nil, 1), []int64{0}, false},
			}
			for i, step := range steps {
				got, more := reply(t, stream, step.input)
				if !reflect.DeepEqual(got, step.want) || more != step.more {
					t.Fatalf("reply %d = %v more=%v, want %v more=%v", i, got, more, step.want, step.more)
				}
			}
		})
	}
}

// While output is pending a real input is refused, on every transport.
func TestContinuedOutputRefusesInputWhilePending(t *testing.T) {
	for _, transport := range tioTransports {
		t.Run(transport.name, func(t *testing.T) {
			stream := openContinuationInput(t, transport, "paged_explode")
			if _, more := reply(t, stream, countsBatch(nil, 5)); !more {
				t.Fatal("the first reply did not say more")
			}
			in := countsBatch(nil, 1)
			defer in.Release()
			_, err := stream.Exchange(context.Background(), in)
			if err == nil || !strings.Contains(err.Error(), "expected a continuation tick") {
				t.Fatalf("input while output is pending: %v, want a refusal", err)
			}
		})
	}
}

// A table-in-out turn that emits no batch fails rather than leave the client
// waiting for a reply: vgirpc's collector refuses an exchange turn without a
// data batch (OutputCollector.validate), on the HTTP and byte-stream paths.
func TestTableInOutTurnWithoutReplyFails(t *testing.T) {
	for _, transport := range tioTransports {
		t.Run(transport.name, func(t *testing.T) {
			stream := openContinuationInput(t, transport, "silent")
			in := countsBatch(nil, 1)
			defer in.Release()
			_, err := stream.Exchange(context.Background(), in)
			if err == nil || !strings.Contains(err.Error(), "No data batch was emitted") {
				t.Fatalf("a turn with no reply: %v, want vgirpc's refusal", err)
			}
		})
	}
}

type silentState struct{ Calls int64 }

// silentFunction is a table-in-out function whose Process emits nothing.
type silentFunction struct{}

func (*silentFunction) Name() string                   { return "silent" }
func (*silentFunction) Metadata() vgi.FunctionMetadata { return vgi.FunctionMetadata{} }
func (*silentFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{{Name: "data", Position: 0, ArrowType: "table"}}
}
func (*silentFunction) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int64}}, nil),
	}, nil
}
func (*silentFunction) NewState(*vgi.ProcessParams) (*silentState, error) { return &silentState{}, nil }
func (*silentFunction) Process(_ context.Context, _ *vgi.ProcessParams, s *silentState, _ arrow.RecordBatch, _ *vgirpc.OutputCollector) error {
	s.Calls++
	return nil
}
func (*silentFunction) Finalize(context.Context, *vgi.ProcessParams, *silentState) ([]arrow.RecordBatch, error) {
	return nil, nil
}
