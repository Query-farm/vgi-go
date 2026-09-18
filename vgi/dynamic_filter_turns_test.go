// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Query-farm/vgi-go/examples/table"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Dynamic-filter state across turns.
//
// DuckDB tightens a running scan's filter (a Top-N bound, a join's runtime
// filter) by sending a delta document on one tick's metadata, once per change.
// A byte-stream transport keeps the applied filter in the stream's live state.
// An HTTP worker rebuilds the stream from its tokens on every turn, so the
// cursor has to carry it -- and it carried nothing: the deltas were collected
// but left out of the token's wire form, so every turn after the one that
// received a delta fell back to the init snapshot and the worker stopped
// pruning. (Results stayed right only because DuckDB re-applies the filter.)
//
// These drive the shipped dynamic_filter_echo fixture -- descending n, filters
// auto-applied, the live filter echoed on every row -- through the worker's
// real HTTP handler and its raw TCP server. The test plays the client: the
// requests are hand-built, every response is the worker's.

var dfeSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	{Name: "pushed_filters", Type: arrow.BinaryTypes.String},
}, nil)

// dfeStream is the part of a producer stream the tests use.
type dfeStream interface {
	NextWithMetadata(ctx context.Context, custom map[string]string) (*vgirpc.ClientBatch, bool, error)
}

// dfeScan is one open dynamic_filter_echo scan.
type dfeScan struct {
	t      *testing.T
	stream dfeStream
	// token is the cursor the worker last minted (HTTP only; nil over TCP).
	token func() string
}

type dfeTransport struct {
	name string
	open func(t *testing.T, params arrow.RecordBatch) *dfeScan
}

var dfeTransports = []dfeTransport{
	{"http", openDFEOverHTTP},
	{"tcp", openDFEOverTCP},
}

func dfeWorker() *vgi.Worker {
	w := vgi.NewWorker()
	w.RegisterTable(table.NewDynamicFilterEchoFunction())
	return w
}

func openDFEOverHTTP(t *testing.T, params arrow.RecordBatch) *dfeScan {
	t.Helper()
	hs, err := dfeWorker().NewHttpServerForTest()
	if err != nil {
		t.Fatalf("building the HTTP server: %v", err)
	}
	ts := httptest.NewServer(hs)
	t.Cleanup(ts.Close)
	client, err := vgirpc.NewHttpClient(ts.URL,
		vgirpc.WithClientProtocol(vgi.ProtocolName),
		vgirpc.WithClientProtocolVersion(vgi.ProtocolVersion))
	if err != nil {
		t.Fatalf("building the HTTP client: %v", err)
	}
	t.Cleanup(client.Close)
	stream, err := client.OpenProducer(context.Background(), "init", params, vgirpc.ClientStreamSchema{HasHeader: true})
	if err != nil {
		t.Fatalf("opening the scan: %v", err)
	}
	t.Cleanup(stream.Close)
	return &dfeScan{t: t, stream: stream, token: stream.Token}
}

func openDFEOverTCP(t *testing.T, params arrow.RecordBatch) *dfeScan {
	t.Helper()
	bound := make(chan int, 1)
	go func() {
		_ = dfeWorker().ServeTcpForTest(5*time.Second, func(_ string, port int) { bound <- port })
	}()
	var port int
	select {
	case port = <-bound:
	case <-time.After(10 * time.Second):
		t.Fatal("the TCP server never bound")
	}
	client, err := vgirpc.NewTcpClient(context.Background(), "127.0.0.1", port,
		vgirpc.WithTcpClientProtocol(vgi.ProtocolName),
		vgirpc.WithTcpClientProtocolVersion(vgi.ProtocolVersion))
	if err != nil {
		t.Fatalf("connecting over TCP: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	stream, err := client.OpenProducer(context.Background(), "init", params, vgirpc.ClientStreamSchema{HasHeader: true})
	if err != nil {
		t.Fatalf("opening the scan: %v", err)
	}
	t.Cleanup(func() { _ = stream.Close(context.Background()) })
	return &dfeScan{t: t, stream: stream}
}

// openDFE opens dynamic_filter_echo(count, batch_size := batchSize) with an
// empty v2 snapshot, the shape DuckDB inits a Top-N scan with, and returns it
// after its first batch (which over HTTP rides the init response).
func openDFE(t *testing.T, transport dfeTransport, count, batchSize int64) *dfeScan {
	t.Helper()
	argsSchema := arrow.NewSchema([]arrow.Field{{Name: "args", Type: arrow.StructOf(
		arrow.Field{Name: "positional_0", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "named_batch_size", Type: arrow.PrimitiveTypes.Int64},
	)}}, nil)
	args := mustIPC(t, jsonRow(t, argsSchema, map[string]any{
		"args": map[string]any{"positional_0": count, "named_batch_size": batchSize},
	}))
	bindCall := mustIPC(t, jsonRow(t, generated.BindRequestSchema, map[string]any{
		"function_name":             "dynamic_filter_echo",
		"arguments":                 args,
		"function_type":             string(vgi.FunctionTypeTable),
		"resolved_secrets_provided": false,
	}))
	outputSchema, err := vgi.SerializeSchema(dfeSchema)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := filterDocument(t, map[string]any{"kind": "snapshot", "predicates": []any{}})
	request := mustIPC(t, jsonRow(t, generated.InitRequestSchema, map[string]any{
		"bind_call":        bindCall,
		"output_schema":    outputSchema,
		"pushdown_filters": snapshot,
	}))
	params := jsonRow(t, generated.InitParamsSchema, map[string]any{"request": request})
	defer params.Release()
	scan := transport.open(t, params)
	rows, echo := scan.turn(nil)
	if len(rows) != int(batchSize) || rows[0] != count-1 || echo != "(none)" {
		t.Fatalf("first batch: rows %v echo %q, want %d rows from %d with no filter", rows, echo, batchSize, count-1)
	}
	return scan
}

// turn drives one producer turn, carrying custom as the tick's metadata, and
// returns the rows the worker emitted and the filter it echoed ("" when it
// emitted no rows).
func (s *dfeScan) turn(custom map[string]string) ([]int64, string) {
	t := s.t
	t.Helper()
	batch, ok, err := s.stream.NextWithMetadata(context.Background(), custom)
	if err != nil {
		t.Fatalf("turn: %v", err)
	}
	if !ok {
		t.Fatal("turn: the scan ended early")
	}
	defer batch.Release()
	rows := []int64{}
	echo := ""
	record := batch.Batch
	if record.NumRows() > 0 {
		n := record.Column(record.Schema().FieldIndices("n")[0]).(*array.Int64)
		for i := 0; i < n.Len(); i++ {
			rows = append(rows, n.Value(i))
		}
		echo = record.Column(record.Schema().FieldIndices("pushed_filters")[0]).(*array.String).Value(0)
	}
	return rows, echo
}

// filterDocument builds a Filter Encoding v2 document batch the way the C++
// client frames one -- filter_spec plus value_<i> payloads -- as IPC bytes.
func filterDocument(t *testing.T, document map[string]any, values ...int64) []byte {
	t.Helper()
	document["encoding"] = "vgi.filters.v2"
	document["semantics"] = "vgi.duckdb.standard.v1"
	spec, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	mem := memory.NewGoAllocator()
	fields := []arrow.Field{{Name: "filter_spec", Type: arrow.BinaryTypes.String}}
	specBuilder := array.NewStringBuilder(mem)
	defer specBuilder.Release()
	specBuilder.Append(string(spec))
	columns := []arrow.Array{specBuilder.NewArray()}
	for i, value := range values {
		fields = append(fields, arrow.Field{Name: "value_" + strconv.Itoa(i), Type: arrow.PrimitiveTypes.Int64, Nullable: true})
		builder := array.NewInt64Builder(mem)
		builder.Append(value)
		columns = append(columns, builder.NewArray())
		builder.Release()
	}
	metadata := arrow.NewMetadata(
		[]string{"vgi_filter_encoding", "vgi_filter_version", "vgi_evaluation_context"},
		[]string{"vgi.filters.v2", "2", "vgi.none.v1"},
	)
	batch := array.NewRecordBatch(arrow.NewSchema(fields, &metadata), columns, 1)
	for _, column := range columns {
		column.Release()
	}
	return mustIPC(t, batch)
}

func upsert(id string, revision int, op string, valueRef int) map[string]any {
	return map[string]any{
		"operation": "upsert", "id": id, "revision": revision, "mode": "advisory", "source": "top_n",
		"expression": map[string]any{
			"node": "comparison", "op": op,
			"left":  map[string]any{"node": "column_ref", "column_index": 0, "column_name": "n"},
			"right": map[string]any{"node": "literal", "value_ref": valueRef},
		},
	}
}

func remove(id string, revision int) map[string]any {
	return map[string]any{"operation": "remove", "id": id, "revision": revision}
}

// tick is the metadata of a tick carrying one delta.
func tick(t *testing.T, updates []map[string]any, values ...int64) map[string]string {
	t.Helper()
	delta := filterDocument(t, map[string]any{"kind": "delta", "updates": updates}, values...)
	return map[string]string{"vgi_pushdown_filters": base64.StdEncoding.EncodeToString(delta)}
}

func descending(from, to int64) []int64 {
	rows := []int64{}
	for n := from; n > to; n-- {
		rows = append(rows, n)
	}
	return rows
}

func equalRows(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A Top-N bound arrives once; every later turn still prunes by it. This is
// ORDER BY n DESC LIMIT 4 over descending data: after the bound, no later row
// can qualify, so every later batch must come back empty. Over HTTP it came
// back full on every turn after the one carrying the delta.
func TestDynamicFilterPrunesOnLaterTurns(t *testing.T) {
	for _, transport := range dfeTransports {
		t.Run(transport.name, func(t *testing.T) {
			scan := openDFE(t, transport, 1000, 10)
			rows, echo := scan.turn(tick(t, []map[string]any{upsert("top_n:0", 1, "gt", 0)}, 985))
			if !equalRows(rows, descending(989, 985)) || echo != "PushdownFilters([ConstantFilter(n > 985)])" {
				t.Fatalf("the turn carrying the bound: rows %v echo %q", rows, echo)
			}
			for turn := 2; turn <= 20; turn++ {
				if rows, echo := scan.turn(nil); len(rows) != 0 {
					t.Fatalf("turn %d emitted %d rows (echo %q): the bound n > 985 was dropped", turn, len(rows), echo)
				}
			}
		})
	}
}

// A bound tightening on every other tick stays in force on the ticks between,
// and the cursor stays flat however many deltas came before: it carries the
// compacted history (one delta per live predicate), never every delta.
func TestDynamicFilterTighteningKeepsTheCursorFlat(t *testing.T) {
	for _, transport := range dfeTransports {
		t.Run(transport.name, func(t *testing.T) {
			scan := openDFE(t, transport, 10000, 10)
			var bound int64
			var tokens []int
			for turn := 1; turn <= 80; turn++ {
				top := 9989 - 10*int64(turn-1) // this turn's first row
				var meta map[string]string
				if turn%2 == 1 {
					// Keep the lower half of this turn's batch; the next
					// turn's batch passes whole, under the same bound.
					bound = top - 4
					meta = tick(t, []map[string]any{upsert("top_n:0", turn, "lt", 0)}, bound)
				}
				rows, echo := scan.turn(meta)
				if want := descending(min(top, bound-1), top-10); !equalRows(rows, want) {
					t.Fatalf("turn %d: rows %v, want %v (bound n < %d)", turn, rows, want, bound)
				}
				if want := "PushdownFilters([ConstantFilter(n < " + strconv.FormatInt(bound, 10) + ")])"; echo != want {
					t.Fatalf("turn %d: echo %q, want %q", turn, echo, want)
				}
				if scan.token != nil {
					tokens = append(tokens, len(scan.token()))
				}
			}
			if len(tokens) > 0 {
				first, last := tokens[0], tokens[len(tokens)-1]
				t.Logf("cursor: %d bytes after the first delta, %d after the 40th", first, last)
				if last > first+64 {
					t.Fatalf("the cursor grew from %d to %d bytes over 40 deltas: %v", first, last, tokens)
				}
			}
		})
	}
}

// A removed predicate stays removed after its upsert is compacted away: the
// tombstone keeps a stale re-send of the upsert from resurrecting it.
func TestDynamicFilterTombstoneSurvivesTurns(t *testing.T) {
	for _, transport := range dfeTransports {
		t.Run(transport.name, func(t *testing.T) {
			scan := openDFE(t, transport, 1000, 10)
			if _, echo := scan.turn(tick(t, []map[string]any{upsert("top_n:0", 1, "lt", 0)}, 100000)); echo != "PushdownFilters([ConstantFilter(n < 100000)])" {
				t.Fatalf("upsert: echo %q", echo)
			}
			if _, echo := scan.turn(tick(t, []map[string]any{remove("top_n:0", 2)})); echo != "(none)" {
				t.Fatalf("remove: echo %q", echo)
			}
			scan.turn(nil) // a turn rebuilt from the tokens alone
			rows, echo := scan.turn(tick(t, []map[string]any{upsert("top_n:0", 1, "lt", 0)}, 5))
			if len(rows) != 10 || echo != "(none)" {
				t.Fatalf("a stale upsert after the removal: %d rows echo %q, want 10 rows and no filter", len(rows), echo)
			}
		})
	}
}

// The state rebuilt on a turn with no delta is the state the previous turn had,
// predicate order included, across a remove and re-add.
func TestDynamicFilterRebuiltStateKeepsOrder(t *testing.T) {
	for _, transport := range dfeTransports {
		t.Run(transport.name, func(t *testing.T) {
			scan := openDFE(t, transport, 1000, 10)
			scan.turn(tick(t, []map[string]any{upsert("top_n:0", 1, "lt", 0), upsert("top_n:1", 1, "gt", 1)}, 100000, 5))
			scan.turn(tick(t, []map[string]any{remove("top_n:0", 2), upsert("top_n:1", 1, "gt", 0)}, 5))
			_, applied := scan.turn(tick(t, []map[string]any{upsert("top_n:0", 3, "lt", 0), upsert("top_n:1", 1, "gt", 1)}, 99999, 5))
			if !strings.Contains(applied, "ConstantFilter(n > 5)") || !strings.Contains(applied, "ConstantFilter(n < 99999)") {
				t.Fatalf("after the re-add: echo %q", applied)
			}
			for turn := 0; turn < 3; turn++ {
				if _, rebuilt := scan.turn(nil); rebuilt != applied {
					t.Fatalf("rebuilt turn %d echoes %q, want %q", turn, rebuilt, applied)
				}
			}
		})
	}
}
