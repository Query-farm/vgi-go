// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Query-farm/vgi-go/examples/table_in_out"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Table-in-out state is keyed per substream, not per process.
//
// A table-in-out function with a finalize upserts its running state into
// execution-scoped storage after each batch (params.Storage.Put), and its
// finalize drains every stored state. Storage is scoped to the execution, which
// every connection of a fanned-out call shares, so the row key has to tell
// those connections apart. It used to be the process id, which does so only
// when each connection is its own process. Under the launcher, TCP or HTTP one
// process serves them all: they overwrote each other's state and the finalize
// undercounted. The key is now InitRequest.substream_id -- minted by the client
// per substream and sent on its init, every tick and its finalize -- with the
// pid kept only for a client that sends none.
//
// These drive the shipped substream_partial_sum fixture through the worker's
// real server over HTTP (every exchange rehydrated from its continuation token)
// and raw TCP (one connection per stream, state held in memory), so every
// connection lands in this one test process -- exactly the deployment the pid
// key broke. The test plays the client, building requests the way the Python
// client fans one execution out: a primary INPUT stream that mints the
// execution, a secondary that joins it on another connection, and one FINALIZE
// carrying the primary's substream id. Every response is the worker's.

var tioInputSchema = arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)

// tioStream is the part of a client stream the tests use; both transports'
// streams have it.
type tioStream interface {
	Header() *vgirpc.ClientBatch
	Exchange(ctx context.Context, input arrow.RecordBatch) (*vgirpc.ClientBatch, error)
	Next(ctx context.Context) (*vgirpc.ClientBatch, bool, error)
}

// tioConn is one client connection to the worker.
type tioConn struct {
	open func(params arrow.RecordBatch, exchange bool) (tioStream, func() error, error)
}

// tioHarness is one worker, served in this process, and a way to reach it.
type tioHarness struct {
	t    *testing.T
	back vgi.FunctionStorage
	dial func() tioConn
}

type tioTransport struct {
	name  string
	serve func(t *testing.T, w *vgi.Worker) func() tioConn
}

var tioTransports = []tioTransport{
	{"http", serveTioHTTP},
	{"tcp", serveTioTCP},
}

func newTioHarness(t *testing.T, transport tioTransport) *tioHarness {
	t.Helper()
	back, err := vgi.NewSQLiteStorage(vgi.SQLiteStorageOptions{Path: filepath.Join(t.TempDir(), "storage.db")})
	if err != nil {
		t.Fatalf("opening storage: %v", err)
	}
	t.Cleanup(func() { _ = back.Close() })
	w := vgi.NewWorker(vgi.WithFunctionStorage(back))
	w.RegisterTableInOut(table_in_out.NewSubstreamPartialSumFunction())
	return &tioHarness{t: t, back: back, dial: transport.serve(t, w)}
}

// serveTioHTTP serves the worker's real HTTP handler; every stream shares one
// client, as HTTP requests are independent.
func serveTioHTTP(t *testing.T, w *vgi.Worker) func() tioConn {
	hs, err := w.NewHttpServerForTest()
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
	return func() tioConn {
		return tioConn{open: func(params arrow.RecordBatch, exchange bool) (tioStream, func() error, error) {
			schemas := vgirpc.ClientStreamSchema{HasHeader: true}
			var stream *vgirpc.HttpClientStream
			var err error
			if exchange {
				schemas.Input = tioInputSchema
				stream, err = client.OpenExchange(context.Background(), "init", params, schemas)
			} else {
				stream, err = client.OpenProducer(context.Background(), "init", params, schemas)
			}
			if err != nil {
				return nil, nil, err
			}
			return stream, func() error { stream.Close(); return nil }, nil
		}}
	}
}

// serveTioTCP serves the worker's raw TCP server; every stream gets its own
// connection, all accepted by this one process -- the launcher's shape.
func serveTioTCP(t *testing.T, w *vgi.Worker) func() tioConn {
	bound := make(chan int, 1)
	go func() {
		_ = w.ServeTcpForTest(5*time.Second, func(_ string, port int) { bound <- port })
	}()
	var port int
	select {
	case port = <-bound:
	case <-time.After(10 * time.Second):
		t.Fatal("the TCP server never bound")
	}
	return func() tioConn {
		client, err := vgirpc.NewTcpClient(context.Background(), "127.0.0.1", port,
			vgirpc.WithTcpClientProtocol(vgi.ProtocolName),
			vgirpc.WithTcpClientProtocolVersion(vgi.ProtocolVersion))
		if err != nil {
			t.Fatalf("connecting over TCP: %v", err)
		}
		t.Cleanup(func() { _ = client.Close() })
		return tioConn{open: func(params arrow.RecordBatch, exchange bool) (tioStream, func() error, error) {
			schemas := vgirpc.ClientStreamSchema{HasHeader: true}
			var stream *vgirpc.TcpClientStream
			var err error
			if exchange {
				schemas.Input = tioInputSchema
				stream, err = client.OpenExchange(context.Background(), "init", params, schemas)
			} else {
				stream, err = client.OpenProducer(context.Background(), "init", params, schemas)
			}
			if err != nil {
				return nil, nil, err
			}
			return stream, func() error { return stream.Close(context.Background()) }, nil
		}}
	}
}

// jsonRow builds a one-row batch of schema from row. Binary values are given
// as []byte, which encoding/json writes as the base64 Arrow's JSON reader
// expects; nullable columns left out are null.
func jsonRow(t *testing.T, schema *arrow.Schema, row map[string]any) arrow.RecordBatch {
	t.Helper()
	raw, err := json.Marshal([]map[string]any{row})
	if err != nil {
		t.Fatal(err)
	}
	batch, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("building a %v row: %v", schema, err)
	}
	return batch
}

func mustIPC(t *testing.T, batch arrow.RecordBatch) []byte {
	t.Helper()
	defer batch.Release()
	data, err := vgi.SerializeRecordBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// initParams is the init request a client sends: the protocol's wrapped
// shape, a `request` column holding an IPC-encoded InitRequest whose bind_call
// is itself an IPC-encoded BindRequest. A nil executionID opens a new
// execution; a nil substreamID is a client that sends none.
func (h *tioHarness) initParams(phase vgi.Phase, executionID, substreamID []byte) arrow.RecordBatch {
	t := h.t
	t.Helper()
	inputSchema, err := vgi.SerializeSchema(tioInputSchema)
	if err != nil {
		t.Fatal(err)
	}
	bindCall := mustIPC(t, jsonRow(t, generated.BindRequestSchema, map[string]any{
		"function_name":             "substream_partial_sum",
		"arguments":                 []byte{},
		"function_type":             string(vgi.FunctionTypeTable),
		"input_schema":              inputSchema,
		"resolved_secrets_provided": false,
	}))
	req := map[string]any{
		"bind_call":     bindCall,
		"output_schema": inputSchema, // the fixture's output is one int64 column too
		"phase":         string(phase),
	}
	if executionID != nil {
		req["execution_id"] = executionID
	}
	if substreamID != nil {
		req["substream_id"] = substreamID
	}
	request := mustIPC(t, jsonRow(t, generated.InitRequestSchema, req))
	return jsonRow(t, generated.InitParamsSchema, map[string]any{"request": request})
}

// inputStream is one open INPUT stream of the fixture.
type inputStream struct {
	h           *tioHarness
	stream      tioStream
	close       func() error
	executionID []byte
}

// openInput opens an INPUT stream on its own connection and returns it with the
// execution id the worker answered with.
func (h *tioHarness) openInput(executionID, substreamID []byte) *inputStream {
	t := h.t
	t.Helper()
	params := h.initParams(vgi.PhaseInput, executionID, substreamID)
	defer params.Release()
	stream, closeFn, err := h.dial().open(params, true)
	if err != nil {
		t.Fatalf("opening an INPUT stream: %v", err)
	}
	header := stream.Header()
	if header == nil {
		t.Fatal("the INPUT init answered no header")
	}
	defer header.Release()
	col, ok := header.Batch.Column(header.Batch.Schema().FieldIndices("execution_id")[0]).(*array.Binary)
	if !ok || col.Len() != 1 || col.IsNull(0) {
		t.Fatalf("the INPUT init header carries no execution_id: %v", header.Batch)
	}
	return &inputStream{h: h, stream: stream, close: closeFn, executionID: bytes.Clone(col.Value(0))}
}

// send exchanges one batch of the values [from, to).
func (s *inputStream) send(from, to int64) {
	t := s.h.t
	t.Helper()
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	for v := from; v < to; v++ {
		b.Append(v)
	}
	col := b.NewArray()
	defer col.Release()
	batch := array.NewRecordBatch(tioInputSchema, []arrow.Array{col}, to-from)
	defer batch.Release()
	out, err := s.stream.Exchange(context.Background(), batch)
	if err != nil {
		t.Fatalf("exchanging [%d, %d): %v", from, to, err)
	}
	out.Release()
}

func (s *inputStream) finish() {
	s.h.t.Helper()
	if err := s.close(); err != nil {
		s.h.t.Fatalf("closing an INPUT stream: %v", err)
	}
}

// finalize runs the FINALIZE phase and returns the one partial it emits.
func (h *tioHarness) finalize(executionID, substreamID []byte) int64 {
	t := h.t
	t.Helper()
	params := h.initParams(vgi.PhaseFinalize, executionID, substreamID)
	defer params.Release()
	stream, closeFn, err := h.dial().open(params, false)
	if err != nil {
		t.Fatalf("opening the FINALIZE stream: %v", err)
	}
	if header := stream.Header(); header != nil {
		header.Release()
	}
	var partials []int64
	for {
		batch, ok, err := stream.Next(context.Background())
		if err != nil {
			t.Fatalf("draining the FINALIZE stream: %v", err)
		}
		if !ok {
			break
		}
		if batch.Batch.NumRows() > 0 {
			col := batch.Batch.Column(0).(*array.Int64)
			for i := 0; i < col.Len(); i++ {
				partials = append(partials, col.Value(i))
			}
		}
		batch.Release()
	}
	if err := closeFn(); err != nil {
		t.Fatalf("closing the FINALIZE stream: %v", err)
	}
	if len(partials) != 1 {
		t.Fatalf("the finalize emitted %d partials %v, want exactly one", len(partials), partials)
	}
	return partials[0]
}

// storedKeys returns the worker-state keys the execution holds.
func (h *tioHarness) storedKeys(executionID []byte) [][]byte {
	h.t.Helper()
	entries, err := h.back.WorkerScan(executionID)
	if err != nil {
		h.t.Fatalf("scanning worker state: %v", err)
	}
	keys := make([][]byte, len(entries))
	for i, e := range entries {
		keys[i] = e.WorkerKey
	}
	return keys
}

func newSubstreamID(t *testing.T) []byte {
	t.Helper()
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		t.Fatal(err)
	}
	return id
}

func containsKey(keys [][]byte, want []byte) bool {
	for _, k := range keys {
		if bytes.Equal(k, want) {
			return true
		}
	}
	return false
}

// sumRange is the sum of [from, to).
func sumRange(from, to int64) int64 { return (to - from) * (from + to - 1) / 2 }

// Two streams of one execution, served by one process with different
// substream ids, both reach the finalize. Keyed by pid they shared one row,
// and whichever stream wrote last was the only one the finalize saw.
func TestTableInOutStateIsKeptPerSubstream(t *testing.T) {
	for _, transport := range tioTransports {
		t.Run(transport.name, func(t *testing.T) {
			h := newTioHarness(t, transport)
			primaryID, secondaryID := newSubstreamID(t), newSubstreamID(t)

			primary := h.openInput(nil, primaryID)
			exec := primary.executionID
			secondary := h.openInput(exec, secondaryID)
			if !bytes.Equal(secondary.executionID, exec) {
				t.Fatalf("the secondary did not join the execution: %x != %x", secondary.executionID, exec)
			}
			// Interleaved, as a fan-out runs them: both streams are live in
			// this process at once, each upserting its running total.
			for i := int64(0); i < 10; i++ {
				primary.send(i*100, i*100+100)
				secondary.send(1000+i*100, 1000+i*100+100)
			}
			primary.finish()
			secondary.finish()

			keys := h.storedKeys(exec)
			if len(keys) != 2 || !containsKey(keys, primaryID) || !containsKey(keys, secondaryID) {
				t.Fatalf("stored worker state under keys %x, want one row per substream (%x, %x)",
					keys, primaryID, secondaryID)
			}
			if got, want := h.finalize(exec, primaryID), sumRange(0, 2000); got != want {
				t.Fatalf("finalize summed %d, want %d: a substream's state was lost", got, want)
			}
		})
	}
}

// A client that sends no substream_id is keyed by process, exactly as before
// the key moved, and still finalizes correctly.
func TestTableInOutStateWithoutSubstreamIDIsKeyedByProcess(t *testing.T) {
	for _, transport := range tioTransports {
		t.Run(transport.name, func(t *testing.T) {
			h := newTioHarness(t, transport)

			stream := h.openInput(nil, nil)
			for i := int64(0); i < 10; i++ {
				stream.send(i*100, i*100+100)
			}
			stream.finish()

			pid := binary.BigEndian.AppendUint64(nil, uint64(os.Getpid()))
			if keys := h.storedKeys(stream.executionID); len(keys) != 1 || !bytes.Equal(keys[0], pid) {
				t.Fatalf("stored worker state under keys %x, want the one pid key %x", keys, pid)
			}
			if got, want := h.finalize(stream.executionID, nil), sumRange(0, 1000); got != want {
				t.Fatalf("finalize summed %d, want %d", got, want)
			}
		})
	}
}
