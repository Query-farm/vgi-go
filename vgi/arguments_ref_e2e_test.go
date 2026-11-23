// Copyright 2026 Query Farm LLC - https://query.farm

package vgi_test

// Argument references end to end (protocol 3.4, vgi docs/large-values-3.4.md
// §2-§6), against the worker's real HTTP handler and vgi-rpc's fake blob
// vendor, the way the extension drives them: one upload through the worker's
// __upload_url__ grant, a bind carrying only the sealed reference, a producer
// whose every turn rehydrates from the token (init StateRef + the argument
// through the object cache), then the refusals.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var refEchoSchema = arrow.NewSchema([]arrow.Field{
	{Name: "i", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "payload_len", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "payload_sha256", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

// refEcho is ref_echo(payload, batches): batches one-row batches of
// (i, strlen(payload), sha256(payload)), one per stream turn.
type refEcho struct{}

type refEchoState struct{ Next int64 }

func (*refEcho) Name() string { return "ref_echo" }

func (*refEcho) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "argument-reference e2e probe", Stability: vgi.StabilityConsistent}
}

func (*refEcho) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "payload", Position: 0, ArrowType: "varchar", IsConst: true, ArrowDataType: arrow.BinaryTypes.String},
		{Name: "batches", Position: 1, ArrowType: "int64", IsConst: true, ArrowDataType: arrow.PrimitiveTypes.Int64},
	}
}

func (*refEcho) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(refEchoSchema)
}

func (*refEcho) NewState(*vgi.ProcessParams) (*refEchoState, error) { return &refEchoState{}, nil }

func (*refEcho) Process(_ context.Context, params *vgi.ProcessParams, state *refEchoState, out *vgirpc.OutputCollector) error {
	n, err := params.Args.GetScalarInt64(1)
	if err != nil {
		return err
	}
	if state.Next >= n {
		return out.Finish()
	}
	payload, err := params.Args.GetScalarString(0)
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(payload))
	b := array.NewRecordBuilder(memory.DefaultAllocator, params.OutputSchema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(state.Next)
	b.Field(1).(*array.Int64Builder).Append(int64(len(payload)))
	b.Field(2).(*array.StringBuilder).Append(hex.EncodeToString(sum[:]))
	state.Next++
	return out.Emit(b.NewRecordBatch()) // the collector owns the batch
}

type refHarness struct {
	t      *testing.T
	vendor string
	url    string
	client *vgirpc.HttpClient
	header http.Header
}

func newRefHarness(t *testing.T, disable bool) *refHarness {
	t.Helper()
	vendor := fakeVendor(t)
	t.Setenv(vgirpc.EnvBlobVendorURL, vendor)
	t.Setenv(vgirpc.EnvBlobVendorToken, "test-token")
	t.Setenv("VGI_TEST_ALLOW_LOOPBACK_EXTERNAL_LOCATIONS", "1")
	t.Setenv(vgi.ArgumentsRefBytesEnv, "65536")
	if disable {
		t.Setenv(vgi.DisableArgumentsRefEnv, "1")
	}
	w := vgi.NewWorker()
	w.RegisterTable(vgi.AsTableFunction[refEchoState](&refEcho{}))
	handler, err := w.HttpHandlerForTest()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	client, err := vgirpc.NewHttpClient(ts.URL, vgirpc.WithClientProtocol(vgi.ProtocolName),
		vgirpc.WithClientProtocolVersion(vgi.ProtocolVersion))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	vgirpc.DefaultObjectCache().Clear()
	return &refHarness{t: t, vendor: vendor, url: ts.URL, client: client, header: resp.Header}
}

func (h *refHarness) stat(key string) int64 {
	h.t.Helper()
	resp, err := http.Get(h.vendor + "/__stats__")
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		h.t.Fatal(err)
	}
	v, ok := out[key].(float64)
	if !ok {
		h.t.Fatalf("no %q in the vendor's /__stats__", key)
	}
	return int64(v)
}

// arguments builds the arguments IPC stream of ref_echo(payload, batches).
func refArguments(t *testing.T, payload string, batches int64) []byte {
	argsSchema := arrow.NewSchema([]arrow.Field{{Name: "args", Type: arrow.StructOf(
		arrow.Field{Name: "positional_0", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "positional_1", Type: arrow.PrimitiveTypes.Int64},
	)}}, nil)
	return mustIPC(t, jsonRow(t, argsSchema, map[string]any{
		"args": map[string]any{"positional_0": payload, "positional_1": batches},
	}))
}

// upload puts args through the worker's grant and returns the location.
func (h *refHarness) upload(args []byte) map[string]any {
	t := h.t
	t.Helper()
	sum := sha256.Sum256(args)
	digest := hex.EncodeToString(sum[:])
	urls, err := h.client.RequestUploadURLs(context.Background(), []vgirpc.UploadRequest{{Size: int64(len(args)), SHA256: digest}})
	if err != nil {
		t.Fatal(err)
	}
	if len(urls[0].Ref) == 0 {
		t.Fatal("the grant minted no argument reference")
	}
	req, err := http.NewRequest(http.MethodPut, urls[0].UploadURL, bytes.NewReader(args))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range urls[0].Headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("PUT: %s", resp.Status)
	}
	return map[string]any{"ref": urls[0].Ref, "size": len(args), "sha256": digest}
}

func refBindRow(location map[string]any) map[string]any {
	return map[string]any{
		"function_name":             "ref_echo",
		"function_type":             string(vgi.FunctionTypeTable),
		"resolved_secrets_provided": false,
		"arguments_location":        location,
	}
}

func (h *refHarness) bind(row map[string]any) error {
	t := h.t
	t.Helper()
	params := jsonRow(t, generated.BindParamsSchema, map[string]any{
		"request": mustIPC(t, jsonRow(t, generated.BindRequestSchema, row)),
	})
	defer params.Release()
	res, err := h.client.CallUnary(context.Background(), "bind", params, nil)
	if err == nil {
		res.Release()
	}
	return err
}

// produce runs the producer to the end and returns its rows.
func (h *refHarness) produce(row map[string]any) ([][3]string, error) {
	t := h.t
	t.Helper()
	outputSchema, err := vgi.SerializeSchema(refEchoSchema)
	if err != nil {
		t.Fatal(err)
	}
	request := mustIPC(t, jsonRow(t, generated.InitRequestSchema, map[string]any{
		"bind_call":     mustIPC(t, jsonRow(t, generated.BindRequestSchema, row)),
		"output_schema": outputSchema,
	}))
	params := jsonRow(t, generated.InitParamsSchema, map[string]any{"request": request})
	defer params.Release()
	stream, err := h.client.OpenProducer(context.Background(), "init", params, vgirpc.ClientStreamSchema{HasHeader: true})
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	if hb := stream.Header(); hb != nil {
		hb.Release()
	}
	var rows [][3]string
	for {
		b, ok, err := stream.Next(context.Background())
		if err != nil {
			return rows, err
		}
		if !ok {
			return rows, nil
		}
		for r := 0; r < int(b.Batch.NumRows()); r++ {
			rows = append(rows, [3]string{
				strconv.FormatInt(b.Batch.Column(0).(*array.Int64).Value(r), 10),
				strconv.FormatInt(b.Batch.Column(1).(*array.Int64).Value(r), 10),
				b.Batch.Column(2).(*array.String).Value(r),
			})
		}
		b.Release()
	}
}

// rpcKind is the decoded error's kind and message (the client's message may
// carry the error type in front of the worker's text).
func rpcKind(err error) (string, string) {
	var re *vgirpc.RpcError
	if errors.As(err, &re) {
		return re.Kind, re.Message
	}
	return "", fmt.Sprint(err)
}

func TestArgumentsRefOverHTTP(t *testing.T) {
	h := newRefHarness(t, false)
	if got := h.header.Get(vgi.ArgumentsRefBytesHeader); got != "65536" {
		t.Fatalf("%s = %q, want 65536", vgi.ArgumentsRefBytesHeader, got)
	}

	payload := strings.Repeat("large argument ", 20000) // ~300 KB
	sum := sha256.Sum256([]byte(payload))
	digest := hex.EncodeToString(sum[:])
	const batches = 12
	location := h.upload(refArguments(t, payload, batches))
	row := refBindRow(location)

	if err := h.bind(row); err != nil {
		t.Fatalf("bind by reference: %v", err)
	}
	vgirpc.DefaultObjectCache().Clear()
	getsBefore := h.stat("gets_upload")
	rows, err := h.produce(row)
	if err != nil {
		t.Fatalf("producer over %d turns: %v", batches, err)
	}
	if len(rows) != batches {
		t.Fatalf("%d rows, want %d", len(rows), batches)
	}
	for i, r := range rows {
		if r != [3]string{strconv.Itoa(i), strconv.Itoa(len(payload)), digest} {
			t.Fatalf("row %d = %v", i, r)
		}
	}
	// Every turn rehydrates from the token and resolves the argument, but the
	// object cache keeps it: one vendor GET for the whole stream.
	if got := h.stat("gets_upload") - getsBefore; got != 1 {
		t.Fatalf("argument GETs = %d over %d turns, want 1", got, batches)
	}

	// A tampered reference does not open.
	tampered := map[string]any{"ref": append([]byte{}, location["ref"].([]byte)...), "size": location["size"], "sha256": location["sha256"]}
	tampered["ref"].([]byte)[len(tampered["ref"].([]byte))-1] ^= 1
	if kind, msg := rpcKind(h.bind(refBindRow(tampered))); kind != vgirpc.ErrorKindInvalidArgumentsRef || !strings.Contains(msg, "invalid_arguments_ref: ") {
		t.Fatalf("tampered ref: kind %q message %q", kind, msg)
	}

	// Both set: invalid_bind_request.
	both := refBindRow(location)
	both["arguments"] = []byte{}
	if kind, msg := rpcKind(h.bind(both)); kind != vgi.ErrorKindInvalidBindRequest || !strings.HasSuffix(msg, vgi.InvalidBindRequestMessage) {
		t.Fatalf("both set: kind %q message %q", kind, msg)
	}

	// The object expired (deleted) and this process's cache is cold: the
	// prepared-statement message.
	resp, err := http.Get(h.vendor + "/__admin__/delete?origin=client_upload")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	vgirpc.DefaultObjectCache().Clear()
	if kind, msg := rpcKind(h.bind(row)); kind != vgirpc.ErrorKindArgumentsRefMissing || !strings.HasSuffix(msg, vgi.ArgumentsRefMissingMessage) {
		t.Fatalf("deleted object: kind %q message %q", kind, msg)
	}
}

// VGI_TEST_DISABLE_ARGUMENTS_REF: the worker behaves like one without
// argument references (the extension's arguments_ref_fallback.test).
func TestArgumentsRefHeaderSwitch(t *testing.T) {
	h := newRefHarness(t, true)
	if got := h.header.Get(vgi.ArgumentsRefBytesHeader); got != "" {
		t.Fatalf("%s = %q with the switch on", vgi.ArgumentsRefBytesHeader, got)
	}
}

// init: StateRef stored form: an init recipe over the inline limit is stored
// once at init (one state_ref PUT) and every later turn carries only the
// sealed tag, resolving it through the object cache.
func TestInitStateRefStoredOverHTTP(t *testing.T) {
	t.Setenv(vgirpc.StateRefInlineBytesEnv, "1024")
	h := newRefHarness(t, false)
	payload := strings.Repeat("x", 8192) // inline argument, so the recipe is > 1 KiB
	row := refBindRow(nil)
	delete(row, "arguments_location")
	row["arguments"] = refArguments(t, payload, 6)
	puts := h.stat("puts_state_ref")
	rows, err := h.produce(row)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 6 {
		t.Fatalf("%d rows, want 6", len(rows))
	}
	if got := h.stat("puts_state_ref") - puts; got != 1 {
		t.Fatalf("state_ref PUTs = %d, want 1 (stored once at init)", got)
	}
}
