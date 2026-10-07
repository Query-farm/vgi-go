// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Query-farm/vgi-go/examples/attach_options"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// vgi-opaque-data-sealing.md over the worker's real HTTP handler, with no
// configured signing key: values come back sealed, open only for their owner,
// a transaction only under its own attach, every failure is the same error,
// and the worker's log never carries a value.

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type sealingE2E struct {
	t   *testing.T
	url string
	log *syncBuffer
}

func newSealingE2E(t *testing.T) *sealingE2E {
	t.Helper()
	t.Setenv("VGI_SIGNING_KEY", "")
	t.Setenv(vgirpc.GrantKeysEnv, "")
	logs := &syncBuffer{}
	w := vgi.NewWorker(
		vgi.WithCatalogName("example"),
		vgi.WithSupportsTransactions(true),
		vgi.WithLogHandler(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	)
	w.SetAuthenticate(headerAuthenticate)
	hs, err := w.NewHttpServerForTest()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(hs)
	t.Cleanup(ts.Close)
	return &sealingE2E{t: t, url: ts.URL, log: logs}
}

func (e *sealingE2E) client(principal string) *vgirpc.HttpClient {
	e.t.Helper()
	opts := []vgirpc.HttpClientOption{vgirpc.WithClientProtocol(vgi.ProtocolName),
		vgirpc.WithClientProtocolVersion(vgi.ProtocolVersion)}
	if principal != "" {
		opts = append(opts, vgirpc.WithClientHeader(grantE2EPrincipalHeader, principal))
	}
	c, err := vgirpc.NewHttpClient(e.url, opts...)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(c.Close)
	return c
}

func sealingAttach(t *testing.T, c *vgirpc.HttpClient, name string) []byte {
	t.Helper()
	request := mustIPC(t, jsonRow(t, generated.CatalogAttachRequestSchema, map[string]any{"name": name}))
	params := jsonRow(t, generated.CatalogAttachParamsSchema, map[string]any{"request": request})
	defer params.Release()
	res, err := c.CallUnary(context.Background(), "catalog_attach", params, nil)
	if err != nil {
		t.Fatalf("attach %s: %v", name, err)
	}
	defer res.Release()
	batch := resultStruct(t, res)
	defer batch.Release()
	return bytes.Clone(batch.Column(batch.Schema().FieldIndices("attach_opaque_data")[0]).(*array.Binary).Value(0))
}

func sealingBegin(t *testing.T, c *vgirpc.HttpClient, attach []byte) []byte {
	t.Helper()
	params := jsonRow(t, generated.CatalogTransactionBeginParamsSchema, map[string]any{"attach_opaque_data": attach})
	defer params.Release()
	res, err := c.CallUnary(context.Background(), "catalog_transaction_begin", params, nil)
	if err != nil {
		t.Fatalf("transaction_begin: %v", err)
	}
	defer res.Release()
	batch := resultStruct(t, res)
	defer batch.Release()
	return bytes.Clone(batch.Column(batch.Schema().FieldIndices("transaction_opaque_data")[0]).(*array.Binary).Value(0))
}

// catalogVersion opens both values, as every worker must to answer.
func catalogVersion(t *testing.T, c *vgirpc.HttpClient, attach, tx []byte) error {
	t.Helper()
	row := map[string]any{"attach_opaque_data": attach}
	if tx != nil {
		row["transaction_opaque_data"] = tx
	}
	params := jsonRow(t, generated.CatalogVersionParamsSchema, row)
	defer params.Release()
	res, err := c.CallUnary(context.Background(), "catalog_version", params, nil)
	if err == nil {
		res.Release()
	}
	return err
}

func rpcShape(err error) string {
	if err == nil {
		return "<accepted>"
	}
	var rpcErr *vgirpc.RpcError
	if !strings.Contains(fmt.Sprintf("%T", err), "RpcError") {
		return err.Error()
	}
	rpcErr = err.(*vgirpc.RpcError)
	return fmt.Sprintf("%s|%s|%s|%d", rpcErr.Code, rpcErr.Kind, rpcErr.Message, len(rpcErr.Details))
}

func TestOpaqueSealingOverHTTP(t *testing.T) {
	e := newSealingE2E(t)
	alice, bob := e.client("alice"), e.client("bob")

	attach := sealingAttach(t, alice, "example")
	other := sealingAttach(t, alice, "example")
	tx := sealingBegin(t, alice, attach)
	if bytes.Contains(attach, []byte("example")) {
		t.Fatal("the attach value carries the catalog name in plaintext: not sealed")
	}
	if err := catalogVersion(t, alice, attach, tx); err != nil {
		t.Fatalf("the owner's own values: %v", err)
	}

	flip := func(b []byte, i int) []byte { out := bytes.Clone(b); out[i] ^= 1; return out }
	attachCases := map[string]error{
		"replayed by bob":         catalogVersion(t, bob, attach, nil),
		"anonymous":               catalogVersion(t, e.client(""), attach, nil),
		"first byte flipped":      catalogVersion(t, alice, flip(attach, 0), nil),
		"middle flipped":          catalogVersion(t, alice, flip(attach, len(attach)/2), nil),
		"last byte flipped":       catalogVersion(t, alice, flip(attach, len(attach)-1), nil),
		"plaintext uuid||catalog": catalogVersion(t, alice, append(make([]byte, 16), "example"...), nil),
		"writable: prefix":        catalogVersion(t, alice, []byte("writable:example"), nil),
	}
	want := ""
	for name, err := range attachCases {
		got := rpcShape(err)
		// What a client sees, exactly: the classified code and kind, the bare
		// message, no details.
		if got != "INVALID_ARGUMENT|opaque_data_not_recognized|attach_opaque_data not recognized|0" {
			t.Fatalf("attach %s: %s", name, got)
		}
		if want == "" {
			want = got
		} else if got != want {
			t.Fatalf("attach %s: %s, not the uniform %s", name, got, want)
		}
	}
	txCases := map[string]error{
		"under another attach": catalogVersion(t, alice, other, tx),
		"first byte flipped":   catalogVersion(t, alice, attach, flip(tx, 0)),
		"last byte flipped":    catalogVersion(t, alice, attach, flip(tx, len(tx)-1)),
		"plaintext uuid":       catalogVersion(t, alice, attach, make([]byte, 16)),
	}
	txWant := strings.Replace(want, "attach_opaque_data", "transaction_opaque_data", 1)
	for name, err := range txCases {
		if got := rpcShape(err); got != txWant {
			t.Fatalf("transaction %s: %s, want %s", name, got, txWant)
		}
	}
	// A transaction replayed by bob under alice's attach fails on the attach.
	if got := rpcShape(catalogVersion(t, bob, attach, tx)); got != want {
		t.Fatalf("bob's replay: %s", got)
	}

	// Never log raw: no 12-byte window of either value, as hex or base64.
	// (The mutation check that logs a raw value proves this check is live.)
	logs := e.log.String()
	for _, v := range [][]byte{attach, other, tx} {
		h := hex.EncodeToString(v)
		for i := 0; i+24 <= len(h); i += 2 {
			if strings.Contains(logs, h[i:i+24]) {
				t.Fatalf("the worker log carries a value's hex: %s", h[i:i+24])
			}
		}
		if strings.Contains(logs, base64.StdEncoding.EncodeToString(v)[:16]) {
			t.Fatal("the worker log carries a value's base64")
		}
	}
}

// A secret attach option never travels in plaintext, on an unsealed transport
// either.
func TestSecretAttachOptionNotInTheValue(t *testing.T) {
	const secret = "sk-secret-opaque-0123456789"
	newWorker := func() *vgi.Worker {
		return vgi.NewWorker(
			vgi.WithCatalogName(attach_options.CatalogName),
			vgi.WithAttachOptions(attach_options.AttachOptionSpecs()...),
			vgi.WithCatalogAliasInfo(attach_options.RequiredCatalogName, vgi.CatalogInfo{Name: attach_options.RequiredCatalogName}),
			vgi.WithAttachOptionsForCatalog(attach_options.RequiredCatalogName, attach_options.RequiredCatalogAttachOptionSpecs()...),
			vgi.WithAttachValidator(func(req *vgi.CatalogAttachRequestWire, _ *vgirpc.CallContext) (*vgi.AttachDecision, error) {
				var opt []byte
				if req.Options != nil {
					opt = *req.Options
				}
				data, err := attach_options.EncodeAttachOpaqueData(opt)
				if err != nil {
					return nil, err
				}
				return &vgi.AttachDecision{AttachOpaqueData: data}, nil
			}),
		)
	}
	options := mustIPC(t, jsonRow(t, arrow.NewSchema([]arrow.Field{
		{Name: "api_key", Type: arrow.BinaryTypes.String},
		{Name: "region", Type: arrow.BinaryTypes.String},
	}, nil), map[string]any{"api_key": secret, "region": "eu-west-2"}))

	attachWith := func(c interface {
		CallUnary(context.Context, string, arrow.RecordBatch, *arrow.Schema) (*vgirpc.ClientBatch, error)
	}, name string) []byte {
		request := mustIPC(t, jsonRow(t, generated.CatalogAttachRequestSchema, map[string]any{"name": name, "options": options}))
		params := jsonRow(t, generated.CatalogAttachParamsSchema, map[string]any{"request": request})
		defer params.Release()
		res, err := c.CallUnary(context.Background(), "catalog_attach", params, nil)
		if err != nil {
			t.Fatalf("attach %s: %v", name, err)
		}
		defer res.Release()
		batch := resultStruct(t, res)
		defer batch.Release()
		return bytes.Clone(batch.Column(batch.Schema().FieldIndices("attach_opaque_data")[0]).(*array.Binary).Value(0))
	}
	assertAbsent := func(where string, v []byte) {
		for _, form := range []string{secret, hex.EncodeToString([]byte(secret)), base64.StdEncoding.EncodeToString([]byte(secret))} {
			if bytes.Contains(v, []byte(form)) || strings.Contains(hex.EncodeToString(v), form) ||
				strings.Contains(base64.StdEncoding.EncodeToString(v), form) {
				t.Fatalf("%s: the secret option is in the attach value", where)
			}
		}
	}

	// Raw TCP: the unsealed transport.
	bound := make(chan int, 1)
	go func() {
		_ = newWorker().ServeTcpForTest(5*time.Second, func(_ string, port int) { bound <- port })
	}()
	port := <-bound
	tcp, err := vgirpc.NewTcpClient(context.Background(), "127.0.0.1", port,
		vgirpc.WithTcpClientProtocol(vgi.ProtocolName), vgirpc.WithTcpClientProtocolVersion(vgi.ProtocolVersion))
	if err != nil {
		t.Fatal(err)
	}
	defer tcp.Close()
	for _, name := range []string{attach_options.CatalogName, attach_options.RequiredCatalogName} {
		assertAbsent("tcp "+name, attachWith(tcp, name))
	}

	// HTTP: sealed.
	t.Setenv("VGI_SIGNING_KEY", "")
	hs, err := newWorker().NewHttpServerForTest()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(hs)
	defer ts.Close()
	httpc, err := vgirpc.NewHttpClient(ts.URL, vgirpc.WithClientProtocol(vgi.ProtocolName),
		vgirpc.WithClientProtocolVersion(vgi.ProtocolVersion))
	if err != nil {
		t.Fatal(err)
	}
	defer httpc.Close()
	for _, name := range []string{attach_options.CatalogName, attach_options.RequiredCatalogName} {
		assertAbsent("http "+name, attachWith(httpc, name))
	}
}
