// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/Query-farm/vgi-go/examples/writable_tx"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// The transaction hooks, per-ATTACH supports_transactions and the
// functions' TransactionOpaqueData over the worker's real HTTP handler, where
// the SDK seals attach and transaction values on the wire.

var txfixOptionsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "transactional", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	{Name: "token", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

// attachResult runs catalog_attach with options and returns (attach value,
// supports_transactions).
func attachWithOptions(t *testing.T, c *contentsClient, name string, options map[string]any) ([]byte, bool) {
	t.Helper()
	req := map[string]any{"name": name}
	if options != nil {
		req["options"] = mustIPC(t, jsonRow(t, txfixOptionsSchema, options))
	}
	request := mustIPC(t, jsonRow(t, generated.CatalogAttachRequestSchema, req))
	res := c.call("catalog_attach", generated.CatalogAttachParamsSchema, map[string]any{"request": request})
	defer res.Release()
	attach := column[*array.Binary](t, res, "attach_opaque_data").Value(0)
	return bytes.Clone(attach), column[*array.Boolean](t, res, "supports_transactions").Value(0)
}

func beginTx(t *testing.T, c *contentsClient, attach []byte) []byte {
	t.Helper()
	res := c.call("catalog_transaction_begin", generated.CatalogTransactionBeginParamsSchema, map[string]any{"attach_opaque_data": attach})
	defer res.Release()
	col := column[*array.Binary](t, res, "transaction_opaque_data")
	if col.IsNull(0) {
		return nil
	}
	return bytes.Clone(col.Value(0))
}

func endTx(c *contentsClient, method string, attach, tx []byte) error {
	schema := generated.CatalogTransactionCommitParamsSchema
	if method == "catalog_transaction_rollback" {
		schema = generated.CatalogTransactionRollbackParamsSchema
	}
	return c.exec(method, schema, map[string]any{"attach_opaque_data": attach, "transaction_opaque_data": tx})
}

// scanTable binds and scans a no-argument table function through attach (and
// tx, when non-nil), returning its rows as strings keyed by column.
func scanTable(t *testing.T, c *contentsClient, attach, tx []byte, fn string) ([]map[string]string, error) {
	t.Helper()
	row := map[string]any{
		"function_name":             fn,
		"arguments":                 []byte{},
		"function_type":             string(vgi.FunctionTypeTable),
		"attach_opaque_data":        attach,
		"resolved_secrets_provided": false,
		"schema_path":               []string{"main"},
	}
	if tx != nil {
		row["transaction_opaque_data"] = tx
	}
	bindCall := mustIPC(t, jsonRow(t, generated.BindRequestSchema, row))
	bindParams := jsonRow(t, generated.BindParamsSchema, map[string]any{"request": bindCall})
	defer bindParams.Release()
	bound, err := c.client.CallUnary(context.Background(), "bind", bindParams, nil)
	if err != nil {
		return nil, err
	}
	defer bound.Release()
	resp := resultStruct(t, bound)
	defer resp.Release()
	initRow := map[string]any{
		"bind_call":     bindCall,
		"output_schema": column[*array.Binary](t, resp, "output_schema").Value(0),
	}
	if opaque := column[*array.Binary](t, resp, "opaque_data"); !opaque.IsNull(0) {
		initRow["bind_opaque_data"] = opaque.Value(0)
	}
	request := mustIPC(t, jsonRow(t, generated.InitRequestSchema, initRow))
	initParams := jsonRow(t, generated.InitParamsSchema, map[string]any{"request": request})
	defer initParams.Release()
	stream, err := c.client.OpenProducer(context.Background(), "init", initParams, vgirpc.ClientStreamSchema{HasHeader: true})
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	if h := stream.Header(); h != nil {
		h.Release()
	}
	var out []map[string]string
	for {
		batch, ok, err := stream.Next(context.Background())
		if err != nil {
			return nil, err
		}
		if !ok {
			return out, nil
		}
		b := batch.Batch
		for r := 0; r < int(b.NumRows()); r++ {
			m := map[string]string{}
			for ci, f := range b.Schema().Fields() {
				if b.Column(ci).IsNull(r) {
					m[f.Name] = "NULL"
				} else {
					m[f.Name] = b.Column(ci).ValueStr(r)
				}
			}
			out = append(out, m)
		}
		batch.Release()
	}
}

func mustScan(t *testing.T, c *contentsClient, attach, tx []byte, fn string) []map[string]string {
	t.Helper()
	rows, err := scanTable(t, c, attach, tx, fn)
	if err != nil {
		t.Fatalf("%s: %v", fn, err)
	}
	return rows
}

func newTxfixClient(t *testing.T) *contentsClient {
	t.Helper()
	t.Setenv("VGI_SIGNING_KEY", "")
	t.Setenv(vgirpc.GrantKeysEnv, "")
	t.Setenv("VGI_TXFIX_STATE_DIR", t.TempDir())
	w := vgi.NewWorker(writable_tx.Options()...)
	writable_tx.Register(w)
	return newContentsClient(t, w)
}

func TestTxfixTransactionsOverHTTP(t *testing.T) {
	c := newTxfixClient(t)
	plain, plainTx := attachWithOptions(t, c, "txfix", map[string]any{"token": "tok-123"})
	ttx, ttxTx := attachWithOptions(t, c, "txfix", map[string]any{"token": "tok-123", "transactional": true})
	if bytes.Contains(plain, []byte("tok-123")) || bytes.Contains(ttx, []byte("tok-123")) {
		t.Fatal("the secret token is on the wire in the attach value")
	}
	if plainTx || !ttxTx {
		t.Fatalf("supports_transactions: plain %v, transactional %v", plainTx, ttxTx)
	}
	mustScan(t, c, plain, nil, "reset")
	for _, attach := range [][]byte{plain, ttx} {
		if who := mustScan(t, c, attach, nil, "whoami"); who[0]["token"] != "tok-123" || who[0]["token_in_attach_data"] != "false" {
			t.Fatalf("whoami %v", who)
		}
	}
	if tx := beginTx(t, c, plain); tx != nil {
		t.Fatal("the non-transactional ATTACH's begin hook started a transaction")
	}

	sealed := beginTx(t, c, ttx)
	if len(sealed) <= 16 {
		t.Fatalf("transaction value not sealed on HTTP (%d bytes)", len(sealed))
	}
	cur := mustScan(t, c, ttx, sealed, "current_tx")
	txID := cur[0]["tx_id"]
	if cur[0]["in_tx"] != "true" || len(txID) != 32 {
		t.Fatalf("current_tx %v", cur)
	}
	// The function saw the hook's plaintext id; a client presenting that
	// plaintext (the old workaround's forgeable path) is refused.
	raw, _ := hex.DecodeString(txID)
	if _, err := scanTable(t, c, ttx, raw, "current_tx"); err == nil {
		t.Fatal("a plaintext transaction id was accepted over HTTP")
	}
	if err := endTx(c, "catalog_transaction_commit", ttx, sealed); err != nil {
		t.Fatalf("commit: %v", err)
	}
	events := mustScan(t, c, plain, nil, "events")
	if len(events) != 2 || events[0]["kind"] != "begin" || events[1]["kind"] != "commit" ||
		events[0]["tx_id"] != txID || events[1]["tx_id"] != txID || events[1]["ok"] != "true" {
		t.Fatalf("the hooks did not see the functions' transaction id %s: %v", txID, events)
	}

	// Rollback reaches its hook with the same opened id.
	sealed2 := beginTx(t, c, ttx)
	if err := endTx(c, "catalog_transaction_rollback", ttx, sealed2); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	events = mustScan(t, c, plain, nil, "events")
	if last := events[len(events)-1]; last["kind"] != "rollback" || last["tx_id"] != events[len(events)-2]["tx_id"] {
		t.Fatalf("rollback event %v", events)
	}
	// Outside a transaction the functions see none.
	if cur := mustScan(t, c, plain, nil, "current_tx"); cur[0]["in_tx"] != "false" || cur[0]["tx_id"] != "NULL" {
		t.Fatalf("current_tx outside a transaction: %v", cur)
	}
}

// txProbe records what InitParams and ProcessParams carried.
type txProbe struct{ seen *sync.Map }

func (txProbe) Name() string { return "tx_probe" }
func (txProbe) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Stability: vgi.StabilityVolatile}
}
func (txProbe) ArgumentSpecs() []vgi.ArgSpec { return nil }
func (txProbe) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil))
}
func (p txProbe) OnInit(ip *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	p.seen.Store("init", bytes.Clone(ip.TransactionOpaqueData))
	p.seen.Store("init_attach", bytes.Clone(ip.AttachScope))
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}
func (txProbe) NewState(*vgi.ProcessParams) (*struct{ Done bool }, error) {
	return &struct{ Done bool }{}, nil
}
func (p txProbe) Process(_ context.Context, pp *vgi.ProcessParams, st *struct{ Done bool }, out *vgirpc.OutputCollector) error {
	p.seen.Store("process", bytes.Clone(pp.TransactionOpaqueData))
	return out.Finish()
}

func TestTransactionHooksOverHTTP(t *testing.T) {
	t.Setenv("VGI_SIGNING_KEY", "")
	t.Setenv(vgirpc.GrantKeysEnv, "")
	var mu sync.Mutex
	var begun, committed, rolledBack []byte
	var hookAttach []byte
	seen := &sync.Map{}
	w := vgi.NewWorker(
		vgi.WithCatalogName("hooks"),
		vgi.WithSupportsTransactions(true),
		vgi.WithTransactionHooks(vgi.TransactionHooks{
			Begin: func(_ context.Context, tx vgi.TransactionHookContext) ([]byte, error) {
				mu.Lock()
				defer mu.Unlock()
				begun = []byte("tx-" + string(rune('a'+len(begun)%26)))
				hookAttach = bytes.Clone(tx.AttachOpaqueData)
				return begun, nil
			},
			Commit: func(_ context.Context, tx vgi.TransactionHookContext) error {
				mu.Lock()
				defer mu.Unlock()
				committed = bytes.Clone(tx.TransactionOpaqueData)
				return errors.New("hooks: commit refused: qty -1 for id 2")
			},
			Rollback: func(_ context.Context, tx vgi.TransactionHookContext) error {
				mu.Lock()
				defer mu.Unlock()
				rolledBack = bytes.Clone(tx.TransactionOpaqueData)
				return nil
			},
		}),
	)
	w.RegisterTable(vgi.AsTableFunction[struct{ Done bool }](txProbe{seen: seen}))
	c := newContentsClient(t, w)
	attach, supports := attachWithOptions(t, c, "hooks", nil)
	if !supports {
		t.Fatal("WithSupportsTransactions(true) not reported")
	}
	sealed := beginTx(t, c, attach)
	if bytes.Equal(sealed, begun) {
		t.Fatal("the Begin hook's id left the worker unsealed")
	}
	if string(hookAttach) != "hooks" {
		t.Fatalf("Begin hook saw attach %q, not the opened catalog bytes", hookAttach)
	}
	mustScan(t, c, attach, sealed, "tx_probe")
	for _, k := range []string{"init", "process"} {
		v, _ := seen.Load(k)
		if !bytes.Equal(v.([]byte), begun) {
			t.Fatalf("%s saw transaction %q, want the hook's %q", k, v, begun)
		}
	}
	if v, _ := seen.Load("init_attach"); string(v.([]byte)) != "hooks" {
		t.Fatalf("InitParams.AttachScope %q", v)
	}
	err := endTx(c, "catalog_transaction_commit", attach, sealed)
	if err == nil || !strings.Contains(err.Error(), "hooks: commit refused: qty -1 for id 2") {
		t.Fatalf("a failing Commit hook must fail COMMIT with its message, got %v", err)
	}
	if !bytes.Equal(committed, begun) {
		t.Fatalf("Commit hook saw %q (sealed?), want %q", committed, begun)
	}
	sealed = beginTx(t, c, attach)
	if err := endTx(c, "catalog_transaction_rollback", attach, sealed); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rolledBack, begun) {
		t.Fatalf("Rollback hook saw %q, want %q", rolledBack, begun)
	}
	// A forged (plaintext) id never reaches a hook.
	if err := endTx(c, "catalog_transaction_rollback", attach, begun); err == nil {
		t.Fatal("rollback accepted a plaintext transaction id")
	}
}
