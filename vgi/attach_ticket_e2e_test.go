// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"math"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Query-farm/vgi-go/examples/ticket_probe"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// The attach-ticket loop end to end over the worker's real HTTP handler, the
// shape the fixture worker serves: ticket_probe as a sub-catalog of another
// catalog, so redemption must happen before routing.
//
//  1. alice, a fresh login, attaches ticket_probe with region and api_key and
//     reads the probe row;
//  2. she calls seal_attach and issue_grant;
//  3. a second client holding only "Bearer <grant>" attaches with only
//     {vgi_attach_ticket: <ticket>} and reads the same row;
//  4. bob's grant with alice's ticket is refused.

const ticketSigningKey = "attach-ticket-e2e-signing-key-0001"

type ticketE2E struct {
	t   *testing.T
	url string
}

func newTicketE2E(t *testing.T) *ticketE2E {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 7)
	}
	keys, err := vgirpc.NewGrantKeys([][]byte{key}, "tickets", 3600)
	if err != nil {
		t.Fatal(err)
	}
	w := vgi.NewWorker(vgi.WithCatalogName("example"), vgi.WithGrantKeys(keys))
	ticket_probe.Register(w)
	t.Setenv(vgi.SigningKeyEnv, ticketSigningKey)
	w.SetAuthenticate(headerAuthenticate)
	hs, err := w.NewHttpServerForTest()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(hs)
	t.Cleanup(ts.Close)
	return &ticketE2E{t: t, url: ts.URL}
}

func (e *ticketE2E) client(protocol, version string, headers ...string) *vgirpc.HttpClient {
	e.t.Helper()
	opts := []vgirpc.HttpClientOption{vgirpc.WithClientProtocol(protocol)}
	if version != "" {
		opts = append(opts, vgirpc.WithClientProtocolVersion(version))
	}
	for i := 0; i+1 < len(headers); i += 2 {
		opts = append(opts, vgirpc.WithClientHeader(headers[i], headers[i+1]))
	}
	c, err := vgirpc.NewHttpClient(e.url, opts...)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(c.Close)
	return c
}

// optionsIPC serializes a one-row options record of string columns, in order.
func optionsIPC(t *testing.T, kv ...string) []byte {
	t.Helper()
	fields := make([]arrow.Field, 0, len(kv)/2)
	row := map[string]any{}
	for i := 0; i+1 < len(kv); i += 2 {
		fields = append(fields, arrow.Field{Name: kv[i], Type: arrow.BinaryTypes.String})
		row[kv[i]] = kv[i+1]
	}
	return mustIPC(t, jsonRow(t, arrow.NewSchema(fields, nil), row))
}

// attach runs catalog_attach and returns the attach_opaque_data, or the error.
func attachCatalog(t *testing.T, c *vgirpc.HttpClient, name string, options []byte) ([]byte, error) {
	t.Helper()
	req := map[string]any{"name": name}
	if options != nil {
		req["options"] = options
	}
	request := mustIPC(t, jsonRow(t, generated.CatalogAttachRequestSchema, req))
	params := jsonRow(t, generated.CatalogAttachParamsSchema, map[string]any{"request": request})
	defer params.Release()
	res, err := c.CallUnary(context.Background(), "catalog_attach", params, nil)
	if err != nil {
		return nil, err
	}
	defer res.Release()
	batch := resultStruct(t, res)
	defer batch.Release()
	col := batch.Column(batch.Schema().FieldIndices("attach_opaque_data")[0]).(*array.Binary)
	return append([]byte{}, col.Value(0)...), nil
}

// readProbe binds and scans main.ticket_probe through attach, returning
// (region, api_key_sha256).
func readProbe(t *testing.T, c *vgirpc.HttpClient, attach []byte) (string, string) {
	t.Helper()
	bindCall := mustIPC(t, jsonRow(t, generated.BindRequestSchema, map[string]any{
		"function_name":             "ticket_probe",
		"arguments":                 []byte{},
		"function_type":             string(vgi.FunctionTypeTable),
		"attach_opaque_data":        attach,
		"resolved_secrets_provided": false,
		"schema_path":               []string{"main"},
	}))
	bindParams := jsonRow(t, generated.BindParamsSchema, map[string]any{"request": bindCall})
	defer bindParams.Release()
	bound, err := c.CallUnary(context.Background(), "bind", bindParams, nil)
	if err != nil {
		t.Fatalf("bind ticket_probe: %v", err)
	}
	defer bound.Release()
	resp := resultStruct(t, bound)
	defer resp.Release()
	outputSchema := resp.Column(resp.Schema().FieldIndices("output_schema")[0]).(*array.Binary).Value(0)
	opaque := resp.Column(resp.Schema().FieldIndices("opaque_data")[0]).(*array.Binary).Value(0)

	request := mustIPC(t, jsonRow(t, generated.InitRequestSchema, map[string]any{
		"bind_call":        bindCall,
		"output_schema":    outputSchema,
		"bind_opaque_data": opaque,
	}))
	initParams := jsonRow(t, generated.InitParamsSchema, map[string]any{"request": request})
	defer initParams.Release()
	stream, err := c.OpenProducer(context.Background(), "init", initParams, vgirpc.ClientStreamSchema{HasHeader: true})
	if err != nil {
		t.Fatalf("init ticket_probe: %v", err)
	}
	defer stream.Close()
	if h := stream.Header(); h != nil {
		h.Release()
	}
	for {
		batch, ok, err := stream.Next(context.Background())
		if err != nil {
			t.Fatalf("scanning ticket_probe: %v", err)
		}
		if !ok {
			t.Fatal("ticket_probe produced no row")
		}
		if batch.Batch.NumRows() == 0 {
			batch.Release()
			continue
		}
		defer batch.Release()
		region := batch.Batch.Column(0).(*array.String).Value(0)
		digest := batch.Batch.Column(1).(*array.String).Value(0)
		return region, digest
	}
}

func unaryStruct(t *testing.T, c *vgirpc.HttpClient, method string, schema *arrow.Schema, row map[string]any) (arrow.RecordBatch, error) {
	t.Helper()
	params := jsonRow(t, schema, row)
	defer params.Release()
	res, err := c.CallUnary(context.Background(), method, params, nil)
	if err != nil {
		return nil, err
	}
	defer res.Release()
	return resultStruct(t, res), nil
}

var sealAttachRequestSchema = arrow.NewSchema([]arrow.Field{
	{Name: "catalog_name", Type: arrow.BinaryTypes.String},
	{Name: "options", Type: arrow.BinaryTypes.Binary, Nullable: true},
	{Name: "data_version_spec", Type: arrow.BinaryTypes.String},
	{Name: "implementation_version", Type: arrow.BinaryTypes.String},
	{Name: "ttl_seconds", Type: arrow.PrimitiveTypes.Int64},
}, nil)

func sealAttach(t *testing.T, c *vgirpc.HttpClient, catalog string, options []byte, ttl int64) (string, float64, error) {
	t.Helper()
	row := map[string]any{"catalog_name": catalog, "data_version_spec": "", "implementation_version": "", "ttl_seconds": ttl}
	if options != nil {
		row["options"] = options
	}
	request := mustIPC(t, jsonRow(t, sealAttachRequestSchema, row))
	out, err := unaryStruct(t, c, "seal_attach", arrow.NewSchema([]arrow.Field{
		{Name: "request", Type: arrow.BinaryTypes.Binary}}, nil), map[string]any{"request": request})
	if err != nil {
		return "", 0, err
	}
	defer out.Release()
	ticket := out.Column(out.Schema().FieldIndices("ticket")[0]).(*array.String).Value(0)
	expires := out.Column(out.Schema().FieldIndices("expires_at")[0]).(*array.Float64).Value(0)
	return ticket, expires, nil
}

func issueGrant(t *testing.T, e *ticketE2E, principal string) string {
	t.Helper()
	identity := e.client(vgirpc.IdentityProtocolName, "", grantE2EPrincipalHeader, principal)
	out, err := unaryStruct(t, identity, "issue_grant", arrow.NewSchema([]arrow.Field{
		{Name: "purpose", Type: arrow.BinaryTypes.String},
		{Name: "scopes", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		{Name: "ttl_seconds", Type: arrow.PrimitiveTypes.Int64},
	}, nil), map[string]any{"purpose": "reattach", "scopes": []string{}, "ttl_seconds": 600})
	if err != nil {
		t.Fatalf("issue_grant as %s: %v", principal, err)
	}
	defer out.Release()
	return out.Column(out.Schema().FieldIndices("token")[0]).(*array.String).Value(0)
}

func errorKind(err error) string {
	var rpcErr *vgirpc.RpcError
	if errors.As(err, &rpcErr) {
		return rpcErr.Kind
	}
	return ""
}

func TestAttachTicketEndToEnd(t *testing.T) {
	e := newTicketE2E(t)
	const apiKey = "sk-test-0123456789"
	options := optionsIPC(t, "region", "eu-west-2", "api_key", apiKey)

	// 1. alice attaches with her options and reads the row.
	alice := e.client(vgi.ProtocolName, vgi.ProtocolVersion, grantE2EPrincipalHeader, "alice")
	attach, err := attachCatalog(t, alice, ticket_probe.CatalogName, options)
	if err != nil {
		t.Fatalf("alice's attach: %v", err)
	}
	region, digest := readProbe(t, alice, attach)
	if region != "eu-west-2" || digest != "0d3b56072291" {
		t.Fatalf("alice reads (%q, %q); want (eu-west-2, 0d3b56072291)", region, digest)
	}

	// 2. seal_attach and issue_grant, as alice.
	tickets := e.client(vgi.AttachTicketsProtocolName, vgi.AttachTicketsProtocolVersion, grantE2EPrincipalHeader, "alice")
	ticket, expiresAt, err := sealAttach(t, tickets, ticket_probe.CatalogName, options, 0)
	if err != nil {
		t.Fatalf("seal_attach: %v", err)
	}
	if !strings.HasPrefix(ticket, vgi.AttachTicketPrefix) || math.IsInf(expiresAt, 0) {
		t.Fatalf("seal_attach = (%.12q..., %v); want a vgia1. ticket capped at the grant maximum", ticket, expiresAt)
	}
	grant := issueGrant(t, e, "alice")

	// 3. A runner with only the grant and the ticket reads the same row.
	runner := e.client(vgi.ProtocolName, vgi.ProtocolVersion, "Authorization", "Bearer "+grant)
	ticketOnly := optionsIPC(t, vgi.AttachTicketOption, ticket)
	reattach, err := attachCatalog(t, runner, "whatever-the-runner-typed", ticketOnly)
	if err != nil {
		t.Fatalf("reattach with the ticket: %v", err)
	}
	if r, d := readProbe(t, runner, reattach); r != region || d != digest {
		t.Fatalf("the runner reads (%q, %q); alice read (%q, %q)", r, d, region, digest)
	}

	// Without the ticket the runner cannot attach: api_key is required.
	if _, err := attachCatalog(t, runner, ticket_probe.CatalogName, nil); err == nil {
		t.Fatal("the runner attached ticket_probe without api_key")
	}
	// Another option beside the ticket is invalid_request.
	if _, err := attachCatalog(t, runner, "x", optionsIPC(t, vgi.AttachTicketOption, ticket, "region", "us-west-1")); errorKind(err) != "invalid_request" {
		t.Fatalf("ticket plus another option: %v; want invalid_request", err)
	}

	// 4. bob's grant does not open alice's ticket.
	bobGrant := issueGrant(t, e, "bob")
	bob := e.client(vgi.ProtocolName, vgi.ProtocolVersion, "Authorization", "Bearer "+bobGrant)
	_, err = attachCatalog(t, bob, ticket_probe.CatalogName, ticketOnly)
	if errorKind(err) != "attach_ticket_invalid" {
		t.Fatalf("bob redeeming alice's ticket: %v; want attach_ticket_invalid", err)
	}
	if strings.Contains(err.Error(), ticket) {
		t.Fatal("the error echoes the ticket")
	}
	// An anonymous caller neither seals nor redeems.
	anon := e.client(vgi.AttachTicketsProtocolName, vgi.AttachTicketsProtocolVersion)
	if _, _, err := sealAttach(t, anon, ticket_probe.CatalogName, options, 0); errorKind(err) != "action_denied" {
		t.Fatalf("anonymous seal_attach: %v; want action_denied", err)
	}
}

func TestSealAttachValidation(t *testing.T) {
	e := newTicketE2E(t)
	alice := e.client(vgi.AttachTicketsProtocolName, vgi.AttachTicketsProtocolVersion, grantE2EPrincipalHeader, "alice")
	cases := map[string]struct {
		catalog string
		options []byte
		ttl     int64
		field   string
	}{
		"unknown catalog":   {"nope", nil, 0, "catalog_name"},
		"missing required":  {ticket_probe.CatalogName, optionsIPC(t, "region", "x"), 0, "options.api_key"},
		"undeclared option": {ticket_probe.CatalogName, optionsIPC(t, "api_key", "k", "colour", "red"), 0, "options.colour"},
		"ticket inside":     {ticket_probe.CatalogName, optionsIPC(t, "api_key", "k", "VGI_Attach_Ticket", "x"), 0, "options.VGI_Attach_Ticket"},
		"negative lifetime": {ticket_probe.CatalogName, optionsIPC(t, "api_key", "k"), -1, "ttl_seconds"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := sealAttach(t, alice, c.catalog, c.options, c.ttl)
			var rpcErr *vgirpc.RpcError
			if !errors.As(err, &rpcErr) || rpcErr.Kind != "invalid_request" {
				t.Fatalf("got %v; want invalid_request", err)
			}
			found := false
			for _, d := range rpcErr.Details {
				if fv, ok := d["field_violations"].([]any); ok {
					for _, v := range fv {
						if m, ok := v.(map[string]any); ok && m["field"] == c.field {
							found = true
						}
					}
				}
			}
			if !found {
				t.Fatalf("no field violation %q in %v", c.field, rpcErr.Details)
			}
		})
	}
	// A positive lifetime is capped at the grant maximum (3600 s).
	_, expires, err := sealAttach(t, alice, ticket_probe.CatalogName, optionsIPC(t, "api_key", "k"), 999999)
	if err != nil {
		t.Fatal(err)
	}
	_, short, err := sealAttach(t, alice, ticket_probe.CatalogName, optionsIPC(t, "api_key", "k"), 60)
	if err != nil {
		t.Fatal(err)
	}
	if expires-short < 3600-60-5 || expires-short > 3600-60+5 {
		t.Fatalf("lifetimes: capped %v, short %v", expires, short)
	}
}

// ticket_probe's secret api_key never travels in its attach value, on an
// unsealed transport either (vgi-opaque-data-sealing.md rule 5): the catalog
// keeps only region and sha256(api_key)[:12].
func TestTicketProbeAttachValueCarriesNoSecret(t *testing.T) {
	const apiKey = "sk-test-0123456789"
	w := vgi.NewWorker(vgi.WithCatalogName("example"))
	ticket_probe.Register(w)
	bound := make(chan int, 1)
	go func() { _ = w.ServeTcpForTest(5*time.Second, func(_ string, port int) { bound <- port }) }()
	tcp, err := vgirpc.NewTcpClient(context.Background(), "127.0.0.1", <-bound,
		vgirpc.WithTcpClientProtocol(vgi.ProtocolName), vgirpc.WithTcpClientProtocolVersion(vgi.ProtocolVersion))
	if err != nil {
		t.Fatal(err)
	}
	defer tcp.Close()
	options := optionsIPC(t, "region", "eu-west-2", "api_key", apiKey)
	request := mustIPC(t, jsonRow(t, generated.CatalogAttachRequestSchema, map[string]any{
		"name": ticket_probe.CatalogName, "options": options}))
	params := jsonRow(t, generated.CatalogAttachParamsSchema, map[string]any{"request": request})
	defer params.Release()
	res, err := tcp.CallUnary(context.Background(), "catalog_attach", params, nil)
	if err != nil {
		t.Fatalf("attach over TCP: %v", err)
	}
	defer res.Release()
	batch := resultStruct(t, res)
	defer batch.Release()
	value := batch.Column(batch.Schema().FieldIndices("attach_opaque_data")[0]).(*array.Binary).Value(0)
	if !bytes.Contains(value, []byte("0d3b56072291")) {
		t.Fatalf("the unsealed attach value does not carry the digest; the check would be vacuous: %q", value)
	}
	for _, form := range []string{apiKey, hex.EncodeToString([]byte(apiKey)), base64.StdEncoding.EncodeToString([]byte(apiKey))} {
		if bytes.Contains(value, []byte(form)) || strings.Contains(hex.EncodeToString(value), form) ||
			strings.Contains(base64.StdEncoding.EncodeToString(value), form) {
			t.Fatal("the api_key is in the attach value")
		}
	}
}
