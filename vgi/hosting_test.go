// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// What reaches the wire -- the hosted list, its order, routing to a hosted
// protocol on stdio, unix and HTTP -- is asserted by vgi-rpc's
// hosted-protocols conformance group against the example worker. These cover
// the build-time rules, which no wire test can reach because a worker that
// breaks them never starts.

func echoProtocol(name string) *vgirpc.Server {
	p := vgirpc.NewProtocol(name)
	vgirpc.Unary(p, "echo", func(_ context.Context, _ *vgirpc.CallContext, in struct {
		Value string `vgirpc:"value"`
	}) (string, error) {
		return in.Value, nil
	})
	return p
}

func TestHostedProtocolsHookIsCalledOncePerBuildOnEveryTransport(t *testing.T) {
	for _, transport := range []serverTransport{transportStdio, transportHTTP, transportUnix, transportTCP} {
		calls := 0
		w := NewWorker(WithHostedProtocols(func() ([]*vgirpc.Server, error) {
			calls++
			return []*vgirpc.Server{echoProtocol("acme.Extra.v1")}, nil
		}))
		if _, err := w.buildServer(transport); err != nil {
			t.Fatalf("transport %d: %v", transport, err)
		}
		if calls != 1 {
			t.Errorf("transport %d: hook called %d times, want 1", transport, calls)
		}
	}
}

func TestHostedProtocolsValidationNamesTheHook(t *testing.T) {
	cases := map[string]func() ([]*vgirpc.Server, error){
		"reserved": func() ([]*vgirpc.Server, error) { return []*vgirpc.Server{echoProtocol("vgi_rpc.Reflection.v1")}, nil },
		"primary":  func() ([]*vgirpc.Server, error) { return []*vgirpc.Server{echoProtocol(ProtocolName)}, nil },
		"duplicate": func() ([]*vgirpc.Server, error) {
			return []*vgirpc.Server{echoProtocol("a.B.v1"), echoProtocol("a.B.v1")}, nil
		},
		"nil":      func() ([]*vgirpc.Server, error) { return []*vgirpc.Server{nil}, nil },
		"unnamed":  func() ([]*vgirpc.Server, error) { return []*vgirpc.Server{vgirpc.NewServer()}, nil },
		"invalid":  func() ([]*vgirpc.Server, error) { return []*vgirpc.Server{echoProtocol("not a name")}, nil },
		"hook err": func() ([]*vgirpc.Server, error) { return nil, errors.New("config missing") },
	}
	for name, fn := range cases {
		_, err := NewWorker(WithHostedProtocols(fn)).buildServer(transportStdio)
		if err == nil {
			t.Errorf("%s: the worker built", name)
			continue
		}
		if !strings.Contains(err.Error(), "WithHostedProtocols") {
			t.Errorf("%s: error does not name the hook: %v", name, err)
		}
	}
}

// Identity without an introspector allowlist refuses to start -- and only on
// HTTP, the one transport that hosts it.
func TestIdentityWithoutAnAllowlistRefusesToStart(t *testing.T) {
	t.Setenv(IntrospectPrincipalsEnv, "")
	resolve := func(string) (vgirpc.TokenIdentity, bool, error) { return vgirpc.TokenIdentity{}, false, nil }
	w := NewWorker(WithIdentity(vgirpc.IdentityConfig{ResolveToken: resolve}))
	if _, err := w.buildServer(transportHTTP); err == nil || !strings.Contains(err.Error(), IntrospectPrincipalsEnv) {
		t.Fatalf("HTTP build = %v, want a refusal naming %s", err, IntrospectPrincipalsEnv)
	}
	if _, err := w.newHttpServer(); err == nil {
		t.Fatal("newHttpServer started without an allowlist")
	}
	if _, err := w.buildServer(transportStdio); err != nil {
		t.Fatalf("stdio does not host identity, so it must not refuse: %v", err)
	}

	t.Setenv(IntrospectPrincipalsEnv, " proxy@example , ")
	if _, err := w.buildServer(transportHTTP); err != nil {
		t.Fatalf("allowlist from the environment: %v", err)
	}

	// Minting alone is not an oracle and needs no allowlist.
	t.Setenv(IntrospectPrincipalsEnv, "")
	mint := func(string, string, []string, int64) (vgirpc.IssuedGrant, error) { return vgirpc.IssuedGrant{}, nil }
	if _, err := NewWorker(WithIdentity(vgirpc.IdentityConfig{MintGrant: mint})).buildServer(transportHTTP); err != nil {
		t.Fatalf("mint-only identity: %v", err)
	}
}

// Sealed-grant keys: a malformed VGI_RPC_GRANT_KEYS stops the worker on every
// transport; well-formed keys alone host Identity.v1 (issue_grant) over HTTP;
// an explicit WithGrantKeys(nil) turns grants off whatever the environment says.
func TestGrantKeysFromTheEnvironment(t *testing.T) {
	t.Setenv(vgirpc.GrantKeysEnv, "not-a-key")
	for _, transport := range []serverTransport{transportStdio, transportHTTP, transportUnix, transportTCP} {
		if _, err := NewWorker().buildServer(transport); err == nil || !strings.Contains(err.Error(), "grant") {
			t.Errorf("transport %d: malformed grant key: %v, want a refusal", transport, err)
		}
	}
	if _, err := NewWorker(WithGrantKeys(nil)).buildServer(transportHTTP); err != nil {
		t.Errorf("WithGrantKeys(nil) must ignore the environment: %v", err)
	}

	t.Setenv(vgirpc.GrantKeysEnv, "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=")
	hs, err := NewWorker().newHttpServer()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(hs)
	defer ts.Close()
	client, err := vgirpc.NewHttpClient(ts.URL, vgirpc.WithClientProtocol(vgirpc.IdentityProtocolName))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	params := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{
		{Name: "purpose", Type: arrow.BinaryTypes.String},
		{Name: "scopes", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		{Name: "ttl_seconds", Type: arrow.PrimitiveTypes.Int64},
	}, nil), grantParamsColumns(), 1)
	defer params.Release()
	_, callErr := client.CallUnary(context.Background(), "issue_grant", params, nil)
	var rpcErr *vgirpc.RpcError
	// Hosted: an anonymous caller is refused stale_auth. Unhosted would be
	// protocol_not_supported.
	if !errors.As(callErr, &rpcErr) || rpcErr.Kind != "stale_auth" {
		t.Fatalf("issue_grant with grant keys from the environment: %v; want stale_auth from a hosted issue_grant", callErr)
	}
	stdio, err := NewWorker().buildServer(transportStdio)
	if err != nil {
		t.Fatal(err)
	}
	_ = stdio
}

func grantParamsColumns() []arrow.Array {
	purpose := array.NewStringBuilder(memory.DefaultAllocator)
	purpose.Append("p")
	scopes := array.NewListBuilder(memory.DefaultAllocator, arrow.BinaryTypes.String)
	scopes.Append(true)
	ttl := array.NewInt64Builder(memory.DefaultAllocator)
	ttl.Append(60)
	return []arrow.Array{purpose.NewArray(), scopes.NewArray(), ttl.NewArray()}
}
