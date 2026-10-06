// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// The sealed-grant loop, end to end over the worker's real HTTP handler:
// issue_grant as a freshly authenticated caller, then present the grant as an
// ordinary bearer on a vgi.v2 method, which runs authenticated as the grant's
// owner. Until vgi-rpc-go 0.32 no worker accepted its own grant.

const grantE2EPrincipalHeader = "X-Test-Principal"

// headerAuthenticate authenticates X-Test-Principal with a fresh auth_time --
// what an IdP-issued login looks like to the freshness guard. A bearer it does
// not own is "not mine", so the grant verifier the HTTP server appends is
// reached; no credential at all stays anonymous.
func headerAuthenticate(r *http.Request) (*vgirpc.AuthContext, error) {
	principal := r.Header.Get(grantE2EPrincipalHeader)
	if principal == "" {
		if r.Header.Get("Authorization") != "" {
			return nil, &vgirpc.RpcError{Type: "ValueError", Message: "not a test principal"}
		}
		return vgirpc.Anonymous(), nil
	}
	return &vgirpc.AuthContext{
		Domain: "test", Authenticated: true, Principal: principal,
		Claims: map[string]any{"auth_time": strconv.FormatInt(time.Now().Unix(), 10)},
	}, nil
}

func TestSealedGrantAuthenticatesAVgiCall(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	keys, err := vgirpc.NewGrantKeys([][]byte{key}, "e2e", 0)
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var seen []*vgirpc.AuthContext
	w := vgi.NewWorker(
		vgi.WithCatalogName("example"),
		vgi.WithGrantKeys(keys),
		vgi.WithAttachValidator(func(_ *vgi.CatalogAttachRequestWire, cc *vgirpc.CallContext) (*vgi.AttachDecision, error) {
			mu.Lock()
			seen = append(seen, cc.Auth)
			mu.Unlock()
			return nil, nil
		}),
	)
	w.SetAuthenticate(headerAuthenticate)
	hs, err := w.NewHttpServerForTest()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(hs)
	defer ts.Close()

	// 1. Mint, as a freshly authenticated alice. The worker supplied no
	//    MintGrant, so the framework mints a sealed grant.
	identity, err := vgirpc.NewHttpClient(ts.URL,
		vgirpc.WithClientProtocol(vgirpc.IdentityProtocolName),
		vgirpc.WithClientHeader(grantE2EPrincipalHeader, "alice"))
	if err != nil {
		t.Fatal(err)
	}
	defer identity.Close()
	issueSchema := arrow.NewSchema([]arrow.Field{
		{Name: "purpose", Type: arrow.BinaryTypes.String},
		{Name: "scopes", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		{Name: "ttl_seconds", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	params := jsonRow(t, issueSchema, map[string]any{"purpose": "nightly", "scopes": []string{"read"}, "ttl_seconds": 600})
	defer params.Release()
	minted, err := identity.CallUnary(context.Background(), "issue_grant", params, nil)
	if err != nil {
		t.Fatalf("issue_grant: %v", err)
	}
	defer minted.Release()
	grant := resultStruct(t, minted)
	defer grant.Release()
	token := grant.Column(grant.Schema().FieldIndices("token")[0]).(*array.String).Value(0)
	if !strings.HasPrefix(token, vgirpc.GrantTokenPrefix) {
		t.Fatalf("issue_grant returned %.12q..., not a sealed grant", token)
	}

	// 2. Call a vgi.v2 method -- catalog_attach -- presenting only the grant.
	vgiClient, err := vgirpc.NewHttpClient(ts.URL,
		vgirpc.WithClientProtocol(vgi.ProtocolName),
		vgirpc.WithClientProtocolVersion(vgi.ProtocolVersion),
		vgirpc.WithClientHeader("Authorization", "Bearer "+token))
	if err != nil {
		t.Fatal(err)
	}
	defer vgiClient.Close()
	request := mustIPC(t, jsonRow(t, generated.CatalogAttachRequestSchema, map[string]any{"name": "example"}))
	attach := jsonRow(t, generated.CatalogAttachParamsSchema, map[string]any{"request": request})
	defer attach.Release()
	result, err := vgiClient.CallUnary(context.Background(), "catalog_attach", attach, nil)
	if err != nil {
		t.Fatalf("catalog_attach with the grant: %v", err)
	}
	result.Release()

	// 3. It ran as the grant's owner, authenticated by the grant.
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 {
		t.Fatalf("attach validator saw %d calls, want 1", len(seen))
	}
	auth := seen[0]
	if !auth.Authenticated || auth.Principal != "alice" || auth.Domain != vgirpc.GrantAuthDomain {
		t.Fatalf("vgi.v2 call ran as %+v; want alice authenticated by her grant", auth)
	}
	if auth.Claims["purpose"] != "nightly" {
		t.Errorf("grant claims %v", auth.Claims)
	}
	if _, has := auth.Claims["auth_time"]; has {
		t.Error("a grant-authenticated call carries auth_time; a grant could then mint grants")
	}

	// A tampered grant is refused, not downgraded to anonymous.
	bad, err := vgirpc.NewHttpClient(ts.URL,
		vgirpc.WithClientProtocol(vgi.ProtocolName),
		vgirpc.WithClientProtocolVersion(vgi.ProtocolVersion),
		vgirpc.WithClientHeader("Authorization", "Bearer "+token[:len(token)-4]+"AAAA"))
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Close()
	if res, err := bad.CallUnary(context.Background(), "catalog_attach", attach, nil); err == nil {
		res.Release()
		t.Fatal("a tampered grant was accepted")
	}
}
