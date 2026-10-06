// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"errors"
	"strings"
	"testing"

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
