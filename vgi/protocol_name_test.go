// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import "testing"

// The VGI wire protocol name is a cross-implementation constant: vgi-python,
// vgi-java, vgi-csharp, vgi-typescript, vgi-rust and the DuckDB C++ extension
// all route on the same string. Changing it here silently would make this port
// unreachable to every client, so pin the literal rather than the symbol.
func TestProtocolNameIsVgiV2(t *testing.T) {
	if ProtocolName != "vgi.v2" {
		t.Fatalf("ProtocolName = %q, want %q", ProtocolName, "vgi.v2")
	}
}

// A constant nobody passes to the framework is decoration. Assert the built
// server actually hosts it — this is what the routing key is matched against,
// and what the "This server hosts: [...]" error enumerates.
func TestBuiltServerHostsProtocolName(t *testing.T) {
	for _, transport := range []serverTransport{transportStdio, transportHTTP, transportUnix, transportTCP} {
		w := NewWorker()
		s := w.buildServer(transport)
		if got := s.ServiceName(); got != ProtocolName {
			t.Errorf("transport %d: server hosts %q, want %q", transport, got, ProtocolName)
		}
	}
}
