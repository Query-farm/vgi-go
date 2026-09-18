// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"time"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// Test-only access for the external vgi_test package, so a test there can
// serve a worker exactly as a deployment does while also importing the example
// functions (which import this package, so an internal test cannot).

// NewHttpServerForTest returns the handler RunHttp serves.
func (w *Worker) NewHttpServerForTest() (*vgirpc.HttpServer, error) {
	return w.newHttpServer()
}

// ServeTcpForTest serves w over raw TCP on a loopback port with the server
// RunTcp builds, calling onBound once listening. It returns after idleTimeout
// with no open connection.
func (w *Worker) ServeTcpForTest(idleTimeout time.Duration, onBound func(host string, port int)) error {
	return w.buildServer(transportTCP).RunTcp("127.0.0.1", 0, idleTimeout, onBound)
}
