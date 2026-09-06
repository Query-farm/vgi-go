package vgi

import (
	"strings"
	"testing"
)

func TestIrohRawUpstreamRequiresLoopback(t *testing.T) {
	worker := NewWorker()
	err := worker.RunIrohTcpUpstream("0.0.0.0", 9400, 0, IrohBridgeOptions{Issuer: "test-mesh"})
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("expected loopback error, got %v", err)
	}
}

func TestIrohRawUpstreamRequiresIssuer(t *testing.T) {
	worker := NewWorker()
	err := worker.RunIrohTcpUpstream("127.0.0.1", 9400, 0, IrohBridgeOptions{})
	if err == nil || !strings.Contains(err.Error(), "issuer") {
		t.Fatalf("expected issuer error, got %v", err)
	}
}
