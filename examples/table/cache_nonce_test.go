// Copyright 2026 Query Farm LLC - https://query.farm

package table

import (
	"os"
	"testing"
)

// The nonce fixtures must mint values that are unique across worker processes,
// not just within one: the client's subprocess pool may serve two cache MISSes
// from two different processes (cache/transaction_scope.test asserts their
// nonces differ). The pid lives in the high 32 bits.
func TestCacheNonceIsProcessUnique(t *testing.T) {
	a := nextCacheNonce()
	b := nextCacheNonce()
	if b != a+1 {
		t.Fatalf("nonce not monotonic within a process: %d then %d", a, b)
	}
	for _, n := range []int64{a, b} {
		if got, want := n>>32, int64(os.Getpid()); got != want {
			t.Fatalf("nonce %d carries pid %d, want %d", n, got, want)
		}
	}
}
