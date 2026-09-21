// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package secretcache is what the secret-dependent cacheable fixtures share.
//
// The C++ result cache keys a secret-dependent result on a fingerprint of the
// secrets its bind resolved (never their values), so a result is reused while
// the secret is unchanged and recomputed the moment it is rotated, re-fielded or
// dropped. cache/secret_scope.test drives one fixture per cache path that
// fingerprint has to reach:
//
//   - secret_cache_nonce()       producer; the secret is declared in metadata.
//     Also the inline-bound data.secret_cache_nonce table, which covers the
//     client's no-bind-RPC path (secrets resolved client-side for init).
//   - secret_cached_scalar(x)    scalar, per-value memoized; secret declared.
//   - secret_cached_lateral(x)   blended map, per-value memoized, called under
//     LATERAL; the secret is requested from OnBind (the two-phase bind).
//
// Each reads the vgi_example secret's secret_string and emits a Nonce minted
// only when the worker really runs. The three live in the table, scalar and
// table_in_out packages; this package keeps what they must agree on in one
// place. Mirrors vgi-python's vgi/_test_fixtures/secret_cache.py.
package secretcache

import (
	"crypto/rand"
	"encoding/binary"

	"github.com/Query-farm/vgi-go/vgi"
)

// SecretType is the secret every fixture reads. Registered by the example
// worker (vgi.WithSecretTypes) and created by the test with
// CREATE SECRET ... (TYPE vgi_example, secret_string '...').
const SecretType = "vgi_example"

// TTLSeconds is the advertised freshness lifetime. Long enough that TTL never
// lapses mid-test, so a MISS can only come from the secret changing.
const TTLSeconds = 300

// Nonce returns a value unique to this invocation across every process in a
// worker pool: 56 random bits from the OS RNG (vgi-python's
// int.from_bytes(os.urandom(7))), non-negative as a BIGINT.
//
// It is random rather than a counter (as cache_nonce uses) because a pooled
// worker can run several processes and per-process counters repeat across
// them. Equal nonces prove a cache HIT and different ones a MISS, on any pool
// size.
func Nonce() int64 {
	var b [8]byte
	// crypto/rand.Read never returns an error; it crashes the program if the
	// OS RNG fails.
	_, _ = rand.Read(b[1:])
	return int64(binary.BigEndian.Uint64(b[:]))
}

// SecretString returns the secret_string field of the resolved vgi_example
// secret. ok is false when no such secret resolved, or it carries no
// secret_string — the state a dropped secret leaves, which the fixtures must
// serve rather than fail on.
func SecretString(secrets vgi.Secrets) (value string, ok bool) {
	matches := secrets.OfType(SecretType)
	if len(matches) == 0 {
		return "", false
	}
	v, present := matches[0]["secret_string"]
	if !present || v == nil {
		return "", false
	}
	return vgi.RenderSecretValue(v), true
}
