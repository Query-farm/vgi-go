// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/google/uuid"
	"golang.org/x/crypto/chacha20poly1305"
)

// Catalog opaque-data AEAD envelopes.
//
// attach_opaque_data and transaction_opaque_data are implementation-chosen
// byte strings the catalog returns and the client round-trips back. On HTTP
// transport (where one worker authenticates many principals) the worker seals
// each value in an authenticated-encrypted envelope whose AAD binds the
// caller's (domain, principal); the transaction envelope additionally binds
// its parent attach envelope. A value sealed for one principal — or one
// attach — cannot be opened by another.
//
// Subprocess / unix-socket transports have no signing key (httpSigningKey is
// empty): the helpers pass values through unchanged, since OS process
// ownership already enforces identity there.
//
// Wire format: version(1 byte) || nonce(24 bytes) || ciphertext+tag.
// This matches vgi-python's vgi_rpc.crypto / vgi.worker envelope exactly.

const (
	// attachEnvelopeVersion is 2: the inner attach plaintext is
	// uuid(16) || catalog_bytes — catalog_attach prepends a framework-minted
	// 16-byte UUID that storage shards on. Bumped from 1 so a stale v1 token
	// (no uuid prefix) is cleanly rejected at open rather than mis-parsed.
	attachEnvelopeVersion      byte = 2
	transactionEnvelopeVersion byte = 2

	// attachUUIDLen is the width of the framework UUID prepended to every
	// attach plaintext. Mirrors vgi-python's _ATTACH_UUID_LEN.
	attachUUIDLen = 16

	cryptoNonceLen = chacha20poly1305.NonceSizeX // 24
	cryptoTagLen   = chacha20poly1305.Overhead   // 16
	cryptoMinLen   = 1 + cryptoNonceLen + cryptoTagLen
)

var (
	attachAADPrefix      = []byte("vgi.attach_opaque_data.v1\x00")
	transactionAADPrefix = []byte("vgi.transaction_opaque_data.v1\x00")

	// errOpaqueDataRejected is the single uniform error every open-failure
	// maps to — wrong principal, wrong parent attach, tampered, malformed,
	// or simply unknown — so a probing caller cannot distinguish them.
	// Catalog values surface it as attachRejected / transactionRejected.
	errOpaqueDataRejected = errors.New("opaque data not recognized")
)

// opaqueRejectedKind is the error_kind of every opaque-value rejection.
const opaqueRejectedKind = "opaque_data_not_recognized"

// attachRejected is the one error for an attach_opaque_data that does not
// open: wrong caller, tampered, malformed, unknown key, or a route this worker
// does not serve. Identical in every case (vgi-opaque-data-sealing.md rule 4):
// INVALID_ARGUMENT, kind opaque_data_not_recognized, exactly the message
// "attach_opaque_data not recognized", no details. It differs from
// transactionRejected only in the field name. A fresh value per call.
func attachRejected() error {
	return &vgirpc.StatusError{Code: vgirpc.CodeInvalidArgument, Kind: opaqueRejectedKind,
		Message: "attach_opaque_data not recognized"}
}

// transactionRejected is attachRejected for transaction_opaque_data, which
// also fails when presented under another attach.
func transactionRejected() error {
	return &vgirpc.StatusError{Code: vgirpc.CodeInvalidArgument, Kind: opaqueRejectedKind,
		Message: "transaction_opaque_data not recognized"}
}

// normalizeCryptoKey stretches/compresses an arbitrary-length key to the 32
// bytes XChaCha20-Poly1305 requires. Matches vgi-python's normalize_key.
func normalizeCryptoKey(key []byte) []byte {
	if len(key) == chacha20poly1305.KeySize {
		return key
	}
	sum := sha256.Sum256(key)
	return sum[:]
}

// identityTail builds the identity portion of an opaque-data AAD. Mirrors the
// (domain, principal) convention: unauthenticated requests get a fixed
// anonymous tail, so an anonymous caller cannot open an envelope sealed for a
// real principal.
func identityTail(auth *vgirpc.AuthContext) []byte {
	if auth == nil || !auth.Authenticated {
		return []byte("\x00anonymous")
	}
	out := make([]byte, 0, 1+len(auth.Domain)+1+len(auth.Principal))
	out = append(out, 0x01)
	out = append(out, auth.Domain...)
	out = append(out, 0x00)
	out = append(out, auth.Principal...)
	return out
}

// attachAAD is the AAD for an attach_opaque_data envelope.
func attachAAD(auth *vgirpc.AuthContext) []byte {
	return append(append([]byte{}, attachAADPrefix...), identityTail(auth)...)
}

// transactionAAD is the AAD for a transaction_opaque_data envelope. It binds
// both the caller identity and the parent attach envelope, so a transaction
// value minted under one attach cannot be replayed against a different attach
// even by the same principal.
func transactionAAD(auth *vgirpc.AuthContext, attachEnvelope []byte) []byte {
	out := append([]byte{}, transactionAADPrefix...)
	out = append(out, identityTail(auth)...)
	out = append(out, 0x00)
	out = append(out, attachEnvelope...)
	return out
}

// sealBytes seals payload into an AEAD envelope: version || nonce || ct+tag.
func sealBytes(payload, key, aad []byte, version byte) ([]byte, error) {
	nonce := make([]byte, cryptoNonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("opaque-data nonce: %w", err)
	}
	return sealBytesWithNonce(payload, key, aad, version, nonce)
}

// sealBytesWithNonce is sealBytes with a caller-chosen 24-byte nonce. Only the
// cross-SDK vectors pin a nonce; every production seal draws a fresh one.
func sealBytesWithNonce(payload, key, aad []byte, version byte, nonce []byte) ([]byte, error) {
	if len(nonce) != cryptoNonceLen {
		return nil, fmt.Errorf("opaque-data nonce must be %d bytes", cryptoNonceLen)
	}
	aead, err := chacha20poly1305.NewX(normalizeCryptoKey(key))
	if err != nil {
		return nil, fmt.Errorf("opaque-data cipher: %w", err)
	}
	ciphertext := aead.Seal(nil, nonce, payload, aad)
	out := make([]byte, 0, 1+cryptoNonceLen+len(ciphertext))
	out = append(out, version)
	out = append(out, nonce...)
	out = append(out, ciphertext...)
	return out, nil
}

// openBytes opens and verifies an envelope produced by sealBytes. Every
// failure mode — malformed, wrong version, tampered, wrong key, wrong AAD
// (cross-principal/cross-attach replay) — returns errOpaqueDataRejected.
func openBytes(token, key, aad []byte, version byte) ([]byte, error) {
	if len(token) < cryptoMinLen || token[0] != version {
		return nil, errOpaqueDataRejected
	}
	aead, err := chacha20poly1305.NewX(normalizeCryptoKey(key))
	if err != nil {
		return nil, fmt.Errorf("opaque-data cipher: %w", err)
	}
	nonce := token[1 : 1+cryptoNonceLen]
	ciphertext := token[1+cryptoNonceLen:]
	plaintext, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, errOpaqueDataRejected
	}
	return plaintext, nil
}

// --- Worker-bound seal/open helpers ---------------------------------------
//
// On HTTP -- the transport that authenticates callers -- every value is sealed
// with the worker's resolved signing key: VGI_SIGNING_KEY / WithHttpSigningKey
// when configured, else the key generated at startup. An unset key never means
// "don't seal". On the OS-owned transports (stdio, unix socket, TCP launcher)
// sealOpaqueData is false and every helper is a pass-through. There is no
// plaintext fallback: a value that does not open is rejected, whatever its
// shape. The catalog implementation always sees plaintext.

// sealAttach seals a plaintext attach value into an envelope bound to the
// caller's identity.
func (w *Worker) sealAttach(plaintext []byte, cc *vgirpc.CallContext) ([]byte, error) {
	if !w.sealOpaqueData {
		return plaintext, nil
	}
	return sealBytes(plaintext, w.httpSigningKey, attachAAD(cc.Auth), attachEnvelopeVersion)
}

// openAttachFull opens an attach_opaque_data envelope, returning the full
// framework plaintext uuid(16) || catalog_bytes (not stripped). Storage shards
// on the leading UUID, so the function-execution paths use this to derive the
// shard key. Pass-through when there is no signing key.
func (w *Worker) openAttachFull(envelope []byte, cc *vgirpc.CallContext) ([]byte, error) {
	if !w.sealOpaqueData {
		return envelope, nil
	}
	plain, err := openBytes(envelope, w.httpSigningKey, attachAAD(callAuth(cc)), attachEnvelopeVersion)
	if err != nil {
		return nil, attachRejected()
	}
	return plain, nil
}

// callAuth is the caller's auth context; a missing call context is anonymous,
// never a reason to skip the open.
func callAuth(cc *vgirpc.CallContext) *vgirpc.AuthContext {
	if cc == nil {
		return nil
	}
	return cc.Auth
}

// mintAttach is catalog_attach's last step: prepend a fresh framework UUID to
// the catalog's own bytes (uuid(16) || catalog_bytes; storage shards on the
// UUID, which comes from crypto/rand) and seal the result for the caller.
func (w *Worker) mintAttach(catalogBytes []byte, cc *vgirpc.CallContext) ([]byte, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	minted := make([]byte, 0, attachUUIDLen+len(catalogBytes))
	minted = append(minted, u[:]...)
	minted = append(minted, catalogBytes...)
	return w.sealAttach(minted, cc)
}

// sealTransaction seals a transaction value for the caller, bound to the
// sealed attach value the same call carried.
func (w *Worker) sealTransaction(plaintext, attachEnvelope []byte, cc *vgirpc.CallContext) ([]byte, error) {
	if !w.sealOpaqueData {
		return plaintext, nil
	}
	return sealBytes(plaintext, w.httpSigningKey, transactionAAD(callAuth(cc), attachEnvelope), transactionEnvelopeVersion)
}

// openAttach opens an attach_opaque_data envelope, returning the catalog's own
// bytes — the framework UUID prefix is stripped. This is what catalog handlers
// and function bodies see (the catalog never knows about the shard UUID);
// storage routing uses openAttachFull to reach the UUID instead.
func (w *Worker) openAttach(envelope []byte, cc *vgirpc.CallContext) ([]byte, error) {
	full, err := w.openAttachFull(envelope, cc)
	if err != nil {
		return nil, err
	}
	if len(full) < attachUUIDLen {
		return full, nil
	}
	return full[attachUUIDLen:], nil
}

// openTransaction opens a transaction_opaque_data envelope. attachEnvelope is
// the (sealed) attach_opaque_data the same call carried — it must match the
// attach the transaction was minted under, or the open fails.
func (w *Worker) openTransaction(envelope, attachEnvelope []byte, cc *vgirpc.CallContext) ([]byte, error) {
	if !w.sealOpaqueData {
		return envelope, nil
	}
	plain, err := openBytes(envelope, w.httpSigningKey, transactionAAD(callAuth(cc), attachEnvelope), transactionEnvelopeVersion)
	if err != nil {
		return nil, transactionRejected()
	}
	return plain, nil
}

// unwrapReqOpaque unwraps the AttachOpaqueData ([]byte) and, if present,
// TransactionOpaqueData (*[]byte) fields of a catalog request struct in
// place, so handler bodies always see plaintext. The transaction envelope is
// opened with the *sealed* attach value as part of its AAD, so it stays bound
// to its parent attach. Pass-through (UUID stripped) on the unsealed
// transports.
func (w *Worker) unwrapReqOpaque(reqPtr any, cc *vgirpc.CallContext) error {
	v := reflect.ValueOf(reqPtr).Elem()
	var sealedAttach []byte
	if af := v.FieldByName("AttachOpaqueData"); af.IsValid() && af.Kind() == reflect.Slice {
		sealedAttach = af.Bytes()
		// Every attach value -- writable catalogs' included -- is the framework
		// envelope minted in catalog_attach (uuid(16) || catalog_bytes, then
		// sealed on HTTP). openAttach opens the seal and strips the UUID, so
		// handler bodies always see the catalog's own bytes regardless of
		// transport. No shape of value skips the open.
		if len(sealedAttach) > 0 {
			plain, err := w.openAttach(sealedAttach, cc)
			if err != nil {
				return err
			}
			af.SetBytes(plain)
		}
	}
	if !w.sealOpaqueData {
		return nil
	}
	if tf := v.FieldByName("TransactionOpaqueData"); tf.IsValid() && tf.Kind() == reflect.Ptr && !tf.IsNil() {
		plain, err := w.openTransaction(tf.Elem().Bytes(), sealedAttach, cc)
		if err != nil {
			return err
		}
		tf.Set(reflect.ValueOf(&plain))
	}
	return nil
}

// unaryCatalog registers a catalog unary handler whose request opaque-data
// fields are unwrapped before the handler body runs.
//
// The handler is also recorded by method name (Worker.catalogMethods), so a
// worker registered as another worker's sub-catalog (RegisterSubCatalog) can be
// dispatched to; s is nil when only recording. A request whose attach routes
// to a sub-catalog or MemoryCatalog is handed to that catalog instead.
func unaryCatalog[P any, R any](w *Worker, s *vgirpc.Server, name string,
	handler func(context.Context, *vgirpc.CallContext, P) (R, error)) {
	w.recordCatalogMethod(name, func(ctx context.Context, cc *vgirpc.CallContext, raw any) (any, error) {
		req := raw.(*P)
		if err := w.unwrapReqOpaque(req, cc); err != nil {
			return nil, err
		}
		return handler(ctx, cc, *req)
	})
	if s == nil {
		return
	}
	vgirpc.Unary[P, R](s, name, func(ctx context.Context, cc *vgirpc.CallContext, req P) (R, error) {
		var zero R
		sealedAttach := sealedAttachOf(&req)
		if out, routed, err := w.dispatchRouted(ctx, cc, name, &req); routed || err != nil {
			if err != nil {
				return zero, err
			}
			r, ok := out.(R)
			if !ok {
				return zero, fmt.Errorf("vgi: routed %s returned %T", name, out)
			}
			return r, w.sealTransactionResult(&r, sealedAttach, cc)
		}
		if err := w.unwrapReqOpaque(&req, cc); err != nil {
			return zero, err
		}
		r, err := handler(ctx, cc, req)
		if err != nil {
			return r, err
		}
		return r, w.sealTransactionResult(&r, sealedAttach, cc)
	})
}

// unaryVoidCatalog is unaryCatalog for void-returning catalog handlers.
func unaryVoidCatalog[P any](w *Worker, s *vgirpc.Server, name string,
	handler func(context.Context, *vgirpc.CallContext, P) error) {
	w.recordCatalogMethod(name, func(ctx context.Context, cc *vgirpc.CallContext, raw any) (any, error) {
		req := raw.(*P)
		if err := w.unwrapReqOpaque(req, cc); err != nil {
			return nil, err
		}
		return nil, handler(ctx, cc, *req)
	})
	if s == nil {
		return
	}
	vgirpc.UnaryVoid[P](s, name, func(ctx context.Context, cc *vgirpc.CallContext, req P) error {
		if _, routed, err := w.dispatchRouted(ctx, cc, name, &req); routed || err != nil {
			return err
		}
		if err := w.unwrapReqOpaque(&req, cc); err != nil {
			return err
		}
		return handler(ctx, cc, req)
	})
}

// sealedAttachOf copies a request's AttachOpaqueData as it arrived, before the
// handler opens it.
func sealedAttachOf(reqPtr any) []byte {
	af := reflect.ValueOf(reqPtr).Elem().FieldByName("AttachOpaqueData")
	if !af.IsValid() || af.Kind() != reflect.Slice {
		return nil
	}
	return bytes.Clone(af.Bytes())
}

// sealTransactionResult seals the transaction value catalog_transaction_begin
// returns (any catalog, routed or not) for the caller, bound to the sealed
// attach the request carried. Every other result passes through.
func (w *Worker) sealTransactionResult(resultPtr any, sealedAttach []byte, cc *vgirpc.CallContext) error {
	tb, ok := resultPtr.(*TransactionBeginResponseWire)
	if !ok || tb.TransactionOpaqueData == nil {
		return nil
	}
	sealed, err := w.sealTransaction(*tb.TransactionOpaqueData, sealedAttach, cc)
	if err != nil {
		return err
	}
	tb.TransactionOpaqueData = &sealed
	return nil
}
