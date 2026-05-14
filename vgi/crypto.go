// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"

	"github.com/Query-farm/vgi-rpc/vgirpc"
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
	attachEnvelopeVersion      byte = 1
	transactionEnvelopeVersion byte = 2

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
	errOpaqueDataRejected = errors.New("opaque data not recognized")
)

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
	aead, err := chacha20poly1305.NewX(normalizeCryptoKey(key))
	if err != nil {
		return nil, fmt.Errorf("opaque-data cipher: %w", err)
	}
	nonce := make([]byte, cryptoNonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("opaque-data nonce: %w", err)
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
// When the worker has no signing key (subprocess / unix transports) every
// helper is a transparent pass-through. The catalog implementation always
// sees plaintext; the client only ever sees sealed envelopes.

// sealAttach seals a plaintext attach value into an envelope bound to the
// caller's identity.
func (w *Worker) sealAttach(plaintext []byte, cc *vgirpc.CallContext) ([]byte, error) {
	if len(w.httpSigningKey) == 0 {
		return plaintext, nil
	}
	return sealBytes(plaintext, w.httpSigningKey, attachAAD(cc.Auth), attachEnvelopeVersion)
}

// openAttach opens an attach_opaque_data envelope, returning the plaintext.
func (w *Worker) openAttach(envelope []byte, cc *vgirpc.CallContext) ([]byte, error) {
	if len(w.httpSigningKey) == 0 {
		return envelope, nil
	}
	return openBytes(envelope, w.httpSigningKey, attachAAD(cc.Auth), attachEnvelopeVersion)
}

// sealTransaction seals a plaintext transaction value into an envelope bound
// to the caller's identity and the parent attach envelope it was minted under.
func (w *Worker) sealTransaction(plaintext, attachEnvelope []byte, cc *vgirpc.CallContext) ([]byte, error) {
	if len(w.httpSigningKey) == 0 {
		return plaintext, nil
	}
	return sealBytes(plaintext, w.httpSigningKey, transactionAAD(cc.Auth, attachEnvelope), transactionEnvelopeVersion)
}

// openTransaction opens a transaction_opaque_data envelope. attachEnvelope is
// the (sealed) attach_opaque_data the same call carried — it must match the
// attach the transaction was minted under, or the open fails.
func (w *Worker) openTransaction(envelope, attachEnvelope []byte, cc *vgirpc.CallContext) ([]byte, error) {
	if len(w.httpSigningKey) == 0 {
		return envelope, nil
	}
	return openBytes(envelope, w.httpSigningKey, transactionAAD(cc.Auth, attachEnvelope), transactionEnvelopeVersion)
}

// unwrapReqOpaque unwraps the AttachOpaqueData ([]byte) and, if present,
// TransactionOpaqueData (*[]byte) fields of a catalog request struct in
// place, so handler bodies always see plaintext. The transaction envelope is
// opened with the *sealed* attach value as part of its AAD, so it stays bound
// to its parent attach. A no-op when the worker has no signing key.
func (w *Worker) unwrapReqOpaque(reqPtr any, cc *vgirpc.CallContext) error {
	if len(w.httpSigningKey) == 0 {
		return nil
	}
	v := reflect.ValueOf(reqPtr).Elem()
	var sealedAttach []byte
	if af := v.FieldByName("AttachOpaqueData"); af.IsValid() && af.Kind() == reflect.Slice {
		sealedAttach = af.Bytes()
		if len(sealedAttach) > 0 {
			plain, err := w.openAttach(sealedAttach, cc)
			if err != nil {
				return err
			}
			af.SetBytes(plain)
		}
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
func unaryCatalog[P any, R any](w *Worker, s *vgirpc.Server, name string,
	handler func(context.Context, *vgirpc.CallContext, P) (R, error)) {
	vgirpc.Unary[P, R](s, name, func(ctx context.Context, cc *vgirpc.CallContext, req P) (R, error) {
		if err := w.unwrapReqOpaque(&req, cc); err != nil {
			var zero R
			return zero, err
		}
		return handler(ctx, cc, req)
	})
}

// unaryVoidCatalog is unaryCatalog for void-returning catalog handlers.
func unaryVoidCatalog[P any](w *Worker, s *vgirpc.Server, name string,
	handler func(context.Context, *vgirpc.CallContext, P) error) {
	vgirpc.UnaryVoid[P](s, name, func(ctx context.Context, cc *vgirpc.CallContext, req P) error {
		if err := w.unwrapReqOpaque(&req, cc); err != nil {
			return err
		}
		return handler(ctx, cc, req)
	})
}
