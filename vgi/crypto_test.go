// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

func authCtx(domain, principal string) *vgirpc.AuthContext {
	return &vgirpc.AuthContext{Domain: domain, Principal: principal, Authenticated: principal != ""}
}

func TestSealOpenRoundTrip(t *testing.T) {
	key := []byte("operator-supplied-signing-key")
	aad := attachAAD(authCtx("test", "alice"))
	token, err := sealBytes([]byte("readonly-catalog-"), key, aad, attachEnvelopeVersion)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	got, err := openBytes(token, key, aad, attachEnvelopeVersion)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if string(got) != "readonly-catalog-" {
		t.Fatalf("round-trip mismatch: %q", got)
	}
}

func TestOpenRejectsWrongPrincipal(t *testing.T) {
	key := []byte("k")
	token, _ := sealBytes([]byte("v"), key, attachAAD(authCtx("test", "alice")), attachEnvelopeVersion)
	if _, err := openBytes(token, key, attachAAD(authCtx("test", "bob")), attachEnvelopeVersion); err != errOpaqueDataRejected {
		t.Fatalf("expected rejection for wrong principal, got %v", err)
	}
}

func TestOpenRejectsWrongDomain(t *testing.T) {
	key := []byte("k")
	token, _ := sealBytes([]byte("v"), key, attachAAD(authCtx("idp-a", "alice")), attachEnvelopeVersion)
	if _, err := openBytes(token, key, attachAAD(authCtx("idp-b", "alice")), attachEnvelopeVersion); err != errOpaqueDataRejected {
		t.Fatalf("expected rejection for wrong domain, got %v", err)
	}
}

func TestOpenRejectsAnonymousReplayOfAuthenticated(t *testing.T) {
	key := []byte("k")
	token, _ := sealBytes([]byte("v"), key, attachAAD(authCtx("test", "alice")), attachEnvelopeVersion)
	if _, err := openBytes(token, key, attachAAD(authCtx("", "")), attachEnvelopeVersion); err != errOpaqueDataRejected {
		t.Fatalf("expected rejection for anonymous replay, got %v", err)
	}
}

func TestOpenRejectsWrongKey(t *testing.T) {
	aad := attachAAD(authCtx("test", "alice"))
	token, _ := sealBytes([]byte("v"), []byte("key-a"), aad, attachEnvelopeVersion)
	if _, err := openBytes(token, []byte("key-b"), aad, attachEnvelopeVersion); err != errOpaqueDataRejected {
		t.Fatalf("expected rejection for wrong key, got %v", err)
	}
}

func TestOpenRejectsTamperedAndMalformed(t *testing.T) {
	key := []byte("k")
	aad := attachAAD(authCtx("test", "alice"))
	token, _ := sealBytes([]byte("secret"), key, aad, attachEnvelopeVersion)
	tampered := bytes.Clone(token)
	tampered[len(tampered)-1] ^= 0x01
	if _, err := openBytes(tampered, key, aad, attachEnvelopeVersion); err != errOpaqueDataRejected {
		t.Fatalf("expected rejection for tampered token, got %v", err)
	}
	if _, err := openBytes([]byte("garbage"), key, aad, attachEnvelopeVersion); err != errOpaqueDataRejected {
		t.Fatalf("expected rejection for malformed token, got %v", err)
	}
	if _, err := openBytes(token, key, aad, attachEnvelopeVersion+1); err != errOpaqueDataRejected {
		t.Fatalf("expected rejection for wrong version byte, got %v", err)
	}
}

func TestTransactionEnvelopeBoundToParentAttach(t *testing.T) {
	key := []byte("k")
	auth := authCtx("test", "alice")
	attachA := []byte("attach-envelope-A")
	attachB := []byte("attach-envelope-B")
	token, _ := sealBytes([]byte("tx"), key, transactionAAD(auth, attachA), transactionEnvelopeVersion)
	// Same principal, correct parent attach: opens.
	if _, err := openBytes(token, key, transactionAAD(auth, attachA), transactionEnvelopeVersion); err != nil {
		t.Fatalf("open under correct parent attach: %v", err)
	}
	// Same principal, different parent attach: rejected.
	if _, err := openBytes(token, key, transactionAAD(auth, attachB), transactionEnvelopeVersion); err != errOpaqueDataRejected {
		t.Fatalf("expected rejection for wrong parent attach, got %v", err)
	}
}

func TestWorkerHelpersPassThroughWithoutSigningKey(t *testing.T) {
	w := &Worker{} // httpSigningKey is nil — subprocess / unix transport
	cc := &vgirpc.CallContext{Auth: authCtx("test", "alice")}
	sealed, err := w.sealAttach([]byte("plain"), cc)
	if err != nil || string(sealed) != "plain" {
		t.Fatalf("expected pass-through seal, got %q err=%v", sealed, err)
	}
	opened, err := w.openAttach([]byte("plain"), cc)
	if err != nil || string(opened) != "plain" {
		t.Fatalf("expected pass-through open, got %q err=%v", opened, err)
	}
}

func TestWorkerHelpersSealWithSigningKey(t *testing.T) {
	w := &Worker{httpSigningKey: []byte("a-32-byte-or-any-length-signkey!")}
	alice := &vgirpc.CallContext{Auth: authCtx("test", "alice")}
	bob := &vgirpc.CallContext{Auth: authCtx("test", "bob")}

	// The framework attach plaintext is uuid(16) || catalog_bytes; openAttach
	// strips the uuid and returns the catalog bytes, openAttachFull keeps both.
	plaintext := append(make([]byte, attachUUIDLen), []byte("readonly-catalog-")...)
	sealed, err := w.sealAttach(plaintext, alice)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if bytes.Equal(sealed, plaintext) {
		t.Fatal("expected sealed envelope to differ from plaintext")
	}
	// Alice opens her own envelope; openAttach returns the catalog bytes (uuid stripped).
	got, err := w.openAttach(sealed, alice)
	if err != nil || string(got) != "readonly-catalog-" {
		t.Fatalf("alice open: %q err=%v", got, err)
	}
	// openAttachFull returns the full uuid(16) || catalog_bytes.
	full, err := w.openAttachFull(sealed, alice)
	if err != nil || !bytes.Equal(full, plaintext) {
		t.Fatalf("alice openFull: %q err=%v", full, err)
	}
	// Bob cannot.
	if _, err := w.openAttach(sealed, bob); err != errOpaqueDataRejected {
		t.Fatalf("expected bob to be rejected, got %v", err)
	}
}
