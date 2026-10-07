// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"crypto/rand"
	"errors"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// vgi-opaque-data-sealing.md, as unit tests: every way a value can fail to
// open gives one identical error per field, there is no plaintext fallback of
// any shape, and HTTP seals even with a generated key.

func sealingWorker() *Worker {
	return &Worker{httpSigningKey: []byte("opaque-sealing-test-key"), sealOpaqueData: true,
		extraCatalogs: map[string]*WritableCatalog{"w": {Name: "w"}}}
}

// wireError renders everything about an error a client can see.
func wireError(t *testing.T, err error) string {
	t.Helper()
	var status *vgirpc.StatusError
	if !errors.As(err, &status) {
		t.Fatalf("not a classified error: %#v", err)
	}
	if status.Code != vgirpc.CodeInvalidArgument || status.Kind != "opaque_data_not_recognized" || len(status.Details) != 0 {
		t.Fatalf("rejection is not INVALID_ARGUMENT / opaque_data_not_recognized without details: %+v", status)
	}
	return string(status.Code) + "|" + status.Kind + "|" + status.Message
}

func flip(b []byte, i int) []byte {
	out := bytes.Clone(b)
	out[i] ^= 0x01
	return out
}

func TestOpaqueValuesRejectUniformly(t *testing.T) {
	w := sealingWorker()
	alice := &vgirpc.CallContext{Auth: authCtx("jwt", "alice")}
	bob := &vgirpc.CallContext{Auth: authCtx("jwt", "bob")}
	aliceOtherDomain := &vgirpc.CallContext{Auth: authCtx("bearer", "alice")}
	anon := &vgirpc.CallContext{Auth: vgirpc.Anonymous()}

	attach, err := w.mintAttach([]byte("example"), alice)
	if err != nil {
		t.Fatal(err)
	}
	other, err := w.mintAttach([]byte("example"), alice)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := w.sealTransaction([]byte("0123456789abcdef"), attach, alice)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := w.openAttach(attach, alice); err != nil || string(got) != "example" {
		t.Fatalf("the owner cannot open her attach: %q %v", got, err)
	}
	if _, err := w.openTransaction(tx, attach, alice); err != nil {
		t.Fatalf("the owner cannot open her transaction: %v", err)
	}

	random := make([]byte, len(attach))
	_, _ = rand.Read(random)
	random[0] = attachEnvelopeVersion
	forged := map[string][]byte{
		"bare uuid":          make([]byte, attachUUIDLen),
		"uuid || catalog":    append(make([]byte, attachUUIDLen), "example"...),
		"writable: prefix":   []byte("writable:w"),
		"uuid || writable":   append(make([]byte, attachUUIDLen), "writable:w"...),
		"envelope-shaped":    random,
		"empty-ish":          {attachEnvelopeVersion},
		"first byte flipped": flip(attach, 0),
		"middle flipped":     flip(attach, len(attach)/2),
		"last byte flipped":  flip(attach, len(attach)-1),
	}
	want := ""
	check := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: accepted", name)
		}
		got := wireError(t, err)
		if want == "" {
			want = got
		}
		if got != want {
			t.Fatalf("%s: %q, want the uniform %q", name, got, want)
		}
	}
	for name, v := range forged {
		_, err := w.openAttach(v, alice)
		check("attach "+name, err)
		// The request path too: no shape skips the open.
		req := &CatalogVersionRequestWire{AttachOpaqueData: v}
		check("unwrap "+name, w.unwrapReqOpaque(req, alice))
	}
	for name, cc := range map[string]*vgirpc.CallContext{"bob": bob, "other domain": aliceOtherDomain, "anonymous": anon, "no call context": nil} {
		_, err := w.openAttach(attach, cc)
		check("attach replayed by "+name, err)
	}
	if !strings.HasSuffix(want, "|attach_opaque_data not recognized") {
		t.Fatalf("attach error %q", want)
	}

	attachWant := want
	want = ""
	for name, f := range map[string]func() error{
		"tx under another attach": func() error { _, err := w.openTransaction(tx, other, alice); return err },
		"tx replayed by bob":      func() error { _, err := w.openTransaction(tx, attach, bob); return err },
		"tx first byte flipped":   func() error { _, err := w.openTransaction(flip(tx, 0), attach, alice); return err },
		"tx middle flipped":       func() error { _, err := w.openTransaction(flip(tx, len(tx)/2), attach, alice); return err },
		"tx last byte flipped":    func() error { _, err := w.openTransaction(flip(tx, len(tx)-1), attach, alice); return err },
		"tx as plaintext uuid":    func() error { _, err := w.openTransaction(make([]byte, 16), attach, alice); return err },
		"tx sealed as an attach":  func() error { _, err := w.openTransaction(attach, attach, alice); return err },
	} {
		check(name, f())
	}
	if strings.Replace(attachWant, "attach_opaque_data", "transaction_opaque_data", 1) != want {
		t.Fatalf("the two fields differ beyond the field name: %q vs %q", attachWant, want)
	}
}

// An unroutable value is the same error as one that does not open, naming
// nothing the worker serves.
func TestUnroutableAttachIsTheUniformError(t *testing.T) {
	w := sealingWorker()
	w.routes = map[string]catalogBackend{"served": &subCatalog{child: NewWorker()}}
	alice := &vgirpc.CallContext{Auth: authCtx("jwt", "alice")}
	attach, err := w.mintAttach(encodeRoutedAttach("gone", []byte("x")), alice)
	if err != nil {
		t.Fatal(err)
	}
	req := &CatalogVersionRequestWire{AttachOpaqueData: attach}
	_, routed, err := w.dispatchRouted(t.Context(), alice, "catalog_version", req)
	if !routed || err == nil || wireError(t, err) != wireError(t, attachRejected()) {
		t.Fatalf("unroutable attach: routed=%v err=%v", routed, err)
	}
	if strings.Contains(err.Error(), "gone") || strings.Contains(err.Error(), "served") {
		t.Fatalf("the error names catalogs: %v", err)
	}
}

// HTTP seals with whatever key it resolved, a generated one included.
func TestHttpSealsWithTheGeneratedKey(t *testing.T) {
	t.Setenv(vgirpc.GrantKeysEnv, "")
	w := NewWorker(WithCatalogName("example"))
	if _, err := w.newHttpServer(); err != nil {
		t.Fatal(err)
	}
	if !w.sealOpaqueData || len(w.httpSigningKey) == 0 {
		t.Fatal("an HTTP worker without a configured key does not seal")
	}
	cc := &vgirpc.CallContext{Auth: vgirpc.Anonymous()}
	sealed, err := w.mintAttach([]byte("example"), cc)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, []byte("example")) || sealed[0] != attachEnvelopeVersion {
		t.Fatal("the anonymous caller's attach value is not sealed")
	}
}

// The OS-owned transports do not seal, and the stdio/unix/TCP worker never
// turns sealing on just because a key was configured.
func TestOsOwnedTransportsDoNotSeal(t *testing.T) {
	w := NewWorker(WithHttpSigningKey([]byte("configured-but-stdio")))
	if _, err := w.buildServer(transportStdio); err != nil {
		t.Fatal(err)
	}
	if w.sealOpaqueData {
		t.Fatal("stdio seals")
	}
}
