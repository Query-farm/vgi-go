// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The cross-SDK vectors (vgi-python's attach_ticket_vectors.json, copied to
// testdata/): every mint token byte for byte, every accept, reject and redeem
// case as written.

type ticketVectors struct {
	Defaults struct {
		SigningKeyB64 string `json:"signing_key_b64"`
		Now           int64  `json:"now"`
	} `json:"defaults"`
	Mint []struct {
		Name                  string `json:"name"`
		SigningKeyB64         string `json:"signing_key_b64"`
		Principal             string `json:"principal"`
		CatalogName           string `json:"catalog_name"`
		OptionsIPCB64         string `json:"options_ipc_b64"`
		DataVersionSpec       string `json:"data_version_spec"`
		ImplementationVersion string `json:"implementation_version"`
		IssuedAt              int64  `json:"issued_at"`
		ExpiresAt             int64  `json:"expires_at"`
		TicketID              string `json:"ticket_id"`
		NonceHex              string `json:"nonce_hex"`
		AADHex                string `json:"aad_hex"`
		PayloadHex            string `json:"payload_hex"`
		Token                 string `json:"token"`
	} `json:"mint"`
	Accept []struct {
		Name          string `json:"name"`
		SigningKeyB64 string `json:"signing_key_b64"`
		Token         string `json:"token"`
		Principal     string `json:"principal"`
		Now           int64  `json:"now"`
		Claims        struct {
			IssuedAt              int64  `json:"issued_at"`
			ExpiresAt             int64  `json:"expires_at"`
			TicketID              string `json:"ticket_id"`
			CatalogName           string `json:"catalog_name"`
			DataVersionSpec       string `json:"data_version_spec"`
			ImplementationVersion string `json:"implementation_version"`
			OptionsIPCB64         string `json:"options_ipc_b64"`
		} `json:"claims"`
	} `json:"accept"`
	Reject []struct {
		Name          string `json:"name"`
		SigningKeyB64 string `json:"signing_key_b64"`
		Token         string `json:"token"`
		Principal     string `json:"principal"`
		Now           *int64 `json:"now"`
		ErrorKind     string `json:"error_kind"`
	} `json:"reject"`
	Redeem []struct {
		Name      string            `json:"name"`
		Options   map[string]string `json:"options"`
		Principal string            `json:"principal"`
		ErrorKind string            `json:"error_kind"`
		Result    *struct {
			CatalogName           string            `json:"catalog_name"`
			DataVersionSpec       *string           `json:"data_version_spec"`
			ImplementationVersion *string           `json:"implementation_version"`
			Options               map[string]string `json:"options"`
		} `json:"result"`
	} `json:"redeem"`
}

func loadTicketVectors(t *testing.T) *ticketVectors {
	t.Helper()
	raw, err := os.ReadFile("testdata/attach_ticket_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v ticketVectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	if len(v.Mint) == 0 || len(v.Accept) == 0 || len(v.Reject) == 0 || len(v.Redeem) == 0 {
		t.Fatal("the vectors file has an empty section")
	}
	return &v
}

func vecKey(t *testing.T, v *ticketVectors, b64 string) []byte {
	t.Helper()
	if b64 == "" {
		b64 = v.Defaults.SigningKeyB64
	}
	key, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func mustB64(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func ticketErrorKind(err error) string {
	var kinded interface{ ErrorKind() string }
	if errors.As(err, &kinded) {
		return kinded.ErrorKind()
	}
	return ""
}

func TestAttachTicketVectorsMint(t *testing.T) {
	v := loadTicketVectors(t)
	for _, c := range v.Mint {
		t.Run(c.Name, func(t *testing.T) {
			claims := AttachTicketClaims{
				IssuedAt: c.IssuedAt, ExpiresAt: c.ExpiresAt, TicketID: c.TicketID,
				CatalogName: c.CatalogName, DataVersionSpec: c.DataVersionSpec,
				ImplementationVersion: c.ImplementationVersion, OptionsIPC: mustB64(t, c.OptionsIPCB64),
			}
			payload, err := encodeTicketPayload(&claims)
			if err != nil {
				t.Fatal(err)
			}
			if got := hex.EncodeToString(payload); got != c.PayloadHex {
				t.Fatalf("payload\n got %s\nwant %s", got, c.PayloadHex)
			}
			if got := hex.EncodeToString(attachTicketAAD(c.Principal)); got != c.AADHex {
				t.Fatalf("aad = %s, want %s", got, c.AADHex)
			}
			nonce, _ := hex.DecodeString(c.NonceHex)
			token, _, err := mintAttachTicket(vecKey(t, v, c.SigningKeyB64), c.Principal, claims, nonce)
			if err != nil {
				t.Fatal(err)
			}
			if token != c.Token {
				t.Fatalf("token\n got %s\nwant %s", token, c.Token)
			}
		})
	}
}

func TestAttachTicketVectorsAccept(t *testing.T) {
	v := loadTicketVectors(t)
	for _, c := range v.Accept {
		t.Run(c.Name, func(t *testing.T) {
			got, err := OpenAttachTicket(vecKey(t, v, c.SigningKeyB64), c.Token, c.Principal, time.Unix(c.Now, 0))
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			w := c.Claims
			if got.IssuedAt != w.IssuedAt || got.ExpiresAt != w.ExpiresAt || got.TicketID != w.TicketID ||
				got.CatalogName != w.CatalogName || got.DataVersionSpec != w.DataVersionSpec ||
				got.ImplementationVersion != w.ImplementationVersion ||
				!bytes.Equal(got.OptionsIPC, mustB64(t, w.OptionsIPCB64)) {
				t.Fatalf("claims %+v, want %+v", got, w)
			}
		})
	}
}

func TestAttachTicketVectorsReject(t *testing.T) {
	v := loadTicketVectors(t)
	for _, c := range v.Reject {
		t.Run(c.Name, func(t *testing.T) {
			now := v.Defaults.Now
			if c.Now != nil {
				now = *c.Now
			}
			_, err := OpenAttachTicket(vecKey(t, v, c.SigningKeyB64), c.Token, c.Principal, time.Unix(now, 0))
			if err == nil {
				t.Fatal("accepted")
			}
			if kind := ticketErrorKind(err); kind != c.ErrorKind {
				t.Fatalf("error kind %q (%v), want %q", kind, err, c.ErrorKind)
			}
			if len(c.Token) > 12 && strings.Contains(err.Error(), c.Token) {
				t.Fatal("the error echoes the ticket")
			}
			var details interface{ ErrorDetails() []vgirpc.ErrorDetail }
			if !errors.As(err, &details) || len(details.ErrorDetails()) == 0 {
				t.Fatal("no error details")
			}
		})
	}
}

// stringOptionsIPC serializes options as a one-row record of string columns,
// in sorted key order (the vectors' maps are unordered; order is irrelevant).
func stringOptionsIPC(t *testing.T, options map[string]string) []byte {
	t.Helper()
	keys := make([]string, 0, len(options))
	for k := range options {
		keys = append(keys, k)
	}
	// deterministic order
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	fields := make([]arrow.Field, 0, len(keys))
	cols := make([]arrow.Array, 0, len(keys))
	for _, k := range keys {
		b := array.NewStringBuilder(memory.DefaultAllocator)
		b.Append(options[k])
		cols = append(cols, b.NewArray())
		b.Release()
		fields = append(fields, arrow.Field{Name: k, Type: arrow.BinaryTypes.String})
	}
	batch := array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, 1)
	for _, c := range cols {
		c.Release()
	}
	defer batch.Release()
	data, err := SerializeRecordBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func decodeStringOptions(t *testing.T, ipcBytes []byte) map[string]string {
	t.Helper()
	out := map[string]string{}
	if len(ipcBytes) == 0 {
		return out
	}
	batch, err := DeserializeRecordBatch(ipcBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()
	for i, f := range batch.Schema().Fields() {
		out[f.Name] = batch.Column(i).(*array.String).Value(0)
	}
	return out
}

func TestAttachTicketVectorsRedeem(t *testing.T) {
	v := loadTicketVectors(t)
	key := vecKey(t, v, "")
	now := time.Unix(v.Defaults.Now, 0)
	for _, c := range v.Redeem {
		t.Run(c.Name, func(t *testing.T) {
			opts := stringOptionsIPC(t, c.Options)
			req := &CatalogAttachRequestWire{Name: "whatever-the-runner-typed", Options: &opts}
			auth := vgirpc.Anonymous()
			if c.Principal != "" {
				auth = &vgirpc.AuthContext{Principal: c.Principal, Authenticated: true, Domain: "grant"}
			}
			restored, err := redeemAttachTicket(req, key, auth, now)
			if c.ErrorKind != "" {
				if kind := ticketErrorKind(err); kind != c.ErrorKind {
					t.Fatalf("error kind %q (%v), want %q", kind, err, c.ErrorKind)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.Result == nil {
				if restored != nil {
					t.Fatalf("an untouched request was replaced: %+v", restored)
				}
				return
			}
			if restored == nil || restored.Name != c.Result.CatalogName {
				t.Fatalf("restored %+v, want catalog %q", restored, c.Result.CatalogName)
			}
			eq := func(got, want *string) bool {
				return (got == nil && want == nil) || (got != nil && want != nil && *got == *want)
			}
			if !eq(restored.DataVersionSpec, c.Result.DataVersionSpec) ||
				!eq(restored.ImplementationVersion, c.Result.ImplementationVersion) {
				t.Fatalf("version specs %v/%v, want %v/%v", restored.DataVersionSpec, restored.ImplementationVersion,
					c.Result.DataVersionSpec, c.Result.ImplementationVersion)
			}
			var got map[string]string
			if restored.Options == nil {
				got = map[string]string{}
			} else {
				got = decodeStringOptions(t, *restored.Options)
			}
			if len(got) != len(c.Result.Options) {
				t.Fatalf("options %v, want %v", got, c.Result.Options)
			}
			for k, want := range c.Result.Options {
				if got[k] != want {
					t.Fatalf("options %v, want %v", got, c.Result.Options)
				}
			}
		})
	}
}

func TestAttachTicketRoundTripRandomNonceAndID(t *testing.T) {
	key := []byte("k")
	c := AttachTicketClaims{IssuedAt: 10, CatalogName: "c"}
	t1, c1, err := MintAttachTicket(key, "alice", c)
	if err != nil {
		t.Fatal(err)
	}
	t2, c2, err := MintAttachTicket(key, "alice", c)
	if err != nil {
		t.Fatal(err)
	}
	if t1 == t2 || c1.TicketID == c2.TicketID {
		t.Fatal("two tickets share a nonce or an id")
	}
	if _, err := OpenAttachTicket(key, t1, "alice", time.Unix(20, 0)); err != nil {
		t.Fatal(err)
	}
}

func TestRedeemWithoutSigningKeyIsInvalid(t *testing.T) {
	v := loadTicketVectors(t)
	opts := stringOptionsIPC(t, map[string]string{AttachTicketOption: v.Mint[0].Token})
	req := &CatalogAttachRequestWire{Name: "x", Options: &opts}
	auth := &vgirpc.AuthContext{Principal: "alice", Authenticated: true}
	_, err := redeemAttachTicket(req, nil, auth, time.Unix(v.Defaults.Now, 0))
	if ticketErrorKind(err) != "attach_ticket_invalid" {
		t.Fatalf("got %v", err)
	}
}

func TestReservedAttachOptionNameRefusesToStart(t *testing.T) {
	for _, name := range []string{"vgi_attach_ticket", "VGI_Attach_Ticket"} {
		w := NewWorker(WithAttachOptions(AttachOptionSpec{Name: name, Type: arrow.BinaryTypes.String}))
		if _, err := w.buildServer(transportStdio); err == nil || !strings.Contains(err.Error(), "reserved") {
			t.Errorf("%s: buildServer = %v; want a reserved-name error", name, err)
		}
	}
	child := NewWorker(WithCatalogName("child"),
		WithAttachOptions(AttachOptionSpec{Name: "Vgi_Attach_Ticket", Type: arrow.BinaryTypes.String}))
	parent := NewWorker(WithCatalogName("parent"))
	parent.RegisterSubCatalog(child)
	if _, err := parent.buildServer(transportStdio); err == nil {
		t.Error("a sub-catalog declaring the reserved name started")
	}
	perCatalog := NewWorker(WithAttachOptionsForCatalog("alias",
		AttachOptionSpec{Name: "vgi_attach_ticket", Type: arrow.BinaryTypes.String}))
	if _, err := perCatalog.buildServer(transportStdio); err == nil {
		t.Error("a per-catalog option with the reserved name started")
	}
}

// ticketsHosted reports whether s hosts vgi.attach_tickets.v1: adding a
// protocol of that name fails exactly when one is already hosted.
func ticketsHosted(s *vgirpc.Server) bool {
	return s.AddProtocol(vgirpc.NewProtocol(AttachTicketsProtocolName)) != nil
}

func testGrantKeys(t *testing.T) *vgirpc.GrantKeys {
	t.Helper()
	keys, err := vgirpc.NewGrantKeys([][]byte{bytes.Repeat([]byte{3}, 32)}, "t", 600)
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestAttachTicketsHostingConditions(t *testing.T) {
	t.Setenv(SigningKeyEnv, "")
	t.Setenv(vgirpc.GrantKeysEnv, "")
	key := WithHttpSigningKey([]byte("an-explicit-signing-key"))
	mint := WithIdentity(vgirpc.IdentityConfig{MintGrant: func(string, string, []string, int64) (vgirpc.IssuedGrant, error) {
		return vgirpc.IssuedGrant{}, errors.New("unused")
	}})
	cases := []struct {
		name      string
		opts      []WorkerOption
		transport serverTransport
		want      bool
	}{
		{"key and grant keys on HTTP", []WorkerOption{key, WithGrantKeys(testGrantKeys(t))}, transportHTTP, true},
		{"key and mint_grant on HTTP", []WorkerOption{key, mint, WithGrantKeys(nil)}, transportHTTP, true},
		{"no key", []WorkerOption{WithGrantKeys(testGrantKeys(t))}, transportHTTP, false},
		{"key, no way to grant", []WorkerOption{key, WithGrantKeys(nil)}, transportHTTP, false},
		{"stdio", []WorkerOption{key, WithGrantKeys(testGrantKeys(t))}, transportStdio, false},
		{"unix", []WorkerOption{key, WithGrantKeys(testGrantKeys(t))}, transportUnix, false},
		{"tcp", []WorkerOption{key, WithGrantKeys(testGrantKeys(t))}, transportTCP, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := NewWorker(c.opts...).buildServer(c.transport)
			if err != nil {
				t.Fatal(err)
			}
			if got := ticketsHosted(s); got != c.want {
				t.Fatalf("hosted = %v, want %v", got, c.want)
			}
		})
	}
}

// A generated per-process key never hosts tickets; VGI_SIGNING_KEY does.
func TestAttachTicketsNeedAConfiguredKey(t *testing.T) {
	t.Setenv(vgirpc.GrantKeysEnv, "")
	t.Setenv(SigningKeyEnv, "")
	w := NewWorker(WithGrantKeys(testGrantKeys(t)))
	if _, err := w.newHttpServer(); err != nil {
		t.Fatal(err)
	}
	if len(w.httpSigningKey) == 0 || w.ticketSigningKey() != nil {
		t.Fatal("a generated key enabled tickets")
	}
	t.Setenv(SigningKeyEnv, "short")
	w = NewWorker(WithGrantKeys(testGrantKeys(t)))
	if _, err := w.newHttpServer(); err != nil {
		t.Fatal(err)
	}
	if string(w.ticketSigningKey()) != "short" {
		t.Fatal("VGI_SIGNING_KEY was not used as the configured key")
	}
}
