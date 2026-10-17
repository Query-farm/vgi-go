// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// vgi.attach_tickets.v1 2.0.0: AttachTicket.expires_at is a nullable
// timestamp[us, tz="UTC"] (null = no expiry), replacing 1.0.0's float64 unix
// seconds with +Inf for "no expiry".
func TestAttachTicketWireSchemaIs2_0_0(t *testing.T) {
	if AttachTicketsProtocolVersion != "2.0.0" {
		t.Fatalf("AttachTicketsProtocolVersion = %q, want 2.0.0", AttachTicketsProtocolVersion)
	}
	got, err := vgirpc.SchemaForStruct(reflect.TypeOf(AttachTicketWire{}))
	if err != nil {
		t.Fatalf("deriving the AttachTicket schema: %v", err)
	}
	want := arrow.NewSchema([]arrow.Field{
		{Name: "ticket", Type: arrow.BinaryTypes.String},
		{Name: "expires_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
	}, nil)
	assertSchemaMatches(t, "AttachTicket", got, want)
}

func TestSealAttachExpiresAt(t *testing.T) {
	w := NewWorker(WithCatalogName("c"))
	w.signingKeyConfigured = true
	w.httpSigningKey = bytes.Repeat([]byte{7}, 32)
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	cc := &vgirpc.CallContext{Auth: &vgirpc.AuthContext{Principal: "alice", Authenticated: true}}
	req := SealAttachRequestWire{CatalogName: "c"}

	// No maximum lifetime and no requested TTL: the ticket never expires,
	// which is null on the wire (not +Inf, not the zero time).
	unbounded := &attachTickets{worker: w, maxTTL: 0, now: func() time.Time { return now }}
	out, err := unbounded.sealAttach(context.Background(), cc, req)
	if err != nil {
		t.Fatalf("seal_attach: %v", err)
	}
	if out.ExpiresAt != nil {
		t.Errorf("expires_at = %v, want null (no expiry)", *out.ExpiresAt)
	}

	// A ceiling: the expiry is issued_at + lifetime, as a UTC instant at
	// whole-second precision (the sealed claim is int64 seconds).
	capped := &attachTickets{worker: w, maxTTL: 3600, now: func() time.Time { return now }}
	for ttl, want := range map[int64]time.Time{
		0:    now.Add(time.Hour),
		60:   now.Add(time.Minute),
		7200: now.Add(time.Hour),
	} {
		req.TTLSeconds = ttl
		out, err := capped.sealAttach(context.Background(), cc, req)
		if err != nil {
			t.Fatalf("seal_attach ttl=%d: %v", ttl, err)
		}
		if out.ExpiresAt == nil {
			t.Fatalf("ttl=%d: expires_at is null, want %v", ttl, want)
		}
		if !out.ExpiresAt.Equal(want) || out.ExpiresAt.Location() != time.UTC {
			t.Errorf("ttl=%d: expires_at = %v, want %v (UTC)", ttl, *out.ExpiresAt, want)
		}
		if got := out.ExpiresAt.UnixMicro(); got != want.UnixMicro() {
			t.Errorf("ttl=%d: expires_at = %d µs, want %d", ttl, got, want.UnixMicro())
		}
	}
}
