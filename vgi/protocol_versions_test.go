// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

// Protocol version negotiation in the SDK (PROTOCOL_VERSIONS_V1): what a
// worker advertises, and the VGI_TEST_PROTOCOL_RANGE fixture switch.

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func versionsOnHealth(t *testing.T) string {
	t.Helper()
	hs, err := vgi.NewWorker(vgi.WithCatalogName("pv")).NewHttpServerForTest()
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	hs.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/health", nil))
	values, ok := rec.Header()[http.CanonicalHeaderKey(vgirpc.ProtocolVersionsHeader)]
	if !ok || len(values) != 1 {
		t.Fatalf("HEAD /health carries no VGI-Protocol-Versions: %v", rec.Header())
	}
	return values[0]
}

func TestWorkerAdvertisesVgiV3(t *testing.T) {
	t.Setenv(vgi.TestProtocolRangeEnv, "")
	if got := versionsOnHealth(t); got != "vgi.v3=3.4-3.4" {
		t.Fatalf("VGI-Protocol-Versions = %q, want vgi.v3=3.4-3.4", got)
	}
}

func TestFixtureSwitchBareRange(t *testing.T) {
	t.Setenv(vgi.TestProtocolRangeEnv, "3.3-3.3")
	if got := versionsOnHealth(t); got != "vgi.v3=3.3-3.3" {
		t.Fatalf("VGI-Protocol-Versions = %q, want vgi.v3=3.3-3.3", got)
	}
}

func TestFixtureSwitchNamedRange(t *testing.T) {
	t.Setenv(vgi.TestProtocolRangeEnv, "vgi.v3=3.3-3.4")
	if got := versionsOnHealth(t); got != "vgi.v3=3.3-3.4" {
		t.Fatalf("VGI-Protocol-Versions = %q", got)
	}
}

func TestFixtureSwitchMalformedRefusesToStart(t *testing.T) {
	for _, value := range []string{"3.x", "vgi.v3=nope", "vgi.v3=3.4-3.3", "nope.v1=3.3-3.3"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(vgi.TestProtocolRangeEnv, value)
			_, err := vgi.NewWorker(vgi.WithCatalogName("pv")).NewHttpServerForTest()
			if err == nil || !strings.Contains(err.Error(), vgi.TestProtocolRangeEnv) {
				t.Fatalf("%q: err = %v, want a refusal naming %s", value, err, vgi.TestProtocolRangeEnv)
			}
		})
	}
}

// A 3.4 client against a 3.3-3.3 fixture: protocol_version_unsupported naming
// both windows, over HTTP.
func TestFixtureSwitchRefusalNamesBothWindows(t *testing.T) {
	t.Setenv(vgi.TestProtocolRangeEnv, "3.3-3.3")
	hs, err := vgi.NewWorker(vgi.WithCatalogName("pv")).NewHttpServerForTest()
	if err != nil {
		t.Fatal(err)
	}
	schema := arrow.NewSchema(nil, nil)
	batch := array.NewRecordBatchWithMetadata(schema, nil, 0, arrow.NewMetadata(
		[]string{vgirpc.MetaMethod, vgirpc.MetaRequestVersion, vgirpc.MetaProtocol, vgirpc.MetaProtocolVersion,
			vgirpc.MetaClientProtocolVersions},
		[]string{"bind", vgirpc.ProtocolVersion, vgi.ProtocolName, vgi.ProtocolVersion, "vgi.v3=3.4-3.4"}))
	defer batch.Release()
	var body bytes.Buffer
	w := ipc.NewWriter(&body, ipc.WithSchema(schema))
	if err := w.Write(batch); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	req := httptest.NewRequest(http.MethodPost, "/vgi.v3/bind", &body)
	req.Header.Set("Content-Type", "application/vnd.apache.arrow.stream")
	rec := httptest.NewRecorder()
	hs.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", rec.Code)
	}
	if got := rec.Header().Get(vgirpc.ProtocolVersionsHeader); got != "vgi.v3=3.3-3.3" {
		t.Fatalf("VGI-Protocol-Versions = %q", got)
	}
	raw, _ := io.ReadAll(rec.Body)
	text := string(raw)
	for _, want := range []string{vgirpc.ErrorKindProtocolVersionUnsupported, "client speaks vgi.v3 3.4 (supports 3.4-3.4)",
		"this server supports vgi.v3 3.3-3.3", "The server is too old"} {
		if !strings.Contains(text, want) {
			t.Fatalf("refusal lacks %q: %q", want, text)
		}
	}
}
