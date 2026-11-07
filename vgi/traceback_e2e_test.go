// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

type tbState struct{ Done bool }

// tbProbe fails bind from a named helper (with a recorded stack) or by
// panicking, depending on its name.
type tbProbe struct{ name string }

func (p tbProbe) Name() string                                { return p.name }
func (tbProbe) Metadata() vgi.FunctionMetadata                { return vgi.DefaultMetadata() }
func (tbProbe) ArgumentSpecs() []vgi.ArgSpec                  { return nil }
func (tbProbe) NewState(*vgi.ProcessParams) (*tbState, error) { return &tbState{}, nil }
func (tbProbe) Process(context.Context, *vgi.ProcessParams, *tbState, *vgirpc.OutputCollector) error {
	return nil
}
func (p tbProbe) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	if p.name == "tb_panic" {
		panicInTracebackProbe()
	}
	return nil, failInTracebackProbe()
}

func failInTracebackProbe() error { return vgirpc.Errorf("upstream lookup failed") }
func panicInTracebackProbe()      { panic("tb probe exploded") }

func tracebackClient(t *testing.T, opts ...vgi.WorkerOption) *vgirpc.HttpClient {
	t.Helper()
	w := vgi.NewWorker(append([]vgi.WorkerOption{vgi.WithCatalogName("example")}, opts...)...)
	w.RegisterTable(vgi.AsTableFunction[tbState](tbProbe{name: "tb_fail"}))
	w.RegisterTable(vgi.AsTableFunction[tbState](tbProbe{name: "tb_panic"}))
	hs, err := w.NewHttpServerForTest()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(hs)
	t.Cleanup(ts.Close)
	c, err := vgirpc.NewHttpClient(ts.URL, vgirpc.WithClientProtocol(vgi.ProtocolName),
		vgirpc.WithClientProtocolVersion(vgi.ProtocolVersion))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// The remote traceback shows the handler's error site (or panic site), not the
// dispatcher's frames; WithIncludeTracebacks(false) and VGI_INCLUDE_TRACEBACKS=0
// turn it off.
func TestTracebacks_E2E(t *testing.T) {
	c := tracebackClient(t)
	for fn, site := range map[string]string{
		"tb_fail":  "failInTracebackProbe",
		"tb_panic": "panicInTracebackProbe",
	} {
		e := bindError(t, c, map[string]any{"function_name": fn, "function_type": string(vgi.FunctionTypeTable)})
		if !strings.Contains(e.Traceback, site) {
			t.Errorf("%s: traceback lacks %s:\n%s", fn, site, e.Traceback)
		}
		if strings.Contains(e.Traceback, "buildErrorExtra") {
			t.Errorf("%s: traceback shows dispatcher frames:\n%s", fn, e.Traceback)
		}
	}

	off := tracebackClient(t, vgi.WithIncludeTracebacks(false))
	if e := bindError(t, off, map[string]any{"function_name": "tb_fail", "function_type": string(vgi.FunctionTypeTable)}); e.Traceback != "" {
		t.Errorf("WithIncludeTracebacks(false) still sent:\n%s", e.Traceback)
	}

	t.Setenv("VGI_INCLUDE_TRACEBACKS", "0")
	envOff := tracebackClient(t)
	if e := bindError(t, envOff, map[string]any{"function_name": "tb_fail", "function_type": string(vgi.FunctionTypeTable)}); e.Traceback != "" {
		t.Errorf("VGI_INCLUDE_TRACEBACKS=0 still sent:\n%s", e.Traceback)
	}
	// The explicit option wins over the environment.
	optOn := tracebackClient(t, vgi.WithIncludeTracebacks(true))
	if e := bindError(t, optOn, map[string]any{"function_name": "tb_fail", "function_type": string(vgi.FunctionTypeTable)}); e.Traceback == "" {
		t.Error("WithIncludeTracebacks(true) overridden by the environment")
	}
}
