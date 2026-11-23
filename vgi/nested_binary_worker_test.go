// Copyright 2026 Query Farm LLC - https://query.farm

package vgi

// Worker-side nested-binary conformance (vgi docs/large-values-3.4.md §3.3
// rule 11, §3.4 test 3). The generated tests in vgi/generated cover the
// wrapper itself; these drive the worker's own request types and handlers.

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func nestedFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "conformance", "nested-binary", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// bindCallColumn reads the bind_call cell of an encoded one-row request.
func bindCallColumn(t *testing.T, encoded []byte) []byte {
	t.Helper()
	r, err := ipc.NewReader(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	if !r.Next() {
		t.Fatal("no batch")
	}
	batch := r.RecordBatch()
	idx := batch.Schema().FieldIndices("bind_call")
	if len(idx) != 1 {
		t.Fatal("no bind_call column")
	}
	return bytes.Clone(batch.Column(idx[0]).(*array.Binary).Value(0))
}

// Rule 11: whatever the worker does with a decoded bind call -- parse it,
// open its sealed values, overwrite fields of its decoded copy -- the
// embedding request re-encodes the bytes it received, and so does the init
// recipe (what rides the init StateRef and table-buffering storage).
func TestBindCallReencodesReceivedBytesAfterOpening(t *testing.T) {
	want := nestedFixture(t, "bind_call.bin")
	w := NewWorker()
	exercise := func(t *testing.T, call BindRequestWire) {
		// Parsing opens the attach / transaction values (here: unsealed
		// pass-through) into BindParams, never into the request.
		_, _ = w.parseBindRequestWith(call, nil, nil)
		plain := []byte("opened plaintext")
		call.AttachOpaqueData = &plain
		call.TransactionOpaqueData = &plain
		names := []string{"rewritten"}
		call.ArgumentNames = &names
	}
	cases := []struct {
		file string
		run  func(t *testing.T, data []byte) []byte
	}{
		{"init-noncanonical.arrow", func(t *testing.T, data []byte) []byte {
			var req InitRequestWire
			if err := vgirpc.UnmarshalRecord(data, &req); err != nil {
				t.Fatal(err)
			}
			call, err := bindCallValue(generated.InitRequest(req).BindCallValue)
			if err != nil {
				t.Fatal(err)
			}
			exercise(t, call)
			recipe := InitRecipe{BindCallIPC: req.BindCall.Bytes()}
			enc, err := encodeInitRecipe(&recipe)
			if err != nil {
				t.Fatal(err)
			}
			back, err := decodeInitRecipe(enc)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(back.BindCallIPC, want) {
				t.Fatal("init recipe does not carry the received bind_call bytes")
			}
			out, err := vgirpc.MarshalRecord(generated.InitRequest(req))
			if err != nil {
				t.Fatal(err)
			}
			return out
		}},
		{"cardinality-noncanonical.arrow", func(t *testing.T, data []byte) []byte {
			var req TableFunctionCardinalityRequestWire
			if err := vgirpc.UnmarshalRecord(data, &req); err != nil {
				t.Fatal(err)
			}
			call, err := bindCallValue(generated.TableFunctionCardinalityRequest(req).BindCallValue)
			if err != nil {
				t.Fatal(err)
			}
			exercise(t, call)
			out, err := vgirpc.MarshalRecord(generated.TableFunctionCardinalityRequest(req))
			if err != nil {
				t.Fatal(err)
			}
			return out
		}},
		{"statistics-noncanonical.arrow", func(t *testing.T, data []byte) []byte {
			var req TableFunctionStatisticsRequestWire
			if err := vgirpc.UnmarshalRecord(data, &req); err != nil {
				t.Fatal(err)
			}
			call, err := bindCallValue(generated.TableFunctionStatisticsRequest(req).BindCallValue)
			if err != nil {
				t.Fatal(err)
			}
			exercise(t, call)
			out, err := vgirpc.MarshalRecord(generated.TableFunctionStatisticsRequest(req))
			if err != nil {
				t.Fatal(err)
			}
			return out
		}},
		{"dynamic-to-string-noncanonical.arrow", func(t *testing.T, data []byte) []byte {
			var req TableFunctionDynamicToStringRequestWire
			if err := vgirpc.UnmarshalRecord(data, &req); err != nil {
				t.Fatal(err)
			}
			call, err := bindCallValue(generated.TableFunctionDynamicToStringRequest(req).BindCallValue)
			if err != nil {
				t.Fatal(err)
			}
			exercise(t, call)
			out, err := vgirpc.MarshalRecord(generated.TableFunctionDynamicToStringRequest(req))
			if err != nil {
				t.Fatal(err)
			}
			return out
		}},
		{"plan-noncanonical.arrow", func(t *testing.T, data []byte) []byte {
			var req TableFunctionPlanRequestWire
			if err := vgirpc.UnmarshalRecord(data, &req); err != nil {
				t.Fatal(err)
			}
			plan, err := planRequestOf(req)
			if err != nil {
				t.Fatal(err)
			}
			exercise(t, plan.BindCall)
			out, err := vgirpc.MarshalRecord(generated.TableFunctionPlanRequest(req))
			if err != nil {
				t.Fatal(err)
			}
			return out
		}},
		{"projection-noncanonical.arrow", func(t *testing.T, data []byte) []byte {
			var req TableFunctionProjectionRequestWire
			if err := vgirpc.UnmarshalRecord(data, &req); err != nil {
				t.Fatal(err)
			}
			call, err := bindCallValue(generated.TableFunctionProjectionRequest(req).BindCallValue)
			if err != nil {
				t.Fatal(err)
			}
			exercise(t, call)
			out, err := vgirpc.MarshalRecord(generated.TableFunctionProjectionRequest(req))
			if err != nil {
				t.Fatal(err)
			}
			return out
		}},
	}
	for _, c := range cases {
		t.Run(c.file, func(t *testing.T) {
			got := bindCallColumn(t, c.run(t, nestedFixture(t, c.file)))
			if !bytes.Equal(got, want) {
				t.Fatalf("re-encoded bind_call (%d bytes) differs from the received bytes (%d bytes)", len(got), len(want))
			}
		})
	}
}

// §3.4 test 3, worker side: a null arguments with a location decodes as
// exactly that (a nil []byte is asserted explicitly); both-null and both-set
// are refused with invalid_bind_request; and a location reaching a worker that
// cannot resolve it is invalid_arguments_ref, never "no arguments".
func TestBindArgumentsExactlyOne(t *testing.T) {
	decode := func(name string) BindRequestWire {
		var req BindRequestWire
		if err := vgirpc.UnmarshalRecord(nestedFixture(t, name), &req); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return req
	}
	w := NewWorker()

	located := decode("bind-arguments-null.arrow")
	if located.Arguments != nil {
		t.Fatalf("arguments = %v, want null", *located.Arguments)
	}
	if located.ArgumentsLocation == nil || located.ArgumentsLocation.Size < 1 || len(located.ArgumentsLocation.Sha256) != 64 {
		t.Fatalf("arguments_location = %+v", located.ArgumentsLocation)
	}
	_, err := w.handleBind(context.Background(), shapeTestCallCtx(), located)
	if vgirpc.ErrorKindOf(err) != vgirpc.ErrorKindInvalidArgumentsRef || !strings.HasPrefix(errMessage(err), "invalid_arguments_ref:") {
		t.Fatalf("unresolvable location: err = %v (kind %q)", err, vgirpc.ErrorKindOf(err))
	}

	for _, name := range []string{"bind-both-null.arrow", "bind-both-set.arrow"} {
		_, err := w.handleBind(context.Background(), shapeTestCallCtx(), decode(name))
		if vgirpc.ErrorKindOf(err) != ErrorKindInvalidBindRequest || errMessage(err) != InvalidBindRequestMessage {
			t.Fatalf("%s: err = %v (kind %q), want invalid_bind_request", name, err, vgirpc.ErrorKindOf(err))
		}
		if re, ok := err.(*vgirpc.RpcError); !ok || re.Code != string(vgirpc.CodeInvalidArgument) {
			t.Fatalf("%s: err = %#v, want code INVALID_ARGUMENT", name, err)
		}
	}
}

func errMessage(err error) string {
	if re, ok := err.(*vgirpc.RpcError); ok {
		return re.Message
	}
	if err == nil {
		return ""
	}
	return err.Error()
}

// The split-token fingerprint tags the argument form: a location contributes
// its size and SHA-256 (never the ref, whose nonce differs between equal
// binds), inline arguments their bytes, and the two never collide.
func TestArgumentsFingerprintInput(t *testing.T) {
	sha := strings.Repeat("ab", 32)
	a := BindRequestWire{FunctionName: "f", ArgumentsLocation: &generated.ArgumentsLocation{Ref: []byte("nonce-1"), Size: 9, Sha256: sha}}
	b := BindRequestWire{FunctionName: "f", ArgumentsLocation: &generated.ArgumentsLocation{Ref: []byte("nonce-2"), Size: 9, Sha256: sha}}
	if !bytes.Equal(bindFingerprintFor(&a), bindFingerprintFor(&b)) {
		t.Fatal("fingerprint depends on the sealed ref")
	}
	c := b
	c.ArgumentsLocation = &generated.ArgumentsLocation{Ref: []byte("nonce-2"), Size: 10, Sha256: sha}
	if bytes.Equal(bindFingerprintFor(&a), bindFingerprintFor(&c)) {
		t.Fatal("fingerprint ignores the size")
	}
	in := argumentsFingerprintInput(&a)
	if in[0] != 0x01 || len(in) != 1+8+32 {
		t.Fatalf("location input = %x", in)
	}
	raw := []byte("args")
	inline := BindRequestWire{FunctionName: "f", Arguments: &raw}
	if got := argumentsFingerprintInput(&inline); got[0] != 0x00 || !bytes.HasSuffix(got, raw) {
		t.Fatalf("inline input = %x", got)
	}
}

func TestArgumentsRefBytesFromEnv(t *testing.T) {
	env := func(v string) func(string) string {
		return func(k string) string {
			if k == ArgumentsRefBytesEnv {
				return v
			}
			return ""
		}
	}
	if v, err := ArgumentsRefBytesFromEnv(env("")); err != nil || v != DefaultArgumentsRefBytes {
		t.Fatalf("default = %d, %v", v, err)
	}
	if v, err := ArgumentsRefBytesFromEnv(env("65536")); err != nil || v != 65536 {
		t.Fatalf("65536 = %d, %v", v, err)
	}
	for _, bad := range []string{"0", "-1", "+5", "012", "1e6", "abc", "12345678901234567890"} {
		if _, err := ArgumentsRefBytesFromEnv(env(bad)); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}
