package vgi

import (
	"errors"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

func mkSecrets() Secrets {
	return Secrets{
		"my_s3":   {"type": "s3", "key_id": "AAA", "scope": "s3://bucket-a"},
		"my_s3_b": {"type": "s3", "key_id": "BBB", "scope": "s3://bucket-b\ns3://bucket-b2"},
		"my_gcs":  {"type": "gcs", "key_id": "G"},
	}
}

func TestSecretTypeAware(t *testing.T) {
	s := mkSecrets()
	if got, _ := s.SecretType("my_s3"); got != "s3" {
		t.Fatalf("SecretType(my_s3)=%q", got)
	}
	if n := len(s.OfType("s3")); n != 2 {
		t.Fatalf("OfType(s3) count=%d, want 2", n)
	}
	if n := len(s.OfType("gcs")); n != 1 {
		t.Fatalf("OfType(gcs) count=%d, want 1", n)
	}
	if n := len(s.OfType("azure")); n != 0 {
		t.Fatalf("OfType(azure) count=%d, want 0", n)
	}
}

func TestForScopeOfTypePerBucket(t *testing.T) {
	s := mkSecrets()
	if v, _ := s.FieldForScope("s3://bucket-a/x.dat", "key_id"); v != "AAA" {
		t.Fatalf("bucket-a key_id=%q, want AAA", v)
	}
	f, ok := s.ForScopeOfType("s3://bucket-b2/y.dat", "s3")
	if !ok || RenderSecretValue(f["key_id"]) != "BBB" {
		t.Fatalf("bucket-b2 -> %v ok=%v", f, ok)
	}
}

func TestForScopeLongestPrefixAndFallback(t *testing.T) {
	s := Secrets{
		"broad":  {"type": "s3", "key_id": "broad", "scope": "s3://bucket"},
		"narrow": {"type": "s3", "key_id": "narrow", "scope": "s3://bucket/data"},
	}
	if v, _ := s.FieldForScope("s3://bucket/data/x.dat", "key_id"); v != "narrow" {
		t.Fatalf("longest-prefix=%q, want narrow", v)
	}
	if v, _ := s.FieldForScope("s3://bucket/other/x.dat", "key_id"); v != "broad" {
		t.Fatalf("broad fallback=%q, want broad", v)
	}
	// Unscoped fallback (old connector / no scope field).
	u := Secrets{"only": {"type": "s3", "key_id": "only"}}
	if v, _ := u.FieldForScope("s3://any/x", "key_id"); v != "only" {
		t.Fatalf("unscoped fallback=%q, want only", v)
	}
	// No match and no fallback.
	if _, ok := s.ForScope("s3://nope/x"); ok {
		t.Fatalf("expected no match for unknown bucket")
	}
}

func TestSecretsRequire(t *testing.T) {
	s := mkSecrets()
	got, err := s.Require("s3")
	if err != nil || got["key_id"] != "AAA" || got["type"] != "s3" {
		t.Fatalf("Require(s3) = %v, %v (want my_s3, the first by name)", got, err)
	}
	_, err = s.Require("azure")
	var missing *MissingSecretError
	if !errors.As(err, &missing) || missing.SecretType != "azure" {
		t.Fatalf("Require(azure) err = %v", err)
	}
	if vgirpc.ErrorCodeOf(err) != vgirpc.CodeFailedPrecondition {
		t.Fatalf("code = %v", vgirpc.ErrorCodeOf(err))
	}
	if _, err := Secrets(nil).Require("s3"); err == nil {
		t.Fatal("nil Secrets satisfied Require")
	}
}

// The extension keys each resolved secret by its unique DuckDB NAME, so by-type
// selection must read the "type" field: an unnamed secret arrives as
// __default_<type>, a named one under any name.
func TestLookupByTypeIgnoresColumnName(t *testing.T) {
	unnamed := Secrets{"__default_brave": {"type": "brave", "name": "__default_brave", "api_key": "k1"}}
	if f, ok := unnamed.Lookup("brave"); !ok || f["api_key"] != "k1" {
		t.Fatalf("unnamed by type = %v, %v", f, ok)
	}
	named := Secrets{"conformance_creds": {"type": "brave", "name": "conformance_creds", "api_key": "k2"}}
	if f, ok := named.Lookup("brave"); !ok || f["api_key"] != "k2" {
		t.Fatalf("named-unlike-type by type = %v, %v", f, ok)
	}
	if got, err := named.Require("brave"); err != nil || got["api_key"] != "k2" {
		t.Fatalf("Require(brave) = %v, %v", got, err)
	}
	// A column named after a type but of another type is not that type.
	decoy := Secrets{"brave": {"type": "s3", "key_id": "x"}}
	if _, ok := decoy.Lookup("brave"); ok {
		t.Fatalf("Lookup matched a column name instead of the type field")
	}
	if n := len(decoy.OfType("brave")); n != 0 {
		t.Fatalf("OfType matched a column name: %d", n)
	}
	if _, err := decoy.Require("brave"); err == nil {
		t.Fatalf("Require matched a column name")
	}
}

// Without a "type" field (an older extension) the column name stands in.
func TestLookupByTypeFallsBackToColumnName(t *testing.T) {
	s := Secrets{"__default_brave": {"api_key": "a"}, "s3": {"key_id": "b"}}
	if f, ok := s.Lookup("brave"); !ok || f["api_key"] != "a" {
		t.Fatalf("__default_ fallback = %v, %v", f, ok)
	}
	if f, ok := s.Lookup("s3"); !ok || f["key_id"] != "b" {
		t.Fatalf("bare-name fallback = %v, %v", f, ok)
	}
	if got, _ := s.SecretType("__default_brave"); got != "brave" {
		t.Fatalf("SecretType fallback = %q", got)
	}
}

// By type alone: an unscoped secret of the type, else the first by name.
func TestLookupPrefersUnscoped(t *testing.T) {
	s := Secrets{
		"a_scoped":  {"type": "t", "v": "scoped", "scope": "x://a/"},
		"z_plain":   {"type": "t", "v": "plain", "scope": ""},
		"other_typ": {"type": "u", "v": "other"},
	}
	for i := 0; i < 20; i++ { // map order must not matter
		if f, ok := s.Lookup("t"); !ok || f["v"] != "plain" {
			t.Fatalf("Lookup(t) = %v, %v; want the unscoped one", f, ok)
		}
	}
	scopedOnly := Secrets{"b": {"type": "t", "v": "b", "scope": "x://b/"}, "a": {"type": "t", "v": "a", "scope": "x://a/"}}
	for i := 0; i < 20; i++ {
		if f, _ := scopedOnly.Lookup("t"); f["v"] != "a" {
			t.Fatalf("Lookup(t) without unscoped = %v; want first by name", f)
		}
	}
	if _, ok := s.Lookup("missing"); ok {
		t.Fatalf("Lookup(missing) found something")
	}
	got := s.OfType("t")
	if len(got) != 2 || got[0]["v"] != "scoped" || got[1]["v"] != "plain" {
		t.Fatalf("OfType order = %v; want sorted by name", got)
	}
}

func TestLookupNamed(t *testing.T) {
	s := Secrets{
		"one":                   {"type": "vgi_example", "v": "1"},
		"two":                   {"type": "vgi_example", "v": "2"},
		"__default_vgi_example": {"type": "vgi_example", "v": "d"},
		"creds_s3":              {"type": "s3", "v": "s"},
	}
	for name, want := range map[string]string{"one": "1", "two": "2", "__default_vgi_example": "d"} {
		if f, ok := s.LookupNamed("vgi_example", name); !ok || f["v"] != want {
			t.Fatalf("LookupNamed(%s) = %v, %v", name, f, ok)
		}
	}
	if _, ok := s.LookupNamed("vgi_example", "nope"); ok {
		t.Fatalf("missing name found")
	}
	if _, ok := s.LookupNamed("vgi_example", "creds_s3"); ok {
		t.Fatalf("name of another type found")
	}
	if f, ok := s.LookupNamed("", "creds_s3"); !ok || f["v"] != "s" {
		t.Fatalf("untyped LookupNamed = %v, %v", f, ok)
	}
}

// Same-type secrets with nested scopes: longest prefix wins, unscoped fallback,
// no match and no fallback -> none; other types never compete.
func TestForScopeOfTypeNested(t *testing.T) {
	s := Secrets{
		"outer":   {"type": "vgi_example", "v": "outer", "scope": "vgi-conformance://a/"},
		"inner":   {"type": "vgi_example", "v": "inner", "scope": "vgi-conformance://a/b/"},
		"plain":   {"type": "vgi_example", "v": "plain", "scope": ""},
		"foreign": {"type": "s3", "v": "foreign", "scope": "vgi-conformance://a/b/c/"},
	}
	cases := map[string]string{
		"vgi-conformance://a/b/x": "inner",
		"vgi-conformance://a/x":   "outer",
		"vgi-conformance://z/x":   "plain",
	}
	for path, want := range cases {
		for i := 0; i < 10; i++ {
			if f, ok := s.ForScopeOfType(path, "vgi_example"); !ok || f["v"] != want {
				t.Fatalf("ForScopeOfType(%s) = %v, %v; want %s", path, f, ok, want)
			}
		}
	}
	delete(s, "plain")
	if f, ok := s.ForScopeOfType("vgi-conformance://z/x", "vgi_example"); ok {
		t.Fatalf("no-match without fallback = %v", f)
	}
	delete(s, "inner")
	if f, _ := s.ForScopeOfType("vgi-conformance://a/b/x", "vgi_example"); f["v"] != "outer" {
		t.Fatalf("after dropping inner = %v; want outer", f)
	}
}

// A requirement without a type used to be serialized as-is and silently never
// resolved; it must now be a clear error at serialization (and at two-phase bind).
func TestRequiredSecretWithoutTypeIsAnError(t *testing.T) {
	if err := validateSecretRequirements("f", []SecretRequirement{{SecretType: "s3"}, {SecretType: "x", Scope: "s3://b/"}}); err != nil {
		t.Fatalf("valid requirements rejected: %v", err)
	}
	for _, bad := range []SecretRequirement{{}, {SecretType: "  "}, {SecretName: "creds"}, {Scope: "s3://b/"}} {
		err := validateSecretRequirements("my_fn", []SecretRequirement{{SecretType: "s3"}, bad})
		if err == nil || !strings.Contains(err.Error(), "my_fn") || !strings.Contains(err.Error(), "RequiredSecrets[1]") {
			t.Fatalf("requirement %+v: err = %v", bad, err)
		}
	}
	if err := validateSecretLookups("my_fn", []SecretLookup{{SecretName: "n"}}); err == nil {
		t.Fatalf("typeless SecretScopeRequest accepted")
	}
}

func TestSerializeFunctionInfoRejectsTypelessSecret(t *testing.T) {
	info := &FunctionInfo{Name: "f", FunctionType: FunctionTypeTable, RequiredSecrets: []SecretRequirement{{SecretType: "s3"}}}
	if _, err := SerializeFunctionInfo(info); err != nil {
		t.Fatalf("valid requirement rejected: %v", err)
	}
	info.RequiredSecrets = append(info.RequiredSecrets, SecretRequirement{Scope: "s3://b/"})
	if _, err := SerializeFunctionInfo(info); err == nil || !strings.Contains(err.Error(), "no SecretType") {
		t.Fatalf("typeless requirement serialized: err = %v", err)
	}
}
