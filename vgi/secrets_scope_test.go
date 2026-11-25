package vgi

import (
	"encoding/json"
	"os"
	"testing"
)

// scopeMatchCase is one entry of testdata/scope_match_vectors.json, generated
// from DuckDB's own BaseSecret::MatchScore and StringUtil::GetURIAuthority
// (Haybarn b2aa6ca, duckdb#28684). The same file is shared by every VGI SDK.
type scopeMatchCase struct {
	Scope string `json:"scope"`
	Path  string `json:"path"`
	Match bool   `json:"match"`
}

// TestScopeMatchesCurated holds the first 25 shared vectors inline, so the
// rule reads without opening the JSON.
func TestScopeMatchesCurated(t *testing.T) {
	cases := []scopeMatchCase{
		{"snmp://10.0.0.1", "snmp://10.0.0.1", true},
		{"snmp://10.0.0.1", "snmp://10.0.0.1/x", true},
		{"snmp://10.0.0.1", "snmp://10.0.0.15", false},
		{"snmp://10.0.0.1", "snmp://10.0.0.1:161", false},
		{"s3://bucket", "s3://bucket/key", true},
		{"s3://bucket", "s3://bucket-evil/key", false},
		{"s3://bucket", "s3://bucket?x=1", false},
		{"dlt://vgi_dlt_fixture", "dlt://vgi_dlt_fixture:shop", false},
		{"dlt://vgi_dlt_fixture", "dlt://vgi_dlt_fixture/shop", true},
		{"s3://bucket/", "s3://bucket/key", true},
		{"s3://bucket/pre", "s3://bucket/prefix/x", true},
		{"s3://bucket/pre", "s3://bucket/other", false},
		{"https://h?a", "https://h?ab", true},
		{"https://h#f", "https://h#fx", true},
		{"a://b\\c", "a://b\\cd", true},
		{"snmp://", "snmp://anything", true},
		{"snmp://10.0.0.", "snmp://10.0.0.15:161", false},
		{"foo", "foobar", true},
		{"c:\\data", "c:\\data2\\x", true},
		{"x:/y://z", "x:/y://zz", true},
		{"s3://bucket", "s3://buck", false},
		{"s3://bucket", "s3://bucket", true},
		{"vgi-conformance://host1", "vgi-conformance://host1/data", true},
		{"vgi-conformance://host1", "vgi-conformance://host10/data", false},
		{"vgi-conformance://host1", "vgi-conformance://host1:9000", false},
	}
	for _, c := range cases {
		if got := ScopeMatches(c.Scope, c.Path); got != c.Match {
			t.Errorf("ScopeMatches(%q, %q) = %v, want %v", c.Scope, c.Path, got, c.Match)
		}
	}
}

// TestScopeMatchesVectors checks ScopeMatches against every case DuckDB's C++
// decided.
func TestScopeMatchesVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/scope_match_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Cases []scopeMatchCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	if len(v.Cases) < 3000 {
		t.Fatalf("scope_match_vectors.json has %d cases, want at least 3000", len(v.Cases))
	}
	for _, c := range v.Cases {
		if got := ScopeMatches(c.Scope, c.Path); got != c.Match {
			t.Errorf("ScopeMatches(%q, %q) = %v, want %v", c.Scope, c.Path, got, c.Match)
		}
	}
}

func TestScopeMatchesEmptyPrefix(t *testing.T) {
	if ScopeMatches("", "s3://bucket/x") {
		t.Fatal("an empty prefix is not a scope")
	}
}

// TestForScopeAuthorityBoundary: an authority-only scope covers its own host
// and paths under it, never a longer host name or another port.
func TestForScopeAuthorityBoundary(t *testing.T) {
	s := Secrets{
		"host1":   {"type": "snmp", "community": "host1", "scope": "snmp://10.0.0.1"},
		"host15":  {"type": "snmp", "community": "host15", "scope": "snmp://10.0.0.15"},
		"default": {"type": "snmp", "community": "default"},
	}
	for path, want := range map[string]string{
		"snmp://10.0.0.1/sys": "host1",
		"snmp://10.0.0.15":    "host15",
		"snmp://10.0.0.1:161": "default",
	} {
		fields, ok := s.ForScopeOfType(path, "snmp")
		if !ok || RenderSecretValue(fields["community"]) != want {
			t.Errorf("ForScopeOfType(%q) = %v ok=%v, want %s", path, fields, ok, want)
		}
		if v, _ := s.FieldForScope(path, "community"); v != want {
			t.Errorf("FieldForScope(%q) = %q, want %s", path, v, want)
		}
	}
}
