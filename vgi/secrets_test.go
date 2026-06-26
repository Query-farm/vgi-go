package vgi

import "testing"

func mkSecrets() Secrets {
	return Secrets{
		"my_s3": {"type": "s3", "key_id": "AAA", "scope": "s3://bucket-a"},
		"my_s3_b": {"type": "s3", "key_id": "BBB", "scope": "s3://bucket-b\ns3://bucket-b2"},
		"my_gcs": {"type": "gcs", "key_id": "G"},
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
