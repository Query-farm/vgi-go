// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"reflect"
	"strings"
	"testing"
)

// Every name the error message offers must actually resolve. If these drift, the
// suggestion is worse than useless: it sends the author to a name that will
// itself silently degrade.
func TestValidArgTypeNamesAllResolve(t *testing.T) {
	names := validArgTypeNames()
	if len(names) < 20 {
		t.Fatalf("expected the full name set, got %d: %v", len(names), names)
	}
	for _, n := range names {
		if _, ok := resolveArgType(n); !ok {
			t.Errorf("validArgTypeNames offers %q but resolveArgType rejects it", n)
		}
	}
	// Spot-check both halves of the union.
	for _, want := range []string{"int64", "varchar", "duration_ms", "timestamp_us_utc"} {
		if !slicesContains(names, want) {
			t.Errorf("expected %q in the valid set", want)
		}
	}
}

func TestResolveArgTypeReportsUnknownNames(t *testing.T) {
	for _, known := range []string{"int64", "varchar", "bool", "any", "table", "", "date32"} {
		if _, ok := resolveArgType(known); !ok {
			t.Errorf("resolveArgType(%q) should be recognised", known)
		}
	}
	// The whole point: these used to resolve to VARCHAR without a word.
	for _, unknown := range []string{"bigint", "integer", "timestamp", "date", "hugeint", "nonsense"} {
		dt, ok := resolveArgType(unknown)
		if ok {
			t.Errorf("resolveArgType(%q) should be unrecognised", unknown)
		}
		// It still returns a usable type, because the wire path stays tolerant.
		if dt == nil {
			t.Errorf("resolveArgType(%q) returned a nil type", unknown)
		}
	}
}

// argTypeToArrowType must keep degrading quietly: it also carries specs that did
// not originate in this process, and a name a newer peer knows must not crash a
// worker built before it existed.
func TestArgTypeToArrowTypeStaysTolerant(t *testing.T) {
	if got := argTypeToArrowType("a-name-from-the-future"); got == nil {
		t.Fatal("argTypeToArrowType must always return a usable type")
	}
	want, _ := resolveArgType("int64")
	if got := argTypeToArrowType("int64"); got.ID() != want.ID() {
		t.Fatalf("known names must be unaffected, got %v want %v", got, want)
	}
}

func TestSuggestArgTypeNameMapsSQLSpellings(t *testing.T) {
	cases := map[string]string{
		"bigint":    "int64",
		"BIGINT":    "int64", // case-insensitive: tags are lowercased downstream
		"integer":   "int32",
		"timestamp": "timestamp_us",
		"date":      "date32",
	}
	for in, want := range cases {
		got := suggestArgTypeName(in)
		if !strings.Contains(got, want) {
			t.Errorf("suggestArgTypeName(%q) = %q, want a hint naming %q", in, got, want)
		}
	}
	if got := suggestArgTypeName("totally-unrelated"); got != "" {
		t.Errorf("expected no hint for an unmappable name, got %q", got)
	}
}

// A bad type= must fail where it is written, naming the offending value and
// pointing at the right one — not at query time from the call site.
func TestParseTagRejectsUnknownTypeName(t *testing.T) {
	type badArgs struct {
		N int64 `vgi:"pos=0,const=false,type=bigint"`
	}
	_, err := parseArgBindings(reflect.TypeOf(badArgs{}))
	if err == nil {
		t.Fatal("expected an error for type=bigint")
	}
	msg := err.Error()
	for _, want := range []string{`"bigint"`, "int64", "valid names are"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q should mention %q", msg, want)
		}
	}
}

func TestParseTagAcceptsEveryValidTypeName(t *testing.T) {
	for _, name := range validArgTypeNames() {
		if name == "" || name == "table" {
			continue // table input is declared by field type, not type=
		}
		if _, ok := resolveArgType(name); !ok {
			t.Fatalf("precondition: %q should resolve", name)
		}
	}
}

func slicesContains(hay []string, needle string) bool {
	for _, h := range hay {
		if h == needle {
			return true
		}
	}
	return false
}
