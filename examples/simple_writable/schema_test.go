// Copyright 2026 Query Farm LLC - https://query.farm

package simple_writable

import "testing"

func TestWritableUserFieldsDoNotClaimUndeclaredConstraints(t *testing.T) {
	for name, schema := range userSchemas {
		for _, field := range schema.Fields() {
			if !field.Nullable {
				t.Fatalf("%s.%s claims unenforced NOT_NULL", name, field.Name)
			}
		}
	}
	if rowIDField().Nullable {
		t.Fatal("required row identifier changed")
	}
}
