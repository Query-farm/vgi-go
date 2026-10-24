// Copyright 2026 Query Farm LLC - https://query.farm

package versioned_tables

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestVersionedUserFieldsDoNotClaimUndeclaredConstraints(t *testing.T) {
	for _, schema := range []*arrow.Schema{AnimalsSchemaV1, AnimalsSchemaV11, PlantsSchema} {
		for _, field := range schema.Fields() {
			if !field.Nullable {
				t.Fatalf("ordinary fixture field %s claims undeclared NOT_NULL", field.Name)
			}
		}
	}
}
