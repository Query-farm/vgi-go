// Copyright 2026 Query Farm LLC - https://query.farm

package table

import (
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

func TestRequiredFilterScanSchemasDoNotDeclareSQLNotNull(t *testing.T) {
	fixtures := []struct {
		name string
		bind func(*vgi.BindParams) (*vgi.BindResponse, error)
	}{
		{"simple", (&RffSimpleScanFunction{}).OnBind},
		{"none", (&RffNoneScanFunction{}).OnBind},
		{"struct", (&RffStructScanFunction{}).OnBind},
		{"nested", (&RffNestedScanFunction{}).OnBind},
		{"multi", (&RffMultiScanFunction{}).OnBind},
		{"rowid", (&RffRowidScanFunction{}).OnBind},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			bound, err := fixture.bind(nil)
			if err != nil {
				t.Fatal(err)
			}
			var check func(arrow.Field, string)
			check = func(field arrow.Field, path string) {
				if !field.Nullable {
					t.Errorf("ordinary field %s claims an undeclared SQL NOT NULL", path)
				}
				if nested, ok := field.Type.(*arrow.StructType); ok {
					for _, child := range nested.Fields() {
						check(child, path+"."+child.Name)
					}
				}
			}
			for _, field := range bound.OutputSchema.Fields() {
				if field.Name == "row_id" {
					if field.Nullable || field.Metadata.FindKey("is_row_id") < 0 {
						t.Fatal("hidden row ID must retain its required storage contract")
					}
					continue
				}
				check(field, field.Name)
			}
		})
	}
}
