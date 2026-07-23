// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"context"
	"strconv"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Same-name-in-two-schemas scalar fixtures (`test_same_name_bind`).
//
// Two distinct scalar functions register under the *same* function name but
// live in different catalog schemas (`main` and `data`). They exist to prove
// that VGI resolves a schema-qualified call to the implementation in that
// schema — `example.main.test_same_name_bind(x)` must reach the main class and
// `example.data.test_same_name_bind(x)` the data one — rather than collapsing
// both into one flat by-name registry entry.
//
// Each returns a VARCHAR tagged with its own schema, so a mis-routed call is
// visible in the query result rather than silently plausible. Driven by
// ../vgi/test/sql/integration/scalar/same_name_schemas.test.

// sameNameFunctionName is the name both implementations register under. The
// collision is the point.
const sameNameFunctionName = "test_same_name_bind"

// tagWithSchema renders "<schemaName>:<value>" for every row, preserving nulls.
func tagWithSchema(schemaName string, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	get := vgi.Int64Accessor(batch.Column(0)) // hoist the type switch out of the row loop
	prefix := schemaName + ":"
	return vgi.MapColumn(params, batch, 0, array.NewStringBuilder,
		func(_ arrow.Array, i int) string {
			return prefix + strconv.FormatInt(get(i), 10)
		})
}

// SameNameMainFunction is `test_same_name_bind` as registered in the `main`
// schema.
type SameNameMainFunction struct{}

func (f *SameNameMainFunction) Name() string { return sameNameFunctionName }

func (f *SameNameMainFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Schema-disambiguation probe; the main-schema implementation",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT example.main.test_same_name_bind(1)", Description: "Returns 'main:1'"},
		},
	}
}

func (f *SameNameMainFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "int64", Doc: "Integer value to tag"},
	}
}

func (f *SameNameMainFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *SameNameMainFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return tagWithSchema("main", params, batch)
}

// SameNameDataFunction is `test_same_name_bind` as registered in the `data`
// schema — same registered name as SameNameMainFunction, different body.
type SameNameDataFunction struct{}

func (f *SameNameDataFunction) Name() string { return sameNameFunctionName }

func (f *SameNameDataFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Schema-disambiguation probe; the data-schema implementation",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT example.data.test_same_name_bind(1)", Description: "Returns 'data:1'"},
		},
	}
}

func (f *SameNameDataFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "value", Position: 0, ArrowType: "int64", Doc: "Integer value to tag"},
	}
}

func (f *SameNameDataFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *SameNameDataFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return tagWithSchema("data", params, batch)
}
