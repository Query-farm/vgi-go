// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// The records in this file are not built column-by-column like the ones in
// wire_schema_completeness_test.go. They are `*Wire` structs whose Arrow schema
// vgi-rpc-go DERIVES from their struct tags — so the drift is not a forgotten
// builder line but a tag, a Go type, or a field position that disagrees with
// what the protocol declares.
//
// That disagreement is invisible from inside this SDK. The derived schema is
// used to both write and read, so a worker talking to itself is always
// self-consistent; only a peer notices. And the peer that notices is the one
// that validates its parameter contract with Schema.Equal — at which point
// EVERY call of that shape fails at once, with a message about an out-of-date
// Arrow schema rather than about the field that moved.
//
// Four real disagreements were sitting here when this test was written, all of
// them on the request records the C++ client sends:
//
//   - InitRequest.projection_ids derived list<int32>; the protocol says int64.
//   - InitRequest.pushdown_filters / .join_keys / .split_tokens derived
//     binary / list<binary>; the protocol says large_binary. (A filter tree, a
//     join-key set or a batch of split tokens can exceed the 2 GiB that 32-bit
//     offsets address, which is why the protocol declares them large.)
//   - TableFunctionPlanRequest repeated the same three.
//   - GlobalInitResponse declared its fields in a different order.
//
// None of them broke a test, because nothing compared the two.

// derivedRecordCase is one reflection-derived wire record.
//
// `origin` is the dataclass name in the generated file's "// Origin: X"
// comment — the same join key TestEveryGeneratedRecordSchemaIsCovered uses, so
// a record covered here counts as covered there.
type derivedRecordCase struct {
	origin string
	typ    reflect.Type
	schema *arrow.Schema
}

// derivedRecordCases lists every generated record whose schema this SDK
// derives from a Go struct rather than assembling by hand.
func derivedRecordCases() []derivedRecordCase {
	return []derivedRecordCase{
		{"BindRequest", reflect.TypeOf(BindRequestWire{}), generated.BindRequestSchema},
		{"CopyFromContext", reflect.TypeOf(CopyFromContextWire{}), generated.CopyFromContextSchema},
		{"CopyToContext", reflect.TypeOf(CopyToContextWire{}), generated.CopyToContextSchema},
		{"InitRequest", reflect.TypeOf(InitRequestWire{}), generated.InitRequestSchema},
		{"GlobalInitResponse", reflect.TypeOf(GlobalInitResponseWire{}), generated.GlobalInitResponseSchema},
		{"CatalogAttachRequest", reflect.TypeOf(CatalogAttachRequestWire{}), generated.CatalogAttachRequestSchema},
		{"TableFunctionCardinalityRequest", reflect.TypeOf(CardinalityRequestWire{}), generated.TableFunctionCardinalityRequestSchema},
		{"TableFunctionPlanRequest", reflect.TypeOf(PlanRequestWire{}), generated.TableFunctionPlanRequestSchema},

		{"AggregateBindRequest", reflect.TypeOf(AggregateBindRequestWire{}), generated.AggregateBindRequestSchema},
		{"AggregateUpdateRequest", reflect.TypeOf(AggregateUpdateRequestWire{}), generated.AggregateUpdateRequestSchema},
		{"AggregateCombineRequest", reflect.TypeOf(AggregateCombineRequestWire{}), generated.AggregateCombineRequestSchema},
		{"AggregateFinalizeRequest", reflect.TypeOf(AggregateFinalizeRequestWire{}), generated.AggregateFinalizeRequestSchema},
		{"AggregateDestructorRequest", reflect.TypeOf(AggregateDestructorRequestWire{}), generated.AggregateDestructorRequestSchema},

		{"TableBufferingProcessRequest", reflect.TypeOf(TableBufferingProcessRequestWire{}), generated.TableBufferingProcessRequestSchema},
		{"TableBufferingCombineRequest", reflect.TypeOf(TableBufferingCombineRequestWire{}), generated.TableBufferingCombineRequestSchema},
		{"TableBufferingDestructorRequest", reflect.TypeOf(TableBufferingDestructorRequestWire{}), generated.TableBufferingDestructorRequestSchema},
	}
}

func TestDerivedWireSchemasMatchGenerated(t *testing.T) {
	for _, tc := range derivedRecordCases() {
		t.Run(tc.origin, func(t *testing.T) {
			got, err := vgirpc.SchemaForStruct(tc.typ)
			if err != nil {
				t.Fatalf("%s: deriving the schema from %s failed: %v", tc.origin, tc.typ, err)
			}
			assertSchemaMatches(t, tc.origin, got, tc.schema)
		})
	}
}

// A case list that has drifted from the type it names would pass vacuously, so
// check the join key itself: every origin here must be one the generator emits.
func TestDerivedRecordCasesNameRealOrigins(t *testing.T) {
	declared := generatedOrigins(t)
	for _, tc := range derivedRecordCases() {
		if !declared[tc.origin] {
			t.Errorf("derivedRecordCases names %q, which the generated schemas do not declare",
				tc.origin)
		}
	}
}
