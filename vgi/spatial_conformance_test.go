// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// intersectsExtentCases is testdata/intersects_extent_cases.json: DuckDB core's
// `a && b` over every ordered pair of a set of edge-case geometries.
type intersectsExtentCases struct {
	Geometries []struct {
		Name  string `json:"name"`
		WKBLE string `json:"wkb_le"`
		WKBBE string `json:"wkb_be"`
	} `json:"geometries"`
	Cases []struct {
		Left     string `json:"left"`
		Right    string `json:"right"`
		Expected bool   `json:"expected"`
	} `json:"cases"`
}

func wkbField(name string) arrow.Field {
	md := arrow.NewMetadata([]string{"ARROW:extension:name"}, []string{"geoarrow.wkb"})
	return arrow.Field{Name: name, Type: arrow.BinaryTypes.Binary, Nullable: true, Metadata: md}
}

func binaryArray(values ...[]byte) arrow.Array {
	builder := array.NewBinaryBuilder(memory.NewGoAllocator(), arrow.BinaryTypes.Binary)
	defer builder.Release()
	for _, value := range values {
		builder.Append(value)
	}
	return builder.NewArray()
}

// spatialFilters decodes `geom && <literal>` as a pushed v2 predicate with the
// given mode, through the same decoder a worker advertising the capability uses.
func spatialFilters(t *testing.T, mode string, literal []byte) *PushdownFilters {
	t.Helper()
	document := `{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"` + mode + `","source":"query","expression":{"node":"call","function":{"namespace":"duckdb.spatial","name":"intersects_extent","version":1},"arguments":[{"node":"column_ref","column_index":0,"column_name":"geom"},{"node":"literal","value_ref":0}]}}]}`
	// The decoded filters reference the encoded batch's literal column, so
	// both stay alive until the test ends.
	value := binaryArray(literal)
	t.Cleanup(value.Release)
	encoded := filterV2Batch(t, document, []arrow.Field{wkbField("value_0")}, []arrow.Array{value})
	t.Cleanup(encoded.Release)
	meta := FunctionMetadata{AdditionalFilterFunctions: []FilterFunctionCapability{{Namespace: "duckdb.spatial", Name: "intersects_extent", Version: 1}}}
	schema := arrow.NewSchema([]arrow.Field{wkbField("geom")}, nil)
	filters, err := deserializeFiltersForMetadata(encoded, nil, nil, meta, schema)
	if err != nil {
		t.Fatal(err)
	}
	return filters
}

func geomBatch(values ...[]byte) arrow.RecordBatch {
	column := binaryArray(values...)
	defer column.Release()
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{wkbField("geom")}, nil), []arrow.Array{column}, int64(len(values)))
}

// The pushed intersects_extent filter answers exactly as DuckDB core's `&&`
// on every conformance case, for each byte order of either side. A false where
// DuckDB says true loses rows: DuckDB removes an exact `&&` from its plan.
func TestIntersectsExtentConformance(t *testing.T) {
	raw, err := os.ReadFile("testdata/intersects_extent_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc intersectsExtentCases
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	decode := func(s string) []byte {
		b, err := hex.DecodeString(s)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	index := map[string]int{}
	le := make([][]byte, len(doc.Geometries))
	be := make([][]byte, len(doc.Geometries))
	for i, g := range doc.Geometries {
		index[g.Name] = i
		le[i], be[i] = decode(g.WKBLE), decode(g.WKBBE)
	}
	expected := map[[2]string]bool{}
	for _, c := range doc.Cases {
		expected[[2]string{c.Left, c.Right}] = c.Expected
	}
	if len(expected) != len(doc.Geometries)*len(doc.Geometries) {
		t.Fatalf("conformance file has %d distinct cases for %d geometries", len(expected), len(doc.Geometries))
	}
	encodings := map[string][][]byte{"le": le, "be": be}
	for _, mode := range []string{"required", "advisory"} {
		for _, leftEncoding := range []string{"le", "be"} {
			for _, rightEncoding := range []string{"le", "be"} {
				t.Run(fmt.Sprintf("%s/%s_%s", mode, leftEncoding, rightEncoding), func(t *testing.T) {
					rows := geomBatch(encodings[leftEncoding]...)
					defer rows.Release()
					mismatches := 0
					for _, right := range doc.Geometries {
						filters := spatialFilters(t, mode, encodings[rightEncoding][index[right.Name]])
						mask, err := filters.Evaluate(context.Background(), rows)
						if err != nil {
							t.Errorf("%s && %s: %v", "<rows>", right.Name, err)
							continue
						}
						got := mask.(*array.Boolean)
						for i, left := range doc.Geometries {
							want := expected[[2]string{left.Name, right.Name}]
							if got.IsNull(i) || got.Value(i) != want {
								mismatches++
								t.Errorf("%s && %s: got %v (null=%v), want %v", left.Name, right.Name, got.Value(i), got.IsNull(i), want)
							}
						}
						mask.Release()
					}
					if mismatches != 0 {
						t.Logf("%d mismatches", mismatches)
					}
				})
			}
		}
	}
}

// A row the evaluator cannot read as WKB fails an exact intersects_extent
// predicate, as it fails DuckDB's own ST_GeomFromWKB, but an advisory one is
// optional: the predicate is skipped and the rows are kept rather than the
// query failing or the rows being dropped.
func TestIntersectsExtentUnreadableWKB(t *testing.T) {
	one, _ := hex.DecodeString("0101000000000000000000f03f000000000000f03f")
	for name, value := range map[string][]byte{
		"truncated":        one[:8],
		"not_wkb":          []byte("hello"),
		"empty":            {},
		"unsupported_type": append([]byte{1, 0xff, 0, 0, 0}, one[5:]...),
		"trailing_bytes":   append(append([]byte{}, one...), 0, 0),
	} {
		t.Run(name, func(t *testing.T) {
			rows := geomBatch(one, value)
			defer rows.Release()
			mask, err := spatialFilters(t, "advisory", one).Evaluate(context.Background(), rows)
			if err != nil {
				t.Fatalf("advisory predicate failed the batch: %v", err)
			}
			defer mask.Release()
			got := mask.(*array.Boolean)
			for i := 0; i < got.Len(); i++ {
				if got.IsNull(i) || !got.Value(i) {
					t.Fatalf("advisory predicate dropped row %d", i)
				}
			}
			if mask, err := spatialFilters(t, "required", one).Evaluate(context.Background(), rows); err == nil {
				mask.Release()
				t.Fatal("exact predicate evaluated unreadable WKB without error")
			}
		})
	}
}

// ToSQLExact has no SQL rendering for intersects_extent and declines it rather
// than widening it to 1=1.
func TestIntersectsExtentIsNotTranslatedToSQL(t *testing.T) {
	one, _ := hex.DecodeString("0101000000000000000000f03f000000000000f03f")
	for _, mode := range []string{"required", "advisory"} {
		if _, _, err := spatialFilters(t, mode, one).ToSQLExact(nil, "?"); !errors.Is(err, ErrFilterNotTranslatable) {
			t.Fatalf("%s: ToSQLExact returned %v, want ErrFilterNotTranslatable", mode, err)
		}
	}
}
