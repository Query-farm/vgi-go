// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// newLandingTestWorker builds a small worker with a scalar, a catalog table, and
// a view, and materializes its read-only catalog (as buildServer would).
func newLandingTestWorker(t *testing.T) *Worker {
	t.Helper()
	w := NewWorker(
		WithCatalogName("testcat"),
		WithCatalogComment("Test catalog for the landing surface"),
	)
	w.RegisterScalar(AsScalarFunction[exampleArgs](exampleImpl{}))
	w.RegisterCatalogTable("main", CatalogTable{
		Name:    "nums",
		Comment: "Numbers table",
		Columns: arrow.NewSchema([]arrow.Field{
			{Name: "value", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	})
	w.RegisterCatalogView("main", CatalogView{
		Name:           "v1",
		Definition:     "SELECT 1 AS x",
		Comment:        "A view",
		ColumnComments: map[string]string{"x": "the x column"},
	})
	w.catalog = NewDefaultReadOnlyCatalog(w.catalogName, w)
	return w
}

// landingTestMux mirrors the route registration RunHttp performs, so the tests
// exercise the same handlers (including net/http path-value extraction).
func landingTestMux(w *Worker) *http.ServeMux {
	mux := http.NewServeMux()
	serverID := "testserver01"
	mux.HandleFunc("GET /{$}", makeLandingHandler(serverID))
	mux.HandleFunc("GET /describe.json", w.makeDescribeJSONHandler("testcat", serverID, false))
	mux.HandleFunc("GET /describe/{catalog}/{schema}/{table}", w.makeColumnsHandler())
	return mux
}

func TestLandingHTMLHasAssetMarker(t *testing.T) {
	if !bytes.Contains(landingHTML, []byte("vgi-landing-asset v")) {
		t.Fatalf("embedded landing.html is missing the vgi-landing-asset marker")
	}
}

func TestLandingRouteHTMLAndJSON(t *testing.T) {
	mux := landingTestMux(newLandingTestWorker(t))

	// Browser → HTML with the pinned asset marker.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept", "text/html")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET / (html): status %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("GET / (html): content-type %q", ct)
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("vgi-landing-asset v")) {
		t.Errorf("GET / (html): missing asset marker")
	}

	// ?format=json → JSON status.
	req = httptest.NewRequest(http.MethodGet, "/?format=json", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	var status map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatalf("GET /?format=json: %v", err)
	}
	if status["status"] != "ok" || status["protocol"] != "vgi" {
		t.Errorf("GET /?format=json: %v", status)
	}
}

func TestDescribeJSONShape(t *testing.T) {
	w := newLandingTestWorker(t)
	doc := w.buildDescribeJSON("testcat", "testserver01", false)

	if doc["landing_schema_version"] != landingSchemaVersion {
		t.Errorf("landing_schema_version = %v", doc["landing_schema_version"])
	}
	worker := doc["worker"].(map[string]any)
	if worker["lang"] != "go" {
		t.Errorf("worker.lang = %v", worker["lang"])
	}
	if worker["name"] != "testcat" {
		t.Errorf("worker.name = %v", worker["name"])
	}
	if doc["cupola_base"] != cupolaBase {
		t.Errorf("cupola_base = %v", doc["cupola_base"])
	}

	catalogs := doc["catalogs"].([]map[string]any)
	if len(catalogs) != 1 {
		t.Fatalf("expected 1 catalog, got %d", len(catalogs))
	}
	cat := catalogs[0]
	if cat["name"] != "testcat" {
		t.Errorf("catalog name = %v", cat["name"])
	}
	// implementation_version / data_version_spec must be present and null.
	if v, ok := cat["implementation_version"]; !ok || v != nil {
		t.Errorf("implementation_version = %v (ok=%v)", v, ok)
	}

	// Find the main schema and assert the table/view/function surfaced.
	var main map[string]any
	for _, s := range cat["schemas"].([]map[string]any) {
		if s["name"] == "main" {
			main = s
		}
	}
	if main == nil {
		t.Fatalf("main schema not found")
	}
	tables := main["tables"].([]map[string]any)
	if len(tables) != 1 || tables[0]["name"] != "nums" || tables[0]["cols"] != 1 {
		t.Errorf("tables = %v", tables)
	}
	views := main["views"].([]map[string]any)
	if len(views) != 1 || views[0]["name"] != "v1" || views[0]["def"] != "SELECT 1 AS x" {
		t.Errorf("views = %v", views)
	}
	foundFn := false
	for _, f := range main["functions"].([]map[string]any) {
		if f["name"] == "double_example" {
			foundFn = true
			if f["type"] != "scalar" {
				t.Errorf("double_example type = %v", f["type"])
			}
		}
	}
	if !foundFn {
		t.Errorf("double_example not in functions")
	}

	// The whole document must round-trip through encoding/json (the wire path).
	if _, err := json.Marshal(doc); err != nil {
		t.Fatalf("describe.json not marshalable: %v", err)
	}
}

// TestDescribeAliasCatalogScoping verifies that a secondary (alias) catalog in
// describe.json reports ONLY the objects that belong to it — its own
// catalog-scoped functions — and does NOT inherit the primary catalog's shared
// tables/views or its unscoped functions.
func TestDescribeAliasCatalogScoping(t *testing.T) {
	w := newLandingTestWorker(t) // main: double_example (unscoped) + nums table + v1 view

	// Simulate a catalog-scoped function belonging to alias catalog "acc"
	// (as RegisterTableForCatalog would wire it).
	main := w.catalog.schemas["main"]
	main.functions = append(main.functions, FunctionInfo{
		Name:         "acc_read",
		SchemaName:   "main",
		FunctionType: FunctionTypeTable,
		catalogHome:  "acc",
	})

	hasFn := func(schemas []map[string]any, schema, fn string) bool {
		for _, s := range schemas {
			if s["name"] != schema {
				continue
			}
			for _, f := range s["functions"].([]map[string]any) {
				if f["name"] == fn {
					return true
				}
			}
		}
		return false
	}

	// Primary catalog: unscoped double_example is present; the acc-scoped
	// function is hidden; shared tables/views are present.
	primary, pc := w.buildDescribeSchemas("testcat", false)
	if !hasFn(primary, "main", "double_example") {
		t.Errorf("primary: expected unscoped double_example to be present")
	}
	if hasFn(primary, "main", "acc_read") {
		t.Errorf("primary: acc-scoped function leaked into primary catalog")
	}
	if pc["tables"] == 0 || pc["views"] == 0 {
		t.Errorf("primary: expected shared tables/views, got counts %v", pc)
	}

	// Alias catalog: only the acc-scoped function; no inherited tables/views;
	// no unscoped functions.
	alias, ac := w.buildDescribeSchemas("acc", true)
	if !hasFn(alias, "main", "acc_read") {
		t.Errorf("alias: expected acc_read in the alias catalog")
	}
	if hasFn(alias, "main", "double_example") {
		t.Errorf("alias: unscoped double_example leaked into the alias catalog")
	}
	if ac["functions"] != 1 || ac["tables"] != 0 || ac["views"] != 0 {
		t.Errorf("alias counts = %v, want functions=1 tables=0 views=0", ac)
	}
}

func TestColumnsRoute(t *testing.T) {
	mux := landingTestMux(newLandingTestWorker(t))

	cases := []struct {
		path     string
		wantCode int
		wantCol  string
	}{
		{"/describe/testcat/main/nums.json", http.StatusOK, "value"},
		{"/describe/testcat/main/v1.json", http.StatusOK, "x"},
		{"/describe/testcat/main/missing.json", http.StatusNotFound, ""},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != tc.wantCode {
			t.Errorf("%s: status %d, want %d", tc.path, rec.Code, tc.wantCode)
			continue
		}
		if tc.wantCode != http.StatusOK {
			continue
		}
		var payload struct {
			Columns []map[string]any `json:"columns"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Errorf("%s: %v", tc.path, err)
			continue
		}
		if len(payload.Columns) != 1 || payload.Columns[0]["name"] != tc.wantCol {
			t.Errorf("%s: columns = %v", tc.path, payload.Columns)
		}
	}
}
