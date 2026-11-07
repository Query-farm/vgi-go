// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

// The SDK's own filter tests evaluate pushed filters, so the test binary links
// the embedded-DuckDB evaluator. A blank import in an external test file
// registers it for the package's internal tests too (one test binary).
import _ "github.com/Query-farm/vgi-go/vgi/exprduckdb"
