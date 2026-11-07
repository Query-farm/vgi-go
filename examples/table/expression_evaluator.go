// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

// The filter examples set AutoApplyFilters (and expression_filter_example /
// spatial_filter_example evaluate ExpressionFilters), so any worker
// registering them links the embedded-DuckDB expression evaluator.
import _ "github.com/Query-farm/vgi-go/vgi/exprduckdb"
