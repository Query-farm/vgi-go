// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

// TableCardinality represents the estimated cardinality of a table function's output.
type TableCardinality struct {
	// Estimate is the estimated number of rows.
	Estimate int64 `vgirpc:"estimate,nullable"`
	// Max is the maximum number of rows (-1 = unknown).
	Max int64 `vgirpc:"max,nullable"`
}
