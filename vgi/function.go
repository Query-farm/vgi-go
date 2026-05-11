// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import "github.com/apache/arrow-go/v18/arrow"

// TypeBoundPredicate is a function that validates whether an Arrow DataType
// is acceptable for a given argument. Used with ArgSpec.TypeBound to constrain
// "any"-typed arguments to specific type families (e.g., numeric types only).
type TypeBoundPredicate func(arrow.DataType) bool

// FunctionType identifies the kind of VGI function.
type FunctionType string

const (
	FunctionTypeScalar    FunctionType = "scalar"
	FunctionTypeTable     FunctionType = "table"
	FunctionTypeAggregate FunctionType = "aggregate"
)

// FunctionStability describes when function results may change.
type FunctionStability string

const (
	StabilityConsistent            FunctionStability = "CONSISTENT"
	StabilityVolatile              FunctionStability = "VOLATILE"
	StabilityConsistentWithinQuery FunctionStability = "CONSISTENT_WITHIN_QUERY"
)

// NullHandling describes how the function handles NULL inputs.
// These values are DuckDB wire-protocol constants and must not be changed.
type NullHandling string

const (
	// NullHandlingDefault tells DuckDB to skip calling the function for NULL
	// inputs and return NULL automatically (standard SQL behaviour).
	NullHandlingDefault NullHandling = "DEFAULT"

	// NullHandlingSpecial tells DuckDB to pass NULL values through to the
	// function, letting it decide what to return. Use this when the function
	// needs to distinguish NULL from non-NULL inputs.
	NullHandlingSpecial NullHandling = "SPECIAL"

	// NullHandlingReceiveNulls is a Go-friendly alias for NullHandlingSpecial.
	NullHandlingReceiveNulls = NullHandlingSpecial
)

// SecretRequirement describes a secret type that a function needs.
type SecretRequirement struct {
	SecretType string
	SecretName string // empty = not specified
	Scope      string // empty = not specified
}

// SecretLookup describes a scoped secret lookup request for two-phase bind.
type SecretLookup struct {
	SecretType string
	SecretName string
	Scope      string
}

// FunctionMetadata holds descriptive metadata about a function.
type FunctionMetadata struct {
	// Description is a human-readable description.
	Description string
	// Stability controls caching/optimization hints.
	Stability FunctionStability
	// NullHandling controls whether NULLs are passed to the function.
	NullHandling NullHandling
	// ProjectionPushdown indicates support for projection pushdown.
	ProjectionPushdown bool
	// FilterPushdown indicates support for filter pushdown.
	FilterPushdown bool
	// SamplingPushdown indicates support for TABLESAMPLE SYSTEM pushdown.
	SamplingPushdown bool
	// SupportedExpressionFilters lists DuckDB expression names that the
	// function can absorb into its scan (e.g. "&&", "list_contains",
	// "starts_with"). Without this, DuckDB inserts a separate FILTER node.
	SupportedExpressionFilters []string
	// SupportsWindow indicates an aggregate also implements the windowed
	// callbacks (window_init, window, window_destructor). Ignored for
	// non-aggregate functions.
	SupportsWindow bool
	// StreamingPartitioned indicates an aggregate also implements the
	// streaming-partitioned protocol (aggregate_streaming_open/_chunk/_close).
	// DuckDB's optimizer can replace a LogicalWindow with the streaming
	// operator when the frame is cumulative. The function MUST also keep its
	// standard update/combine/finalize path for the fallback case.
	StreamingPartitioned bool
	// HasFinalize signals a TableInOut function whose Finalize method emits
	// meaningful batches. Defaults to false; set to true only for functions
	// that accumulate during Process and flush at end-of-stream. DuckDB
	// skips the FINALIZE phase RPC entirely when this is false — required
	// for LATERAL compatibility (avoids "FinalExecute not supported for
	// project_input").
	HasFinalize bool
	// OrderPreservation declares how a table function's output rows relate
	// to its inputs. Values: "PRESERVES_ORDER", "FIXED_ORDER",
	// "NO_ORDER_GUARANTEE". Empty leaves the field null and uses the C++
	// extension default. Maps to DuckDB's OrderPreservationType.
	OrderPreservation string
	// OrderDependent declares whether the aggregate result depends on the
	// row order. Empty defaults to NOT_ORDER_DEPENDENT.
	OrderDependent string
	// DistinctDependent declares whether DISTINCT changes the result.
	// Empty defaults to NOT_DISTINCT_DEPENDENT.
	DistinctDependent string
	// AutoApplyFilters indicates the framework should auto-apply pushdown filters.
	AutoApplyFilters bool
	// Categories is a list of classification tags for the function.
	Categories []string
	// ReturnType is the static return type for scalar functions.
	// When set, the catalog registers this concrete type instead of ANY.
	// Leave nil for functions with dynamic return types (resolved at bind time).
	ReturnType arrow.DataType
	// RequiredSecrets lists secret types the function needs at bind time.
	RequiredSecrets []SecretRequirement
}

// DefaultMetadata returns metadata with default values.
func DefaultMetadata() FunctionMetadata {
	return FunctionMetadata{
		Stability:    StabilityConsistent,
		NullHandling: NullHandlingDefault,
	}
}

// ArgSpec describes a single argument in a function's signature.
type ArgSpec struct {
	// Name is the argument name (empty for positional-only).
	Name string
	// Position is the positional index (0-based). -1 for named-only.
	Position int
	// ArrowType is the Arrow type string (e.g., "int64", "varchar", "any").
	ArrowType string
	// Doc is a documentation string.
	Doc string
	// IsConst is true for constant (scalar) parameters.
	IsConst bool
	// IsVarargs is true for variadic parameters.
	IsVarargs bool
	// HasDefault is true if the parameter has a default value.
	HasDefault bool
	// DefaultValue is the string representation of the default.
	DefaultValue string
	// ArrowDataType is an optional concrete Arrow DataType for the argument.
	// When set, it takes precedence over ArrowType for schema building.
	// Use this for complex types like structs where the string representation
	// is insufficient (e.g., arrow.StructOf(...) for typed struct params).
	ArrowDataType arrow.DataType
	// TypeBound is an optional slice of type predicates for "any"-typed arguments.
	// At bind time, the input schema field type must satisfy at least one predicate
	// (OR logic). Nil means no type constraint (any type is accepted).
	TypeBound []TypeBoundPredicate
}
