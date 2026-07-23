// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// resolveOverload picks the best-matching function from a list of candidates
// given the parsed arguments and optional input schema.
//
// DuckDB resolves overloads on its side and sends only const args in
// args.Positional. The algorithm:
//  1. Count matching: const spec count must match len(args.Positional)
//  2. Type scoring: score const args and inputSchema fields
//  3. Pick the candidate with the highest score
func resolveOverload(candidates []interface{}, args *Arguments, inputSchema *arrow.Schema) (interface{}, error) {
	if len(candidates) == 1 {
		return candidates[0], nil
	}

	type scored struct {
		fn    interface{}
		score int
	}

	var matches []scored

	for _, fn := range candidates {
		specs := getArgSpecsFromFn(fn)

		// Split specs into const-positional and non-const-positional
		var constSpecs []ArgSpec
		var nonConstSpecs []ArgSpec
		hasVarargs := false
		var varargsSpec *ArgSpec

		for i := range specs {
			if specs[i].IsVarargs {
				hasVarargs = true
				varargsSpec = &specs[i]
			}
			if specs[i].Position >= 0 && specs[i].IsConst {
				constSpecs = append(constSpecs, specs[i])
			} else if specs[i].Position >= 0 && !specs[i].IsConst {
				nonConstSpecs = append(nonConstSpecs, specs[i])
			}
		}

		// Count matching: check const positional args against args.Positional
		numPositionalArgs := len(args.Positional)
		numConstSpecs := len(constSpecs)

		if hasVarargs {
			// For varargs functions, actual args must be >= the non-varargs specs
			if varargsSpec.IsConst {
				// All args are const (table functions with varargs)
				nonVarargsCount := numConstSpecs
				if varargsSpec != nil {
					nonVarargsCount-- // subtract the varargs spec itself
				}
				if numPositionalArgs < nonVarargsCount {
					continue
				}
			} else {
				// Varargs are columns (scalar functions with varargs)
				if numPositionalArgs < numConstSpecs {
					continue
				}
				// Non-const varargs: check inputSchema field count
				if inputSchema != nil {
					inputFields := inputSchema.NumFields()
					nonVarargsNonConst := len(nonConstSpecs)
					if varargsSpec != nil {
						nonVarargsNonConst-- // subtract the varargs spec
					}
					if inputFields < nonVarargsNonConst {
						continue
					}
				}
			}
		} else {
			// No varargs: exact const arg count match
			if numPositionalArgs != numConstSpecs {
				continue
			}
			// Also check non-const count against inputSchema
			if inputSchema != nil && len(nonConstSpecs) > 0 {
				if inputSchema.NumFields() != len(nonConstSpecs) {
					continue
				}
			}
		}

		// Type scoring
		score := 0
		rejected := false

		// Score const args against args.Positional
		for i, spec := range constSpecs {
			if spec.IsVarargs {
				// Score each remaining positional arg against the varargs type
				for j := i; j < numPositionalArgs; j++ {
					if j < len(args.Positional) && args.Positional[j] != nil {
						s := scoreType(args.Positional[j].DataType(), spec)
						if s < 0 {
							rejected = true
							break
						}
						score += s
					}
				}
				break
			}
			if i < len(args.Positional) && args.Positional[i] != nil {
				s := scoreType(args.Positional[i].DataType(), spec)
				if s < 0 {
					rejected = true
					break
				}
				score += s
			}
		}
		if rejected {
			continue
		}

		// Score non-const args against inputSchema
		if inputSchema != nil {
			fieldIdx := 0
			for _, spec := range nonConstSpecs {
				if spec.IsVarargs {
					// Score each remaining field against the varargs type
					for fi := fieldIdx; fi < inputSchema.NumFields(); fi++ {
						s := scoreType(inputSchema.Field(fi).Type, spec)
						if s < 0 {
							rejected = true
							break
						}
						score += s
					}
					break
				}
				if fieldIdx < inputSchema.NumFields() {
					s := scoreType(inputSchema.Field(fieldIdx).Type, spec)
					if s < 0 {
						rejected = true
						break
					}
					score += s
					fieldIdx++
				}
			}
		}
		if rejected {
			continue
		}

		matches = append(matches, scored{fn: fn, score: score})
	}

	if len(matches) == 0 {
		return nil, nil
	}

	// Pick the candidate with the highest score
	best := matches[0]
	for _, m := range matches[1:] {
		if m.score > best.score {
			best = m
		}
	}

	LogRPC.Debug("overload: resolved",
		"candidates", len(candidates),
		"matches", len(matches),
		"score", best.score,
	)

	return best.fn, nil
}

// scoreType scores how well an actual Arrow DataType matches a spec.
// Returns: 2 for exact match, 1 for family match, 0 for any-type spec, -1 for incompatible.
func scoreType(actual arrow.DataType, spec ArgSpec) int {
	// Determine the expected type
	var expected arrow.DataType
	if spec.ArrowDataType != nil {
		expected = spec.ArrowDataType
	} else {
		expected = argTypeToArrowType(spec.ArrowType)
	}

	// "any" type spec always matches with score 0
	if spec.ArrowType == "any" || spec.ArrowType == "" {
		if expected == arrow.Null {
			return 0
		}
	}

	if expected == nil || expected == arrow.Null {
		return 0
	}

	// Exact match
	if arrow.TypeEqual(actual, expected) {
		return 2
	}

	// Family match
	if typesInSameFamily(actual, expected) {
		return 1
	}

	return -1
}

// typesInSameFamily checks if two types belong to the same type family.
func typesInSameFamily(a, b arrow.DataType) bool {
	// Integer family
	if IsIntegerType(a) && IsIntegerType(b) {
		return true
	}
	// Float family (includes decimal)
	if (IsFloatingType(a) || IsDecimalType(a)) && (IsFloatingType(b) || IsDecimalType(b)) {
		return true
	}
	// String family
	if isStringFamily(a) && isStringFamily(b) {
		return true
	}
	// Binary family
	if isBinaryFamily(a) && isBinaryFamily(b) {
		return true
	}
	return false
}

func isStringFamily(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.STRING, arrow.LARGE_STRING:
		return true
	default:
		return false
	}
}

func isBinaryFamily(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.BINARY, arrow.LARGE_BINARY:
		return true
	default:
		return false
	}
}

// getArgSpecsFromFn extracts ArgumentSpecs from any function type.
func getArgSpecsFromFn(fn interface{}) []ArgSpec {
	switch f := fn.(type) {
	case ScalarFunction:
		return f.ArgumentSpecs()
	case TableFunction:
		return f.ArgumentSpecs()
	case TableInOutFunction:
		return f.ArgumentSpecs()
	case TableBufferingFunction:
		return f.ArgumentSpecs()
	}
	return nil
}

// functionLookup names the implementation a call is asking for. A function name
// alone is not a unique key — the same name may be declared in two schemas of
// one catalog, or in two catalogs served by one worker process — so resolution
// takes the whole (catalog, schema, name) triple plus the argument shape.
type functionLookup struct {
	// Name is the function name the caller invoked.
	Name string
	// Type is the DuckDB-side function type (scalar/table/aggregate/...).
	Type FunctionType
	// Schema is the catalog schema the caller named, lowercased by the
	// resolver. Empty when the caller named none (a COPY handler bind, or a
	// pre-1.1.0 client), which widens the lookup to every schema.
	Schema string
	// Catalog is the catalog the call arrived through, derived from the
	// attachment. Empty when there is no attachment (or it could not be
	// opened), which disables catalog scoping for this lookup.
	Catalog string
	// Args and InputSchema drive overload resolution within the matched set.
	Args        *Arguments
	InputSchema *arrow.Schema
}

// candidate pairs a registered implementation with the origin recorded for it,
// so the resolver can filter on the declaring schema/catalog.
type candidate struct {
	fn     interface{}
	origin funcOrigin
}

// collect appends every registration of (kind, name) from one registry.
func collect[T any](w *Worker, reg map[string][]T, kind funcKind, name string, out []candidate) []candidate {
	for i, fn := range reg[name] {
		out = append(out, candidate{fn: fn, origin: w.originOf(kind, name, i)})
	}
	return out
}

// candidatesFor gathers every registration of name, preferring the registry
// matching the requested function type and falling back to the others (DuckDB
// reports table-in-out and table-buffering functions as TABLE, and old clients
// may send an unrecognized type).
func (w *Worker) candidatesFor(name string, ft FunctionType) []candidate {
	var out []candidate

	switch normalizeFunctionType(ft) {
	case FunctionTypeScalar:
		out = collect(w, w.scalars, kindScalar, name, out)
	case FunctionTypeTable:
		out = collect(w, w.tables, kindTable, name, out)
		if len(out) == 0 {
			out = collect(w, w.tableInOuts, kindTableInOut, name, out)
		}
		if len(out) == 0 {
			out = collect(w, w.tableBufferings, kindTableBuffering, name, out)
		}
	case FunctionTypeAggregate:
		out = collect(w, w.tableInOuts, kindTableInOut, name, out)
	case FunctionTypeTableBuffering:
		out = collect(w, w.tableBufferings, kindTableBuffering, name, out)
	}

	// Fallback: try all registries.
	if len(out) == 0 {
		out = collect(w, w.scalars, kindScalar, name, out)
	}
	if len(out) == 0 {
		out = collect(w, w.tables, kindTable, name, out)
	}
	if len(out) == 0 {
		out = collect(w, w.tableInOuts, kindTableInOut, name, out)
	}
	if len(out) == 0 {
		out = collect(w, w.tableBufferings, kindTableBuffering, name, out)
	}
	return out
}

// schemasOf lists, sorted and deduplicated, the schemas the given candidates
// were declared in. Used to make a failed or ambiguous lookup actionable.
func schemasOf(cands []candidate) []string {
	seen := make(map[string]struct{}, len(cands))
	var out []string
	for _, c := range cands {
		if _, ok := seen[c.origin.schema]; ok {
			continue
		}
		seen[c.origin.schema] = struct{}{}
		out = append(out, c.origin.schema)
	}
	sort.Strings(out)
	return out
}

// scopeToHome narrows candidates to the registrations the caller could possibly
// mean, by their recorded home:
//
//   - Catalog: a call arriving through catalog X can only reach functions homed
//     in X. This is what lets one worker process serve two catalogs that each
//     declare the same function name.
//   - Schema: a schema-qualified call is exact — only functions declared in that
//     schema survive, so a name registered in two schemas resolves to the one
//     the caller named instead of colliding. Naming a schema that does not hold
//     the function reports where it does live rather than the generic
//     unknown-function list.
//
// A caller that names no schema keeps every candidate; the caller decides
// whether a surviving cross-schema tie is an error (see crossSchemaAmbiguity).
func (w *Worker) scopeToHome(cands []candidate, name, schema, catalog string) ([]candidate, error) {
	if catalog != "" {
		var inCatalog []candidate
		for _, c := range cands {
			if c.origin.catalog == catalog {
				inCatalog = append(inCatalog, c)
			}
		}
		if len(inCatalog) == 0 {
			return nil, &vgirpc.RpcError{
				Type:    "ValueError",
				Message: fmt.Sprintf("Function '%s' is not available in catalog '%s'", name, catalog),
			}
		}
		cands = inCatalog
	}

	if lowered := strings.ToLower(schema); lowered != "" {
		var inSchema []candidate
		for _, c := range cands {
			if c.origin.schema == lowered {
				inSchema = append(inSchema, c)
			}
		}
		if len(inSchema) == 0 {
			return nil, &vgirpc.RpcError{
				Type: "ValueError",
				Message: fmt.Sprintf("Function '%s' is not registered in schema '%s'. It is available in: %v",
					name, schema, schemasOf(cands)),
			}
		}
		cands = inSchema
	}

	return cands, nil
}

// crossSchemaAmbiguity reports the error for an unqualified call whose surviving
// candidates span several schemas — the caller named no schema and there is no
// non-arbitrary winner. Returns nil when the call is unambiguous.
func crossSchemaAmbiguity(cands []candidate, name, schema string) error {
	if schema != "" || len(cands) < 2 {
		return nil
	}
	schemas := schemasOf(cands)
	if len(schemas) < 2 {
		return nil
	}
	return &vgirpc.RpcError{
		Type: "ValueError",
		Message: fmt.Sprintf("Ambiguous function call '%s': declared in more than one schema (%v) — "+
			"qualify the call with a schema to disambiguate", name, schemas),
	}
}

// resolveAggregate resolves an aggregate RPC to a single registered
// implementation.
//
// Every aggregate RPC (bind / update / combine / finalize / destructor, the four
// window calls, the three streaming calls) re-resolves the function by name:
// they are stateless unary requests with no bound connection, so the request is
// the only carrier of identity. Before protocol 1.2.0 none of them carried a
// schema, so an aggregate name declared in two schemas ran whichever the by-name
// lookup found first — bind could resolve correctly and update/finalize then
// return the other schema's answer.
//
// Aggregates do not disambiguate by argument signature (overloads of one
// aggregate share state semantics, so any of them drives dispatch); the home is
// the whole of the resolution.
func (w *Worker) resolveAggregate(name, schema, catalog string) (AggregateFunction, error) {
	var cands []candidate
	cands = collect(w, w.aggregates, kindAggregate, name, cands)
	if len(cands) == 0 {
		return nil, &vgirpc.RpcError{
			Type:    "ValueError",
			Message: fmt.Sprintf("aggregate function %q is not registered", name),
		}
	}
	cands, err := w.scopeToHome(cands, name, schema, catalog)
	if err != nil {
		return nil, err
	}
	if err := crossSchemaAmbiguity(cands, name, schema); err != nil {
		return nil, err
	}
	fn, ok := cands[0].fn.(AggregateFunction)
	if !ok {
		return nil, &vgirpc.RpcError{
			Type:    "ValueError",
			Message: fmt.Sprintf("function %q is not an AggregateFunction", name),
		}
	}
	return fn, nil
}

// resolveFunction resolves a call to a single registered implementation.
//
// Scoping by home (see scopeToHome) runs first, narrowing "every registration of
// this name" to "the registrations the caller could possibly mean". Only then do
// argument signatures pick between same-schema overloads.
func (w *Worker) resolveFunction(lk functionLookup) (interface{}, error) {
	cands := w.candidatesFor(lk.Name, lk.Type)
	if len(cands) == 0 {
		return nil, &vgirpc.RpcError{
			Type:    "ValueError",
			Message: fmt.Sprintf("Unknown function: '%s'", lk.Name),
		}
	}

	cands, err := w.scopeToHome(cands, lk.Name, lk.Schema, lk.Catalog)
	if err != nil {
		return nil, err
	}

	if len(cands) == 1 {
		return cands[0].fn, nil
	}

	if err := crossSchemaAmbiguity(cands, lk.Name, lk.Schema); err != nil {
		return nil, err
	}

	impls := make([]interface{}, 0, len(cands))
	for _, c := range cands {
		impls = append(impls, c.fn)
	}
	fn, err := resolveOverload(impls, lk.Args, lk.InputSchema)
	if err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, &vgirpc.RpcError{
			Type:    "ValueError",
			Message: fmt.Sprintf("No matching overload for function '%s'", lk.Name),
		}
	}
	return fn, nil
}
