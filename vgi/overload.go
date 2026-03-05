// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"fmt"
	"log/slog"

	"github.com/Query-farm/vgi-rpc/vgirpc"
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

	slog.Debug("overload: resolved",
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
	}
	return nil
}

// resolveFunctionWithOverload looks up a function by name and type, then uses
// overload resolution when multiple candidates exist.
func (w *Worker) resolveFunctionWithOverload(name string, ft FunctionType, args *Arguments, inputSchema *arrow.Schema) (interface{}, error) {
	ft = normalizeFunctionType(ft)

	var candidates []interface{}

	switch ft {
	case FunctionTypeScalar:
		if fns, ok := w.scalars[name]; ok {
			for _, fn := range fns {
				candidates = append(candidates, fn)
			}
		}
	case FunctionTypeTable:
		if fns, ok := w.tables[name]; ok {
			for _, fn := range fns {
				candidates = append(candidates, fn)
			}
		}
		if len(candidates) == 0 {
			if fns, ok := w.tableInOuts[name]; ok {
				for _, fn := range fns {
					candidates = append(candidates, fn)
				}
			}
		}
	case FunctionTypeAggregate:
		if fns, ok := w.tableInOuts[name]; ok {
			for _, fn := range fns {
				candidates = append(candidates, fn)
			}
		}
	}

	// Fallback: try all registries
	if len(candidates) == 0 {
		if fns, ok := w.scalars[name]; ok {
			for _, fn := range fns {
				candidates = append(candidates, fn)
			}
		}
	}
	if len(candidates) == 0 {
		if fns, ok := w.tables[name]; ok {
			for _, fn := range fns {
				candidates = append(candidates, fn)
			}
		}
	}
	if len(candidates) == 0 {
		if fns, ok := w.tableInOuts[name]; ok {
			for _, fn := range fns {
				candidates = append(candidates, fn)
			}
		}
	}

	if len(candidates) == 0 {
		return nil, &vgirpc.RpcError{
			Type:    "ValueError",
			Message: fmt.Sprintf("Unknown function: '%s'", name),
		}
	}

	if len(candidates) == 1 {
		return candidates[0], nil
	}

	fn, err := resolveOverload(candidates, args, inputSchema)
	if err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, &vgirpc.RpcError{
			Type:    "ValueError",
			Message: fmt.Sprintf("No matching overload for function '%s'", name),
		}
	}
	return fn, nil
}
