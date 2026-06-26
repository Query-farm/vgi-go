// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ---------------------------------------------------------------------------
// union_varargs(union...) — echoes the active member tag + value of each
// union-typed vararg. DuckDB serializes UNION as a SPARSE Arrow union, so the
// vararg arg type is declared as a sparse union over [i:int64, s:varchar]; this
// makes DuckDB bind the varargs as UNION(i BIGINT, s VARCHAR) and lets the
// worker recover the active member discriminator (which a plain scalar decode
// would drop). Mirrors vgi-python's UnionVarargsFunction.
//
// Output schema is fixed: idx BIGINT, tag VARCHAR, value VARCHAR. One row per
// vararg in positional order; tag is the active member's field name and value
// is the member value stringified.
// ---------------------------------------------------------------------------

// unionVarargsArgType is the sparse union shared by every union_varargs
// argument. DuckDB only ever emits sparse unions (+us:) over Arrow, so this
// round-trips end-to-end.
var unionVarargsArgType = arrow.SparseUnionOf(
	[]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: arrow.BinaryTypes.String},
	},
	[]arrow.UnionTypeCode{0, 1},
)

var unionVarargsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "idx", Type: arrow.PrimitiveTypes.Int64},
	{Name: "tag", Type: arrow.BinaryTypes.String},
	{Name: "value", Type: arrow.BinaryTypes.String},
}, nil)

// UnionVarargsFunction echoes the active member tag and value of each union vararg.
type UnionVarargsFunction struct{}

var _ vgi.TypedTableFunc[unionVarargsState] = (*UnionVarargsFunction)(nil)

func (f *UnionVarargsFunction) Name() string { return "union_varargs" }

func (f *UnionVarargsFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Echo the active member tag and value of each union vararg",
		Stability:   vgi.StabilityConsistent,
	}
}

// unionVarargsArgs is the typed argument schema. The single varargs field is
// declared as []any so BindArgs leaves it nil — the function reads the raw
// union arrow.Array values from params.Args.Positional directly. The argument
// type is overridden to the sparse union via ArgumentSpecs below so DuckDB
// binds UNION(i BIGINT, s VARCHAR).
type unionVarargsArgs struct {
	Configs []any `vgi:"pos=0,varargs,doc=Union values whose active member tag is echoed back"`
}

func (f *UnionVarargsFunction) ArgumentSpecs() []vgi.ArgSpec {
	specs := vgi.DeriveArgSpecs(unionVarargsArgs{})
	// Override the varargs element type from "any" to the concrete sparse
	// union so DuckDB renders the parameter type as UNION(i BIGINT, s VARCHAR)
	// and the discriminator survives the round-trip.
	specs[0].ArrowType = "union"
	specs[0].ArrowDataType = unionVarargsArgType
	return specs
}

func (f *UnionVarargsFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(unionVarargsSchema)
}

type unionVarargsState struct {
	vgi.BatchState
	Idx    []int64
	Tags   []string
	Values []string
}

func (f *UnionVarargsFunction) NewState(params *vgi.ProcessParams) (*unionVarargsState, error) {
	positional := params.Args.Positional
	n := len(positional)

	idx := make([]int64, n)
	tags := make([]string, n)
	values := make([]string, n)

	for i, col := range positional {
		tag, value, err := decodeUnionScalar(col)
		if err != nil {
			return nil, fmt.Errorf("union_varargs: argument %d: %w", i, err)
		}
		idx[i] = int64(i)
		tags[i] = tag
		values[i] = value
	}

	return &unionVarargsState{
		// One batch of n rows; GenerateBatch finishes after emitting it.
		BatchState: vgi.NewBatchState(int64(n), int64(n)+1),
		Idx:        idx,
		Tags:       tags,
		Values:     values,
	}, nil
}

func (f *UnionVarargsFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *unionVarargsState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		idxArr := vgi.BuildInt64Array(size, func(i int64) int64 { return state.Idx[i] })
		tagArr := vgi.BuildStringArray(size, func(i int64) string { return state.Tags[i] })
		valArr := vgi.BuildStringArray(size, func(i int64) string { return state.Values[i] })
		return []arrow.Array{idxArr, tagArr, valArr}, nil
	})
}

func NewUnionVarargsFunction() vgi.TableFunction {
	return vgi.AsTableFunction[unionVarargsState](&UnionVarargsFunction{})
}

// decodeUnionScalar extracts the active member's field name (tag) and its
// stringified value from a single-row union array. DuckDB sends sparse unions,
// so the child value lives at the same row index (0) as the discriminator.
func decodeUnionScalar(col arrow.Array) (string, string, error) {
	u, ok := col.(array.Union)
	if !ok {
		return "", "", fmt.Errorf("expected union array, got %T", col)
	}
	if u.Len() == 0 {
		return "", "", fmt.Errorf("empty union array")
	}
	childID := u.ChildID(0)
	tag := u.UnionType().Fields()[childID].Name
	child := u.Field(childID)
	// Sparse union: the value index equals the parent index (0).
	value := stringifyArrayValue(child, 0)
	return tag, value, nil
}

// stringifyArrayValue renders the value at index i of arr as a string. Covers
// the member types union_varargs exercises (int64, varchar); falls back to the
// array's generic value-string for anything else.
func stringifyArrayValue(arr arrow.Array, i int) string {
	if arr.IsNull(i) {
		return ""
	}
	switch c := arr.(type) {
	case *array.Int64:
		return fmt.Sprintf("%d", c.Value(i))
	case *array.String:
		return c.Value(i)
	default:
		return c.ValueStr(i)
	}
}
