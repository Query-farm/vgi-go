// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Blended ("UNNEST-style") table-in-out fixtures. A blended function's
// POSITIONAL args ARE its per-row input columns (real typed args, no synthetic
// TABLE placeholder), so ONE registration serves every call shape:
//
//	f(52, 13)                       -- literal   -> one input row
//	FROM t, f(t.x, t.y)             -- columns   -> streaming input
//	SELECT ... FROM t, LATERAL f(t.x, t.y)
//
// The blended signal is FunctionMetadata.InputFromArgs (surfaced through the
// catalog as FunctionInfo.input_from_args). Positional args are read from the
// input batch (by declared name for fixed args, positionally for varargs);
// named args stay bind-time scalars on ProcessParams.Args. Blended functions
// are map-shaped with NO finalize (DuckDB forbids FinalExecute under
// correlated LATERAL, one of the call shapes blended must serve). Mirrors
// vgi-python's RowTransformFunction fixtures.
package table_in_out

import (
	"context"
	"math"
	"strconv"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// pyFloat formats v the way Python's str(float) does for the values these
// fixtures emit: shortest decimal form, with a trailing ".0" for
// integer-valued floats (52.0 -> "52.0", 52.5 -> "52.5").
func pyFloat(v float64) string {
	s := strconv.FormatFloat(v, 'f', -1, 64)
	if !strings.ContainsAny(s, ".eE") {
		s += ".0"
	}
	return s
}

// roundTo rounds v to p decimal places.
func roundTo(v float64, p int64) float64 {
	scale := math.Pow(10, float64(p))
	return math.Round(v*scale) / scale
}

// geoEncodeBatch renders one geohash string per input row from the named
// coordinate columns, rounding each coordinate to precision decimals. A NULL
// in any coordinate yields a NULL geohash for that row.
func geoEncodeBatch(batch arrow.RecordBatch, colNames []string, precision int64) (arrow.RecordBatch, error) {
	cols := make([]arrow.Array, len(colNames))
	for i, name := range colNames {
		col := vgi.FindColumn(batch, name)
		if col == nil {
			// The literal single-row form may deliver positionally-named columns;
			// fall back to position.
			col = batch.Column(i)
		}
		cols[i] = col
	}
	n := int(batch.NumRows())
	mem := memory.NewGoAllocator()
	b := array.NewStringBuilder(mem)
	defer b.Release()
	for row := 0; row < n; row++ {
		null := false
		parts := make([]string, len(cols))
		for ci, col := range cols {
			if col.IsNull(row) {
				null = true
				break
			}
			parts[ci] = pyFloat(roundTo(vgi.GetFloat64Value(col, row), precision))
		}
		if null {
			b.AppendNull()
		} else {
			b.Append(strings.Join(parts, ":"))
		}
	}
	col := b.NewArray()
	defer col.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "geohash", Type: arrow.BinaryTypes.String}}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{col}, int64(n)), nil
}

// ---------------------------------------------------------------------------
// geo_encode(latitude, longitude [, precision := n]) — simple blended fixture
// ---------------------------------------------------------------------------

// GeoEncodeFunction is the blended geo encoder — one registration serves the
// literal, column, and LATERAL call shapes. latitude/longitude are POSITIONAL
// args = the per-row input columns (read from the batch by declared name — the
// C++ bind builds the input schema from the declared arg names and casts each
// input to its declared type). precision is a NAMED arg, surfaced on
// ProcessParams.Args (positional args are NOT). Emits one geohash string per
// input row: "<lat>:<lon>" rounded to precision decimals — deterministic so
// tests assert exact values.
type GeoEncodeFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*GeoEncodeFunction)(nil)

func (f *GeoEncodeFunction) Name() string { return "geo_encode" }

func (f *GeoEncodeFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:   "Blended per-row geo encoder (lat, lon -> geohash)",
		Stability:     vgi.StabilityConsistent,
		Categories:    []string{"geo", "blended"},
		InputFromArgs: true,
	}
}

func (f *GeoEncodeFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "latitude", Position: 0, ArrowType: "double", Doc: "Latitude input column"},
		{Name: "longitude", Position: 1, ArrowType: "double", Doc: "Longitude input column"},
		{Name: "precision", Position: -1, ArrowType: "int64", Doc: "Rounding precision",
			HasDefault: true, DefaultValue: "4"},
	}
}

func (f *GeoEncodeFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "geohash", Type: arrow.BinaryTypes.String},
		}, nil),
	}, nil
}

func (f *GeoEncodeFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

func (f *GeoEncodeFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	precision := vgi.OptionalInt64(params.Args, "precision", 4)
	result, err := geoEncodeBatch(batch, []string{"latitude", "longitude"}, precision)
	if err != nil {
		return err
	}
	return out.Emit(result)
}

func (f *GeoEncodeFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewGeoEncodeFunction creates a GeoEncodeFunction wrapped for registration.
func NewGeoEncodeFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&GeoEncodeFunction{})
}

// ---------------------------------------------------------------------------
// geo_encode(latitude, longitude, altitude [, precision := n]) — arity overload
// ---------------------------------------------------------------------------

// GeoEncode3Function is the arity-overloaded blended geo encoder — same SQL
// name as GeoEncodeFunction ("geo_encode") but 3 positional input columns
// (lat, lon, alt). Proves same-name blended overloads resolve by input-column
// arity: blended functions use REAL value types (no TABLE-typed arg), so
// DuckDB permits multiple overloads. geo_encode(52,13) resolves to the 2-arg
// overload, geo_encode(52,13,100) to this 3-arg one, in both literal and
// column shapes.
type GeoEncode3Function struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*GeoEncode3Function)(nil)

func (f *GeoEncode3Function) Name() string { return "geo_encode" }

func (f *GeoEncode3Function) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:   "Blended per-row geo encoder (lat, lon, alt -> geohash)",
		Stability:     vgi.StabilityConsistent,
		Categories:    []string{"geo", "blended"},
		InputFromArgs: true,
	}
}

func (f *GeoEncode3Function) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "latitude", Position: 0, ArrowType: "double", Doc: "Latitude input column"},
		{Name: "longitude", Position: 1, ArrowType: "double", Doc: "Longitude input column"},
		{Name: "altitude", Position: 2, ArrowType: "double", Doc: "Altitude input column"},
		{Name: "precision", Position: -1, ArrowType: "int64", Doc: "Rounding precision",
			HasDefault: true, DefaultValue: "4"},
	}
}

func (f *GeoEncode3Function) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "geohash", Type: arrow.BinaryTypes.String},
		}, nil),
	}, nil
}

func (f *GeoEncode3Function) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

func (f *GeoEncode3Function) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	precision := vgi.OptionalInt64(params.Args, "precision", 4)
	result, err := geoEncodeBatch(batch, []string{"latitude", "longitude", "altitude"}, precision)
	if err != nil {
		return err
	}
	return out.Emit(result)
}

func (f *GeoEncode3Function) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewGeoEncode3Function creates a GeoEncode3Function wrapped for registration.
func NewGeoEncode3Function() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&GeoEncode3Function{})
}

// ---------------------------------------------------------------------------
// row_sum(v1, v2, ... [, absolute := b]) — VARARGS blended fixture
// ---------------------------------------------------------------------------

// RowSumFunction is the blended VARARGS row-wise sum — proves the varargs
// input path. values is a varargs positional arg: the per-row input is N
// columns of the declared type. A varargs blended function has no per-column
// declared names, so the worker reads the columns POSITIONALLY.
// row_sum(1,2,3) -> 6; FROM t, row_sum(t.a,t.b,t.c) sums each row's columns.
// The absolute named option is surfaced on ProcessParams.Args.
type RowSumFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*RowSumFunction)(nil)

func (f *RowSumFunction) Name() string { return "row_sum" }

func (f *RowSumFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:   "Blended per-row varargs sum",
		Stability:     vgi.StabilityConsistent,
		Categories:    []string{"numeric", "blended"},
		InputFromArgs: true,
	}
}

func (f *RowSumFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "values", Position: 0, ArrowType: "double", Doc: "Numeric input columns", IsVarargs: true},
		{Name: "absolute", Position: -1, ArrowType: "boolean", Doc: "Sum absolute values",
			HasDefault: true, DefaultValue: "false"},
	}
}

func (f *RowSumFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "row_sum", Type: arrow.PrimitiveTypes.Float64},
		}, nil),
	}, nil
}

func (f *RowSumFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

func (f *RowSumFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	absolute := vgi.OptionalBool(params.Args, "absolute", false)
	n := int(batch.NumRows())
	numCols := int(batch.NumCols())
	mem := memory.NewGoAllocator()
	b := array.NewFloat64Builder(mem)
	defer b.Release()
	for row := 0; row < n; row++ {
		var sum float64
		null := false
		for ci := 0; ci < numCols; ci++ {
			col := batch.Column(ci)
			if col.IsNull(row) {
				null = true
				break
			}
			v := vgi.GetFloat64Value(col, row)
			if absolute {
				v = math.Abs(v)
			}
			sum += v
		}
		if null {
			b.AppendNull()
		} else {
			b.Append(sum)
		}
	}
	col := b.NewArray()
	defer col.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "row_sum", Type: arrow.PrimitiveTypes.Float64}}, nil)
	result := array.NewRecordBatch(schema, []arrow.Array{col}, int64(n))
	return out.Emit(result)
}

func (f *RowSumFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewRowSumFunction creates a RowSumFunction wrapped for registration.
func NewRowSumFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&RowSumFunction{})
}

// ---------------------------------------------------------------------------
// blended_drop(x) — 1->0 edge-case fixture
// ---------------------------------------------------------------------------

// BlendedDropFunction is a blended 1->0 map: it emits a single 0-row output
// batch for its input row. Exercises the literal scan-mode drain loop's
// "empty-but-not-EOS -> keep reading, finish only at true EOS" branch: the
// worker's whole output for the one synthesized input row is a 0-row batch, so
// PhysicalTableScan must reach FINISHED cleanly and NOT infinite-loop
// re-feeding the input.
type BlendedDropFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*BlendedDropFunction)(nil)

func (f *BlendedDropFunction) Name() string { return "blended_drop" }

func (f *BlendedDropFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:   "Blended 1->0 map emitting a single 0-row batch (literal scan-mode)",
		Stability:     vgi.StabilityConsistent,
		Categories:    []string{"blended", "test"},
		InputFromArgs: true,
	}
}

func (f *BlendedDropFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "x", Position: 0, ArrowType: "double", Doc: "Input column (ignored)"},
	}
}

func (f *BlendedDropFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "v", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}, nil
}

func (f *BlendedDropFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

func (f *BlendedDropFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	empty := vgi.EmptyBatch(params.OutputSchema)
	return out.Emit(empty)
}

func (f *BlendedDropFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewBlendedDropFunction creates a BlendedDropFunction wrapped for registration.
func NewBlendedDropFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&BlendedDropFunction{})
}
