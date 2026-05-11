// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"encoding/binary"
	"math"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// makeWKBPoint encodes a 2D point as little-endian WKB:
//
//	byte 0:    byte order (1 = little-endian)
//	bytes 1-4: geometry type (1 = Point)
//	bytes 5-12, 13-20: x, y as float64 little-endian
func makeWKBPoint(x, y float64) []byte {
	buf := make([]byte, 21)
	buf[0] = 1
	binary.LittleEndian.PutUint32(buf[1:5], 1)
	binary.LittleEndian.PutUint64(buf[5:13], math.Float64bits(x))
	binary.LittleEndian.PutUint64(buf[13:21], math.Float64bits(y))
	return buf
}

// geometryField is an Arrow binary field with geoarrow.wkb extension metadata
// so DuckDB recognizes it as the GEOMETRY type.
var geometryField = arrow.Field{
	Name: "geom",
	Type: arrow.BinaryTypes.Binary,
	Metadata: arrow.NewMetadata(
		[]string{"ARROW:extension:name", "ARROW:extension:metadata"},
		[]string{"geoarrow.wkb", "{}"},
	),
}

var spatialFilterOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	{Name: "x", Type: arrow.PrimitiveTypes.Float64},
	{Name: "y", Type: arrow.PrimitiveTypes.Float64},
	geometryField,
}, nil)

// SpatialFilterExampleFunction generates points on a deterministic grid in
// [0,1)x[0,1) for testing expression filter pushdown with spatial predicates.
//
// Grid layout: For count=N with cols=ceil(sqrt(N)), point i has coordinates
// x=(i%cols)/cols, y=(i//cols)/cols. With count=100, cols=10 → x,y ∈
// {0.0, 0.1, ..., 0.9}, giving predictable bounding-box filter counts.
type SpatialFilterExampleFunction struct{}

var _ vgi.TypedTableFunc[spatialFilterState] = (*SpatialFilterExampleFunction)(nil)

func (f *SpatialFilterExampleFunction) Name() string { return "spatial_filter_example" }

func (f *SpatialFilterExampleFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:                "Generates points on a grid with WKB geometry for spatial filter testing",
		Stability:                  vgi.StabilityConsistent,
		ProjectionPushdown:         true,
		FilterPushdown:             true,
		AutoApplyFilters:           true,
		SupportedExpressionFilters: []string{"&&", "st_intersects_extent"},
		Categories:                 []string{"generator", "spatial", "testing"},
	}
}

// spatialFilterExampleArgs is the typed argument schema for spatial_filter_example().
type spatialFilterExampleArgs struct {
	Count     int64 `vgi:"pos=0,doc=Number of points to generate"`
	BatchSize int64 `vgi:"default=1024,doc=Rows per batch"`
}

func (f *SpatialFilterExampleFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(spatialFilterExampleArgs{})
}

func (f *SpatialFilterExampleFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(spatialFilterOutputSchema)
}

func (f *SpatialFilterExampleFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	var args spatialFilterExampleArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: args.Count, Max: args.Count}, nil
}

type spatialFilterState struct {
	vgi.BatchState
	Cols int64
}

func (f *SpatialFilterExampleFunction) NewState(params *vgi.ProcessParams) (*spatialFilterState, error) {
	var args spatialFilterExampleArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	cols := int64(math.Ceil(math.Sqrt(float64(args.Count))))
	if cols < 1 {
		cols = 1
	}
	return &spatialFilterState{
		BatchState: vgi.NewBatchState(args.Count, args.BatchSize),
		Cols:       cols,
	}, nil
}

func (f *SpatialFilterExampleFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *spatialFilterState, out *vgirpc.OutputCollector) error {
	projected := vgi.ProjectedColumns(params.ProjectionIDs, spatialFilterOutputSchema)
	cols := state.Cols
	return vgi.GenerateBatchMap(&state.BatchState, out, params.OutputSchema, func(size int64) (map[string]arrow.Array, error) {
		start := state.Index
		colMap := make(map[string]arrow.Array)
		if projected.Contains("n") {
			colMap["n"] = vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
		}
		if projected.Contains("x") {
			colMap["x"] = vgi.BuildFloat64Array(size, func(i int64) float64 {
				return float64((start+i)%cols) / float64(cols)
			})
		}
		if projected.Contains("y") {
			colMap["y"] = vgi.BuildFloat64Array(size, func(i int64) float64 {
				return float64((start+i)/cols) / float64(cols)
			})
		}
		if projected.Contains("geom") {
			colMap["geom"] = vgi.BuildBinaryArray(size, func(i int64) []byte {
				idx := start + i
				x := float64(idx%cols) / float64(cols)
				y := float64(idx/cols) / float64(cols)
				return makeWKBPoint(x, y)
			})
		}
		return colMap, nil
	})
}

func NewSpatialFilterExampleFunction() vgi.TableFunction {
	return vgi.AsTableFunction[spatialFilterState](&SpatialFilterExampleFunction{})
}
