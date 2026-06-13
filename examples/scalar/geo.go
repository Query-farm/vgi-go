// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"context"
	"math"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

var pointStructType = arrow.StructOf(
	arrow.Field{Name: "lat", Type: arrow.PrimitiveTypes.Float64},
	arrow.Field{Name: "lon", Type: arrow.PrimitiveTypes.Float64},
)

var centroidOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "result", Type: pointStructType},
}, nil)

func euclideanDistance(lat1, lon1, lat2, lon2 float64) float64 {
	dlat := lat2 - lat1
	dlon := lon2 - lon1
	return math.Sqrt(dlat*dlat + dlon*dlon)
}

// buildCentroidResult computes the centroid of N point columns using a
// StructBuilder to ensure the output struct type matches the output schema
// exactly (including nullability). getLatLon extracts (lat, lon) from column c
// at row i.
func buildCentroidResult(params *vgi.ProcessParams, batch arrow.RecordBatch, getLatLon func(c, i int) (float64, float64)) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	n := int(batch.NumRows())
	numCols := int(batch.NumCols())

	sb := array.NewStructBuilder(mem, pointStructType)
	defer sb.Release()
	latBuilder := sb.FieldBuilder(0).(*array.Float64Builder)
	lonBuilder := sb.FieldBuilder(1).(*array.Float64Builder)

	for i := 0; i < n; i++ {
		anyNull := false
		for c := 0; c < numCols; c++ {
			if batch.Column(c).IsNull(i) {
				anyNull = true
				break
			}
		}
		if anyNull {
			sb.AppendNull()
			continue
		}
		sb.Append(true)
		var sumLat, sumLon float64
		for c := 0; c < numCols; c++ {
			lat, lon := getLatLon(c, i)
			sumLat += lat
			sumLon += lon
		}
		latBuilder.Append(sumLat / float64(numCols))
		lonBuilder.Append(sumLon / float64(numCols))
	}

	structArr := sb.NewStructArray()
	defer structArr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{structArr}, int64(n)), nil
}

// ---------------------------------------------------------------------------
// geo_distance_struct
// ---------------------------------------------------------------------------

type GeoDistanceStructFunction struct{}

func (f *GeoDistanceStructFunction) Name() string { return "geo_distance_struct" }

func (f *GeoDistanceStructFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Euclidean distance between two struct points",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.PrimitiveTypes.Float64,
	}
}

func (f *GeoDistanceStructFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "p1", Position: 0, ArrowType: "struct", ArrowDataType: pointStructType, Doc: "First point"},
		{Name: "p2", Position: 1, ArrowType: "struct", ArrowDataType: pointStructType, Doc: "Second point"},
	}
}

func (f *GeoDistanceStructFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.PrimitiveTypes.Float64)
}

func (f *GeoDistanceStructFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapColumns(params, batch, []int{0, 1}, array.NewFloat64Builder,
		func(cols []arrow.Array, i int) float64 {
			p1 := cols[0].(*array.Struct)
			p2 := cols[1].(*array.Struct)
			lat1 := vgi.GetFloat64Value(p1.Field(0), i)
			lon1 := vgi.GetFloat64Value(p1.Field(1), i)
			lat2 := vgi.GetFloat64Value(p2.Field(0), i)
			lon2 := vgi.GetFloat64Value(p2.Field(1), i)
			return euclideanDistance(lat1, lon1, lat2, lon2)
		})
}

// ---------------------------------------------------------------------------
// geo_distance_list
// ---------------------------------------------------------------------------

type GeoDistanceListFunction struct{}

func (f *GeoDistanceListFunction) Name() string { return "geo_distance_list" }

func (f *GeoDistanceListFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Euclidean distance between two list points",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.PrimitiveTypes.Float64,
	}
}

func (f *GeoDistanceListFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "p1", Position: 0, ArrowType: "list", ArrowDataType: arrow.ListOf(arrow.PrimitiveTypes.Float64), Doc: "First point"},
		{Name: "p2", Position: 1, ArrowType: "list", ArrowDataType: arrow.ListOf(arrow.PrimitiveTypes.Float64), Doc: "Second point"},
	}
}

func (f *GeoDistanceListFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.PrimitiveTypes.Float64)
}

func (f *GeoDistanceListFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapColumns(params, batch, []int{0, 1}, array.NewFloat64Builder,
		func(cols []arrow.Array, i int) float64 {
			p1 := cols[0].(*array.List)
			p2 := cols[1].(*array.List)
			p1vals := p1.ListValues()
			p2vals := p2.ListValues()
			p1start, p1end := p1.ValueOffsets(i)
			p2start, p2end := p2.ValueOffsets(i)
			var lat1, lon1, lat2, lon2 float64
			if p1end-p1start >= 2 {
				lat1 = vgi.GetFloat64Value(p1vals, int(p1start))
				lon1 = vgi.GetFloat64Value(p1vals, int(p1start+1))
			}
			if p2end-p2start >= 2 {
				lat2 = vgi.GetFloat64Value(p2vals, int(p2start))
				lon2 = vgi.GetFloat64Value(p2vals, int(p2start+1))
			}
			return euclideanDistance(lat1, lon1, lat2, lon2)
		})
}

// ---------------------------------------------------------------------------
// geo_distance_fixed
// ---------------------------------------------------------------------------

type GeoDistanceFixedFunction struct{}

func (f *GeoDistanceFixedFunction) Name() string { return "geo_distance_fixed" }

func (f *GeoDistanceFixedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Euclidean distance between two fixed-size list points",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.PrimitiveTypes.Float64,
	}
}

func (f *GeoDistanceFixedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "p1", Position: 0, ArrowType: "fixed_list", ArrowDataType: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float64), Doc: "First point"},
		{Name: "p2", Position: 1, ArrowType: "fixed_list", ArrowDataType: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float64), Doc: "Second point"},
	}
}

func (f *GeoDistanceFixedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.PrimitiveTypes.Float64)
}

func (f *GeoDistanceFixedFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return vgi.MapColumns(params, batch, []int{0, 1}, array.NewFloat64Builder,
		func(cols []arrow.Array, i int) float64 {
			p1 := cols[0].(*array.FixedSizeList)
			p2 := cols[1].(*array.FixedSizeList)
			p1vals := p1.ListValues()
			p2vals := p2.ListValues()
			lat1 := vgi.GetFloat64Value(p1vals, i*2)
			lon1 := vgi.GetFloat64Value(p1vals, i*2+1)
			lat2 := vgi.GetFloat64Value(p2vals, i*2)
			lon2 := vgi.GetFloat64Value(p2vals, i*2+1)
			return euclideanDistance(lat1, lon1, lat2, lon2)
		})
}

// ---------------------------------------------------------------------------
// geo_centroid_struct
// ---------------------------------------------------------------------------

type GeoCentroidStructFunction struct{}

func (f *GeoCentroidStructFunction) Name() string { return "geo_centroid_struct" }

func (f *GeoCentroidStructFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Centroid of N struct points",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  pointStructType,
	}
}

func (f *GeoCentroidStructFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "points", Position: 0, ArrowType: "struct", ArrowDataType: pointStructType, Doc: "Point structs", IsVarargs: true},
	}
}

func (f *GeoCentroidStructFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: centroidOutputSchema}, nil
}

func (f *GeoCentroidStructFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return buildCentroidResult(params, batch, func(c int, i int) (float64, float64) {
		s := batch.Column(c).(*array.Struct)
		return vgi.GetFloat64Value(s.Field(0), i), vgi.GetFloat64Value(s.Field(1), i)
	})
}

// ---------------------------------------------------------------------------
// geo_centroid_list
// ---------------------------------------------------------------------------

type GeoCentroidListFunction struct{}

func (f *GeoCentroidListFunction) Name() string { return "geo_centroid_list" }

func (f *GeoCentroidListFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Centroid of N list points",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  pointStructType,
	}
}

func (f *GeoCentroidListFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "points", Position: 0, ArrowType: "list", ArrowDataType: arrow.ListOf(arrow.PrimitiveTypes.Float64), Doc: "Point lists", IsVarargs: true},
	}
}

func (f *GeoCentroidListFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: centroidOutputSchema}, nil
}

func (f *GeoCentroidListFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return buildCentroidResult(params, batch, func(c int, i int) (float64, float64) {
		list := batch.Column(c).(*array.List)
		vals := list.ListValues()
		start, _ := list.ValueOffsets(i)
		return vgi.GetFloat64Value(vals, int(start)), vgi.GetFloat64Value(vals, int(start+1))
	})
}

// ---------------------------------------------------------------------------
// geo_centroid_fixed
// ---------------------------------------------------------------------------

type GeoCentroidFixedFunction struct{}

func (f *GeoCentroidFixedFunction) Name() string { return "geo_centroid_fixed" }

func (f *GeoCentroidFixedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Centroid of N fixed-size list points",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  pointStructType,
	}
}

func (f *GeoCentroidFixedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "points", Position: 0, ArrowType: "fixed_list", ArrowDataType: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float64), Doc: "Point fixed lists", IsVarargs: true},
	}
}

func (f *GeoCentroidFixedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: centroidOutputSchema}, nil
}

func (f *GeoCentroidFixedFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return buildCentroidResult(params, batch, func(c int, i int) (float64, float64) {
		fsl := batch.Column(c).(*array.FixedSizeList)
		vals := fsl.ListValues()
		return vgi.GetFloat64Value(vals, i*2), vgi.GetFloat64Value(vals, i*2+1)
	})
}
