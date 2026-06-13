// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package versioned_tables

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var (
	AnimalsSchemaV1 = arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "legs", Type: arrow.PrimitiveTypes.Int64},
		{Name: "sound", Type: arrow.BinaryTypes.String},
	}, nil)

	AnimalsSchemaV11 = arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "legs", Type: arrow.PrimitiveTypes.Int64},
		{Name: "sound", Type: arrow.BinaryTypes.String},
		{Name: "color", Type: arrow.BinaryTypes.String},
	}, nil)

	PlantsSchema = arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "kind", Type: arrow.BinaryTypes.String},
		{Name: "height_m", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	animalNames  = []string{"chicken", "cow", "horse", "pig", "sheep"}
	animalLegs   = []int64{2, 4, 4, 4, 4}
	animalSounds = []string{"cluck", "moo", "neigh", "oink", "baa"}
	animalColors = []string{"red", "brown", "black", "pink", "white"}

	plantNames    = []string{"oak", "pine", "rose", "tomato", "wheat"}
	plantKinds    = []string{"tree", "tree", "flower", "vegetable", "grass"}
	plantHeightsM = []float64{20.0, 25.0, 0.6, 1.5, 1.0}
)

type emitOnceState struct {
	Done bool
}

// ----- animals (1.0.0) -----

type AnimalsScanFunction struct{}

var _ vgi.TypedTableFunc[emitOnceState] = (*AnimalsScanFunction)(nil)

func (f *AnimalsScanFunction) Name() string { return "versioned_tables_animals_scan" }
func (f *AnimalsScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "Animals table for data_version 1.0.0", Stability: vgi.StabilityConsistent}
}
func (f *AnimalsScanFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }
func (f *AnimalsScanFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(AnimalsSchemaV1)
}
func (f *AnimalsScanFunction) NewState(params *vgi.ProcessParams) (*emitOnceState, error) {
	return &emitOnceState{}, nil
}
func (f *AnimalsScanFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *emitOnceState, out *vgirpc.OutputCollector) error {
	if state.Done {
		return out.Finish()
	}
	state.Done = true
	mem := memory.NewGoAllocator()
	cols := []arrow.Array{
		buildStringArr(mem, animalNames),
		buildInt64Arr(mem, animalLegs),
		buildStringArr(mem, animalSounds),
	}
	defer releaseAll(cols)
	return out.Emit(array.NewRecordBatch(params.OutputSchema, cols, int64(len(animalNames))))
}

func NewAnimalsScanFunction() vgi.TableFunction {
	return vgi.AsTableFunction[emitOnceState](&AnimalsScanFunction{})
}

// ----- animals with color (1.1.0) -----

type AnimalsColorScanFunction struct{}

var _ vgi.TypedTableFunc[emitOnceState] = (*AnimalsColorScanFunction)(nil)

func (f *AnimalsColorScanFunction) Name() string { return "versioned_tables_animals_color_scan" }
func (f *AnimalsColorScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "Animals table for data_version 1.1.0 (with color)", Stability: vgi.StabilityConsistent}
}
func (f *AnimalsColorScanFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }
func (f *AnimalsColorScanFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(AnimalsSchemaV11)
}
func (f *AnimalsColorScanFunction) NewState(params *vgi.ProcessParams) (*emitOnceState, error) {
	return &emitOnceState{}, nil
}
func (f *AnimalsColorScanFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *emitOnceState, out *vgirpc.OutputCollector) error {
	if state.Done {
		return out.Finish()
	}
	state.Done = true
	mem := memory.NewGoAllocator()
	cols := []arrow.Array{
		buildStringArr(mem, animalNames),
		buildInt64Arr(mem, animalLegs),
		buildStringArr(mem, animalSounds),
		buildStringArr(mem, animalColors),
	}
	defer releaseAll(cols)
	return out.Emit(array.NewRecordBatch(params.OutputSchema, cols, int64(len(animalNames))))
}

func NewAnimalsColorScanFunction() vgi.TableFunction {
	return vgi.AsTableFunction[emitOnceState](&AnimalsColorScanFunction{})
}

// ----- plants (2.0.0, 3.0.0) -----

type PlantsScanFunction struct{}

var _ vgi.TypedTableFunc[emitOnceState] = (*PlantsScanFunction)(nil)

func (f *PlantsScanFunction) Name() string { return "versioned_tables_plants_scan" }
func (f *PlantsScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "Plants table for data_version 2.0.0 and 3.0.0", Stability: vgi.StabilityConsistent}
}
func (f *PlantsScanFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }
func (f *PlantsScanFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(PlantsSchema)
}
func (f *PlantsScanFunction) NewState(params *vgi.ProcessParams) (*emitOnceState, error) {
	return &emitOnceState{}, nil
}
func (f *PlantsScanFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *emitOnceState, out *vgirpc.OutputCollector) error {
	if state.Done {
		return out.Finish()
	}
	state.Done = true
	mem := memory.NewGoAllocator()
	cols := []arrow.Array{
		buildStringArr(mem, plantNames),
		buildStringArr(mem, plantKinds),
		buildFloat64Arr(mem, plantHeightsM),
	}
	defer releaseAll(cols)
	return out.Emit(array.NewRecordBatch(params.OutputSchema, cols, int64(len(plantNames))))
}

func NewPlantsScanFunction() vgi.TableFunction {
	return vgi.AsTableFunction[emitOnceState](&PlantsScanFunction{})
}

// Helpers.

func buildStringArr(mem memory.Allocator, vals []string) arrow.Array {
	b := array.NewStringBuilder(mem)
	defer b.Release()
	for _, v := range vals {
		b.Append(v)
	}
	return b.NewArray()
}

func buildInt64Arr(mem memory.Allocator, vals []int64) arrow.Array {
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for _, v := range vals {
		b.Append(v)
	}
	return b.NewArray()
}

func buildFloat64Arr(mem memory.Allocator, vals []float64) arrow.Array {
	b := array.NewFloat64Builder(mem)
	defer b.Release()
	for _, v := range vals {
		b.Append(v)
	}
	return b.NewArray()
}

func releaseAll(arrs []arrow.Array) {
	for _, a := range arrs {
		a.Release()
	}
}
