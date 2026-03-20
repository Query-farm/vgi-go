// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const currentVersion = 3

// versionedSchemas maps version numbers to their Arrow schemas.
var versionedSchemas = map[int64]*arrow.Schema{
	1: arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil),
	2: arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64},
		{Name: "active", Type: &arrow.BooleanType{}},
	}, nil),
	3: arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64},
	}, nil),
}

// VersionedSchema returns the Arrow schema for a given version number.
func VersionedSchema(version int64) *arrow.Schema {
	return versionedSchemas[version]
}

// ResolveVersion converts AT clause parameters to a version number.
// Returns currentVersion when atUnit is nil or empty.
func ResolveVersion(atUnit, atValue *string) (int64, error) {
	if atUnit == nil || *atUnit == "" {
		return currentVersion, nil
	}

	switch strings.ToUpper(*atUnit) {
	case "VERSION":
		v, err := strconv.ParseInt(*atValue, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("Unknown version: %s", *atValue)
		}
		if _, ok := versionedSchemas[v]; !ok {
			return 0, fmt.Errorf("Unknown version: %d", v)
		}
		return v, nil

	case "TIMESTAMP":
		// Parse year from timestamp string (e.g. "2020-06-15 00:00:00")
		val := *atValue
		if len(val) < 4 {
			return 0, fmt.Errorf("invalid timestamp: %s", val)
		}
		year, err := strconv.Atoi(val[:4])
		if err != nil {
			return 0, fmt.Errorf("invalid timestamp: %s", val)
		}
		if year < 2020 {
			return 0, fmt.Errorf("table did not exist before 2020")
		}
		if year <= 2020 {
			return 1, nil
		}
		if year <= 2021 {
			return 2, nil
		}
		return 3, nil

	default:
		return 0, fmt.Errorf("Unsupported at_unit: %s", *atUnit)
	}
}

// VersionedDataFunction returns version-specific data demonstrating
// time travel with schema evolution.
type VersionedDataFunction struct{}

var _ vgi.TypedTableFunc[versionedDataState] = (*VersionedDataFunction)(nil)

func (f *VersionedDataFunction) Name() string { return "versioned_data_scan" }

func (f *VersionedDataFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Returns versioned data with schema evolution",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"generator", "testing"},
	}
}

func (f *VersionedDataFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "version", Position: 0, ArrowType: "int64", Doc: "Data version to return", IsConst: true,
			HasDefault: true, DefaultValue: fmt.Sprintf("%d", currentVersion)},
	}
}

func (f *VersionedDataFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	version := int64(currentVersion)
	if len(params.Args.Positional) > 0 {
		v, err := params.Args.GetScalarInt64(0)
		if err != nil {
			return nil, err
		}
		version = v
	}
	schema, ok := versionedSchemas[version]
	if !ok {
		return nil, fmt.Errorf("Unknown version: %d", version)
	}
	return vgi.BindSchema(schema)
}

type versionedDataState struct {
	Done    bool
	Version int64
}

func (f *VersionedDataFunction) NewState(params *vgi.ProcessParams) (*versionedDataState, error) {
	version := int64(currentVersion)
	if len(params.Args.Positional) > 0 {
		v, err := params.Args.GetScalarInt64(0)
		if err != nil {
			return nil, err
		}
		version = v
	}
	return &versionedDataState{Version: version}, nil
}

func (f *VersionedDataFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *versionedDataState, out *vgirpc.OutputCollector) error {
	if state.Done {
		out.Finish()
		return nil
	}
	state.Done = true

	mem := memory.NewGoAllocator()

	switch state.Version {
	case 1:
		ids := vgi.BuildInt64Array(3, func(i int64) int64 { return i + 1 })
		batch := array.NewRecordBatch(versionedSchemas[1], []arrow.Array{ids}, 3)
		out.Emit(batch)

	case 2:
		ids := vgi.BuildInt64Array(5, func(i int64) int64 { return i + 1 })
		names := vgi.BuildStringArray(5, func(i int64) string {
			return []string{"alice", "bob", "carol", "dave", "eve"}[i]
		})
		scores := vgi.BuildFloat64Array(5, func(i int64) float64 {
			return []float64{10.0, 20.0, 30.0, 40.0, 50.0}[i]
		})
		activeB := array.NewBooleanBuilder(mem)
		for _, v := range []bool{true, false, true, false, true} {
			activeB.Append(v)
		}
		active := activeB.NewArray()
		activeB.Release()

		batch := array.NewRecordBatch(versionedSchemas[2], []arrow.Array{ids, names, scores, active}, 5)
		out.Emit(batch)

	case 3:
		ids := vgi.BuildInt64Array(4, func(i int64) int64 { return i + 1 })
		scores := vgi.BuildFloat64Array(4, func(i int64) float64 {
			return []float64{15.0, 25.0, 35.0, 45.0}[i]
		})
		batch := array.NewRecordBatch(versionedSchemas[3], []arrow.Array{ids, scores}, 4)
		out.Emit(batch)
	}

	return nil
}

func NewVersionedDataFunction() vgi.TableFunction {
	return vgi.AsTableFunction[versionedDataState](&VersionedDataFunction{})
}
