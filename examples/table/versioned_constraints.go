// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const versionedConstraintsCurrentVersion = 3

var VersionedConstraintsSchemas = map[int64]*arrow.Schema{
	1: arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil),
	2: arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "email", Type: arrow.BinaryTypes.String},
	}, nil),
	3: arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "email", Type: arrow.BinaryTypes.String},
		{Name: "department_id", Type: arrow.PrimitiveTypes.Int64},
	}, nil),
}

// VersionedConstraintsSchema returns the Arrow schema for a given version.
func VersionedConstraintsSchema(version int64) *arrow.Schema {
	return VersionedConstraintsSchemas[version]
}

// ResolveVersionedConstraintsVersion converts AT clause parameters to a version number.
func ResolveVersionedConstraintsVersion(atUnit, atValue *string) (int64, error) {
	if atUnit == nil || *atUnit == "" {
		return versionedConstraintsCurrentVersion, nil
	}
	switch strings.ToUpper(*atUnit) {
	case "VERSION":
		v, err := strconv.ParseInt(*atValue, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("Unknown version: %s", *atValue)
		}
		if _, ok := VersionedConstraintsSchemas[v]; !ok {
			return 0, fmt.Errorf("Unknown version: %d", v)
		}
		return v, nil
	default:
		return 0, fmt.Errorf("Unsupported at_unit: %s", *atUnit)
	}
}

// VersionedConstraintsScanFunction returns version-specific data with constraint evolution.
type VersionedConstraintsScanFunction struct{}

var _ vgi.TypedTableFunc[versionedConstraintsState] = (*VersionedConstraintsScanFunction)(nil)

func (f *VersionedConstraintsScanFunction) Name() string { return "versioned_constraints_scan" }

func (f *VersionedConstraintsScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Returns versioned data with constraint evolution",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"generator", "testing"},
	}
}

// versionedConstraintsArgs is the typed argument schema for versioned_constraints_scan().
var versionedConstraintsDefault = fmt.Sprintf("%d", versionedConstraintsCurrentVersion)

type versionedConstraintsArgs struct {
	Version int64 `vgi:"pos=0,doc=Data version to return"`
}

func (f *VersionedConstraintsScanFunction) ArgumentSpecs() []vgi.ArgSpec {
	specs := vgi.DeriveArgSpecs(versionedConstraintsArgs{})
	specs[0].HasDefault = true
	specs[0].DefaultValue = versionedConstraintsDefault
	return specs
}

func (f *VersionedConstraintsScanFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	var args versionedConstraintsArgs
	args.Version = versionedConstraintsCurrentVersion
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	if args.Version == 0 {
		args.Version = versionedConstraintsCurrentVersion
	}
	schema, ok := VersionedConstraintsSchemas[args.Version]
	if !ok {
		return nil, fmt.Errorf("Unknown version: %d", args.Version)
	}
	return vgi.BindSchema(schema)
}

type versionedConstraintsState struct {
	Done    bool
	Version int64
}

func (f *VersionedConstraintsScanFunction) NewState(params *vgi.ProcessParams) (*versionedConstraintsState, error) {
	var args versionedConstraintsArgs
	args.Version = versionedConstraintsCurrentVersion
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	if args.Version == 0 {
		args.Version = versionedConstraintsCurrentVersion
	}
	return &versionedConstraintsState{Version: args.Version}, nil
}

func (f *VersionedConstraintsScanFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *versionedConstraintsState, out *vgirpc.OutputCollector) error {
	if state.Done {
		out.Finish()
		return nil
	}
	state.Done = true

	switch state.Version {
	case 1:
		ids := vgi.BuildInt64Array(2, func(i int64) int64 { return i + 1 })
		names := vgi.BuildStringArray(2, func(i int64) string {
			return []string{"Alice", "Bob"}[i]
		})
		batch := array.NewRecordBatch(VersionedConstraintsSchemas[1], []arrow.Array{ids, names}, 2)
		out.Emit(batch)

	case 2:
		ids := vgi.BuildInt64Array(3, func(i int64) int64 { return i + 1 })
		names := vgi.BuildStringArray(3, func(i int64) string {
			return []string{"Alice", "Bob", "Carol"}[i]
		})
		emails := vgi.BuildStringArray(3, func(i int64) string {
			return []string{"a@co", "b@co", "c@co"}[i]
		})
		batch := array.NewRecordBatch(VersionedConstraintsSchemas[2], []arrow.Array{ids, names, emails}, 3)
		out.Emit(batch)

	case 3:
		ids := vgi.BuildInt64Array(3, func(i int64) int64 { return i + 1 })
		names := vgi.BuildStringArray(3, func(i int64) string {
			return []string{"Alice", "Bob", "Carol"}[i]
		})
		emails := vgi.BuildStringArray(3, func(i int64) string {
			return []string{"a@co", "b@co", "c@co"}[i]
		})
		deptIDs := vgi.BuildInt64Array(3, func(i int64) int64 {
			return []int64{1, 2, 1}[i]
		})
		batch := array.NewRecordBatch(VersionedConstraintsSchemas[3], []arrow.Array{ids, names, emails, deptIDs}, 3)
		out.Emit(batch)
	}

	return nil
}

func NewVersionedConstraintsScanFunction() vgi.TableFunction {
	return vgi.AsTableFunction[versionedConstraintsState](&VersionedConstraintsScanFunction{})
}
