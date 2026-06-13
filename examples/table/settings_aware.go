// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"
	"fmt"
	"strconv"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// SettingsAwareFunction generates data demonstrating settings are passed.
type SettingsAwareFunction struct{}

var _ vgi.TypedTableFunc[settingsAwareState] = (*SettingsAwareFunction)(nil)

func (f *SettingsAwareFunction) Name() string { return "settings_aware" }

func (f *SettingsAwareFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generates data demonstrating settings are passed",
		Stability:   vgi.StabilityConsistent,
	}
}

// settingsAwareArgs is the typed argument schema for settings_aware().
type settingsAwareArgs struct {
	Count int64 `vgi:"pos=0,doc=Number of rows to generate"`
}

func (f *SettingsAwareFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(settingsAwareArgs{})
}

func (f *SettingsAwareFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "greeting", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}

	// Add details column if verbose mode is enabled
	if params.Settings != nil {
		if v, ok := params.Settings["vgi_verbose_mode"]; ok {
			isVerbose := false
			switch sv := v.(type) {
			case bool:
				isVerbose = sv
			case string:
				isVerbose = sv == "true"
			}
			if isVerbose {
				fields = append(fields, arrow.Field{Name: "details", Type: arrow.BinaryTypes.String})
			}
		}
	}

	return vgi.BindSchema(arrow.NewSchema(fields, nil))
}

func (f *SettingsAwareFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type settingsAwareState struct {
	vgi.BatchState
}

const settingsAwareBatchSize = 1000

func (f *SettingsAwareFunction) NewState(params *vgi.ProcessParams) (*settingsAwareState, error) {
	var args settingsAwareArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &settingsAwareState{
		BatchState: vgi.NewBatchState(args.Count, settingsAwareBatchSize),
	}, nil
}

func (f *SettingsAwareFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *settingsAwareState, out *vgirpc.OutputCollector) error {
	// Extract settings
	verbose := false
	greeting := "Hello"
	multiplier := int64(1)
	if params.Settings != nil {
		if v, ok := params.Settings["vgi_verbose_mode"]; ok {
			switch sv := v.(type) {
			case bool:
				verbose = sv
			case string:
				verbose = sv == "true"
			}
		}
		if v, ok := params.Settings["greeting"]; ok {
			if sv, ok := v.(string); ok {
				greeting = sv
			}
		}
		if v, ok := params.Settings["multiplier"]; ok {
			switch mv := v.(type) {
			case string:
				if n, err := strconv.ParseInt(mv, 10, 64); err == nil {
					multiplier = n
				}
			case int64:
				multiplier = mv
			case int32:
				multiplier = int64(mv)
			}
		}
	}

	return vgi.GenerateBatchMap(&state.BatchState, out, params.OutputSchema, func(size int64) (map[string]arrow.Array, error) {
		start := state.Index
		colMap := make(map[string]arrow.Array)
		colMap["id"] = vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
		colMap["greeting"] = vgi.BuildStringArray(size, func(i int64) string { return greeting })
		colMap["value"] = vgi.BuildFloat64Array(size, func(i int64) float64 { return float64(start+i) * 2.5 * float64(multiplier) })
		if verbose {
			colMap["details"] = vgi.BuildStringArray(size, func(i int64) string { return fmt.Sprintf("row_%d", start+i) })
		}
		return colMap, nil
	})
}

func NewSettingsAwareFunction() vgi.TableFunction {
	return vgi.AsTableFunction[settingsAwareState](&SettingsAwareFunction{})
}
