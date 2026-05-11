// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// StructSettingsFunction generates a sequence configured by a struct setting.
type StructSettingsFunction struct{}

var _ vgi.TypedTableFunc[structSettingsState] = (*StructSettingsFunction)(nil)

func (f *StructSettingsFunction) Name() string { return "struct_settings" }

func (f *StructSettingsFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generate a sequence configured by a struct setting",
		Stability:   vgi.StabilityConsistent,
	}
}

// structSettingsArgs is the typed argument schema for struct_settings().
type structSettingsArgs struct {
	Count int64 `vgi:"pos=0,doc=Number of rows to generate"`
}

func (f *StructSettingsFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(structSettingsArgs{})
}

func (f *StructSettingsFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		{Name: "label", Type: arrow.BinaryTypes.String},
	}, nil))
}

func (f *StructSettingsFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type structSettingsState struct {
	vgi.BatchState
	Start int64
	Step  int64
	Label string
}

const structSettingsBatchSize = 1000

func (f *StructSettingsFunction) NewState(params *vgi.ProcessParams) (*structSettingsState, error) {
	var args structSettingsArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}

	start := int64(0)
	step := int64(1)
	label := "item"

	if params.Settings != nil {
		if v, ok := params.Settings["config"]; ok {
			if cfg, ok := v.(map[string]interface{}); ok {
				if s, ok := cfg["start"].(int64); ok {
					start = s
				}
				if s, ok := cfg["step"].(int64); ok {
					step = s
				}
				if s, ok := cfg["label"].(string); ok {
					label = s
				}
			}
		}
	}

	return &structSettingsState{
		BatchState: vgi.NewBatchState(args.Count, structSettingsBatchSize),
		Start:      start,
		Step:       step,
		Label:      label,
	}, nil
}

func (f *StructSettingsFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *structSettingsState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatchMap(&state.BatchState, out, params.OutputSchema, func(size int64) (map[string]arrow.Array, error) {
		idx := state.Index
		colMap := make(map[string]arrow.Array)
		colMap["n"] = vgi.BuildInt64Array(size, func(i int64) int64 { return state.Start + (idx+i)*state.Step })
		colMap["label"] = vgi.BuildStringArray(size, func(i int64) string { return fmt.Sprintf("%s_%d", state.Label, idx+i) })
		return colMap, nil
	})
}

func NewStructSettingsFunction() vgi.TableFunction {
	return vgi.AsTableFunction[structSettingsState](&StructSettingsFunction{})
}
