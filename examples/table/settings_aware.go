// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"context"
	"fmt"
	"strconv"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// SettingsAwareFunction generates data demonstrating settings are passed.
type SettingsAwareFunction struct{}

func (f *SettingsAwareFunction) Name() string { return "settings_aware" }

func (f *SettingsAwareFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Generates data demonstrating settings are passed",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *SettingsAwareFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "count", Position: 0, ArrowType: "int64", Doc: "Number of rows to generate"},
	}
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

	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema(fields, nil),
	}, nil
}

func (f *SettingsAwareFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}

func (f *SettingsAwareFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type settingsAwareState struct {
	remaining    int64
	currentIndex int64
}

func (f *SettingsAwareFunction) NewState(params *vgi.ProcessParams) (interface{}, error) {
	count, _ := params.Args.GetScalarInt64(0)
	return &settingsAwareState{remaining: count, currentIndex: 0}, nil
}

const settingsAwareBatchSize = 1000

func (f *SettingsAwareFunction) Process(ctx context.Context, params *vgi.ProcessParams, state interface{}, out *vgirpc.OutputCollector) error {
	s := state.(*settingsAwareState)
	if s.remaining <= 0 {
		return out.Finish()
	}

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

	size := int64(settingsAwareBatchSize)
	if s.remaining < size {
		size = s.remaining
	}

	mem := memory.NewGoAllocator()

	idBuilder := array.NewInt64Builder(mem)
	defer idBuilder.Release()
	greetingBuilder := array.NewStringBuilder(mem)
	defer greetingBuilder.Release()
	valueBuilder := array.NewFloat64Builder(mem)
	defer valueBuilder.Release()

	var detailsBuilder *array.StringBuilder
	if verbose {
		detailsBuilder = array.NewStringBuilder(mem)
		defer detailsBuilder.Release()
	}

	for i := int64(0); i < size; i++ {
		idx := s.currentIndex + i
		idBuilder.Append(idx)
		greetingBuilder.Append(greeting)
		valueBuilder.Append(float64(idx) * 2.5 * float64(multiplier))
		if verbose {
			detailsBuilder.Append(fmt.Sprintf("row_%d", idx))
		}
	}

	idArr := idBuilder.NewArray()
	defer idArr.Release()
	greetingArr := greetingBuilder.NewArray()
	defer greetingArr.Release()
	valueArr := valueBuilder.NewArray()
	defer valueArr.Release()

	var cols []arrow.Array
	if verbose {
		detailsArr := detailsBuilder.NewArray()
		defer detailsArr.Release()
		cols = []arrow.Array{idArr, greetingArr, valueArr, detailsArr}
	} else {
		cols = []arrow.Array{idArr, greetingArr, valueArr}
	}

	s.currentIndex += size
	s.remaining -= size
	return out.EmitArrays(cols, size)
}
