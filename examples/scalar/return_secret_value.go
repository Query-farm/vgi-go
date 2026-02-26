// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package scalar

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ReturnSecretValueFunction returns a secret's value as JSON.
type ReturnSecretValueFunction struct{}

func (f *ReturnSecretValueFunction) Name() string { return "return_secret_value" }

func (f *ReturnSecretValueFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Return a secret's value",
		Stability:   vgi.StabilityConsistent,
	}
}

func (f *ReturnSecretValueFunction) ArgumentSpecs() []vgi.ArgSpec {
	return nil
}

func (f *ReturnSecretValueFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "result", Type: arrow.BinaryTypes.String},
		}, nil),
	}, nil
}

func (f *ReturnSecretValueFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	n := int(batch.NumRows())

	// Get the vgi_example_secret
	var jsonStr string
	if params.Secrets != nil {
		if secret, ok := params.Secrets["vgi_example_secret"]; ok {
			// Convert to ordered JSON
			orderedMap := make(map[string]interface{})
			for k, v := range secret {
				orderedMap[k] = v
			}
			jsonBytes, err := marshalOrderedJSON(orderedMap)
			if err == nil {
				jsonStr = string(jsonBytes)
			}
		}
	}
	if jsonStr == "" {
		jsonStr = "{}"
	}

	builder := array.NewStringBuilder(mem)
	defer builder.Release()

	for i := 0; i < n; i++ {
		builder.Append(jsonStr)
	}

	resultArr := builder.NewArray()
	defer resultArr.Release()

	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{resultArr}, int64(n)), nil
}

// marshalOrderedJSON marshals a map with sorted keys for deterministic output.
func marshalOrderedJSON(m map[string]interface{}) ([]byte, error) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ordered := make(map[string]interface{}, len(m))
	for _, k := range keys {
		ordered[k] = m[k]
	}
	return json.Marshal(ordered)
}
