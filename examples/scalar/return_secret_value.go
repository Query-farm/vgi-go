// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ReturnSecretValueFunction returns a secret's value as JSON.
type ReturnSecretValueFunction struct{}

func (f *ReturnSecretValueFunction) Name() string { return "return_secret_value" }

func (f *ReturnSecretValueFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Return a secret's value",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
		RequiredSecrets: []vgi.SecretRequirement{
			{SecretType: "vgi_example"},
		},
	}
}

func (f *ReturnSecretValueFunction) ArgumentSpecs() []vgi.ArgSpec {
	return nil
}

func (f *ReturnSecretValueFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *ReturnSecretValueFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	var jsonStr string
	if params.Secrets != nil {
		if matches := params.Secrets.OfType("vgi_example"); len(matches) > 0 {
			jsonBytes, err := marshalOrderedJSON(matches[0])
			if err == nil {
				jsonStr = string(jsonBytes)
			}
		}
	}
	if jsonStr == "" {
		jsonStr = "{}"
	}

	return vgi.GenerateColumn(params, batch, array.NewStringBuilder,
		func(i int) string { return jsonStr })
}

// marshalOrderedJSON marshals a map with sorted keys for deterministic output.
func marshalOrderedJSON(m map[string]interface{}) ([]byte, error) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		keyJSON, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		buf.Write(keyJSON)
		buf.WriteByte(':')
		valJSON, err := json.Marshal(m[k])
		if err != nil {
			return nil, err
		}
		buf.Write(valJSON)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}
