// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"bytes"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// CatalogExample is a usage example attached to a FunctionInfo.
type CatalogExample struct {
	SQL            string
	Description    string
	ExpectedOutput *string
}

// FunctionInfo describes a function in the catalog.
type FunctionInfo struct {
	Name                       string
	SchemaName                 string
	FunctionType               FunctionType
	ArgSchema                  *arrow.Schema // argument schema
	OutputSchema               *arrow.Schema // return schema
	Stability                  FunctionStability
	NullHandling               NullHandling
	Description                string
	Comment                    string
	Tags                       map[string]string
	Examples                   []CatalogExample
	Categories                 []string
	ProjectionPushdown         *bool
	FilterPushdown             *bool
	SamplingPushdown           *bool
	SupportedExpressionFilters []string
	OrderPreservation          OrderPreservation // "" = null
	MaxWorkers                 int32
	SupportsBatchIndex         bool               // opt-in per-batch batch_index tagging
	PartitionKind              PartitionKind      // default PartitionKindNotPartitioned
	OrderDependent             OrderDependence    // default NOT_ORDER_DEPENDENT
	DistinctDependent          DistinctDependence // default NOT_DISTINCT_DEPENDENT
	SupportsWindow             bool
	StreamingPartitioned       bool
	HasFinalize                bool
	RequiredSettings           []string
	RequiredSecrets            []SecretRequirement
}

var dictType = &arrow.DictionaryType{
	IndexType: arrow.PrimitiveTypes.Int16,
	ValueType: arrow.BinaryTypes.String,
}

// exampleStructType is the struct type for one CatalogExample.
var exampleStructType = arrow.StructOf(
	arrow.Field{Name: "sql", Type: arrow.BinaryTypes.String},
	arrow.Field{Name: "description", Type: arrow.BinaryTypes.String},
	arrow.Field{Name: "expected_output", Type: arrow.BinaryTypes.String, Nullable: true},
)

// requiredSecretStructType is the struct type for one required-secret entry.
var requiredSecretStructType = arrow.StructOf(
	arrow.Field{Name: "secret_type", Type: arrow.BinaryTypes.String},
	arrow.Field{Name: "scope", Type: arrow.BinaryTypes.String, Nullable: true},
	arrow.Field{Name: "secret_name", Type: arrow.BinaryTypes.String, Nullable: true},
)

var functionInfoSchema = generated.FunctionInfoSchema

// SerializeFunctionInfo serializes a FunctionInfo to IPC bytes.
func SerializeFunctionInfo(info *FunctionInfo) ([]byte, error) {
	mem := memory.NewGoAllocator()

	// comment
	commentBuilder := array.NewStringBuilder(mem)
	defer commentBuilder.Release()
	if info.Comment != "" {
		commentBuilder.Append(info.Comment)
	} else {
		commentBuilder.AppendNull()
	}

	// tags
	tagsBuilder := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
	defer tagsBuilder.Release()
	tagsBuilder.Append(true)
	if len(info.Tags) > 0 {
		kb := tagsBuilder.KeyBuilder().(*array.StringBuilder)
		vb := tagsBuilder.ItemBuilder().(*array.StringBuilder)
		for k, v := range info.Tags {
			kb.Append(k)
			vb.Append(v)
		}
	}

	// name
	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release()
	nameBuilder.Append(info.Name)

	// schema_name
	schemaNameBuilder := array.NewStringBuilder(mem)
	defer schemaNameBuilder.Release()
	schemaName := info.SchemaName
	if schemaName == "" {
		schemaName = "main"
	}
	schemaNameBuilder.Append(schemaName)

	// function_type
	ftBuilder := array.NewDictionaryBuilder(mem, dictType)
	defer ftBuilder.Release()
	ftBuilder.(*array.BinaryDictionaryBuilder).AppendString(string(info.FunctionType))

	// arguments
	argBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer argBuilder.Release()
	if info.ArgSchema != nil {
		argBytes, err := SerializeSchema(info.ArgSchema)
		if err != nil {
			return nil, err
		}
		argBuilder.Append(argBytes)
	} else {
		argBuilder.Append([]byte{})
	}

	// output_schema
	outputBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer outputBuilder.Release()
	if info.OutputSchema != nil {
		outBytes, err := SerializeSchema(info.OutputSchema)
		if err != nil {
			return nil, err
		}
		outputBuilder.Append(outBytes)
	} else {
		outputBuilder.Append([]byte{})
	}

	// stability
	stabBuilder := array.NewDictionaryBuilder(mem, dictType)
	defer stabBuilder.Release()
	if info.Stability != "" {
		stabBuilder.(*array.BinaryDictionaryBuilder).AppendString(string(info.Stability))
	} else {
		stabBuilder.AppendNull()
	}

	// null_handling
	nhBuilder := array.NewDictionaryBuilder(mem, dictType)
	defer nhBuilder.Release()
	if info.NullHandling != "" {
		nhBuilder.(*array.BinaryDictionaryBuilder).AppendString(string(info.NullHandling))
	} else {
		nhBuilder.AppendNull()
	}

	// description
	descBuilder := array.NewStringBuilder(mem)
	defer descBuilder.Release()
	descBuilder.Append(info.Description)

	// examples
	examplesBuilder := array.NewListBuilder(mem, exampleStructType)
	defer examplesBuilder.Release()
	examplesBuilder.Append(true)
	exSb := examplesBuilder.ValueBuilder().(*array.StructBuilder)
	for _, ex := range info.Examples {
		exSb.Append(true)
		exSb.FieldBuilder(0).(*array.StringBuilder).Append(ex.SQL)
		exSb.FieldBuilder(1).(*array.StringBuilder).Append(ex.Description)
		if ex.ExpectedOutput != nil {
			exSb.FieldBuilder(2).(*array.StringBuilder).Append(*ex.ExpectedOutput)
		} else {
			exSb.FieldBuilder(2).(*array.StringBuilder).AppendNull()
		}
	}

	// categories
	categoriesBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer categoriesBuilder.Release()
	categoriesBuilder.Append(true)
	if len(info.Categories) > 0 {
		vb := categoriesBuilder.ValueBuilder().(*array.StringBuilder)
		for _, cat := range info.Categories {
			vb.Append(cat)
		}
	}

	// projection_pushdown
	ppBuilder := array.NewBooleanBuilder(mem)
	defer ppBuilder.Release()
	if info.ProjectionPushdown != nil {
		ppBuilder.Append(*info.ProjectionPushdown)
	} else {
		ppBuilder.AppendNull()
	}

	// filter_pushdown
	fpBuilder := array.NewBooleanBuilder(mem)
	defer fpBuilder.Release()
	if info.FilterPushdown != nil {
		fpBuilder.Append(*info.FilterPushdown)
	} else {
		fpBuilder.AppendNull()
	}

	// sampling_pushdown
	spBuilder := array.NewBooleanBuilder(mem)
	defer spBuilder.Release()
	if info.SamplingPushdown != nil {
		spBuilder.Append(*info.SamplingPushdown)
	} else {
		spBuilder.AppendNull()
	}

	// supported_expression_filters
	sefBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer sefBuilder.Release()
	sefBuilder.Append(true)
	sefVb := sefBuilder.ValueBuilder().(*array.StringBuilder)
	for _, f := range info.SupportedExpressionFilters {
		sefVb.Append(f)
	}

	// order_preservation
	opBuilder := array.NewDictionaryBuilder(mem, dictType)
	defer opBuilder.Release()
	if info.OrderPreservation != "" {
		opBuilder.(*array.BinaryDictionaryBuilder).AppendString(string(info.OrderPreservation))
	} else {
		opBuilder.AppendNull()
	}

	// max_workers (non-null)
	mwBuilder := array.NewInt32Builder(mem)
	defer mwBuilder.Release()
	mwBuilder.Append(info.MaxWorkers)

	// supports_batch_index (non-null)
	sbiBuilder := array.NewBooleanBuilder(mem)
	defer sbiBuilder.Release()
	sbiBuilder.Append(info.SupportsBatchIndex)

	// partition_kind (non-null dict)
	pkBuilder := array.NewDictionaryBuilder(mem, dictType)
	defer pkBuilder.Release()
	pk := info.PartitionKind
	if pk == "" {
		pk = PartitionKindNotPartitioned
	}
	pkBuilder.(*array.BinaryDictionaryBuilder).AppendString(string(pk))

	// order_dependent (non-null dict)
	odBuilder := array.NewDictionaryBuilder(mem, dictType)
	defer odBuilder.Release()
	od := info.OrderDependent
	if od == "" {
		od = OrderDependenceNotDependent
	}
	odBuilder.(*array.BinaryDictionaryBuilder).AppendString(string(od))

	// distinct_dependent (non-null dict)
	ddBuilder := array.NewDictionaryBuilder(mem, dictType)
	defer ddBuilder.Release()
	dd := info.DistinctDependent
	if dd == "" {
		dd = DistinctDependenceNotDependent
	}
	ddBuilder.(*array.BinaryDictionaryBuilder).AppendString(string(dd))

	// supports_window
	swBuilder := array.NewBooleanBuilder(mem)
	defer swBuilder.Release()
	swBuilder.Append(info.SupportsWindow)

	// streaming_partitioned
	spartBuilder := array.NewBooleanBuilder(mem)
	defer spartBuilder.Release()
	spartBuilder.Append(info.StreamingPartitioned)

	// has_finalize
	hfBuilder := array.NewBooleanBuilder(mem)
	defer hfBuilder.Release()
	hfBuilder.Append(info.HasFinalize)

	// required_settings
	reqSettingsBuilder := array.NewListBuilder(mem, arrow.BinaryTypes.String)
	defer reqSettingsBuilder.Release()
	reqSettingsBuilder.Append(true)
	rsVb := reqSettingsBuilder.ValueBuilder().(*array.StringBuilder)
	for _, s := range info.RequiredSettings {
		rsVb.Append(s)
	}

	// required_secrets (non-null list)
	rsListBuilder := array.NewListBuilder(mem, requiredSecretStructType)
	defer rsListBuilder.Release()
	rsListBuilder.Append(true)
	rsSb := rsListBuilder.ValueBuilder().(*array.StructBuilder)
	for _, rs := range info.RequiredSecrets {
		rsSb.Append(true)
		rsSb.FieldBuilder(0).(*array.StringBuilder).Append(rs.SecretType)
		if rs.Scope != "" {
			rsSb.FieldBuilder(1).(*array.StringBuilder).Append(rs.Scope)
		} else {
			rsSb.FieldBuilder(1).(*array.StringBuilder).AppendNull()
		}
		if rs.SecretName != "" {
			rsSb.FieldBuilder(2).(*array.StringBuilder).Append(rs.SecretName)
		} else {
			rsSb.FieldBuilder(2).(*array.StringBuilder).AppendNull()
		}
	}

	cols := []arrow.Array{
		commentBuilder.NewArray(),
		tagsBuilder.NewArray(),
		nameBuilder.NewArray(),
		schemaNameBuilder.NewArray(),
		ftBuilder.NewArray(),
		argBuilder.NewArray(),
		outputBuilder.NewArray(),
		stabBuilder.NewArray(),
		nhBuilder.NewArray(),
		descBuilder.NewArray(),
		examplesBuilder.NewArray(),
		categoriesBuilder.NewArray(),
		ppBuilder.NewArray(),
		fpBuilder.NewArray(),
		spBuilder.NewArray(),
		sefBuilder.NewArray(),
		opBuilder.NewArray(),
		mwBuilder.NewArray(),
		sbiBuilder.NewArray(),
		pkBuilder.NewArray(),
		odBuilder.NewArray(),
		ddBuilder.NewArray(),
		swBuilder.NewArray(),
		spartBuilder.NewArray(),
		hfBuilder.NewArray(),
		reqSettingsBuilder.NewArray(),
		rsListBuilder.NewArray(),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(functionInfoSchema, cols, 1)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(functionInfoSchema))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// SchemaInfo describes a schema in the catalog.
type SchemaInfo struct {
	Name             string
	Comment          string
	Tags             map[string]string
	AttachOpaqueData []byte
	// EstimatedObjectCount, when non-nil, advertises the approximate per-kind
	// population (e.g. {"table": 0, "view": 12}). A value of 0 is a hard
	// guarantee — the C++ client skips the corresponding bulk RPC and any
	// per-name lookup for that kind. Nil disables the optimisation entirely.
	EstimatedObjectCount map[string]int64
}

var schemaInfoSchema = generated.SchemaInfoSchema

// SerializeSchemaInfo serializes a SchemaInfo to IPC bytes.
func SerializeSchemaInfo(info *SchemaInfo) ([]byte, error) {
	mem := memory.NewGoAllocator()

	commentBuilder := array.NewStringBuilder(mem)
	defer commentBuilder.Release()
	if info.Comment != "" {
		commentBuilder.Append(info.Comment)
	} else {
		commentBuilder.AppendNull()
	}

	tagsBuilder := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
	defer tagsBuilder.Release()
	tagsBuilder.Append(true) // always non-null, empty map if no tags
	if len(info.Tags) > 0 {
		kb := tagsBuilder.KeyBuilder().(*array.StringBuilder)
		vb := tagsBuilder.ItemBuilder().(*array.StringBuilder)
		for k, v := range info.Tags {
			kb.Append(k)
			vb.Append(v)
		}
	}

	attachOpaqueDataBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer attachOpaqueDataBuilder.Release()
	if info.AttachOpaqueData != nil {
		attachOpaqueDataBuilder.Append(info.AttachOpaqueData)
	} else {
		attachOpaqueDataBuilder.Append([]byte{})
	}

	nameBuilder := array.NewStringBuilder(mem)
	defer nameBuilder.Release()
	nameBuilder.Append(info.Name)

	// estimated_object_count: nullable map<string, int64>
	eocBuilder := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64, false)
	defer eocBuilder.Release()
	if info.EstimatedObjectCount == nil {
		eocBuilder.AppendNull()
	} else {
		eocBuilder.Append(true)
		kb := eocBuilder.KeyBuilder().(*array.StringBuilder)
		vb := eocBuilder.ItemBuilder().(*array.Int64Builder)
		for k, v := range info.EstimatedObjectCount {
			kb.Append(k)
			vb.Append(v)
		}
	}

	cols := []arrow.Array{
		commentBuilder.NewArray(),
		tagsBuilder.NewArray(),
		attachOpaqueDataBuilder.NewArray(),
		nameBuilder.NewArray(),
		eocBuilder.NewArray(),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(schemaInfoSchema, cols, 1)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schemaInfoSchema))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
