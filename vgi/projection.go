// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package vgi

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"
)

// ScalarProjectionProfile is the canonical scalar SQL proposal profile. Offering
// it does not promise support for every expression: a callback may decline.
const ScalarProjectionProfile = scalarExpressionProfile

type projectionDecodedRequest struct {
	BindCall     BindRequestWire  `vgirpc:"bind_call"`
	BindResult   BindResponseWire `vgirpc:"bind_result"`
	Expressions  [][]byte         `vgirpc:"expressions,elem=large_binary"`
	ResultSchema []byte           `vgirpc:"result_schema"`
}
type TableFunctionProjectionRequestWire struct {
	BindCall     []byte   `vgirpc:"bind_call"`
	BindResult   []byte   `vgirpc:"bind_result"`
	Expressions  [][]byte `vgirpc:"expressions,elem=large_binary"`
	ResultSchema []byte   `vgirpc:"result_schema"`
}
type TableFunctionProjectionResponseWire struct {
	AcceptedIndices []int64           `vgirpc:"accepted_indices"`
	BindResult      *BindResponseWire `vgirpc:"bind_result"`
}

func (BindResponseWire) ArrowSchema() *arrow.Schema { return generated.BindResultSchema }

// ProjectionProposal is read-only planning input. No execution resources may be
// allocated and no rows consumed here. Opaque bytes must be self-contained.
type ProjectionProposal struct {
	Original     *BindResponse
	Expressions  [][]byte
	ResultSchema *arrow.Schema
}
type ProjectionAcceptance struct {
	AcceptedIndices []int64
	BindResult      *BindResponse
}

// TableFunctionWithProjection is optional. An absent hook or nil acceptance
// declines; the engine retains ordinary local expression evaluation.
type TableFunctionWithProjection interface {
	Projection(*BindParams, *ProjectionProposal) (*ProjectionAcceptance, error)
}

func validateProjectionProfiles(profiles []string) error {
	if len(profiles) > 64 {
		return fmt.Errorf("invalid projection profile offer")
	}
	seen := map[string]bool{}
	pattern := regexp.MustCompile(`^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*\.v[1-9][0-9]*$`)
	for _, p := range profiles {
		if len(p) > 256 || !pattern.MatchString(p) || seen[p] {
			return fmt.Errorf("invalid projection profile offer")
		}
		seen[p] = true
	}
	return nil
}
func wireProjectionBind(result *BindResponse) (BindResponseWire, error) {
	if err := validateProjectionProfiles(result.ProjectionExpressionProfiles); err != nil {
		return BindResponseWire{}, err
	}
	schema, err := SerializeSchema(result.OutputSchema)
	return BindResponseWire{OutputSchema: schema, OpaqueData: result.OpaqueData, ProjectionExpressionProfiles: result.ProjectionExpressionProfiles}, err
}
func (w workerService) handleProjection(ctx context.Context, cc *vgirpc.CallContext, req projectionDecodedRequest) (TableFunctionProjectionResponseWire, error) {
	decline := TableFunctionProjectionResponseWire{AcceptedIndices: []int64{}}
	if !strings.EqualFold(req.BindCall.FunctionType, string(FunctionTypeTable)) || req.BindCall.InputSchema != nil || req.BindCall.SchemaPath == nil {
		return decline, fmt.Errorf("projection requires an ordinary table and explicit owner")
	}
	if err := validateProjectionProfiles(req.BindResult.ProjectionExpressionProfiles); err != nil {
		return decline, err
	}
	if len(req.BindResult.LookupSecretTypes)+len(req.BindResult.LookupScopes)+len(req.BindResult.LookupNames) > 0 {
		return decline, fmt.Errorf("projection cannot request secrets")
	}
	total := len(req.ResultSchema) + len(req.BindResult.OutputSchema) + len(req.BindResult.OpaqueData) + len(req.BindCall.Arguments)
	for _, raw := range req.Expressions {
		total += len(raw)
	}
	if total > 8*1024*1024 {
		return decline, fmt.Errorf("projection exceeds 8MiB")
	}
	source, err := DeserializeSchema(req.BindResult.OutputSchema)
	if err != nil {
		return decline, err
	}
	result, err := DeserializeSchema(req.ResultSchema)
	if err != nil {
		return decline, err
	}
	if len(req.Expressions) < 1 || len(req.Expressions) > 1024 || result.NumFields() != len(req.Expressions) || result.Metadata().Len() != 0 {
		return decline, fmt.Errorf("invalid projection result schema/count")
	}
	names := map[string]bool{}
	for _, field := range append(source.Fields(), result.Fields()...) {
		name := strings.Map(func(r rune) rune {
			if r >= 'A' && r <= 'Z' {
				return r + 32
			}
			return r
		}, field.Name)
		if name == "" || !utf8.ValidString(field.Name) || strings.ContainsRune(name, 0) || names[name] {
			return decline, fmt.Errorf("ambiguous projection column names")
		}
		names[name] = true
	}
	var contextFields []arrow.Array
	for _, expression := range req.Expressions {
		if !slices.Contains(req.BindResult.ProjectionExpressionProfiles, ScalarProjectionProfile) {
			return decline, fmt.Errorf("projection profile was not offered")
		}
		if err := validateGeneratedExpression(base64.StdEncoding.EncodeToString(expression), *req.BindCall.SchemaPath); err != nil {
			return decline, err
		}
		record, err := DeserializeRecordBatch(expression)
		if err != nil {
			return decline, err
		}
		defer record.Release()
		fields := record.Column(3).(*array.Struct)
		if contextFields == nil {
			for i := 0; i < fields.NumField(); i++ {
				contextFields = append(contextFields, fields.Field(i))
			}
		} else {
			for i := 0; i < fields.NumField(); i++ {
				if !array.Equal(contextFields[i], fields.Field(i)) {
					return decline, fmt.Errorf("projection contexts differ")
				}
			}
		}
	}
	params, err := w.parseBindRequest(req.BindCall, cc)
	if err != nil {
		return decline, err
	}
	if cc != nil {
		params.Auth = cc.Auth
	}
	if backend, err := w.functionStorage(); err == nil {
		params.txBackend = backend
	}
	fn, err := w.resolveFunction(w.lookupFor(&req.BindCall, params))
	if err != nil {
		return decline, err
	}
	if _, ordinary := fn.(TableFunction); !ordinary {
		return decline, fmt.Errorf("projection requires ordinary table")
	}
	params.Args.RemapPositionalArgs(w.getArgSpecs(fn))
	provider, ok := fn.(TableFunctionWithProjection)
	if !ok {
		return decline, nil
	}
	accepted, err := provider.Projection(params, &ProjectionProposal{Original: &BindResponse{OutputSchema: source, OpaqueData: req.BindResult.OpaqueData, ProjectionExpressionProfiles: req.BindResult.ProjectionExpressionProfiles}, Expressions: req.Expressions, ResultSchema: result})
	if err != nil {
		return decline, err
	}
	if accepted == nil {
		return decline, nil
	}
	if (len(accepted.AcceptedIndices) == 0) != (accepted.BindResult == nil) {
		return decline, fmt.Errorf("projection indices and bind result must occur together")
	}
	if accepted.BindResult == nil {
		return decline, nil
	}
	fields := source.Fields()
	last := int64(-1)
	for _, index := range accepted.AcceptedIndices {
		if index <= last || index >= int64(len(req.Expressions)) {
			return decline, fmt.Errorf("invalid accepted projection index")
		}
		last = index
		fields = append(fields, result.Field(int(index)))
	}
	md := source.Metadata()
	expected := arrow.NewSchemaWithEndian(fields, &md, source.Endianness())
	value := accepted.BindResult
	if !value.OutputSchema.Equal(expected) || !value.OutputSchema.Metadata().Equal(md) || !slices.Equal(value.ProjectionExpressionProfiles, req.BindResult.ProjectionExpressionProfiles) || len(value.SecretScopeRequest) > 0 {
		return decline, fmt.Errorf("accepted projection changed bind contract")
	}
	wire, err := wireProjectionBind(value)
	return TableFunctionProjectionResponseWire{AcceptedIndices: accepted.AcceptedIndices, BindResult: &wire}, err
}

// readProjectionRecord validates one complete nested record before inspecting it.
func readProjectionRecord(raw []byte, schema *arrow.Schema) (arrow.RecordBatch, error) {
	if len(raw) > 8*1024*1024 {
		return nil, fmt.Errorf("projection exceeds 8MiB")
	}
	source := bytes.NewReader(raw)
	r, err := ipc.NewReader(source)
	if err != nil {
		return nil, err
	}
	defer r.Release()
	if !r.Next() {
		return nil, fmt.Errorf("projection record missing")
	}
	row := r.RecordBatch()
	if row.NumRows() != 1 || !row.Schema().Equal(schema) {
		return nil, fmt.Errorf("invalid projection record schema")
	}
	for i, f := range schema.Fields() {
		if !f.Nullable && row.Column(i).IsNull(0) {
			return nil, fmt.Errorf("required projection field is NULL")
		}
	}
	row.Retain()
	if r.Next() || r.Err() != nil || source.Len() != 0 {
		row.Release()
		return nil, fmt.Errorf("projection record requires one complete IPC batch")
	}
	return row, nil
}
func projectionStrings(col arrow.Array) ([]string, error) {
	if col.IsNull(0) {
		return nil, nil
	}
	list := col.(*array.List)
	start, end := list.ValueOffsets(0)
	values := list.ListValues().(*array.String)
	result := make([]string, 0, end-start)
	for i := start; i < end; i++ {
		if values.IsNull(int(i)) {
			return nil, fmt.Errorf("projection identifier/profile is NULL")
		}
		result = append(result, values.Value(int(i)))
	}
	return result, nil
}
func decodeProjectionCall(raw []byte) (BindRequestWire, error) {
	row, err := readProjectionRecord(raw, bindRequestWireSchema)
	if err != nil {
		return BindRequestWire{}, err
	}
	defer row.Release()
	binary := func(name string) *[]byte {
		c := FindColumn(row, name)
		if c.IsNull(0) {
			return nil
		}
		value := bytes.Clone(c.(*array.Binary).Value(0))
		return &value
	}
	stringValue := func(name string) *string {
		c := FindColumn(row, name)
		if c.IsNull(0) {
			return nil
		}
		v := c.(*array.String).Value(0)
		return &v
	}
	enum := FindColumn(row, "function_type").(*array.Dictionary)
	kind := enum.Dictionary().(*array.String).Value(enum.GetValueIndex(0))
	r := BindRequestWire{FunctionName: *stringValue("function_name"), Arguments: *binary("arguments"), FunctionType: kind, InputSchema: binary("input_schema"), Settings: binary("settings"), Secrets: binary("secrets"), AttachOpaqueData: binary("attach_opaque_data"), TransactionOpaqueData: binary("transaction_opaque_data"), ResolvedSecretsProvided: FindColumn(row, "resolved_secrets_provided").(*array.Boolean).Value(0), AtUnit: stringValue("at_unit"), AtValue: stringValue("at_value")}
	if !FindColumn(row, "schema_path").IsNull(0) {
		names, e := projectionStrings(FindColumn(row, "schema_path"))
		if e != nil {
			return r, e
		}
		r.SchemaPath = &names
	}
	if c := FindColumn(row, "argument_names"); !c.IsNull(0) {
		list := c.(*array.List)
		a, b := list.ValueOffsets(0)
		values := list.ListValues().(*array.String)
		names := make([]*string, 0, b-a)
		for i := a; i < b; i++ {
			var v *string
			if !values.IsNull(int(i)) {
				value := values.Value(int(i))
				v = &value
			}
			names = append(names, v)
		}
		r.ArgumentNames = &names
	}
	if c := FindColumn(row, "copy_from"); !c.IsNull(0) {
		v := c.(*array.Struct)
		r.CopyFrom = &CopyFromContextWire{Format: v.Field(0).(*array.String).Value(0), FilePath: v.Field(1).(*array.String).Value(0), ExpectedSchema: bytes.Clone(v.Field(2).(*array.Binary).Value(0))}
	}
	if c := FindColumn(row, "copy_to"); !c.IsNull(0) {
		v := c.(*array.Struct)
		r.CopyTo = &CopyToContextWire{Format: v.Field(0).(*array.String).Value(0), FilePath: v.Field(1).(*array.String).Value(0)}
	}
	return r, nil
}
func decodeProjectionBind(raw []byte) (BindResponseWire, error) {
	row, err := readProjectionRecord(raw, generated.BindResultSchema)
	if err != nil {
		return BindResponseWire{}, err
	}
	defer row.Release()
	r := BindResponseWire{OutputSchema: bytes.Clone(row.Column(0).(*array.Binary).Value(0))}
	if !row.Column(1).IsNull(0) {
		r.OpaqueData = bytes.Clone(row.Column(1).(*array.Binary).Value(0))
	}
	targets := []*[]string{&r.LookupSecretTypes, &r.LookupScopes, &r.LookupNames, &r.ProjectionExpressionProfiles}
	for i, target := range targets {
		*target, err = projectionStrings(row.Column(i + 2))
		if err != nil {
			return r, err
		}
	}
	return r, nil
}
func encodeProjectionBind(value BindResponseWire) ([]byte, error) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, generated.BindResultSchema)
	defer b.Release()
	b.Field(0).(*array.BinaryBuilder).Append(value.OutputSchema)
	if value.OpaqueData == nil {
		b.Field(1).AppendNull()
	} else {
		b.Field(1).(*array.BinaryBuilder).Append(value.OpaqueData)
	}
	for i, values := range [][]string{value.LookupSecretTypes, value.LookupScopes, value.LookupNames, value.ProjectionExpressionProfiles} {
		list := b.Field(i + 2).(*array.ListBuilder)
		list.Append(true)
		list.ValueBuilder().(*array.StringBuilder).AppendValues(values, nil)
	}
	row := b.NewRecordBatch()
	defer row.Release()
	return SerializeRecordBatch(row)
}
func (w workerService) TableFunctionProjection(ctx context.Context, cc *vgirpc.CallContext, raw TableFunctionProjectionRequestWire) (response TableFunctionProjectionResponseWire, err error) {
	defer RecoverPanic("projection", "", &err)
	decline := TableFunctionProjectionResponseWire{AcceptedIndices: []int64{}}
	size := len(raw.BindCall) + len(raw.BindResult) + len(raw.ResultSchema)
	for _, e := range raw.Expressions {
		size += len(e)
	}
	if size > 8*1024*1024 || len(raw.Expressions) > 1024 {
		return decline, fmt.Errorf("projection exceeds limits")
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, generated.TableFunctionProjectionRequestSchema)
	defer builder.Release()
	builder.Field(0).(*array.BinaryBuilder).Append(raw.BindCall)
	builder.Field(1).(*array.BinaryBuilder).Append(raw.BindResult)
	list := builder.Field(2).(*array.ListBuilder)
	list.Append(true)
	for _, e := range raw.Expressions {
		list.ValueBuilder().(*array.BinaryBuilder).Append(e)
	}
	builder.Field(3).(*array.BinaryBuilder).Append(raw.ResultSchema)
	row := builder.NewRecordBatch()
	encoded, err := SerializeRecordBatch(row)
	row.Release()
	if err != nil {
		return decline, err
	}
	if len(encoded) > 8*1024*1024 {
		return decline, fmt.Errorf("projection exceeds 8MiB")
	}

	call, err := decodeProjectionCall(raw.BindCall)
	if err != nil {
		return decline, err
	}
	bind, err := decodeProjectionBind(raw.BindResult)
	if err != nil {
		return decline, err
	}
	result, err := w.handleProjection(ctx, cc, projectionDecodedRequest{BindCall: call, BindResult: bind, Expressions: raw.Expressions, ResultSchema: raw.ResultSchema})
	if err != nil {
		return decline, err
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, generated.TableFunctionProjectionResultSchema)
	defer b.Release()
	indices := b.Field(0).(*array.ListBuilder)
	indices.Append(true)
	indices.ValueBuilder().(*array.Int64Builder).AppendValues(result.AcceptedIndices, nil)
	if result.BindResult == nil {
		b.Field(1).AppendNull()
	} else {
		nested, e := encodeProjectionBind(*result.BindResult)
		if e != nil {
			return decline, e
		}
		b.Field(1).(*array.BinaryBuilder).Append(nested)
	}
	r := b.NewRecordBatch()
	defer r.Release()
	payload, e := SerializeRecordBatch(r)
	if e != nil {
		return decline, e
	}
	if len(payload) > 8*1024*1024 {
		return decline, fmt.Errorf("projection response exceeds 8MiB")
	}
	return result, nil
}
