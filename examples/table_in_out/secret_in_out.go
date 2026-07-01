// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table_in_out

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// SecretInOutFunction is a table-in-out function whose OnBind resolves the
// vgi_example secret and appends its secret_string value as a column on every
// input row. The output schema is the input schema plus a secret_string column.
//
// It declares RequiredSecrets statically in Metadata, so the extension
// pre-resolves the vgi_example secret and delivers it on the bind/process
// request. Process reads the resolved secret from params.Secrets.
type SecretInOutFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*SecretInOutFunction)(nil)

func (f *SecretInOutFunction) Name() string { return "secret_in_out" }

func (f *SecretInOutFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Append a resolved secret value to each input row",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"transform", "secret"},
		RequiredSecrets: []vgi.SecretRequirement{
			{SecretType: "vgi_example"},
		},
	}
}

func (f *SecretInOutFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"},
	}
}

func (f *SecretInOutFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	// Output schema = input schema + a trailing secret_string column.
	fields := make([]arrow.Field, 0, params.InputSchema.NumFields()+1)
	fields = append(fields, params.InputSchema.Fields()...)
	fields = append(fields, arrow.Field{Name: "secret_string", Type: arrow.BinaryTypes.String})
	return vgi.BindSchema(arrow.NewSchema(fields, nil))
}

func (f *SecretInOutFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

func (f *SecretInOutFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	// Resolve the secret_string value from the pre-resolved vgi_example secret.
	var value string
	if params.Secrets != nil {
		if matches := params.Secrets.OfType("vgi_example"); len(matches) > 0 {
			if v, ok := matches[0]["secret_string"]; ok {
				value = vgi.RenderSecretValue(v)
			}
		}
	}

	n := int(batch.NumRows())
	mem := memory.NewGoAllocator()

	// Preserve every input column (retained so the emitted batch owns its own
	// reference), then append a constant secret_string column. The emitted batch
	// is consumed asynchronously by the stream writer after Process returns, so
	// every column must remain valid past this call — NewRecordBatch retains the
	// arrays, and we drop only our local references.
	cols := make([]arrow.Array, 0, batch.NumCols()+1)
	for i := 0; i < int(batch.NumCols()); i++ {
		src := batch.Column(i)
		src.Retain()
		cols = append(cols, src)
	}

	b := array.NewStringBuilder(mem)
	for i := 0; i < n; i++ {
		b.Append(value)
	}
	secretCol := b.NewArray()
	b.Release()
	cols = append(cols, secretCol)

	rb := array.NewRecordBatch(params.OutputSchema, cols, int64(n))
	// NewRecordBatch took its own reference to each array; release ours. The
	// batch itself is handed to the stream writer, which releases it after use.
	for _, c := range cols {
		c.Release()
	}
	return out.Emit(rb)
}

func (f *SecretInOutFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewSecretInOutFunction creates a SecretInOutFunction wrapped for registration.
func NewSecretInOutFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&SecretInOutFunction{})
}
