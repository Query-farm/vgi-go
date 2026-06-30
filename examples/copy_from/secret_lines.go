// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package copy_from

import (
	"context"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// secretLinesInArgs are the COPY options for the secret_lines_in format.
type secretLinesInArgs struct {
	SecretType string `vgi:"default=vgi_example,doc=Secret type to fetch, scoped by the source path"`
}

// SecretLinesCopyFromFunction is a COPY ... FROM reader that forwards a CREATE
// SECRET credential. It exercises the COPY-FROM secret-bind hook
// (CopyFromSecretProvider): it requests the secret_type secret scoped to the
// source path during bind, and Read emits a single VARCHAR row holding the
// resolved secret's api_key (or NONE) — so a test can assert the caller's secret
// reached the reader. Mirrors vgi-python's SecretLinesCopyFromFunction.
type SecretLinesCopyFromFunction struct{}

var (
	_ vgi.CopyFromFunction       = (*SecretLinesCopyFromFunction)(nil)
	_ vgi.CopyFromCommenter      = (*SecretLinesCopyFromFunction)(nil)
	_ vgi.CopyFromSecretProvider = (*SecretLinesCopyFromFunction)(nil)
)

func (f *SecretLinesCopyFromFunction) Name() string { return "secret_lines_reader" }

func (f *SecretLinesCopyFromFunction) CopyFromFormat() string { return "secret_lines_in" }

func (f *SecretLinesCopyFromFunction) CopyFromComment() string {
	return "Reader that forwards a CREATE SECRET credential (test fixture)"
}

func (f *SecretLinesCopyFromFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Emit the resolved secret's api_key as a single VARCHAR row",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"copy", "test", "secret"},
		Tags:        map[string]string{"category": "copy_from", "stability": "test"},
	}
}

func (f *SecretLinesCopyFromFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(secretLinesInArgs{})
}

func secretLinesInType(args *vgi.Arguments) string {
	var opts secretLinesInArgs
	_ = vgi.BindArgs(args, &opts)
	if opts.SecretType == "" {
		return "vgi_example"
	}
	return opts.SecretType
}

// SecretLookups requests the source-scoped secret; the framework's two-phase
// secret bind resolves it and surfaces it on params.Secrets at Read time.
func (f *SecretLinesCopyFromFunction) SecretLookups(params *vgi.BindParams) []vgi.SecretLookup {
	if params.CopyFrom == nil {
		return nil
	}
	return []vgi.SecretLookup{{
		SecretType: secretLinesInType(params.Args),
		Scope:      params.CopyFrom.FilePath,
	}}
}

// Read emits one row carrying the forwarded secret's api_key (or NONE).
func (f *SecretLinesCopyFromFunction) Read(ctx context.Context, params *vgi.ProcessParams, path string, expectedSchema *arrow.Schema, out *vgirpc.OutputCollector) error {
	apiKey := "NONE"
	if fields, ok := params.Secrets.ForScopeOfType(path, secretLinesInType(params.Args)); ok {
		if v, ok := fields["api_key"]; ok {
			apiKey = vgi.RenderSecretValue(v)
		}
	}
	if expectedSchema.NumFields() != 1 {
		return fmt.Errorf("secret_lines_in: expected a single-column target, got %d columns", expectedSchema.NumFields())
	}

	mem := memory.NewGoAllocator()
	b := array.NewStringBuilder(mem)
	defer b.Release()
	b.Append(apiKey)
	arr := b.NewArray()
	defer arr.Release()

	batch := array.NewRecordBatch(expectedSchema, []arrow.Array{arr}, 1)
	return out.Emit(batch)
}
