// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Same-name-in-two-schemas producer backing a DECLARATIVE TABLE in each schema.
//
// The table-dispatch member of the schema-disambiguation family (see
// examples/scalar/same_name.go, examples/table_in_out/same_name.go,
// examples/aggregate/same_name.go for direct dispatch and
// examples/table/same_name_cached.go for the result cache). Those probe a
// function called directly; this one probes catalog_table_scan_function_get /
// catalog_table_scan_branches_get — the RPC pair that tells a client which
// function backs a *declarative catalog table*.
//
// test_same_name_table_scan is a one-row producer registered under one name in
// BOTH the `main` and `data` schemas of the `example` catalog, each emitting a
// row tagged with its own schema. A declarative test_same_name_table is
// declared in each schema too (see cmd/vgi-example-worker/main.go), backed by
// that schema's own implementation.
//
// This is also the end-to-end guard for protocol 1.5.0's
// ScanFunctionResult.SchemaPath / ScanBranch.SchemaPath: the C++ extension now
// prefers the worker-declared schema over its old table-schema/default-schema
// heuristic when resolving which catalog entry function_name refers to. The
// old heuristic still happens to get this two-schema case right (the table's
// own schema is tried first and always matches here), so a worker that stopped
// reporting the schema shows up not as a wrong row but as the pushdown
// breakage cache/filter_pushdown_keys.test catches. What this file's test does
// catch is a worker and client that stop agreeing on which schema's table gets
// which schema's function. Driven by
// ../vgi/test/sql/integration/table/same_name_schemas.test.

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// sameNameTableScanName is deliberately shared across the two schemas — the
// collision is the point. SameNameTableName is the declarative table it backs.
const (
	sameNameTableScanName = "test_same_name_table_scan"
	// SameNameTableName is the declarative table each schema declares over its
	// own implementation; the worker binary wires it up per schema.
	SameNameTableName = "test_same_name_table"
)

// sameNameTableSchema is the single VARCHAR column every instance emits.
var sameNameTableSchema = arrow.NewSchema([]arrow.Field{
	{Name: "tag", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

// sameNameTableState is the one-shot emit latch for the single output row.
type sameNameTableState struct {
	Done bool
}

// SameNameTableScanFunction emits one row tagged with the schema it was
// declared in. Two instances register under one name, in `main` and in `data`.
type SameNameTableScanFunction struct {
	// schema is the catalog schema this instance is registered into — the tag
	// it stamps, so a mis-routed scan is visible in the query result.
	schema string
}

var _ vgi.TypedTableFunc[sameNameTableState] = (*SameNameTableScanFunction)(nil)

func (f *SameNameTableScanFunction) Name() string { return sameNameTableScanName }

func (f *SameNameTableScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Schema-disambiguation probe; the " + f.schema + "-schema table producer",
		Categories:  []string{"generator", "testing"},
		Examples: []vgi.CatalogExample{
			{
				SQL:         "SELECT * FROM example." + f.schema + "." + SameNameTableName,
				Description: "One row tagged '" + f.schema + "'",
			},
		},
	}
}

func (f *SameNameTableScanFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(sameNameTableArgs{})
}

// sameNameTableArgs declares the empty argument list.
type sameNameTableArgs struct{}

func (f *SameNameTableScanFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(sameNameTableSchema)
}

func (f *SameNameTableScanFunction) OnInit(params *vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}

func (f *SameNameTableScanFunction) NewState(params *vgi.ProcessParams) (*sameNameTableState, error) {
	return &sameNameTableState{}, nil
}

func (f *SameNameTableScanFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *sameNameTableState, out *vgirpc.OutputCollector) error {
	if state.Done {
		return out.Finish()
	}
	tag := f.schema
	arr := vgi.BuildStringArray(1, func(int64) string { return tag })
	defer arr.Release()
	batch := array.NewRecordBatch(params.OutputSchema, []arrow.Array{arr}, 1)
	if err := vgi.Emit(out, batch); err != nil {
		return err
	}
	state.Done = true
	return nil
}

// NewSameNameTableScanFunction wraps the producer for registration into
// schemaPath, which is also the tag it stamps.
func NewSameNameTableScanFunction(schemaPath string) vgi.TableFunction {
	return vgi.AsTableFunction[sameNameTableState](&SameNameTableScanFunction{schema: schemaPath})
}
