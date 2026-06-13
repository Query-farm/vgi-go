// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// dictFilterEchoDictType is dictionary<int8, utf8> *without* ENUM metadata, so
// DuckDB types the column as plain VARCHAR and pushes VARCHAR (string) literals
// down. The worker still emits the column dictionary-encoded, so the auto-applied
// filter must compare a dictionary column against a string literal — see
// dictionary_varchar.test.
var dictFilterEchoDictType = &arrow.DictionaryType{
	IndexType: arrow.PrimitiveTypes.Int8,
	ValueType: arrow.BinaryTypes.String,
}

var dictFilterEchoOutputSchema = arrow.NewSchema([]arrow.Field{
	{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	{Name: "s", Type: dictFilterEchoDictType},
}, nil)

// dictFilterEchoValues cycles per row: row i carries dictFilterEchoValues[i % 3].
var dictFilterEchoValues = []string{"red", "green", "blue"}

// DictFilterEchoFunction emits a dictionary-encoded VARCHAR column for
// filter-pushdown testing.
type DictFilterEchoFunction struct{}

var _ vgi.TypedTableFunc[dictFilterEchoState] = (*DictFilterEchoFunction)(nil)

func (f *DictFilterEchoFunction) Name() string { return "dict_filter_echo" }

func (f *DictFilterEchoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Emits a dictionary-encoded VARCHAR column for filter-pushdown testing",
		Stability:          vgi.StabilityConsistent,
		ProjectionPushdown: true,
		FilterPushdown:     true,
		AutoApplyFilters:   true,
		Categories:         []string{"generator", "diagnostic", "testing"},
	}
}

// dictFilterEchoArgs is the typed argument schema for dict_filter_echo().
type dictFilterEchoArgs struct {
	Count     int64 `vgi:"pos=0,doc=Number of rows to generate"`
	BatchSize int64 `vgi:"default=2048,doc=Batch size for output"`
}

func (f *DictFilterEchoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(dictFilterEchoArgs{})
}

func (f *DictFilterEchoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(dictFilterEchoOutputSchema)
}

func (f *DictFilterEchoFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	count, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: count, Max: count}, nil
}

type dictFilterEchoState struct {
	vgi.BatchState
}

func (f *DictFilterEchoFunction) NewState(params *vgi.ProcessParams) (*dictFilterEchoState, error) {
	var args dictFilterEchoArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &dictFilterEchoState{BatchState: vgi.NewBatchState(args.Count, args.BatchSize)}, nil
}

func (f *DictFilterEchoFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *dictFilterEchoState, out *vgirpc.OutputCollector) error {
	projected := vgi.ProjectedColumns(params.ProjectionIDs, dictFilterEchoOutputSchema)
	return vgi.GenerateBatchMap(&state.BatchState, out, params.OutputSchema, func(size int64) (map[string]arrow.Array, error) {
		start := state.Index
		colMap := make(map[string]arrow.Array)
		if projected.Contains("n") {
			colMap["n"] = vgi.BuildInt64Array(size, func(i int64) int64 { return start + i })
		}
		if projected.Contains("s") {
			arr, err := buildDictFilterEchoColumn(size, start)
			if err != nil {
				return nil, err
			}
			colMap["s"] = arr
		}
		return colMap, nil
	})
}

// buildDictFilterEchoColumn builds a dictionary<int8, utf8> array of `size` rows
// where row i carries dictFilterEchoValues[(start+i) % 3].
func buildDictFilterEchoColumn(size, start int64) (arrow.Array, error) {
	b := array.NewDictionaryBuilder(memory.NewGoAllocator(), dictFilterEchoDictType).(*array.BinaryDictionaryBuilder)
	defer b.Release()
	for i := int64(0); i < size; i++ {
		v := dictFilterEchoValues[(start+i)%int64(len(dictFilterEchoValues))]
		if err := b.AppendString(v); err != nil {
			return nil, err
		}
	}
	return b.NewArray(), nil
}

func NewDictFilterEchoFunction() vgi.TableFunction {
	return vgi.AsTableFunction[dictFilterEchoState](&DictFilterEchoFunction{})
}
