// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table

import (
	"context"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ColorsScanFunction emits 3 fixed rows (id, color, hex_code) backing the
// data.colors catalog table used in the column-statistics integration tests.
type ColorsScanFunction struct{}

var _ vgi.TypedTableFunc[colorsScanState] = (*ColorsScanFunction)(nil)

func (*ColorsScanFunction) Name() string { return "colors_scan" }

func (*ColorsScanFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Fixed 3-row colors table (blue, green, red)",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"static", "internal"},
	}
}

func (*ColorsScanFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

var colorsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "color", Type: arrow.BinaryTypes.String},
	{Name: "hex_code", Type: arrow.BinaryTypes.String},
}, nil)

func (*ColorsScanFunction) OnBind(p *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(colorsSchema)
}

func (*ColorsScanFunction) Cardinality(p *vgi.BindParams) (*vgi.TableCardinality, error) {
	return &vgi.TableCardinality{Estimate: 3, Max: 3}, nil
}

type colorsScanState struct{ Emitted bool }

func (*ColorsScanFunction) NewState(p *vgi.ProcessParams) (*colorsScanState, error) {
	return &colorsScanState{}, nil
}

func (*ColorsScanFunction) Process(ctx context.Context, p *vgi.ProcessParams, state *colorsScanState, out *vgirpc.OutputCollector) error {
	if state.Emitted {
		out.Finish()
		return nil
	}
	ids := []int64{1, 2, 3}
	colors := []string{"blue", "green", "red"}
	hex := []string{"#0000FF", "#00FF00", "#FF0000"}
	idCol := vgi.BuildInt64Array(3, func(i int64) int64 { return ids[i] })
	colorCol := vgi.BuildStringArray(3, func(i int64) string { return colors[i] })
	hexCol := vgi.BuildStringArray(3, func(i int64) string { return hex[i] })
	defer idCol.Release()
	defer colorCol.Release()
	defer hexCol.Release()
	batch := array.NewRecordBatch(colorsSchema, []arrow.Array{idCol, colorCol, hexCol}, 3)
	defer batch.Release()
	state.Emitted = true
	if err := out.Emit(batch); err != nil {
		return err
	}
	out.Finish()
	return nil
}

func NewColorsScanFunction() vgi.TableFunction {
	return vgi.AsTableFunction[colorsScanState](&ColorsScanFunction{})
}
