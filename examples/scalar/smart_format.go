// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package scalar

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// formatFloat formats a float with shortest representation, ensuring at least
// one decimal place (e.g., 0 → "0.0", 3.14 → "3.14").
func formatFloat(v float64) string {
	s := strconv.FormatFloat(v, 'f', -1, 64)
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	return s
}

// SmartFormatWidthFunction formats a number right-aligned to a given width.
type SmartFormatWidthFunction struct{}

func (f *SmartFormatWidthFunction) Name() string { return "smart_format" }

func (f *SmartFormatWidthFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Format number right-aligned to specified width",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (f *SmartFormatWidthFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "width", Position: 0, ArrowType: "int64", Doc: "Field width", IsConst: true},
		{Name: "value", Position: 1, ArrowType: "double", Doc: "Number to format"},
	}
}

func (f *SmartFormatWidthFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *SmartFormatWidthFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	width, _ := params.Args.GetScalarInt64(0)
	return vgi.MapColumn(params, batch, 0, array.NewStringBuilder,
		func(col arrow.Array, i int) string {
			return fmt.Sprintf("%*s", width, formatFloat(vgi.GetFloat64Value(col, i)))
		})
}

// SmartFormatPrefixFunction formats a number with a string prefix.
type SmartFormatPrefixFunction struct{}

func (f *SmartFormatPrefixFunction) Name() string { return "smart_format" }

func (f *SmartFormatPrefixFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Format number with string prefix",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.String,
	}
}

func (f *SmartFormatPrefixFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "prefix", Position: 0, ArrowType: "varchar", Doc: "Prefix string", IsConst: true},
		{Name: "value", Position: 1, ArrowType: "double", Doc: "Number to format"},
	}
}

func (f *SmartFormatPrefixFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.String)
}

func (f *SmartFormatPrefixFunction) Process(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	prefix, _ := params.Args.GetScalarString(0)
	return vgi.MapColumn(params, batch, 0, array.NewStringBuilder,
		func(col arrow.Array, i int) string {
			return prefix + formatFloat(vgi.GetFloat64Value(col, i))
		})
}
