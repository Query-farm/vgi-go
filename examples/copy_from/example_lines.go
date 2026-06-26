// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package copy_from provides example custom COPY ... FROM format readers,
// mirroring vgi-python's vgi._test_fixtures.copy_from.
package copy_from

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// exampleLinesArgs are the COPY options for the example_lines format. The source
// file_path is supplied by the COPY statement, never as an option.
//
// null_string has no default (it is required — the worker enforces this);
// delimiter/skip_rows/on_error carry defaults. doc= must be last in each tag.
type exampleLinesArgs struct {
	NullString string `vgi:"doc=Token parsed as SQL NULL"`
	Delimiter  string `vgi:"default=',',doc=Field separator"`
	SkipRows   int64  `vgi:"default=0,doc=Leading lines to skip before data"`
	OnError    string `vgi:"default=fail,doc=Behavior on a row whose column count does not match the target"`
}

// ExampleLinesCopyFromFunction is a toy delimited-text COPY ... FROM reader used
// by the integration tests. Mirrors vgi-python's ExampleLinesCopyFromFunction.
type ExampleLinesCopyFromFunction struct{}

var _ vgi.CopyFromFunction = (*ExampleLinesCopyFromFunction)(nil)
var _ vgi.CopyFromCommenter = (*ExampleLinesCopyFromFunction)(nil)

// Name is the handler's registered function name (Meta.name in vgi-python).
func (f *ExampleLinesCopyFromFunction) Name() string { return "example_lines_copy_reader" }

// CopyFromFormat is the SQL FORMAT identifier users type.
func (f *ExampleLinesCopyFromFunction) CopyFromFormat() string { return "example_lines" }

// CopyFromComment is the free-text comment surfaced by vgi_copy_formats().
func (f *ExampleLinesCopyFromFunction) CopyFromComment() string {
	return "Toy delimited-text reader for tests"
}

func (f *ExampleLinesCopyFromFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Read a delimited text file into the COPY target table",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"copy", "test"},
		Tags:        map[string]string{"category": "copy_from", "stability": "test"},
	}
}

func (f *ExampleLinesCopyFromFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(exampleLinesArgs{})
}

// Read parses path line-by-line and emits one batch matching expectedSchema.
func (f *ExampleLinesCopyFromFunction) Read(ctx context.Context, params *vgi.ProcessParams, path string, expectedSchema *arrow.Schema, out *vgirpc.OutputCollector) error {
	// null_string is required: it has no default, so it must be present.
	if params.Args == nil || params.Args.Named == nil {
		return fmt.Errorf("example_lines: required option null_string is missing")
	}
	if _, ok := params.Args.Named["null_string"]; !ok {
		return fmt.Errorf("example_lines: required option null_string is missing")
	}

	var args exampleLinesArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return err
	}
	if args.Delimiter == "" {
		args.Delimiter = ","
	}
	if args.OnError == "" {
		args.OnError = "fail"
	}
	switch args.OnError {
	case "fail", "skip":
	default:
		return fmt.Errorf("example_lines: invalid on_error %q (expected 'fail' or 'skip')", args.OnError)
	}
	if args.SkipRows < 0 {
		return fmt.Errorf("example_lines: skip_rows must be >= 0, got %d", args.SkipRows)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("example_lines: reading %q: %w", path, err)
	}
	lines := strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")
	if int64(len(lines)) > args.SkipRows {
		lines = lines[args.SkipRows:]
	} else {
		lines = nil
	}

	ncols := expectedSchema.NumFields()
	// Column-major cells: cols[c][r] is the raw string for column c, row r;
	// a nil pointer marks a NULL (cell == null_string).
	cols := make([][]*string, ncols)
	for line := range lines {
		l := lines[line]
		if l == "" {
			continue
		}
		cells := strings.Split(l, args.Delimiter)
		if len(cells) != ncols {
			if args.OnError == "skip" {
				continue
			}
			return fmt.Errorf("example_lines: row has %d fields, expected %d: %q", len(cells), ncols, l)
		}
		for c := 0; c < ncols; c++ {
			if cells[c] == args.NullString {
				cols[c] = append(cols[c], nil)
			} else {
				v := cells[c]
				cols[c] = append(cols[c], &v)
			}
		}
	}

	mem := memory.NewGoAllocator()
	arrays := make([]arrow.Array, ncols)
	for c := 0; c < ncols; c++ {
		field := expectedSchema.Field(c)
		arr, err := buildColumn(mem, field.Type, cols[c])
		if err != nil {
			for _, a := range arrays[:c] {
				a.Release()
			}
			return fmt.Errorf("example_lines: column %q: %w", field.Name, err)
		}
		arrays[c] = arr
	}

	// NewRecordBatch retains the column arrays; release our refs and hand the
	// batch to Emit, which takes ownership (do not release it here).
	batch := array.NewRecordBatch(expectedSchema, arrays, int64(numRows(cols, ncols)))
	for _, a := range arrays {
		a.Release()
	}
	return out.Emit(batch)
}

func numRows(cols [][]*string, ncols int) int {
	if ncols == 0 {
		return 0
	}
	return len(cols[0])
}

// buildColumn parses string cells (nil = NULL) into an Arrow array of dt. It
// covers the scalar types the COPY target column might use; DuckDB inserts no
// cast, so the emitted type must match the target exactly.
func buildColumn(mem memory.Allocator, dt arrow.DataType, cells []*string) (arrow.Array, error) {
	switch dt.ID() {
	case arrow.STRING:
		b := array.NewStringBuilder(mem)
		defer b.Release()
		for _, c := range cells {
			if c == nil {
				b.AppendNull()
			} else {
				b.Append(*c)
			}
		}
		return b.NewArray(), nil
	case arrow.LARGE_STRING:
		b := array.NewLargeStringBuilder(mem)
		defer b.Release()
		for _, c := range cells {
			if c == nil {
				b.AppendNull()
			} else {
				b.Append(*c)
			}
		}
		return b.NewArray(), nil
	case arrow.BOOL:
		b := array.NewBooleanBuilder(mem)
		defer b.Release()
		for _, c := range cells {
			if c == nil {
				b.AppendNull()
				continue
			}
			v, err := strconv.ParseBool(strings.TrimSpace(*c))
			if err != nil {
				return nil, err
			}
			b.Append(v)
		}
		return b.NewArray(), nil
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return buildIntColumn(mem, dt, cells)
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return buildUintColumn(mem, dt, cells)
	case arrow.FLOAT32:
		b := array.NewFloat32Builder(mem)
		defer b.Release()
		for _, c := range cells {
			if c == nil {
				b.AppendNull()
				continue
			}
			v, err := strconv.ParseFloat(strings.TrimSpace(*c), 32)
			if err != nil {
				return nil, err
			}
			b.Append(float32(v))
		}
		return b.NewArray(), nil
	case arrow.FLOAT64:
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		for _, c := range cells {
			if c == nil {
				b.AppendNull()
				continue
			}
			v, err := strconv.ParseFloat(strings.TrimSpace(*c), 64)
			if err != nil {
				return nil, err
			}
			b.Append(v)
		}
		return b.NewArray(), nil
	default:
		return nil, fmt.Errorf("unsupported target type %s", dt)
	}
}

func buildIntColumn(mem memory.Allocator, dt arrow.DataType, cells []*string) (arrow.Array, error) {
	b := array.NewBuilder(mem, dt)
	defer b.Release()
	for _, c := range cells {
		if c == nil {
			b.AppendNull()
			continue
		}
		v, err := strconv.ParseInt(strings.TrimSpace(*c), 10, 64)
		if err != nil {
			return nil, err
		}
		switch bb := b.(type) {
		case *array.Int8Builder:
			bb.Append(int8(v))
		case *array.Int16Builder:
			bb.Append(int16(v))
		case *array.Int32Builder:
			bb.Append(int32(v))
		case *array.Int64Builder:
			bb.Append(v)
		}
	}
	return b.NewArray(), nil
}

func buildUintColumn(mem memory.Allocator, dt arrow.DataType, cells []*string) (arrow.Array, error) {
	b := array.NewBuilder(mem, dt)
	defer b.Release()
	for _, c := range cells {
		if c == nil {
			b.AppendNull()
			continue
		}
		v, err := strconv.ParseUint(strings.TrimSpace(*c), 10, 64)
		if err != nil {
			return nil, err
		}
		switch bb := b.(type) {
		case *array.Uint8Builder:
			bb.Append(uint8(v))
		case *array.Uint16Builder:
			bb.Append(uint16(v))
		case *array.Uint32Builder:
			bb.Append(uint32(v))
		case *array.Uint64Builder:
			bb.Append(v)
		}
	}
	return b.NewArray(), nil
}
