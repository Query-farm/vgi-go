// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package copy_to provides example custom COPY ... TO format writers, mirroring
// vgi-python's vgi._test_fixtures.copy_to.
package copy_to

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// shardNS is the state-log key COPY TO shards are appended under, mirroring the
// Python fixture's _SHARD_NS. One bucket per execution.
var shardNS = []byte("copy_to_shard")

// exampleLinesOutArgs are the COPY options for the example_lines_out format. The
// destination file_path is supplied by the COPY statement, never as an option.
//
// null_string has no default (it is required — the worker enforces this);
// delimiter/header/on_exists carry defaults. doc= must be last in each tag.
type exampleLinesOutArgs struct {
	NullString string `vgi:"doc=Token written for SQL NULL"`
	Delimiter  string `vgi:"default=',',doc=Field separator"`
	Header     bool   `vgi:"default=false,doc=Write a header row of column names"`
	OnExists   string `vgi:"default=overwrite,doc=Behavior when the destination file already exists"`
}

// ExampleLinesCopyToFunction is a toy delimited-text COPY ... TO writer used by
// the integration tests. Mirrors vgi-python's ExampleLinesCopyToFunction. It
// buffers each input batch as an IPC blob in execution-scoped storage (the
// cross-process-safe pattern) and concatenates them to the destination in
// Close.
type ExampleLinesCopyToFunction struct{}

var _ vgi.CopyToFunction = (*ExampleLinesCopyToFunction)(nil)
var _ vgi.CopyToCommenter = (*ExampleLinesCopyToFunction)(nil)

// Name is the handler's registered function name (Meta.name in vgi-python).
func (f *ExampleLinesCopyToFunction) Name() string { return "example_lines_writer" }

// CopyToFormat is the SQL FORMAT identifier users type.
func (f *ExampleLinesCopyToFunction) CopyToFormat() string { return "example_lines_out" }

// CopyToComment is the free-text comment surfaced by vgi_copy_formats().
func (f *ExampleLinesCopyToFunction) CopyToComment() string {
	return "Toy delimited-text writer for tests"
}

func (f *ExampleLinesCopyToFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Write the COPY source to a delimited text file",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"copy", "test"},
		Tags:        map[string]string{"category": "copy_to", "stability": "test"},
	}
}

func (f *ExampleLinesCopyToFunction) ArgumentSpecs() []vgi.ArgSpec {
	return vgi.DeriveArgSpecs(exampleLinesOutArgs{})
}

// bindOptions parses + validates the COPY options. null_string is required (it
// has no default), so its absence is a worker-side error.
func bindOptions(params *vgi.ProcessParams) (exampleLinesOutArgs, error) {
	var args exampleLinesOutArgs
	if params.Args == nil || params.Args.Named == nil {
		return args, fmt.Errorf("example_lines_out: required option null_string is missing")
	}
	if _, ok := params.Args.Named["null_string"]; !ok {
		return args, fmt.Errorf("example_lines_out: required option null_string is missing")
	}
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return args, err
	}
	if args.Delimiter == "" {
		args.Delimiter = ","
	}
	if args.OnExists == "" {
		args.OnExists = "overwrite"
	}
	switch args.OnExists {
	case "overwrite", "error":
	default:
		return args, fmt.Errorf("example_lines_out: invalid on_exists %q (expected 'overwrite' or 'error')", args.OnExists)
	}
	return args, nil
}

// Write buffers one input batch as an IPC blob in execution-scoped storage.
// state_append is atomic + race-safe across parallel sink threads/workers.
func (f *ExampleLinesCopyToFunction) Write(ctx context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) error {
	if _, err := bindOptions(params); err != nil {
		return err
	}
	data, err := vgi.SerializeRecordBatch(batch)
	if err != nil {
		return fmt.Errorf("example_lines_out: serializing batch: %w", err)
	}
	if _, err := params.Storage.StateAppend(shardNS, data); err != nil {
		return err
	}
	return nil
}

// Close concatenates every shard and writes the delimited destination file
// (once). Called even when zero rows were buffered (empty COPY).
func (f *ExampleLinesCopyToFunction) Close(ctx context.Context, params *vgi.ProcessParams) error {
	args, err := bindOptions(params)
	if err != nil {
		return err
	}
	if params.CopyTo == nil {
		return fmt.Errorf("example_lines_out: missing COPY TO context at close")
	}
	path := params.CopyTo.FilePath

	if args.OnExists == "error" {
		if _, statErr := os.Stat(path); statErr == nil {
			return fmt.Errorf("example_lines_out: destination already exists: %s", path)
		}
	}

	entries, err := params.Storage.StateLogScan(shardNS, -1, 0)
	if err != nil {
		return err
	}

	fh, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("example_lines_out: creating %q: %w", path, err)
	}
	defer fh.Close()

	wroteHeader := false
	for _, e := range entries {
		batch, derr := vgi.DeserializeRecordBatch(e.Value)
		if derr != nil {
			return fmt.Errorf("example_lines_out: reading shard: %w", derr)
		}
		if args.Header && !wroteHeader {
			if _, werr := fmt.Fprintln(fh, strings.Join(schemaNames(batch.Schema()), args.Delimiter)); werr != nil {
				batch.Release()
				return werr
			}
			wroteHeader = true
		}
		nrows := int(batch.NumRows())
		ncols := int(batch.NumCols())
		for r := 0; r < nrows; r++ {
			cells := make([]string, ncols)
			for c := 0; c < ncols; c++ {
				cells[c] = formatCell(batch.Column(c), r, args.NullString)
			}
			if _, werr := fmt.Fprintln(fh, strings.Join(cells, args.Delimiter)); werr != nil {
				batch.Release()
				return werr
			}
		}
		batch.Release()
	}

	// Empty COPY with header=true still emits the header row. Take the column
	// names from the bind's input (source) schema.
	if args.Header && !wroteHeader && params.InputSchema != nil {
		if _, werr := fmt.Fprintln(fh, strings.Join(schemaNames(params.InputSchema), args.Delimiter)); werr != nil {
			return werr
		}
	}
	return nil
}

// schemaNames returns the field names of schema in order.
func schemaNames(schema *arrow.Schema) []string {
	names := make([]string, schema.NumFields())
	for i, fld := range schema.Fields() {
		names[i] = fld.Name
	}
	return names
}

// ExampleLinesOrderedCopyToFunction is the ordered variant of
// ExampleLinesCopyToFunction. SinkOrderDependent=true makes the extension use a
// single-thread sink, so the worker receives every batch in source order and
// writes the file in order. Mirrors vgi-python's
// ExampleLinesOrderedCopyToFunction.
type ExampleLinesOrderedCopyToFunction struct {
	ExampleLinesCopyToFunction
}

var _ vgi.CopyToFunction = (*ExampleLinesOrderedCopyToFunction)(nil)
var _ vgi.CopyToCommenter = (*ExampleLinesOrderedCopyToFunction)(nil)

func (f *ExampleLinesOrderedCopyToFunction) Name() string { return "example_lines_ordered_writer" }

func (f *ExampleLinesOrderedCopyToFunction) CopyToFormat() string { return "example_lines_ordered_out" }

func (f *ExampleLinesOrderedCopyToFunction) CopyToComment() string {
	return "Toy delimited-text writer (ordered, single-thread sink)"
}

func (f *ExampleLinesOrderedCopyToFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:        "Write the COPY source to a delimited file, preserving source order",
		Stability:          vgi.StabilityConsistent,
		Categories:         []string{"copy", "test"},
		Tags:               map[string]string{"category": "copy_to", "stability": "test"},
		SinkOrderDependent: true, // ordered COPY TO → single-thread sink
	}
}

// formatCell renders one Arrow array cell as a string, mirroring Python's
// str(value) with NULL → null_string. Covers the scalar types the COPY source
// columns might use.
func formatCell(col arrow.Array, row int, nullString string) string {
	if col.IsNull(row) {
		return nullString
	}
	switch a := col.(type) {
	case *array.String:
		return a.Value(row)
	case *array.LargeString:
		return a.Value(row)
	case *array.Boolean:
		return strconv.FormatBool(a.Value(row))
	case *array.Int8:
		return strconv.FormatInt(int64(a.Value(row)), 10)
	case *array.Int16:
		return strconv.FormatInt(int64(a.Value(row)), 10)
	case *array.Int32:
		return strconv.FormatInt(int64(a.Value(row)), 10)
	case *array.Int64:
		return strconv.FormatInt(a.Value(row), 10)
	case *array.Uint8:
		return strconv.FormatUint(uint64(a.Value(row)), 10)
	case *array.Uint16:
		return strconv.FormatUint(uint64(a.Value(row)), 10)
	case *array.Uint32:
		return strconv.FormatUint(uint64(a.Value(row)), 10)
	case *array.Uint64:
		return strconv.FormatUint(a.Value(row), 10)
	case *array.Float32:
		return strconv.FormatFloat(float64(a.Value(row)), 'g', -1, 32)
	case *array.Float64:
		return strconv.FormatFloat(a.Value(row), 'g', -1, 64)
	default:
		// Fall back to the Arrow value's default string rendering.
		return fmt.Sprintf("%v", col.GetOneForMarshal(row))
	}
}
