// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

var copyFromFormatInfoSchema = generated.CopyFromFormatInfoSchema

// SerializeCopyFromFormatInfo serializes one copy-from/copy-to format record to
// IPC bytes matching CopyFromFormatInfoSchema (comment, tags, format_name,
// handler, options, direction, description, ordered). The options field carries the IPC-serialized
// Arrow argument schema built from the handler's ArgSpecs — the same encoding as
// FunctionInfo.arguments — so option type/default/doc surface identically to
// vgi_function_arguments().
func SerializeCopyFromFormatInfo(rec copyFromFormatRecord) ([]byte, error) {
	mem := memory.NewGoAllocator()

	// comment (nullable)
	commentBuilder := array.NewStringBuilder(mem)
	defer commentBuilder.Release()
	if rec.comment != "" {
		commentBuilder.Append(rec.comment)
	} else {
		commentBuilder.AppendNull()
	}

	// tags (map<string,string>)
	tagsBuilder := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
	defer tagsBuilder.Release()
	tagsBuilder.Append(true)
	if len(rec.tags) > 0 {
		kb := tagsBuilder.KeyBuilder().(*array.StringBuilder)
		vb := tagsBuilder.ItemBuilder().(*array.StringBuilder)
		for k, v := range rec.tags {
			kb.Append(k)
			vb.Append(v)
		}
	}

	// format_name
	formatNameBuilder := array.NewStringBuilder(mem)
	defer formatNameBuilder.Release()
	formatNameBuilder.Append(rec.formatName)

	// handler
	handlerBuilder := array.NewStringBuilder(mem)
	defer handlerBuilder.Release()
	handlerBuilder.Append(rec.handler)

	// options (binary: serialized Arrow argument schema)
	optionsBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer optionsBuilder.Release()
	argBytes, err := SerializeSchema(BuildArgSchema(rec.argSpecs))
	if err != nil {
		return nil, err
	}
	optionsBuilder.Append(argBytes)

	// direction
	directionBuilder := array.NewStringBuilder(mem)
	defer directionBuilder.Release()
	direction := rec.direction
	if direction == "" {
		direction = CopyFromDirectionFrom
	}
	directionBuilder.Append(direction)

	// description
	descBuilder := array.NewStringBuilder(mem)
	defer descBuilder.Release()
	descBuilder.Append(rec.description)

	// ordered (bool): COPY ... TO writers that need source order set this; the
	// C++ extension maps it to a single-thread sink. Always false for FROM.
	orderedBuilder := array.NewBooleanBuilder(mem)
	defer orderedBuilder.Release()
	orderedBuilder.Append(rec.ordered)

	cols := []arrow.Array{
		commentBuilder.NewArray(),
		tagsBuilder.NewArray(),
		formatNameBuilder.NewArray(),
		handlerBuilder.NewArray(),
		optionsBuilder.NewArray(),
		directionBuilder.NewArray(),
		descBuilder.NewArray(),
		orderedBuilder.NewArray(),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(copyFromFormatInfoSchema, cols, 1)
	defer batch.Release()

	var buf bytes.Buffer
	wtr := ipc.NewWriter(&buf, ipc.WithSchema(copyFromFormatInfoSchema))
	if err := wtr.Write(batch); err != nil {
		return nil, err
	}
	if err := wtr.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// CopyBothDirections marks a format served by one handler in both directions.
// The extension wires the reader and the writer onto a single DuckDB
// CopyFunction when it sees this.
const CopyBothDirections = "both"

// recordCopyFormat appends rec to w.copyFromFormats, reconciling it with any
// format already registered under the same name.
//
// The extension's COPY registry is keyed by `<attach-alias>.<format>` with NO
// direction component, and on a hit it silently skips the entry — it reads a hit
// as an idempotent re-ATTACH. Two records sharing a format name therefore lose
// one of them without a word, and the loser only shows up as
// "COPY TO is not supported for FORMAT ..." much later.
//
// A record carries a single handler and a single option schema, so one name can
// only serve both directions when ONE handler serves both. That case is merged
// here into direction="both". Anything else is a genuine ambiguity the wire
// format cannot express, and is rejected at registration rather than silently
// half-working.
func (w *Worker) recordCopyFormat(rec copyFromFormatRecord) {
	for i := range w.copyFromFormats {
		existing := &w.copyFromFormats[i]
		if existing.formatName != rec.formatName {
			continue
		}
		if existing.direction == rec.direction {
			// Re-registering the identical pair is a no-op; a second handler
			// claiming the same name and direction is a conflict.
			if existing.handler == rec.handler {
				return
			}
			panic(fmt.Sprintf(
				"vgi: COPY format %q is already registered for direction %q by handler %q; "+
					"handler %q cannot also claim it — give one of them a different format name",
				rec.formatName, rec.direction, existing.handler, rec.handler))
		}
		if existing.handler != rec.handler {
			panic(fmt.Sprintf(
				"vgi: COPY format %q is registered by handler %q (%s) and handler %q (%s). "+
					"The extension keys COPY functions by name alone and would silently drop one of them. "+
					"Give each direction its own format name (e.g. %q and %q), or serve both from one handler",
				rec.formatName, existing.handler, existing.direction, rec.handler, rec.direction,
				rec.formatName, rec.formatName+"_out"))
		}
		// One handler, both directions: exactly what direction="both" is for.
		existing.direction = CopyBothDirections
		if existing.comment == "" {
			existing.comment = rec.comment
		}
		existing.ordered = existing.ordered || rec.ordered
		return
	}
	w.copyFromFormats = append(w.copyFromFormats, rec)
}
