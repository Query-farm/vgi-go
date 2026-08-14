// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"strings"
	"testing"
)

// Two handlers claiming one format name is an ambiguity the wire format cannot
// express: a record carries a single handler and a single option schema, and the
// extension's registry is keyed by name with no direction component. Left
// unchecked, one of the two is silently dropped and only surfaces later as
// "COPY TO is not supported for FORMAT ...".
func TestRecordCopyFormatRejectsCrossDirectionCollision(t *testing.T) {
	w := NewWorker()
	w.recordCopyFormat(copyFromFormatRecord{
		formatName: "widgets", handler: "read_widgets", direction: CopyFromDirectionFrom,
	})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected a panic when two handlers claim one format name")
		}
		msg, _ := r.(string)
		for _, want := range []string{"read_widgets", "write_widgets", "widgets_out"} {
			if !strings.Contains(msg, want) {
				t.Errorf("panic message should mention %q; got: %s", want, msg)
			}
		}
	}()
	w.recordCopyFormat(copyFromFormatRecord{
		formatName: "widgets", handler: "write_widgets", direction: CopyToDirectionTo,
	})
}

// One handler serving both directions is the case direction="both" exists for,
// and the extension wires the reader and writer onto a single CopyFunction.
func TestRecordCopyFormatMergesOneHandlerServingBoth(t *testing.T) {
	w := NewWorker()
	w.recordCopyFormat(copyFromFormatRecord{
		formatName: "widgets", handler: "widgets", direction: CopyFromDirectionFrom,
	})
	w.recordCopyFormat(copyFromFormatRecord{
		formatName: "widgets", handler: "widgets", direction: CopyToDirectionTo, ordered: true,
	})

	if len(w.copyFromFormats) != 1 {
		t.Fatalf("want one merged record, got %d", len(w.copyFromFormats))
	}
	rec := w.copyFromFormats[0]
	if rec.direction != CopyBothDirections {
		t.Errorf("direction = %q, want %q", rec.direction, CopyBothDirections)
	}
	if !rec.ordered {
		t.Error("the writer's ordered flag should survive the merge")
	}
}

// The recommended shape: a distinct name per direction.
func TestRecordCopyFormatKeepsDistinctNamesApart(t *testing.T) {
	w := NewWorker()
	w.recordCopyFormat(copyFromFormatRecord{
		formatName: "widgets", handler: "read_widgets", direction: CopyFromDirectionFrom,
	})
	w.recordCopyFormat(copyFromFormatRecord{
		formatName: "widgets_out", handler: "write_widgets", direction: CopyToDirectionTo,
	})
	if len(w.copyFromFormats) != 2 {
		t.Fatalf("want two records, got %d", len(w.copyFromFormats))
	}
}

// Registering the identical handler and direction twice is a no-op, not a
// conflict — re-registration happens in tests and in composed workers.
func TestRecordCopyFormatIsIdempotent(t *testing.T) {
	w := NewWorker()
	rec := copyFromFormatRecord{formatName: "widgets", handler: "read_widgets", direction: CopyFromDirectionFrom}
	w.recordCopyFormat(rec)
	w.recordCopyFormat(rec)
	if len(w.copyFromFormats) != 1 {
		t.Fatalf("want one record, got %d", len(w.copyFromFormats))
	}
}
