// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"encoding/binary"
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
)

// TestFlatbuffersVtableObjectSize guards the producer dependency used by Arrow
// IPC. Matching field offsets alone cannot justify sharing a vtable: its object
// size must cover every inline field. See google/flatbuffers#9413 and #9414.
func TestFlatbuffersVtableObjectSize(t *testing.T) {
	builder := flatbuffers.NewBuilder(0)
	builder.StartObject(1)
	builder.PrependInt32Slot(0, 1, 0)
	small := builder.EndObject()
	builder.StartObject(1)
	builder.PrependInt64Slot(0, 2, 0)
	large := builder.EndObject()
	builder.Finish(large)

	for _, object := range []struct {
		name   string
		offset flatbuffers.UOffsetT
		width  int
	}{{"int32", small, 4}, {"int64", large, 8}} {
		t.Run(object.name, func(t *testing.T) {
			position := len(builder.Bytes) - int(object.offset)
			vtable := position - int(int32(binary.LittleEndian.Uint32(builder.Bytes[position:])))
			objectSize := int(binary.LittleEndian.Uint16(builder.Bytes[vtable+2:]))
			fieldOffset := int(binary.LittleEndian.Uint16(builder.Bytes[vtable+4:]))
			if fieldOffset+object.width > objectSize {
				t.Fatalf("field ends at %d beyond declared object size %d; use the documented FlatBuffers replacement", fieldOffset+object.width, objectSize)
			}
		})
	}
}
