// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"encoding/binary"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// Arrow Go's MAP IPC reader rebuilds key/value fields from types, losing
// metadata. Recover the original field vectors before exposing the schema.
type ipcFieldMetadata struct {
	metadata arrow.Metadata
	children []ipcFieldMetadata
}

func restoreSchemaFieldMetadata(data []byte, schema *arrow.Schema) (result *arrow.Schema, err error) {
	defer func() {
		if failure := recover(); failure != nil {
			result = nil
			err = fmt.Errorf("invalid field metadata IPC: %v", failure)
		}
	}()
	u32 := func(p int) int { return int(binary.LittleEndian.Uint32(data[p : p+4])) }
	prefix := 4
	if u32(0) == 0xffffffff {
		prefix = 8
	}
	length := u32(prefix - 4)
	if length <= 0 || length > len(data)-prefix {
		return nil, fmt.Errorf("invalid schema metadata length")
	}
	data = data[prefix : prefix+length]
	slot := func(p, id int) int {
		v := p - int(int32(u32(p)))
		offset := 4 + 2*id
		if offset >= int(binary.LittleEndian.Uint16(data[v:v+2])) {
			return 0
		}
		return int(binary.LittleEndian.Uint16(data[v+offset : v+offset+2]))
	}
	ref := func(p, id int) int {
		o := slot(p, id)
		if o == 0 {
			return 0
		}
		return p + o + u32(p+o)
	}
	vector := func(p, id int, visit func(int)) {
		v := ref(p, id)
		if v == 0 {
			return
		}
		n := u32(v)
		if n > (len(data)-v-4)/4 {
			panic("invalid field vector")
		}
		for i := 0; i < n; i++ {
			q := v + 4 + i*4
			visit(q + u32(q))
		}
	}
	text := func(p, id int) string {
		v := ref(p, id)
		if v == 0 {
			return ""
		}
		n := u32(v)
		return string(data[v+4 : v+4+n])
	}
	var field func(int, int) ipcFieldMetadata
	field = func(p, depth int) ipcFieldMetadata {
		if depth > 64 {
			panic("field nesting too deep")
		}
		var keys, values []string
		vector(p, 6, func(m int) { keys = append(keys, text(m, 0)); values = append(values, text(m, 1)) })
		node := ipcFieldMetadata{metadata: arrow.NewMetadata(keys, values)}
		vector(p, 5, func(c int) { node.children = append(node.children, field(c, depth+1)) })
		return node
	}
	message := u32(0)
	root := ref(message, 2)
	var nodes []ipcFieldMetadata
	vector(root, 1, func(p int) { nodes = append(nodes, field(p, 0)) })
	if len(nodes) != schema.NumFields() {
		return nil, fmt.Errorf("schema field metadata shape mismatch")
	}
	fields := schema.Fields()
	for i := range fields {
		fields[i], err = restoreColumnField(fields[i], nodes[i])
		if err != nil {
			return nil, err
		}
	}
	metadata := schema.Metadata()
	return arrow.NewSchemaWithEndian(fields, &metadata, schema.Endianness()), nil
}
func restoreColumnField(field arrow.Field, node ipcFieldMetadata) (arrow.Field, error) {
	field.Metadata = node.metadata
	if _, ok := field.Type.(arrow.ExtensionType); ok {
		// Registered extensions own these two serialization keys. Match the
		// IPC reader's removal so re-encoding retains ordinary field metadata.
		var keys, values []string
		seen := make(map[string]bool)
		for i, key := range node.metadata.Keys() {
			if key == ipc.ExtensionTypeKeyName || key == ipc.ExtensionMetadataKeyName {
				if seen[key] {
					return field, fmt.Errorf("duplicate extension metadata key: %s", key)
				}
				seen[key] = true
				continue
			}
			keys = append(keys, key)
			values = append(values, node.metadata.Values()[i])
		}
		field.Metadata = arrow.NewMetadata(keys, values)
	}
	t, err := restoreColumnType(field.Type, node.children)
	field.Type = t
	return field, err
}
func restoreColumnType(t arrow.DataType, nodes []ipcFieldMetadata) (arrow.DataType, error) {
	if dictionary, ok := t.(*arrow.DictionaryType); ok {
		value, err := restoreColumnType(dictionary.ValueType, nodes)
		if err != nil {
			return nil, err
		}
		return &arrow.DictionaryType{IndexType: dictionary.IndexType, ValueType: value, Ordered: dictionary.Ordered}, nil
	}
	if extension, ok := t.(arrow.ExtensionType); ok {
		storage, err := restoreColumnType(extension.StorageType(), nodes)
		if err != nil {
			return nil, err
		}
		return extension.Deserialize(storage, extension.Serialize())
	}
	nested, ok := t.(arrow.NestedType)
	if !ok || len(nodes) == 0 {
		return t, nil
	}
	children := nested.Fields()
	if len(children) != len(nodes) {
		return nil, fmt.Errorf("nested field metadata shape mismatch")
	}
	var err error
	for i := range children {
		children[i], err = restoreColumnField(children[i], nodes[i])
		if err != nil {
			return nil, err
		}
	}
	switch value := t.(type) {
	case *arrow.StructType:
		return arrow.StructOf(children...), nil
	case *arrow.ListType:
		return arrow.ListOfField(children[0]), nil
	case *arrow.LargeListType:
		return arrow.LargeListOfField(children[0]), nil
	case *arrow.FixedSizeListType:
		return arrow.FixedSizeListOfField(value.Len(), children[0]), nil
	case *arrow.MapType:
		if children[0].Metadata.Len() != 0 {
			return nil, fmt.Errorf("arrow-go cannot preserve MAP entries field metadata")
		}
		pair := children[0].Type.(*arrow.StructType)
		out := arrow.MapOfFields(pair.Field(0), pair.Field(1))
		out.KeysSorted = value.KeysSorted
		return out, nil
	default:
		// Other Arrow nested types already retain their field metadata; do not
		// silently discard a repair required by a MAP nested inside one of them.
		for i, original := range nested.Fields() {
			if !original.Equal(children[i]) {
				return nil, fmt.Errorf("cannot preserve nested metadata in %s", t)
			}
		}
		return t, nil
	}
}
