// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// SchemaPath identifies a schema by its components from root to leaf.
type SchemaPath = []string

func singleSchemaPath(name string) SchemaPath {
	if name == "" {
		return nil
	}
	return SchemaPath{name}
}

func schemaPathKey(path SchemaPath) string {
	parts := make([]string, len(path))
	for i, part := range path {
		parts[i] = strings.ToLower(part)
	}
	return strings.Join(parts, "\x00")
}

func schemaPathDisplay(path SchemaPath) string {
	return strings.Join(path, ".")
}

func schemaPathOrMain(path SchemaPath) SchemaPath {
	if len(path) == 0 {
		return SchemaPath{"main"}
	}
	return path
}

func appendSchemaPath(builder *array.ListBuilder, path SchemaPath) {
	builder.Append(true)
	values := builder.ValueBuilder().(*array.StringBuilder)
	values.AppendValues(path, nil)
}

func appendOptionalSchemaPath(builder *array.ListBuilder, path *SchemaPath) {
	if path == nil {
		builder.AppendNull()
		return
	}
	appendSchemaPath(builder, *path)
}
