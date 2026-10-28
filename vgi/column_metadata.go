// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"encoding/json"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

// ValidateColumnMetadata validates reserved field metadata without rewriting it.
// Absent and present-empty values remain distinct; unrelated metadata is opaque.
func ValidateColumnMetadata(schema *arrow.Schema) error {
	if schema == nil {
		return nil
	}
	for _, field := range schema.Fields() {
		if err := validateColumnField(field); err != nil {
			return err
		}
	}
	return nil
}
func validateColumnField(field arrow.Field) error {
	seen := map[string]bool{}
	for i, key := range field.Metadata.Keys() {
		if seen[key] {
			return fmt.Errorf("duplicate column metadata key %q", key)
		}
		seen[key] = true
		value := field.Metadata.Values()[i]
		if key != "comment" && key != "tags" {
			continue
		}
		if !utf8.ValidString(value) {
			return fmt.Errorf("column %q %s metadata must be UTF-8", field.Name, key)
		}
		if key == "tags" {
			if err := validateColumnTags(value); err != nil {
				return fmt.Errorf("column %q tags: %w", field.Name, err)
			}
		}
	}
	return validateColumnType(field.Type)
}
func validateColumnType(t arrow.DataType) error {
	if extension, ok := t.(arrow.ExtensionType); ok {
		return validateColumnType(extension.StorageType())
	}
	if dictionary, ok := t.(*arrow.DictionaryType); ok {
		return validateColumnType(dictionary.ValueType)
	}
	if nested, ok := t.(arrow.NestedType); ok {
		for _, child := range nested.Fields() {
			if err := validateColumnField(child); err != nil {
				return err
			}
		}
	}
	return nil
}
func validateColumnTags(value string) error {
	// encoding/json replaces unpaired escaped surrogates. Reject them before decoding.
	inString := false
	for i := 0; i < len(value); i++ {
		if value[i] == '"' {
			inString = !inString
			continue
		}
		if !inString || value[i] != '\\' {
			continue
		}
		i++
		if i >= len(value) {
			break
		}
		if value[i] != 'u' {
			continue
		}
		if i+4 >= len(value) {
			return fmt.Errorf("invalid JSON unicode escape")
		}
		code, err := strconv.ParseUint(value[i+1:i+5], 16, 16)
		if err != nil {
			return err
		}
		i += 4
		if code >= 0xD800 && code <= 0xDBFF {
			if i+6 >= len(value) || value[i+1:i+3] != "\\u" {
				return fmt.Errorf("unpaired JSON surrogate")
			}
			low, err := strconv.ParseUint(value[i+3:i+7], 16, 16)
			if err != nil || low < 0xDC00 || low > 0xDFFF {
				return fmt.Errorf("unpaired JSON surrogate")
			}
			i += 6
		} else if code >= 0xDC00 && code <= 0xDFFF {
			return fmt.Errorf("unpaired JSON surrogate")
		}
	}
	decoder := json.NewDecoder(strings.NewReader(value))
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return fmt.Errorf("expected a JSON object of strings")
	}
	seen := map[string]bool{}
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return fmt.Errorf("duplicate or invalid tag key")
		}
		seen[key] = true
		token, err = decoder.Token()
		if err != nil {
			return err
		}
		if _, ok := token.(string); !ok {
			return fmt.Errorf("tag values must be strings")
		}
	}
	token, err = decoder.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('}') {
		return fmt.Errorf("invalid tag object")
	}
	if _, err = decoder.Token(); err != io.EOF {
		return fmt.Errorf("trailing JSON content")
	}
	return nil
}
