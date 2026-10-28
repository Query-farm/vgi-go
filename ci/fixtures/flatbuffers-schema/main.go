// Command flatbuffers-schema writes the Arrow IPC schema that reproduces
// google/flatbuffers#9413: dictionary encodings surrounding field metadata.
package main

import (
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func main() {
	if err := writeSchema(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func writeSchema() error {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "first", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint8, ValueType: arrow.BinaryTypes.String}},
		{Name: "plain", Type: arrow.PrimitiveTypes.Int32, Metadata: arrow.NewMetadata([]string{"key"}, []string{"value"})},
		{Name: "second", Type: &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint16, ValueType: arrow.BinaryTypes.String}},
	}, nil)
	writer := ipc.NewWriter(os.Stdout, ipc.WithSchema(schema))
	return writer.Close()
}
