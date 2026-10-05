// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// AttachOptionSpec describes an ATTACH-time option the worker accepts.
// Mirrors vgi-python's AttachOptionSpec: wire format is the same as
// SettingSpec so DuckDB can parse both with shared code.
//
// DefaultBatch, when non-nil, must be a single-row RecordBatch whose only
// column (name "value") has type Type. Use this to carry defaults for types
// like list/struct/decimal/date/time/timestamp that are awkward to express
// as Go scalars. Callers that only need primitive defaults can use
// BuildDefaultValueBatch.
//
// Required marks an option the caller must supply at ATTACH time. A catalog
// that cannot be attached without it advertises that at discovery, so a client
// can say so before attempting the attach rather than surfacing a failure that
// reads like an empty catalog. It is mutually exclusive with a default: an
// option that falls back to a value is by definition satisfiable without the
// caller.
//
// Secret marks an option whose value is a credential. Credential options (API
// keys, tokens, passwords) MUST be declared Secret. Clients and the DuckDB
// extension mask a secret option's value, keep it out of cache keys (the
// extension keys results on a salted HMAC of the value, never the plain text),
// out of logs and telemetry, and out of exported or shared configuration. The
// extension can also supply a secret option from a `vgi_attach` DuckDB secret
// scoped to the worker, so the credential never appears in the ATTACH text:
//
//	CREATE SECRET (TYPE vgi_attach, SCOPE 'https://worker.example.com', api_key '...');
//	ATTACH 'https://worker.example.com' AS w (TYPE vgi);
//
// Secret combines freely with Required (a credential the catalog cannot be
// attached without). It is allowed with a default, but a secret option
// normally has none: a default credential would be published to every client
// in the discovery listing.
type AttachOptionSpec struct {
	Name         string
	Description  string
	Type         arrow.DataType
	DefaultBatch arrow.RecordBatch
	Required     bool
	Secret       bool
}

var attachOptionSpecSchema = arrow.NewSchema([]arrow.Field{
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "description", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "type", Type: arrow.BinaryTypes.Binary, Nullable: false},
	{Name: "default_value", Type: arrow.BinaryTypes.Binary, Nullable: true},
	// Nullable and appended LAST: a peer that predates this column reads the
	// batch by name and simply doesn't see it. Absent and explicit-null both
	// mean "not required".
	{Name: "required", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	// Appended after "required" on the same terms: readers look it up by
	// name, and an absent or null column means "not secret".
	{Name: "secret", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
}, nil)

// serializeAttachOptionSpec serializes an AttachOptionSpec to Arrow IPC bytes.
func serializeAttachOptionSpec(spec AttachOptionSpec) ([]byte, error) {
	if spec.Type == nil {
		return nil, fmt.Errorf("attach option %q: Type must not be nil", spec.Name)
	}
	// Mirrors AttachOptionSpec.__post_init__ on the Python side: an option that
	// falls back to a value is always satisfiable without the caller, so the
	// combination is a declaration bug rather than a runtime condition.
	if spec.Required && spec.DefaultBatch != nil {
		return nil, fmt.Errorf(
			"attach option %q is required but also declares a default; an option with a "+
				"default is always satisfiable without the caller; drop one", spec.Name)
	}
	mem := memory.NewGoAllocator()

	typeSchema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: spec.Type}}, nil)
	typeBytes, err := SerializeSchema(typeSchema)
	if err != nil {
		return nil, fmt.Errorf("serializing attach option type: %w", err)
	}

	var defaultBytes []byte
	if spec.DefaultBatch != nil {
		defaultBytes, err = SerializeRecordBatch(spec.DefaultBatch)
		if err != nil {
			return nil, fmt.Errorf("serializing attach option default: %w", err)
		}
	}

	nameB := array.NewStringBuilder(mem)
	defer nameB.Release()
	nameB.Append(spec.Name)

	descB := array.NewStringBuilder(mem)
	defer descB.Release()
	descB.Append(spec.Description)

	typeB := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer typeB.Release()
	typeB.Append(typeBytes)

	defB := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer defB.Release()
	if defaultBytes != nil {
		defB.Append(defaultBytes)
	} else {
		defB.AppendNull()
	}

	reqB := array.NewBooleanBuilder(mem)
	defer reqB.Release()
	// Written explicitly rather than left null so a reader sees false, not
	// NULL, for an option that simply isn't required.
	reqB.Append(spec.Required)

	secB := array.NewBooleanBuilder(mem)
	defer secB.Release()
	secB.Append(spec.Secret)

	cols := []arrow.Array{
		nameB.NewArray(), descB.NewArray(), typeB.NewArray(), defB.NewArray(), reqB.NewArray(), secB.NewArray(),
	}
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()

	batch := array.NewRecordBatch(attachOptionSpecSchema, cols, 1)
	defer batch.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(attachOptionSpecSchema))
	if err := w.Write(batch); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DeserializeAttachOptionSpec reads an AttachOptionSpec from the one-row Arrow
// IPC batch produced by the worker's attach-option discovery listing.
// Columns are looked up by name, so a batch from an older peer that lacks the
// trailing "required" or "secret" column (or carries it as null) reads as
// false. The returned DefaultBatch, when non-nil, is owned by the caller and
// must be released.
func DeserializeAttachOptionSpec(data []byte) (AttachOptionSpec, error) {
	var spec AttachOptionSpec
	r, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return spec, fmt.Errorf("reading attach option spec: %w", err)
	}
	defer r.Release()
	if !r.Next() {
		if err := r.Err(); err != nil {
			return spec, fmt.Errorf("reading attach option spec: %w", err)
		}
		return spec, fmt.Errorf("attach option spec batch is empty")
	}
	batch := r.RecordBatch()
	if batch.NumRows() != 1 {
		return spec, fmt.Errorf("attach option spec batch has %d rows, expected 1", batch.NumRows())
	}
	column := func(name string) arrow.Array {
		idx := batch.Schema().FieldIndices(name)
		if len(idx) == 0 {
			return nil
		}
		return batch.Column(idx[0])
	}
	flag := func(name string) (bool, error) {
		col := column(name)
		if col == nil || col.IsNull(0) {
			return false, nil
		}
		b, ok := col.(*array.Boolean)
		if !ok {
			return false, fmt.Errorf("attach option spec column %q has type %s, expected bool", name, col.DataType())
		}
		return b.Value(0), nil
	}

	name, ok := column("name").(*array.String)
	if !ok {
		return spec, fmt.Errorf("attach option spec has no string `name` column")
	}
	spec.Name = name.Value(0)
	if desc, ok := column("description").(*array.String); ok && !desc.IsNull(0) {
		spec.Description = desc.Value(0)
	}
	typeCol, ok := column("type").(*array.Binary)
	if !ok || typeCol.IsNull(0) {
		return spec, fmt.Errorf("attach option %q: spec has no binary `type` column", spec.Name)
	}
	typeSchema, err := DeserializeSchema(typeCol.Value(0))
	if err != nil {
		return spec, fmt.Errorf("attach option %q type: %w", spec.Name, err)
	}
	if typeSchema.NumFields() != 1 {
		return spec, fmt.Errorf("attach option %q type schema has %d fields, expected 1", spec.Name, typeSchema.NumFields())
	}
	spec.Type = typeSchema.Field(0).Type
	if spec.Required, err = flag("required"); err != nil {
		return spec, err
	}
	if spec.Secret, err = flag("secret"); err != nil {
		return spec, err
	}
	if def, ok := column("default_value").(*array.Binary); ok && !def.IsNull(0) {
		spec.DefaultBatch, err = DeserializeRecordBatch(def.Value(0))
		if err != nil {
			return spec, fmt.Errorf("attach option %q default: %w", spec.Name, err)
		}
	}
	return spec, nil
}

// MissingAttachOptionsError reports an ATTACH that omitted options declared
// Required. Missing carries the names so a caller can act on them without
// parsing the message. Mirrors Python's MissingAttachOptionsError, message
// included — the extension's integration suite matches on its text.
type MissingAttachOptionsError struct {
	CatalogName string
	Missing     []string
}

// Error renders the missing option names, matching the Python and Go
// implementations' message text.
func (e *MissingAttachOptionsError) Error() string {
	// Single-quoted, not %q: Python renders these names with repr() and Java
	// with explicit single quotes, and the shared integration test
	// (attach/attach_options_required.test) matches on that text.
	quoted := make([]string, len(e.Missing))
	for i, name := range e.Missing {
		quoted[i] = "'" + name + "'"
	}
	plural := ""
	if len(e.Missing) > 1 {
		plural = "s"
	}
	return fmt.Sprintf("Catalog '%s' cannot be attached without the required option%s %s.",
		e.CatalogName, plural, strings.Join(quoted, ", "))
}

// suppliedAttachOptionNames reads the option keys out of the serialized
// options batch. Each supplied option is a column of a single-row batch, so
// the column names are the keys; nil or empty bytes mean none were supplied.
func suppliedAttachOptionNames(optionsIPC []byte) (map[string]struct{}, error) {
	supplied := map[string]struct{}{}
	if len(optionsIPC) == 0 {
		return supplied, nil
	}
	r, err := ipc.NewReader(bytes.NewReader(optionsIPC))
	if err != nil {
		return nil, fmt.Errorf("reading attach options: %w", err)
	}
	defer r.Release()
	for _, f := range r.Schema().Fields() {
		supplied[strings.ToLower(f.Name)] = struct{}{}
	}
	return supplied, nil
}

// validateRequiredAttachOptions returns a *MissingAttachOptionsError when a
// spec marked Required has no corresponding entry in the supplied options.
// Names are matched case-insensitively, mirroring DuckDB's handling of ATTACH
// option keys.
func validateRequiredAttachOptions(catalogName string, specs []AttachOptionSpec, optionsIPC []byte) error {
	needed := make([]string, 0, len(specs))
	for _, spec := range specs {
		if spec.Required {
			needed = append(needed, spec.Name)
		}
	}
	if len(needed) == 0 {
		return nil
	}
	supplied, err := suppliedAttachOptionNames(optionsIPC)
	if err != nil {
		return err
	}
	var missing []string
	for _, name := range needed {
		if _, ok := supplied[strings.ToLower(name)]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return &MissingAttachOptionsError{CatalogName: catalogName, Missing: missing}
	}
	return nil
}
