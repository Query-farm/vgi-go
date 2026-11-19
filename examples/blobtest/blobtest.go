// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package blobtest is the `blob_test` fixture catalog: functions sized by
// their arguments, for the vgi extension's blob-vendor externalization tests
// (test/sql/external_storage/). EXAMPLE/TEST FUNCTIONS ONLY.
//
// Over HTTP with VGI_BLOB_VENDOR_URL set, results over
// VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES come back as vendor pointers, requests
// over VGI_RPC_MAX_REQUEST_BYTES go up through vendor upload URLs, and (with
// VGI_STORAGE_EXTERNALIZE=1) storage values over VGI_STORAGE_EXTERNALIZE_BYTES
// live in the vendor.
//
//	blob_repeat(s, n)        scalar           large output: s repeated n times
//	blob_len(s)              scalar           large input: strlen(s) in bytes
//	blob_rows(rows, width)   table            large output: rows x width chars
//	blob_param_echo(p)       table            large bind parameter: length + sha256
//	blob_buffer_echo(t)      table-buffering  stores each input batch, replays it
//	blob_tio(t)              table-in-out     stores each batch, reads it back
//	blob_store_values(t)     table-in-out     stores each value of column 0 exactly
//
// The checksum columns are payload_len (UTF-8 byte length) and payload_sha256
// (lowercase hex SHA-256) of a value as text, matching DuckDB's strlen(x) and
// sha256(x). The storage functions compute them from what storage returns,
// never from their input.
//
// The SDK-neutral contract is the vgi extension's
// docs/testing/blob-test-catalog.md; vgi-python's
// vgi/_test_fixtures/blob_catalog.py is the reference implementation.
package blobtest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// CatalogName is the catalog the blob_test functions live under.
const CatalogName = "blob_test"

const rowsPerBatch = 2048

var checksumFields = []arrow.Field{
	{Name: "payload_len", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "payload_sha256", Type: arrow.BinaryTypes.String, Nullable: true},
}

// Register wires the seven functions onto w, scoped to the blob_test catalog.
// The caller must also pass vgi.WithCatalogAliases(CatalogName) to NewWorker
// so the catalog is ATTACHable.
func Register(w *vgi.Worker) {
	w.RegisterScalarForCatalog(CatalogName, &BlobRepeatFunction{})
	w.RegisterScalarForCatalog(CatalogName, &BlobLenFunction{})
	w.RegisterTableForCatalog(CatalogName, vgi.AsTableFunction[blobRowsState](&BlobRowsFunction{}))
	w.RegisterTableForCatalog(CatalogName, vgi.AsTableFunction[paramEchoState](&BlobParamEchoFunction{}))
	w.RegisterTableBufferingForCatalog(CatalogName, &BlobBufferEchoFunction{})
	w.RegisterTableInOutForCatalog(CatalogName, vgi.AsTableInOutFunction[struct{}](&BlobTioFunction{}))
	w.RegisterTableInOutForCatalog(CatalogName, vgi.AsTableInOutFunction[struct{}](&BlobStoreValuesFunction{}))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// textValues returns column as text: nil for NULL, the UTF-8 bytes otherwise.
// Non-string columns are cast to string first.
func textValues(col arrow.Array) ([][]byte, error) {
	out := make([][]byte, col.Len())
	switch c := col.(type) {
	case *array.String:
		for i := range out {
			if c.IsValid(i) {
				out[i] = []byte(c.Value(i))
			}
		}
	case *array.LargeString:
		for i := range out {
			if c.IsValid(i) {
				out[i] = []byte(c.Value(i))
			}
		}
	case *array.StringView:
		for i := range out {
			if c.IsValid(i) {
				out[i] = []byte(c.Value(i))
			}
		}
	default:
		cast, err := compute.CastArray(context.Background(), col, compute.SafeCastOptions(arrow.BinaryTypes.String))
		if err != nil {
			return nil, fmt.Errorf("cannot cast %s to text: %w", col.DataType(), err)
		}
		defer cast.Release()
		return textValues(cast)
	}
	return out, nil
}

// checksumArrays builds payload_len / payload_sha256 for values (nil = NULL).
func checksumArrays(values [][]byte) (arrow.Array, arrow.Array) {
	lens := array.NewInt64Builder(memory.DefaultAllocator)
	defer lens.Release()
	digests := array.NewStringBuilder(memory.DefaultAllocator)
	defer digests.Release()
	for _, v := range values {
		if v == nil {
			lens.AppendNull()
			digests.AppendNull()
			continue
		}
		sum := sha256.Sum256(v)
		lens.Append(int64(len(v)))
		digests.Append(hex.EncodeToString(sum[:]))
	}
	return lens.NewArray(), digests.NewArray()
}

// withChecksums returns columns + the checksum columns of values.
func withChecksums(schema *arrow.Schema, columns []arrow.Array, values [][]byte, rows int64) arrow.RecordBatch {
	lens, digests := checksumArrays(values)
	defer lens.Release()
	defer digests.Release()
	cols := append(append([]arrow.Array(nil), columns...), lens, digests)
	return array.NewRecordBatch(schema, cols, rows)
}

// batchWithChecksums appends the checksum columns of batch's first column.
func batchWithChecksums(schema *arrow.Schema, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	if batch.NumCols() == 0 {
		return nil, errors.New("blob_test: the input table has no columns")
	}
	values, err := textValues(batch.Column(0))
	if err != nil {
		return nil, err
	}
	return withChecksums(schema, batch.Columns(), values, batch.NumRows()), nil
}

func checksumSchema(fields []arrow.Field) *arrow.Schema {
	return arrow.NewSchema(append(append([]arrow.Field(nil), fields...), checksumFields...), nil)
}

func stringAt(col arrow.Array, i int) (string, bool) {
	if col.IsNull(i) {
		return "", false
	}
	switch c := col.(type) {
	case *array.String:
		return c.Value(i), true
	case *array.LargeString:
		return c.Value(i), true
	case *array.StringView:
		return c.Value(i), true
	}
	return vgi.GetStringValue(col, i), true
}

func newKey() []byte {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return b[:]
}

func tableArg() []vgi.ArgSpec {
	return []vgi.ArgSpec{{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"}}
}

// ---------------------------------------------------------------------------
// Scalars
// ---------------------------------------------------------------------------

// BlobRepeatFunction is blob_repeat(s, n): s repeated n times (large outputs).
type BlobRepeatFunction struct{}

func (*BlobRepeatFunction) Name() string { return "blob_repeat" }

func (*BlobRepeatFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Repeat a string n times (externalization test: large scalar output)",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.BinaryTypes.LargeString,
		Examples:    []vgi.CatalogExample{{SQL: "SELECT blob_repeat('ab', 3)", Description: "Returns 'ababab'"}},
	}
}

func (*BlobRepeatFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "s", Position: 0, ArrowType: "varchar", Doc: "String to repeat"},
		{Name: "n", Position: 1, ArrowType: "int64", Doc: "Number of copies"},
	}
}

func (*BlobRepeatFunction) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.BinaryTypes.LargeString)
}

// Process repeats each s n times (n < 0 counts as 0); NULL in either gives NULL.
func (*BlobRepeatFunction) Process(_ context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	s, n := batch.Column(0), batch.Column(1)
	b := array.NewLargeStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	for i := 0; i < int(batch.NumRows()); i++ {
		str, ok := stringAt(s, i)
		if !ok || n.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(strings.Repeat(str, int(max(vgi.GetInt64Value(n, i), 0))))
	}
	out := b.NewArray()
	defer out.Release()
	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{out}, batch.NumRows()), nil
}

// BlobLenFunction is blob_len(s): the UTF-8 byte length of s (large inputs).
type BlobLenFunction struct{}

func (*BlobLenFunction) Name() string { return "blob_len" }

func (*BlobLenFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "UTF-8 byte length of a string (externalization test: large scalar input)",
		Stability:   vgi.StabilityConsistent,
		ReturnType:  arrow.PrimitiveTypes.Int64,
		Examples:    []vgi.CatalogExample{{SQL: "SELECT blob_len('abc')", Description: "Returns 3"}},
	}
}

func (*BlobLenFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{{Name: "s", Position: 0, ArrowType: "varchar", Doc: "String to measure"}}
}

func (*BlobLenFunction) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(arrow.PrimitiveTypes.Int64)
}

// Process returns strlen(s) for each row.
func (*BlobLenFunction) Process(_ context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	s := batch.Column(0)
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	for i := 0; i < int(batch.NumRows()); i++ {
		str, ok := stringAt(s, i)
		if !ok {
			b.AppendNull()
			continue
		}
		b.Append(int64(len(str)))
	}
	out := b.NewArray()
	defer out.Release()
	return array.NewRecordBatch(params.OutputSchema, []arrow.Array{out}, batch.NumRows()), nil
}

// ---------------------------------------------------------------------------
// Table functions
// ---------------------------------------------------------------------------

var blobRowsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "i", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

type blobRowsArgs struct {
	Rows  int64 `vgi:"pos=0,ge=0,doc=Number of rows"`
	Width int64 `vgi:"pos=1,ge=0,doc=Characters per row"`
}

// BlobRowsFunction is blob_rows(rows, width): row i is
// repeat(chr(97 + i % 26), width) (large table outputs).
type BlobRowsFunction struct{}

type blobRowsState struct {
	vgi.BatchState
	Width int64
}

var _ vgi.TypedTableFunc[blobRowsState] = (*BlobRowsFunction)(nil)

func (*BlobRowsFunction) Name() string { return "blob_rows" }

func (*BlobRowsFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Rows of a given width (externalization test: large table output)",
		Stability:   vgi.StabilityConsistent,
		Examples:    []vgi.CatalogExample{{SQL: "SELECT * FROM blob_rows(3, 4)", Description: "aaaa, bbbb, cccc"}},
	}
}

func (*BlobRowsFunction) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(blobRowsArgs{}) }

func (*BlobRowsFunction) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(blobRowsSchema)
}

// Cardinality is exactly rows.
func (*BlobRowsFunction) Cardinality(params *vgi.BindParams) (*vgi.TableCardinality, error) {
	var args blobRowsArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &vgi.TableCardinality{Estimate: args.Rows, Max: args.Rows}, nil
}

func (*BlobRowsFunction) NewState(params *vgi.ProcessParams) (*blobRowsState, error) {
	var args blobRowsArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	return &blobRowsState{BatchState: vgi.NewBatchState(max(args.Rows, 0), rowsPerBatch), Width: max(args.Width, 0)}, nil
}

// Process emits up to 2,048 rows per tick, in order of i.
func (*BlobRowsFunction) Process(_ context.Context, _ *vgi.ProcessParams, state *blobRowsState, out *vgirpc.OutputCollector) error {
	start, width := state.Index, int(state.Width)
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		ids := vgi.BuildInt64Array(size, func(j int64) int64 { return start + j })
		payloads := vgi.BuildStringArray(size, func(j int64) string {
			return strings.Repeat(string(rune(97+(start+j)%26)), width)
		})
		return []arrow.Array{ids, payloads}, nil
	})
}

var paramEchoSchema = checksumSchema(nil)

// BlobParamEchoFunction is blob_param_echo(payload): one row with the
// parameter's byte length and SHA-256 (large bind parameters).
type BlobParamEchoFunction struct{}

type paramEchoState struct {
	Emitted bool
}

var _ vgi.TypedTableFunc[paramEchoState] = (*BlobParamEchoFunction)(nil)

func (*BlobParamEchoFunction) Name() string { return "blob_param_echo" }

func (*BlobParamEchoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Length and SHA-256 of a parameter (externalization test: large bind parameter)",
		Stability:   vgi.StabilityConsistent,
		Examples:    []vgi.CatalogExample{{SQL: "SELECT * FROM blob_param_echo('abc')", Description: "3, sha256('abc')"}},
	}
}

func (*BlobParamEchoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{{Name: "payload", Position: 0, ArrowType: "varchar", IsConst: true,
		ArrowDataType: arrow.BinaryTypes.String, Doc: "Parameter to measure"}}
}

func (*BlobParamEchoFunction) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(paramEchoSchema)
}

// Cardinality is one row.
func (*BlobParamEchoFunction) Cardinality(*vgi.BindParams) (*vgi.TableCardinality, error) {
	return &vgi.TableCardinality{Estimate: 1, Max: 1}, nil
}

func (*BlobParamEchoFunction) NewState(*vgi.ProcessParams) (*paramEchoState, error) {
	return &paramEchoState{}, nil
}

// Process emits the one row, then finishes.
func (*BlobParamEchoFunction) Process(_ context.Context, params *vgi.ProcessParams, state *paramEchoState, out *vgirpc.OutputCollector) error {
	if state.Emitted {
		return out.Finish()
	}
	payload, err := params.Args.GetScalarString(0)
	if err != nil {
		return err
	}
	state.Emitted = true
	lens, digests := checksumArrays([][]byte{[]byte(payload)})
	defer lens.Release()
	defer digests.Release()
	return out.EmitArrays([]arrow.Array{lens, digests}, 1)
}

// ---------------------------------------------------------------------------
// Table-buffering and table-in-out
// ---------------------------------------------------------------------------

var bufferNS = []byte("blob_buffer")

// BlobBufferEchoFunction is blob_buffer_echo(t): process stores each input
// batch (an Arrow IPC stream) as one value of the execution's log; finalize
// replays them with the checksum columns of their first column.
type BlobBufferEchoFunction struct{}

var _ vgi.TableBufferingFunction = (*BlobBufferEchoFunction)(nil)

func (*BlobBufferEchoFunction) Name() string { return "blob_buffer_echo" }

func (*BlobBufferEchoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Buffer input batches in storage and replay them (externalization test: storage values)",
		Stability:   vgi.StabilityConsistent,
		Examples: []vgi.CatalogExample{{
			SQL:         "SELECT * FROM blob_buffer_echo((SELECT repeat('x', 10) AS s))",
			Description: "s, payload_len, payload_sha256",
		}},
	}
}

func (*BlobBufferEchoFunction) ArgumentSpecs() []vgi.ArgSpec { return tableArg() }

func (*BlobBufferEchoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	if params.InputSchema == nil {
		return nil, errors.New("blob_buffer_echo requires a table input")
	}
	return vgi.BindSchema(checksumSchema(params.InputSchema.Fields()))
}

// Process appends the batch to the execution's log.
func (*BlobBufferEchoFunction) Process(_ context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) ([]byte, error) {
	data, err := vgi.SerializeRecordBatch(batch)
	if err != nil {
		return nil, err
	}
	if _, err := params.Storage.StateAppend(bufferNS, data); err != nil {
		return nil, err
	}
	return params.ExecutionID, nil
}

// Combine yields one replay stream per execution.
func (*BlobBufferEchoFunction) Combine(_ context.Context, params *vgi.ProcessParams, _ [][]byte) ([][]byte, error) {
	return [][]byte{params.ExecutionID}, nil
}

// Finalize replays every stored batch, as read back, with its checksums.
func (*BlobBufferEchoFunction) Finalize(_ context.Context, params *vgi.ProcessParams, _ []byte) ([]arrow.RecordBatch, error) {
	entries, err := params.Storage.StateLogScan(bufferNS, -1, 0)
	if err != nil {
		return nil, err
	}
	out := make([]arrow.RecordBatch, 0, len(entries))
	for _, e := range entries {
		stored, err := vgi.DeserializeRecordBatch(e.Value)
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		b, err := batchWithChecksums(params.OutputSchema, stored)
		stored.Release()
		if err != nil {
			releaseAll(out)
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

func releaseAll(batches []arrow.RecordBatch) {
	for _, b := range batches {
		b.Release()
	}
}

var tioNS = []byte("blob_tio")

// BlobTioFunction is blob_tio(t): each input batch is stored (an Arrow IPC
// stream) under a fresh key, read straight back, deleted, and emitted as read
// back with the checksum columns of its first column.
type BlobTioFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*BlobTioFunction)(nil)

func (*BlobTioFunction) Name() string { return "blob_tio" }

func (*BlobTioFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Round-trip each batch through storage (externalization test: table-in-out state)",
		Stability:   vgi.StabilityConsistent,
		Examples: []vgi.CatalogExample{{
			SQL:         "SELECT * FROM blob_tio((SELECT repeat('x', 10) AS s))",
			Description: "s, payload_len, payload_sha256",
		}},
	}
}

func (*BlobTioFunction) ArgumentSpecs() []vgi.ArgSpec { return tableArg() }

func (*BlobTioFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	if params.InputSchema == nil {
		return nil, errors.New("blob_tio requires a table input")
	}
	return vgi.BindSchema(checksumSchema(params.InputSchema.Fields()))
}

func (*BlobTioFunction) NewState(*vgi.ProcessParams) (*struct{}, error) { return &struct{}{}, nil }

// Process stores the batch, reads it back, deletes it and emits what was read.
func (*BlobTioFunction) Process(_ context.Context, params *vgi.ProcessParams, _ *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	store, err := params.Storage.AttachStore(params.ExecutionID)
	if err != nil {
		return err
	}
	data, err := vgi.SerializeRecordBatch(batch)
	if err != nil {
		return err
	}
	key := newKey()
	if err := store.Put(tioNS, key, data); err != nil {
		return err
	}
	stored, err := store.Get(tioNS, key)
	if err != nil {
		return err
	}
	if stored == nil {
		return errors.New("blob_tio: the batch just stored is missing")
	}
	if err := store.DeleteKey(tioNS, key); err != nil {
		return err
	}
	back, err := vgi.DeserializeRecordBatch(stored)
	if err != nil {
		return err
	}
	defer back.Release()
	result, err := batchWithChecksums(params.OutputSchema, back)
	if err != nil {
		return err
	}
	defer result.Release()
	return out.Emit(result)
}

func (*BlobTioFunction) Finalize(context.Context, *vgi.ProcessParams, *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

var valuesNS = []byte("blob_values")

// BlobStoreValuesFunction is blob_store_values(t): each non-NULL value of the
// first column (as text) is stored as its own storage value of exactly that
// size, all are read back and deleted, and the output is input columns 2..n
// plus the checksum columns of the bytes read back (NULL for a NULL input,
// which stores nothing).
type BlobStoreValuesFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*BlobStoreValuesFunction)(nil)

func (*BlobStoreValuesFunction) Name() string { return "blob_store_values" }

func (*BlobStoreValuesFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Store each value of the first column exactly and read it back (externalization test)",
		Stability:   vgi.StabilityConsistent,
		Examples: []vgi.CatalogExample{{
			SQL:         "SELECT * FROM blob_store_values((SELECT repeat('x', 10) AS s, 1 AS id))",
			Description: "id, payload_len, payload_sha256",
		}},
	}
}

func (*BlobStoreValuesFunction) ArgumentSpecs() []vgi.ArgSpec { return tableArg() }

func (*BlobStoreValuesFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	if params.InputSchema == nil || params.InputSchema.NumFields() == 0 {
		return nil, errors.New("blob_store_values requires a table input with at least one column")
	}
	return vgi.BindSchema(checksumSchema(params.InputSchema.Fields()[1:]))
}

func (*BlobStoreValuesFunction) NewState(*vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

// Process stores every non-NULL value under its own key, reads them all
// back, deletes them, and emits their lengths and SHA-256s.
func (*BlobStoreValuesFunction) Process(_ context.Context, params *vgi.ProcessParams, _ *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	values, err := textValues(batch.Column(0))
	if err != nil {
		return err
	}
	store, err := params.Storage.AttachStore(params.ExecutionID)
	if err != nil {
		return err
	}
	prefix := newKey()
	keys := make([][]byte, len(values))
	for i, v := range values {
		if v == nil {
			continue
		}
		keys[i] = binary.BigEndian.AppendUint32(append([]byte(nil), prefix...), uint32(i))
		if err := store.Put(valuesNS, keys[i], v); err != nil {
			return err
		}
	}
	back := make([][]byte, len(values))
	for i, v := range values {
		if v == nil {
			continue
		}
		got, err := store.Get(valuesNS, keys[i])
		if err != nil {
			return err
		}
		if got == nil {
			return errors.New("blob_store_values: a value just stored is missing")
		}
		back[i] = got
	}
	for i, v := range values {
		if v != nil {
			if err := store.DeleteKey(valuesNS, keys[i]); err != nil {
				return err
			}
		}
	}
	result := withChecksums(params.OutputSchema, batch.Columns()[1:], back, batch.NumRows())
	defer result.Release()
	return out.Emit(result)
}

func (*BlobStoreValuesFunction) Finalize(context.Context, *vgi.ProcessParams, *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}
