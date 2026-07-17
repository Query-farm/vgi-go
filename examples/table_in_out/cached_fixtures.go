// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Exchange-mode result-cache fixtures (see cache/exchange_*.test):
//
//   - cached_double       — cacheable blended 1->1 map (LATERAL exchange cache, M2)
//   - cached_echo         — cacheable classic TABLE-input passthrough (streaming, M1)
//   - cached_sum_all      — cacheable buffered reducer (buffered exchange cache, M3)
//   - cached_reval_echo   — classic passthrough with the always-revalidate (304) contract
//   - cached_reval_double — blended map with the always-revalidate (304) contract
//
// Mirrors vgi-python's CachedDoubleFunction / CachedEchoFunction /
// CachedSumAllColumnsFunction / CachedRevalidatingEchoFunction /
// CachedRevalidatingDoubleFunction.
package table_in_out

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// doubledBatch renders {doubled: x*2} from the input batch's first column.
func doubledBatch(batch arrow.RecordBatch) arrow.RecordBatch {
	col := batch.Column(0)
	n := int(batch.NumRows())
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for row := 0; row < n; row++ {
		if col.IsNull(row) {
			b.AppendNull()
		} else {
			b.Append(vgi.GetInt64Value(col, row) * 2)
		}
	}
	arr := b.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "doubled", Type: arrow.PrimitiveTypes.Int64}}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{arr}, int64(n))
}

// contentEtag derives a stable etag from a batch's content (deterministic
// across runs for equal data): the first 16 hex chars of a sha256 over each
// column's type and rendered values. Hashing values (not the IPC bytes) keeps
// the etag stable when the transport attaches per-batch custom metadata (e.g.
// the conditional-request validators themselves). The client only ever echoes
// it back verbatim, so it need not match other SDKs' etag derivations — only
// be stable here.
func contentEtag(batch arrow.RecordBatch) string {
	h := sha256.New()
	for i := 0; i < int(batch.NumCols()); i++ {
		col := batch.Column(i)
		_, _ = fmt.Fprintf(h, "%s|%s;", col.DataType().String(), col.String())
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// ---------------------------------------------------------------------------
// cached_double(x) — cacheable blended map
// ---------------------------------------------------------------------------

// CachedDoubleFunction is a cacheable blended 1->1 map (x -> x*2) advertising
// vgi.cache.*. Backs exchange-mode result-cache tests on BOTH call shapes
// served by the same registration: the streaming column form
// FROM t, cached_double(t.x) and the correlated form LATERAL cached_double(t.x)
// (routed through the batched LATERAL operator's per-chunk memoization).
// Advertises a ttl on every output batch (the C++ side latches the first).
type CachedDoubleFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*CachedDoubleFunction)(nil)

func (f *CachedDoubleFunction) Name() string { return "cached_double" }

func (f *CachedDoubleFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:   "Cacheable blended map x -> x*2 (advertises vgi.cache.ttl)",
		Stability:     vgi.StabilityConsistent,
		Categories:    []string{"blended", "cache", "test"},
		InputFromArgs: true,
	}
}

func (f *CachedDoubleFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "x", Position: 0, ArrowType: "int64", Doc: "Input column"},
	}
}

func (f *CachedDoubleFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "doubled", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}, nil
}

func (f *CachedDoubleFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

func (f *CachedDoubleFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	result := doubledBatch(batch)
	return vgi.Emit(out, result, vgi.WithCacheControl(&vgi.CacheControl{Ttl: vgi.Seconds(300)}))
}

func (f *CachedDoubleFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewCachedDoubleFunction creates a CachedDoubleFunction wrapped for registration.
func NewCachedDoubleFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&CachedDoubleFunction{})
}

// ---------------------------------------------------------------------------
// cached_echo(data) — cacheable classic (TABLE-input) passthrough
// ---------------------------------------------------------------------------

// CachedEchoFunction is a cacheable CLASSIC (TABLE-input) streaming
// table-in-out passthrough. Called as FROM cached_echo((SELECT ... FROM t)) —
// a NON-correlated table-in-out routed through the streaming exchange (M1
// per-input-batch memoization), unlike the blended column/LATERAL forms which
// decorrelate to the batched-LATERAL operator (M2). Passthrough output (input
// schema) advertising a ttl on each output batch.
type CachedEchoFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*CachedEchoFunction)(nil)

func (f *CachedEchoFunction) Name() string { return "cached_echo" }

func (f *CachedEchoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Cacheable classic (TABLE-input) passthrough (advertises vgi.cache.ttl)",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"cache", "test"},
	}
}

func (f *CachedEchoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"},
	}
}

func (f *CachedEchoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindInputSchema(params)
}

func (f *CachedEchoFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

func (f *CachedEchoFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	return vgi.Emit(out, batch, vgi.WithCacheControl(&vgi.CacheControl{Ttl: vgi.Seconds(300)}))
}

func (f *CachedEchoFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewCachedEchoFunction creates a CachedEchoFunction wrapped for registration.
func NewCachedEchoFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&CachedEchoFunction{})
}

// ---------------------------------------------------------------------------
// cached_sum_all(data) — cacheable buffered reducer
// ---------------------------------------------------------------------------

// CachedSumAllColumnsFunction is a cacheable BUFFERED whole-input reducer
// (column-wise sum) advertising vgi.cache.*. It reuses SumAllColumnsFunction's
// Sink+Combine accumulation and declares Metadata().CacheControl, which the
// framework attaches to its finalize output — backing the exchange-mode
// buffered result cache (M3). A repeat query with the same input multiset (any
// order) replays the cached single-row result and skips the combine +
// finalize-drain on the worker (the Sink ingestion still runs — the key is
// only known after all input is folded).
type CachedSumAllColumnsFunction struct {
	SumAllColumnsFunction
}

var _ vgi.TableBufferingFunction = (*CachedSumAllColumnsFunction)(nil)

func (f *CachedSumAllColumnsFunction) Name() string { return "cached_sum_all" }

func (f *CachedSumAllColumnsFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:  "Cacheable column-wise sum across all input (advertises vgi.cache.ttl)",
		Stability:    vgi.StabilityConsistent,
		Categories:   []string{"aggregation", "cache", "test"},
		CacheControl: &vgi.CacheControl{Ttl: vgi.Seconds(300)},
	}
}

// ---------------------------------------------------------------------------
// cached_reval_echo(data) — classic passthrough, always-revalidate (304)
// ---------------------------------------------------------------------------

// CachedRevalidatingEchoFunction is a classic (TABLE-input) passthrough with
// the always-revalidate (304) contract. It advertises CacheControl{Ttl: 0,
// ETag, Revalidatable} on its output — the "no-cache" semantic: stored but
// immediately stale, so every repeat sends a conditional request
// (vgi.cache.if_none_match). On a matching validator the worker answers with a
// 0-row CacheControl{NotModified: true} batch and the C++ side reuses the
// stored bytes instead of re-streaming. The etag is derived from the input
// content so it is stable across identical repeats. Backs the streaming
// exchange-cache revalidation test (M1).
type CachedRevalidatingEchoFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*CachedRevalidatingEchoFunction)(nil)

func (f *CachedRevalidatingEchoFunction) Name() string { return "cached_reval_echo" }

func (f *CachedRevalidatingEchoFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Classic passthrough with always-revalidate (304 not_modified) contract",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"cache", "test"},
	}
}

func (f *CachedRevalidatingEchoFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table"},
	}
}

func (f *CachedRevalidatingEchoFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindInputSchema(params)
}

func (f *CachedRevalidatingEchoFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

func (f *CachedRevalidatingEchoFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	etag := contentEtag(batch)
	if params.IfNoneMatch != nil && *params.IfNoneMatch == etag {
		// 304 Not Modified: the client's stored copy for this input is still valid.
		empty := batch.NewSlice(0, 0)
		return vgi.Emit(out, empty, vgi.WithCacheControl(&vgi.CacheControl{
			NotModified: true, Ttl: vgi.Seconds(0), ETag: etag, Revalidatable: true,
		}))
	}
	return vgi.Emit(out, batch, vgi.WithCacheControl(&vgi.CacheControl{
		Ttl: vgi.Seconds(0), ETag: etag, Revalidatable: true,
	}))
}

func (f *CachedRevalidatingEchoFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewCachedRevalidatingEchoFunction creates a CachedRevalidatingEchoFunction
// wrapped for registration.
func NewCachedRevalidatingEchoFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&CachedRevalidatingEchoFunction{})
}

// ---------------------------------------------------------------------------
// cached_reval_double(x) — blended map, always-revalidate (304)
// ---------------------------------------------------------------------------

// CachedRevalidatingDoubleFunction is a blended map (x -> x*2) with the
// always-revalidate (304) contract. Like CachedRevalidatingEchoFunction but
// blended, so it exercises the LATERAL exchange-cache revalidation path (M2).
// The etag is derived from the worker-input content (the positional arg) —
// stable across identical repeats. On a matching if_none_match it answers
// 0-row not_modified; the LATERAL operator then slides the stored POST-STAMP
// entry's TTL and replays it. Called FROM t, LATERAL cached_reval_double(t.x).
type CachedRevalidatingDoubleFunction struct{}

var _ vgi.TypedTableInOutFunc[struct{}] = (*CachedRevalidatingDoubleFunction)(nil)

func (f *CachedRevalidatingDoubleFunction) Name() string { return "cached_reval_double" }

func (f *CachedRevalidatingDoubleFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:   "Blended map x->x*2 with always-revalidate (304 not_modified) contract",
		Stability:     vgi.StabilityConsistent,
		Categories:    []string{"blended", "cache", "test"},
		InputFromArgs: true,
	}
}

func (f *CachedRevalidatingDoubleFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "x", Position: 0, ArrowType: "int64", Doc: "Input column"},
	}
}

func (f *CachedRevalidatingDoubleFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "doubled", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}, nil
}

func (f *CachedRevalidatingDoubleFunction) NewState(params *vgi.ProcessParams) (*struct{}, error) {
	return &struct{}{}, nil
}

func (f *CachedRevalidatingDoubleFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	etag := contentEtag(batch)
	if params.IfNoneMatch != nil && *params.IfNoneMatch == etag {
		empty := vgi.EmptyBatch(params.OutputSchema)
		return vgi.Emit(out, empty, vgi.WithCacheControl(&vgi.CacheControl{
			NotModified: true, Ttl: vgi.Seconds(0), ETag: etag, Revalidatable: true,
		}))
	}
	result := doubledBatch(batch)
	return vgi.Emit(out, result, vgi.WithCacheControl(&vgi.CacheControl{
		Ttl: vgi.Seconds(0), ETag: etag, Revalidatable: true,
	}))
}

func (f *CachedRevalidatingDoubleFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewCachedRevalidatingDoubleFunction creates a CachedRevalidatingDoubleFunction
// wrapped for registration.
func NewCachedRevalidatingDoubleFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[struct{}](&CachedRevalidatingDoubleFunction{})
}
