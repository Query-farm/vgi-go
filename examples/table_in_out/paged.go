// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Continued-output fixtures: one input batch answered with several replies
// (vgi.WithMoreOutput + Resume). Their contracts are in the extension's test
// headers, which every SDK's fixture worker implements exactly so:
//
//   - blended_paged  — table_in_out/lateral_continuation.test
//   - blended_sliced — table_in_out/input_rows_per_call.test
//   - paged_explode  — table_in_out/continuation_classic.test
//
// fail_on_page, revalidate and not_modified_more are described in
// table_in_out/continuation_resume_error_revalidate.test. Mirrors vgi-python's
// BlendedPagedFunction / BlendedSlicedFunction / PagedExplodeFunction.
package table_in_out

import (
	"context"
	"fmt"
	"math"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// PagedState is where the next reply resumes: the input's fan-out counts and a
// position in them. It holds no output rows, so it stays small in an HTTP state
// token.
type PagedState struct {
	Counts     []int64
	Row        int64
	Offset     int64
	CallRows   int64 // blended_sliced: the input rows of the current call
	Page       int64 // replies sent for the current input
	EmptyFirst bool  // not_modified_more: the first reply is empty and says more
	Bump       int64 // added to every output value (revalidate: the data changed); -1 = one reply
}

// pagedArgSpecs are the named, bind-time arguments blended_paged and
// paged_explode share.
func pagedArgSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "page_size", Position: -1, ArrowType: "int64", Doc: "Output rows per reply",
			HasDefault: true, DefaultValue: "2"},
		{Name: "cache_ttl", Position: -1, ArrowType: "int64", Doc: "When > 0, advertise vgi.cache.ttl on the first reply",
			HasDefault: true, DefaultValue: "0"},
		{Name: "fail_on_page", Position: -1, ArrowType: "int64", Doc: "When > 0, fail instead of sending that (1-based) reply",
			HasDefault: true, DefaultValue: "0"},
		{Name: "revalidate", Position: -1, ArrowType: "boolean", Doc: "Answer conditional requests with changed, paged output",
			HasDefault: true, DefaultValue: "false"},
		{Name: "not_modified_more", Position: -1, ArrowType: "boolean",
			Doc:        "With revalidate, answer a conditional request with an empty not_modified reply that says more",
			HasDefault: true, DefaultValue: "false"},
	}
}

// checkPagedArgs is the bind-time validation every paged fixture shares.
func checkPagedArgs(args *vgi.Arguments, defaultPageSize int64) error {
	if v := vgi.OptionalInt64(args, "page_size", defaultPageSize); v < 1 {
		return fmt.Errorf("page_size must be at least 1, got %d", v)
	}
	if v := vgi.OptionalInt64(args, "cache_ttl", 0); v < 0 {
		return fmt.Errorf("cache_ttl must not be negative, got %d", v)
	}
	return nil
}

// pagedCounts reads an input batch's fan-out counts from col (NULL or negative
// counts as 0).
func pagedCounts(col arrow.Array) []int64 {
	counts := make([]int64, col.Len())
	for i := range counts {
		if !col.IsNull(i) {
			counts[i] = max(vgi.GetInt64Value(col, i), 0)
		}
	}
	return counts
}

// pagedBegin starts a new input for blended_paged / paged_explode and returns
// its first reply's cache control.
//
// revalidate makes the output depend on the request alone. A plain request gets
// the whole output in one reply, cacheable but immediately stale (ttl 0, etag
// v1, revalidatable), so the client stores it and revalidates every repeat. A
// conditional request (the client sends if_none_match) is told the data
// changed: every value is 1000 larger, the etag is v2, and the output is paged
// by page_size. A client must then return the new pages and store nothing,
// since that output continued.
//
// not_modified_more (with revalidate) breaks the protocol on purpose: a
// conditional request is answered with an empty not_modified reply (etag v1)
// that also says more, then the changed pages. A 304 stands for an input's
// whole stored output, so a client must refuse it rather than serve its entry.
func pagedBegin(params *vgi.ProcessParams, state *PagedState, counts []int64) *vgi.CacheControl {
	*state = PagedState{Counts: counts}
	if !vgi.OptionalBool(params.Args, "revalidate", false) {
		if ttl := vgi.OptionalInt64(params.Args, "cache_ttl", 0); ttl > 0 {
			return &vgi.CacheControl{Ttl: vgi.Seconds(ttl)}
		}
		return nil
	}
	if params.IfNoneMatch == nil {
		state.Bump = -1 // one reply: the whole output
		return &vgi.CacheControl{Ttl: vgi.Seconds(0), ETag: "v1", Revalidatable: true}
	}
	state.Bump = 1000
	if vgi.OptionalBool(params.Args, "not_modified_more", false) {
		state.EmptyFirst = true
		return &vgi.CacheControl{NotModified: true, Ttl: vgi.Seconds(0), ETag: "v1", Revalidatable: true}
	}
	return &vgi.CacheControl{Ttl: vgi.Seconds(0), ETag: "v2", Revalidatable: true}
}

// pagedNext is the next reply of blended_paged / paged_explode, or the failure
// fail_on_page asks for. Replies are counted per input batch.
func pagedNext(params *vgi.ProcessParams, state *PagedState, pageSize int64) ([]int64, []int32, bool, error) {
	state.Page++
	if state.Page == vgi.OptionalInt64(params.Args, "fail_on_page", 0) {
		return nil, nil, false, fmt.Errorf("failing on page %d as asked", state.Page)
	}
	if state.Page == 1 && state.EmptyFirst {
		return nil, nil, true, nil
	}
	if state.Bump < 0 {
		vals, parents, more := nextPage(state, math.MaxInt64)
		return vals, parents, more, nil
	}
	vals, parents, more := nextPage(state, pageSize)
	for i := range vals {
		vals[i] += state.Bump
	}
	return vals, parents, more, nil
}

// nextPage takes the next pageSize rows of the output (rows 0..n-1 for each
// input row in order, so a page may span input rows) and reports whether more
// follow. Input rows with no output after the last non-empty page are skipped,
// so the last page is never followed by an empty one.
func nextPage(state *PagedState, pageSize int64) ([]int64, []int32, bool) {
	var vals []int64
	var parents []int32
	rows := int64(len(state.Counts))
	for state.Row < rows && int64(len(vals)) < pageSize {
		take := min(state.Counts[state.Row]-state.Offset, pageSize-int64(len(vals)))
		for i := int64(0); i < take; i++ {
			vals = append(vals, state.Offset+i)
			parents = append(parents, int32(state.Row))
		}
		state.Offset += take
		if state.Offset == state.Counts[state.Row] {
			state.Row++
			state.Offset = 0
		}
	}
	for state.Row < rows && state.Counts[state.Row] == 0 {
		state.Row++
	}
	return vals, parents, state.Row < rows
}

// int64Batch builds a batch of int64 columns, one per name, all of length n.
func int64Batch(names []string, cols ...[]int64) arrow.RecordBatch {
	mem := memory.NewGoAllocator()
	fields := make([]arrow.Field, len(names))
	arrs := make([]arrow.Array, len(names))
	for i, name := range names {
		fields[i] = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64}
		b := array.NewInt64Builder(mem)
		b.AppendValues(cols[i], nil)
		arrs[i] = b.NewArray()
		b.Release()
	}
	defer func() {
		for _, a := range arrs {
			a.Release()
		}
	}()
	n := int64(0)
	if len(cols) > 0 {
		n = int64(len(cols[0]))
	}
	return array.NewRecordBatch(arrow.NewSchema(fields, nil), arrs, n)
}

// ---------------------------------------------------------------------------
// blended_paged(n [, page_size, cache_ttl, fail_on_page, revalidate,
// not_modified_more]) — blended_explode's output, page_size rows per reply
// ---------------------------------------------------------------------------

// BlendedPagedFunction is a blended 1->N fan-out returned across several
// replies, page_size rows each. Same output as blended_explode (rows 0..n-1 per
// input row, with parent-row provenance), but one input batch's output is split
// into pages: Process emits the first with vgi.WithMoreOutput and Resume the
// rest, the way a function backed by a paged API would.
type BlendedPagedFunction struct{}

var (
	_ vgi.TypedTableInOutFunc[PagedState]    = (*BlendedPagedFunction)(nil)
	_ vgi.TypedTableInOutResumer[PagedState] = (*BlendedPagedFunction)(nil)
)

func (f *BlendedPagedFunction) Name() string { return "blended_paged" }

func (f *BlendedPagedFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description:   "Blended 1->N fan-out (emit 0..n-1 per input row), page_size rows per reply",
		Stability:     vgi.StabilityConsistent,
		Categories:    []string{"blended", "test"},
		InputFromArgs: true,
	}
}

func (f *BlendedPagedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return append([]vgi.ArgSpec{
		{Name: "n", Position: 0, ArrowType: "int64", Doc: "Fan-out count: emit rows 0..n-1 for this input row"},
	}, pagedArgSpecs()...)
}

func (f *BlendedPagedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	if err := checkPagedArgs(params.Args, 2); err != nil {
		return nil, err
	}
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int64}}, nil),
	}, nil
}

func (f *BlendedPagedFunction) NewState(params *vgi.ProcessParams) (*PagedState, error) {
	return &PagedState{}, nil
}

func (f *BlendedPagedFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *PagedState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	cc := pagedBegin(params, state, pagedCounts(batch.Column(0)))
	return f.emitPage(params, state, out, cc)
}

func (f *BlendedPagedFunction) Resume(ctx context.Context, params *vgi.ProcessParams, state *PagedState, out *vgirpc.OutputCollector) error {
	return f.emitPage(params, state, out, nil)
}

func (f *BlendedPagedFunction) emitPage(params *vgi.ProcessParams, state *PagedState, out *vgirpc.OutputCollector, cc *vgi.CacheControl) error {
	vals, parents, more, err := pagedNext(params, state, vgi.OptionalInt64(params.Args, "page_size", 2))
	if err != nil {
		return err
	}
	return vgi.EmitParentRows(out, int64Batch([]string{"i"}, vals), parents,
		vgi.WithCacheControl(cc), vgi.WithMoreOutput(more))
}

func (f *BlendedPagedFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *PagedState) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewBlendedPagedFunction creates a BlendedPagedFunction wrapped for registration.
func NewBlendedPagedFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[PagedState](&BlendedPagedFunction{})
}

// ---------------------------------------------------------------------------
// blended_sliced(n [, page_size, cache_ttl]) — input_rows_per_call = 3
// ---------------------------------------------------------------------------

// blendedSlicedRowsPerCall is blended_sliced's declared InputRowsPerCall.
const blendedSlicedRowsPerCall = 3

// BlendedSlicedFunction is blended_paged that takes at most 3 input rows per
// call, and says how many it got. It declares InputRowsPerCall = 3, so the
// client must split every input chunk into calls of at most 3 rows; each output
// row carries call_rows, the row count of the call that produced it, so a test
// can see the split. With cache_ttl > 0 each call's first reply advertises
// per-value caching, so the client stores each call's values for reuse.
type BlendedSlicedFunction struct{}

var (
	_ vgi.TypedTableInOutFunc[PagedState]    = (*BlendedSlicedFunction)(nil)
	_ vgi.TypedTableInOutResumer[PagedState] = (*BlendedSlicedFunction)(nil)
)

func (f *BlendedSlicedFunction) Name() string { return "blended_sliced" }

func (f *BlendedSlicedFunction) Metadata() vgi.FunctionMetadata {
	rowsPerCall := int64(blendedSlicedRowsPerCall)
	return vgi.FunctionMetadata{
		Description:      "Blended 1->N fan-out taking at most 3 input rows per call, reporting each call's size",
		Stability:        vgi.StabilityConsistent,
		Categories:       []string{"blended", "test"},
		InputFromArgs:    true,
		InputRowsPerCall: &rowsPerCall,
	}
}

func (f *BlendedSlicedFunction) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "n", Position: 0, ArrowType: "int64", Doc: "Fan-out count: emit rows 0..n-1 for this input row"},
		{Name: "page_size", Position: -1, ArrowType: "int64", Doc: "Output rows per reply",
			HasDefault: true, DefaultValue: "1000000"},
		{Name: "cache_ttl", Position: -1, ArrowType: "int64", Doc: "If > 0, per-value cacheable for this many seconds",
			HasDefault: true, DefaultValue: "0"},
	}
}

func (f *BlendedSlicedFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	if err := checkPagedArgs(params.Args, 1_000_000); err != nil {
		return nil, err
	}
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{
			{Name: "i", Type: arrow.PrimitiveTypes.Int64},
			{Name: "call_rows", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}, nil
}

func (f *BlendedSlicedFunction) NewState(params *vgi.ProcessParams) (*PagedState, error) {
	return &PagedState{}, nil
}

func (f *BlendedSlicedFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *PagedState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	if batch.NumRows() > blendedSlicedRowsPerCall {
		return fmt.Errorf("blended_sliced declares input_rows_per_call = %d but got %d rows",
			blendedSlicedRowsPerCall, batch.NumRows())
	}
	*state = PagedState{Counts: pagedCounts(batch.Column(0)), CallRows: batch.NumRows()}
	var cc *vgi.CacheControl
	if ttl := vgi.OptionalInt64(params.Args, "cache_ttl", 0); ttl > 0 {
		cc = &vgi.CacheControl{Ttl: vgi.Seconds(ttl), PerValue: true}
	}
	return f.emitPage(params, state, out, cc)
}

func (f *BlendedSlicedFunction) Resume(ctx context.Context, params *vgi.ProcessParams, state *PagedState, out *vgirpc.OutputCollector) error {
	return f.emitPage(params, state, out, nil)
}

func (f *BlendedSlicedFunction) emitPage(params *vgi.ProcessParams, state *PagedState, out *vgirpc.OutputCollector, cc *vgi.CacheControl) error {
	vals, parents, more := nextPage(state, vgi.OptionalInt64(params.Args, "page_size", 1_000_000))
	callRows := make([]int64, len(vals))
	for i := range callRows {
		callRows[i] = state.CallRows
	}
	return vgi.EmitParentRows(out, int64Batch([]string{"i", "call_rows"}, vals, callRows), parents,
		vgi.WithCacheControl(cc), vgi.WithMoreOutput(more))
}

func (f *BlendedSlicedFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *PagedState) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewBlendedSlicedFunction creates a BlendedSlicedFunction wrapped for registration.
func NewBlendedSlicedFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[PagedState](&BlendedSlicedFunction{})
}

// ---------------------------------------------------------------------------
// paged_explode(data [, page_size, cache_ttl, fail_on_page, revalidate,
// not_modified_more]) — classic (TABLE-input) continued output
// ---------------------------------------------------------------------------

// PagedExplodeFunction is a classic (TABLE-input) table-in-out continuing its
// output across replies. For each input row with count n (the table's first
// column), it emits rows i = 0..n-1, page_size per reply, with more on every
// reply but an input batch's last. The classic counterpart of blended_paged: it
// runs through the extension's stock in-out path, on a per-substream
// connection.
type PagedExplodeFunction struct{}

var (
	_ vgi.TypedTableInOutFunc[PagedState]    = (*PagedExplodeFunction)(nil)
	_ vgi.TypedTableInOutResumer[PagedState] = (*PagedExplodeFunction)(nil)
)

func (f *PagedExplodeFunction) Name() string { return "paged_explode" }

func (f *PagedExplodeFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Classic table-in-out 1->N fan-out, page_size rows per reply",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"table_in_out", "test"},
	}
}

func (f *PagedExplodeFunction) ArgumentSpecs() []vgi.ArgSpec {
	return append([]vgi.ArgSpec{
		{Name: "data", Position: 0, ArrowType: "table", Doc: "Input table; its first column is the fan-out count"},
	}, pagedArgSpecs()...)
}

func (f *PagedExplodeFunction) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	if err := checkPagedArgs(params.Args, 2); err != nil {
		return nil, err
	}
	return &vgi.BindResponse{
		OutputSchema: arrow.NewSchema([]arrow.Field{{Name: "i", Type: arrow.PrimitiveTypes.Int64}}, nil),
	}, nil
}

func (f *PagedExplodeFunction) NewState(params *vgi.ProcessParams) (*PagedState, error) {
	return &PagedState{}, nil
}

func (f *PagedExplodeFunction) Process(ctx context.Context, params *vgi.ProcessParams, state *PagedState, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	var counts []int64
	if batch.NumCols() > 0 {
		counts = pagedCounts(batch.Column(0))
	}
	return f.emitPage(params, state, out, pagedBegin(params, state, counts))
}

func (f *PagedExplodeFunction) Resume(ctx context.Context, params *vgi.ProcessParams, state *PagedState, out *vgirpc.OutputCollector) error {
	return f.emitPage(params, state, out, nil)
}

func (f *PagedExplodeFunction) emitPage(params *vgi.ProcessParams, state *PagedState, out *vgirpc.OutputCollector, cc *vgi.CacheControl) error {
	vals, _, more, err := pagedNext(params, state, vgi.OptionalInt64(params.Args, "page_size", 2))
	if err != nil {
		return err
	}
	return vgi.Emit(out, int64Batch([]string{"i"}, vals), vgi.WithCacheControl(cc), vgi.WithMoreOutput(more))
}

func (f *PagedExplodeFunction) Finalize(ctx context.Context, params *vgi.ProcessParams, state *PagedState) ([]arrow.RecordBatch, error) {
	return nil, nil
}

// NewPagedExplodeFunction creates a PagedExplodeFunction wrapped for registration.
func NewPagedExplodeFunction() vgi.TableInOutFunction {
	return vgi.AsTableInOutFunction[PagedState](&PagedExplodeFunction{})
}
