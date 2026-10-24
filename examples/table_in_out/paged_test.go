// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package table_in_out

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The paged fixtures' paging logic, against the contracts in the extension's
// table_in_out/{lateral_continuation,continuation_classic,
// continuation_resume_error_revalidate}.test headers.

type page struct {
	vals    []int64
	parents []int32
	more    bool
}

// allPages runs nextPage to the end of one input batch's output.
func allPages(counts []int64, pageSize int64) []page {
	state := &PagedState{Counts: counts}
	var pages []page
	for {
		vals, parents, more := nextPage(state, pageSize)
		pages = append(pages, page{vals, parents, more})
		if !more || len(pages) > 100 {
			return pages
		}
	}
}

func TestNextPage(t *testing.T) {
	for _, tc := range []struct {
		name     string
		counts   []int64
		pageSize int64
		want     []page
	}{
		{"pages span input rows", []int64{3, 2}, 2, []page{
			{[]int64{0, 1}, []int32{0, 0}, true},
			{[]int64{2, 0}, []int32{0, 1}, true},
			{[]int64{1}, []int32{1}, false},
		}},
		{"exact fit: no empty last page", []int64{2, 2}, 2, []page{
			{[]int64{0, 1}, []int32{0, 0}, true},
			{[]int64{0, 1}, []int32{1, 1}, false},
		}},
		{"rows with no output are skipped", []int64{0, 2, 0, 0, 1}, 2, []page{
			{[]int64{0, 1}, []int32{1, 1}, true},
			{[]int64{0}, []int32{4}, false},
		}},
		{"trailing rows with no output add no reply", []int64{2, 0, 0}, 2, []page{
			{[]int64{0, 1}, []int32{0, 0}, false},
		}},
		{"a batch with no output is one empty reply", []int64{0, 0}, 2, []page{{nil, nil, false}}},
		{"an empty batch is one empty reply", nil, 2, []page{{nil, nil, false}}},
		{"page larger than the output", []int64{1, 2}, 10, []page{
			{[]int64{0, 0, 1}, []int32{0, 1, 1}, false},
		}},
		{"page size 1", []int64{2, 1}, 1, []page{
			{[]int64{0}, []int32{0}, true},
			{[]int64{1}, []int32{0}, true},
			{[]int64{0}, []int32{1}, false},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := allPages(tc.counts, tc.pageSize); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("pages = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPagedCountsNullAndNegativeAreZero(t *testing.T) {
	b := array.NewInt64Builder(memory.NewGoAllocator())
	defer b.Release()
	b.AppendValues([]int64{2, 0, -3, 4}, []bool{true, true, true, true})
	b.AppendNull()
	col := b.NewArray()
	defer col.Release()
	if got, want := pagedCounts(col), []int64{2, 0, 0, 4, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pagedCounts = %v, want %v", got, want)
	}
}

// pagedArgs builds the fixtures' named arguments; unset ones take their
// defaults.
func pagedArgs(named map[string]any) *vgi.Arguments {
	args := &vgi.Arguments{Named: map[string]arrow.Array{}}
	mem := memory.NewGoAllocator()
	for name, v := range named {
		switch v := v.(type) {
		case int64:
			b := array.NewInt64Builder(mem)
			b.Append(v)
			args.Named[name] = b.NewArray()
			b.Release()
		case bool:
			b := array.NewBooleanBuilder(mem)
			b.Append(v)
			args.Named[name] = b.NewArray()
			b.Release()
		}
	}
	return args
}

type reply struct {
	vals []int64
	more bool
	err  string // "" = no error
}

// runPaged is one input batch through blended_paged / paged_explode's shared
// logic: pagedBegin, then pagedNext until a reply says no more or fails. It
// returns the first reply's cache control and every reply.
func runPaged(named map[string]any, ifNoneMatch *string, counts []int64) (*vgi.CacheControl, []reply) {
	params := &vgi.ProcessParams{Args: pagedArgs(named), IfNoneMatch: ifNoneMatch}
	pageSize := vgi.OptionalInt64(params.Args, "page_size", 2)
	state := &PagedState{}
	cc := pagedBegin(params, state, counts)
	var replies []reply
	for len(replies) < 100 {
		vals, _, more, err := pagedNext(params, state, pageSize)
		if err != nil {
			return cc, append(replies, reply{err: err.Error()})
		}
		replies = append(replies, reply{vals: vals, more: more})
		if !more {
			break
		}
	}
	return cc, replies
}

func strp(s string) *string { return &s }

func TestPagedBeginAndNext(t *testing.T) {
	v1 := &vgi.CacheControl{Ttl: vgi.Seconds(0), ETag: "v1", Revalidatable: true}
	v2 := &vgi.CacheControl{Ttl: vgi.Seconds(0), ETag: "v2", Revalidatable: true}
	notModified := &vgi.CacheControl{NotModified: true, Ttl: vgi.Seconds(0), ETag: "v1", Revalidatable: true}
	counts := []int64{3, 2} // output 0 1 2 0 1
	for _, tc := range []struct {
		name        string
		args        map[string]any
		ifNoneMatch *string
		wantCC      *vgi.CacheControl
		want        []reply
	}{
		{"plain: page_size pages, no cache control", nil, nil, nil, []reply{
			{vals: []int64{0, 1}, more: true}, {vals: []int64{2, 0}, more: true}, {vals: []int64{1}},
		}},
		{"cache_ttl on the first reply", map[string]any{"cache_ttl": int64(30), "page_size": int64(4)}, nil,
			&vgi.CacheControl{Ttl: vgi.Seconds(30)}, []reply{
				{vals: []int64{0, 1, 2, 0}, more: true}, {vals: []int64{1}},
			}},
		{"fail_on_page counts replies from 1", map[string]any{"fail_on_page": int64(2)}, nil, nil, []reply{
			{vals: []int64{0, 1}, more: true}, {err: "failing on page 2 as asked"},
		}},
		{"fail_on_page 1 fails Process's reply", map[string]any{"fail_on_page": int64(1)}, nil, nil, []reply{
			{err: "failing on page 1 as asked"},
		}},
		{"fail_on_page past the output never fails", map[string]any{"fail_on_page": int64(9)}, nil, nil, []reply{
			{vals: []int64{0, 1}, more: true}, {vals: []int64{2, 0}, more: true}, {vals: []int64{1}},
		}},
		{"revalidate, plain request: one whole reply, v1, cache_ttl ignored",
			map[string]any{"revalidate": true, "cache_ttl": int64(30)}, nil, v1, []reply{
				{vals: []int64{0, 1, 2, 0, 1}},
			}},
		{"revalidate, conditional: changed (+1000) and paged, v2", map[string]any{"revalidate": true}, strp("v1"), v2, []reply{
			{vals: []int64{1000, 1001}, more: true}, {vals: []int64{1002, 1000}, more: true}, {vals: []int64{1001}},
		}},
		{"not_modified_more, conditional: empty not_modified reply that says more, then the changed pages",
			map[string]any{"revalidate": true, "not_modified_more": true}, strp("v1"), notModified, []reply{
				{vals: nil, more: true}, {vals: []int64{1000, 1001}, more: true},
				{vals: []int64{1002, 1000}, more: true}, {vals: []int64{1001}},
			}},
		{"not_modified_more: the empty reply counts for fail_on_page",
			map[string]any{"revalidate": true, "not_modified_more": true, "fail_on_page": int64(2)}, strp("v1"), notModified, []reply{
				{vals: nil, more: true}, {err: "failing on page 2 as asked"},
			}},
		{"not_modified_more, plain request: as revalidate", map[string]any{"revalidate": true, "not_modified_more": true}, nil, v1, []reply{
			{vals: []int64{0, 1, 2, 0, 1}},
		}},
		{"not_modified_more without revalidate: no effect", map[string]any{"not_modified_more": true}, strp("v1"), nil, []reply{
			{vals: []int64{0, 1}, more: true}, {vals: []int64{2, 0}, more: true}, {vals: []int64{1}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cc, got := runPaged(tc.args, tc.ifNoneMatch, counts)
			if !reflect.DeepEqual(cc, tc.wantCC) {
				t.Errorf("first reply's cache control = %+v, want %+v", cc, tc.wantCC)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("replies = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// Replies are counted per input batch: pagedBegin starts the count over, so
// fail_on_page fails every input that reaches that reply, not just the first.
func TestPagedReplyCountIsPerInput(t *testing.T) {
	params := &vgi.ProcessParams{Args: pagedArgs(map[string]any{"fail_on_page": int64(2)})}
	state := &PagedState{}
	for input := 0; input < 2; input++ {
		pagedBegin(params, state, []int64{5})
		if _, _, more, err := pagedNext(params, state, 2); err != nil || !more {
			t.Fatalf("input %d, reply 1: more=%v err=%v; want more, no error", input, more, err)
		}
		if _, _, _, err := pagedNext(params, state, 2); err == nil || !strings.Contains(err.Error(), "page 2") {
			t.Fatalf("input %d, reply 2: %v; want the fail_on_page error", input, err)
		}
	}
}

func TestCheckPagedArgs(t *testing.T) {
	for _, tc := range []struct {
		args map[string]any
		want string
	}{
		{nil, ""},
		{map[string]any{"page_size": int64(0)}, "page_size must be at least 1, got 0"},
		{map[string]any{"cache_ttl": int64(-1)}, "cache_ttl must not be negative, got -1"},
	} {
		err := checkPagedArgs(pagedArgs(tc.args), 2)
		if tc.want == "" && err != nil || tc.want != "" && (err == nil || err.Error() != tc.want) {
			t.Errorf("checkPagedArgs(%v) = %v, want %q", tc.args, err, tc.want)
		}
	}
}
