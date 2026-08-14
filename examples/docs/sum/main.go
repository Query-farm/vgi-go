// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Command sum is the aggregate example for the vgi-go documentation.
//
// An aggregate folds many rows into one value per GROUP BY group. It runs in
// four phases, and the split is what lets DuckDB parallelise it:
//
//   - NewState  — the identity value for a group (0 for a sum).
//
//   - Update    — fold a batch of rows into per-group state. Runs in every
//     worker, over that worker's share of the rows.
//
//   - Combine   — merge two partial states for the same group.
//
//   - Finalize  — turn state into one output row per group.
//
//     go build -o sumworker .
//     # then, in a Haybarn shell:
//     ATTACH 'agg' (TYPE vgi, LOCATION './sumworker');
//     SELECT category, agg.vgi_sum(value) FROM t GROUP BY category;
package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"log"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// SumState is the per-group accumulator. It is serialized between phases —
// Update and Combine can run in different processes — so keep it small and
// keep it a plain struct of exported fields.
type SumState struct {
	Total int64
}

// Register the state with gob.
//
// Aggregate state is gob-encoded between phases. Through v0.21.0 nothing does
// this for you: the typed adapters (AsTableFunction, AsTableInOutFunction) call
// gob.Register(new(S)) themselves, but RegisterAggregate takes the interface
// directly and has no adapter. An unregistered state compiles, attaches, and
// then fails on the first GROUP BY with:
//
//	encoding aggregate state: gob: type not registered for interface: main.SumState
//
// Later versions register it from NewState, at which point this line becomes a
// harmless no-op — registering the same type twice is fine — so it is safe to
// keep either way.
func init() { gob.Register(&SumState{}) }

type sumArgs struct {
	Value int64 `vgi:"pos=0,const=false,doc=Column to sum"`
}

// SumFn sums a BIGINT column per group.
type SumFn struct{}

var _ vgi.AggregateFunction = (*SumFn)(nil)

func (*SumFn) Name() string { return "vgi_sum" }

func (*SumFn) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Sums a BIGINT column per group",
		Stability:   vgi.StabilityConsistent,
		// NULLs are skipped rather than treated as zero, matching SQL's SUM.
		NullHandling:      vgi.NullHandlingDefault,
		ReturnType:        arrow.PrimitiveTypes.Int64,
		OrderDependent:    vgi.OrderDependenceNotDependent,
		DistinctDependent: vgi.DistinctDependenceNotDependent,
	}
}

func (*SumFn) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(sumArgs{}) }

func (*SumFn) OnBind(_ *vgi.AggregateBindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{
		{Name: "result", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
}

func (*SumFn) NewState(*vgi.AggregateProcessParams) interface{} { return &SumState{} }

// Update folds one batch into per-group state.
//
// A group is only created when a row genuinely contributes. Skipping a NULL
// without calling EnsureState is what makes SUM over an all-NULL group return
// NULL rather than 0 — the group never reaches storage, and Finalize sees no
// state for it.
func (*SumFn) Update(states map[int64]interface{}, gids *vgi.Int64Slice, columns []arrow.Array, _ *vgi.AggregateProcessParams) error {
	if len(columns) == 0 {
		return fmt.Errorf("vgi_sum: missing value column")
	}
	col, ok := columns[0].(*array.Int64)
	if !ok {
		return fmt.Errorf("vgi_sum: value column is %T, expected int64", columns[0])
	}
	for i := 0; i < gids.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		s := vgi.EnsureState(states, gids.At(i), func() *SumState { return &SumState{} })
		s.Total += col.Value(i)
	}
	return nil
}

// Combine merges two partials for the same group. It must be associative and
// commutative: DuckDB decides how many workers run and in what order they merge.
func (*SumFn) Combine(source, target interface{}, _ *vgi.AggregateProcessParams) (interface{}, error) {
	return &SumState{Total: source.(*SumState).Total + target.(*SumState).Total}, nil
}

// Finalize emits exactly one row per group id, in the order given. A group with
// no state contributed nothing, so it emits NULL.
func (*SumFn) Finalize(gids []int64, states map[int64]interface{}, _ *vgi.AggregateProcessParams) (arrow.RecordBatch, error) {
	mem := memory.NewGoAllocator()
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for _, gid := range gids {
		if s, ok := states[gid].(*SumState); ok && s != nil {
			b.Append(s.Total)
		} else {
			b.AppendNull()
		}
	}
	arr := b.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{arr}, int64(len(gids))), nil
}

func main() {
	httpMode := flag.Bool("http", false, "serve over HTTP instead of stdio")
	logFlags := vgi.RegisterLoggingFlags(flag.CommandLine)
	flag.Parse()
	if err := logFlags.Apply(); err != nil {
		log.Fatalf("logging flags: %v", err)
	}

	w := vgi.NewWorker(
		vgi.WithCatalogName("agg"),
		vgi.WithCatalogComment("Documentation example: a distributed aggregate"),
	)
	w.RegisterAggregate(&SumFn{})

	if *httpMode {
		if err := w.RunHttp("127.0.0.1:0"); err != nil {
			log.Fatal(err)
		}
		return
	}
	w.RunStdio()
}
