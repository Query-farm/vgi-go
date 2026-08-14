// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Command catalog is the catalog example for the vgi-go documentation.
//
// A worker does not have to be a bag of functions. It can present itself as a
// database: a named catalog you ATTACH, holding schemas that hold tables and
// views, queried with ordinary qualified names.
//
// The table here is *function-backed*: `RegisterCatalogTable` is given a table
// function plus the arguments to call it with, so `SELECT * FROM cat.data.cities`
// runs the function with those arguments baked in. The user never passes them,
// and never sees the function.
//
//	go build -o catalogworker .
//	# then, in a Haybarn shell:
//	ATTACH 'cat' (TYPE vgi, LOCATION './catalogworker');
//	SELECT * FROM cat.data.cities;
//	SELECT * FROM cat.data.big_cities;   -- a view over the table
package main

import (
	"context"
	"flag"
	"log"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// The table's shape. Declaring Columns explicitly on the CatalogTable lets
// DuckDB describe the table without calling the worker at all.
var citiesSchema = arrow.NewSchema([]arrow.Field{
	{Name: "name", Type: arrow.BinaryTypes.String},
	{Name: "population", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// Fields are exported because this type ends up inside the scan state, and
// state has to be gob-encodable so it can survive an HTTP continuation. The SDK
// checks at registration: a struct whose fields are all unexported panics with
// "type ... has no exported fields" the moment AsTableFunction is called, rather
// than mid-query on the first continuation.
type city struct {
	Name string
	Pop  int64
}

// Stands in for whatever the worker actually fronts — a remote API, a file
// format, a device.
var cities = []city{
	{Name: "Charlottesville", Pop: 51_000},
	{Name: "Richmond", Pop: 230_000},
	{Name: "Virginia Beach", Pop: 457_000},
}

type citiesArgs struct {
	MinPopulation int64 `vgi:"pos=0,ge=0,doc=Only return cities at least this large"`
}

// citiesState materializes the whole filtered result up front. That is fine for
// three rows and wrong for three million: state is carried between Process calls
// and gob-encoded across an HTTP continuation, so anything held here is paid for
// repeatedly. A real scan keeps a *cursor* — an offset, a page token, an open
// iterator id — and fetches each batch in Process.
type citiesState struct {
	vgi.BatchState
	Rows []city
}

// CitiesFn is the scan behind the table. It is an ordinary table function —
// nothing about it knows it is backing a catalog table.
type CitiesFn struct{}

var _ vgi.TypedTableFunc[citiesState] = (*CitiesFn)(nil)

func (*CitiesFn) Name() string { return "cities_scan" }

func (*CitiesFn) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Scans the cities table, optionally filtered by population",
		Stability:   vgi.StabilityConsistent,
	}
}

func (*CitiesFn) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(citiesArgs{}) }

func (*CitiesFn) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(citiesSchema)
}

func (*CitiesFn) NewState(params *vgi.ProcessParams) (*citiesState, error) {
	var args citiesArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	rows := selectCities(args.MinPopulation)
	return &citiesState{
		BatchState: vgi.NewBatchState(int64(len(rows)), 1024),
		Rows:       rows,
	}, nil
}

// selectCities is the filter, kept separate from the plumbing so it can be read
// and tested on its own.
func selectCities(minPopulation int64) []city {
	var out []city
	for _, c := range cities {
		if c.Pop >= minPopulation {
			out = append(out, c)
		}
	}
	return out
}

func (*CitiesFn) Process(_ context.Context, _ *vgi.ProcessParams, state *citiesState, out *vgirpc.OutputCollector) error {
	return vgi.GenerateBatch(&state.BatchState, out, func(size int64) ([]arrow.Array, error) {
		start := state.Index
		mem := memory.NewGoAllocator()

		names := array.NewStringBuilder(mem)
		defer names.Release()
		pops := array.NewInt64Builder(mem)
		defer pops.Release()

		for i := int64(0); i < size; i++ {
			row := state.Rows[start+i]
			names.Append(row.Name)
			pops.Append(row.Pop)
		}
		return []arrow.Array{names.NewArray(), pops.NewArray()}, nil
	})
}

// NewCitiesScan returns the registration-ready scan function.
func NewCitiesScan() vgi.TableFunction {
	return vgi.AsTableFunction[citiesState](&CitiesFn{})
}

func main() {
	httpMode := flag.Bool("http", false, "serve over HTTP instead of stdio")
	logFlags := vgi.RegisterLoggingFlags(flag.CommandLine)
	flag.Parse()
	if err := logFlags.Apply(); err != nil {
		log.Fatalf("logging flags: %v", err)
	}

	w := vgi.NewWorker(
		vgi.WithCatalogName("cat"),
		vgi.WithCatalogComment("Documentation example: a worker presented as a database"),
	)

	// A function-backed table. FuncArgs are bound at scan time, so the user
	// writes `SELECT * FROM cat.data.cities` with no arguments at all.
	//
	// Registering the table also registers its backing function in the
	// catalog's default schema — that is where the extension resolves the scan.
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:     "cities",
		Comment:  "Every city the worker knows about",
		Columns:  citiesSchema,
		Function: NewCitiesScan(),
		FuncArgs: []vgi.CatalogTableArg{
			{Position: 0, Value: int64(0), Type: arrow.PrimitiveTypes.Int64},
		},
		NotNull:        []string{"name"},
		ColumnComments: map[string]string{"population": "Most recent estimate"},
	})

	// A view is pure SQL that DuckDB evaluates — no worker round trip at all
	// once the definition has been advertised.
	w.RegisterCatalogView("data", vgi.CatalogView{
		Name:       "big_cities",
		Comment:    "Cities with a population of at least 100,000",
		Definition: "SELECT * FROM cat.data.cities WHERE population >= 100000",
	})

	if *httpMode {
		if err := w.RunHttp("127.0.0.1:0"); err != nil {
			log.Fatal(err)
		}
		return
	}
	w.RunStdio()
}
