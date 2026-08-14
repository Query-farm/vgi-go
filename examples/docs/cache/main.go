// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Command cache is the result-caching example for the vgi-go documentation.
//
// Caching is advertised, not requested: the worker attaches vgi.cache.* metadata
// to the FIRST data batch it emits, and the client (the DuckDB extension) decides
// what to do with it. Nothing is cached unless you say so.
//
// This worker exposes rates(), standing in for a slow upstream whose answer is
// worth reusing, and shows the whole vocabulary: a freshness lifetime, a
// validator plus Revalidatable so the client can ask "still good?" instead of
// paying for a recompute, and the 304-equivalent reply to such a request.
//
//	go build -o cacheworker .
//	# then, in a Haybarn shell:
//	ATTACH 'rates' (TYPE vgi, LOCATION './cacheworker');
//	SELECT * FROM rates.rates();   -- repeat calls inside the TTL never land here
package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var ratesSchema = arrow.NewSchema([]arrow.Field{
	{Name: "currency", Type: arrow.BinaryTypes.String},
	{Name: "rate", Type: arrow.PrimitiveTypes.Float64},
}, nil)

// dataVersion stands in for whatever makes the upstream's answer change — a
// last-modified header, a version column, a content hash. It is the ONLY thing
// revalidation compares.
const dataVersion = `"rates-v3"`

var (
	currencies = []string{"EUR", "GBP", "JPY"}
	rates      = []float64{1.09, 1.27, 0.0067}
)

type ratesState struct {
	Done bool
}

// RatesFn emits the rate table once, advertising it as cacheable.
type RatesFn struct{}

var _ vgi.TypedTableFunc[ratesState] = (*RatesFn)(nil)

func (*RatesFn) Name() string { return "rates" }

func (*RatesFn) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Exchange rates from a slow upstream, cacheable for 5 minutes",
		Stability:   vgi.StabilityConsistent,
	}
}

func (*RatesFn) ArgumentSpecs() []vgi.ArgSpec { return nil }

func (*RatesFn) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(ratesSchema)
}

func (*RatesFn) NewState(_ *vgi.ProcessParams) (*ratesState, error) { return &ratesState{}, nil }

func (*RatesFn) Process(_ context.Context, params *vgi.ProcessParams, state *ratesState, out *vgirpc.OutputCollector) error {
	if state.Done {
		return out.Finish()
	}
	state.Done = true

	// Observable proof that the cache is working: this line appears once per
	// real invocation. Run the same SELECT several times in one session and you
	// should see it once. Logs go to stderr because stdout is the protocol.
	log.SetOutput(os.Stderr)
	log.Println("upstream fetch")

	// Conditional request: the client holds a stale-but-revalidatable copy and
	// is asking whether it may keep it. Both validators are nil on a normal call.
	if params.IfNoneMatch != nil && *params.IfNoneMatch == dataVersion {
		empty := array.NewRecordBatch(ratesSchema, emptyColumns(), 0)
		defer empty.Release()
		if err := vgi.Emit(out, empty, vgi.WithCacheControl(&vgi.CacheControl{
			Ttl:           vgi.Seconds(300),
			ETag:          dataVersion,
			Revalidatable: true,
			NotModified:   true, // 304: keep what you have
		})); err != nil {
			return err
		}
		return out.Finish()
	}

	batch := buildRates()
	defer batch.Release()

	// The metadata rides on the FIRST data batch. Attaching it to a later batch
	// has no effect — by then the client has decided how to treat the stream.
	if err := vgi.Emit(out, batch, vgi.WithCacheControl(&vgi.CacheControl{
		Ttl:                  vgi.Seconds(300), // reusable for 5 minutes without asking
		ETag:                 dataVersion,      // ...and after that, cheap to revalidate
		Revalidatable:        true,             // gates whether the client ever asks
		StaleWhileRevalidate: vgi.Seconds(60),  // serve stale while refreshing behind it
		StaleIfError:         vgi.Seconds(600), // serve stale rather than fail
	})); err != nil {
		return err
	}
	return out.Finish()
}

func emptyColumns() []arrow.Array {
	mem := memory.NewGoAllocator()
	names := array.NewStringBuilder(mem)
	defer names.Release()
	vals := array.NewFloat64Builder(mem)
	defer vals.Release()
	return []arrow.Array{names.NewArray(), vals.NewArray()}
}

func buildRates() arrow.RecordBatch {
	mem := memory.NewGoAllocator()
	names := array.NewStringBuilder(mem)
	defer names.Release()
	vals := array.NewFloat64Builder(mem)
	defer vals.Release()
	names.AppendValues(currencies, nil)
	vals.AppendValues(rates, nil)
	return array.NewRecordBatch(ratesSchema, []arrow.Array{names.NewArray(), vals.NewArray()}, int64(len(currencies)))
}

// NewRates returns the registration-ready function.
func NewRates() vgi.TableFunction { return vgi.AsTableFunction[ratesState](&RatesFn{}) }

func main() {
	httpMode := flag.Bool("http", false, "serve over HTTP instead of stdio")
	logFlags := vgi.RegisterLoggingFlags(flag.CommandLine)
	flag.Parse()
	if err := logFlags.Apply(); err != nil {
		log.Fatalf("logging flags: %v", err)
	}

	w := vgi.NewWorker(
		vgi.WithCatalogName("rates"),
		vgi.WithCatalogComment("Documentation example: an advertised-cacheable result"),
	)
	w.RegisterTable(NewRates())

	if *httpMode {
		if err := w.RunHttp("127.0.0.1:0"); err != nil {
			log.Fatal(err)
		}
		return
	}
	w.RunStdio()
}
