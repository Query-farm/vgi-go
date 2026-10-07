// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// Package ticket_probe is the cross-SDK fixture catalog for attach tickets.
//
// Every SDK's fixture worker serves it identically (vgi-python
// vgi/_test_fixtures/ticket_probe.py), and the extension's attach_ticket
// sqllogictests run against each. The contract (vgi-attach-tickets.md §7):
//
//   - Catalog ticket_probe, default schema main.
//   - Attach options, in order: region (VARCHAR, default 'us-east-1') and
//     api_key (VARCHAR, required, secret).
//   - Table main.probe, backed by the table function main.ticket_probe (no
//     arguments): one row, region and api_key_sha256 (the first 12 lowercase
//     hex characters of SHA-256(UTF-8(api_key))). The key itself is never
//     returned.
//
// So a reattach with nothing but vgi_attach_ticket reading the same row proves
// the secret option took effect without travelling again.
package ticket_probe

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	// CatalogName is the fixture catalog's name.
	CatalogName = "ticket_probe"
	// DefaultRegion is region's default.
	DefaultRegion = "us-east-1"
)

// attachMarker opens the payload this catalog stores in attach_opaque_data:
// marker || region || NUL || digest. The function finds it by the marker, so
// it does not depend on how the framework wraps a routed attach.
var attachMarker = []byte("vgi.ticket_probe\x01")

var probeSchema = arrow.NewSchema([]arrow.Field{
	{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "api_key_sha256", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

// APIKeyDigest is the first 12 lowercase hex characters of
// SHA-256(UTF-8(apiKey)).
func APIKeyDigest(apiKey string) string {
	sum := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(sum[:])[:12]
}

// AttachOptionSpecs are region (default 'us-east-1') and api_key (required,
// secret), in that order.
func AttachOptionSpecs() []vgi.AttachOptionSpec {
	b := array.NewStringBuilder(memory.NewGoAllocator())
	defer b.Release()
	b.Append(DefaultRegion)
	value := b.NewArray()
	defer value.Release()
	defaultBatch := array.NewRecordBatch(
		arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil),
		[]arrow.Array{value}, 1)
	return []vgi.AttachOptionSpec{
		{Name: "region", Description: "Region the probe reports back", Type: arrow.BinaryTypes.String,
			DefaultBatch: defaultBatch},
		{Name: "api_key", Description: "API key; only its digest is ever returned", Type: arrow.BinaryTypes.String,
			Required: true, Secret: true},
	}
}

// NewWorker builds the ticket_probe catalog, to be served by another worker
// with RegisterSubCatalog.
func NewWorker() *vgi.Worker {
	w := vgi.NewWorker(
		vgi.WithCatalogName(CatalogName),
		vgi.WithCatalogComment("Attach-ticket probe: one plain and one secret attach option"),
		vgi.WithAttachOptions(AttachOptionSpecs()...),
		vgi.WithAttachValidator(attach),
	)
	fn := vgi.AsTableFunction[probeState](&probeFunction{})
	w.RegisterTable(fn)
	w.RegisterCatalogTable("main", vgi.CatalogTable{
		Name:     "probe",
		Comment:  "The options this attach was made with",
		Function: fn,
	})
	return w
}

// Register serves ticket_probe from w.
func Register(w *vgi.Worker) { w.RegisterSubCatalog(NewWorker()) }

// attach records region and sha256(api_key)[:12] -- never the key -- in the
// attach value. The framework has already refused an attach without api_key.
func attach(req *vgi.CatalogAttachRequestWire, _ *vgirpc.CallContext) (*vgi.AttachDecision, error) {
	region, apiKey := DefaultRegion, ""
	if req.Options != nil && len(*req.Options) > 0 {
		batch, err := vgi.DeserializeRecordBatch(*req.Options)
		if err != nil {
			return nil, fmt.Errorf("reading attach options: %w", err)
		}
		defer batch.Release()
		if batch.NumRows() > 0 {
			for i, f := range batch.Schema().Fields() {
				col, ok := batch.Column(i).(*array.String)
				if !ok || col.IsNull(0) {
					continue
				}
				switch strings.ToLower(f.Name) {
				case "region":
					region = col.Value(0)
				case "api_key":
					apiKey = col.Value(0)
				}
			}
		}
	}
	value := append(append([]byte{}, attachMarker...), region...)
	value = append(value, 0)
	value = append(value, APIKeyDigest(apiKey)...)
	return &vgi.AttachDecision{AttachOpaqueData: value}, nil
}

// probeRow recovers (region, digest) from an attach value.
func probeRow(attachOpaqueData []byte) (string, string, error) {
	i := bytes.LastIndex(attachOpaqueData, attachMarker)
	if i < 0 {
		return "", "", fmt.Errorf("ticket_probe must be read through an attach of the ticket_probe catalog")
	}
	region, digest, ok := bytes.Cut(attachOpaqueData[i+len(attachMarker):], []byte{0})
	if !ok {
		return "", "", fmt.Errorf("ticket_probe: malformed attach value")
	}
	return string(region), string(digest), nil
}

type probeFunction struct{}

type probeState struct{ Emitted bool }

func (f *probeFunction) Name() string { return "ticket_probe" }

func (f *probeFunction) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{
		Description: "Report the attach options of this ticket_probe attach (the api_key only as a digest)",
		Stability:   vgi.StabilityConsistent,
		Categories:  []string{"generator", "testing"},
	}
}

func (f *probeFunction) ArgumentSpecs() []vgi.ArgSpec { return nil }

// OnBind answers the fixed schema whatever the attach: catalog discovery
// (catalog_schema_contents_tables resolving main.probe's columns) binds it
// without the attach the row comes from. The row is read in Process.
func (f *probeFunction) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: probeSchema}, nil
}

func (f *probeFunction) OnInit(*vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return &vgi.GlobalInitResponse{MaxWorkers: 1}, nil
}

func (f *probeFunction) Cardinality(*vgi.BindParams) (*vgi.TableCardinality, error) {
	one := int64(1)
	return &vgi.TableCardinality{Estimate: one, Max: one}, nil
}

func (f *probeFunction) NewState(*vgi.ProcessParams) (*probeState, error) { return &probeState{}, nil }

func (f *probeFunction) Process(_ context.Context, params *vgi.ProcessParams, state *probeState, out *vgirpc.OutputCollector) error {
	if state.Emitted {
		out.Finish()
		return nil
	}
	// AttachScope is the opened attach value of the query's ATTACH.
	region, digest, err := probeRow(params.AttachScope)
	if err != nil {
		return err
	}
	mem := memory.NewGoAllocator()
	rb := array.NewStringBuilder(mem)
	defer rb.Release()
	rb.Append(region)
	db := array.NewStringBuilder(mem)
	defer db.Release()
	db.Append(digest)
	rc, dc := rb.NewArray(), db.NewArray()
	defer rc.Release()
	defer dc.Release()
	batch := array.NewRecordBatch(probeSchema, []arrow.Array{rc, dc}, 1)
	defer batch.Release()
	out.Emit(batch)
	state.Emitted = true
	return nil
}
