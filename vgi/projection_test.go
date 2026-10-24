// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package vgi

import (
	"context"
	"encoding/base64"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"testing"
)

type projectionTestFunction struct{ accept *ProjectionAcceptance }

func (*projectionTestFunction) Name() string                                    { return "projection_test" }
func (*projectionTestFunction) Metadata() FunctionMetadata                      { return FunctionMetadata{} }
func (*projectionTestFunction) ArgumentSpecs() []ArgSpec                        { return nil }
func (*projectionTestFunction) OnBind(*BindParams) (*BindResponse, error)       { return nil, nil }
func (*projectionTestFunction) OnInit(*InitParams) (*GlobalInitResponse, error) { return DefaultInit() }
func (*projectionTestFunction) NewState(*ProcessParams) (interface{}, error)    { return nil, nil }
func (*projectionTestFunction) Process(context.Context, *ProcessParams, interface{}, *vgirpc.OutputCollector) error {
	return nil
}
func (f *projectionTestFunction) Projection(*BindParams, *ProjectionProposal) (*ProjectionAcceptance, error) {
	return f.accept, nil
}
func TestProjectionAcceptanceContract(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	md := arrow.MetadataFrom(map[string]string{"source": "keep"})
	fm := arrow.MetadataFrom(map[string]string{"result": "keep"})
	source := arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, &md)
	candidate := arrow.Field{Name: "wide", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: fm}
	result := arrow.NewSchema([]arrow.Field{candidate}, nil)
	expected := arrow.NewSchema(append(source.Fields(), candidate), &md)
	schemaBytes, _ := SerializeSchema(source)
	resultBytes, _ := SerializeSchema(result)
	args, _ := SerializeSchema(arrow.NewSchema(nil, nil))
	owner := []string{"main"}
	text, _ := GeneratedExpressionMetadata(`CAST("n" AS BIGINT)`, owner)
	expression, _ := base64.StdEncoding.DecodeString(text)
	req := projectionDecodedRequest{BindCall: BindRequestWire{FunctionName: "projection_test", FunctionType: "TABLE", Arguments: args, SchemaPath: &owner}, BindResult: BindResponseWire{OutputSchema: schemaBytes, ProjectionExpressionProfiles: []string{ScalarProjectionProfile}, Repeatability: string(BindRepeatabilityUnknown)}, Expressions: [][]byte{expression}, ResultSchema: resultBytes}
	f := &projectionTestFunction{}
	w := NewWorker()
	w.RegisterTable(f)
	cc := &vgirpc.CallContext{Auth: vgirpc.Anonymous()}
	makeAccepted := func() *ProjectionAcceptance {
		return &ProjectionAcceptance{AcceptedIndices: []int64{0}, BindResult: &BindResponse{OutputSchema: expected, OpaqueData: []byte("state"), ProjectionExpressionProfiles: []string{ScalarProjectionProfile}}}
	}
	for _, name := range []string{"valid", "decline", "duplicate", "negative", "range", "schema", "metadata", "offer", "secret", "repeatability"} {
		t.Run(name, func(t *testing.T) {
			f.accept = makeAccepted()
			switch name {
			case "decline":
				f.accept = nil
			case "duplicate":
				f.accept.AcceptedIndices = []int64{0, 0}
			case "negative":
				f.accept.AcceptedIndices = []int64{-1}
			case "range":
				f.accept.AcceptedIndices = []int64{1}
			case "schema":
				f.accept.BindResult.OutputSchema = source
			case "metadata":
				f.accept.BindResult.OutputSchema = arrow.NewSchema(expected.Fields(), nil)
			case "offer":
				f.accept.BindResult.ProjectionExpressionProfiles = nil
			case "repeatability":
				f.accept.BindResult.Repeatability = BindRepeatabilityStableWithinQuery
			case "secret":
				f.accept.BindResult.SecretScopeRequest = []SecretLookup{{SecretType: "test"}}
			}
			response, err := w.vgiService().handleProjection(context.Background(), cc, req)
			if name == "valid" || name == "decline" {
				if err != nil {
					t.Fatal(err)
				}
				if name == "valid" && string(response.BindResult.OpaqueData) != "state" {
					t.Fatal("state lost")
				}
			} else if err == nil {
				t.Fatal("invalid callback response accepted")
			}
		})
	}
}
func TestProjectionOfferProfiles(t *testing.T) {
	for _, p := range [][]string{nil, {ScalarProjectionProfile, "vendor.scalar.v3"}} {
		if err := validateProjectionProfiles(p); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range [][]string{{"bad"}, {"vendor.scalar.v0"}, {ScalarProjectionProfile, ScalarProjectionProfile}} {
		if validateProjectionProfiles(p) == nil {
			t.Fatal("invalid offer accepted")
		}
	}
}
