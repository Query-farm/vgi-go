// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package table

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"strconv"
)

// ProjectionSequence is a finite projection example, not a general SQL evaluator.
type ProjectionSequence struct{}
type projectionSequenceState struct{ Done bool }

func init() { gob.Register(&projectionSequenceState{}) }

func (*ProjectionSequence) Name() string { return "projection_sequence" }
func (*ProjectionSequence) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{ProjectionPushdown: true}
}
func (*ProjectionSequence) ArgumentSpecs() []vgi.ArgSpec { return nil }
func (*ProjectionSequence) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return &vgi.BindResponse{OutputSchema: arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil), ProjectionExpressionProfiles: []string{vgi.ScalarProjectionProfile}}, nil
}
func (*ProjectionSequence) Projection(_ *vgi.BindParams, p *vgi.ProjectionProposal) (*vgi.ProjectionAcceptance, error) {
	fields := p.Original.OutputSchema.Fields()
	indices := []int64{}
	for i, raw := range p.Expressions {
		row, err := vgi.DeserializeRecordBatch(raw)
		if err != nil {
			return nil, err
		}
		sql := row.Column(0).(*array.String).Value(0)
		c := row.Column(3).(*array.Struct)
		defaults := c.Field(0).(*array.String).Value(0) == "UTC" && c.Field(1).(*array.String).Value(0) == "binary" && c.Field(2).(*array.String).Value(0) == "asc" && c.Field(3).(*array.String).Value(0) == "nulls_last" && c.Field(4).(*array.Boolean).Value(0)
		row.Release()
		if sql == `CAST("n" AS BIGINT)` && defaults && p.ResultSchema.Field(i).Type.ID() == arrow.INT64 {
			indices = append(indices, int64(i))
			fields = append(fields, p.ResultSchema.Field(i))
		}
	}
	if len(indices) == 0 {
		return nil, nil
	}
	md := p.Original.OutputSchema.Metadata()
	return &vgi.ProjectionAcceptance{AcceptedIndices: indices, BindResult: &vgi.BindResponse{OutputSchema: arrow.NewSchema(fields, &md), OpaqueData: []byte(strconv.Itoa(len(indices))), ProjectionExpressionProfiles: p.Original.ProjectionExpressionProfiles}}, nil
}
func (*ProjectionSequence) OnInit(*vgi.InitParams) (*vgi.GlobalInitResponse, error) {
	return vgi.DefaultInit()
}
func (*ProjectionSequence) NewState(p *vgi.ProcessParams) (interface{}, error) {
	if len(p.BindOpaqueData) > 0 {
		n, e := strconv.Atoi(string(p.BindOpaqueData))
		if e != nil || n < 1 {
			return nil, fmt.Errorf("invalid projection state")
		}
	}
	return &projectionSequenceState{}, nil
}
func (*ProjectionSequence) Process(_ context.Context, p *vgi.ProcessParams, state interface{}, out *vgirpc.OutputCollector) error {
	s := state.(*projectionSequenceState)
	if s.Done {
		out.Finish()
		return nil
	}
	s.Done = true
	// Emit one batch; finish on the next tick so the batch is not discarded.
	b := array.NewRecordBuilder(memory.DefaultAllocator, p.OutputSchema)
	defer b.Release()
	for i, f := range p.OutputSchema.Fields() {
		for n := 0; n < 10; n++ {
			if f.Name == "n" {
				b.Field(i).(*array.Int32Builder).Append(int32(n))
			} else {
				if len(p.BindOpaqueData) == 0 {
					return fmt.Errorf("projection state missing")
				}
				b.Field(i).(*array.Int64Builder).Append(int64(n))
			}
		}
	}
	batch := b.NewRecordBatch()
	return out.Emit(batch)
}
