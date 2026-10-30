// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func mustRecord(t *testing.T, spec vgi.ConstraintRecordSpec) []byte {
	t.Helper()
	record, err := vgi.ConstraintRecord(spec)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func enforced(spec vgi.ConstraintRecordSpec) vgi.ConstraintRecordSpec {
	spec.Enforcement, spec.Validation = vgi.ConstraintEnforced, vgi.ConstraintAllRows
	return spec
}

func TestConstraintRecordRoundTrip(t *testing.T) {
	specs := []vgi.ConstraintRecordSpec{
		enforced(vgi.ConstraintRecordSpec{SchemaPath: []string{"data"}, Table: "t", ID: "t:nn", Kind: vgi.ConstraintNotNull,
			Columns: [][]string{{"bbox", "x.min"}}}),
		enforced(vgi.ConstraintRecordSpec{SchemaPath: []string{"data"}, Table: "t", ID: "t:pk", Kind: vgi.ConstraintPrimaryKey,
			Columns: [][]string{{"a"}, {"b"}}, DeclaredName: "t_pk"}),
		{SchemaPath: []string{"a", "b"}, Table: "t", ID: "t:u", Kind: vgi.ConstraintUnique, Columns: [][]string{{"email"}},
			Enforcement: vgi.ConstraintMetadataOnly, Validation: vgi.ConstraintUnvalidated},
		enforced(vgi.ConstraintRecordSpec{SchemaPath: []string{"data"}, Table: "t", ID: "t:ck", Kind: vgi.ConstraintCheck,
			Columns: [][]string{}, CheckSQL: "budget >= 0"}),
		enforced(vgi.ConstraintRecordSpec{SchemaPath: []string{"data"}, Table: "emp", ID: "emp:fk", Kind: vgi.ConstraintForeignKey,
			Columns: [][]string{{"dept_id"}}, References: &vgi.ConstraintReference{
				SchemaPath: []string{"data"}, Table: "dept", Columns: [][]string{{"id"}}, ConstraintID: "dept:pk"}}),
	}
	for _, spec := range specs {
		t.Run(spec.ID, func(t *testing.T) {
			got, err := vgi.ReadConstraintRecord(mustRecord(t, spec))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, spec) {
				t.Fatalf("round trip changed the record:\n got %#v\nwant %#v", got, spec)
			}
		})
	}
}

func TestConstraintRecordMatchesGeneratedSchemas(t *testing.T) {
	record := mustRecord(t, enforced(vgi.ConstraintRecordSpec{SchemaPath: []string{"data"}, Table: "t", ID: "t:ck",
		Kind: vgi.ConstraintCheck, CheckSQL: "x > 0"}))
	batch, err := vgi.DeserializeRecordBatch(record)
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()
	if !batch.Schema().Equal(generated.ConstraintInfoSchema) || batch.NumRows() != 1 {
		t.Fatalf("not a canonical ConstraintInfo record: %v", batch.Schema())
	}
}

func TestConstraintRecordRejectsInvalidSpecs(t *testing.T) {
	base := enforced(vgi.ConstraintRecordSpec{SchemaPath: []string{"data"}, Table: "t", ID: "k",
		Kind: vgi.ConstraintNotNull, Columns: [][]string{{"id"}}})
	cases := map[string]func(*vgi.ConstraintRecordSpec){
		"empty id":            func(s *vgi.ConstraintRecordSpec) { s.ID = "" },
		"implicit enforce":    func(s *vgi.ConstraintRecordSpec) { s.Enforcement = "" },
		"implicit validation": func(s *vgi.ConstraintRecordSpec) { s.Validation = "" },
		"unknown kind":        func(s *vgi.ConstraintRecordSpec) { s.Kind = "EXCLUDE" },
		"two not null paths":  func(s *vgi.ConstraintRecordSpec) { s.Columns = [][]string{{"a"}, {"b"}} },
		"no paths":            func(s *vgi.ConstraintRecordSpec) { s.Columns = nil },
		"empty component":     func(s *vgi.ConstraintRecordSpec) { s.Columns = [][]string{{""}} },
		"check sql on non-check": func(s *vgi.ConstraintRecordSpec) {
			s.CheckSQL = "x > 0"
		},
		"check without sql": func(s *vgi.ConstraintRecordSpec) { s.Kind, s.Columns = vgi.ConstraintCheck, nil },
		"check with paths": func(s *vgi.ConstraintRecordSpec) {
			s.Kind, s.CheckSQL = vgi.ConstraintCheck, "x > 0"
		},
		"fk without references": func(s *vgi.ConstraintRecordSpec) { s.Kind = vgi.ConstraintForeignKey },
		"fk arity": func(s *vgi.ConstraintRecordSpec) {
			s.Kind = vgi.ConstraintForeignKey
			s.References = &vgi.ConstraintReference{Table: "u", Columns: [][]string{{"a"}, {"b"}}, ConstraintID: "u:pk"}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			spec := base
			mutate(&spec)
			if _, err := vgi.ConstraintRecord(spec); err == nil {
				t.Fatal("invalid spec was encoded")
			}
		})
	}
}

func serializeTable(columns *arrow.Schema, constraints ...[]byte) error {
	_, err := vgi.SerializeTableInfo(&vgi.TableInfo{Name: "t", SchemaPath: []string{"data"}, Columns: columns, Constraints: constraints})
	return err
}

func notNullOn(t *testing.T, path ...string) []byte {
	return mustRecord(t, enforced(vgi.ConstraintRecordSpec{SchemaPath: []string{"data"}, Table: "t",
		ID: "nn:" + strings.Join(path, "."), Kind: vgi.ConstraintNotNull, Columns: [][]string{path}}))
}

func wantErr(t *testing.T, err error, parts ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error mentioning %q", parts)
	}
	for _, part := range parts {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("error %q does not mention %q", err, part)
		}
	}
}

// Rule (b): DuckDB reads only ENFORCED + ALL_ROWS records.
func TestServedRecordsMustBeEnforcedAllRows(t *testing.T) {
	columns := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	for _, state := range [][2]string{{"METADATA_ONLY", "ALL_ROWS"}, {"ENFORCED", "FUTURE_WRITES"}, {"ENFORCED", "UNVALIDATED"}} {
		record := mustRecord(t, vgi.ConstraintRecordSpec{SchemaPath: []string{"data"}, Table: "t", ID: "t:u",
			Kind: vgi.ConstraintUnique, Columns: [][]string{{"id"}},
			Enforcement: vgi.ConstraintEnforcement(state[0]), Validation: vgi.ConstraintValidation(state[1])})
		wantErr(t, serializeTable(columns, record), `table "t"`, `"t:u"`, state[0]+"/"+state[1], "ENFORCED + ALL_ROWS")
	}
	ok := mustRecord(t, enforced(vgi.ConstraintRecordSpec{SchemaPath: []string{"data"}, Table: "t", ID: "t:u",
		Kind: vgi.ConstraintUnique, Columns: [][]string{{"id"}}}))
	if err := serializeTable(columns, ok); err != nil {
		t.Fatal(err)
	}
}

// Rule (c): Arrow nullability equals NOT_NULL ∪ PRIMARY_KEY membership.
func TestServedNullabilityMatchesRecords(t *testing.T) {
	i64 := arrow.PrimitiveTypes.Int64
	nested := func(childNullable bool, metadata arrow.Metadata) *arrow.Schema {
		return arrow.NewSchema([]arrow.Field{{Name: "s", Nullable: true, Metadata: metadata,
			Type: arrow.StructOf(arrow.Field{Name: "x", Type: i64, Nullable: childNullable})}}, nil)
	}
	required := arrow.NewSchema([]arrow.Field{{Name: "id", Type: i64}}, nil)
	optional := arrow.NewSchema([]arrow.Field{{Name: "id", Type: i64, Nullable: true}}, nil)
	pk := mustRecord(t, enforced(vgi.ConstraintRecordSpec{SchemaPath: []string{"data"}, Table: "t", ID: "t:pk",
		Kind: vgi.ConstraintPrimaryKey, Columns: [][]string{{"id"}}}))

	// Even a table with no records.
	wantErr(t, serializeTable(required), `table "t"`, "column id is non-nullable", "vgi.ConstraintRecord")
	wantErr(t, serializeTable(optional, notNullOn(t, "id")), `table "t"`, "column id is in a NOT_NULL/PRIMARY_KEY record but its Arrow field is nullable")
	wantErr(t, serializeTable(optional, pk), "column id")
	for name, err := range map[string]error{
		"nullable, no record": serializeTable(optional),
		"not null record":     serializeTable(required, notNullOn(t, "id")),
		"primary key record":  serializeTable(required, pk),
		"nested record":       serializeTable(nested(false, arrow.Metadata{}), notNullOn(t, "s", "x")),
		"nested nullable":     serializeTable(nested(true, arrow.Metadata{})),
		// Storage members of a logical type are not SQL column paths.
		"extension struct": serializeTable(nested(false, arrow.NewMetadata([]string{"ARROW:extension:name"}, []string{"geoarrow.point"}))),
		"logical struct":   serializeTable(nested(false, arrow.NewMetadata([]string{"vgi.logical_type.name"}, []string{"x"}))),
		"row id exempt": serializeTable(arrow.NewSchema([]arrow.Field{{Name: "rid", Type: i64,
			Metadata: arrow.NewMetadata([]string{"is_row_id"}, []string{""})}}, nil)),
	} {
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	wantErr(t, serializeTable(nested(false, arrow.Metadata{})), `table "t"`, "column s.x is non-nullable")
	wantErr(t, serializeTable(nested(true, arrow.Metadata{}), notNullOn(t, "s", "x")), "column s.x is in a NOT_NULL")
}

func TestLegacyConstraintFieldsNameTheReplacement(t *testing.T) {
	w := vgi.NewWorker(vgi.WithCatalogName("legacy"))
	w.RegisterCatalogTable("data", vgi.CatalogTable{Name: "t", NotNull: []string{"id"},
		Columns: arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)})
	client := newContentsClient(t, w)
	attach := client.attach("legacy")
	wantErr(t, client.contentsErr(attach.AttachOpaqueData, nil), `"t"`, "vgi.ConstraintRecord")
	_, err := vgi.SerializeTableInfo(&vgi.TableInfo{Name: "t", NotNullConstraints: []int32{0}})
	wantErr(t, err, `"t"`, "vgi.ConstraintRecord")
}

func clientCapabilities(t *testing.T, read ...string) []byte {
	read = append([]string{}, read...)
	offer := mustIPC(t, jsonRow(t, generated.CatalogCapabilitiesSchema, map[string]any{
		"read": read, "ddl": []string{}, "write": []string{"vgi.constraint.enforce.v1"}, "transaction": []string{},
	}))
	return mustIPC(t, jsonRow(t, generated.ClientCapabilitiesSchema, map[string]any{
		"engine": "duckdb", "native_formats": []string{}, "catalogs": []string{}, "can_stream": true,
		"filter_encodings": []string{}, "catalog": offer,
	}))
}

// attachProfiles runs catalog_attach and returns (advertised, selected)
// capability records; nil when the result left them NULL.
func attachProfiles(t *testing.T, w *vgi.Worker, name string, client []byte) (*generated.CatalogCapabilities, *generated.CatalogCapabilities) {
	t.Helper()
	row := map[string]any{"name": name}
	if client != nil {
		row["client_capabilities"] = client
	}
	request := mustIPC(t, jsonRow(t, generated.CatalogAttachRequestSchema, row))
	result := newContentsClient(t, w).call("catalog_attach", generated.CatalogAttachParamsSchema, map[string]any{"request": request})
	defer result.Release()
	decode := func(column string) *generated.CatalogCapabilities {
		values := result.Column(result.Schema().FieldIndices(column)[0]).(*array.Binary)
		if values.IsNull(0) {
			return nil
		}
		batch, err := vgi.DeserializeRecordBatch(values.Value(0))
		if err != nil {
			t.Fatal(err)
		}
		defer batch.Release()
		list := func(name string) []string {
			l := batch.Column(batch.Schema().FieldIndices(name)[0]).(*array.List)
			start, end := l.ValueOffsets(0)
			out := []string{}
			for i := start; i < end; i++ {
				out = append(out, l.ListValues().(*array.String).Value(int(i)))
			}
			return out
		}
		return &generated.CatalogCapabilities{Read: list("read"), Ddl: list("ddl"), Write: list("write"), Transaction: list("transaction")}
	}
	return decode("catalog_capabilities"), decode("selected_capabilities")
}

func constrainedWorker(t *testing.T, opts ...vgi.WorkerOption) *vgi.Worker {
	w := vgi.NewWorker(append([]vgi.WorkerOption{vgi.WithCatalogName("records")}, opts...)...)
	w.RegisterCatalogTable("data", vgi.CatalogTable{Name: "t",
		Columns:     arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil),
		Constraints: [][]byte{notNullOn(t, "id")}})
	return w
}

// A declarative catalog whose tables carry records negotiates the read
// profile itself, without an attach validator.
func TestDeclarativeRecordsNegotiateReadProfile(t *testing.T) {
	both := []string{vgi.ConstraintRecordsReadProfile, vgi.ScalarExpressionReadProfile}
	cases := []struct {
		name   string
		client []byte
		want   []string
	}{
		{"offered both", clientCapabilities(t, vgi.ScalarExpressionReadProfile, vgi.ConstraintRecordsReadProfile), both},
		{"offered records", clientCapabilities(t, vgi.ConstraintRecordsReadProfile), []string{vgi.ConstraintRecordsReadProfile}},
		// Scalar expressions are selectable only alongside records.
		{"offered scalar only", clientCapabilities(t, vgi.ScalarExpressionReadProfile), []string{}},
		{"offered nothing", clientCapabilities(t), []string{}},
		{"no client capabilities", nil, []string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			advertised, selected := attachProfiles(t, constrainedWorker(t), "records", tc.client)
			if advertised == nil || !slices.Equal(advertised.Read, both) {
				t.Fatalf("advertised %v, want read %v", advertised, both)
			}
			if selected == nil || !slices.Equal(selected.Read, tc.want) || len(selected.Write) != 0 {
				t.Fatalf("selected %v, want read %v", selected, tc.want)
			}
		})
	}
	// Without records nothing is advertised.
	plain := vgi.NewWorker(vgi.WithCatalogName("plain"))
	plain.RegisterCatalogTable("data", vgi.CatalogTable{Name: "t",
		Columns: arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)})
	if advertised, selected := attachProfiles(t, plain, "plain", clientCapabilities(t, vgi.ConstraintRecordsReadProfile)); advertised != nil || selected != nil {
		t.Fatalf("catalog without records negotiated %v / %v", advertised, selected)
	}
}

// The SDK's selection is merged with an attach validator's decision rather
// than replacing it.
func TestReadProfilesMergeWithAttachValidator(t *testing.T) {
	record := func(c generated.CatalogCapabilities) []byte {
		return mustIPC(t, jsonRow(t, generated.CatalogCapabilitiesSchema, map[string]any{
			"read": c.Read, "ddl": c.Ddl, "write": c.Write, "transaction": c.Transaction}))
	}
	enforce := []string{"vgi.constraint.enforce.v1"}
	w := constrainedWorker(t, vgi.WithAttachValidator(func(*vgi.CatalogAttachRequestWire, *vgirpc.CallContext) (*vgi.AttachDecision, error) {
		return &vgi.AttachDecision{
			CatalogCapabilities:  record(generated.CatalogCapabilities{Read: []string{vgi.ConstraintRecordsReadProfile}, Ddl: []string{}, Write: enforce, Transaction: []string{}}),
			SelectedCapabilities: record(generated.CatalogCapabilities{Read: []string{}, Ddl: []string{}, Write: enforce, Transaction: []string{}}),
		}, nil
	}))
	advertised, selected := attachProfiles(t, w, "records", clientCapabilities(t, vgi.ConstraintRecordsReadProfile, vgi.ScalarExpressionReadProfile))
	both := []string{vgi.ConstraintRecordsReadProfile, vgi.ScalarExpressionReadProfile}
	if !slices.Equal(advertised.Read, both) || !slices.Equal(advertised.Write, enforce) {
		t.Fatalf("advertised %+v", advertised)
	}
	if !slices.Equal(selected.Read, both) || !slices.Equal(selected.Write, enforce) {
		t.Fatalf("selected %+v", selected)
	}
}

func TestSelectReadProfiles(t *testing.T) {
	got, err := vgi.SelectReadProfiles([]string{vgi.ScalarExpressionReadProfile, vgi.ConstraintRecordsReadProfile, "vgi.index.read.v1"},
		clientCapabilities(t, vgi.ConstraintRecordsReadProfile, vgi.ScalarExpressionReadProfile))
	if err != nil || !slices.Equal(got, []string{vgi.ConstraintRecordsReadProfile, vgi.ScalarExpressionReadProfile}) {
		t.Fatalf("got %v, %v", got, err)
	}
	if _, err := vgi.SelectReadProfiles([]string{vgi.ConstraintRecordsReadProfile}, []byte("garbage")); err == nil {
		t.Fatal("malformed client capabilities were accepted")
	}
}
