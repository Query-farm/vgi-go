// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi_test

import (
	"context"
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
		enforced(vgi.ConstraintRecordSpec{SchemaPath: []string{"data"}, Table: "t", ID: "t:u", Kind: vgi.ConstraintUnique,
			Columns: [][]string{{"email"}}}),
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
		// What the DuckDB extension refuses at ATTACH is refused up front.
		"metadata only":        func(s *vgi.ConstraintRecordSpec) { s.Enforcement = vgi.ConstraintMetadataOnly },
		"future writes":        func(s *vgi.ConstraintRecordSpec) { s.Validation = vgi.ConstraintFutureWrites },
		"unvalidated":          func(s *vgi.ConstraintRecordSpec) { s.Validation = vgi.ConstraintUnvalidated },
		"no schema path":       func(s *vgi.ConstraintRecordSpec) { s.SchemaPath = nil },
		"two-part schema path": func(s *vgi.ConstraintRecordSpec) { s.SchemaPath = []string{"a", "b"} },
		"duplicate path": func(s *vgi.ConstraintRecordSpec) {
			s.Kind, s.Columns = vgi.ConstraintUnique, [][]string{{"a"}, {"a"}}
		},
		"nested unique path": func(s *vgi.ConstraintRecordSpec) {
			s.Kind, s.Columns = vgi.ConstraintUnique, [][]string{{"s", "x"}}
		},
		"cross-schema fk": func(s *vgi.ConstraintRecordSpec) {
			s.Kind = vgi.ConstraintForeignKey
			s.References = &vgi.ConstraintReference{SchemaPath: []string{"other"}, Table: "u", Columns: [][]string{{"a"}}, ConstraintID: "u:pk"}
		},
		"check outside grammar": func(s *vgi.ConstraintRecordSpec) {
			s.Kind, s.Columns, s.CheckSQL = vgi.ConstraintCheck, nil, "x > 0 AND x < 10"
		},
		"check function": func(s *vgi.ConstraintRecordSpec) {
			s.Kind, s.Columns, s.CheckSQL = vgi.ConstraintCheck, nil, "abs(x) > 0"
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

// The builder refuses records DuckDB never reads; a raw record that slips
// past it (another producer) is refused when served.
func TestServedRecordsMustBeEnforcedAllRows(t *testing.T) {
	columns := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	for _, state := range [][2]string{{"METADATA_ONLY", "ALL_ROWS"}, {"ENFORCED", "FUTURE_WRITES"}, {"ENFORCED", "UNVALIDATED"}} {
		spec := vgi.ConstraintRecordSpec{SchemaPath: []string{"data"}, Table: "t", ID: "t:u",
			Kind: vgi.ConstraintUnique, Columns: [][]string{{"id"}},
			Enforcement: vgi.ConstraintEnforcement(state[0]), Validation: vgi.ConstraintValidation(state[1])}
		_, err := vgi.ConstraintRecord(spec)
		wantErr(t, err, `"t:u"`, state[0]+"/"+state[1], "ENFORCED + ALL_ROWS")
		wantErr(t, serializeTable(columns, vgi.EncodeConstraintRecordUnchecked(t, spec)), `table "t"`, `"t:u"`, state[0]+"/"+state[1], "ENFORCED + ALL_ROWS")
	}
	ok := mustRecord(t, enforced(vgi.ConstraintRecordSpec{SchemaPath: []string{"data"}, Table: "t", ID: "t:u",
		Kind: vgi.ConstraintUnique, Columns: [][]string{{"id"}}}))
	if err := serializeTable(columns, ok); err != nil {
		t.Fatal(err)
	}
}

// servedColumns serializes a table and returns the columns it was served with.
func servedColumns(t *testing.T, columns *arrow.Schema, constraints ...[]byte) *arrow.Schema {
	t.Helper()
	data, err := vgi.SerializeTableInfo(&vgi.TableInfo{Name: "t", SchemaPath: []string{"data"}, Columns: columns, Constraints: constraints})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := vgi.DeserializeRecordBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()
	schema, err := vgi.DeserializeSchema(batch.Column(batch.Schema().FieldIndices("columns")[0]).(*array.Binary).Value(0))
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

// Nullability is served from the records (relax policy): a field is
// non-nullable exactly when a NOT_NULL/PRIMARY_KEY record names it.
func TestServedNullabilityFollowsRecords(t *testing.T) {
	i64 := arrow.PrimitiveTypes.Int64
	nested := func(childNullable bool, metadata arrow.Metadata) *arrow.Schema {
		return arrow.NewSchema([]arrow.Field{{Name: "s", Nullable: true, Metadata: metadata,
			Type: arrow.StructOf(arrow.Field{Name: "x", Type: i64, Nullable: childNullable})}}, nil)
	}
	required := arrow.NewSchema([]arrow.Field{{Name: "id", Type: i64}}, nil)
	optional := arrow.NewSchema([]arrow.Field{{Name: "id", Type: i64, Nullable: true}}, nil)
	pk := mustRecord(t, enforced(vgi.ConstraintRecordSpec{SchemaPath: []string{"data"}, Table: "t", ID: "t:pk",
		Kind: vgi.ConstraintPrimaryKey, Columns: [][]string{{"id"}}}))
	child := func(s *arrow.Schema) arrow.Field { return s.Field(0).Type.(*arrow.StructType).Field(0) }

	// An unattested non-nullable field is relaxed, not refused.
	if got := servedColumns(t, required); !got.Field(0).Nullable {
		t.Fatal("unattested non-nullable column was not served nullable")
	}
	if got := servedColumns(t, nested(false, arrow.Metadata{})); !child(got).Nullable {
		t.Fatal("unattested non-nullable struct child was not served nullable")
	}
	// A record-named field is served non-nullable, whatever Arrow said.
	for name, got := range map[string]*arrow.Schema{
		"not null record on nullable": servedColumns(t, optional, notNullOn(t, "id")),
		"pk record on nullable":       servedColumns(t, optional, pk),
		"not null record on required": servedColumns(t, required, notNullOn(t, "id")),
		"nested record on nullable":   nested(false, arrow.Metadata{}),
	} {
		if name == "nested record on nullable" {
			if c := child(servedColumns(t, nested(true, arrow.Metadata{}), notNullOn(t, "s", "x"))); c.Nullable {
				t.Errorf("%s: served nullable", name)
			}
			continue
		}
		if got.Field(0).Nullable {
			t.Errorf("%s: served nullable", name)
		}
	}
	// Storage members of a logical type and the row-id column are untouched.
	for _, md := range []arrow.Metadata{
		arrow.NewMetadata([]string{"ARROW:extension:name"}, []string{"geoarrow.point"}),
		arrow.NewMetadata([]string{"vgi.logical_type.name"}, []string{"x"}),
	} {
		if child(servedColumns(t, nested(false, md))).Nullable {
			t.Errorf("logical-type storage child %v was rewritten", md)
		}
	}
	rowID := arrow.NewSchema([]arrow.Field{{Name: "v", Type: i64},
		{Name: "rid", Type: i64, Metadata: arrow.NewMetadata([]string{"is_row_id"}, []string{""})}}, nil)
	if got := servedColumns(t, rowID); got.Field(1).Nullable || !got.Field(0).Nullable {
		t.Fatalf("row id must stay as declared while others relax: %v", got)
	}
	// The caller's schema is never mutated.
	if required.Field(0).Nullable {
		t.Fatal("SerializeTableInfo mutated the caller's schema")
	}
	// A path that does not resolve is refused, naming table, id and path.
	wantErr(t, serializeTable(optional, notNullOn(t, "missing")), `table "t"`, `"nn:missing"`, "missing")
	wantErr(t, serializeTable(nested(true, arrow.Metadata{}), notNullOn(t, "s", "y")), `"nn:s.y"`, "nested field")
}

// A function-backed table's bind output is served with the same policy.
func TestFunctionBackedTableServedNullable(t *testing.T) {
	w := vgi.NewWorker(vgi.WithCatalogName("fb"))
	seq := vgi.AsTableFunction[struct{}](&requiredOutputTable{})
	w.RegisterTable(seq)
	w.RegisterCatalogTable("main", vgi.CatalogTable{Name: "nums", Function: seq, InlineBind: true})
	client := newContentsClient(t, w)
	attach := client.attach("fb")
	items := client.items("catalog_schema_contents_tables", generated.CatalogSchemaContentsTablesParamsSchema,
		map[string]any{"attach_opaque_data": attach.AttachOpaqueData, "path": []string{"main"}})
	if len(items) != 1 {
		t.Fatalf("got %d tables", len(items))
	}
	batch, err := vgi.DeserializeRecordBatch(items[0])
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()
	schema, err := vgi.DeserializeSchema(batch.Column(batch.Schema().FieldIndices("columns")[0]).(*array.Binary).Value(0))
	if err != nil {
		t.Fatal(err)
	}
	if !schema.Field(0).Nullable || !schema.Field(1).Type.(*arrow.StructType).Field(0).Nullable {
		t.Fatalf("function-backed output was not served nullable: %v", schema)
	}
}

type requiredOutputTable struct{}

func (*requiredOutputTable) Name() string                   { return "required_output" }
func (*requiredOutputTable) Metadata() vgi.FunctionMetadata { return vgi.FunctionMetadata{} }
func (*requiredOutputTable) ArgumentSpecs() []vgi.ArgSpec   { return nil }
func (*requiredOutputTable) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: arrow.StructOf(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64})}}, nil))
}
func (*requiredOutputTable) NewState(*vgi.ProcessParams) (*struct{}, error) { return &struct{}{}, nil }
func (*requiredOutputTable) Process(context.Context, *vgi.ProcessParams, *struct{}, *vgirpc.OutputCollector) error {
	return nil
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

func checkRecord(t *testing.T, sql string) []byte {
	return vgi.EncodeConstraintRecordUnchecked(t, enforced(vgi.ConstraintRecordSpec{SchemaPath: []string{"data"}, Table: "t",
		ID: "ck", Kind: vgi.ConstraintCheck, CheckSQL: sql}))
}

// CHECK records are served only inside the extension's numeric comparison
// grammar (ParseNumericCheck).
func TestCheckRecordGrammar(t *testing.T) {
	columns := arrow.NewSchema([]arrow.Field{
		{Name: "Budget", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "ratio", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "count", Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: "label", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	for _, sql := range []string{"budget >= 0", "0 < ratio", "\"Budget\" <> -5", "(budget = 1.5)", "budget != ratio", "ratio <= 1e3", "budget == 2"} {
		if err := serializeTable(columns, checkRecord(t, sql)); err != nil {
			t.Errorf("%q: %v", sql, err)
		}
	}
	for sql, want := range map[string]string{
		"budget > 0 AND ratio > 0": "comparison",
		"abs(budget) > 0":          "comparison",
		"budget + 1 > 0":           "comparison",
		"budget::INT > 0":          "comparison",
		"budget > NULL":            "numeric operands",
		"budget > '5'":             "numeric operands",
		"count > 0":                "signed numeric",
		"label > 0":                "signed numeric",
		"missing > 0":              "missing",
		"t.budget > 0":             "qualified",
		"budget IN (1, 2)":         "IN",
		"budget BETWEEN 1 AND 2":   "BETWEEN",
		"budget > 0 AS ok":         "aliases",
		"budget":                   "comparison",
	} {
		wantErr(t, serializeTable(columns, checkRecord(t, sql)), `"ck"`, want)
	}
}

// Generated columns follow the extension's VIRTUAL restrictions.
func TestGeneratedColumnRestrictions(t *testing.T) {
	i64, str := arrow.PrimitiveTypes.Int64, arrow.BinaryTypes.String
	serve := func(expr string, generatedType arrow.DataType, constraints ...[]byte) error {
		schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: i64}, {Name: "s", Type: str},
			{Name: "f", Type: arrow.PrimitiveTypes.Float64}, {Name: "g", Type: generatedType}}, nil)
		w := vgi.NewWorker(vgi.WithCatalogName("gen"))
		w.RegisterCatalogTable("data", vgi.CatalogTable{Name: "t", Columns: schema, Generated: map[string]string{"g": expr}, Constraints: constraints})
		client := newContentsClient(t, w)
		return client.contentsErr(client.attach("gen").AttachOpaqueData, nil)
	}
	for expr, typ := range map[string]arrow.DataType{
		"n * 2": i64, "abs(n) % 3": i64, "-n": i64, "upper(s) || 'x'": str, "coalesce(s, 'none')": str,
		"n > 1 AND s IS NOT NULL": arrow.FixedWidthTypes.Boolean, "CAST(n AS VARCHAR)": str, "n::BIGINT + 1": i64, "NOT (n = 2)": arrow.FixedWidthTypes.Boolean,
	} {
		if err := serve(expr, typ); err != nil {
			t.Errorf("%q: %v", expr, err)
		}
	}
	for expr, want := range map[string]string{
		"n / 2":                             "unsupported scalar function",
		"n * 1.5":                           "literal type DECIMAL",
		"round(n)":                          "unsupported scalar function",
		"TRY_CAST(n AS BIGINT)":             "TRY_CAST",
		"CAST(n AS DOUBLE)":                 "unsupported scalar cast",
		"f + 1":                             "operand type",
		"g + 1":                             "itself",
		"missing + 1":                       "unknown column",
		"CASE WHEN n > 0 THEN 1 ELSE 0 END": "CASE",
	} {
		wantErr(t, serve(expr, i64), want)
	}
	wantErr(t, serve("n * 2", arrow.PrimitiveTypes.Float64), "BOOLEAN, TINYINT..BIGINT or VARCHAR")
	// A generated column is served nullable; a NOT_NULL record cannot name it.
	gRecord := mustRecord(t, enforced(vgi.ConstraintRecordSpec{SchemaPath: []string{"data"}, Table: "t", ID: "g:nn",
		Kind: vgi.ConstraintNotNull, Columns: [][]string{{"g"}}}))
	wantErr(t, serve("n * 2", i64, gRecord), "generated", "nullable")
}

// The table-level checks the extension runs at ATTACH.
func TestServedRecordSetChecks(t *testing.T) {
	columns := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int64}, {Name: "b", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rec := func(spec vgi.ConstraintRecordSpec) []byte {
		if spec.SchemaPath == nil {
			spec.SchemaPath = []string{"data"}
		}
		if spec.Table == "" {
			spec.Table = "t"
		}
		return mustRecord(t, enforced(spec))
	}
	pkA := rec(vgi.ConstraintRecordSpec{ID: "pk", Kind: vgi.ConstraintPrimaryKey, Columns: [][]string{{"a"}}, DeclaredName: "Key"})
	wantErr(t, serializeTable(columns, pkA, rec(vgi.ConstraintRecordSpec{ID: "pk", Kind: vgi.ConstraintUnique, Columns: [][]string{{"b"}}})),
		`"pk"`, "duplicate constraint id")
	wantErr(t, serializeTable(columns, pkA, rec(vgi.ConstraintRecordSpec{ID: "u", Kind: vgi.ConstraintUnique, Columns: [][]string{{"b"}}, DeclaredName: "KEY"})),
		`"u"`, "case-insensitively")
	wantErr(t, serializeTable(columns, pkA, rec(vgi.ConstraintRecordSpec{ID: "pk2", Kind: vgi.ConstraintPrimaryKey, Columns: [][]string{{"b"}}})),
		`"pk2"`, "at most one PRIMARY KEY")
	wantErr(t, serializeTable(columns, rec(vgi.ConstraintRecordSpec{Table: "other", ID: "x", Kind: vgi.ConstraintUnique, Columns: [][]string{{"a"}}})),
		`"x"`, "not the serving table")
	wantErr(t, serializeTable(columns, rec(vgi.ConstraintRecordSpec{SchemaPath: []string{"elsewhere"}, ID: "x", Kind: vgi.ConstraintUnique, Columns: [][]string{{"a"}}})),
		`"x"`, "not the serving table")
	wantErr(t, serializeTable(columns, rec(vgi.ConstraintRecordSpec{ID: "u", Kind: vgi.ConstraintUnique, Columns: [][]string{{"c"}}})),
		`"u"`, "column c is missing")
}

// A declarative FOREIGN KEY must name a key its target table declares.
func TestDeclarativeForeignKeyTargets(t *testing.T) {
	ids := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	fk := func(target string) []byte {
		return mustRecord(t, enforced(vgi.ConstraintRecordSpec{SchemaPath: []string{"data"}, Table: "child", ID: "fk",
			Kind: vgi.ConstraintForeignKey, Columns: [][]string{{"id"}},
			References: &vgi.ConstraintReference{Table: "parent", Columns: [][]string{{"id"}}, ConstraintID: target}}))
	}
	build := func(target string) error {
		w := vgi.NewWorker(vgi.WithCatalogName("fk"))
		w.RegisterCatalogTable("data", vgi.CatalogTable{Name: "parent", Columns: ids, Constraints: [][]byte{mustRecord(t, enforced(vgi.ConstraintRecordSpec{
			SchemaPath: []string{"data"}, Table: "parent", ID: "parent:pk", Kind: vgi.ConstraintPrimaryKey, Columns: [][]string{{"id"}}}))}})
		w.RegisterCatalogTable("data", vgi.CatalogTable{Name: "child", Columns: ids, Constraints: [][]byte{fk(target)}})
		client := newContentsClient(t, w)
		return client.contentsErr(client.attach("fk").AttachOpaqueData, nil)
	}
	if err := build("parent:pk"); err != nil {
		t.Fatal(err)
	}
	wantErr(t, build("parent:nope"), `"fk"`, `"parent:nope"`, "does not declare")
}

// Handler-served records: WithConstraintReadProfile negotiates on every attach.
func TestWithConstraintReadProfile(t *testing.T) {
	both := []string{vgi.ConstraintRecordsReadProfile, vgi.ScalarExpressionReadProfile}
	w := vgi.NewWorker(vgi.WithCatalogName("handler"), vgi.WithConstraintReadProfile())
	advertised, selected := attachProfiles(t, w, "handler", clientCapabilities(t, both...))
	if advertised == nil || !slices.Equal(advertised.Read, both) || selected == nil || !slices.Equal(selected.Read, both) {
		t.Fatalf("advertised %v selected %v", advertised, selected)
	}
}

func TestReadProfilesDecision(t *testing.T) {
	available, selected, err := vgi.ReadProfilesDecision(clientCapabilities(t, vgi.ConstraintRecordsReadProfile), vgi.ConstraintRecordsReadProfile, vgi.ScalarExpressionReadProfile)
	if err != nil {
		t.Fatal(err)
	}
	a, err := vgi.DecodeCatalogCapabilities(available)
	if err != nil {
		t.Fatal(err)
	}
	s, err := vgi.DecodeCatalogCapabilities(selected)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Read) != 2 || !slices.Equal(s.Read, []string{vgi.ConstraintRecordsReadProfile}) {
		t.Fatalf("available %v selected %v", a, s)
	}
	// Constraint DDL/write profiles need the records read profile.
	picked, err := vgi.SelectCatalogCapabilities(vgi.CatalogCapabilities{Write: []string{vgi.ConstraintEnforceProfile}},
		clientCapabilities(t))
	if err != nil || len(picked.Write) != 0 {
		t.Fatalf("enforce selected without records read: %v, %v", picked, err)
	}
}

// A memory catalog accepts CREATE TABLE ... NOT NULL (its tables have no
// write path) and negotiates the profiles for it.
func TestMemoryCatalogNotNullCreate(t *testing.T) {
	host := vgi.NewWorker(vgi.WithCatalogName("host"))
	host.RegisterMemoryCatalog(vgi.NewMemoryCatalog("mem"))
	offer := mustIPC(t, jsonRow(t, generated.CatalogCapabilitiesSchema, map[string]any{
		"read": []string{vgi.ConstraintRecordsReadProfile}, "ddl": []string{vgi.ConstraintCreateProfile},
		"write": []string{vgi.ConstraintEnforceProfile}, "transaction": []string{},
	}))
	client := mustIPC(t, jsonRow(t, generated.ClientCapabilitiesSchema, map[string]any{
		"engine": "duckdb", "native_formats": []string{}, "catalogs": []string{}, "can_stream": true,
		"filter_encodings": []string{}, "catalog": offer,
	}))
	_, selected := attachProfiles(t, host, "mem", client)
	if selected == nil || !slices.Equal(selected.Ddl, []string{vgi.ConstraintCreateProfile}) || !slices.Equal(selected.Write, []string{vgi.ConstraintEnforceProfile}) {
		t.Fatalf("selected %+v", selected)
	}
}
