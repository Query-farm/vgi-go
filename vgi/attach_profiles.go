// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"
	"slices"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// ConstraintCreateProfile is the catalog DDL profile under which a catalog
// accepts CREATE TABLE with canonical constraint definitions.
const ConstraintCreateProfile = "vgi.constraint.create.v1"

// ConstraintEnforceProfile is the catalog write profile under which a
// catalog states it enforces its constraint records on every write path. The
// DuckDB extension requires it before writing to a table that has records.
const ConstraintEnforceProfile = "vgi.constraint.enforce.v1"

// CatalogCapabilities lists catalog profiles by category; it is the
// CatalogCapabilities protocol record carried by
// AttachDecision.CatalogCapabilities / SelectedCapabilities.
type CatalogCapabilities = generated.CatalogCapabilities

// EncodeCatalogCapabilities serializes a CatalogCapabilities record. Nil
// categories encode as empty lists; each list is sorted and deduplicated.
func EncodeCatalogCapabilities(c CatalogCapabilities) ([]byte, error) {
	return encodeCanonicalRow(generated.CatalogCapabilitiesSchema, map[string]any{
		"read": sortedUnique(slices.Clone(nonNilStrings(c.Read))), "ddl": sortedUnique(slices.Clone(nonNilStrings(c.Ddl))),
		"write": sortedUnique(slices.Clone(nonNilStrings(c.Write))), "transaction": sortedUnique(slices.Clone(nonNilStrings(c.Transaction))),
	})
}

// DecodeCatalogCapabilities reads a CatalogCapabilities record. Empty input
// decodes as no profiles.
func DecodeCatalogCapabilities(data []byte) (CatalogCapabilities, error) {
	return decodeCapabilities(&data)
}

// SelectCatalogCapabilities returns the profiles of available that the
// client offered in ClientCapabilities.catalog, category by category, with
// the extension's prerequisites applied: the scalar-expression read and every
// constraint DDL/write profile are selected only alongside
// ConstraintRecordsReadProfile. A nil clientCapabilities (an older client)
// selects nothing.
func SelectCatalogCapabilities(available CatalogCapabilities, clientCapabilities []byte) (CatalogCapabilities, error) {
	selected := CatalogCapabilities{Read: []string{}, Ddl: []string{}, Write: []string{}, Transaction: []string{}}
	if len(clientCapabilities) == 0 {
		return selected, nil
	}
	offer, err := clientCatalogOffer(clientCapabilities)
	if err != nil || offer == nil {
		return selected, err
	}
	pick := func(available, offered []string) []string {
		out := []string{}
		for _, profile := range available {
			if slices.Contains(offered, profile) {
				out = append(out, profile)
			}
		}
		return sortedUnique(out)
	}
	selected.Read = finishReadSelection(pick(available.Read, offer.Read))
	selected.Ddl = pick(available.Ddl, offer.Ddl)
	selected.Write = pick(available.Write, offer.Write)
	selected.Transaction = pick(available.Transaction, offer.Transaction)
	if !slices.Contains(selected.Read, ConstraintRecordsReadProfile) {
		drop := func(p string) bool {
			return p == ConstraintCreateProfile || p == ConstraintEnforceProfile || p == "vgi.constraint.mutate.v1"
		}
		selected.Ddl = slices.DeleteFunc(selected.Ddl, drop)
		selected.Write = slices.DeleteFunc(selected.Write, drop)
	}
	return selected, nil
}

// ReadProfilesDecision encodes the capability pair an AttachValidator
// returns (AttachDecision.CatalogCapabilities and SelectedCapabilities) to
// advertise the given read profiles and select those the client offered. A
// validator serving its own tables passes ConstraintRecordsReadProfile (and
// ScalarExpressionReadProfile when they carry CHECK records), typically with
// req.ClientCapabilities.
func ReadProfilesDecision(clientCapabilities []byte, profiles ...string) (available, selected []byte, err error) {
	advertised := CatalogCapabilities{Read: profiles}
	chosen, err := SelectCatalogCapabilities(advertised, clientCapabilities)
	if err != nil {
		return nil, nil, err
	}
	if available, err = EncodeCatalogCapabilities(advertised); err != nil {
		return nil, nil, err
	}
	if selected, err = EncodeCatalogCapabilities(chosen); err != nil {
		return nil, nil, err
	}
	return available, selected, nil
}

// WithConstraintReadProfile makes the worker's default catalog advertise and
// (when the client offers them) select ConstraintRecordsReadProfile and
// ScalarExpressionReadProfile on every ATTACH. The SDK negotiates them by
// itself when a registered CatalogTable carries Constraints; use this option
// when records are served some other way — WithSchemaContentsHandler,
// WithAttachTableGetHandler, a TableGetHandler or a catalog_contents handler —
// where the SDK cannot see them at ATTACH. Without the profile DuckDB refuses
// every table that carries a record.
func WithConstraintReadProfile() WorkerOption {
	return func(w *Worker) {
		w.constraintReadProfile = true
	}
}

// constraintReadProfiles are the profiles a catalog that serves records reads under.
var constraintReadProfiles = []string{ConstraintRecordsReadProfile, ScalarExpressionReadProfile}

// declarativeReadProfiles returns the read profiles the declarative catalog
// tables served to an attach of name need: constraint records (with scalar
// CHECK expressions) when any of them carries Constraints, or always under
// WithConstraintReadProfile. Alias-info catalogs see only the "main" schema,
// as listSchemaInfos serves them.
func (w *Worker) declarativeReadProfiles(name string) []string {
	if w.constraintReadProfile {
		return constraintReadProfiles
	}
	_, aliasOnly := w.catalogAliasInfos[name]
	for schema, tables := range w.catalogTables {
		if aliasOnly && schema != "main" {
			continue
		}
		for i := range tables {
			if len(tables[i].Constraints) > 0 {
				return constraintReadProfiles
			}
		}
	}
	return nil
}

// mergeReadProfiles adds the SDK's own read profiles to an attach result's
// capability records without replacing what an attach validator decided:
// the advertised read list is the union, and the selected read list is the
// validator's selection plus the client-offered SDK profiles. Other
// categories (ddl/write/transaction) pass through unchanged. With no SDK
// profiles the validator's bytes are returned untouched.
func mergeReadProfiles(sdk []string, available, selected *[]byte, clientCapabilities *[]byte) (*[]byte, *[]byte, error) {
	return mergeCapabilities(CatalogCapabilities{Read: sdk}, available, selected, clientCapabilities)
}

// mergeCapabilities is mergeReadProfiles for every category.
func mergeCapabilities(sdk CatalogCapabilities, available, selected *[]byte, clientCapabilities *[]byte) (*[]byte, *[]byte, error) {
	if len(sdk.Read)+len(sdk.Ddl)+len(sdk.Write)+len(sdk.Transaction) == 0 {
		return available, selected, nil
	}
	advertised, err := decodeCapabilities(available)
	if err != nil {
		return nil, nil, fmt.Errorf("attach validator catalog_capabilities: %w", err)
	}
	chosen, err := decodeCapabilities(selected)
	if err != nil {
		return nil, nil, fmt.Errorf("attach validator selected_capabilities: %w", err)
	}
	var client []byte
	if clientCapabilities != nil {
		client = *clientCapabilities
	}
	picked, err := SelectCatalogCapabilities(sdk, client)
	if err != nil {
		return nil, nil, err
	}
	union := func(a, b []string) []string { return sortedUnique(append(slices.Clone(a), b...)) }
	advertised.Read = union(advertised.Read, sdk.Read)
	advertised.Ddl = union(advertised.Ddl, sdk.Ddl)
	advertised.Write = union(advertised.Write, sdk.Write)
	advertised.Transaction = union(advertised.Transaction, sdk.Transaction)
	chosen.Read = finishReadSelection(union(chosen.Read, picked.Read))
	chosen.Ddl = union(chosen.Ddl, picked.Ddl)
	chosen.Write = union(chosen.Write, picked.Write)
	chosen.Transaction = union(chosen.Transaction, picked.Transaction)
	a, err := EncodeCatalogCapabilities(advertised)
	if err != nil {
		return nil, nil, err
	}
	s, err := EncodeCatalogCapabilities(chosen)
	if err != nil {
		return nil, nil, err
	}
	return &a, &s, nil
}

func sortedUnique(values []string) []string {
	slices.Sort(values)
	return slices.Compact(values)
}

// clientCatalogOffer reads ClientCapabilities.catalog; nil when absent.
func clientCatalogOffer(clientCapabilities []byte) (*CatalogCapabilities, error) {
	client, err := readCanonicalRow(clientCapabilities, generated.ClientCapabilitiesSchema)
	if err != nil {
		return nil, fmt.Errorf("ClientCapabilities: %w", err)
	}
	defer client.Release()
	catalog := rowColumn(client, "catalog")
	if catalog.IsNull(0) {
		return nil, nil
	}
	value := rowBinary(client, "catalog")
	offer, err := decodeCapabilities(&value)
	if err != nil {
		return nil, fmt.Errorf("CatalogCapabilities: %w", err)
	}
	return &offer, nil
}

func decodeCapabilities(data *[]byte) (generated.CatalogCapabilities, error) {
	out := generated.CatalogCapabilities{Read: []string{}, Ddl: []string{}, Write: []string{}, Transaction: []string{}}
	if data == nil || len(*data) == 0 {
		return out, nil
	}
	row, err := readCanonicalRow(*data, generated.CatalogCapabilitiesSchema)
	if err != nil {
		return out, err
	}
	defer row.Release()
	out.Read, out.Ddl = rowStrings(row, "read"), rowStrings(row, "ddl")
	out.Write, out.Transaction = rowStrings(row, "write"), rowStrings(row, "transaction")
	return out, nil
}
