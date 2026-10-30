// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"
	"slices"

	"github.com/Query-farm/vgi-go/vgi/generated"
)

// declarativeReadProfiles returns the read profiles the declarative catalog
// tables served to an attach of name need: constraint records (with scalar
// CHECK expressions) when any of them carries Constraints. Alias-info
// catalogs see only the "main" schema, as listSchemaInfos serves them.
func (w *Worker) declarativeReadProfiles(name string) []string {
	_, aliasOnly := w.catalogAliasInfos[name]
	for schema, tables := range w.catalogTables {
		if aliasOnly && schema != "main" {
			continue
		}
		for i := range tables {
			if len(tables[i].Constraints) > 0 {
				return []string{ConstraintRecordsReadProfile, ScalarExpressionReadProfile}
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
	if len(sdk) == 0 {
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
	picked, err := SelectReadProfiles(sdk, client)
	if err != nil {
		return nil, nil, err
	}
	advertised.Read = sortedUnique(append(slices.Clone(advertised.Read), sdk...))
	chosen.Read = finishReadSelection(append(slices.Clone(chosen.Read), picked...))
	a, err := encodeCapabilities(advertised)
	if err != nil {
		return nil, nil, err
	}
	s, err := encodeCapabilities(chosen)
	if err != nil {
		return nil, nil, err
	}
	return &a, &s, nil
}

func sortedUnique(values []string) []string {
	slices.Sort(values)
	return slices.Compact(values)
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

func encodeCapabilities(c generated.CatalogCapabilities) ([]byte, error) {
	return encodeCanonicalRow(generated.CatalogCapabilitiesSchema, map[string]any{
		"read": nonNilStrings(c.Read), "ddl": nonNilStrings(c.Ddl),
		"write": nonNilStrings(c.Write), "transaction": nonNilStrings(c.Transaction),
	})
}
