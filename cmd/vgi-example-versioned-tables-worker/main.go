// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Example VGI worker whose exposed tables vary per requested data version.
// Mirrors vgi-python's vgi-example-versioned-tables-worker so the shared
// integration tests (test/sql/integration/attach/versioned_tables*.test) can
// run against either implementation.
//
// Data-version-specific table layouts:
//
//	1.0.0 -> animals                      (name, legs, sound)
//	1.1.0 -> animals (with color column)  (name, legs, sound, color)
//	2.0.0 -> animals + plants
//	3.0.0 -> plants
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"

	ex "github.com/Query-farm/vgi-go/examples/versioned_tables"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

const (
	catalogName                   = "versioned_tables"
	dataVersionSpec               = ">=1.0.0,<4.0.0"
	defaultDataVersion            = "3.0.0"
	defaultImplementationVersion  = "11.0.0"
	stickyCookieName              = "vgi_sticky"
	attachIDSeparator             = 0x00
)

var (
	supportedDataVersions           = []string{"1.0.0", "1.1.0", "2.0.0", "3.0.0"}
	supportedImplementationVersions = []string{"10.0.0", "10.1.0", "11.0.0"}
)

// versionedTable names the scan function + schema for one (version, table) pair.
type versionedTable struct {
	FunctionName string
	Columns      *arrow.Schema
}

// versionTables maps resolved_data_version → table_name → versionedTable.
var versionTables = map[string]map[string]versionedTable{
	"1.0.0": {
		"animals": {"versioned_tables_animals_scan", ex.AnimalsSchemaV1},
	},
	"1.1.0": {
		"animals": {"versioned_tables_animals_color_scan", ex.AnimalsSchemaV11},
	},
	"2.0.0": {
		"animals": {"versioned_tables_animals_scan", ex.AnimalsSchemaV1},
		"plants":  {"versioned_tables_plants_scan", ex.PlantsSchema},
	},
	"3.0.0": {
		"plants": {"versioned_tables_plants_scan", ex.PlantsSchema},
	},
}

func main() {
	httpMode := flag.Bool("http", false, "Run as HTTP server instead of stdio")
	flag.Parse()

	implDefault := defaultImplementationVersion
	dvs := dataVersionSpec

	w := vgi.NewWorker(
		vgi.WithCatalogName(catalogName),
		vgi.WithCatalogInfo(vgi.CatalogInfo{
			Name:                  catalogName,
			ImplementationVersion: &implDefault,
			DataVersionSpec:       &dvs,
		}),
		vgi.WithAttachValidator(func(req *vgi.CatalogAttachRequestWire, ctx *vgirpc.CallContext) (*vgi.AttachDecision, error) {
			implSpec := ""
			if req.ImplementationVersion != nil {
				implSpec = *req.ImplementationVersion
			}
			dataSpec := ""
			if req.DataVersionSpec != nil {
				dataSpec = *req.DataVersionSpec
			}
			resolvedImpl, err := ex.Resolve(implSpec, supportedImplementationVersions, defaultImplementationVersion, "implementation_version")
			if err != nil {
				return nil, err
			}
			resolvedData, err := ex.Resolve(dataSpec, supportedDataVersions, defaultDataVersion, "data_version_spec")
			if err != nil {
				return nil, err
			}
			// Encode attach_id as <resolved_data_version>\x00<uuid16> so every
			// follow-up RPC can decode the version without per-worker state.
			attachID := make([]byte, 0, len(resolvedData)+1+16)
			attachID = append(attachID, []byte(resolvedData)...)
			attachID = append(attachID, attachIDSeparator)
			attachID = append(attachID, randomBytes(16)...)
			if ctx != nil {
				_ = ctx.SetCookie(stickyCookieName, randomHex(16), vgirpc.CookieAttrs{Path: "/"})
			}
			return &vgi.AttachDecision{
				ResolvedDataVersion:           resolvedData,
				ResolvedImplementationVersion: resolvedImpl,
				AttachID:                      attachID,
			}, nil
		}),
		vgi.WithSchemaContentsHandler(func(attachID []byte, schemaName string) ([]vgi.SerializedSchemaItem, bool) {
			if schemaName != "main" {
				return []vgi.SerializedSchemaItem{}, true
			}
			tables := tablesForAttachID(attachID)
			out := make([]vgi.SerializedSchemaItem, 0, len(tables))
			// Emit in alphabetical order for stable test output.
			for _, name := range sortedKeys(tables) {
				data, err := vgi.SerializeTableInfo(&vgi.TableInfo{
					Name:       name,
					SchemaName: "main",
					Columns:    tables[name].Columns,
				})
				if err != nil {
					log.Printf("serialize TableInfo %q failed: %v", name, err)
					continue
				}
				out = append(out, vgi.SerializedSchemaItem(data))
			}
			return out, true
		}),
		vgi.WithAttachTableGetHandler(func(attachID []byte, schemaName, name string, _ *string, _ *string) ([]byte, bool, error) {
			if schemaName != "main" {
				return nil, true, nil
			}
			table, ok := tablesForAttachID(attachID)[name]
			if !ok {
				return nil, true, nil
			}
			data, err := vgi.SerializeTableInfo(&vgi.TableInfo{
				Name:       name,
				SchemaName: "main",
				Columns:    table.Columns,
			})
			if err != nil {
				return nil, true, err
			}
			return data, true, nil
		}),
		vgi.WithAttachScanFunctionGetHandler(func(attachID []byte, schemaName, name string, _ *string, _ *string) (*vgi.ScanFunctionResult, bool, error) {
			if schemaName != "main" {
				return nil, true, fmt.Errorf("Unknown schema: %s", schemaName)
			}
			table, ok := tablesForAttachID(attachID)[name]
			if !ok {
				return nil, true, fmt.Errorf("Table main.%s not visible at this data version", name)
			}
			return &vgi.ScanFunctionResult{FunctionName: table.FunctionName}, true, nil
		}),
	)

	// Register the scan functions backing the versioned tables.
	w.RegisterTable(ex.NewAnimalsScanFunction())
	w.RegisterTable(ex.NewAnimalsColorScanFunction())
	w.RegisterTable(ex.NewPlantsScanFunction())

	if *httpMode {
		if err := w.RunHttp("127.0.0.1:0"); err != nil {
			log.Fatal(err)
		}
	} else {
		w.RunStdio()
	}
}

func tablesForAttachID(attachID []byte) map[string]versionedTable {
	sep := bytes.IndexByte(attachID, attachIDSeparator)
	if sep <= 0 {
		return nil
	}
	version := string(attachID[:sep])
	return versionTables[version]
}

func sortedKeys(m map[string]versionedTable) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Bubble sort — n is at most 2, keep the binary dependency-free.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

func randomBytes(n int) []byte {
	buf := make([]byte, n)
	_, _ = rand.Read(buf)
	return buf
}

func randomHex(n int) string {
	return hex.EncodeToString(randomBytes(n))
}
