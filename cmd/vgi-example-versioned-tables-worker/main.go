// Copyright 2025, 2026 Query Farm LLC - https://query.farm

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
	"sort"

	ex "github.com/Query-farm/vgi-go/examples/versioned_tables"
	"github.com/Query-farm/vgi-go/internal/covflush"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

const (
	catalogName                  = "versioned_tables"
	dataVersionSpec              = ">=1.0.0,<4.0.0"
	defaultDataVersion           = "3.0.0"
	defaultImplementationVersion = "11.0.0"
	stickyCookieName             = "vgi_sticky"
	attachOpaqueDataSeparator    = 0x00
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
	logFlags := vgi.RegisterLoggingFlags(flag.CommandLine)
	flag.Parse()
	if err := logFlags.Apply(); err != nil {
		log.Fatalf("logging flags: %v", err)
	}
	covflush.Start()

	implDefault := defaultImplementationVersion
	dvs := dataVersionSpec

	w := vgi.NewWorker(
		vgi.WithCatalogName(catalogName),
		vgi.WithCatalogInfo(vgi.CatalogInfo{
			Name:                  catalogName,
			ImplementationVersion: &implDefault,
			DataVersionSpec:       &dvs,
		}),
		// Assert the HTTP cookie jar round-trips the sticky cookie we set at
		// ATTACH. Matches vgi-python's VersionedTablesCatalog.catalog_version —
		// a broken cookie jar manifests here as a hard failure instead of
		// silently leaking sessions across ATTACH calls.
		vgi.WithCatalogVersionHook(func(_ []byte, ctx *vgirpc.CallContext) error {
			if ctx == nil || len(ctx.Cookies) == 0 {
				return nil
			}
			if _, ok := ctx.Cookies[stickyCookieName]; !ok {
				names := make([]string, 0, len(ctx.Cookies))
				for k := range ctx.Cookies {
					names = append(names, k)
				}
				return fmt.Errorf("expected cookie %q on follow-up request; got %v", stickyCookieName, names)
			}
			return nil
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
			// Encode attach_opaque_data as <resolved_data_version>\x00<uuid16> so every
			// follow-up RPC can decode the version without per-worker state.
			attachOpaqueData := make([]byte, 0, len(resolvedData)+1+16)
			attachOpaqueData = append(attachOpaqueData, []byte(resolvedData)...)
			attachOpaqueData = append(attachOpaqueData, attachOpaqueDataSeparator)
			attachOpaqueData = append(attachOpaqueData, randomBytes(16)...)
			if ctx != nil {
				_ = ctx.SetCookie(stickyCookieName, randomHex(16), vgirpc.CookieAttrs{Path: "/"})
			}
			return &vgi.AttachDecision{
				ResolvedDataVersion:           resolvedData,
				ResolvedImplementationVersion: resolvedImpl,
				AttachOpaqueData:              attachOpaqueData,
			}, nil
		}),
		vgi.WithSchemaContentsHandler(func(attachOpaqueData []byte, schemaName string) ([]vgi.SerializedSchemaItem, bool) {
			if schemaName != "main" {
				return []vgi.SerializedSchemaItem{}, true
			}
			tables := tablesForAttachOpaqueData(attachOpaqueData)
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
		vgi.WithAttachTableGetHandler(func(attachOpaqueData []byte, schemaName, name string, _ *string, _ *string) ([]byte, bool, error) {
			if schemaName != "main" {
				return nil, true, nil
			}
			table, ok := tablesForAttachOpaqueData(attachOpaqueData)[name]
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
		vgi.WithAttachScanFunctionGetHandler(func(attachOpaqueData []byte, schemaName, name string, _ *string, _ *string) (*vgi.ScanFunctionResult, bool, error) {
			if schemaName != "main" {
				return nil, true, fmt.Errorf("Unknown schema: %s", schemaName)
			}
			table, ok := tablesForAttachOpaqueData(attachOpaqueData)[name]
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

func tablesForAttachOpaqueData(attachOpaqueData []byte) map[string]versionedTable {
	sep := bytes.IndexByte(attachOpaqueData, attachOpaqueDataSeparator)
	if sep <= 0 {
		return nil
	}
	version := string(attachOpaqueData[:sep])
	return versionTables[version]
}

func sortedKeys(m map[string]versionedTable) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
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
