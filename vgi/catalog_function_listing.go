// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import "sync"

// Function listings, built once.
//
// A catalog_schema_contents_functions answer is derived only from the static
// registrations the catalog was built from, yet it used to be rebuilt on every
// request, Arrow-encoding every listed FunctionInfo again: ~66 ms of worker time
// per listing of the example worker's main schema, ~151 ms of CPU for a full
// catalog listing, paid again by every DuckDB function-set load. Each listing is
// now built on first request and kept, keyed by everything it depends on --
// schema, function-type filter and the attach's catalog -- and each FunctionInfo
// is encoded at most once, whichever listings (or ATTACH global-function lists)
// it appears in.

// functionListingKey is what a function listing depends on. kind is the
// normalized type filter: "" when the request filters nothing (no type, or one
// that is not a filter), so equivalent requests share one entry and the key
// space stays bounded by the catalog's schemas and catalogs.
type functionListingKey struct {
	schema  string
	kind    FunctionType
	catalog string
}

// encodedFunction is one FunctionInfo's wire encoding, computed at most once.
type encodedFunction struct {
	once sync.Once
	data []byte
	err  error
}

// functionListingCache is a catalog's built function listings and item
// encodings. It is created with the catalog and is safe for concurrent use.
type functionListingCache struct {
	listings sync.Map // functionListingKey -> [][]byte
	mu       sync.Mutex
	encoded  map[*FunctionInfo]*encodedFunction
}

// encode returns fi's wire encoding, encoding it on first use. The returned
// bytes are shared and must not be modified.
func (c *functionListingCache) encode(fi *FunctionInfo) ([]byte, error) {
	c.mu.Lock()
	if c.encoded == nil {
		c.encoded = make(map[*FunctionInfo]*encodedFunction)
	}
	entry, ok := c.encoded[fi]
	if !ok {
		entry = &encodedFunction{}
		c.encoded[fi] = entry
	}
	c.mu.Unlock()
	entry.once.Do(func() { entry.data, entry.err = SerializeFunctionInfo(fi) })
	return entry.data, entry.err
}

// functionListingKind normalizes a listing request's type filter into the
// kind a functionListingKey carries. DuckDB sends "SCALAR_FUNCTION",
// "TABLE_FUNCTION", etc.; normalizeFunctionType also accepts the short forms.
// An unrecognized type filters nothing.
func functionListingKind(requested string) FunctionType {
	if requested == "" {
		return ""
	}
	switch want := normalizeFunctionType(FunctionType(requested)); want {
	case FunctionTypeTable, FunctionTypeScalar, FunctionTypeAggregate:
		return want
	}
	return ""
}

// listsFunction reports whether a listing of kind includes a function of type
// ft. Table-buffering functions register as DuckDB table functions, so they
// match a TABLE_FUNCTION request.
func listsFunction(kind, ft FunctionType) bool {
	switch kind {
	case "":
		return true
	case FunctionTypeTable:
		return ft == FunctionTypeTable || ft == FunctionTypeTableBuffering
	}
	return ft == kind
}

// functionListing returns the encoded items of catalog_schema_contents_functions
// for the schema at schemaKey, filtered by the requested type and the attach's
// catalog, building and caching it on first request. The returned slice is
// shared and must not be modified. ok is false for an unknown schema.
func (cat *DefaultReadOnlyCatalog) functionListing(schemaKey, requestedType, catalogName string) (items [][]byte, ok bool, err error) {
	si, ok := cat.schemas[schemaKey]
	if !ok {
		return nil, false, nil
	}
	key := functionListingKey{schema: schemaKey, kind: functionListingKind(requestedType), catalog: catalogName}
	if cached, hit := cat.functionCache.listings.Load(key); hit {
		return cached.([][]byte), true, nil
	}
	for i := range si.functions {
		fi := &si.functions[i]
		if !listsFunction(key.kind, fi.FunctionType) {
			continue
		}
		// Every function has exactly one home catalog; a catalog only lists
		// what it owns. (An unresolvable attach leaves catalogName empty --
		// nothing to compare against, so list all.)
		if fi.unlisted {
			continue
		}
		if catalogName != "" && catalogName != fi.catalogHome {
			continue
		}
		data, err := cat.functionCache.encode(fi)
		if err != nil {
			return nil, true, err
		}
		items = append(items, data)
	}
	cached, _ := cat.functionCache.listings.LoadOrStore(key, items)
	return cached.([][]byte), true, nil
}
