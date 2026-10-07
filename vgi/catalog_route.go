// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// Routed catalogs: one worker process serving several independent catalogs.
//
// A Worker's own catalog is its static default catalog (plus aliases and
// writable catalogs, which share its implementation). A routed catalog has an
// implementation of its own and answers every catalog_* RPC for its attaches:
//
//   - RegisterSubCatalog hosts another *Worker's catalog (its own tables,
//     views, macros, schemas, catalog_contents behaviour) under that worker's
//     catalog name. The sub-worker's functions are dispatched by this worker,
//     homed in the sub-catalog, so a bind through the sub-catalog's attach
//     reaches exactly the functions the sub-catalog lists.
//   - RegisterMemoryCatalog hosts a MemoryCatalog: an in-memory, DDL-capable
//     catalog whose every ATTACH is private.
//
// This is the Go counterpart of vgi-python's MetaWorker and vgi-typescript's
// CompositeCatalogInterface. Routing rides the attach_opaque_data: catalog_attach
// for a routed name asks the routed catalog to attach, then wraps the attach
// value it minted as marker || name || NUL || inner before minting and sealing
// it like any other attach. Every later catalog_* request carrying that attach
// is unwrapped back to the inner value and handed to the routed catalog.

// routedMethod is a catalog_* handler that takes a pointer to its request wire
// struct and returns its response value (nil for void methods).
type routedMethod func(ctx context.Context, cc *vgirpc.CallContext, req any) (any, error)

// catalogBackend is a routed catalog's implementation.
type catalogBackend interface {
	// catalogInfoItems are the serialized CatalogInfo records catalog_catalogs
	// lists for this catalog.
	catalogInfoItems(ctx context.Context, cc *vgirpc.CallContext) ([][]byte, error)
	// method returns the handler for a catalog_* method, or false when the
	// catalog does not implement it.
	method(name string) (routedMethod, bool)
}

// catalogRouteMarker opens a routed attach plaintext. It begins with NUL, so
// catalogNameOf reads no catalog name off a routed attach and nothing else this
// worker mints can collide with it.
const catalogRouteMarker = "\x00vgi.route\x00"

func encodeRoutedAttach(name string, inner []byte) []byte {
	out := make([]byte, 0, len(catalogRouteMarker)+len(name)+1+len(inner))
	out = append(out, catalogRouteMarker...)
	out = append(out, name...)
	out = append(out, 0)
	return append(out, inner...)
}

// parseRoutedAttach splits a routed attach plaintext into the routed catalog's
// name and its own attach value (a copy).
func parseRoutedAttach(plain []byte) (name string, inner []byte, ok bool) {
	if !bytes.HasPrefix(plain, []byte(catalogRouteMarker)) {
		return "", nil, false
	}
	rest := plain[len(catalogRouteMarker):]
	i := bytes.IndexByte(rest, 0)
	if i < 0 {
		return "", nil, false
	}
	return string(rest[:i]), bytes.Clone(rest[i+1:]), true
}

func (w *Worker) recordCatalogMethod(name string, m routedMethod) {
	if w.catalogMethods == nil {
		w.catalogMethods = make(map[string]routedMethod)
	}
	w.catalogMethods[name] = m
}

// addRoute registers a routed catalog. A name may be served once: it must not
// be this worker's catalog, an alias, a writable catalog or another route.
func (w *Worker) addRoute(name string, b catalogBackend) {
	if name == "" {
		panic("vgi: a routed catalog needs a name")
	}
	_, alias := w.catalogAliases[name]
	_, aliasInfo := w.catalogAliasInfos[name]
	_, writable := w.extraCatalogs[name]
	_, routed := w.routes[name]
	if name == w.catalogName || alias || aliasInfo || writable || routed {
		panic(fmt.Sprintf("vgi: catalog %q is already served by this worker", name))
	}
	if w.routesPrepared {
		panic(fmt.Sprintf("vgi: catalog %q registered after the worker's server was built", name))
	}
	if w.routes == nil {
		w.routes = make(map[string]catalogBackend)
	}
	w.routes[name] = b
}

// RegisterSubCatalog serves child's catalog from this worker, under child's
// catalog name (WithCatalogName). Every catalog_* RPC for an attach of that
// name is answered by child exactly as child would answer it as a worker of its
// own — its tables, views, macros, schema comments, catalog_contents
// advertising, handler and etag mode, version. child's functions are dispatched
// by this worker, homed in child's catalog, so they are reachable only through
// an attach of it and never collide with this worker's own functions.
//
// Register every function, table and option on child before registering it; a
// sub-catalog may not itself have routed catalogs. Panics if the name is
// already served.
func (w *Worker) RegisterSubCatalog(child *Worker) {
	if child == nil || child == w {
		panic("vgi: RegisterSubCatalog needs another worker")
	}
	if len(child.routes) > 0 {
		panic(fmt.Sprintf("vgi: sub-catalog %q may not have routed catalogs of its own", child.catalogName))
	}
	w.addRoute(child.catalogName, &subCatalog{child: child})
}

// RegisterMemoryCatalog serves an in-memory DDL-capable catalog from this
// worker, under m's name. See MemoryCatalog.
func (w *Worker) RegisterMemoryCatalog(m *MemoryCatalog) {
	if m == nil {
		panic("vgi: RegisterMemoryCatalog needs a catalog")
	}
	w.addRoute(m.name, m)
}

// prepareRoutes brings the routed catalogs up when the server is built: each
// sub-catalog builds its static catalog and records its catalog handlers, and
// its functions are imported into this worker's registry, homed in its
// catalog.
func (w *Worker) prepareRoutes() {
	if w.routesPrepared {
		return
	}
	w.routesPrepared = true
	for _, name := range slices.Sorted(maps.Keys(w.routes)) {
		if sc, ok := w.routes[name].(*subCatalog); ok {
			sc.prepare(w)
		}
	}
}

// routedCatalog reports whether catalog names a routed catalog.
func (w *Worker) routedCatalog(catalog string) bool {
	_, ok := w.routes[catalog]
	return ok
}

// dispatchRouted hands a catalog_* request to the routed catalog its attach
// belongs to. It reports routed=false for any other request (including one
// whose attach does not open: the regular path reports that error).
func (w *Worker) dispatchRouted(ctx context.Context, cc *vgirpc.CallContext, method string, reqPtr any) (any, bool, error) {
	if len(w.routes) == 0 {
		return nil, false, nil
	}
	af := reflect.ValueOf(reqPtr).Elem().FieldByName("AttachOpaqueData")
	if !af.IsValid() || af.Kind() != reflect.Slice || af.Len() == 0 {
		return nil, false, nil
	}
	sealedAttach := bytes.Clone(af.Bytes())
	plain, err := w.openAttach(sealedAttach, cc)
	if err != nil {
		return nil, true, err
	}
	name, inner, ok := parseRoutedAttach(plain)
	if !ok {
		return nil, false, nil
	}
	b, ok := w.routes[name]
	if !ok {
		// Routing is a check too: an unroutable value gets the same answer as
		// one that does not open, naming nothing this worker serves.
		return nil, true, attachRejected()
	}
	m, ok := b.method(method)
	if !ok {
		return nil, true, &vgirpc.RpcError{Type: "NotImplementedError",
			Message: fmt.Sprintf("catalog '%s' does not support %s", name, method)}
	}
	// The routed catalog never sees a sealed value: open the transaction here,
	// bound to the sealed attach, before handing over the inner attach.
	if tf := reflect.ValueOf(reqPtr).Elem().FieldByName("TransactionOpaqueData"); w.sealOpaqueData &&
		tf.IsValid() && tf.Kind() == reflect.Pointer && !tf.IsNil() {
		txPlain, err := w.openTransaction(tf.Elem().Bytes(), sealedAttach, cc)
		if err != nil {
			return nil, true, err
		}
		tf.Set(reflect.ValueOf(&txPlain))
	}
	af.SetBytes(inner)
	out, err := m(ctx, cc, reqPtr)
	return out, true, err
}

// attachRouted serves catalog_attach for a routed catalog: the routed catalog
// attaches, and its attach value is wrapped, minted and sealed.
func (w *Worker) attachRouted(ctx context.Context, cc *vgirpc.CallContext, req CatalogAttachRequestWire, b catalogBackend) (CatalogAttachResultWire, error) {
	m, ok := b.method("catalog_attach")
	if !ok {
		return CatalogAttachResultWire{}, fmt.Errorf("vgi: catalog %q has no catalog_attach", req.Name)
	}
	out, err := m(ctx, cc, &req)
	if err != nil {
		return CatalogAttachResultWire{}, err
	}
	res, ok := out.(CatalogAttachResultWire)
	if !ok {
		return CatalogAttachResultWire{}, fmt.Errorf("vgi: catalog %q attach returned %T", req.Name, out)
	}
	routed := encodeRoutedAttach(req.Name, res.AttachOpaqueData)
	if res.AttachOpaqueData, err = w.mintAttach(routed, cc); err != nil {
		return CatalogAttachResultWire{}, err
	}
	// Routing lives in the attach value, so the client must keep sending it.
	res.AttachOpaqueDataRequired = true
	return res, nil
}

// routedCatalogInfos lists the routed catalogs for catalog_catalogs, by name.
func (w *Worker) routedCatalogInfos(ctx context.Context, cc *vgirpc.CallContext) ([][]byte, error) {
	var items [][]byte
	for _, name := range slices.Sorted(maps.Keys(w.routes)) {
		more, err := w.routes[name].catalogInfoItems(ctx, cc)
		if err != nil {
			return nil, err
		}
		items = append(items, more...)
	}
	return items, nil
}

// ---------------------------------------------------------------------------
// Sub-catalogs (RegisterSubCatalog)
// ---------------------------------------------------------------------------

type subCatalog struct {
	child *Worker
}

func (sc *subCatalog) prepare(parent *Worker) {
	child := sc.child
	child.routesPrepared = true
	child.catalog = NewDefaultReadOnlyCatalog(child.catalogName, child)
	registerVgiCatalogMethods(child, nil, child.vgiService())
	home := child.catalogName
	importRegistry(parent, child, parent.scalars, child.scalars, kindScalar, home)
	importRegistry(parent, child, parent.tables, child.tables, kindTable, home)
	importRegistry(parent, child, parent.tableInOuts, child.tableInOuts, kindTableInOut, home)
	importRegistry(parent, child, parent.tableBufferings, child.tableBufferings, kindTableBuffering, home)
	importRegistry(parent, child, parent.aggregates, child.aggregates, kindAggregate, home)
}

// importRegistry appends child's registrations of one kind to parent's,
// keeping parent's origin slices index-aligned and homing each import in the
// sub-catalog (in the schema child declared it in).
func importRegistry[T any](parent, child *Worker, dst, src map[string][]T, kind funcKind, home string) {
	for _, name := range slices.Sorted(maps.Keys(src)) {
		for i, fn := range src[name] {
			// A registry slice poked without recordOrigin has fewer origins than
			// entries; pad with the defaults originOf would report, so the
			// imported origin lands at the imported entry's index.
			for len(parent.funcOrigins[funcKey{kind: kind, name: name}]) < len(dst[name]) {
				parent.recordOrigin(kind, name, parent.originOf(kind, name, len(parent.funcOrigins[funcKey{kind: kind, name: name}])))
			}
			o := child.originOf(kind, name, i)
			dst[name] = append(dst[name], fn)
			parent.recordOrigin(kind, name, funcOrigin{catalog: home, schema: o.schema, unlisted: o.unlisted})
		}
	}
}

func (sc *subCatalog) catalogInfoItems(ctx context.Context, cc *vgirpc.CallContext) ([][]byte, error) {
	m, ok := sc.method("catalog_catalogs")
	if !ok {
		return nil, nil
	}
	out, err := m(ctx, cc, &CatalogCatalogsParams{})
	if err != nil {
		return nil, err
	}
	return out.(CatalogsResponseWire).Items, nil
}

func (sc *subCatalog) method(name string) (routedMethod, bool) {
	m, ok := sc.child.catalogMethods[name]
	return m, ok
}
