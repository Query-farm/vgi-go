# Changelog

## 0.42.2 (2026-11-16)

### Fixed

- A `CatalogTable`'s scan `Function` is homed in the table's own schema, like
  its write functions (and as in vgi-python and vgi-rust 0.51.1), instead of
  the catalog's default schema `main`. Listed in `main`, a table-only scan
  named like a host function (e.g. `range`) captured a `main` macro's bare
  call of it. With `ExposeScan`, the scan is callable in the table's schema.
- Tables in different schemas may now use different scan functions of the
  same name; each table scans with its own. The registry used to keep only the
  first function registered under a name, worker-wide, and silently scanned
  every later table with it. Within one schema a name must denote one
  function: a different function reusing a scan name there panics at
  registration. Sharing one function across tables still works; sameness is
  identity or deep equality of the implementation, never the name alone.
- The same holds for a `CatalogTable`'s `InsertFunction` / `UpdateFunction` /
  `DeleteFunction`: they were deduplicated worker-wide by name, so two tables
  whose different write functions shared a name both wrote with the first.
  They are now per schema: same-named writers in different schemas each reach
  their own function, one function may back several tables, and a different
  function reusing a write function's name in one schema -- another table's
  writer or the author's own table in-out function, in either order -- panics
  at registration. A write function is `table_only` only in the schema a
  table registered it in; an unrelated same-named table in-out function in
  another schema stays public.
- `table_function_dynamic_to_string` resolves the function in the schema the
  bind names, rather than the first registration of the name.

## 0.42.0 (2026-11-11)

### Changed

- **Protocol 3.3.0** (`vgi.ProtocolVersion`). Requires the matching VGI DuckDB
  extension (protocol 3.3); peers must agree on major and minor.
- `FunctionInfo` carries a `visibility` field (`vgi.FunctionVisibilityPublic`
  / `vgi.FunctionVisibilityTableOnly`, null on the wire means public). Every
  function a `CatalogTable` names is now always sent in the function listing,
  so the extension always has its capabilities (pushdown, required secrets,
  `has_finalize`, ...).
- **User-visible:** catalog tables' scan functions and write functions
  (Insert/Update/Delete) disappear from `duckdb_functions()` and can no longer
  be called directly from SQL. Write functions are always table-only. A scan
  stays callable if the table sets the new `CatalogTable.ExposeScan` field, or
  if the function is also registered with `RegisterTable` (or a sibling).
- `RegisterTableUnlisted` now means "send as table_only": the function is
  listed with its capabilities instead of being omitted. It is needed only for
  functions that tables name dynamically; a `CatalogTable.Function` is
  table-only by default.
- The writable catalog's generic `vgi_writable_scan` / `_insert` / `_update` /
  `_delete` functions are table-only.
- **Protocol 3.3 write path** (wire changes, same protocol version 3.3.0):
  - `catalog_table_scan_function_get` is removed; `catalog_table_scan_branches_get`
    is the only scan-discovery method. Nothing changes for authors:
    `CatalogTable.Function`, `WithAttachScanFunctionGetHandler` and
    `SetScanFunctionGetHandler` keep working and are served as one branch.
    The generated `CatalogTableScanFunctionGetParams` type is gone.
  - `catalog_table_create` returns `created` (`TableCreateResponseWire`):
    false when `IF NOT EXISTS` found an existing table, so `CREATE TABLE IF NOT
    EXISTS ... AS SELECT` no longer appends into it. `WritableCatalog` and
    `MemoryCatalog` report it.
  - The write-options batch carries only `result_mode`.
    `WriteOptions.OnConflict` / `OnConflictColumns` are deprecated and always
    `"throw"` / empty (DuckDB turns every `INSERT ... ON CONFLICT` into a MERGE
    before the catalog sees it).
- Regenerated `vgi/generated` and `vgi/vgi_service_gen.go` from vgi-python.
  This fixes `generated.CatalogContractHash`, which the protocol 3.3
  visibility change had left at its 3.2 value: the catalog publication cache
  (keyed on it) could have reused snapshots published under the 3.2 contract.
- `examples/writable_tx` (txfix) follows the new fixture contract: `items`
  gains `items_insert` and `items_delete`, all three write functions set
  `HasFinalize`, `qty` is `BIGINT DEFAULT 0`, the transactional attach stages
  inserts and deletes, and `writes()` lists every write call.
- `examples/simple_writable` supports `CREATE TABLE` / CTAS / `DROP TABLE` of
  insert-only tables.

### Added

- `CatalogTable.ExposeScan`, `FunctionInfo.Visibility`, `vgi.FunctionVisibility`.
- `WithAttachTableCreateHandler` / `WithAttachTableDropHandler`: `CREATE TABLE`
  and `DROP TABLE` for catalogs served by handlers.
- `Worker.RegisterTableInOutUnlisted`, the table in-out counterpart of
  `RegisterTableUnlisted` (for write functions a handler names).
