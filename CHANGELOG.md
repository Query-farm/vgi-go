# Changelog

## Unreleased

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
