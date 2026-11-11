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

### Added

- `CatalogTable.ExposeScan`, `FunctionInfo.Visibility`, `vgi.FunctionVisibility`.
