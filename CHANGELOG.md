# Changelog

## 0.44.2 (2026-11-25)

Patch release; the wire protocol stays 3.4 and the dependencies are unchanged
from 0.44.1.

### Fixed

- Secret scope matching now follows DuckDB 2.0.1's authority boundary
  (duckdb#28684). An authority-only scope such as `snmp://10.0.0.1` covers
  `snmp://10.0.0.1` and `snmp://10.0.0.1/x` but no longer `snmp://10.0.0.15`
  or `snmp://10.0.0.1:161`, and `s3://bucket` no longer covers
  `s3://bucket-evil/`. `Secrets.ForScope`, `ForScopeOfType` and
  `FieldForScope` use the new exported `vgi.ScopeMatches`, so a worker never
  picks a secret DuckDB itself would not resolve for that path. Checked
  against 3,025 cases generated from DuckDB's C++
  (`vgi/testdata/scope_match_vectors.json`).

## 0.44.1 (2026-11-24)

Patch release; the wire protocol stays 3.4. Requires vgi-rpc-go v0.38.1
(`vgirpc/gcs`, `vgirpc/jwtauth`, `vgirpc/s3` v0.17.1). The expression-filter engine
(`vgi/exprduckdb`) moves to Haybarn 2.0.1rc3 (haybarn-go v2.20001.0-rc.3);
the 3.4 extension is served only to Haybarn 2.0.1.

### Added

- **Protocol version negotiation** (PROTOCOL_VERSIONS_V1, via vgi-rpc-go).
  Every response advertises the protocols the worker gates
  (`VGI-Protocol-Versions: vgi.v3=3.4-3.4`, plus `vgi.attach_tickets.v1=2.0-2.0`
  in hosted mode and any versioned protocol the worker hosts), hosted-mode 401s
  and `HEAD /health` included, and reflection carries the same string as
  `vgi_rpc.protocol_versions` on every transport. A request outside the window
  is refused with `protocol_version_unsupported` naming both windows.
  - `VGI_TEST_PROTOCOL_RANGE` (fixture switch): a bare range (`3.3-3.3`)
    overrides the `vgi.v3` window, `name=MIN-MAX[, ...]` names protocols; a
    malformed value refuses to start.
- **Direct object storage (S3, GCS) for self-hosted workers** (direct-storage
  contract v1). With `VGI_STORAGE_BACKEND=s3` or `gcs`, a worker keeps
  externalized results, upload grants, argument references and StateRef
  objects in the operator's own bucket instead of the blob vendor. Setting
  `VGI_STORAGE_S3_BUCKET` or `VGI_STORAGE_GCS_BUCKET` alone also selects the
  backend. The worker signs its own URLs.
  - Configuration is through `VGI_STORAGE_S3_*` / `VGI_STORAGE_GCS_*`,
    `VGI_STORAGE_URL_TTL_SECONDS` and `VGI_STORAGE_ALLOW_PLAIN_HTTP`.
  - Link the backends with
    `import _ "github.com/Query-farm/vgi-go/vgi/storage/objectstore/all"`;
    the example worker does.
  - A worker refuses to start when several backends are set without
    `VGI_STORAGE_BACKEND`, or when the selected backend is not linked.
  - `VGI_STORAGE_S3_CHECKSUM=none` turns off upload grants and argument
    references.
  - Key layout: `<prefix>rpc/` (results, grants, StateRef and argument
    objects; 1-day lifecycle) and `<prefix>fs/` (function storage; 2-day
    lifecycle).
  - Function storage (`VGI_STORAGE_EXTERNALIZE`) can use the bucket's `fs/`
    area (`StorageBlobs`, `NewObjectStoreBlobs`), with best-effort deletes.
  - New settings: `VGI_STORAGE_S3_SESSION_EXPIRES` (caps URL lifetimes under an
    explicit session token; without it a startup warning) and
    `VGI_STORAGE_GCS_VERIFY`. An `http://` endpoint refuses to start without
    `VGI_STORAGE_ALLOW_PLAIN_HTTP=1`.
  - New integration lane `s3`: the whole suite over http with RustFS as the
    storage.

### Fixed

- **Large storage transfers on slow links.** Through vgi-rpc-go v0.38.1, every storage request (the
  worker's externalized-result PUTs, argument, StateRef and pointer GETs,
  request uploads) has a 30 s connect timeout and a 30 s stall timeout
  (`VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS`) instead of a 30 s whole-request
  deadline. A 100 MB result or a 300 MB argument at 2 MB/s now moves in one
  attempt; it used to time out, and the upload was retried three times.
- Catalog snapshot objects (`S3CatalogObjectStorage` requests and the
  publication readback) keep a whole-request deadline, now
  max(30 s, size / `MinTransferRate`) with a 64 KiB/s default floor
  (`CatalogSnapshotPolicy.MinTransferRate`,
  `S3CatalogObjectStorage.MinTransferRate`), on the storage transport. It was
  a flat 30 s, which cut off any catalog object slower than size / 30 s.

## 0.44.0 (2026-11-23)

### Breaking

- **Protocol 3.4.0** (`vgi.ProtocolVersion`). Requires the matching VGI DuckDB
  extension (protocol 3.4); peers must agree on major and minor. Nothing in
  vgi itself changes: 3.4 signals vgi-rpc upload-URL protocol v2. The HTTP
  serve path advertises `VGI-Upload-URL-Support: 2`, client uploads are
  vended as sized grants (size + SHA-256, verified by storage), and a request
  pointer without `vgi_rpc.location.sha256` is refused. Requires vgi-rpc-go
  v0.38.0 and `vgirpc/jwtauth` v0.17.0.
- **Large values (protocol 3.4, vgi `docs/large-values-3.4.md`).**
  - Generated bind types: `BindRequestWire`, `InitRequestWire` and the
    cardinality, statistics, plan, projection and dynamic-to-string requests
    are the generated records (`vgi/generated`). Every `bind_call` is a
    raw-preserving `generated.Nested[generated.BindRequest]`, decoded on access
    (`BindCallValue()`) and re-encoded as the bytes received.
    `BindRequestWire.Arguments` is now `*[]byte` (nullable),
    `ArgumentNames` is `*[]string` (an unnamed vararg arrives as `""`; parsed
    `BindParams.ArgumentNames` still has `nil` there), `CopyFrom` / `CopyTo` are
    the generated `CopyFromContext` / `CopyToContext`. The hand-written
    `CopyFromContextWire` / `CopyToContextWire` are gone; `PlanRequestWire`
    (what a split planner gets) and `CardinalityRequestWire` stay as the
    decoded forms.
  - Argument references: a bind call sets exactly one of `arguments` and
    `arguments_location`, else `invalid_bind_request` (INVALID_ARGUMENT,
    "BindRequest must set exactly one of 'arguments' and
    'arguments_location'"); a bind with no arguments now needs an empty
    arguments stream, not a null. A location is resolved at each request's
    entry through vgi-rpc (sealed ref, size/SHA-256, the object cache, one
    fetch per process): `invalid_arguments_ref`, `arguments_ref_integrity`,
    and `arguments_ref_missing` with "this prepared statement's large argument
    has expired; prepare it again". The HTTP server sends
    `VGI-Arguments-Ref-Bytes` (default 4 MiB, `VGI_ARGUMENTS_REF_BYTES`)
    whenever it vends upload URLs from an object store
    (`VGI_TEST_DISABLE_ARGUMENTS_REF=1` turns it off). Split-token
    fingerprints take a location's size and SHA-256, never the ref.
  - `init: StateRef`: stream states carry their init recipe as one vgi-rpc
    `StateRef` (`Init`), inline when small and stored once when large; the
    recipe holds the bind call's received bytes (`InitRecipe.BindCallIPC`,
    replacing `InitRecipe.BindCall`), so an externalized argument travels by
    reference. `Recipe` is no longer serialized. HTTP rehydration resolves the
    recipe and the arguments at the turn's entry.
  - HTTP streams live up to 6 h (`TokenTTL`; vgi-rpc-go defaulted to 5 min),
    and an over-budget cursor is refused (`cursor_too_large`).
  - Projection: a null `arguments` cell no longer panics the decoder.
  - A table-buffering finalize stream's token carries only the batches still
    to emit (it carried the whole flush, so a single wide batch emitted on
    the init turn put the next continuation over the turn budget).
  - The blob_test fixture catalog is a sub-catalog and gains
    `blob_param_batches`, `blob_param_streams`, `blob_param_scalar` and the
    `blob_param_table` table.

### Added

- **Orchard hosted mode** (`VGI_ORCHARD_HOSTED=1`). `RunHttp` accepts only an
  Orchard Pass (RFC 9068 `at+jwt`, ES256, exact `iss` and single `aud`, `exp`
  required, 60 s leeway) on every request but `HEAD /health` and CORS
  preflights, verified against a JWKS cached for at most 10 minutes that fails
  closed (503) rather than serving stale keys. Startup refuses any other way
  in (bearer tokens, grant keys, identity hooks, OAuth, proxy proof, Iroh, a
  custom authenticator, non-HTTP transports including stdio) and a missing or
  short `VGI_SIGNING_KEY`. Claims reach `ProcessParams.Auth` with their JSON
  types, domain `orchard`. See the README. The example worker registers the
  `pass_claims` fixture in hosted mode, and CI runs vgi's hosted-mode harness.

## 0.43.0 (2026-11-19)

### Security

- **Sealed framework payloads (v1).** Framework state the SDK writes to
  storage is sealed with the cross-SDK format: HKDF-SHA256 over the signing
  key (`VGI_SIGNING_KEY` / `WithHttpSigningKey`, used as its raw configured
  bytes; a host-local key without one; info `vgi.storage.payload.v1`),
  AES-256-GCM, with the shard key, execution, namespace and key bound as AAD.
  Wire form `0x01 || nonce || ciphertext || tag`, byte-identical to
  vgi-python's test vector. An unknown version or a short or unauthenticated
  envelope is a `*StoragePayloadError`, never read as plaintext.
- **No plaintext secrets in storage.** The buffered-function init recipe
  (resolved secrets, opened transaction scope) and aggregate const args are
  stored sealed; the opened attach scope and options are no longer stored.
- Buffering process/combine take the shard key, function name, schema path,
  attach scope/options and caller from the authenticated request, never from
  stored state; a function-name mismatch is refused.
- A wrong-type aggregate state is a clean error (`*AggregateStateTypeError`)
  and aggregate handlers recover panics instead of crashing the worker.
- Per-execution caches are keyed by `(shard_key, execution_id)`, so another
  attach can never read or re-pin an execution's view; they are bounded
  (LRU + TTL) and evicted by the destructors.

### Performance

- Aggregate const args are cached per execution and function (functions
  without const params never read them): update 3 -> 2, combine 3 -> 2,
  finalize 2 -> 1 storage calls.
- Decoded window partitions are cached between evaluate blocks (2 -> 0
  storage calls per block).
- The buffered finalize flush packs batches into ~1 MiB log rows (one append
  per row instead of per batch).
- `storage/cfdo`: retries use jittered exponential backoff
  (`RetryBaseDelay` / `RetryMaxDelay`, defaults 50 ms / 1 s) with a stable
  `attempt_id`, and `StateLogScan` pages 1000 rows per request.

### Compatibility

- Storage written by 0.42.x is not readable by 0.43.0 (it is unsealed);
  storage is per execution, so let in-flight queries finish before upgrading
  workers that share a storage backend.

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
