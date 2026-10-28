# Catalog collections and external publication

This preparation branch requires a client using VGI wire major/minor `3.0`,
including the DuckDB extension and browser/WASM clients. Wire `2.x` peers are
incompatible; upgrade both ends together. The protocol routing name is `vgi.v3`.

Wire `vgi.v3` revision `3.0.0` carries six nullable Arrow streams in
`catalog_contents`: schemas, tables, views, functions, macros and indexes. Each
stream spans every schema and may contain multiple record batches. Readers
consume all rows and retain function/macro subtypes. Existing author callbacks
returning grouped `CatalogContentsResult.Schemas` remain supported; the default
writer uses the same metadata field builders as singleton RPCs directly.

`FunctionInfo.arguments`, `output_schema` and `parameter_default_values` use
`dictionary<int32, binary>` on the wire, including singleton RPCs. Equal bytes
share dictionary entries; existing schema and record-batch author APIs remain
unchanged. The first two fields remain required, and absent defaults remain null.

`SchemaInfo.AttachOpaqueData` is deprecated and ignored by serialization. The
attachment from `catalog_attach` remains required on later catalog/execution
requests. A matching ETag produces inline `not_modified` and six null streams.

## Enable publication

```go
objects := &vgi.S3CatalogObjectStorage{
    ObjectPrefix: "vgi/catalog-results/",
    Sign: func(key, method string, headers http.Header) (vgi.CatalogS3SignedRequest, error) {
        // Sign using the application's existing AWS SDK/R2 credentials.
        // Include every supplied header, including conditional headers.
        return signCatalogRequest(key, method, headers)
        // Expiring GETs return URL and ExpiresAt; explicitly durable URLs
        // return URL and Stable: true.
    },
}
repository := objects.Repository("catalog-bucket-v1", "vgi/catalog-manifests/", nil)
worker := vgi.NewWorker(vgi.WithCatalogName("weather"),
    vgi.WithCatalogSnapshotPolicy(&vgi.CatalogSnapshotPolicy{
        Delivery: "published", Repository: repository, Compression: "gzip",
        Identity: func(call *vgi.CatalogContentsCall) (vgi.SnapshotIdentity, error) {
            return vgi.SnapshotIdentity{
                CatalogNamespace: "weather", SourceRevision: deploymentRevision,
                CatalogVersion: call.CatalogVersion,
                // Empty VisibilityScope defaults to this validated sealed attachment.
            }, nil
        },
    }))
```

Ordinary delivery remains the default. Namespace and source revision are explicit.
The framework validates the attachment before lookup. An omitted visibility scope
hashes the original sealed attachment; sharing across attaches requires an explicit
scope incorporating every principal, tenant and option affecting visible metadata.
Use opaque scope identifiers rather than credentials. Identity includes protocol,
the canonical contract hash, codec revision, catalog version, repository profile
and compression. Change the profile when storage namespace/delivery policy changes.

Publication uses `vgirpc.PublishExternalResult` for the complete RPC result
envelope, with SHA-256 over uncompressed Arrow bytes. The manager retrieves the
object, validates the outer envelope, all category streams, schema paths, subtypes,
version, ETag and original category bytes, then commits a manifest. Concurrent
requests in one manager share a build. Warm hits use only the small manifest. A
fresh manager recovers a durable manifest and validates object readback once.

`NewMemoryCatalogPublicationRepository` provides process-local reuse.
`S3CatalogPublicationRepository` with `S3CatalogObjectStorage` implements immutable
objects and conditional create/update/delete of manifests. Its signer uses the
application's existing credentials. Signed URLs can be renewed and validated
without rebuilding/uploading the object; stable flag, digest, sizes, identity and
retention cannot change during renewal. Retain objects until all previously issued
references expire.

## Limits and maintenance

Defaults are 64 MiB raw/stored caps, 128 cached manifests, 60 seconds of reference
validity and a 30-second object-request deadline. Stricter resolver limits remain
in force. Caps are checked before upload. Identity, gzip and zstd are supported
and use separate cache entries. Generation changes during collection fail the
attempt. Errors fail the request unless `OnPublicationFailure: "ordinary"`
explicitly enables a fresh inline response with identity consistency checks.

`NewCatalogSnapshotManager(policy)` exposes `Key`, `Warm`, `Import` and
`Invalidate`. Direct callers provide an explicit visibility scope and an authorized,
consistent builder; the Worker supplies attachment-local scope for RPC calls.
Import validates readback before durable CAS commit. Invalidation waits for that
manager's pending publication, then conditionally removes its manifest. Immutable
objects remain governed by storage retention. The existing `CatalogContentsCall`
builder API and per-schema RPC encodings remain available.
