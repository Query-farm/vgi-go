module github.com/Query-farm/vgi-go

go 1.26.0

// The Arrow fork adds RecordBatch custom-metadata support required by the SHM
// zero-copy transport (the shm pointer-batch metadata). Required only for the
// shared-memory side channel; stdio and HTTP transports work on upstream Arrow.
// NOTE: replace directives do not propagate to importers — consumers who enable
// SHM must add this same replace to their own go.mod.
replace github.com/apache/arrow-go/v18 => github.com/Query-farm/arrow-go/v18 v18.0.0-20260220022719-2d45cbd918a4

require (
	github.com/Query-farm/vgi-rpc-go v0.26.0
	github.com/Query-farm/vgi-rpc-go/vgirpc/jwtauth v0.16.0
	github.com/apache/arrow-go/v18 v18.7.0
	github.com/duckdb/duckdb-go/v2 v2.10505.0
	github.com/google/uuid v1.6.0
	golang.org/x/crypto v0.55.0
	modernc.org/sqlite v1.56.0
)

require (
	github.com/MicahParks/jwkset v0.11.3 // indirect
	github.com/MicahParks/keyfunc/v3 v3.8.1 // indirect
	github.com/andybalholm/brotli v1.2.2 // indirect
	github.com/apache/thrift v0.24.0 // indirect
	github.com/duckdb/duckdb-go-bindings v0.10505.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-amd64 v0.10505.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-arm64 v0.10505.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-amd64 v0.10505.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-arm64 v0.10505.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/windows-amd64 v0.10505.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/klauspost/asmfmt v1.3.2 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/minio/asm2plan9s v0.0.0-20200509001527-cdd76441f9d8 // indirect
	github.com/minio/c2goasm v0.0.0-20190812172519-36a3d3bbc4f3 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.28 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/exp v0.0.0-20260812173653-3d80eb74bc5b // indirect
	golang.org/x/mod v0.39.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/telemetry v0.0.0-20260811182544-a038080d80e5 // indirect
	golang.org/x/time v0.15.0 // indirect
	golang.org/x/tools v0.48.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	modernc.org/libc v1.75.3 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.12.0 // indirect
)
