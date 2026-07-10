module github.com/Query-farm/vgi-go

go 1.25.0

// The Arrow fork adds RecordBatch custom-metadata support required by the SHM
// zero-copy transport (the shm pointer-batch metadata). Required only for the
// shared-memory side channel; stdio and HTTP transports work on upstream Arrow.
// NOTE: replace directives do not propagate to importers — consumers who enable
// SHM must add this same replace to their own go.mod.
replace github.com/apache/arrow-go/v18 => github.com/Query-farm/arrow-go/v18 v18.0.0-20260220022719-2d45cbd918a4

require (
	github.com/Query-farm/vgi-rpc-go v0.13.0
	github.com/Query-farm/vgi-rpc-go/vgirpc/jwtauth v0.9.3
	github.com/apache/arrow-go/v18 v18.5.2
	github.com/duckdb/duckdb-go/v2 v2.10502.0
	github.com/google/uuid v1.6.0
	golang.org/x/crypto v0.51.0
	modernc.org/sqlite v1.46.1
)

require (
	github.com/MicahParks/jwkset v0.5.19 // indirect
	github.com/MicahParks/keyfunc/v3 v3.3.5 // indirect
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/apache/thrift v0.22.0 // indirect
	github.com/duckdb/duckdb-go-bindings v0.10502.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-amd64 v0.10502.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-arm64 v0.10502.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-amd64 v0.10502.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-arm64 v0.10502.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/windows-amd64 v0.10502.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/golang-jwt/jwt/v5 v5.2.1 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/klauspost/asmfmt v1.3.2 // indirect
	github.com/klauspost/compress v1.18.4 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/minio/asm2plan9s v0.0.0-20200509001527-cdd76441f9d8 // indirect
	github.com/minio/c2goasm v0.0.0-20190812172519-36a3d3bbc4f3 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.25 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96 // indirect
	golang.org/x/mod v0.33.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.44.0 // indirect
	golang.org/x/telemetry v0.0.0-20260209163413-e7419c687ee4 // indirect
	golang.org/x/time v0.5.0 // indirect
	golang.org/x/tools v0.42.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	modernc.org/libc v1.67.6 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)
