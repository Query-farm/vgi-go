#!/usr/bin/env bash
# Verify the documented replacements in an independent consumer module.
set -euo pipefail

sdk_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
consumer_dir=$(mktemp -d "${TMPDIR:-/tmp}/vgi-flatbuffers-consumer.XXXXXX")
trap 'rm -rf "$consumer_dir"' EXIT
export GOWORK=off

cp "$sdk_root/vgi/flatbuffers_test.go" "$consumer_dir/flatbuffers_test.go"
cp "$sdk_root/ci/fixtures/flatbuffers-schema/main.go" "$consumer_dir/main.go"
# The imported SDK enters the consumer's module graph, but its replacements do
# not. The standalone regression exercises the dependency actually selected.
sed 's/^package vgi$/package main/' "$consumer_dir/flatbuffers_test.go" > "$consumer_dir/flatbuffers_test.tmp"
mv "$consumer_dir/flatbuffers_test.tmp" "$consumer_dir/flatbuffers_test.go"
printf 'package main\nimport _ "github.com/Query-farm/vgi-go/vgi"\n' > "$consumer_dir/sdk.go"
cd "$consumer_dir"
go mod init example.com/vgi-flatbuffers-consumer
go mod edit -require=github.com/Query-farm/vgi-go@v0.38.0
go mod edit "-replace=github.com/Query-farm/vgi-go=$sdk_root"
go mod edit -replace=github.com/google/flatbuffers=github.com/rustyconover/flatbuffers@v25.12.20-0.20261026151427-0acd09bcd1ff+incompatible
# First prove that stdio/HTTP consumers need only the FlatBuffers replacement.
go mod tidy
go test -run TestFlatbuffersVtableObjectSize -count=1 .
go run . > /dev/null
# SHM consumers additionally select the Arrow fork, exactly as documented.
go mod edit -replace=github.com/apache/arrow-go/v18=github.com/Query-farm/arrow-go/v18@v18.0.0-20260220022719-2d45cbd918a4
go mod tidy
go test -run TestFlatbuffersVtableObjectSize -count=1 .
go run . > /dev/null
