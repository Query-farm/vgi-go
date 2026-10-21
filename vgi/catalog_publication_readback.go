// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package vgi

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/klauspost/compress/zstd"
)

// Validate the exact downloaded stream before RPC log filtering selects a data
// batch. This preserves the released RPC resolver's URL/redirect/digest checks,
// while separately enforcing encoded and decoded caps for gzip and zstd.
type catalogReadbackTransport struct {
	next              http.RoundTripper
	rawCap, storedCap int64
	manifest          CatalogPublicationManifest
}

func (t catalogReadbackTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	request.Header = request.Header.Clone()
	// Prevent net/http from automatically decoding gzip before encoded counting.
	request.Header.Set("Accept-Encoding", "identity")
	response, err := t.next.RoundTrip(request)
	if err != nil || response.StatusCode != http.StatusOK {
		return response, err
	}
	defer response.Body.Close()
	if response.Uncompressed {
		return nil, fmt.Errorf("catalog readback transport must preserve encoded response bytes")
	}
	if response.ContentLength > t.storedCap {
		return nil, fmt.Errorf("catalog readback exceeds stored byte limit")
	}
	encoded, err := io.ReadAll(io.LimitReader(response.Body, t.storedCap+1))
	if err != nil {
		return nil, err
	}
	if int64(len(encoded)) > t.storedCap {
		return nil, fmt.Errorf("catalog readback exceeds stored byte limit")
	}
	var reader io.Reader = bytes.NewReader(encoded)
	switch strings.ToLower(response.Header.Get("Content-Encoding")) {
	case "", "identity":
	case "gzip":
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
		defer gzipReader.Close()
		reader = gzipReader
	case "zstd":
		zstdReader, err := zstd.NewReader(reader, zstd.WithDecoderMaxMemory(uint64(t.rawCap)))
		if err != nil {
			return nil, err
		}
		defer zstdReader.Close()
		reader = zstdReader
	default:
		return nil, fmt.Errorf("unsupported catalog publication content encoding")
	}
	raw, err := io.ReadAll(io.LimitReader(reader, t.rawCap+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > t.rawCap {
		return nil, fmt.Errorf("catalog readback exceeds raw byte limit")
	}
	if int64(len(raw)) != t.manifest.RawSize || int64(len(encoded)) != t.manifest.StoredSize {
		return nil, fmt.Errorf("catalog publication size mismatch")
	}
	if err = validateCatalogOuterStream(raw); err != nil {
		return nil, err
	}
	response.Body = io.NopCloser(bytes.NewReader(raw))
	response.ContentLength = int64(len(raw))
	response.Header = response.Header.Clone()
	response.Header.Del("Content-Encoding")
	response.Header.Set("Content-Length", strconv.Itoa(len(raw)))
	return response, nil
}

func validateCatalogOuterStream(raw []byte) error {
	source := bytes.NewReader(raw)
	reader, err := ipc.NewReader(source)
	if err != nil {
		return err
	}
	defer reader.Release()
	if !reader.Schema().Equal(catalogOuterSchema) {
		return fmt.Errorf("invalid published RPC envelope schema")
	}
	batches := 0
	for reader.Next() {
		batches++
		record := reader.RecordBatch()
		if batches != 1 || record.NumRows() != 1 {
			return fmt.Errorf("published RPC envelope requires exactly one response batch and row")
		}
		if tagged, ok := record.(interface{ Metadata() arrow.Metadata }); ok {
			metadata := tagged.Metadata()
			for _, key := range []string{vgirpc.MetaLocation, vgirpc.MetaLogLevel} {
				if _, present := metadata.GetValue(key); present {
					return fmt.Errorf("published RPC envelope contains pointer/log metadata")
				}
			}
		}
	}
	if err := reader.Err(); err != nil {
		return err
	}
	if batches != 1 || source.Len() != 0 {
		return fmt.Errorf("published RPC envelope is incomplete or has trailing data")
	}
	return nil
}
