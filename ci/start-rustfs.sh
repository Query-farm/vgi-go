#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Source me: start RustFS (an S3-compatible server; the image vgi-rpc-java's
# storage-integration job uses) in docker over plain http, create a bucket,
# and export the direct-S3 environment (direct-storage contract v1) so the
# workers booted afterwards keep large values there instead of a blob vendor.
#
#   source ci/start-rustfs.sh   # exports VGI_STORAGE_* below; stop_rustfs stops it
#
# Exports: VGI_STORAGE_BACKEND=s3, VGI_STORAGE_S3_BUCKET/_ENDPOINT/_PREFIX,
# VGI_STORAGE_S3_ACCESS_KEY_ID/_SECRET_ACCESS_KEY, VGI_STORAGE_S3_VERIFY=1,
# VGI_STORAGE_ALLOW_PLAIN_HTTP=1, and the same caps the vendor lane uses:
# VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES (4 KiB), VGI_RPC_MAX_REQUEST_BYTES
# (32 KiB), VGI_RPC_MAX_UPLOAD_BYTES (256 MiB), and
# VGI_TEST_ALLOW_LOOPBACK_EXTERNAL_LOCATIONS=1 for the extension.

: "${RUSTFS_IMAGE:=ghcr.io/rustfs/rustfs@sha256:29c02251c085cb04edce556304a9ec0f8fba0c40300266cf4f3d953783fe2450}"
: "${RUSTFS_PORT:=39591}"
RUSTFS_NAME="vgi-go-rustfs-$$"
RUSTFS_ACCESS=vgigoci
RUSTFS_SECRET=vgigocisecret

stop_rustfs() {
    docker rm -f "$RUSTFS_NAME" >/dev/null 2>&1 || true
}

docker run -d --name "$RUSTFS_NAME" -p "127.0.0.1:$RUSTFS_PORT:9000" \
    -e RUSTFS_ACCESS_KEY=$RUSTFS_ACCESS -e RUSTFS_SECRET_KEY=$RUSTFS_SECRET -e RUSTFS_CONSOLE_ENABLE=false \
    "$RUSTFS_IMAGE" >/dev/null || return 1 2>/dev/null || exit 1
for _ in $(seq 1 60); do
    curl -fs -o /dev/null "http://127.0.0.1:$RUSTFS_PORT/health" && break
    sleep 1
done
curl -fs -o /dev/null "http://127.0.0.1:$RUSTFS_PORT/health" || {
    docker logs "$RUSTFS_NAME" | tail -20; echo "::error::RustFS did not start"; stop_rustfs
    return 1 2>/dev/null || exit 1
}
# Create the bucket (SigV4 through curl; no AWS CLI needed).
curl -fs -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" --user "$RUSTFS_ACCESS:$RUSTFS_SECRET" \
    "http://127.0.0.1:$RUSTFS_PORT/vgi-go-ci" -o /dev/null || {
    echo "::error::could not create the RustFS bucket"; stop_rustfs; return 1 2>/dev/null || exit 1
}
echo "RustFS: http://127.0.0.1:$RUSTFS_PORT (container $RUSTFS_NAME), bucket vgi-go-ci"

export VGI_STORAGE_BACKEND=s3
export VGI_STORAGE_S3_BUCKET=vgi-go-ci
export VGI_STORAGE_S3_ENDPOINT="http://127.0.0.1:$RUSTFS_PORT"
export VGI_STORAGE_S3_PREFIX=ci/
export VGI_STORAGE_S3_ACCESS_KEY_ID=$RUSTFS_ACCESS
export VGI_STORAGE_S3_SECRET_ACCESS_KEY=$RUSTFS_SECRET
export VGI_STORAGE_S3_VERIFY=1
export VGI_STORAGE_ALLOW_PLAIN_HTTP=1
export VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES="${VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES:-4096}"
export VGI_RPC_MAX_REQUEST_BYTES="${VGI_RPC_MAX_REQUEST_BYTES:-32768}"
export VGI_RPC_MAX_UPLOAD_BYTES="${VGI_RPC_MAX_UPLOAD_BYTES:-268435456}"
export VGI_TEST_ALLOW_LOOPBACK_EXTERNAL_LOCATIONS=1
