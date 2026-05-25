// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgi

import (
	"encoding/hex"
	"fmt"

	"github.com/Query-farm/vgi-rpc/vgirpc"
)

// deriveShardKey returns the Cloudflare-DO routing key for an attach: the
// 16-byte framework UUID at the head of the unwrapped attach plaintext, as
// "att-" + hex(uuid). One DO per logical ATTACH — stable across re-seals and
// globally unique (unlike the random-nonce ciphertext or possibly-non-unique
// catalog bytes). Mirrors vgi-python's _derive_shard_key.
//
// The UUID must be exactly 16 bytes: the storage path is always bound to a
// logical ATTACH, so a missing/short value is a programming error.
func deriveShardKey(attachUUID []byte) (string, error) {
	if len(attachUUID) != attachUUIDLen {
		return "", fmt.Errorf("shard_key requires a %d-byte attach uuid, got %d", attachUUIDLen, len(attachUUID))
	}
	return "att-" + hex.EncodeToString(attachUUID), nil
}

// shardKeyForAttach unwraps the sealed attach and derives its shard key. An
// empty/absent attach yields "" (non-sharding backends ignore the key; the CfDo
// backend rejects an empty key server-side, the "must not happen" case).
func (w *Worker) shardKeyForAttach(sealed []byte, cc *vgirpc.CallContext) (string, error) {
	if len(sealed) == 0 {
		return "", nil
	}
	full, err := w.openAttachFull(sealed, cc)
	if err != nil {
		return "", err
	}
	if len(full) < attachUUIDLen {
		return "", nil
	}
	return deriveShardKey(full[:attachUUIDLen])
}

// shardKeyForAttachPtr is shardKeyForAttach for a nilable wire field.
func (w *Worker) shardKeyForAttachPtr(sealed *[]byte, cc *vgirpc.CallContext) (string, error) {
	if sealed == nil {
		return "", nil
	}
	return w.shardKeyForAttach(*sealed, cc)
}
