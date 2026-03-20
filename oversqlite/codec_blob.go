// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

func isHexStringValue(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func tryDecodeBase64Exact(s string) ([]byte, bool) {
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, false
	}
	// Avoid treating arbitrary strings as base64: ensure round-trip equality.
	if base64.StdEncoding.EncodeToString(decoded) != s {
		return nil, false
	}
	return decoded, true
}

// decodeBlobBytesFromString is for local/internal blob representations only.
// The supported wire contract is stricter and must go through
// decodeCanonicalWireBlobBytes / decodeCanonicalWireUUIDBytes instead.
func decodeBlobBytesFromString(s string) ([]byte, error) {
	if s == "" {
		return []byte{}, nil
	}

	// Accept UUID string (dashed or 32-hex) as bytes.
	if parsed, err := uuid.Parse(s); err == nil {
		b := parsed[:]
		return b, nil
	}

	if decoded, ok := tryDecodeBase64Exact(s); ok {
		return decoded, nil
	}

	hs := strings.TrimSpace(s)
	if len(hs)%2 == 0 && isHexStringValue(hs) {
		decoded, err := hex.DecodeString(hs)
		if err == nil {
			return decoded, nil
		}
	}

	return nil, fmt.Errorf("invalid blob encoding")
}

// decodeCanonicalWireBlobBytes accepts only the canonical wire format for non-key
// binary payload fields: standard base64.
func decodeCanonicalWireBlobBytes(s string) ([]byte, error) {
	if s == "" {
		return []byte{}, nil
	}
	decoded, ok := tryDecodeBase64Exact(s)
	if !ok {
		return nil, fmt.Errorf("invalid canonical wire blob encoding")
	}
	return decoded, nil
}

// decodeUUIDBytesFromString is for local/internal UUID-as-bytes representations only.
// The supported wire contract for UUID-valued keys and key columns is stricter and
// must go through decodeCanonicalWireUUIDBytes instead.
func decodeUUIDBytesFromString(s string) ([]byte, error) {
	// Prefer UUID parsing for canonical UUID strings (with dashes) and raw 32-hex.
	if parsed, err := uuid.Parse(s); err == nil {
		b := parsed[:]
		return b, nil
	}

	if decoded, ok := tryDecodeBase64Exact(s); ok {
		if len(decoded) != 16 {
			return nil, fmt.Errorf("base64 decoded length is %d, want 16", len(decoded))
		}
		return decoded, nil
	}

	hs := strings.TrimSpace(s)
	if len(hs)%2 == 0 && isHexStringValue(hs) {
		decoded, err := hex.DecodeString(hs)
		if err != nil {
			return nil, fmt.Errorf("invalid hex: %w", err)
		}
		if len(decoded) != 16 {
			return nil, fmt.Errorf("hex decoded length is %d, want 16", len(decoded))
		}
		return decoded, nil
	}

	return nil, fmt.Errorf("not a UUID encoding")
}

// decodeCanonicalWireUUIDBytes accepts only the canonical wire format for UUID-valued
// keys and UUID payload key columns: dashed lowercase UUID text.
func decodeCanonicalWireUUIDBytes(s string) ([]byte, error) {
	if strings.TrimSpace(s) != s || len(s) != 36 {
		return nil, fmt.Errorf("invalid canonical wire UUID encoding")
	}
	parsed, err := uuid.Parse(s)
	if err != nil {
		return nil, fmt.Errorf("invalid canonical wire UUID encoding: %w", err)
	}
	if parsed.String() != strings.ToLower(s) {
		return nil, fmt.Errorf("invalid canonical wire UUID encoding")
	}
	b := parsed[:]
	return b, nil
}
