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
