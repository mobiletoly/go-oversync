// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// BlobPayloadExtractor provides utilities for extracting BLOB fields from JSON payloads.
// This handles both hex-encoded (from triggers) and base64-encoded (from server) BLOB data.
type BlobPayloadExtractor struct {
	data map[string]any
}

// NewBlobPayloadExtractor creates a new BlobPayloadExtractor from JSON payload bytes.
func NewBlobPayloadExtractor(payload []byte) (*BlobPayloadExtractor, error) {
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, fmt.Errorf("parse payload: %w", err)
	}
	return &BlobPayloadExtractor{data: m}, nil
}

// BlobField extracts a BLOB field and returns the decoded bytes.
// Handles both hex-encoded (from triggers) and base64-encoded (from server) data.
// Returns nil if the field is missing, null, not a string, or cannot be decoded.
func (p *BlobPayloadExtractor) BlobField(key string) []byte {
	if v, ok := p.data[key]; ok && v != nil {
		if str, ok := v.(string); ok {
			// Detect format based on string characteristics
			// Hex strings are typically even length and contain only hex characters
			if len(str) > 0 && len(str)%2 == 0 && isHexString(str) {
				// Try hex first for hex-like strings
				if decoded, err := hex.DecodeString(str); err == nil {
					return decoded
				}
			}
			// Try base64 for other strings (server format)
			if decoded, err := base64.StdEncoding.DecodeString(str); err == nil {
				return decoded
			}
			// Fallback to hex if base64 failed
			if decoded, err := hex.DecodeString(str); err == nil {
				return decoded
			}
		}
	}
	return nil
}

// isHexString checks if a string contains only hexadecimal characters
func isHexString(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// BlobFieldRequired extracts a required BLOB field and returns the decoded bytes.
// Returns an error if the field is missing, null, not a string, or cannot be decoded.
func (p *BlobPayloadExtractor) BlobFieldRequired(key string) ([]byte, error) {
	if v, ok := p.data[key]; ok && v != nil {
		if str, ok := v.(string); ok {
			// Use the same logic as BlobField
			// Detect format based on string characteristics
			if len(str) > 0 && len(str)%2 == 0 && isHexString(str) {
				// Try hex first for hex-like strings
				if decoded, err := hex.DecodeString(str); err == nil {
					return decoded, nil
				}
			}
			// Try base64 for other strings (server format)
			if decoded, err := base64.StdEncoding.DecodeString(str); err == nil {
				return decoded, nil
			}
			// Fallback to hex if base64 failed
			if decoded, err := hex.DecodeString(str); err == nil {
				return decoded, nil
			}
			return nil, fmt.Errorf("field '%s' is not valid base64 or hex", key)
		}
		return nil, fmt.Errorf("field '%s' is not a string", key)
	}
	return nil, fmt.Errorf("required BLOB field '%s' is missing or null", key)
}

// BlobUUIDField extracts a BLOB UUID field and returns the UUID.
// This is useful for BLOB primary keys and foreign keys.
// Returns zero UUID if the field is missing, null, not a string, or cannot be decoded.
func (p *BlobPayloadExtractor) BlobUUIDField(key string) uuid.UUID {
	if blobData := p.BlobField(key); blobData != nil {
		if parsedUUID, err := uuid.FromBytes(blobData); err == nil {
			return parsedUUID
		}
	}
	return uuid.Nil
}

// BlobUUIDFieldRequired extracts a required BLOB UUID field and returns the UUID.
// Returns an error if the field is missing, null, not a string, cannot be decoded, or is not a valid UUID.
func (p *BlobPayloadExtractor) BlobUUIDFieldRequired(key string) (uuid.UUID, error) {
	blobData, err := p.BlobFieldRequired(key)
	if err != nil {
		return uuid.Nil, err
	}

	parsedUUID, err := uuid.FromBytes(blobData)
	if err != nil {
		return uuid.Nil, fmt.Errorf("field '%s' is not a valid UUID: %w", key, err)
	}

	return parsedUUID, nil
}

// StringField extracts a string field from the payload.
// Returns nil if the field is missing, null, or not a string.
func (p *BlobPayloadExtractor) StringField(key string) *string {
	if v, ok := p.data[key]; ok && v != nil {
		if str, ok := v.(string); ok {
			return &str
		}
	}
	return nil
}

// StringFieldRequired extracts a required string field from the payload.
// Returns an error if the field is missing, null, or not a string.
func (p *BlobPayloadExtractor) StringFieldRequired(key string) (string, error) {
	if v, ok := p.data[key]; ok && v != nil {
		if str, ok := v.(string); ok {
			return str, nil
		}
		return "", fmt.Errorf("field '%s' is not a string", key)
	}
	return "", fmt.Errorf("required string field '%s' is missing or null", key)
}

// UUIDField extracts a UUID field from the payload (as string).
// Returns zero UUID if the field is missing, null, not a string, or not a valid UUID.
func (p *BlobPayloadExtractor) UUIDField(key string) uuid.UUID {
	if str := p.StringField(key); str != nil {
		if parsedUUID, err := uuid.Parse(*str); err == nil {
			return parsedUUID
		}
	}
	return uuid.Nil
}

// UUIDFieldRequired extracts a required UUID field from the payload (as string).
// Returns an error if the field is missing, null, not a string, or not a valid UUID.
func (p *BlobPayloadExtractor) UUIDFieldRequired(key string) (uuid.UUID, error) {
	str, err := p.StringFieldRequired(key)
	if err != nil {
		return uuid.Nil, err
	}

	parsedUUID, err := uuid.Parse(str)
	if err != nil {
		return uuid.Nil, fmt.Errorf("field '%s' is not a valid UUID: %w", key, err)
	}

	return parsedUUID, nil
}

// EncodeBlobAsHex encodes binary data as a hex string (lowercase) for trigger storage.
func EncodeBlobAsHex(data []byte) string {
	return fmt.Sprintf("%x", data)
}

// EncodeBlobUUIDAsBase64 encodes a UUID as a base64 string for server communication.
func EncodeBlobUUIDAsBase64(id uuid.UUID) string {
	return base64.StdEncoding.EncodeToString(id[:])
}

// EncodeBlobUUIDAsHex encodes a UUID as a hex string (lowercase) for trigger storage.
func EncodeBlobUUIDAsHex(id uuid.UUID) string {
	return fmt.Sprintf("%x", id[:])
}

// DecodeBlobUUIDFromBase64 decodes a base64 string back to a UUID.
func DecodeBlobUUIDFromBase64(base64Str string) (uuid.UUID, error) {
	decoded, err := base64.StdEncoding.DecodeString(base64Str)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid base64: %w", err)
	}

	parsedUUID, err := uuid.FromBytes(decoded)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid UUID bytes: %w", err)
	}

	return parsedUUID, nil
}

// DecodeBlobUUIDFromHex decodes a hex string back to a UUID.
func DecodeBlobUUIDFromHex(hexStr string) (uuid.UUID, error) {
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid hex: %w", err)
	}

	parsedUUID, err := uuid.FromBytes(decoded)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid UUID bytes: %w", err)
	}

	return parsedUUID, nil
}
