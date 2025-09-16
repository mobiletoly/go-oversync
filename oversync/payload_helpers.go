// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// PayloadExtractor provides utilities for extracting typed fields from JSON payloads.
// This is commonly used in ApplyUpsert handlers to parse sync payloads.
type PayloadExtractor struct {
	data map[string]any
}

// NewPayloadExtractor creates a new PayloadExtractor from JSON payload bytes.
func NewPayloadExtractor(payload []byte) (*PayloadExtractor, error) {
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, fmt.Errorf("parse payload: %w", err)
	}
	return &PayloadExtractor{data: m}, nil
}

// NewPayloadExtractorFromMap creates a new PayloadExtractor from an existing map.
func NewPayloadExtractorFromMap(data map[string]any) *PayloadExtractor {
	return &PayloadExtractor{data: data}
}

// StrField extracts a nullable string from the payload.
// Returns nil if the field is missing, null, or not a string.
func (p *PayloadExtractor) StrField(key string) *string {
	if v, ok := p.data[key]; ok && v != nil {
		if s, ok2 := v.(string); ok2 {
			return &s
		}
	}
	return nil
}

// StrFieldRequired extracts a required string from the payload.
// Returns an error if the field is missing, null, or not a string.
func (p *PayloadExtractor) StrFieldRequired(key string) (string, error) {
	if s := p.StrField(key); s != nil {
		return *s, nil
	}
	return "", fmt.Errorf("required string field '%s' is missing or invalid", key)
}

// Int64Field extracts a nullable int64 from the payload.
// Accepts both numeric values and numeric strings.
// Returns nil if the field is missing, null, or cannot be converted.
func (p *PayloadExtractor) Int64Field(key string) *int64 {
	if v, ok := p.data[key]; ok && v != nil {
		switch t := v.(type) {
		case float64:
			n := int64(t)
			return &n
		case string:
			if t == "" {
				return nil
			}
			var n int64
			_, err := fmt.Sscan(t, &n)
			if err == nil {
				return &n
			}
		}
	}
	return nil
}

// Int64FieldRequired extracts a required int64 from the payload.
// Returns an error if the field is missing, null, or cannot be converted.
func (p *PayloadExtractor) Int64FieldRequired(key string) (int64, error) {
	if n := p.Int64Field(key); n != nil {
		return *n, nil
	}
	return 0, fmt.Errorf("required int64 field '%s' is missing or invalid", key)
}

// Float64Field extracts a nullable float64 from the payload.
// Accepts both numeric values and numeric strings.
// Returns nil if the field is missing, null, or cannot be converted.
func (p *PayloadExtractor) Float64Field(key string) *float64 {
	if v, ok := p.data[key]; ok && v != nil {
		switch t := v.(type) {
		case float64:
			return &t
		case string:
			if t == "" {
				return nil
			}
			var n float64
			_, err := fmt.Sscan(t, &n)
			if err == nil {
				return &n
			}
		}
	}
	return nil
}

// Float64FieldRequired extracts a required float64 from the payload.
// Returns an error if the field is missing, null, or cannot be converted.
func (p *PayloadExtractor) Float64FieldRequired(key string) (float64, error) {
	if n := p.Float64Field(key); n != nil {
		return *n, nil
	}
	return 0, fmt.Errorf("required float64 field '%s' is missing or invalid", key)
}

// BoolField extracts a nullable bool from the payload.
// Accepts bool values, numeric values (0=false, non-zero=true), and string values ("true"/"false", "1"/"0").
// Returns nil if the field is missing, null, or cannot be converted.
func (p *PayloadExtractor) BoolField(key string) *bool {
	if v, ok := p.data[key]; ok && v != nil {
		switch t := v.(type) {
		case bool:
			return &t
		case float64:
			b := t != 0
			return &b
		case string:
			if t == "" {
				return nil
			}
			if t == "1" || t == "true" || t == "TRUE" {
				v := true
				return &v
			}
			if t == "0" || t == "false" || t == "FALSE" {
				v := false
				return &v
			}
		}
	}
	return nil
}

// BoolFieldRequired extracts a required bool from the payload.
// Returns an error if the field is missing, null, or cannot be converted.
func (p *PayloadExtractor) BoolFieldRequired(key string) (bool, error) {
	if b := p.BoolField(key); b != nil {
		return *b, nil
	}
	return false, fmt.Errorf("required bool field '%s' is missing or invalid", key)
}

// UUIDField extracts a nullable UUID from the payload.
// Accepts string values that can be parsed as UUIDs.
// Returns nil if the field is missing, null, or cannot be parsed as UUID.
func (p *PayloadExtractor) UUIDField(key string) *uuid.UUID {
	if s := p.StrField(key); s != nil {
		if id, err := uuid.Parse(*s); err == nil {
			return &id
		}
	}
	return nil
}

// UUIDFieldRequired extracts a required UUID from the payload.
// Returns an error if the field is missing, null, or cannot be parsed as UUID.
func (p *PayloadExtractor) UUIDFieldRequired(key string) (uuid.UUID, error) {
	if s := p.StrField(key); s != nil {
		if id, err := uuid.Parse(*s); err == nil {
			return id, nil
		}
		return uuid.Nil, fmt.Errorf("field '%s' is not a valid UUID: %s", key, *s)
	}
	return uuid.Nil, fmt.Errorf("required UUID field '%s' is missing or invalid", key)
}

// HasField checks if a field exists in the payload (even if it's null).
func (p *PayloadExtractor) HasField(key string) bool {
	_, ok := p.data[key]
	return ok
}

// Base64Field extracts a nullable base64-decoded byte slice from the payload.
// Returns nil if the field is missing, null, not a string, or cannot be decoded.
func (p *PayloadExtractor) Base64Field(key string) []byte {
	if s := p.StrField(key); s != nil {
		if decoded, err := base64.StdEncoding.DecodeString(*s); err == nil {
			return decoded
		}
	}
	return nil
}

// Base64FieldRequired extracts a required base64-decoded byte slice from the payload.
// Returns an error if the field is missing, null, not a string, or cannot be decoded.
func (p *PayloadExtractor) Base64FieldRequired(key string) ([]byte, error) {
	if s := p.StrField(key); s != nil {
		if decoded, err := base64.StdEncoding.DecodeString(*s); err == nil {
			return decoded, nil
		} else {
			return nil, fmt.Errorf("field '%s' is not valid base64: %w", key, err)
		}
	}
	return nil, fmt.Errorf("required base64 field '%s' is missing or invalid", key)
}

func OptionallyConvertBase64EncodedUUID(payloadValue string) (any, error) {
	decoded, err := base64.StdEncoding.DecodeString(payloadValue)
	if err != nil {
		return payloadValue, nil
	}
	decodedUUID, err := uuid.FromBytes(decoded)
	if err != nil {
		return payloadValue, fmt.Errorf("base64 decoded value is not a valid UUID: %w", err)
	}
	return decodedUUID, nil
}

func ConvertBase64EncodedUUID(value string) (uuid.UUID, error) {
	if decoded, err := base64.StdEncoding.DecodeString(value); err == nil {
		return uuid.FromBytes(decoded)
	}
	return uuid.Nil, fmt.Errorf("field is not valid base64: %s", value)
}
