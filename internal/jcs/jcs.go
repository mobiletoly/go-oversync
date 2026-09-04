// Package jcs exposes Oversync's RFC 8785 canonical JSON boundary.
//
// Exact database integers and decimals are strings before they reach this
// package. JSON number tokens are therefore limited to the ordinary JCS
// binary64 domain; this package intentionally has no arbitrary-precision
// number extension.
package jcs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"unicode/utf8"

	jsoncanonicalizer "github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
)

// Canonicalize validates raw JSON and returns its RFC 8785 representation.
// Oversync hash inputs are always objects or arrays, as required by the
// reference implementation.
func Canonicalize(raw []byte) ([]byte, error) {
	if !utf8.Valid(raw) {
		return nil, fmt.Errorf("JCS input is not valid UTF-8")
	}
	value, err := validateSingleJSONValue(raw, validationOptions{maxDepth: 64, requireFiniteBinary64: true})
	if err != nil {
		return nil, err
	}
	input := raw
	scalar := false
	if _, ok := value.(map[string]any); !ok {
		if _, ok := value.([]any); !ok {
			scalar = true
			input = append(append([]byte{'['}, raw...), ']')
		}
	}
	canonical, err := jsoncanonicalizer.Transform(input)
	if err != nil {
		return nil, fmt.Errorf("canonicalize RFC 8785 JSON: %w", err)
	}
	if scalar {
		canonical = canonical[1 : len(canonical)-1]
	}
	return canonical, nil
}

// Decode validates one JSON value and decodes it using json.Number so callers
// can replace schema-typed database number tokens with strings before JCS
// canonicalization. It never routes number tokens through binary floating
// point.
func Decode(raw []byte) (any, error) {
	if !utf8.Valid(raw) {
		return nil, fmt.Errorf("JSON input is not valid UTF-8")
	}
	return validateSingleJSONValue(raw, validationOptions{maxDepth: 64, requireFiniteBinary64: true})
}

// DecodeObject is Decode restricted to a JSON object.
func DecodeObject(raw []byte) (map[string]any, error) {
	value, err := Decode(raw)
	if err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("JCS value must be an object")
	}
	return object, nil
}

// DecodeObjectLossless decodes a database-produced JSON object without
// constraining JSON number tokens to binary64. Callers must apply their
// schema-specific numeric and nested-JSON validation before canonicalizing
// the result.
func DecodeObjectLossless(raw []byte) (map[string]any, error) {
	if !utf8.Valid(raw) {
		return nil, fmt.Errorf("JSON input is not valid UTF-8")
	}
	value, err := validateSingleJSONValue(raw, validationOptions{maxDepth: -1})
	if err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("JCS value must be an object")
	}
	return object, nil
}

// ValidateStructure validates one JSON value without applying the JCS
// binary64-number or nesting-depth domain. It is used at HTTP decode
// boundaries so malformed UTF-8, Unicode escapes, and duplicate object names
// are rejected before encoding/json can repair or collapse them.
func ValidateStructure(raw []byte) error {
	if !utf8.Valid(raw) {
		return fmt.Errorf("JSON input is not valid UTF-8")
	}
	_, err := validateSingleJSONValue(raw, validationOptions{maxDepth: -1})
	return err
}

// Marshal serializes value and returns RFC 8785 canonical bytes.
func Marshal(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal JSON for JCS: %w", err)
	}
	return Canonicalize(raw)
}

// Equal reports whether two JSON objects or arrays have identical RFC 8785
// canonical representations.
func Equal(left, right []byte) (bool, error) {
	leftCanonical, err := Canonicalize(left)
	if err != nil {
		return false, err
	}
	rightCanonical, err := Canonicalize(right)
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftCanonical, rightCanonical), nil
}

type validationOptions struct {
	maxDepth              int
	requireFiniteBinary64 bool
}

func validateSingleJSONValue(raw []byte, options validationOptions) (any, error) {
	if err := validateUnicodeScalarEscapes(raw); err != nil {
		return nil, err
	}
	if err := validateTokenStructure(raw, options); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("invalid JSON for JCS: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("JCS input contains more than one JSON value")
		}
		return nil, fmt.Errorf("invalid trailing JSON data for JCS: %w", err)
	}
	return value, nil
}

func validateTokenStructure(raw []byte, options validationOptions) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := validateTokenValue(decoder, -1, options); err != nil {
		return err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("JCS input contains more than one JSON value after %v", token)
		}
		return fmt.Errorf("invalid trailing JSON data for JCS: %w", err)
	}
	return nil
}

func validateTokenValue(decoder *json.Decoder, parentDepth int, options validationOptions) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("invalid JSON for JCS: %w", err)
	}
	switch value := token.(type) {
	case json.Delim:
		depth := parentDepth + 1
		if options.maxDepth >= 0 && depth > options.maxDepth {
			return fmt.Errorf("JCS nesting exceeds maximum depth %d", options.maxDepth)
		}
		switch value {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				nameToken, err := decoder.Token()
				if err != nil {
					return fmt.Errorf("invalid JSON object member for JCS: %w", err)
				}
				name, ok := nameToken.(string)
				if !ok {
					return fmt.Errorf("invalid JSON object member name for JCS")
				}
				if _, exists := seen[name]; exists {
					return fmt.Errorf("duplicate JSON object member %q is not allowed for JCS", name)
				}
				seen[name] = struct{}{}
				if err := validateTokenValue(decoder, depth, options); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return fmt.Errorf("invalid JSON object terminator for JCS")
			}
		case '[':
			for decoder.More() {
				if err := validateTokenValue(decoder, depth, options); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return fmt.Errorf("invalid JSON array terminator for JCS")
			}
		default:
			return fmt.Errorf("unexpected JSON delimiter %q for JCS", value)
		}
	case json.Number:
		if options.requireFiniteBinary64 {
			parsed, err := strconv.ParseFloat(value.String(), 64)
			if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
				return fmt.Errorf("JSON number is outside the finite binary64 JCS domain")
			}
		}
	}
	return nil
}

func validateUnicodeScalarEscapes(raw []byte) error {
	inString := false
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '"':
			inString = !inString
		case '\\':
			if !inString || i+1 >= len(raw) {
				continue
			}
			if raw[i+1] != 'u' {
				i++
				continue
			}
			value, ok := parseHex4(raw, i+2)
			if !ok {
				continue
			}
			if value == 0 {
				return fmt.Errorf("U+0000 is outside the PostgreSQL-compatible Unicode scalar domain")
			}
			if value >= 0xdc00 && value <= 0xdfff {
				return fmt.Errorf("unpaired low surrogate is not a Unicode scalar value")
			}
			if value >= 0xd800 && value <= 0xdbff {
				if i+12 > len(raw) || raw[i+6] != '\\' || raw[i+7] != 'u' {
					return fmt.Errorf("unpaired high surrogate is not a Unicode scalar value")
				}
				low, lowOK := parseHex4(raw, i+8)
				if !lowOK || low < 0xdc00 || low > 0xdfff {
					return fmt.Errorf("unpaired high surrogate is not a Unicode scalar value")
				}
				i += 11
				continue
			}
			i += 5
		}
	}
	return nil
}

func parseHex4(raw []byte, start int) (uint16, bool) {
	if start+4 > len(raw) {
		return 0, false
	}
	var value uint16
	for _, b := range raw[start : start+4] {
		value <<= 4
		switch {
		case b >= '0' && b <= '9':
			value |= uint16(b - '0')
		case b >= 'a' && b <= 'f':
			value |= uint16(b-'a') + 10
		case b >= 'A' && b <= 'F':
			value |= uint16(b-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}
