package oversync

import (
	"encoding/base64"
	"testing"
)

func TestPayloadExtractor_StrField(t *testing.T) {
	payload := []byte(`{
		"name": "John Doe",
		"empty": "",
		"null_field": null,
		"number": 42
	}`)

	extractor, err := NewPayloadExtractor(payload)
	if err != nil {
		t.Fatalf("Failed to create extractor: %v", err)
	}

	// Test valid string
	if name := extractor.StrField("name"); name == nil || *name != "John Doe" {
		t.Errorf("Expected 'John Doe', got %v", name)
	}

	// Test empty string
	if empty := extractor.StrField("empty"); empty == nil || *empty != "" {
		t.Errorf("Expected empty string, got %v", empty)
	}

	// Test null field
	if null := extractor.StrField("null_field"); null != nil {
		t.Errorf("Expected nil for null field, got %v", null)
	}

	// Test missing field
	if missing := extractor.StrField("missing"); missing != nil {
		t.Errorf("Expected nil for missing field, got %v", missing)
	}

	// Test non-string field
	if number := extractor.StrField("number"); number != nil {
		t.Errorf("Expected nil for non-string field, got %v", number)
	}
}

func TestPayloadExtractor_StrFieldRequired(t *testing.T) {
	payload := []byte(`{"name": "John", "empty": "", "null_field": null}`)
	extractor, _ := NewPayloadExtractor(payload)

	// Test valid string
	if name, err := extractor.StrFieldRequired("name"); err != nil || name != "John" {
		t.Errorf("Expected 'John', got %v, err: %v", name, err)
	}

	// Test empty string (should be valid)
	if empty, err := extractor.StrFieldRequired("empty"); err != nil || empty != "" {
		t.Errorf("Expected empty string, got %v, err: %v", empty, err)
	}

	// Test missing field
	if _, err := extractor.StrFieldRequired("missing"); err == nil {
		t.Error("Expected error for missing field")
	}

	// Test null field
	if _, err := extractor.StrFieldRequired("null_field"); err == nil {
		t.Error("Expected error for null field")
	}
}

func TestPayloadExtractor_Int64Field(t *testing.T) {
	payload := []byte(`{
		"number": 42,
		"string_number": "123",
		"float": 42.5,
		"empty_string": "",
		"invalid": "abc",
		"null_field": null
	}`)

	extractor, _ := NewPayloadExtractor(payload)

	// Test numeric value
	if num := extractor.Int64Field("number"); num == nil || *num != 42 {
		t.Errorf("Expected 42, got %v", num)
	}

	// Test string number
	if num := extractor.Int64Field("string_number"); num == nil || *num != 123 {
		t.Errorf("Expected 123, got %v", num)
	}

	// Test float (should truncate)
	if num := extractor.Int64Field("float"); num == nil || *num != 42 {
		t.Errorf("Expected 42, got %v", num)
	}

	// Test empty string
	if num := extractor.Int64Field("empty_string"); num != nil {
		t.Errorf("Expected nil for empty string, got %v", num)
	}

	// Test invalid string
	if num := extractor.Int64Field("invalid"); num != nil {
		t.Errorf("Expected nil for invalid string, got %v", num)
	}

	// Test null field
	if num := extractor.Int64Field("null_field"); num != nil {
		t.Errorf("Expected nil for null field, got %v", num)
	}
}

func TestPayloadExtractor_BoolField(t *testing.T) {
	payload := []byte(`{
		"bool_true": true,
		"bool_false": false,
		"string_true": "true",
		"string_false": "false",
		"string_1": "1",
		"string_0": "0",
		"number_1": 1,
		"number_0": 0,
		"empty_string": "",
		"invalid": "maybe",
		"null_field": null
	}`)

	extractor, _ := NewPayloadExtractor(payload)

	tests := []struct {
		field    string
		expected *bool
	}{
		{"bool_true", &[]bool{true}[0]},
		{"bool_false", &[]bool{false}[0]},
		{"string_true", &[]bool{true}[0]},
		{"string_false", &[]bool{false}[0]},
		{"string_1", &[]bool{true}[0]},
		{"string_0", &[]bool{false}[0]},
		{"number_1", &[]bool{true}[0]},
		{"number_0", &[]bool{false}[0]},
		{"empty_string", nil},
		{"invalid", nil},
		{"null_field", nil},
		{"missing", nil},
	}

	for _, test := range tests {
		result := extractor.BoolField(test.field)
		if test.expected == nil {
			if result != nil {
				t.Errorf("Field %s: expected nil, got %v", test.field, result)
			}
		} else {
			if result == nil || *result != *test.expected {
				t.Errorf("Field %s: expected %v, got %v", test.field, *test.expected, result)
			}
		}
	}
}

func TestPayloadExtractor_UUIDField(t *testing.T) {
	validUUID := "550e8400-e29b-41d4-a716-446655440000"
	payload := []byte(`{
		"valid_uuid": "` + validUUID + `",
		"invalid_uuid": "not-a-uuid",
		"empty_string": "",
		"null_field": null
	}`)

	extractor, _ := NewPayloadExtractor(payload)

	// Test valid UUID
	if id := extractor.UUIDField("valid_uuid"); id == nil || id.String() != validUUID {
		t.Errorf("Expected %s, got %v", validUUID, id)
	}

	// Test invalid UUID
	if id := extractor.UUIDField("invalid_uuid"); id != nil {
		t.Errorf("Expected nil for invalid UUID, got %v", id)
	}

	// Test empty string
	if id := extractor.UUIDField("empty_string"); id != nil {
		t.Errorf("Expected nil for empty string, got %v", id)
	}

	// Test null field
	if id := extractor.UUIDField("null_field"); id != nil {
		t.Errorf("Expected nil for null field, got %v", id)
	}
}

func TestPayloadExtractor_UUIDFieldRequired(t *testing.T) {
	validUUID := "550e8400-e29b-41d4-a716-446655440000"
	payload := []byte(`{
		"valid_uuid": "` + validUUID + `",
		"invalid_uuid": "not-a-uuid"
	}`)

	extractor, _ := NewPayloadExtractor(payload)

	// Test valid UUID
	if id, err := extractor.UUIDFieldRequired("valid_uuid"); err != nil || id.String() != validUUID {
		t.Errorf("Expected %s, got %v, err: %v", validUUID, id, err)
	}

	// Test invalid UUID
	if _, err := extractor.UUIDFieldRequired("invalid_uuid"); err == nil {
		t.Error("Expected error for invalid UUID")
	}

	// Test missing field
	if _, err := extractor.UUIDFieldRequired("missing"); err == nil {
		t.Error("Expected error for missing field")
	}
}

func TestPayloadExtractor_HasField(t *testing.T) {
	payload := []byte(`{"existing": "value", "null_field": null}`)
	extractor, _ := NewPayloadExtractor(payload)

	if !extractor.HasField("existing") {
		t.Error("Expected HasField to return true for existing field")
	}

	if !extractor.HasField("null_field") {
		t.Error("Expected HasField to return true for null field")
	}

	if extractor.HasField("missing") {
		t.Error("Expected HasField to return false for missing field")
	}
}

func TestNewPayloadExtractorFromMap(t *testing.T) {
	data := map[string]any{
		"name": "John",
		"age":  float64(30), // Use float64 to match JSON unmarshaling behavior
	}

	extractor := NewPayloadExtractorFromMap(data)

	if name := extractor.StrField("name"); name == nil || *name != "John" {
		t.Errorf("Expected 'John', got %v", name)
	}

	if age := extractor.Int64Field("age"); age == nil || *age != 30 {
		t.Errorf("Expected 30, got %v", age)
	}
}

func TestPayloadExtractor_Base64Field(t *testing.T) {
	testData := "Hello, World!"
	encodedData := base64.StdEncoding.EncodeToString([]byte(testData))

	payload := []byte(`{
		"valid_base64": "` + encodedData + `",
		"invalid_base64": "not-valid-base64!",
		"empty_string": "",
		"null_field": null
	}`)

	extractor, _ := NewPayloadExtractor(payload)

	// Test valid base64
	if decoded := extractor.Base64Field("valid_base64"); decoded == nil || string(decoded) != testData {
		t.Errorf("Expected '%s', got %v", testData, string(decoded))
	}

	// Test invalid base64
	if decoded := extractor.Base64Field("invalid_base64"); decoded != nil {
		t.Errorf("Expected nil for invalid base64, got %v", decoded)
	}

	// Test empty string (valid base64, decodes to empty byte slice)
	if decoded := extractor.Base64Field("empty_string"); decoded == nil || len(decoded) != 0 {
		t.Errorf("Expected empty byte slice for empty string, got %v", decoded)
	}

	// Test null field
	if decoded := extractor.Base64Field("null_field"); decoded != nil {
		t.Errorf("Expected nil for null field, got %v", decoded)
	}

	// Test missing field
	if decoded := extractor.Base64Field("missing"); decoded != nil {
		t.Errorf("Expected nil for missing field, got %v", decoded)
	}
}

func TestPayloadExtractor_Base64FieldRequired(t *testing.T) {
	testData := "Hello, World!"
	encodedData := base64.StdEncoding.EncodeToString([]byte(testData))

	payload := []byte(`{
		"valid_base64": "` + encodedData + `",
		"invalid_base64": "not-valid-base64!"
	}`)

	extractor, _ := NewPayloadExtractor(payload)

	// Test valid base64
	if decoded, err := extractor.Base64FieldRequired("valid_base64"); err != nil || string(decoded) != testData {
		t.Errorf("Expected '%s', got %v, err: %v", testData, string(decoded), err)
	}

	// Test invalid base64
	if _, err := extractor.Base64FieldRequired("invalid_base64"); err == nil {
		t.Error("Expected error for invalid base64")
	}

	// Test missing field
	if _, err := extractor.Base64FieldRequired("missing"); err == nil {
		t.Error("Expected error for missing field")
	}
}
