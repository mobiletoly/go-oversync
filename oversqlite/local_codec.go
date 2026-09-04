package oversqlite

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/internal/wirevalue"
	"github.com/mobiletoly/go-oversync/oversync"
)

func (c *Client) declaredColumn(table, column string) (LocalColumnCodec, oversync.GenerationColumn, bool) {
	if c == nil || c.localGeneration == nil {
		return "", oversync.GenerationColumn{}, false
	}
	table = strings.ToLower(table)
	column = strings.ToLower(column)
	codec, ok := c.localGeneration.codecs[c.schemaForTable(table)+"."+table][column]
	if !ok {
		return "", oversync.GenerationColumn{}, false
	}
	for _, tableContract := range c.localGeneration.contract.Tables() {
		if tableContract.Schema != c.schemaForTable(table) || tableContract.Table != table {
			continue
		}
		for _, declared := range tableContract.Columns {
			if declared.Name == column {
				return codec, declared, true
			}
		}
	}
	return "", oversync.GenerationColumn{}, false
}

func (c *Client) processPayloadWithDeclaredCodecs(table string, payload map[string]any) error {
	for column, value := range payload {
		codec, declared, ok := c.declaredColumn(table, column)
		if !ok {
			return fmt.Errorf("column %s.%s is absent from the local generation contract", table, column)
		}
		if value == nil {
			if !declared.Nullable {
				return fmt.Errorf("column %s.%s must not be null", table, column)
			}
			continue
		}
		encoded, err := wireValueFromLocalCodec(codec, value)
		if err != nil {
			return fmt.Errorf("column %s.%s codec %s: %w", table, column, codec, err)
		}
		payload[column] = encoded
	}
	return nil
}

func wireValueFromLocalCodec(codec LocalColumnCodec, value any) (any, error) {
	rawString := func() (string, error) {
		switch typed := value.(type) {
		case string:
			return typed, nil
		case []byte:
			return string(typed), nil
		default:
			return "", fmt.Errorf("local value must be text")
		}
	}
	switch codec {
	case LocalCodecBooleanInteger:
		raw, err := rawString()
		if err != nil {
			return nil, err
		}
		switch raw {
		case "0":
			return false, nil
		case "1":
			return true, nil
		default:
			return nil, fmt.Errorf("Boolean INTEGER must be 0 or 1")
		}
	case LocalCodecInt64Integer:
		raw, err := rawString()
		if err != nil {
			return nil, err
		}
		_, err = wirevalue.ParseInt64(raw)
		if err != nil {
			return nil, err
		}
		return raw, nil
	case LocalCodecDecimalText:
		raw, err := rawString()
		if err != nil {
			return nil, err
		}
		if err := wirevalue.ValidateDecimal(raw); err != nil {
			return nil, err
		}
		return raw, nil
	case LocalCodecFloat64Real:
		var value64 float64
		var err error
		switch typed := value.(type) {
		case float64:
			value64 = typed
		case float32:
			value64 = float64(typed)
		case json.Number:
			value64, err = typed.Float64()
		case string:
			value64, err = strconv.ParseFloat(typed, 64)
		case []byte:
			value64, err = strconv.ParseFloat(string(typed), 64)
		default:
			err = fmt.Errorf("local floating value has unsupported type %T", value)
		}
		if err != nil || math.IsInf(value64, 0) || math.IsNaN(value64) {
			return nil, fmt.Errorf("floating value must be finite")
		}
		return wirevalue.RenderFloat64(value64)
	case LocalCodecText:
		raw, err := rawString()
		if err != nil {
			return nil, err
		}
		if !utf8.ValidString(raw) || strings.IndexByte(raw, 0) >= 0 {
			return nil, fmt.Errorf("text must be valid UTF-8 without U+0000")
		}
		return raw, nil
	case LocalCodecBytesBlob:
		raw, err := rawString()
		if err != nil {
			return nil, err
		}
		decoded, err := hex.DecodeString(raw)
		if err != nil {
			return nil, fmt.Errorf("BLOB capture is not lowercase hexadecimal")
		}
		if hex.EncodeToString(decoded) != raw {
			return nil, fmt.Errorf("BLOB capture is not lowercase hexadecimal")
		}
		return base64.StdEncoding.EncodeToString(decoded), nil
	case LocalCodecUUIDText:
		raw, err := rawString()
		if err != nil {
			return nil, err
		}
		parsed, err := uuid.Parse(raw)
		if err != nil || len(raw) != 36 || parsed.String() != raw {
			return nil, fmt.Errorf("UUID text must use lowercase dashed form")
		}
		return raw, nil
	case LocalCodecUUIDBlob:
		raw, err := rawString()
		if err != nil {
			return nil, err
		}
		decoded, err := hex.DecodeString(raw)
		if err != nil || len(decoded) != 16 || hex.EncodeToString(decoded) != raw {
			return nil, fmt.Errorf("UUID BLOB must contain 16 bytes")
		}
		parsed, err := uuid.FromBytes(decoded)
		if err != nil {
			return nil, err
		}
		return parsed.String(), nil
	case LocalCodecJSONText:
		raw, err := rawString()
		if err != nil {
			return nil, err
		}
		canonical, err := jcs.Canonicalize([]byte(raw))
		if err != nil || jsonValueContainsNUL(raw) {
			return nil, fmt.Errorf("JSON text must be valid without U+0000")
		}
		return string(canonical), nil
	default:
		return nil, fmt.Errorf("unsupported local codec")
	}
}

func sqliteValueFromLocalCodec(codec LocalColumnCodec, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	raw, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("stored local value must be text")
	}
	switch codec {
	case LocalCodecBooleanInteger:
		if raw == "0" {
			return int64(0), nil
		}
		if raw == "1" {
			return int64(1), nil
		}
		return nil, fmt.Errorf("Boolean INTEGER must be 0 or 1")
	case LocalCodecInt64Integer:
		parsed, err := wirevalue.ParseInt64(raw)
		if err != nil {
			return nil, err
		}
		return parsed, nil
	case LocalCodecDecimalText:
		if err := wirevalue.ValidateDecimal(raw); err != nil {
			return nil, err
		}
		return raw, nil
	case LocalCodecFloat64Real:
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
			return nil, fmt.Errorf("floating value must be finite")
		}
		return parsed, nil
	case LocalCodecText:
		if !utf8.ValidString(raw) || strings.IndexByte(raw, 0) >= 0 {
			return nil, fmt.Errorf("text must be valid UTF-8 without U+0000")
		}
		return raw, nil
	case LocalCodecBytesBlob:
		decoded, err := hex.DecodeString(raw)
		if err != nil || hex.EncodeToString(decoded) != raw {
			return nil, fmt.Errorf("BLOB capture is not lowercase hexadecimal")
		}
		return decoded, nil
	case LocalCodecUUIDText:
		parsed, err := uuid.Parse(raw)
		if err != nil || len(raw) != 36 || parsed.String() != raw {
			return nil, fmt.Errorf("UUID text must use lowercase dashed form")
		}
		return raw, nil
	case LocalCodecUUIDBlob:
		decoded, err := hex.DecodeString(raw)
		if err != nil || len(decoded) != 16 || hex.EncodeToString(decoded) != raw {
			return nil, fmt.Errorf("UUID BLOB must contain 16 bytes")
		}
		return decoded, nil
	case LocalCodecJSONText:
		_, err := jcs.Canonicalize([]byte(raw))
		if err != nil || jsonValueContainsNUL(raw) {
			return nil, fmt.Errorf("JSON text must be valid without U+0000")
		}
		return raw, nil
	default:
		return nil, fmt.Errorf("unsupported local codec")
	}
}

func wireValueFromSQLiteCodec(codec LocalColumnCodec, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch codec {
	case LocalCodecBooleanInteger:
		integer, ok := value.(int64)
		if !ok || (integer != 0 && integer != 1) {
			return nil, fmt.Errorf("Boolean INTEGER must be 0 or 1")
		}
		return integer == 1, nil
	case LocalCodecInt64Integer:
		integer, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("INTEGER value is invalid")
		}
		return strconv.FormatInt(integer, 10), nil
	case LocalCodecFloat64Real:
		real, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("REAL value is invalid")
		}
		return wirevalue.RenderFloat64(real)
	case LocalCodecBytesBlob:
		blob, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("BLOB value is invalid")
		}
		return base64.StdEncoding.EncodeToString(blob), nil
	case LocalCodecUUIDBlob:
		blob, ok := value.([]byte)
		if !ok || len(blob) != 16 {
			return nil, fmt.Errorf("UUID BLOB must contain 16 bytes")
		}
		parsed, err := uuid.FromBytes(blob)
		if err != nil {
			return nil, err
		}
		return parsed.String(), nil
	case LocalCodecDecimalText, LocalCodecText, LocalCodecUUIDText, LocalCodecJSONText:
		return wireValueFromLocalCodec(codec, value)
	default:
		return nil, fmt.Errorf("unsupported local codec")
	}
}

func (c *Client) sqliteValueWithDeclaredCodec(table string, col ColumnInfo, value any) (any, error) {
	codec, declared, ok := c.declaredColumn(table, col.Name)
	if !ok {
		return nil, fmt.Errorf("column is absent from the local generation contract")
	}
	if value == nil {
		if !declared.Nullable {
			return nil, fmt.Errorf("non-null column received null")
		}
		return nil, nil
	}
	switch codec {
	case LocalCodecBooleanInteger:
		boolean, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("wire Boolean must be true or false")
		}
		if boolean {
			return int64(1), nil
		}
		return int64(0), nil
	case LocalCodecInt64Integer:
		raw, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("wire integer must be a string")
		}
		parsed, err := wirevalue.ParseInt64(raw)
		if err != nil {
			return nil, err
		}
		return parsed, nil
	case LocalCodecDecimalText:
		raw, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("wire decimal must be a string")
		}
		if err := wirevalue.ValidateDecimal(raw); err != nil {
			return nil, err
		}
		return raw, nil
	case LocalCodecFloat64Real:
		raw, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("wire float64 must be a string")
		}
		return wirevalue.ParseFloat64(raw)
	case LocalCodecText:
		raw, ok := value.(string)
		if !ok || !utf8.ValidString(raw) || strings.IndexByte(raw, 0) >= 0 {
			return nil, fmt.Errorf("wire text must be valid UTF-8 without U+0000")
		}
		return raw, nil
	case LocalCodecBytesBlob:
		raw, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("wire bytes must be a base64 string")
		}
		decoded, err := base64.StdEncoding.DecodeString(raw)
		if err != nil || base64.StdEncoding.EncodeToString(decoded) != raw {
			return nil, fmt.Errorf("wire bytes must use canonical padded base64")
		}
		return decoded, nil
	case LocalCodecUUIDText:
		raw, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("wire UUID must be a string")
		}
		parsed, err := uuid.Parse(raw)
		if err != nil || len(raw) != 36 || parsed.String() != raw {
			return nil, fmt.Errorf("wire UUID must use lowercase dashed form")
		}
		return raw, nil
	case LocalCodecUUIDBlob:
		raw, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("wire UUID must be a string")
		}
		parsed, err := uuid.Parse(raw)
		if err != nil || len(raw) != 36 || parsed.String() != raw {
			return nil, fmt.Errorf("wire UUID must use lowercase dashed form")
		}
		bytes, _ := parsed.MarshalBinary()
		return bytes, nil
	case LocalCodecJSONText:
		raw, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("wire JSON must be canonical inner JCS text")
		}
		canonical, err := jcs.Canonicalize([]byte(raw))
		if err != nil || string(canonical) != raw || jsonValueContainsNUL(raw) {
			return nil, fmt.Errorf("wire JSON must be canonical inner JCS text without U+0000")
		}
		return raw, nil
	default:
		return nil, fmt.Errorf("unsupported local codec")
	}
}

func jsonValueContainsNUL(raw string) bool {
	var value any
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil {
		return true
	}
	var inspect func(any) bool
	inspect = func(item any) bool {
		switch typed := item.(type) {
		case string:
			return strings.IndexByte(typed, 0) >= 0
		case []any:
			for _, child := range typed {
				if inspect(child) {
					return true
				}
			}
		case map[string]any:
			for key, child := range typed {
				if strings.IndexByte(key, 0) >= 0 || inspect(child) {
					return true
				}
			}
		}
		return false
	}
	return inspect(value)
}
