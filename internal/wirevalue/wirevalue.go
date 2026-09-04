// Package wirevalue validates numeric strings used by the Oversync wire
// contract.
package wirevalue

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/jcs"
)

var decimalPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?$`)

// ParseInt64 validates the canonical exact-int64 string grammar and range.
func ParseInt64(raw string) (int64, error) {
	if raw == "" || raw == "-0" || !canonicalIntegerSpelling(raw) {
		return 0, fmt.Errorf("must use canonical signed 64-bit integer text")
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("must be within signed 64-bit range")
	}
	return value, nil
}

// ValidateDecimal validates the accepted exact-decimal lexical grammar. It
// does not normalize precision: PostgreSQL's committed text is authoritative.
func ValidateDecimal(raw string) error {
	if raw == "" || !decimalPattern.MatchString(raw) || isNegativeDecimalZero(raw) {
		return fmt.Errorf("must use canonical finite decimal text")
	}
	unsigned := raw
	if unsigned[0] == '-' {
		unsigned = unsigned[1:]
	}
	integerDigits := len(unsigned)
	fractionalDigits := 0
	if dot := strings.IndexByte(unsigned, '.'); dot >= 0 {
		integerDigits = dot
		fractionalDigits = len(unsigned) - dot - 1
	}
	if integerDigits > 1000 || fractionalDigits > 1000 || integerDigits+fractionalDigits > 1000 {
		return fmt.Errorf("decimal coefficient must contain at most 1000 digits")
	}
	return nil
}

// ParseFloat32 validates the canonical promoted-binary32 spelling required by
// the generation wire contract and returns the represented binary32 value.
func ParseFloat32(raw string) (float32, error) {
	if raw == "" {
		return 0, fmt.Errorf("must use canonical finite binary32 text")
	}
	parsed, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
		return 0, fmt.Errorf("must use canonical finite binary32 text")
	}
	value := float32(parsed)
	if math.IsInf(float64(value), 0) {
		return 0, fmt.Errorf("must use canonical finite binary32 text")
	}
	canonical, err := RenderFloat32(value)
	if err != nil || canonical != raw {
		return 0, fmt.Errorf("must use canonical finite binary32 text")
	}
	return value, nil
}

// RenderFloat32 promotes an exact binary32 value to binary64 and applies the
// RFC 8785/ECMAScript shortest-round-trip rendering rule.
func RenderFloat32(value float32) (string, error) {
	return RenderFloat64(float64(value))
}

// ParseFloat64 validates the canonical RFC 8785 spelling of one finite
// binary64 value and returns the parsed value.
func ParseFloat64(raw string) (float64, error) {
	if raw == "" {
		return 0, fmt.Errorf("must use canonical finite binary64 text")
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
		return 0, fmt.Errorf("must use canonical finite binary64 text")
	}
	canonical, err := RenderFloat64(value)
	if err != nil || canonical != raw {
		return 0, fmt.Errorf("must use canonical finite binary64 text")
	}
	return value, nil
}

// RenderFloat64 returns the RFC 8785/ECMAScript shortest round-trip spelling
// for one finite binary64 value. Negative zero is normalized to "0".
func RenderFloat64(value float64) (string, error) {
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return "", fmt.Errorf("must be a finite binary64 value")
	}
	raw := strconv.FormatFloat(value, 'g', -1, 64)
	canonical, err := jcs.Canonicalize([]byte(raw))
	if err != nil {
		return "", fmt.Errorf("render canonical binary64 text: %w", err)
	}
	return string(canonical), nil
}

func isNegativeDecimalZero(raw string) bool {
	if len(raw) == 0 || raw[0] != '-' {
		return false
	}
	for i := 1; i < len(raw); i++ {
		if raw[i] == 'e' || raw[i] == 'E' {
			break
		}
		if raw[i] >= '1' && raw[i] <= '9' {
			return false
		}
	}
	return true
}

func canonicalIntegerSpelling(raw string) bool {
	start := 0
	if raw[0] == '-' {
		start = 1
		if len(raw) == 1 {
			return false
		}
	}
	if raw[start] == '0' {
		return len(raw) == start+1
	}
	if raw[start] < '1' || raw[start] > '9' {
		return false
	}
	for i := start + 1; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return false
		}
	}
	return true
}
