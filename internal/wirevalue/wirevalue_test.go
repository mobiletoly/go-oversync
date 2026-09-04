package wirevalue

import (
	"math"
	"strings"
	"testing"
)

func TestValidateDecimal(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"0", "0.0", "1234567890.123456789", strings.Repeat("9", 1000)} {
		if err := ValidateDecimal(value); err != nil {
			t.Errorf("ValidateDecimal(%q): %v", value, err)
		}
	}
	for _, value := range []string{"", "+1", "01", ".1", "1.", "1e01", "1.25e3", "-0", "-0.0", strings.Repeat("9", 1001), "0." + strings.Repeat("1", 1000)} {
		if err := ValidateDecimal(value); err == nil {
			t.Errorf("ValidateDecimal(%q) succeeded, want rejection", value)
		}
	}
}

func TestParseFloat32(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"0", "1.2345678806304932", "1.401298464324817e-45", "3.4028234663852886e+38"} {
		if _, err := ParseFloat32(value); err != nil {
			t.Errorf("ParseFloat32(%q): %v", value, err)
		}
	}
	for _, value := range []string{"", "-0", "1.2345678901234567", "3.4028236e+38", "NaN", "Infinity"} {
		if _, err := ParseFloat32(value); err == nil {
			t.Errorf("ParseFloat32(%q) succeeded, want rejection", value)
		}
	}
}

func TestParseInt64(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"0", "-9223372036854775808", "9223372036854775807"} {
		if _, err := ParseInt64(value); err != nil {
			t.Errorf("ParseInt64(%q): %v", value, err)
		}
	}
	for _, value := range []string{"", "-0", "+1", "01", "9223372036854775808", "-9223372036854775809"} {
		if _, err := ParseInt64(value); err == nil {
			t.Errorf("ParseInt64(%q) succeeded, want rejection", value)
		}
	}
}

func TestParseFloat64(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"0", "1", "5e-324", "1.7976931348623157e+308", "0.000001", "1e+21"} {
		if _, err := ParseFloat64(value); err != nil {
			t.Errorf("ParseFloat64(%q): %v", value, err)
		}
	}
	for _, value := range []string{"", "-0", "1.0", "1e0", "1E+21", "01", "NaN", "Infinity", "-Infinity"} {
		if _, err := ParseFloat64(value); err == nil {
			t.Errorf("ParseFloat64(%q) succeeded, want rejection", value)
		}
	}
}

func TestRenderFloat64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		value float64
		want  string
	}{
		{value: math.Copysign(0, -1), want: "0"},
		{value: math.SmallestNonzeroFloat64, want: "5e-324"},
		{value: math.MaxFloat64, want: "1.7976931348623157e+308"},
		{value: 0.000001, want: "0.000001"},
		{value: 1e21, want: "1e+21"},
	}
	for _, test := range tests {
		got, err := RenderFloat64(test.value)
		if err != nil {
			t.Errorf("RenderFloat64(%v): %v", test.value, err)
			continue
		}
		if got != test.want {
			t.Errorf("RenderFloat64(%v) = %q, want %q", test.value, got, test.want)
		}
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := RenderFloat64(value); err == nil {
			t.Errorf("RenderFloat64(%v) succeeded, want rejection", value)
		}
	}
}
