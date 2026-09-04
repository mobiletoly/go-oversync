package jcs

import (
	"strings"
	"testing"
)

func TestDecodeRejectsDuplicateMembersSurrogatesAndExcessiveDepth(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		`{"a":1,"a":2}`,
		`{"value":"\ud800"}`,
		`{"value":"\udc00"}`,
		`{"value":"\u0000"}`,
		`{"\u0000":"value"}`,
		strings.Repeat("[", 66) + "0" + strings.Repeat("]", 66),
	} {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Errorf("Decode(%q) succeeded, want rejection", raw)
		}
	}
}

func TestDecodeObjectLosslessPreservesLargeNumbersForSchemaConversion(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"amount":` + strings.Repeat("9", 400) + `}`)
	object, err := DecodeObjectLossless(raw)
	if err != nil {
		t.Fatalf("lossless database decode rejected bounded PostgreSQL numeric: %v", err)
	}
	if got := object["amount"].(interface{ String() string }).String(); got != strings.Repeat("9", 400) {
		t.Fatalf("decoded amount = %q", got)
	}
	if _, err := DecodeObject(raw); err == nil {
		t.Fatal("strict JCS decode accepted a number outside the finite binary64 domain")
	}
}

func TestValidateStructureRejectsLossyUnicodeAndDuplicatesWithoutInnerLimits(t *testing.T) {
	t.Parallel()

	for _, raw := range [][]byte{
		[]byte(`{"key":"\ud800"}`),
		[]byte(`{"key":"\udc00"}`),
		[]byte(`{"key":"\u0000"}`),
		[]byte(`{"key":"one","key":"two"}`),
		{0xff},
	} {
		if err := ValidateStructure(raw); err == nil {
			t.Fatalf("ValidateStructure(%q) succeeded, want rejection", raw)
		}
	}

	largeAndDeep := []byte(`{"amount":` + strings.Repeat("9", 400) + `,"nested":` + strings.Repeat("[", 80) + `0` + strings.Repeat("]", 80) + `}`)
	if err := ValidateStructure(largeAndDeep); err != nil {
		t.Fatalf("structural validation applied an inner wire-value limit: %v", err)
	}
}

func TestDecodeAcceptsMaximumDepthAndPairedSurrogates(t *testing.T) {
	t.Parallel()

	maximumDepth := strings.Repeat("[", 65) + `"\ud83d\ude00"` + strings.Repeat("]", 65)
	if _, err := Decode([]byte(maximumDepth)); err != nil {
		t.Fatalf("maximum depth and paired surrogate rejected: %v", err)
	}
}
