package jcs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// The single-pass path must agree with the reference path wherever it accepts
// an input, and the public functions must agree with the reference path on
// every input, including the error text of every refusal.

var singlePassCorpus = []string{
	// objects, arrays, whitespace
	`{}`, `[]`, ` { } `, "\t[\r\n]\n", `{"a":1}`, `{"b":1,"a":2}`, `[1,2,3]`, `[[],{},[{}]]`,
	`{"a":{"c":[1,{"e":null,"d":true}],"b":false}}`, ` {"a" : [ 1 , 2 ] , "b" : "x" } `,
	// scalars at the top level
	`"x"`, `1`, `-0`, `true`, `false`, `null`, ` "s" `, `0.5`,
	// numbers in RFC 8785 formatting
	`[0,-0,1,-1,1.0,1.5,-1.5,1e0,1E2,1e+2,1e-2,1.25e-7,1e21,1e20,123456789012345678901234567890]`,
	`[0.000001,0.0000001,9007199254740993,1.7976931348623157e308,5e-324,4.9e-324,2.5e-324,1e-400,0e0,-0.0]`,
	`[100,1e-6,1e-7,12.5e3,0.1,0.2,0.30000000000000004,333333333.33333329,1e23,-1e-7]`,
	`{"n":1e400}`, `{"n":-1e400}`, `{"n":` + strings.Repeat("9", 400) + `}`,
	// invalid numbers
	`[01]`, `[+1]`, `[.5]`, `[1.]`, `[1e]`, `[1e+]`, `[-]`, `[--1]`, `[1.5.2]`, `[0x10]`, `[Infinity]`, `[NaN]`, `[1_000]`,
	// strings and escapes
	`["plain","with space","quote\"","back\\slash","slash\/","\b\f\n\r\t"]`,
	`["\u0001\u001f\u007f\u0080\u00e9\u2028\u2029\uffff\ufffd"]`, "[\"é\",\"日本\",\"😀\",\"😀 \"]", `["\ud83d\ude00","\uD83D\uDE00"]`,
	`["AB",">","<","&","<>&"]`, "[\"\x7f\"]", `["a\u0000b"]`, `{"\u0000":1}`,
	`["\ud800"]`, `["\udc00"]`, `["\ud800A"]`, `["\ud800\\u0041"]`, `["\ud83d"]`, `["\ude00\ud83d"]`,
	`["\u12"]`, `["\u12G4"]`, `["\x41"]`, `["\'"]`, `["\U0041"]`, "[\"tab\tinside\"]", "[\"nl\ninside\"]", `["unterminated]`,
	// member order: UTF-16 code units, not bytes or code points
	`{"b":1,"a":2,"aa":3,"":4,"A":5}`, `{"\ue000":1,"\ud83d\ude00":2,"\uffff":3,"\u00ff":4,"z":5}`,
	`{"\ud83d\ude00":1,"\ud83d\ude01":2,"\ud83d\ude00b":3,"\ud834\udd1e":4,"\uFB01":5,"\u20ac":6,"\r":7,"1":8}`, "{\"\":1,\"😀\":2,\"￿\":3,\"𝄞\":4}",
	`{"a\u0000":1}`, `{"\u00e9":1,"e\u0301":2}`,
	// duplicates, structure and trailing data
	`{"a":1,"a":2}`, `{"a":1,"\u0061":2}`, `{"😀":1,"\ud83d\ude00":2}`, `{"a":1,}`, `[1,]`, `{,}`, `[,]`, `{"a"}`, `{"a":}`,
	`{a:1}`, `{"a" 1}`, `[1 2]`, `{} {}`, `{}x`, `1 2`, ``, ` `, `{`, `[`, `"`, `}`, `]`, `nul`, `tru`, `falsey`, `[true1]`,
	"\ufeff{}", "{}\x00", "{\"a\":\"\xff\"}", "[\"\xed\xa0\x80\"]",
	// depth: 65 nested containers are allowed by the strict options, 66 are not
	strings.Repeat("[", 65) + `0` + strings.Repeat("]", 65),
	strings.Repeat("[", 66) + `0` + strings.Repeat("]", 66),
	strings.Repeat(`{"a":`, 65) + `0` + strings.Repeat("}", 65),
	strings.Repeat(`{"a":`, 66) + `0` + strings.Repeat("}", 66),
	strings.Repeat("[", 80) + `1e400` + strings.Repeat("]", 80),
	// encoding/json refuses the 10,001st nested container under every option
	strings.Repeat("[", 10000) + strings.Repeat("]", 10000),
	strings.Repeat("[", 10001) + strings.Repeat("]", 10001),
	`{"a":` + strings.Repeat("[", 9999) + strings.Repeat("]", 9999) + `}`,
	`{"a":` + strings.Repeat("[", 10000) + strings.Repeat("]", 10000) + `}`,
	strings.Repeat(`{"a":`, 10000) + `0` + strings.Repeat("}", 10000),
	strings.Repeat(`{"a":`, 10001) + `0` + strings.Repeat("}", 10001),
	// member names that first differ inside a multi-byte rune, and objects nested in arrays
	`{"\u00ea":1,"\u00e9":2,"\u0800":3,"\u07ff":4,"\uffff":5,"\ud800\udc00":6,"\ue000":7}`,
	"{\"\u00ea\":1,\"\u00e9\":2,\"\u0800\":3,\"\u07ff\":4,\"\U00010000\":5,\"\U0010FFFF\":6,\"\uFFFF\":7}",
	`[{"b":[{"d":1,"c":[{"f":2,"e":{"h":3,"g":4}}]}],"a":{"j":[{"l":5,"k":6}]}},{"z":1,"y":{}},[[{"n":7,"m":8}]]]`,
	// a realistic wire payload
	`{"id":"8c0a2f6e-0d0b-4e8f-9a0b-1c2d3e4f5a6b","name":"Row 1","note":null,"count_value":"42","small_count":"-7",` +
		`"exact_amount":"12.3400","enabled_flag":true,"rating":"0.1","float4_value":"3.5","data":"AAEC","created_at":"2026-11-14"}`,
}

func TestSinglePassMatchesReferenceOnCorpus(t *testing.T) {
	t.Parallel()
	for _, raw := range singlePassCorpus {
		assertSinglePassMatchesReference(t, []byte(raw))
	}
}

func TestSinglePassAcceptsOrdinaryInput(t *testing.T) {
	t.Parallel()
	// The single-pass path must handle ordinary wire JSON itself; declining it
	// would be correct but slow.
	for _, raw := range []string{
		`{"b":"x","a":[1,2.5,-3e-7,true,null],"c":{"d":"\u00e9\n\"q\""},"😀":"\ud83d\ude00"}`,
		singlePassCorpus[len(singlePassCorpus)-1],
		`"scalar"`, `[]`, `{}`,
	} {
		if _, ok := canonicalizeSinglePass([]byte(raw)); !ok {
			t.Errorf("canonicalizeSinglePass declined %s", raw)
		}
		if _, ok := decodeSinglePass([]byte(raw), strictOptions); !ok {
			t.Errorf("decodeSinglePass declined %s", raw)
		}
		if !validateSinglePass([]byte(raw)) {
			t.Errorf("validateSinglePass declined %s", raw)
		}
		tree, err := decodeReference([]byte(raw), strictOptions)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := marshalSinglePass(tree); !ok {
			t.Errorf("marshalSinglePass declined the tree of %s", raw)
		}
	}
}

func TestMarshalSinglePassMatchesReferenceOnTrees(t *testing.T) {
	t.Parallel()
	deep := func(levels int, leaf any) any {
		value := leaf
		for range levels {
			value = []any{value}
		}
		return value
	}
	deepObject := func(levels int) any {
		var value any = "leaf"
		for range levels {
			value = map[string]any{"k": value}
		}
		return value
	}
	trees := []any{
		nil, true, false, "", "plain", "a\u0000b", "bad\xffutf8", "\u2028<>&\x7f\x01\x1f", "\U0001F600",
		json.Number("1"), json.Number("-0"), json.Number("1.0"), json.Number("1E2"), json.Number("1e400"),
		json.Number("01"), json.Number(""), json.Number(" 1"), json.Number("0x10"), json.Number("NaN"),
		json.Number(strings.Repeat("9", 400)), json.Number("5e-324"), json.Number("1e-400"),
		map[string]any(nil), []any(nil), map[string]any{}, []any{},
		map[string]any{"b": 1.5, "a": "x"}, // float64 is not a decoded-tree type: falls back
		map[string]any{"b": []any{nil, map[string]any(nil), []any(nil)}, "a": json.Number("2")},
		map[string]any{"\ue000": 1, "\U0001F600": "x"}, map[string]any{"\ue000": "1", "\U0001F600": "x", "\uffff": "y", "": "z"},
		map[string]any{"k\u0000": "v"}, map[string]any{"k\xff": "v"}, map[string]any{"\xfe": "a", "\xff": "b"},
		[]any{"a", json.Number("1e21"), json.Number("1e-7"), true, nil, map[string]any{"z": "1", "y": []any{}}},
		[]string{"x", "y"}, map[string]string{"b": "1", "a": "2"}, struct{ A string }{"x"}, 7, int64(-1), []byte("bytes"),
		json.RawMessage(`{"b":1,"a":2}`), []any{json.RawMessage(`[1]`)},
		deep(64, "x"), deep(65, "x"), deepObject(64), deepObject(65), deep(64, []any(nil)), deep(65, map[string]any(nil)),
	}
	for i, tree := range trees {
		assertMarshalMatchesReference(t, fmt.Sprintf("tree %d (%T)", i, tree), tree, false)
	}
}

func TestSinglePassMatchesReferenceOnGeneratedJSON(t *testing.T) {
	t.Parallel()
	random := rand.New(rand.NewSource(20261114))
	for range 20000 {
		raw := generateJSON(random, 0)
		assertSinglePassMatchesReference(t, []byte(raw))
		if t.Failed() {
			return
		}
	}
}

func FuzzSinglePassMatchesReference(f *testing.F) {
	for _, raw := range singlePassCorpus {
		f.Add([]byte(raw))
	}
	random := rand.New(rand.NewSource(1))
	for range 50 {
		f.Add([]byte(generateJSON(random, 0)))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		assertSinglePassMatchesReference(t, raw)
	})
}

func assertSinglePassMatchesReference(t *testing.T, raw []byte) {
	t.Helper()

	// Canonicalize, the public function and the single-pass path alone.
	referenceCanonical, referenceErr := canonicalizeReference(raw)
	canonical, err := Canonicalize(raw)
	assertSameOutcome(t, "Canonicalize", raw, canonical, err, referenceCanonical, referenceErr)
	if single, ok := canonicalizeSinglePass(raw); ok {
		if referenceErr != nil || !bytes.Equal(single, referenceCanonical) {
			t.Errorf("canonicalizeSinglePass(%.200q) = %.300q, reference = %.300q, %.300v", raw, single, referenceCanonical, referenceErr)
		}
	} else if referenceErr == nil {
		t.Errorf("canonicalizeSinglePass(%.200q) declined an input the reference path accepts", raw)
	}

	// Decode, DecodeObject and DecodeObjectLossless.
	for _, options := range []validationOptions{strictOptions, losslessOptions} {
		referenceTree, referenceErr := decodeReference(raw, options)
		if single, ok := decodeSinglePass(raw, options); ok {
			if referenceErr != nil || !reflect.DeepEqual(single, referenceTree) {
				t.Errorf("decodeSinglePass(%.200q, %+v) = %.300v, reference = %.300v, %.300v", raw, options, single, referenceTree, referenceErr)
			}
		} else if referenceErr == nil {
			t.Errorf("decodeSinglePass(%.200q, %+v) declined an input the reference path accepts", raw, options)
		}
		if referenceErr == nil {
			assertMarshalMatchesReference(t, fmt.Sprintf("tree of %.200q", raw), referenceTree, true)
		}
	}
	strictTree, strictErr := decodeReference(raw, strictOptions)
	tree, err := Decode(raw)
	assertSameTree(t, "Decode", raw, tree, err, strictTree, strictErr)
	referenceObject, referenceObjectErr := decodeObjectReference(raw)
	object, objectErr := DecodeObject(raw)
	assertSameTree(t, "DecodeObject", raw, object, objectErr, referenceObject, referenceObjectErr)
	referenceLossless, referenceLosslessErr := decodeObjectLosslessReference(raw)
	lossless, losslessErr := DecodeObjectLossless(raw)
	assertSameTree(t, "DecodeObjectLossless", raw, lossless, losslessErr, referenceLossless, referenceLosslessErr)

	// ValidateStructure.
	referenceValidateErr := validateStructureReference(raw)
	if accepted := validateSinglePass(raw); accepted != (referenceValidateErr == nil) {
		t.Errorf("validateSinglePass(%.200q) = %v, reference error %v", raw, accepted, referenceValidateErr)
	}
	assertSameError(t, "ValidateStructure", raw, ValidateStructure(raw), referenceValidateErr)

	// Equal, against itself and against its reference canonical form.
	if referenceErr == nil {
		equal, err := Equal(raw, referenceCanonical)
		if err != nil || !equal {
			t.Errorf("Equal(%.200q, its canonical form) = %v, %v", raw, equal, err)
		}
	}
}

func decodeObjectReference(raw []byte) (map[string]any, error) {
	value, err := decodeReference(raw, strictOptions)
	if err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("JCS value must be an object")
	}
	return object, nil
}

// assertMarshalMatchesReference compares Marshal with the reference path.
// decodedTree marks a tree decoded from JSON, which the single-pass encoder
// must take itself whenever the reference path succeeds.
func assertMarshalMatchesReference(t *testing.T, label string, tree any, decodedTree bool) {
	t.Helper()
	reference, referenceErr := marshalReference(tree)
	if single, ok := marshalSinglePass(tree); ok {
		if referenceErr != nil || !bytes.Equal(single, reference) {
			t.Errorf("marshalSinglePass(%.200s) = %.300q, reference = %.300q, %.300v", label, single, reference, referenceErr)
		}
	} else if decodedTree && referenceErr == nil {
		t.Errorf("marshalSinglePass(%.200s) declined a decoded tree the reference path marshals", label)
	}
	got, err := Marshal(tree)
	assertSameOutcome(t, "Marshal", []byte(label), got, err, reference, referenceErr)
}

func assertSameOutcome(t *testing.T, function string, raw, got []byte, err error, want []byte, wantErr error) {
	t.Helper()
	assertSameError(t, function, raw, err, wantErr)
	if err == nil && wantErr == nil && !bytes.Equal(got, want) {
		t.Errorf("%s(%.200q) = %.300q, reference %.300q", function, raw, got, want)
	}
}

func assertSameTree[T any](t *testing.T, function string, raw []byte, got T, err error, want T, wantErr error) {
	t.Helper()
	assertSameError(t, function, raw, err, wantErr)
	if err == nil && wantErr == nil && !reflect.DeepEqual(got, want) {
		t.Errorf("%s(%.200q) = %.300v, reference %.300v", function, raw, got, want)
	}
}

func assertSameError(t *testing.T, function string, raw []byte, err, wantErr error) {
	t.Helper()
	if (err == nil) != (wantErr == nil) || (err != nil && err.Error() != wantErr.Error()) {
		t.Errorf("%s(%.200q) error = %.300v, reference error = %.300v", function, raw, err, wantErr)
	}
}

// generateJSON writes a random JSON value with random whitespace, escapes,
// member names across the UTF-16 ordering boundaries and numbers in many
// forms. A few values are made invalid on purpose.
func generateJSON(random *rand.Rand, depth int) string {
	space := func() string { return []string{"", "", "", " ", "\n", "\t", "\r\n  "}[random.Intn(7)] }
	switch kind := random.Intn(10); {
	case depth < 6 && kind < 2:
		count := random.Intn(5)
		parts := make([]string, count)
		for i := range parts {
			parts[i] = space() + generateString(random) + space() + ":" + space() + generateJSON(random, depth+1) + space()
		}
		if count > 1 && random.Intn(40) == 0 {
			parts[1] = parts[0] // duplicate member
		}
		return "{" + strings.Join(parts, ",") + space() + "}"
	case depth < 6 && kind < 4:
		count := random.Intn(5)
		parts := make([]string, count)
		for i := range parts {
			parts[i] = space() + generateJSON(random, depth+1) + space()
		}
		return "[" + strings.Join(parts, ",") + "]"
	case kind < 6:
		return generateString(random)
	case kind < 8:
		return generateNumber(random)
	default:
		return []string{"true", "false", "null", "tru", "nulls"}[random.Intn(5)]
	}
}

func generateString(random *rand.Rand) string {
	runes := []rune{'a', 'b', 'z', 'A', '0', ' ', '"', '\\', '/', '\b', '\f', '\n', '\r', '\t', 0x01, 0x1f, 0x7f, 0x80,
		0xe9, 0x2028, 0xd7ff, 0xe000, 0xfb01, 0xfffd, 0xffff, 0x10000, 0x1d11e, 0x1f600, 0x1f601, 0x10ffff}
	var b strings.Builder
	b.WriteByte('"')
	for range random.Intn(6) {
		r := runes[random.Intn(len(runes))]
		switch choice := random.Intn(6); {
		case r == '"' || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r < 0x20 && choice < 3 && strings.ContainsRune("\b\f\n\r\t", r):
			b.WriteString(map[rune]string{'\b': `\b`, '\f': `\f`, '\n': `\n`, '\r': `\r`, '\t': `\t`}[r])
		case r < 0x20 && choice < 5:
			fmt.Fprintf(&b, `\u%04X`, r)
		case r < 0x20:
			b.WriteRune(r) // a raw control character: invalid
		case choice == 0 && r >= 0x10000:
			r1, r2 := utf16Pair(r)
			fmt.Fprintf(&b, `\u%04x\u%04X`, r1, r2)
		case choice == 1 && r < 0x10000:
			fmt.Fprintf(&b, `\u%04x`, r)
		case choice == 2 && r == '/':
			b.WriteString(`\/`)
		default:
			b.WriteRune(r)
		}
	}
	if random.Intn(60) == 0 {
		b.WriteString([]string{`\ud800`, `\udc00`, `\u0000`, `\x`, "\x00"}[random.Intn(5)])
	}
	b.WriteByte('"')
	return b.String()
}

func utf16Pair(r rune) (rune, rune) {
	r -= 0x10000
	return 0xd800 + (r >> 10), 0xdc00 + (r & 0x3ff)
}

func generateNumber(random *rand.Rand) string {
	switch random.Intn(8) {
	case 0:
		return strconv.Itoa(random.Intn(2000) - 1000)
	case 1:
		return strconv.FormatInt(random.Int63()-random.Int63(), 10)
	case 2:
		return strconv.FormatFloat(random.NormFloat64()*math.Pow(10, float64(random.Intn(60)-30)), 'g', -1, 64)
	case 3:
		return strconv.FormatFloat(random.Float64(), 'f', random.Intn(20), 64)
	case 4:
		return fmt.Sprintf("%d.%de%s%d", random.Intn(100), random.Intn(1000), []string{"", "+", "-"}[random.Intn(3)], random.Intn(400))
	case 5:
		return []string{"-0", "0", "0.0", "-0.0", "1E400", "1e-400", "5e-324", "9007199254740993", "1e21", "1e-7"}[random.Intn(10)]
	case 6:
		return strings.Repeat("9", 1+random.Intn(40))
	default:
		return []string{"01", "+1", ".5", "1.", "1e", "-", "1e+", "0x1"}[random.Intn(8)]
	}
}
