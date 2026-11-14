package jcs

import (
	"cmp"
	"encoding/json"
	"math"
	"slices"
	"strconv"
	"unicode/utf8"

	jsoncanonicalizer "github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
)

// This file holds the single-pass path behind the public functions in jcs.go.
// It parses, validates and encodes in one walk over the input, where the
// reference path validates tokens, decodes with encoding/json and then parses
// a third time in the RFC 8785 reference implementation.
//
// The single-pass path never reports an error. It returns ok=false for any
// input it does not accept, and the caller then runs the reference path, which
// produces the result or the error. On inputs it accepts it must produce
// exactly the reference bytes or tree, and it must never accept an input the
// reference path refuses. Both properties are checked against the reference
// path in canonical_test.go.

// canonicalizeSinglePass is Canonicalize for inputs the single-pass path
// accepts.
func canonicalizeSinglePass(raw []byte) ([]byte, bool) {
	if !utf8.Valid(raw) {
		return nil, false
	}
	p := singlePassParser{data: raw, options: strictOptions}
	p.skipSpace()
	out, ok := p.canonicalValue(make([]byte, 0, len(raw)), 0)
	if !ok || !p.atEnd() {
		return nil, false
	}
	return out, true
}

// validateSinglePass is ValidateStructure for inputs the single-pass path
// accepts. It applies the lossless options: numbers are checked against the
// JSON grammar only and nesting is limited only by encoding/json.
func validateSinglePass(raw []byte) bool {
	if !utf8.Valid(raw) {
		return false
	}
	p := singlePassParser{data: raw, options: losslessOptions, validateOnly: true}
	p.skipSpace()
	_, ok := p.canonicalValue(nil, 0)
	return ok && p.atEnd()
}

// decodeSinglePass is Decode with the given options for inputs the
// single-pass path accepts. It builds the tree encoding/json builds with
// UseNumber: map[string]any, []any, string, json.Number, bool and nil.
func decodeSinglePass(raw []byte, options validationOptions) (any, bool) {
	if !utf8.Valid(raw) {
		return nil, false
	}
	p := singlePassParser{data: raw, options: options}
	p.skipSpace()
	value, ok := p.treeValue(0)
	if !ok || !p.atEnd() {
		return nil, false
	}
	return value, true
}

type singlePassParser struct {
	data    []byte
	pos     int
	options validationOptions
	// validateOnly (lossless options only) copies number tokens instead of
	// formatting them; the output is discarded.
	validateOnly bool
	// Scratch buffers and member lists for the objects being encoded, one per
	// nesting depth. At most one object per depth is open at a time.
	scratch [][]byte
	members [][]objectMember
}

type objectMember struct {
	key        []byte
	valueStart int
	valueEnd   int
}

func (p *singlePassParser) skipSpace() {
	for p.pos < len(p.data) {
		switch p.data[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func (p *singlePassParser) atEnd() bool {
	p.skipSpace()
	return p.pos == len(p.data)
}

// next returns the byte at the current position, or 0 at the end of input.
// A literal NUL byte is never valid outside a string, so 0 never matches an
// expected delimiter.
func (p *singlePassParser) next() byte {
	if p.pos < len(p.data) {
		return p.data[p.pos]
	}
	return 0
}

// encodingJSONMaxNesting is encoding/json's own nesting limit: its scanner
// refuses the 10,001st nested container, so the reference path refuses that
// input under every option.
const encodingJSONMaxNesting = 10000

func (p *singlePassParser) depthAllowed(depth int) bool {
	return depth < encodingJSONMaxNesting && (p.options.maxDepth < 0 || depth <= p.options.maxDepth)
}

// canonicalValue appends the RFC 8785 encoding of the value at the current
// position. depth is the nesting depth a container at this position has; the
// outermost container is at depth 0, as in validateTokenValue.
func (p *singlePassParser) canonicalValue(dst []byte, depth int) ([]byte, bool) {
	switch c := p.next(); c {
	case '{':
		return p.canonicalObject(dst, depth)
	case '[':
		return p.canonicalArray(dst, depth)
	case '"':
		text, ok := p.stringToken()
		if !ok {
			return dst, false
		}
		return appendCanonicalString(dst, text), true
	case 't':
		return p.literal(dst, "true")
	case 'f':
		return p.literal(dst, "false")
	case 'n':
		return p.literal(dst, "null")
	default:
		token, ok := p.numberToken()
		if !ok {
			return dst, false
		}
		if p.validateOnly {
			return append(dst, token...), true
		}
		value, ok := finiteBinary64(string(token))
		if !ok {
			return dst, false
		}
		return appendCanonicalNumber(dst, value)
	}
}

func (p *singlePassParser) literal(dst []byte, word string) ([]byte, bool) {
	if !p.matchLiteral(word) {
		return dst, false
	}
	return append(dst, word...), true
}

func (p *singlePassParser) matchLiteral(word string) bool {
	end := p.pos + len(word)
	if end > len(p.data) || string(p.data[p.pos:end]) != word {
		return false
	}
	p.pos = end
	return true
}

func (p *singlePassParser) canonicalArray(dst []byte, depth int) ([]byte, bool) {
	if !p.depthAllowed(depth) {
		return dst, false
	}
	p.pos++
	dst = append(dst, '[')
	p.skipSpace()
	if p.next() == ']' {
		p.pos++
		return append(dst, ']'), true
	}
	for {
		p.skipSpace()
		var ok bool
		if dst, ok = p.canonicalValue(dst, depth+1); !ok {
			return dst, false
		}
		p.skipSpace()
		switch p.next() {
		case ',':
			p.pos++
			dst = append(dst, ',')
		case ']':
			p.pos++
			return append(dst, ']'), true
		default:
			return dst, false
		}
	}
}

// canonicalObject encodes each member value into the scratch buffer for this
// depth, then writes the members in RFC 8785 order (UTF-16 code units of the
// names). Duplicate names are declined.
func (p *singlePassParser) canonicalObject(dst []byte, depth int) ([]byte, bool) {
	if !p.depthAllowed(depth) {
		return dst, false
	}
	p.pos++
	for len(p.scratch) <= depth {
		p.scratch = append(p.scratch, nil)
		p.members = append(p.members, nil)
	}
	buf := p.scratch[depth][:0]
	members := p.members[depth][:0]
	p.skipSpace()
	if p.next() == '}' {
		p.pos++
		return append(dst, '{', '}'), true
	}
	for {
		p.skipSpace()
		if p.next() != '"' {
			return dst, false
		}
		key, ok := p.stringToken()
		if !ok {
			return dst, false
		}
		p.skipSpace()
		if p.next() != ':' {
			return dst, false
		}
		p.pos++
		p.skipSpace()
		start := len(buf)
		if buf, ok = p.canonicalValue(buf, depth+1); !ok {
			return dst, false
		}
		members = append(members, objectMember{key: key, valueStart: start, valueEnd: len(buf)})
		p.skipSpace()
		switch p.next() {
		case ',':
			p.pos++
			continue
		case '}':
			p.pos++
		default:
			return dst, false
		}
		break
	}
	slices.SortFunc(members, func(a, b objectMember) int { return compareUTF16(a.key, b.key) })
	for i := 1; i < len(members); i++ {
		if compareUTF16(members[i-1].key, members[i].key) == 0 {
			return dst, false
		}
	}
	dst = append(dst, '{')
	for i, member := range members {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendCanonicalString(dst, member.key)
		dst = append(dst, ':')
		dst = append(dst, buf[member.valueStart:member.valueEnd]...)
	}
	p.scratch[depth] = buf[:0]
	p.members[depth] = members[:0]
	return append(dst, '}'), true
}

// treeValue decodes the value at the current position into the tree
// encoding/json builds with UseNumber.
func (p *singlePassParser) treeValue(depth int) (any, bool) {
	switch c := p.next(); c {
	case '{':
		return p.treeObject(depth)
	case '[':
		return p.treeArray(depth)
	case '"':
		text, ok := p.stringToken()
		if !ok {
			return nil, false
		}
		return string(text), true
	case 't':
		return true, p.matchLiteral("true")
	case 'f':
		return false, p.matchLiteral("false")
	case 'n':
		return nil, p.matchLiteral("null")
	default:
		token, ok := p.numberToken()
		if !ok {
			return nil, false
		}
		text := string(token)
		if p.options.requireFiniteBinary64 {
			if _, ok := finiteBinary64(text); !ok {
				return nil, false
			}
		}
		return json.Number(text), true
	}
}

func (p *singlePassParser) treeArray(depth int) (any, bool) {
	if !p.depthAllowed(depth) {
		return nil, false
	}
	p.pos++
	values := make([]any, 0)
	p.skipSpace()
	if p.next() == ']' {
		p.pos++
		return values, true
	}
	for {
		p.skipSpace()
		value, ok := p.treeValue(depth + 1)
		if !ok {
			return nil, false
		}
		values = append(values, value)
		p.skipSpace()
		switch p.next() {
		case ',':
			p.pos++
		case ']':
			p.pos++
			return values, true
		default:
			return nil, false
		}
	}
}

func (p *singlePassParser) treeObject(depth int) (any, bool) {
	if !p.depthAllowed(depth) {
		return nil, false
	}
	p.pos++
	object := make(map[string]any)
	p.skipSpace()
	if p.next() == '}' {
		p.pos++
		return object, true
	}
	for {
		p.skipSpace()
		if p.next() != '"' {
			return nil, false
		}
		keyBytes, ok := p.stringToken()
		if !ok {
			return nil, false
		}
		key := string(keyBytes)
		if _, exists := object[key]; exists {
			return nil, false
		}
		p.skipSpace()
		if p.next() != ':' {
			return nil, false
		}
		p.pos++
		p.skipSpace()
		value, ok := p.treeValue(depth + 1)
		if !ok {
			return nil, false
		}
		object[key] = value
		p.skipSpace()
		switch p.next() {
		case ',':
			p.pos++
		case '}':
			p.pos++
			return object, true
		default:
			return nil, false
		}
	}
}

// stringToken decodes the JSON string at the current position (which is a
// quote) and returns its UTF-8 bytes. A string without escapes is returned as
// a sub-slice of the input. It declines control characters, unknown escapes,
// U+0000 and unpaired surrogates, as the reference path refuses them.
func (p *singlePassParser) stringToken() ([]byte, bool) {
	p.pos++
	start := p.pos
	for p.pos < len(p.data) {
		c := p.data[p.pos]
		if c == '"' {
			text := p.data[start:p.pos]
			p.pos++
			return text, true
		}
		if c == '\\' {
			break
		}
		if c < 0x20 {
			return nil, false
		}
		p.pos++
	}
	if p.pos >= len(p.data) {
		return nil, false
	}
	text := make([]byte, 0, p.pos-start+16)
	text = append(text, p.data[start:p.pos]...)
	for p.pos < len(p.data) {
		c := p.data[p.pos]
		switch {
		case c == '"':
			p.pos++
			return text, true
		case c < 0x20:
			return nil, false
		case c != '\\':
			text = append(text, c)
			p.pos++
			continue
		}
		if p.pos+1 >= len(p.data) {
			return nil, false
		}
		escape := p.data[p.pos+1]
		p.pos += 2
		switch escape {
		case '"', '\\', '/':
			text = append(text, escape)
		case 'b':
			text = append(text, '\b')
		case 'f':
			text = append(text, '\f')
		case 'n':
			text = append(text, '\n')
		case 'r':
			text = append(text, '\r')
		case 't':
			text = append(text, '\t')
		case 'u':
			r, ok := p.unicodeEscape()
			if !ok {
				return nil, false
			}
			text = utf8.AppendRune(text, r)
		default:
			return nil, false
		}
	}
	return nil, false
}

// unicodeEscape reads the four hex digits after `\u`, and the low half of a
// surrogate pair when the first escape is a high surrogate.
func (p *singlePassParser) unicodeEscape() (rune, bool) {
	first, ok := p.hex4()
	if !ok || first == 0 || (first >= 0xdc00 && first <= 0xdfff) {
		return 0, false
	}
	if first < 0xd800 || first > 0xdbff {
		return first, true
	}
	if p.pos+2 > len(p.data) || p.data[p.pos] != '\\' || p.data[p.pos+1] != 'u' {
		return 0, false
	}
	p.pos += 2
	second, ok := p.hex4()
	if !ok || second < 0xdc00 || second > 0xdfff {
		return 0, false
	}
	return 0x10000 + (first-0xd800)<<10 + (second - 0xdc00), true
}

func (p *singlePassParser) hex4() (rune, bool) {
	value, ok := parseHex4(p.data, p.pos)
	if !ok {
		return 0, false
	}
	p.pos += 4
	return rune(value), true
}

// numberToken returns the JSON number at the current position, checked
// against the RFC 8259 grammar.
func (p *singlePassParser) numberToken() ([]byte, bool) {
	start := p.pos
	end, ok := scanJSONNumber(p.data, p.pos)
	if !ok {
		return nil, false
	}
	p.pos = end
	return p.data[start:end], true
}

// scanJSONNumber returns the end of the RFC 8259 number starting at start.
func scanJSONNumber[T string | []byte](s T, start int) (int, bool) {
	i := start
	if i < len(s) && s[i] == '-' {
		i++
	}
	switch {
	case i >= len(s):
		return 0, false
	case s[i] == '0':
		i++
	case s[i] >= '1' && s[i] <= '9':
		i = skipDigits(s, i+1)
	default:
		return 0, false
	}
	if i < len(s) && s[i] == '.' {
		end := skipDigits(s, i+1)
		if end == i+1 {
			return 0, false
		}
		i = end
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		end := skipDigits(s, i)
		if end == i {
			return 0, false
		}
		i = end
	}
	return i, true
}

func skipDigits[T string | []byte](s T, i int) int {
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	return i
}

// finiteBinary64 parses a JSON number token as the reference path does when
// it checks the finite binary64 domain and when RFC 8785 formats it.
func finiteBinary64(token string) (float64, bool) {
	value, err := strconv.ParseFloat(token, 64)
	if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
		return 0, false
	}
	return value, true
}

func appendCanonicalNumber(dst []byte, value float64) ([]byte, bool) {
	formatted, err := jsoncanonicalizer.NumberToJSON(value)
	if err != nil {
		return dst, false
	}
	return append(dst, formatted...), true
}

const lowerHex = "0123456789abcdef"

// appendCanonicalString writes a string as the RFC 8785 reference
// implementation does: the two-character escapes for `"`, `\`, backspace,
// form feed, newline, carriage return and tab, `\u00xx` with lowercase hex
// for the other control characters, and every other byte as it is.
func appendCanonicalString[T string | []byte](dst []byte, text T) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(text); i++ {
		c := text[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		dst = append(dst, text[start:i]...)
		switch c {
		case '"', '\\':
			dst = append(dst, '\\', c)
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			dst = append(dst, '\\', 'u', '0', '0', lowerHex[c>>4], lowerHex[c&0xf])
		}
		start = i + 1
	}
	dst = append(dst, text[start:]...)
	return append(dst, '"')
}

// compareUTF16 orders two valid UTF-8 strings by their UTF-16 code units, the
// RFC 8785 member order.
func compareUTF16[T string | []byte](a, b T) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	if i == n {
		return cmp.Compare(len(a), len(b))
	}
	// Both strings are valid UTF-8 and equal before i, so the rune that
	// contains byte i starts at the same offset in both.
	for i > 0 && !utf8.RuneStart(a[i]) {
		i--
	}
	ra, rb := runeAt(a, i), runeAt(b, i)
	if ua, ub := utf16Lead(ra), utf16Lead(rb); ua != ub {
		return cmp.Compare(ua, ub)
	}
	// Both are supplementary runes with the same high surrogate; their low
	// surrogates order as the runes do.
	return cmp.Compare(ra, rb)
}

// utf16Lead returns the first UTF-16 code unit of r.
func utf16Lead(r rune) rune {
	if r < 0x10000 {
		return r
	}
	return 0xd800 + (r-0x10000)>>10
}

// runeAt decodes the rune starting at i of valid UTF-8.
func runeAt[T string | []byte](s T, i int) rune {
	c := s[i]
	switch {
	case c < 0x80:
		return rune(c)
	case c < 0xe0:
		return rune(c&0x1f)<<6 | rune(s[i+1]&0x3f)
	case c < 0xf0:
		return rune(c&0x0f)<<12 | rune(s[i+1]&0x3f)<<6 | rune(s[i+2]&0x3f)
	default:
		return rune(c&0x07)<<18 | rune(s[i+1]&0x3f)<<12 | rune(s[i+2]&0x3f)<<6 | rune(s[i+3]&0x3f)
	}
}

// marshalSinglePass is Marshal for decoded JSON trees: map[string]any, []any,
// string, json.Number, bool and nil, nested to the reference depth limit. Any
// other type, a string that is not valid UTF-8 or contains U+0000, and a
// number outside the finite binary64 domain are declined, and the caller
// falls back to encoding/json and Canonicalize.
func marshalSinglePass(value any) ([]byte, bool) {
	return appendCanonicalTree(make([]byte, 0, 128), value, 0)
}

func appendCanonicalTree(dst []byte, value any, depth int) ([]byte, bool) {
	switch typed := value.(type) {
	case nil:
		return append(dst, "null"...), true
	case bool:
		if typed {
			return append(dst, "true"...), true
		}
		return append(dst, "false"...), true
	case string:
		if !marshalableString(typed) {
			return dst, false
		}
		return appendCanonicalString(dst, typed), true
	case json.Number:
		text := string(typed)
		if end, ok := scanJSONNumber(text, 0); !ok || end != len(text) {
			return dst, false
		}
		number, ok := finiteBinary64(text)
		if !ok {
			return dst, false
		}
		return appendCanonicalNumber(dst, number)
	case map[string]any:
		return appendCanonicalTreeObject(dst, typed, depth)
	case []any:
		if typed == nil {
			return append(dst, "null"...), true
		}
		if depth > strictOptions.maxDepth {
			return dst, false
		}
		dst = append(dst, '[')
		for i, element := range typed {
			if i > 0 {
				dst = append(dst, ',')
			}
			var ok bool
			if dst, ok = appendCanonicalTree(dst, element, depth+1); !ok {
				return dst, false
			}
		}
		return append(dst, ']'), true
	default:
		return dst, false
	}
}

func appendCanonicalTreeObject(dst []byte, object map[string]any, depth int) ([]byte, bool) {
	if object == nil {
		return append(dst, "null"...), true
	}
	if depth > strictOptions.maxDepth {
		return dst, false
	}
	keys := make([]string, 0, len(object))
	for key := range object {
		if !marshalableString(key) {
			return dst, false
		}
		keys = append(keys, key)
	}
	slices.SortFunc(keys, compareUTF16[string])
	dst = append(dst, '{')
	for i, key := range keys {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendCanonicalString(dst, key)
		dst = append(dst, ':')
		var ok bool
		if dst, ok = appendCanonicalTree(dst, object[key], depth+1); !ok {
			return dst, false
		}
	}
	return append(dst, '}'), true
}

// marshalableString reports whether encoding/json writes text so that the
// reference path reads back exactly text: valid UTF-8 (encoding/json replaces
// invalid bytes) without U+0000 (written as \u0000, which the reference path
// refuses).
func marshalableString(text string) bool {
	for i := 0; i < len(text); i++ {
		if text[i] == 0 {
			return false
		}
	}
	return utf8.ValidString(text)
}
