// Package protocolhash defines the language-neutral logical inputs for C1
// request and committed-bundle hashes.
package protocolhash

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	stdhash "hash"
	"strconv"
	"unicode/utf8"

	"github.com/mobiletoly/go-oversync/internal/jcs"
)

type PushRow struct {
	Schema         string
	Table          string
	Key            map[string]any
	Op             string
	BaseRowVersion int64
	Payload        []byte
}

type BundleRow struct {
	Schema     string
	Table      string
	Key        map[string]any
	Op         string
	RowVersion int64
	Payload    []byte
}

func PushRequest(rows []PushRow) (string, int64, error) {
	return hashPushRows(rows)
}

// GenerationPinnedPushRequest hashes the exact Q39 generation-bound request envelope.
func GenerationPinnedPushRequest(generationID, generationFingerprint string, rows []PushRow) (string, int64, error) {
	hasher := NewGenerationPinnedPushRequestHasher(generationID, generationFingerprint)
	for _, row := range rows {
		if err := hasher.Add(row); err != nil {
			return "", 0, err
		}
	}
	return hasher.Sum()
}

// GenerationPinnedPushRequestHasher incrementally hashes the exact Q39 object.
type GenerationPinnedPushRequestHasher struct {
	digest    stdhash.Hash
	rowCount  int64
	byteCount int64
	finished  bool
	hash      string
}

func NewGenerationPinnedPushRequestHasher(generationID, generationFingerprint string) *GenerationPinnedPushRequestHasher {
	prefix := []byte(`{"generation_fingerprint":`)
	prefix = appendCanonicalJSONString(prefix, generationFingerprint)
	prefix = append(prefix, `,"generation_id":`...)
	prefix = appendCanonicalJSONString(prefix, generationID)
	prefix = append(prefix, `,"rows":[`...)
	h := &GenerationPinnedPushRequestHasher{digest: sha256.New(), byteCount: int64(len(prefix))}
	_, _ = h.digest.Write(prefix)
	return h
}

func (h *GenerationPinnedPushRequestHasher) Add(row PushRow) error {
	if h == nil {
		return fmt.Errorf("generation-pinned push request hasher is nil")
	}
	if h.finished {
		return fmt.Errorf("generation-pinned push request hash is already finalized")
	}
	key, err := jcs.Marshal(row.Key)
	if err != nil {
		return err
	}
	payload, err := payloadValue(row.Op, row.Payload)
	if err != nil {
		return err
	}
	payloadBytes, err := jcs.Marshal(payload)
	if err != nil {
		return err
	}
	canonical := []byte(`{"base_row_version":`)
	canonical = appendCanonicalJSONString(canonical, strconv.FormatInt(row.BaseRowVersion, 10))
	canonical = append(canonical, `,"key":`...)
	canonical = append(canonical, key...)
	canonical = append(canonical, `,"op":`...)
	canonical = appendCanonicalJSONString(canonical, row.Op)
	canonical = append(canonical, `,"payload":`...)
	canonical = append(canonical, payloadBytes...)
	canonical = append(canonical, `,"row_ordinal":`...)
	canonical = appendCanonicalJSONString(canonical, strconv.FormatInt(h.rowCount, 10))
	canonical = append(canonical, `,"schema":`...)
	canonical = appendCanonicalJSONString(canonical, row.Schema)
	canonical = append(canonical, `,"table":`...)
	canonical = appendCanonicalJSONString(canonical, row.Table)
	canonical = append(canonical, '}')
	if h.rowCount > 0 {
		_, _ = h.digest.Write([]byte{','})
		h.byteCount++
	}
	_, _ = h.digest.Write(canonical)
	h.byteCount += int64(len(canonical))
	h.rowCount++
	return nil
}

func (h *GenerationPinnedPushRequestHasher) CurrentByteCount() int64 {
	if h == nil {
		return 0
	}
	return h.byteCount + int64(len(`],"type":"oversync.generation-pinned-push-request.v1"}`))
}

func (h *GenerationPinnedPushRequestHasher) Sum() (string, int64, error) {
	if h == nil {
		return "", 0, fmt.Errorf("generation-pinned push request hasher is nil")
	}
	if !h.finished {
		suffix := []byte(`],"type":"oversync.generation-pinned-push-request.v1"}`)
		_, _ = h.digest.Write(suffix)
		h.byteCount += int64(len(suffix))
		h.hash = hex.EncodeToString(h.digest.Sum(nil))
		h.finished = true
	}
	return h.hash, h.byteCount, nil
}

func hashPushRows(rows []PushRow) (string, int64, error) {
	logicalRows, err := logicalPushRows(rows)
	if err != nil {
		return "", 0, err
	}
	return hash(logicalRows)
}

func logicalPushRows(rows []PushRow) ([]map[string]any, error) {
	logicalRows := make([]map[string]any, 0, len(rows))
	for ordinal, row := range rows {
		payload, err := payloadValue(row.Op, row.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode request payload for %s.%s row %d: %w", row.Schema, row.Table, ordinal, err)
		}
		logicalRows = append(logicalRows, map[string]any{
			"row_ordinal":      strconv.Itoa(ordinal),
			"schema":           row.Schema,
			"table":            row.Table,
			"key":              row.Key,
			"op":               row.Op,
			"base_row_version": strconv.FormatInt(row.BaseRowVersion, 10),
			"payload":          payload,
		})
	}
	return logicalRows, nil
}

func CommittedBundle(rows []BundleRow) (string, int64, error) {
	hasher := NewCommittedBundleHasher()
	for _, row := range rows {
		if err := hasher.Add(row); err != nil {
			return "", 0, err
		}
	}
	return hasher.Sum()
}

// CommittedBundleHasher incrementally hashes the same canonical JSON array as
// CommittedBundle without retaining a complete logical bundle in memory.
type CommittedBundleHasher struct {
	digest    stdhash.Hash
	rowCount  int
	byteCount int64
	finished  bool
	hash      string
}

func NewCommittedBundleHasher() *CommittedBundleHasher {
	hasher := &CommittedBundleHasher{
		digest:    sha256.New(),
		byteCount: 1,
	}
	_, _ = hasher.digest.Write([]byte{'['})
	return hasher
}

func (h *CommittedBundleHasher) Add(row BundleRow) error {
	if h == nil {
		return fmt.Errorf("committed bundle hasher is nil")
	}
	if h.finished {
		return fmt.Errorf("committed bundle hash is already finalized")
	}
	key, err := jcs.Marshal(row.Key)
	if err != nil {
		return err
	}
	payload := []byte("null")
	if row.Op != "DELETE" && len(row.Payload) > 0 {
		payload, err = jcs.Canonicalize(row.Payload)
		if err != nil {
			return fmt.Errorf("decode committed payload for %s.%s row %d: %w", row.Schema, row.Table, h.rowCount, err)
		}
	}
	canonical := make([]byte, 0, len(key)+len(payload)+len(row.Schema)+len(row.Table)+len(row.Op)+96)
	canonical = append(canonical, `{"key":`...)
	canonical = append(canonical, key...)
	canonical = append(canonical, `,"op":`...)
	canonical = appendCanonicalJSONString(canonical, row.Op)
	canonical = append(canonical, `,"payload":`...)
	canonical = append(canonical, payload...)
	canonical = append(canonical, `,"row_ordinal":"`...)
	canonical = strconv.AppendInt(canonical, int64(h.rowCount), 10)
	canonical = append(canonical, `","row_version":"`...)
	canonical = strconv.AppendInt(canonical, row.RowVersion, 10)
	canonical = append(canonical, `","schema":`...)
	canonical = appendCanonicalJSONString(canonical, row.Schema)
	canonical = append(canonical, `,"table":`...)
	canonical = appendCanonicalJSONString(canonical, row.Table)
	canonical = append(canonical, '}')
	if h.rowCount > 0 {
		_, _ = h.digest.Write([]byte{','})
		h.byteCount++
	}
	_, _ = h.digest.Write(canonical)
	h.byteCount += int64(len(canonical))
	h.rowCount++
	return nil
}

// CurrentByteCount returns the complete array byte count if the stream ended
// after the rows added so far. It does not finalize the hasher.
func (h *CommittedBundleHasher) CurrentByteCount() int64 {
	if h == nil {
		return 0
	}
	return h.byteCount + 1
}

func appendCanonicalJSONString(dst []byte, value string) []byte {
	const hexDigits = "0123456789abcdef"
	dst = append(dst, '"')
	for len(value) > 0 {
		r, size := utf8.DecodeRuneInString(value)
		if r == utf8.RuneError && size == 1 {
			r = '\ufffd'
		}
		value = value[size:]
		switch r {
		case '"', '\\':
			dst = append(dst, '\\', byte(r))
		case '\b':
			dst = append(dst, `\b`...)
		case '\t':
			dst = append(dst, `\t`...)
		case '\n':
			dst = append(dst, `\n`...)
		case '\f':
			dst = append(dst, `\f`...)
		case '\r':
			dst = append(dst, `\r`...)
		default:
			if r < 0x20 {
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[byte(r)>>4], hexDigits[byte(r)&0x0f])
			} else {
				dst = utf8.AppendRune(dst, r)
			}
		}
	}
	return append(dst, '"')
}

func (h *CommittedBundleHasher) Sum() (string, int64, error) {
	if h == nil {
		return "", 0, fmt.Errorf("committed bundle hasher is nil")
	}
	if !h.finished {
		_, _ = h.digest.Write([]byte{']'})
		h.byteCount++
		h.hash = hex.EncodeToString(h.digest.Sum(nil))
		h.finished = true
	}
	return h.hash, h.byteCount, nil
}

func IsCanonicalHash(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func payloadValue(op string, payload []byte) (any, error) {
	if op == "DELETE" || len(payload) == 0 {
		return nil, nil
	}
	return jcs.Decode(payload)
}

func hash(value any) (string, int64, error) {
	canonical, err := jcs.Marshal(value)
	if err != nil {
		return "", 0, err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), int64(len(canonical)), nil
}
