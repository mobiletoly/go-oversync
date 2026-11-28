// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/oversync"
)

func computeCanonicalPushRequestHash(rows []oversync.PushRequestRow) (string, error) {
	logicalRows := make([]protocolhash.PushRow, 0, len(rows))
	for _, row := range rows {
		logicalRows = append(logicalRows, protocolhash.PushRow{
			Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op,
			BaseRowVersion: row.BaseRowVersion, Payload: row.Payload,
		})
	}
	hash, _, err := protocolhash.PushRequest(logicalRows)
	if err != nil {
		return "", fmt.Errorf("failed to canonicalize push request rows: %w", err)
	}
	return hash, nil
}

func computeGenerationPinnedPushRequestHash(proof oversync.SchemaGenerationProof, rows []oversync.PushRequestRow) (string, error) {
	logicalRows := make([]protocolhash.PushRow, 0, len(rows))
	for _, row := range rows {
		logicalRows = append(logicalRows, protocolhash.PushRow{Schema: row.Schema, Table: row.Table, Key: row.Key, Op: row.Op, BaseRowVersion: row.BaseRowVersion, Payload: row.Payload})
	}
	hash, _, err := protocolhash.GenerationPinnedPushRequest(strconv.FormatInt(int64(proof.ID), 10), proof.Fingerprint, logicalRows)
	if err != nil {
		return "", fmt.Errorf("failed to canonicalize generation-pinned push request rows: %w", err)
	}
	return hash, nil
}

func canonicalizeJSONBytes(raw []byte) ([]byte, error) {
	return jcs.Canonicalize(raw)
}

func replayEquivalentJSON(leftRaw, rightRaw string) (bool, error) {
	left, err := jcs.Decode([]byte(leftRaw))
	if err != nil {
		return false, fmt.Errorf("failed to decode left replay JSON: %w", err)
	}
	right, err := jcs.Decode([]byte(rightRaw))
	if err != nil {
		return false, fmt.Errorf("failed to decode right replay JSON: %w", err)
	}
	return replayEquivalentValue(left, right), nil
}

func replayEquivalentValue(left, right any) bool {
	switch l := left.(type) {
	case nil:
		return right == nil
	case bool:
		r, ok := right.(bool)
		return ok && l == r
	case json.Number:
		r, ok := right.(json.Number)
		if !ok {
			return false
		}
		ln, lerr := strconv.ParseFloat(l.String(), 64)
		rn, rerr := strconv.ParseFloat(r.String(), 64)
		return lerr == nil && rerr == nil && ln == rn
	case string:
		r, ok := right.(string)
		if !ok {
			return false
		}
		if li, lok := parseRFC3339Instant(l); lok {
			if ri, rok := parseRFC3339Instant(r); rok {
				return li.Equal(ri)
			}
		}
		return l == r
	case []any:
		r, ok := right.([]any)
		if !ok || len(l) != len(r) {
			return false
		}
		for i := range l {
			if !replayEquivalentValue(l[i], r[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		r, ok := right.(map[string]any)
		if !ok || len(l) != len(r) {
			return false
		}
		for key, lv := range l {
			rv, ok := r[key]
			if !ok || !replayEquivalentValue(lv, rv) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func parseRFC3339Instant(raw string) (time.Time, bool) {
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}
