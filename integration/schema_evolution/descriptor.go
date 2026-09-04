package schemaevolution

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/oversync"
)

var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type SelectedRow struct {
	Key         uuid.UUID
	Version     string
	InputDigest string
}

type RowRepair struct {
	FailedOperation uuid.UUID
	Key             uuid.UUID
	Replacement     string
}

type BatchDescriptor struct {
	Repair             *RowRepair `json:",omitempty"`
	Epoch              int64
	Census             string
	SQLDigest          string
	BoundsDigest       string
	Authority          string
	Job                string
	Revision           string
	Algorithm          string
	TransitionRevision string
	Phase              string
	Scope              string
	Incarnation        uuid.UUID
	ScopeSequence      int64
	Ordinal            int64
	Rows               []SelectedRow
	Required           []oversync.RegisteredEffectTable
	Forbidden          []oversync.RegisteredEffectTable
	WriterID           string
	OperationID        uuid.UUID
	ValidUntil         time.Time
}

// CanonicalBytes uses UTF-8 length frames (unsigned BE uint64), signed BE int64
// fields, raw 16-byte UUID keys and UTC Unix microseconds. No map iteration,
// JSON number spelling or platform byte order enters operation identity.
func (d BatchDescriptor) CanonicalBytes() ([]byte, error) {
	if d.Epoch < 1 || !digestPattern.MatchString(d.Census) || !digestPattern.MatchString(d.SQLDigest) || !digestPattern.MatchString(d.BoundsDigest) || d.Authority == "" || d.Job == "" || d.Revision == "" || d.Algorithm == "" || d.Scope == "" || d.Incarnation == uuid.Nil || d.OperationID == uuid.Nil || d.Ordinal < 1 || len(d.Rows) > 100 || d.ValidUntil.IsZero() {
		return nil, fmt.Errorf("invalid batch descriptor")
	}
	if d.ScopeSequence < 1 || d.TransitionRevision == "" || d.Phase == "" || d.WriterID == "" || len(d.Rows) == 0 || d.ValidUntil.Nanosecond()%1000 != 0 || !digestPattern.MatchString(d.Algorithm) {
		return nil, fmt.Errorf("malformed descriptor identity")
	}
	for _, row := range d.Rows {
		if row.Version == "" || !digestPattern.MatchString(row.InputDigest) {
			return nil, fmt.Errorf("malformed descriptor input")
		}
	}
	var b bytes.Buffer
	frame := func(s string) { _ = binary.Write(&b, binary.BigEndian, uint64(len(s))); b.WriteString(s) }
	frame("app-backfill-operation-v3")
	_ = binary.Write(&b, binary.BigEndian, d.Epoch)
	frame(d.Census)
	frame(d.SQLDigest)
	frame(d.BoundsDigest)
	for _, s := range []string{d.Authority, d.Job, d.Revision, d.Algorithm, d.TransitionRevision, d.Phase, d.Scope} {
		frame(s)
	}
	b.Write(d.Incarnation[:])
	_ = binary.Write(&b, binary.BigEndian, d.ScopeSequence)
	_ = binary.Write(&b, binary.BigEndian, d.Ordinal)
	_ = binary.Write(&b, binary.BigEndian, uint64(len(d.Rows)))
	var previous uuid.UUID
	for i, row := range d.Rows {
		if i > 0 && bytes.Compare(previous[:], row.Key[:]) >= 0 {
			return nil, fmt.Errorf("keys must be unique and ordered")
		}
		previous = row.Key
		b.Write(row.Key[:])
		frame(row.Version)
		frame(row.InputDigest)
	}
	for _, effects := range [][]oversync.RegisteredEffectTable{d.Required, d.Forbidden} {
		_ = binary.Write(&b, binary.BigEndian, uint64(len(effects)))
		for _, table := range effects {
			frame(table.Schema)
			frame(table.Table)
		}
	}
	frame(d.WriterID)
	b.Write(d.OperationID[:])
	_ = binary.Write(&b, binary.BigEndian, d.ValidUntil.UTC().UnixMicro())
	if d.Repair != nil {
		if d.Repair.FailedOperation == uuid.Nil || len(d.Repair.Replacement) == 0 || len(d.Repair.Replacement) > 4096 {
			return nil, fmt.Errorf("invalid repair descriptor")
		}
		frame("app-backfill-repair-v1")
		b.Write(d.Repair.FailedOperation[:])
		b.Write(d.Repair.Key[:])
		frame(d.Repair.Replacement)
	}
	return b.Bytes(), nil
}

func (d BatchDescriptor) WriteOptions() (oversync.ScopeWriteOptions, error) {
	encoded, err := d.CanonicalBytes()
	if err != nil {
		return oversync.ScopeWriteOptions{}, err
	}
	hash, _ := hex.DecodeString(digest(encoded))
	var array [32]byte
	copy(array[:], hash)
	return oversync.ScopeWriteOptions{WriterID: d.WriterID, RetryableWriteOptions: oversync.RetryableWriteOptions{OperationID: d.OperationID, OperationHash: array, OperationValidUntil: d.ValidUntil}, RequiredEffectTables: d.Required, ForbiddenEffectTables: d.Forbidden}, nil
}
