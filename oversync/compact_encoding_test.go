package oversync

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCompactEncoding_UUIDKeyBytesRoundTrip(t *testing.T) {
	keyID := uuid.New()

	keyBytes, dbValue, err := encodeKeyBytes(syncKeyTypeUUID, keyID.String())
	require.NoError(t, err)
	require.Len(t, keyBytes, 16)
	require.Equal(t, keyID, dbValue)

	keyString, decodedValue, err := decodeKeyBytes(syncKeyTypeUUID, keyBytes)
	require.NoError(t, err)
	require.Equal(t, keyID.String(), keyString)
	require.Equal(t, keyID, decodedValue)
}

func TestCompactEncoding_TextKeyBytesRoundTrip(t *testing.T) {
	keyBytes, dbValue, err := encodeKeyBytes(syncKeyTypeText, "doc-42")
	require.NoError(t, err)
	require.Equal(t, []byte("doc-42"), keyBytes)
	require.Equal(t, "doc-42", dbValue)

	keyString, decodedValue, err := decodeKeyBytes(syncKeyTypeText, keyBytes)
	require.NoError(t, err)
	require.Equal(t, "doc-42", keyString)
	require.Equal(t, "doc-42", decodedValue)
}

func TestCompactEncoding_RenderBundleHashUsesLowercaseHex(t *testing.T) {
	rawHash, err := hex.DecodeString("A0B1C2D3E4F5A6B7C8D9E0F102030405060708090A0B0C0D0E0F101112131415")
	require.NoError(t, err)
	require.Equal(t, "a0b1c2d3e4f5a6b7c8d9e0f102030405060708090a0b0c0d0e0f101112131415", renderBundleHash(rawHash))
}

func TestCompactEncoding_CommittedBundleHashCanonicalizesLogicalJSON(t *testing.T) {
	rowsA := []BundleRow{{
		Schema:     "public",
		Table:      "users",
		Key:        SyncKey{"id": "row-1"},
		Op:         OpInsert,
		RowVersion: 3,
		Payload:    json.RawMessage(`{"name":"Alice","email":"alice@example.com"}`),
	}}
	rowsB := []BundleRow{{
		Schema:     "public",
		Table:      "users",
		Key:        SyncKey{"id": "row-1"},
		Op:         OpInsert,
		RowVersion: 3,
		Payload:    json.RawMessage(`{"email":"alice@example.com","name":"Alice"}`),
	}}

	hashA, byteCountA, err := computeCommittedBundleHash(rowsA)
	require.NoError(t, err)
	hashB, byteCountB, err := computeCommittedBundleHash(rowsB)
	require.NoError(t, err)
	require.Equal(t, hashA, hashB)
	require.Equal(t, byteCountA, byteCountB)
	require.Positive(t, byteCountA)
}
