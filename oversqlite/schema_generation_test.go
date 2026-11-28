// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversqlite

import (
	"context"
	"database/sql"
	"net/http"
	"strconv"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func TestBorrowedClientValidatesGenerationSelectionBeforeOwnershipOrMutation(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL)`)
	require.NoError(t, err)
	for _, id := range []oversync.SchemaGenerationID{0, -1} {
		t.Run(strconv.FormatInt(int64(id), 10), func(t *testing.T) {
			config := DefaultConfig(id, []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}})
			config.DatabaseID, config.ServiceID = "selection-db", "selection-service"
			_, err := newClientFromDB(db, "http://example.invalid", tokenProviderForTests, config)
			require.ErrorContains(t, err, "positive generation ID")
			var count int
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name LIKE '_sync_%'`).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestClientRetainsImmutableGenerationProofForEveryRequest(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, func() error {
		_, err := db.Exec(`CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL)`)
		return err
	}())
	proof := oversync.SchemaGenerationProof{ID: 7, Fingerprint: strings.Repeat("b", 64)}
	config := admittedFixtureConfig("main", []SyncTable{{TableName: "users", SyncKeyColumnName: "id"}}, proof)
	client, err := newTestClient(t, db, "http://example.invalid", func(context.Context) (string, error) { return "token", nil }, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	expected := client.schemaGenerationProof

	client.config.generationProof = oversync.SchemaGenerationProof{ID: 8, Fingerprint: strings.Repeat("c", 64)}
	request, err := http.NewRequest(http.MethodGet, "http://example.invalid/sync/generations/1", nil)
	require.NoError(t, err)
	client.sourceID = "device-a"
	require.NoError(t, client.applyAuthenticatedSyncHeaders(request, "token"))
	require.Equal(t, strconv.Itoa(int(expected.ID)), request.Header.Get(oversync.SchemaGenerationIDHeader))
	require.Equal(t, expected.Fingerprint, request.Header.Get(oversync.SchemaGenerationFingerprintHeader))
}
