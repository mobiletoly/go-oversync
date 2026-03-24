package oversqlite_e2e

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversqlite"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type typedRowFixture struct {
	ID                 string
	Name               string
	Note               *string
	CountValue         *int64
	EnabledFlag        int64
	RatingLiteral      *string
	RatingExpectedText *string
	DataHex            *string
	CreatedAt          *string
}

func TestEndToEnd_TypedRowsPushPullHydrateAndImmediatePullStayConsistent(t *testing.T) {
	ctx := context.Background()
	schema := "e2e_typed_rows_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	server := newExampleServer(t, schema)

	userID := "e2e-typed-user-" + uuid.NewString()
	config := oversqlite.DefaultConfig(schema, syncTables("typed_rows"))

	seedClient, seedDB := newSQLiteClient(t, server, userID, "device-seed", config, typedRowsDDL)
	activeClient, activeDB := newSQLiteClient(t, server, userID, "device-active", oversqlite.DefaultConfig(schema, syncTables("typed_rows")), typedRowsDDL)
	hydrateClient, hydrateDB := newSQLiteClient(t, server, userID, "device-hydrate", oversqlite.DefaultConfig(schema, syncTables("typed_rows")), typedRowsDDL)

	seedRatingLiteral := "1.25"
	seedRatingExpected := "1.25"
	seedDataHex := "00112233445566778899aabbccddeeff"
	seedCreatedAt := "2026-03-24T18:42:11Z"
	seedCountValue := int64(42)
	seedRow := typedRowFixture{
		ID:                 uuid.NewString(),
		Name:               "Seed Typed Row",
		CountValue:         &seedCountValue,
		EnabledFlag:        1,
		RatingLiteral:      &seedRatingLiteral,
		RatingExpectedText: &seedRatingExpected,
		DataHex:            &seedDataHex,
		CreatedAt:          &seedCreatedAt,
	}
	insertTypedRow(t, seedDB, seedRow)
	require.NoError(t, seedClient.PushPending(ctx))

	activeNote := "second-device"
	activeRatingLiteral := "6.57111473696007"
	activeRatingExpected := "6.57111473696007"
	activeRow := typedRowFixture{
		ID:                 uuid.NewString(),
		Name:               "Active Typed Row",
		Note:               &activeNote,
		EnabledFlag:        0,
		RatingLiteral:      &activeRatingLiteral,
		RatingExpectedText: &activeRatingExpected,
	}
	insertTypedRow(t, activeDB, activeRow)

	peerNote := "peer-from-server"
	peerRatingLiteral := "2.5"
	peerRatingExpected := "2.5"
	peerCreatedAt := "2026-03-24T10:42:11-08:00"
	peerRow := typedRowFixture{
		ID:                 uuid.NewString(),
		Name:               "Peer Typed Row",
		Note:               &peerNote,
		EnabledFlag:        1,
		RatingLiteral:      &peerRatingLiteral,
		RatingExpectedText: &peerRatingExpected,
		CreatedAt:          &peerCreatedAt,
	}
	serverActor := oversync.Actor{UserID: userID, SourceID: "server-writer"}
	require.NoError(t, server.SyncService.WithinSyncBundle(ctx, serverActor, oversync.BundleSource{
		SourceID:       serverActor.SourceID,
		SourceBundleID: 101,
	}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.typed_rows (id, name, note, count_value, enabled_flag, rating, data, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, pgx.Identifier{server.BusinessSchema}.Sanitize()),
			peerRow.ID,
			peerRow.Name,
			peerNote,
			nil,
			peerRow.EnabledFlag,
			2.5,
			nil,
			peerCreatedAt,
		)
		return err
	}))

	require.NoError(t, activeClient.PushPending(ctx))
	require.NoError(t, activeClient.PullToStable(ctx))
	require.NoError(t, hydrateClient.Hydrate(ctx))

	assertTableCount(t, activeDB, "typed_rows", 3)
	assertTableCount(t, hydrateDB, "typed_rows", 3)
	assertTableCount(t, activeDB, "_sync_dirty_rows", 0)
	assertTableCount(t, hydrateDB, "_sync_dirty_rows", 0)

	assertTypedRowState(t, activeDB, seedRow)
	assertTypedRowState(t, activeDB, activeRow)
	assertTypedRowState(t, activeDB, peerRow)
	assertTypedRowState(t, hydrateDB, seedRow)
	assertTypedRowState(t, hydrateDB, activeRow)
	assertTypedRowState(t, hydrateDB, peerRow)
}

func insertTypedRow(t *testing.T, db *sql.DB, row typedRowFixture) {
	t.Helper()

	var dataBytes []byte
	if row.DataHex != nil {
		var err error
		dataBytes, err = hex.DecodeString(*row.DataHex)
		require.NoError(t, err)
	}

	_, err := db.Exec(`
		INSERT INTO typed_rows(id, name, note, count_value, enabled_flag, rating, data, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`,
		row.ID,
		row.Name,
		nullStringPointer(row.Note),
		nullInt64Pointer(row.CountValue),
		row.EnabledFlag,
		nullStringPointer(row.RatingLiteral),
		nullBytesPointer(dataBytes, row.DataHex != nil),
		nullStringPointer(row.CreatedAt),
	)
	require.NoError(t, err)
}

func assertTypedRowState(t *testing.T, db *sql.DB, row typedRowFixture) {
	t.Helper()

	var (
		name        string
		note        sql.NullString
		countValue  sql.NullInt64
		enabledFlag int64
		ratingText  sql.NullString
		dataLen     sql.NullInt64
		dataHex     sql.NullString
		createdAt   sql.NullString
	)

	require.NoError(t, db.QueryRow(`
		SELECT
			name,
			note,
			count_value,
			enabled_flag,
			CASE WHEN rating IS NULL THEN NULL ELSE quote(rating) END,
			CASE WHEN data IS NULL THEN NULL ELSE length(data) END,
			CASE WHEN data IS NULL THEN NULL ELSE hex(data) END,
			created_at
		FROM typed_rows
		WHERE id = ?
	`, row.ID).Scan(&name, &note, &countValue, &enabledFlag, &ratingText, &dataLen, &dataHex, &createdAt))

	require.Equal(t, row.Name, name)
	assertNullStringEquals(t, note, row.Note)
	assertNullInt64Equals(t, countValue, row.CountValue)
	require.Equal(t, row.EnabledFlag, enabledFlag)
	assertNullStringEquals(t, ratingText, row.RatingExpectedText)

	if row.DataHex == nil {
		require.False(t, dataLen.Valid)
		require.False(t, dataHex.Valid)
	} else {
		require.True(t, dataLen.Valid)
		require.True(t, dataHex.Valid)
		require.Equal(t, int64(len(*row.DataHex)/2), dataLen.Int64)
		require.Equal(t, strings.ToUpper(*row.DataHex), dataHex.String)
	}

	if row.CreatedAt == nil {
		require.False(t, createdAt.Valid)
		return
	}

	require.True(t, createdAt.Valid)
	require.Equal(t, mustParseRFC3339Instant(t, *row.CreatedAt), mustParseRFC3339Instant(t, createdAt.String))
}

func assertTableCount(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)).Scan(&count))
	require.Equal(t, want, count)
}

func assertNullStringEquals(t *testing.T, got sql.NullString, want *string) {
	t.Helper()
	if want == nil {
		require.False(t, got.Valid)
		return
	}
	require.True(t, got.Valid)
	require.Equal(t, *want, got.String)
}

func assertNullInt64Equals(t *testing.T, got sql.NullInt64, want *int64) {
	t.Helper()
	if want == nil {
		require.False(t, got.Valid)
		return
	}
	require.True(t, got.Valid)
	require.Equal(t, *want, got.Int64)
}

func mustParseRFC3339Instant(t *testing.T, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	require.NoError(t, err)
	return parsed.UTC()
}

func nullStringPointer(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullInt64Pointer(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullBytesPointer(value []byte, valid bool) any {
	if !valid {
		return nil
	}
	return value
}
