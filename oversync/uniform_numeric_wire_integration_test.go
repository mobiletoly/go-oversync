package oversync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestUniformNumericWire_PostgresRoundTripAcrossServerSurfaces(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "uniform_numeric_" + suffix
	tableIdent := createUniformNumericTestTable(t, ctx, pool, schemaName)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	registered := []RegisteredTable{{Schema: schemaName, Table: "numeric_values", SyncKeyColumns: []string{"id"}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsFromDatabase(t, ctx, pool, registered, nil), AppName: "uniform-numeric-wire-round-trip",
		RegisteredTables:               registered,
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	userID := "uniform-numeric-user-" + suffix
	writer := Actor{UserID: userID, SourceID: "sqlite-client"}
	rowID := uuid.New()
	requestRows := []PushRequestRow{{
		Schema:         schemaName,
		Table:          "numeric_values",
		Key:            SyncKey{"id": rowID.String()},
		Op:             OpInsert,
		BaseRowVersion: 0,
		Payload: json.RawMessage(fmt.Sprintf(`{
			"id":"%s",
			"small_count":"-32768",
			"count":"2147483647",
			"large_count":"9007199254740993",
			"amount":"1234567890.1234567890",
			"ratio":"1.2345678806304932",
			"score":"5e-324",
			"enabled":true
		}`, rowID)),
	}}
	bundle, err := pushRowsOneShot(t, ctx, service, writer, 1, requestRows)
	require.NoError(t, err)
	require.NotNil(t, bundle)
	require.Len(t, bundle.Rows, 1)
	requireUniformNumericPayload(t, bundle.Rows[0].Payload, rowID, true)

	t.Run("PostgreSQL storage", func(t *testing.T) {
		var small int16
		var count int32
		var large, amount, ratio, score string
		var enabled bool
		require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT small_count, count, large_count::text, amount::text, ratio::text, score::text, enabled
			FROM %s
			WHERE _sync_scope_id = $1 AND id = $2
		`, tableIdent), userID, rowID).Scan(&small, &count, &large, &amount, &ratio, &score, &enabled))
		require.Equal(t, int16(-32768), small)
		require.Equal(t, int32(2147483647), count)
		require.Equal(t, "9007199254740993", large)
		require.Equal(t, "1234567890.1234567890", amount)
		require.Equal(t, "1.2345679", ratio)
		require.Equal(t, "5e-324", score)
		require.True(t, enabled)
	})

	t.Run("committed hash and bytes use materialized strings", func(t *testing.T) {
		wantHash, wantBytes, err := computeCommittedBundleHash(bundle.Rows)
		require.NoError(t, err)
		userPK, err := lookupUserPK(ctx, pool, userID)
		require.NoError(t, err)
		var storedHash []byte
		var storedBytes int64
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT bundle_hash, byte_count
			FROM sync.bundle_log
			WHERE user_pk = $1 AND bundle_seq = $2
		`, userPK, bundle.BundleSeq).Scan(&storedHash, &storedBytes))
		require.Equal(t, wantHash, storedHash)
		require.Equal(t, wantBytes, storedBytes)
	})

	reader := Actor{UserID: userID, SourceID: "reader"}
	_, err = service.Connect(ctx, reader, testSchemaGenerationProofForService(service), &ConnectRequest{})
	require.NoError(t, err)
	t.Run("pull", func(t *testing.T) {
		page, err := service.ProcessPull(ctx, reader, testSchemaGenerationProofForService(service), 0, 10, 0)
		require.NoError(t, err)
		require.Len(t, page.Bundles, 1)
		requireUniformNumericPayload(t, page.Bundles[0].Rows[0].Payload, rowID, true)
	})

	t.Run("snapshot", func(t *testing.T) {
		session, err := service.CreateSnapshotSession(ctx, reader, testSchemaGenerationProofForService(service))
		require.NoError(t, err)
		chunk, err := service.GetSnapshotChunk(ctx, reader, testSchemaGenerationProofForService(service), session.SnapshotID, 0, 10, defaultBytesPerSnapshotChunk)
		require.NoError(t, err)
		require.Len(t, chunk.Rows, 1)
		requireUniformNumericPayload(t, chunk.Rows[0].Payload, rowID, true)
	})

	t.Run("conflict server row", func(t *testing.T) {
		_, err := pushRowsOneShot(t, ctx, service, Actor{UserID: userID, SourceID: "conflicting-client"}, 1, []PushRequestRow{{
			Schema:         schemaName,
			Table:          "numeric_values",
			Key:            SyncKey{"id": rowID.String()},
			Op:             OpInsert,
			BaseRowVersion: 0,
			Payload:        requestRows[0].Payload,
		}})
		var conflict *PushConflictError
		require.ErrorAs(t, err, &conflict)
		requireUniformNumericPayload(t, conflict.Conflict.ServerRow, rowID, true)
	})

	t.Run("direct capture", func(t *testing.T) {
		directID := uuid.New()
		directActor := Actor{UserID: userID, SourceID: "server-app"}
		require.NoError(t, service.WithinSyncBundle(ctx, directActor, BundleSource{SourceID: directActor.SourceID, SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				INSERT INTO %s (id, small_count, count, large_count, amount, ratio, score, enabled)
				VALUES ($1, -32768, 2147483647, 9007199254740993, 1234567890.1234567890, 1.2345678901234567, 5e-324, FALSE)
			`, tableIdent), directID)
			return err
		}))
		var bundleSeq int64
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT max(bundle_seq)
			FROM sync.bundle_log
			WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
		`, userID).Scan(&bundleSeq))
		directBundle := loadCommittedBundleForUser(t, ctx, service, userID, bundleSeq)
		require.Len(t, directBundle.Rows, 1)
		requireUniformNumericPayload(t, directBundle.Rows[0].Payload, directID, false)
	})
}

func TestUniformNumericWire_NonFiniteManagedWriteRollsBack(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "uniform_nonfinite_" + suffix
	tableIdent := createUniformNumericTestTable(t, ctx, pool, schemaName)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	registered := []RegisteredTable{{Schema: schemaName, Table: "numeric_values", SyncKeyColumns: []string{"id"}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsFromDatabase(t, ctx, pool, registered, nil), AppName: "uniform-numeric-nonfinite",
		RegisteredTables:               registered,
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	userID := "uniform-nonfinite-user-" + suffix
	actor := Actor{UserID: userID, SourceID: "server-app"}
	mustInitializeEmptyScope(t, ctx, service, userID, actor.SourceID)
	for _, test := range []struct {
		name   string
		amount string
		ratio  string
		score  string
	}{
		{name: "numeric NaN", amount: `'NaN'::numeric`, ratio: `1`, score: `1`},
		{name: "float4 infinity", amount: `1`, ratio: `'Infinity'::float4`, score: `1`},
		{name: "float8 NaN", amount: `1`, ratio: `1`, score: `'NaN'::float8`},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := service.WithinSyncBundle(ctx, actor, BundleSource{SourceID: actor.SourceID, SourceBundleID: 1}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
				_, err := tx.Exec(ctx, fmt.Sprintf(`
					INSERT INTO %s (id, small_count, count, large_count, amount, ratio, score, enabled)
					VALUES ($1, 1, 1, 1, %s, %s, %s, TRUE)
				`, tableIdent, test.amount, test.ratio, test.score), uuid.New())
				return err
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), "canonicalize")
		})
	}

	var businessRows, bundleRows int64
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE _sync_scope_id = $1`, tableIdent), userID).Scan(&businessRows))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*)
		FROM sync.bundle_log
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, userID).Scan(&bundleRows))
	require.Zero(t, businessRows)
	require.Zero(t, bundleRows)
}

func TestUniformNumericWire_InvalidPushRejectsWithoutPersistence(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "uniform_invalid_push_" + suffix
	tableIdent := createUniformNumericTestTable(t, ctx, pool, schemaName)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	registered := []RegisteredTable{{Schema: schemaName, Table: "numeric_values", SyncKeyColumns: []string{"id"}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsFromDatabase(t, ctx, pool, registered, nil), AppName: "uniform-numeric-invalid-push",
		RegisteredTables:               registered,
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	actor := Actor{UserID: "uniform-invalid-push-user-" + suffix, SourceID: "sqlite-client"}
	initializationID := resolveConnectForPush(t, ctx, service, actor, true)
	scenarios := []struct {
		name          string
		payloadFields string
		errorCategory string
	}{
		{
			name:          "legacy JSON number",
			payloadFields: `"small_count":"1","count":"1","large_count":1,"amount":"1","ratio":"1","score":"1","enabled":true`,
			errorCategory: "legacy_json_number",
		},
		{
			name:          "float4 overflow",
			payloadFields: `"small_count":"1","count":"1","large_count":"1","amount":"1","ratio":"3.5e+38","score":"1","enabled":true`,
			errorCategory: "destination_type_invalid",
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			rowID := uuid.New()
			rows := []PushRequestRow{{
				Schema: schemaName, Table: "numeric_values", Key: SyncKey{"id": rowID.String()}, Op: OpInsert,
				Payload: json.RawMessage(fmt.Sprintf(`{"id":"%s",%s}`, rowID, scenario.payloadFields)),
			}}
			_, err := service.PushOneShot(ctx, actor, testSchemaGenerationProofForService(service), &OneShotPushRequest{
				SourceBundleID: 1, CanonicalRequestHash: mustCanonicalPushRequestHash(t, service, rows), InitializationID: initializationID, Rows: rows,
			})

			var invalid *PushValidationError
			require.ErrorAs(t, err, &invalid)
			require.ErrorContains(t, err, scenario.errorCategory)

			var businessRows int64
			require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, tableIdent)).Scan(&businessRows))
			require.Zero(t, businessRows)
		})
	}
}

func TestUniformNumericWire_ParameterizedNumericGenerationIsRejected(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schemaName := "numeric_typmod_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s; CREATE TABLE %s.values (_sync_scope_id TEXT NOT NULL,id UUID NOT NULL,amount NUMERIC(30,10) NOT NULL,PRIMARY KEY (_sync_scope_id,id))`, schemaIdent, schemaIdent))
	require.NoError(t, err)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	registered := []RegisteredTable{{Schema: schemaName, Table: "values", SyncKeyColumns: []string{"id"}}}
	service, err := newTestRuntimeService(pool, &ServiceConfig{
		RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
		SchemaGenerations: []SchemaGeneration{{ID: 1, Tables: []GenerationTable{{Schema: schemaName, Table: "values", Columns: []GenerationColumn{{Name: "amount", WireType: GenerationWireTypeDecimal}, {Name: "id", WireType: GenerationWireTypeUUID}}}}}},
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	err = service.Bootstrap(ctx)
	var unsupported *UnsupportedSchemaError
	require.ErrorAs(t, err, &unsupported)
	require.Equal(t, UnsupportedSchemaGenerationColumnTypeMismatch, unsupported.Code)
	require.Equal(t, "amount", unsupported.Column)
}

func TestUniformNumericWire_SnapshotRejectsNonFiniteRowsAtomically(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "uniform_snapshot_nonfinite_" + suffix
	tableIdent := createUniformNumericTestTable(t, ctx, pool, schemaName)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	registered := []RegisteredTable{{Schema: schemaName, Table: "numeric_values", SyncKeyColumns: []string{"id"}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsFromDatabase(t, ctx, pool, registered, nil), AppName: "uniform-numeric-snapshot-nonfinite",
		RegisteredTables:               registered,
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))

	userID := "uniform-snapshot-user-" + suffix
	actor := Actor{UserID: userID, SourceID: "writer"}
	rowID := uuid.New()
	_, err := pushRowsOneShot(t, ctx, service, actor, 1, []PushRequestRow{{
		Schema: schemaName, Table: "numeric_values", Key: SyncKey{"id": rowID.String()}, Op: OpInsert,
		Payload: json.RawMessage(fmt.Sprintf(`{"id":"%s","small_count":"1","count":"1","large_count":"1","amount":"1","ratio":"1","score":"1","enabled":true}`, rowID)),
	}})
	require.NoError(t, err)

	_, err = pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DISABLE TRIGGER ALL`, tableIdent))
	require.NoError(t, err)
	_, updateErr := pool.Exec(ctx, fmt.Sprintf(`UPDATE %s SET score = 'Infinity'::float8 WHERE _sync_scope_id = $1 AND id = $2`, tableIdent), userID, rowID)
	_, enableErr := pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ENABLE TRIGGER ALL`, tableIdent))
	require.NoError(t, updateErr)
	require.NoError(t, enableErr)

	reader := Actor{UserID: userID, SourceID: "reader"}
	_, err = service.Connect(ctx, reader, testSchemaGenerationProofForService(service), &ConnectRequest{})
	require.NoError(t, err)
	_, err = service.CreateSnapshotSession(ctx, reader, testSchemaGenerationProofForService(service))
	require.ErrorContains(t, err, "database value must be finite")
	var sessionCount int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*)
		FROM sync.snapshot_sessions
		WHERE user_pk = (SELECT user_pk FROM sync.user_state WHERE user_id = $1)
	`, userID).Scan(&sessionCount))
	require.Zero(t, sessionCount)
}

func TestUniformNumericWire_BootstrapRejectsNumericDomainAtomically(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "uniform_domain_" + suffix
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	tableIdent := pgx.Identifier{schemaName, "numeric_values"}.Sanitize()
	require.NoError(t, func() error {
		_, err := pool.Exec(ctx, fmt.Sprintf(`
			CREATE SCHEMA %s;
			CREATE DOMAIN %s.amount_domain AS NUMERIC(20, 4);
			CREATE TABLE %s (
				_sync_scope_id TEXT NOT NULL,
				id UUID NOT NULL,
				amount %s.amount_domain NOT NULL,
				PRIMARY KEY (_sync_scope_id, id)
			)
		`, schemaIdent, schemaIdent, tableIdent, schemaIdent))
		return err
	}())
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })

	registered := []RegisteredTable{{Schema: schemaName, Table: "numeric_values", SyncKeyColumns: []string{"id"}}}
	service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables(registered), AppName: "uniform-numeric-domain-rejection",
		RegisteredTables:               registered,
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	err = service.Bootstrap(ctx)
	var unsupported *UnsupportedSchemaError
	require.True(t, errors.As(err, &unsupported), "bootstrap error: %v", err)
	require.ErrorContains(t, err, "unsupported numeric domain")

	var syncSchemaExists bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT to_regnamespace('sync') IS NOT NULL`).Scan(&syncSchemaExists))
	require.False(t, syncSchemaExists)
}

func TestUniformNumericWire_BootstrapRejectsNonFiniteAdoptionAtomically(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	schemaName := "uniform_adoption_nonfinite_" + suffix
	tableIdent := createUniformNumericTestTable(t, ctx, pool, schemaName)
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schemaName) })
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (_sync_scope_id, id, small_count, count, large_count, amount, ratio, score, enabled)
		VALUES ($1, $2, 1, 1, 1, 1, 1, 'NaN'::float8, TRUE)
	`, tableIdent), "adoption-user", uuid.New())
	require.NoError(t, err)

	registered := []RegisteredTable{{Schema: schemaName, Table: "numeric_values", SyncKeyColumns: []string{"id"}}}
	service, err := newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsFromDatabase(t, ctx, pool, registered, nil), AppName: "uniform-numeric-adoption-nonfinite",
		RegisteredTables:               registered,
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	require.NoError(t, err)
	err = service.Bootstrap(ctx)
	require.ErrorContains(t, err, "baseline_unrepresentable")
	require.ErrorContains(t, err, "database value must be finite")

	var syncSchemaExists bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT to_regnamespace('sync') IS NOT NULL`).Scan(&syncSchemaExists))
	require.False(t, syncSchemaExists)
}

func createUniformNumericTestTable(t *testing.T, ctx context.Context, pool interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, schemaName string) string {
	t.Helper()
	schemaIdent := pgx.Identifier{schemaName}.Sanitize()
	tableIdent := pgx.Identifier{schemaName, "numeric_values"}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE SCHEMA %s;
		CREATE TABLE %s (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			small_count SMALLINT NOT NULL,
			count INTEGER NOT NULL,
			large_count BIGINT NOT NULL,
			amount NUMERIC NOT NULL,
			ratio REAL NOT NULL,
			score DOUBLE PRECISION NOT NULL,
			enabled BOOLEAN NOT NULL,
			PRIMARY KEY (_sync_scope_id, id),
			CONSTRAINT numeric_values_amount_bounds_chk CHECK (
				amount = 'NaN'::numeric
				OR (abs(amount) < 100000000000000000000::numeric AND scale(amount) <= 10)
			)
		)
	`, schemaIdent, tableIdent))
	require.NoError(t, err)
	return tableIdent
}

func requireUniformNumericPayload(t *testing.T, payload json.RawMessage, rowID uuid.UUID, enabled bool) {
	t.Helper()
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &fields))
	require.Equal(t, `"`+rowID.String()+`"`, string(fields["id"]))
	require.Equal(t, `"-32768"`, string(fields["small_count"]))
	require.Equal(t, `"2147483647"`, string(fields["count"]))
	require.Equal(t, `"9007199254740993"`, string(fields["large_count"]))
	require.Equal(t, `"1234567890.1234567890"`, string(fields["amount"]))
	require.Equal(t, `"1.2345678806304932"`, string(fields["ratio"]))
	require.Equal(t, `"5e-324"`, string(fields["score"]))
	require.Equal(t, enabled, string(fields["enabled"]) == "true")
}
