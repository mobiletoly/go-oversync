//go:build oversync_audit

package oversync

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	auditBenchmarkProfileEnvironment = "OVERSYNC_AUDIT_BENCHMARK_PROFILE"
	auditBenchmarkTimeoutEnvironment = "OVERSYNC_AUDIT_BENCHMARK_OPERATION_TIMEOUT"
	defaultAuditBenchmarkTimeout     = 10 * time.Minute
)

type auditBenchmarkProfile int

const (
	auditBenchmarkProfileSmoke auditBenchmarkProfile = iota
	auditBenchmarkProfileFull
	auditBenchmarkProfileTop
)

func (p auditBenchmarkProfile) String() string {
	switch p {
	case auditBenchmarkProfileSmoke:
		return "smoke"
	case auditBenchmarkProfileFull:
		return "full"
	case auditBenchmarkProfileTop:
		return "top"
	default:
		return "unknown"
	}
}

func currentAuditBenchmarkProfile(b *testing.B) auditBenchmarkProfile {
	b.Helper()

	raw := strings.ToLower(strings.TrimSpace(os.Getenv(auditBenchmarkProfileEnvironment)))
	if (raw == "full" || raw == "large" || raw == "top") && strings.EqualFold(os.Getenv("GITHUB_ACTIONS"), "true") {
		b.Fatalf("heavy audit benchmark profile is local-only and must not run in GitHub Actions")
	}
	switch raw {
	case "", "smoke":
		return auditBenchmarkProfileSmoke
	case "full":
		return auditBenchmarkProfileFull
	case "large", "top":
		return auditBenchmarkProfileTop
	default:
		b.Fatalf(
			"%s must be smoke, full, or top (large is accepted as an alias)",
			auditBenchmarkProfileEnvironment,
		)
		return auditBenchmarkProfileSmoke
	}
}

func requireAuditBenchmarkProfile(b *testing.B, required auditBenchmarkProfile) {
	b.Helper()

	actual := currentAuditBenchmarkProfile(b)
	if actual < required {
		b.Skipf(
			"requires %s=%s (current profile %s keeps ordinary audit benchmark runs bounded)",
			auditBenchmarkProfileEnvironment,
			required,
			actual,
		)
	}
}

func auditBenchmarkOperationTimeout(b *testing.B) time.Duration {
	b.Helper()

	raw := strings.TrimSpace(os.Getenv(auditBenchmarkTimeoutEnvironment))
	if raw == "" {
		return defaultAuditBenchmarkTimeout
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		b.Fatalf("%s must be a positive Go duration, got %q", auditBenchmarkTimeoutEnvironment, raw)
	}
	return duration
}

func auditBenchmarkFatalOperation(b *testing.B, operation string, timeout time.Duration, err error) {
	b.Helper()

	if errors.Is(err, context.DeadlineExceeded) {
		b.Fatalf(
			"%s exceeded %s=%s; record this as explicit timeout evidence if the top-scale case cannot complete five warmups and ten measured samples: %v",
			operation,
			auditBenchmarkTimeoutEnvironment,
			timeout,
			err,
		)
	}
	b.Fatalf("%s: %v", operation, err)
}

type auditDatabaseBenchmarkFixture struct {
	ctx                 context.Context
	pool                *pgxpool.Pool
	svc                 *SyncService
	schemaName          string
	writer              Actor
	reader              Actor
	operationTimeout    time.Duration
	postgresContainerID string
}

func newAuditDatabaseBenchmarkFixture(b *testing.B, scenario string) *auditDatabaseBenchmarkFixture {
	b.Helper()

	ctx := context.Background()
	operationTimeout := auditBenchmarkOperationTimeout(b)
	sequence := managedIntegrationDatabaseSequence.Add(1)
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	externalDatabase := databaseURL != ""
	var managedServer *managedIntegrationPostgresServer
	var databaseName string
	var pool *pgxpool.Pool
	var svc *SyncService
	var schemaName string

	if externalDatabase {
		callerManagedIntegrationDatabaseMu.Lock()
	} else {
		var err error
		managedServer, err = getManagedIntegrationPostgres()
		if err != nil {
			b.Fatalf("start managed benchmark PostgreSQL: %v", err)
		}
		databaseName = fmt.Sprintf("oversync_benchmark_%d_%d", os.Getpid(), sequence)
		if err := managedServer.createDatabase(ctx, databaseName); err != nil {
			b.Fatalf("create benchmark database %s: %v", databaseName, err)
		}
		databaseURL = managedServer.databaseURL(databaseName)
	}

	b.Cleanup(func() {
		cleanupTimeout := operationTimeout
		if cleanupTimeout < 30*time.Second {
			cleanupTimeout = 30 * time.Second
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()

		if svc != nil {
			if err := svc.Close(cleanupCtx); err != nil {
				b.Errorf("close benchmark service: %v", err)
			}
		}
		if pool != nil {
			if externalDatabase {
				if schemaName != "" {
					if err := dropTestSchema(cleanupCtx, pool, schemaName); err != nil {
						b.Errorf("drop benchmark schema: %v", err)
					}
				}
				if err := resetTestSyncSchema(cleanupCtx, pool); err != nil {
					b.Errorf("reset caller-managed benchmark database: %v", err)
				}
			}
			pool.Close()
		}
		if managedServer != nil && databaseName != "" {
			if err := managedServer.dropDatabase(cleanupCtx, databaseName); err != nil {
				b.Errorf("drop benchmark database %s: %v", databaseName, err)
			}
		}
		if externalDatabase {
			callerManagedIntegrationDatabaseMu.Unlock()
		}
	})

	var err error
	pool, err = pgxpool.New(ctx, databaseURL)
	if err != nil {
		b.Fatalf("create benchmark pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		b.Fatalf("ping benchmark database: %v", err)
	}
	if externalDatabase {
		if err := resetTestSyncSchema(ctx, pool); err != nil {
			b.Fatalf("reset caller-managed benchmark database: %v", err)
		}
	}

	scenario = strings.ReplaceAll(scenario, "-", "_")
	schemaName = fmt.Sprintf("audit_bench_%s_%d", scenario, sequence)
	if err := resetTestBusinessSchema(ctx, pool, schemaName); err != nil {
		b.Fatalf("create benchmark schema: %v", err)
	}

	svc, err = newTestRuntimeService(pool, &ServiceConfig{SchemaGenerations: testSchemaGenerationsForRegisteredTables([]RegisteredTable{
		{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
	}), AppName: "oversync-audit-" + scenario,
		RegisteredTables: []RegisteredTable{
			{Schema: schemaName, Table: "users", SyncKeyColumns: []string{"id"}},
			{Schema: schemaName, Table: "files", SyncKeyColumns: []string{"id"}},
		},
		RegisteredTableCatalogRevision: 1,
	}, integrationTestLogger(slog.LevelWarn))
	if err != nil {
		b.Fatalf("create benchmark service: %v", err)
	}
	if err := svc.Bootstrap(ctx); err != nil {
		b.Fatalf("bootstrap benchmark service: %v", err)
	}

	userID := fmt.Sprintf("audit-benchmark-%s-%d", scenario, sequence)
	writer := Actor{UserID: userID, SourceID: "writer"}
	proof := testSchemaGenerationProofForService(svc)
	connect, err := svc.Connect(ctx, writer, proof, &ConnectRequest{HasLocalPendingRows: false})
	if err != nil {
		b.Fatalf("initialize benchmark scope: %v", err)
	}
	if connect.Resolution != "initialize_empty" && connect.Resolution != "remote_authoritative" {
		b.Fatalf("unexpected benchmark connect resolution %q", connect.Resolution)
	}
	reader := Actor{UserID: userID, SourceID: "reader"}
	if _, err := svc.Connect(ctx, reader, proof, &ConnectRequest{HasLocalPendingRows: false}); err != nil {
		b.Fatalf("bind benchmark reader: %v", err)
	}

	fixture := &auditDatabaseBenchmarkFixture{
		ctx:                 ctx,
		pool:                pool,
		svc:                 svc,
		schemaName:          schemaName,
		writer:              writer,
		reader:              reader,
		operationTimeout:    operationTimeout,
		postgresContainerID: "",
	}
	if managedServer != nil {
		fixture.postgresContainerID = managedServer.containerID
	}
	return fixture
}

type auditBenchmarkPayloadShape struct {
	name         string
	payloadBytes int
	binary       bool
}

var auditBenchmarkPayloadShapes = []auditBenchmarkPayloadShape{
	{name: "json_1kib", payloadBytes: 1 << 10},
	{name: "json_64kib", payloadBytes: 64 << 10},
	{name: "binary_1kib", payloadBytes: 1 << 10, binary: true},
}

func auditBenchmarkUUID(sourceBundleID int64, rowOrdinal int) uuid.UUID {
	var id uuid.UUID
	binary.BigEndian.PutUint64(id[:8], uint64(sourceBundleID))
	binary.BigEndian.PutUint64(id[8:], uint64(rowOrdinal+1))
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return id
}

func auditBenchmarkEmail(id uuid.UUID) string {
	return "audit-" + id.String() + "@example.com"
}

func auditBenchmarkTextName(id uuid.UUID, targetPayloadBytes int) string {
	prefix := fmt.Sprintf(`{"id":"%s","name":"`, id)
	suffix := fmt.Sprintf(`","email":"%s"}`, auditBenchmarkEmail(id))
	nameBytes := targetPayloadBytes - len(prefix) - len(suffix)
	if nameBytes < 1 {
		nameBytes = 1
	}
	return strings.Repeat("x", nameBytes)
}

func auditBenchmarkTextPayload(id uuid.UUID, targetPayloadBytes int) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(
		`{"id":"%s","name":"%s","email":"%s"}`,
		id,
		auditBenchmarkTextName(id, targetPayloadBytes),
		auditBenchmarkEmail(id),
	))
}

func auditBenchmarkBinaryValue(sourceBundleID int64, rowOrdinal, byteCount int) []byte {
	value := make([]byte, byteCount)
	for i := range value {
		value[i] = byte(uint64(i) + uint64(rowOrdinal) + uint64(sourceBundleID))
	}
	return value
}

func auditBenchmarkRowsForShape(
	schemaName string,
	sourceBundleID int64,
	rowCount int,
	shape auditBenchmarkPayloadShape,
) ([]PushRequestRow, int64) {
	rows := make([]PushRequestRow, rowCount)
	var payloadBytes int64
	for rowOrdinal := range rows {
		id := auditBenchmarkUUID(sourceBundleID, rowOrdinal)
		row := PushRequestRow{
			Schema:         schemaName,
			Key:            SyncKey{"id": id.String()},
			Op:             OpInsert,
			BaseRowVersion: 0,
		}
		if shape.binary {
			row.Table = "files"
			payload, err := json.Marshal(struct {
				ID   string `json:"id"`
				Name string `json:"name"`
				Data string `json:"data"`
			}{
				ID:   id.String(),
				Name: fmt.Sprintf("Audit binary %d %d", sourceBundleID, rowOrdinal),
				Data: base64.StdEncoding.EncodeToString(
					auditBenchmarkBinaryValue(sourceBundleID, rowOrdinal, shape.payloadBytes),
				),
			})
			if err != nil {
				panic(fmt.Sprintf("marshal deterministic audit benchmark payload: %v", err))
			}
			row.Payload = payload
		} else {
			row.Table = "users"
			row.Payload = auditBenchmarkTextPayload(id, shape.payloadBytes)
		}
		payloadBytes += int64(len(row.Payload))
		rows[rowOrdinal] = row
	}
	return rows, payloadBytes
}

// auditBenchmarkRows preserves the original small-payload helper contract used
// by the audit profile-metrics benchmarks. Capacity profiles use the explicit
// shape-aware helper above.
func auditBenchmarkRows(schemaName string, sourceBundleID int64, rowCount int) []PushRequestRow {
	rows := make([]PushRequestRow, rowCount)
	for rowOrdinal := range rows {
		id := auditBenchmarkUUID(sourceBundleID, rowOrdinal)
		name := fmt.Sprintf("Audit %d %d", sourceBundleID, rowOrdinal)
		rows[rowOrdinal] = PushRequestRow{
			Schema:         schemaName,
			Table:          "users",
			Key:            SyncKey{"id": id.String()},
			Op:             OpInsert,
			BaseRowVersion: 0,
			Payload: json.RawMessage(fmt.Sprintf(
				`{"id":"%s","name":%q,"email":"audit-%d-%d@example.com"}`,
				id,
				name,
				sourceBundleID,
				rowOrdinal,
			)),
		}
	}
	return rows
}

func auditBenchmarkCaptureRows(
	scopeID string,
	sourceBundleID int64,
	rowCount int,
	shape auditBenchmarkPayloadShape,
) (string, []string, [][]any, int64) {
	rows := make([][]any, rowCount)
	var inputBytes int64
	for rowOrdinal := range rows {
		id := auditBenchmarkUUID(sourceBundleID, rowOrdinal)
		if shape.binary {
			value := auditBenchmarkBinaryValue(sourceBundleID, rowOrdinal, shape.payloadBytes)
			rows[rowOrdinal] = []any{
				scopeID,
				id,
				fmt.Sprintf("Audit binary %d %d", sourceBundleID, rowOrdinal),
				value,
			}
			inputBytes += int64(len(value))
			continue
		}

		name := auditBenchmarkTextName(id, shape.payloadBytes)
		rows[rowOrdinal] = []any{scopeID, id, name, auditBenchmarkEmail(id)}
		inputBytes += int64(shape.payloadBytes)
	}
	if shape.binary {
		return "files", []string{"_sync_scope_id", "id", "name", "data"}, rows, inputBytes
	}
	return "users", []string{"_sync_scope_id", "id", "name", "email"}, rows, inputBytes
}

func auditBenchmarkInsertStatement(table pgx.Identifier, columns []string, rows [][]any) (string, []any) {
	columnSQL := make([]string, len(columns))
	for index, column := range columns {
		columnSQL[index] = pgx.Identifier{column}.Sanitize()
	}
	var statement strings.Builder
	statement.WriteString("INSERT INTO ")
	statement.WriteString(table.Sanitize())
	statement.WriteString(" (")
	statement.WriteString(strings.Join(columnSQL, ","))
	statement.WriteString(") VALUES ")
	arguments := make([]any, 0, len(rows)*len(columns))
	parameter := 1
	for rowIndex, row := range rows {
		if rowIndex != 0 {
			statement.WriteByte(',')
		}
		statement.WriteByte('(')
		for columnIndex, value := range row {
			if columnIndex != 0 {
				statement.WriteByte(',')
			}
			fmt.Fprintf(&statement, "$%d", parameter)
			parameter++
			arguments = append(arguments, value)
		}
		statement.WriteByte(')')
	}
	return statement.String(), arguments
}

func auditBenchmarkPush(
	ctx context.Context,
	svc *SyncService,
	actor Actor,
	sourceBundleID int64,
	rows []PushRequestRow,
) (*Bundle, error) {
	connect, err := svc.Connect(ctx, actor, testSchemaGenerationProofForService(svc), &ConnectRequest{HasLocalPendingRows: true})
	if err != nil {
		return nil, err
	}
	requestHash, err := computeGenerationPinnedPushRequestHash(testSchemaGenerationProofForService(svc), rows)
	if err != nil {
		return nil, err
	}
	response, err := svc.PushOneShot(ctx, actor, testSchemaGenerationProofForService(svc), &OneShotPushRequest{
		SourceBundleID:       sourceBundleID,
		CanonicalRequestHash: requestHash,
		InitializationID:     connect.InitializationID,
		Rows:                 rows,
	})
	if err != nil {
		return nil, err
	}
	return response.Bundle, nil
}

func auditBenchmarkPushProfile(rowCount int, shape auditBenchmarkPayloadShape) auditBenchmarkProfile {
	if rowCount == 1 {
		return auditBenchmarkProfileSmoke
	}
	if rowCount == 5_000 && shape.payloadBytes == 64<<10 {
		return auditBenchmarkProfileTop
	}
	return auditBenchmarkProfileFull
}

func auditBenchmarkSeedPullHistory(
	b *testing.B,
	fixture *auditDatabaseBenchmarkFixture,
	bundleCount int,
	rowsPerBundle int,
) {
	b.Helper()

	seedCtx, cancelSeed := context.WithTimeout(fixture.ctx, fixture.operationTimeout)
	defer cancelSeed()
	shape := auditBenchmarkPayloadShapes[0]
	for sourceBundleID := 1; sourceBundleID <= bundleCount; sourceBundleID++ {
		rows, _ := auditBenchmarkRowsForShape(fixture.schemaName, int64(sourceBundleID), rowsPerBundle, shape)
		if _, err := auditBenchmarkPush(seedCtx, fixture.svc, fixture.writer, int64(sourceBundleID), rows); err != nil {
			auditBenchmarkFatalOperation(
				b,
				fmt.Sprintf("seed pull history at bundle %d/%d", sourceBundleID, bundleCount),
				fixture.operationTimeout,
				err,
			)
		}
	}
}

// auditBenchmarkSeedSnapshotRows bypasses registered-table triggers inside one
// test-owned transaction, then COPY-seeds matching authoritative and row_state
// rows. This audit-only path avoids manufacturing up to one million historical
// bundles merely to profile snapshot materialization. ALTER TABLE is
// transactional, so any COPY error rolls trigger state back with the seed.
func auditBenchmarkSeedSnapshotRows(
	ctx context.Context,
	fixture *auditDatabaseBenchmarkFixture,
	rowCount int,
) error {
	if rowCount == 0 {
		return nil
	}

	userPK, err := lookupUserPK(ctx, fixture.pool, fixture.writer.UserID)
	if err != nil {
		return err
	}
	tableID, err := fixture.svc.tableIDForTable(fixture.schemaName, "users")
	if err != nil {
		return err
	}
	tableIdent := pgx.Identifier{fixture.schemaName, "users"}
	tableSQL := tableIdent.Sanitize()

	err = pgx.BeginTxFunc(ctx, fixture.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DISABLE TRIGGER USER`, tableSQL)); err != nil {
			return fmt.Errorf("disable registered-table triggers for snapshot seed: %w", err)
		}

		businessRows, err := tx.CopyFrom(
			ctx,
			tableIdent,
			[]string{"_sync_scope_id", "id", "name", "email"},
			pgx.CopyFromSlice(rowCount, func(rowOrdinal int) ([]any, error) {
				id := auditBenchmarkUUID(0, rowOrdinal)
				return []any{
					fixture.writer.UserID,
					id,
					fmt.Sprintf("Snapshot %d", rowOrdinal),
					auditBenchmarkEmail(id),
				}, nil
			}),
		)
		if err != nil {
			return fmt.Errorf("COPY snapshot business rows: %w", err)
		}
		if businessRows != int64(rowCount) {
			return fmt.Errorf("COPY snapshot business rows wrote %d, want %d", businessRows, rowCount)
		}

		stateRows, err := tx.CopyFrom(
			ctx,
			pgx.Identifier{"sync", "row_state"},
			[]string{"user_pk", "table_id", "key_bytes", "bundle_seq", "deleted"},
			pgx.CopyFromSlice(rowCount, func(rowOrdinal int) ([]any, error) {
				id := auditBenchmarkUUID(0, rowOrdinal)
				keyBytes := make([]byte, len(id))
				copy(keyBytes, id[:])
				return []any{userPK, tableID, keyBytes, int64(0), false}, nil
			}),
		)
		if err != nil {
			return fmt.Errorf("COPY snapshot row_state rows: %w", err)
		}
		if stateRows != int64(rowCount) {
			return fmt.Errorf("COPY snapshot row_state rows wrote %d, want %d", stateRows, rowCount)
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ENABLE TRIGGER USER`, tableSQL)); err != nil {
			return fmt.Errorf("re-enable registered-table triggers after snapshot seed: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	var businessRows, stateRows int64
	err = fixture.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT
			(SELECT COUNT(*) FROM %s WHERE _sync_scope_id = $1),
			(SELECT COUNT(*) FROM sync.row_state WHERE user_pk = $2 AND table_id = $3 AND deleted = FALSE)
	`, tableSQL), fixture.writer.UserID, userPK, tableID).Scan(&businessRows, &stateRows)
	if err != nil {
		return fmt.Errorf("verify snapshot capacity seed: %w", err)
	}
	if businessRows != int64(rowCount) || stateRows != int64(rowCount) {
		return fmt.Errorf(
			"snapshot capacity seed mismatch: business=%d row_state=%d want=%d",
			businessRows,
			stateRows,
			rowCount,
		)
	}
	return nil
}

func BenchmarkAuditDatabasePushCommit(b *testing.B) {
	for _, shape := range auditBenchmarkPayloadShapes {
		b.Run(shape.name, func(b *testing.B) {
			for _, rowCount := range []int{1, 100, 1_000, 5_000} {
				b.Run(fmt.Sprintf("rows_%d", rowCount), func(b *testing.B) {
					requireAuditBenchmarkProfile(b, auditBenchmarkPushProfile(rowCount, shape))
					fixture := newAuditDatabaseBenchmarkFixture(
						b,
						fmt.Sprintf("push_%s_%d", shape.name, rowCount),
					)
					b.ReportAllocs()
					b.ResetTimer()

					var payloadBytesPerOp int64
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						sourceBundleID := int64(i + 1)
						rows, payloadBytes := auditBenchmarkRowsForShape(
							fixture.schemaName,
							sourceBundleID,
							rowCount,
							shape,
						)
						payloadBytesPerOp = payloadBytes
						opCtx, cancelOp := context.WithTimeout(fixture.ctx, fixture.operationTimeout)
						b.StartTimer()

						commit, err := auditBenchmarkPush(opCtx, fixture.svc, fixture.writer, sourceBundleID, rows)
						b.StopTimer()
						cancelOp()
						if err != nil {
							auditBenchmarkFatalOperation(b, "push/capture commit", fixture.operationTimeout, err)
						}
						if commit.RowCount != int64(rowCount) {
							b.Fatalf("committed %d rows, want %d", commit.RowCount, rowCount)
						}
						b.StartTimer()
					}
					b.StopTimer()

					b.SetBytes(payloadBytesPerOp)
					b.ReportMetric(float64(payloadBytesPerOp), "payload_bytes/op")
					b.ReportMetric(float64(rowCount), "rows/op")
					if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
						b.ReportMetric(float64(rowCount*b.N)/elapsed, "rows/s")
					}
				})
			}
		})
	}
}

func BenchmarkAuditDatabaseServerCapture(b *testing.B) {
	for _, shape := range auditBenchmarkPayloadShapes {
		b.Run(shape.name, func(b *testing.B) {
			for _, rowCount := range []int{1, 100, 1_000, 5_000} {
				b.Run(fmt.Sprintf("rows_%d", rowCount), func(b *testing.B) {
					requireAuditBenchmarkProfile(b, auditBenchmarkPushProfile(rowCount, shape))
					fixture := newAuditDatabaseBenchmarkFixture(
						b,
						fmt.Sprintf("capture_%s_%d", shape.name, rowCount),
					)
					manager := NewScopeManager(fixture.svc, ScopeManagerConfig{})
					b.ReportAllocs()
					b.ResetTimer()

					var inputBytesPerOp int64
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						tableName, columns, rows, inputBytes := auditBenchmarkCaptureRows(
							fixture.writer.UserID,
							int64(i+1),
							rowCount,
							shape,
						)
						inputBytesPerOp = inputBytes
						tableIdent := pgx.Identifier{fixture.schemaName, tableName}
						insertSQL, insertArgs := auditBenchmarkInsertStatement(tableIdent, columns, rows)
						opCtx, cancelOp := context.WithTimeout(fixture.ctx, fixture.operationTimeout)
						b.StartTimer()

						result, err := manager.ExecWrite(
							opCtx,
							fixture.writer.UserID,
							ScopeWriteOptions{WriterID: "server-capture", RetryableWriteOptions: retryableWriteOptionsForTest()},
							func(ctx context.Context, tx DatabaseWriteTx) error {
								tag, err := tx.Exec(ctx, insertSQL, insertArgs...)
								if err != nil {
									return err
								}
								if tag.RowsAffected() != int64(rowCount) {
									return fmt.Errorf("captured insert wrote %d rows, want %d", tag.RowsAffected(), rowCount)
								}
								return nil
							},
						)
						b.StopTimer()
						cancelOp()
						if err != nil {
							auditBenchmarkFatalOperation(b, "server capture commit", fixture.operationTimeout, err)
						}
						if result.Bundle.RowCount != int64(rowCount) {
							b.Fatalf("captured bundle row count = %#v, want %d", result.Bundle, rowCount)
						}
						b.StartTimer()
					}
					b.StopTimer()

					b.SetBytes(inputBytesPerOp)
					b.ReportMetric(float64(inputBytesPerOp), "input_bytes/op")
					b.ReportMetric(float64(rowCount), "rows/op")
					if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
						b.ReportMetric(float64(rowCount*b.N)/elapsed, "rows/s")
					}
				})
			}
		})
	}
}

type auditDatabasePullLayout struct {
	name          string
	bundleCount   int
	rowsPerBundle int
}

func BenchmarkAuditDatabasePull(b *testing.B) {
	for _, bundleCount := range []int{1, 10, 100, 1_000} {
		requiredProfile := auditBenchmarkProfileSmoke
		if bundleCount > 10 {
			requiredProfile = auditBenchmarkProfileFull
		}
		auditRunDatabasePullBenchmarkGroup(
			b,
			fmt.Sprintf("bundle_count_%d", bundleCount),
			fmt.Sprintf("pull_%d", bundleCount),
			requiredProfile,
			[]auditDatabasePullLayout{
				{name: "sparse_rows_per_bundle_1", bundleCount: bundleCount, rowsPerBundle: 1},
				{name: "dense_rows_per_bundle_10", bundleCount: bundleCount, rowsPerBundle: 10},
			},
		)
	}
}

func BenchmarkAuditDatabasePullEqualRows(b *testing.B) {
	for _, historyRows := range []int{1, 10, 100, 1_000} {
		requiredProfile := auditBenchmarkProfileSmoke
		if historyRows > 10 {
			requiredProfile = auditBenchmarkProfileFull
		}
		fewLargeBundleCount := 1
		fewLargeRowsPerBundle := historyRows
		if historyRows > 1 {
			fewLargeBundleCount = historyRows / 10
			fewLargeRowsPerBundle = 10
		}
		auditRunDatabasePullBenchmarkGroup(
			b,
			fmt.Sprintf("history_rows_%d", historyRows),
			fmt.Sprintf("pull_equal_rows_%d", historyRows),
			requiredProfile,
			[]auditDatabasePullLayout{
				{name: fmt.Sprintf("many_small_bundles_%d", historyRows), bundleCount: historyRows, rowsPerBundle: 1},
				{name: fmt.Sprintf("few_large_bundles_%d", fewLargeBundleCount), bundleCount: fewLargeBundleCount, rowsPerBundle: fewLargeRowsPerBundle},
			},
		)
	}
}

func auditRunDatabasePullBenchmarkGroup(
	b *testing.B,
	groupName string,
	fixturePrefix string,
	requiredProfile auditBenchmarkProfile,
	layouts []auditDatabasePullLayout,
) {
	b.Helper()
	b.Run(groupName, func(b *testing.B) {
		for _, layout := range layouts {
			b.Run(layout.name, func(b *testing.B) {
				requireAuditBenchmarkProfile(b, requiredProfile)
				fixture := newAuditDatabaseBenchmarkFixture(b, fixturePrefix+"_"+layout.name)
				auditBenchmarkSeedPullHistory(b, fixture, layout.bundleCount, layout.rowsPerBundle)
				historyRows := layout.bundleCount * layout.rowsPerBundle

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					opCtx, cancelOp := context.WithTimeout(fixture.ctx, fixture.operationTimeout)
					b.StartTimer()
					page, err := fixture.svc.ProcessPull(opCtx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), 0, layout.bundleCount, 0)
					b.StopTimer()
					cancelOp()
					if err != nil {
						auditBenchmarkFatalOperation(b, "pull history", fixture.operationTimeout, err)
					}
					pulledRows := 0
					for _, bundle := range page.Bundles {
						pulledRows += len(bundle.Rows)
					}
					if len(page.Bundles) != layout.bundleCount || pulledRows != historyRows {
						b.Fatalf(
							"pulled history = %d bundles/%d rows, want %d/%d",
							len(page.Bundles),
							pulledRows,
							layout.bundleCount,
							historyRows,
						)
					}
					b.StartTimer()
				}
				b.StopTimer()

				b.ReportMetric(float64(layout.bundleCount), "bundles/op")
				b.ReportMetric(float64(historyRows), "rows/op")
				if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
					b.ReportMetric(float64(layout.bundleCount*b.N)/elapsed, "bundles/s")
					b.ReportMetric(float64(historyRows*b.N)/elapsed, "rows/s")
				}
			})
		}
	})
}

func BenchmarkAuditDatabaseSnapshotCreate(b *testing.B) {
	for _, rowCount := range []int{0, 1_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("rows_%d", rowCount), func(b *testing.B) {
			requiredProfile := auditBenchmarkProfileSmoke
			if rowCount >= 100_000 {
				requiredProfile = auditBenchmarkProfileTop
			}
			requireAuditBenchmarkProfile(b, requiredProfile)
			if b.N != 1 {
				b.Fatalf("run this audit benchmark with -benchtime=1x; b.N=%d", b.N)
			}
			fixture := newAuditDatabaseBenchmarkFixture(b, fmt.Sprintf("snapshot_%d", rowCount))

			seedStartedAt := time.Now()
			seedCtx, cancelSeed := context.WithTimeout(fixture.ctx, fixture.operationTimeout)
			seedErr := auditBenchmarkSeedSnapshotRows(seedCtx, fixture, rowCount)
			cancelSeed()
			seedElapsed := time.Since(seedStartedAt)
			if seedErr != nil {
				auditBenchmarkFatalOperation(b, "seed snapshot capacity state", fixture.operationTimeout, seedErr)
			}
			seedRowsPerSecond := float64(0)
			if rowCount > 0 && seedElapsed > 0 {
				seedRowsPerSecond = float64(rowCount) / seedElapsed.Seconds()
			}
			b.Logf("COPY-seeded snapshot authoritative rows=%d in %s", rowCount, seedElapsed)

			var snapshotTableBytesBefore int64
			if err := fixture.pool.QueryRow(fixture.ctx, `SELECT pg_total_relation_size('sync.snapshot_session_rows')`).Scan(&snapshotTableBytesBefore); err != nil {
				b.Fatalf("measure snapshot row table bytes before materialization: %v", err)
			}
			var snapshotWALStart string
			if err := fixture.pool.QueryRow(fixture.ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&snapshotWALStart); err != nil {
				b.Fatalf("read snapshot materialization starting WAL LSN: %v", err)
			}
			var snapshotTableBytesAfterCreate int64
			var snapshotWALBytesAfterCreate float64

			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)
			highWater := startAuditGoMemoryHighWater()
			postgresMemory, err := startAuditContainerMemoryHighWater(fixture.postgresContainerID)
			if err != nil {
				b.Fatalf("start PostgreSQL container memory sampler: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				opCtx, cancelOp := context.WithTimeout(fixture.ctx, fixture.operationTimeout)
				b.StartTimer()
				session, err := fixture.svc.CreateSnapshotSession(opCtx, fixture.reader, testSchemaGenerationProofForService(fixture.svc))
				b.StopTimer()
				cancelOp()
				if err != nil {
					auditBenchmarkFatalOperation(b, "create snapshot session", fixture.operationTimeout, err)
				}
				if session.RowCount != int64(rowCount) {
					b.Fatalf("snapshot materialized %d rows, want %d", session.RowCount, rowCount)
				}
				if err := fixture.pool.QueryRow(fixture.ctx, `
					SELECT pg_total_relation_size('sync.snapshot_session_rows'),
					       pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)
				`, snapshotWALStart).Scan(&snapshotTableBytesAfterCreate, &snapshotWALBytesAfterCreate); err != nil {
					b.Fatalf("measure snapshot materialization storage: %v", err)
				}
				chunkCtx, cancelChunk := context.WithTimeout(fixture.ctx, fixture.operationTimeout)
				chunk, chunkErr := fixture.svc.GetSnapshotChunk(chunkCtx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID, 0, fixture.svc.maxRowsPerSnapshotChunk(), fixture.svc.maxBytesPerSnapshotChunk())
				cancelChunk()
				if chunkErr != nil {
					auditBenchmarkFatalOperation(b, "select snapshot chunk", fixture.operationTimeout, chunkErr)
				}
				if rowCount > 0 && len(chunk.Rows) == 0 {
					b.Fatal("non-empty snapshot returned empty first chunk")
				}

				deleteCtx, cancelDelete := context.WithTimeout(fixture.ctx, fixture.operationTimeout)
				deleteErr := fixture.svc.DeleteSnapshotSession(deleteCtx, fixture.reader, testSchemaGenerationProofForService(fixture.svc), session.SnapshotID)
				cancelDelete()
				if deleteErr != nil {
					auditBenchmarkFatalOperation(b, "delete benchmark snapshot", fixture.operationTimeout, deleteErr)
				}
				b.StartTimer()
			}
			b.StopTimer()
			maxHeap, maxInUse := highWater.finish()
			postgresBaseline, postgresPeak, postgresMemoryErr := postgresMemory.finish()
			if postgresMemoryErr != nil {
				b.Fatalf("sample PostgreSQL container memory: %v", postgresMemoryErr)
			}
			peakHeapDelta := uint64(0)
			if maxHeap > before.HeapAlloc {
				peakHeapDelta = maxHeap - before.HeapAlloc
			}
			peakInUseDelta := uint64(0)
			if maxInUse > before.HeapInuse {
				peakInUseDelta = maxInUse - before.HeapInuse
			}

			b.ReportMetric(float64(rowCount), "rows/op")
			if seedRowsPerSecond > 0 {
				b.ReportMetric(seedRowsPerSecond, "seed_rows/s")
			}
			if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
				b.ReportMetric(float64(rowCount*b.N)/elapsed, "rows/s")
			}
			metrics := fixture.svc.snapshotRuntimeMetricsSnapshot()
			b.ReportMetric(float64(metrics.MaterializationBatchRowsHighWater), "material_batch_rows_high_water")
			b.ReportMetric(float64(metrics.MaterializationBatchBytesHighWater), "material_batch_bytes_high_water")
			b.ReportMetric(float64(metrics.MaterializationPageQueries), "material_page_queries")
			b.ReportMetric(float64(metrics.MaterializationCopyBatches), "material_copy_batches")
			b.ReportMetric(float64(metrics.ChunkQueries), "chunk_queries")
			b.ReportMetric(float64(metrics.ChunkRowsHighWater), "chunk_rows_high_water")
			b.ReportMetric(float64(metrics.ChunkBytesHighWater), "chunk_bytes_high_water")
			b.ReportMetric(float64(metrics.ChunkRetainedRowsHighWater), "chunk_retained_rows_high_water")
			b.ReportMetric(float64(before.HeapAlloc), "baseline_heap_B")
			b.ReportMetric(float64(maxHeap), "peak_heap_B")
			b.ReportMetric(float64(peakHeapDelta), "peak_heap_delta_B")
			b.ReportMetric(float64(peakInUseDelta), "peak_heap_inuse_delta_B")
			b.ReportMetric(float64(postgresBaseline), "postgres_container_baseline_B")
			b.ReportMetric(float64(postgresPeak), "postgres_container_peak_B")
			b.ReportMetric(float64(snapshotTableBytesBefore), "snapshot_table_bytes_before")
			b.ReportMetric(float64(snapshotTableBytesAfterCreate), "snapshot_table_bytes_after_create")
			b.ReportMetric(snapshotWALBytesAfterCreate, "snapshot_wal_bytes_after_create")
			b.Logf(
				"snapshot_create_storage rows=%d table_bytes_before=%d table_bytes_after_create=%d wal_bytes_after_create=%.0f material_page_queries=%d material_copy_batches=%d chunk_queries=%d",
				rowCount,
				snapshotTableBytesBefore,
				snapshotTableBytesAfterCreate,
				snapshotWALBytesAfterCreate,
				metrics.MaterializationPageQueries,
				metrics.MaterializationCopyBatches,
				metrics.ChunkQueries,
			)
		})
	}
}
