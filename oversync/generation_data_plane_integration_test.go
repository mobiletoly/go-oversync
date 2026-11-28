// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func pushRowsWithGeneration(
	t *testing.T,
	ctx context.Context,
	service *SyncService,
	actor Actor,
	proof SchemaGenerationProof,
	sourceBundleID int64,
	rows []PushRequestRow,
) (*Bundle, string, error) {
	t.Helper()
	hash, err := computeGenerationPinnedPushRequestHash(proof, rows)
	if err != nil {
		return nil, "", err
	}
	response, err := service.PushOneShot(ctx, actor, proof, &OneShotPushRequest{
		SourceBundleID: sourceBundleID, CanonicalRequestHash: hash, Rows: rows,
	})
	if err != nil {
		return nil, "", err
	}
	if response.Outcome.Status == "rejected" && response.Outcome.Rejection != nil {
		if response.Outcome.Rejection.Conflict != nil {
			return nil, "", &PushConflictError{Conflict: response.Outcome.Rejection.Conflict}
		}
		return nil, "", fmt.Errorf("terminal push rejection: %s", response.Outcome.Rejection.Code)
	}
	return response.Bundle, "", nil
}

func TestGenerationVisibleDataPlane_DifferingViewsPushReplayPullSnapshotAndCoverage(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)
	schema := "generation_plane_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schema))
	t.Cleanup(func() { _ = dropTestSchema(ctx, pool, schema) })
	schemaID := pgx.Identifier{schema}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		ALTER TABLE %s.users ALTER COLUMN email SET DEFAULT 'hidden@example.com';
		CREATE TABLE %s.audit_events (
			_sync_scope_id TEXT NOT NULL,
			id UUID NOT NULL,
			detail TEXT NOT NULL,
			PRIMARY KEY (_sync_scope_id, id)
		);
		CREATE FUNCTION %s.normalize_and_audit_user() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			NEW.name := upper(NEW.name);
			INSERT INTO %s.audit_events(_sync_scope_id,id,detail)
			VALUES (NEW._sync_scope_id,NEW.id,'normalized');
			RETURN NEW;
		END $$;
		CREATE TRIGGER normalize_and_audit_user BEFORE INSERT ON %s.users
		FOR EACH ROW EXECUTE FUNCTION %s.normalize_and_audit_user();
		CREATE FUNCTION %s.suppress_named_user() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.name = 'SUPPRESS' THEN
				INSERT INTO %s.audit_events(_sync_scope_id,id,detail)
				VALUES (NEW._sync_scope_id,NEW.id,'must-rollback');
				RETURN NULL;
			END IF;
			RETURN NEW;
		END $$;
		CREATE TRIGGER aa_suppress_named_user BEFORE INSERT ON %s.users
		FOR EACH ROW EXECUTE FUNCTION %s.suppress_named_user()
	`, schemaID, schemaID, schemaID, schemaID, schemaID, schemaID, schemaID, schemaID, schemaID, schemaID))
	require.NoError(t, err)

	registered := []RegisteredTable{
		{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}},
		{Schema: schema, Table: "audit_events", SyncKeyColumns: []string{"id"}},
	}
	oldUsers := GenerationTable{Schema: schema, Table: "users", Columns: []GenerationColumn{
		{Name: "id", WireType: GenerationWireTypeUUID},
		{Name: "name", WireType: GenerationWireTypeText},
	}}
	newUsers := GenerationTable{Schema: schema, Table: "users", Columns: []GenerationColumn{
		{Name: "email", WireType: GenerationWireTypeText},
		{Name: "id", WireType: GenerationWireTypeUUID},
		{Name: "name", WireType: GenerationWireTypeText},
	}}
	newAudit := GenerationTable{Schema: schema, Table: "audit_events", Columns: []GenerationColumn{
		{Name: "detail", WireType: GenerationWireTypeText},
		{Name: "id", WireType: GenerationWireTypeUUID},
	}}
	serviceConfig := func() *ServiceConfig {
		return &ServiceConfig{
			RegisteredTables: registered, RegisteredTableCatalogRevision: 1,
			SchemaGenerations: []SchemaGeneration{{ID: 1, Tables: []GenerationTable{oldUsers}}, {ID: 2, Tables: []GenerationTable{newUsers, newAudit}}},
			AppName:           "generation-visible-data-plane",
		}
	}
	service := newBootstrappedIntegrationService(t, ctx, pool, serviceConfig(), nil)
	oldProof, newProof := proofForGeneration(t, service, 1), proofForGeneration(t, service, 2)
	userID := "generation-visible-user"
	oldActor := Actor{UserID: userID, SourceID: "old-client"}
	newActor := Actor{UserID: userID, SourceID: "new-client"}
	connected, err := service.Connect(ctx, oldActor, oldProof, &ConnectRequest{HasLocalPendingRows: false})
	require.NoError(t, err)
	require.Equal(t, "initialize_empty", connected.Resolution)
	connected, err = service.Connect(ctx, newActor, newProof, &ConnectRequest{HasLocalPendingRows: false})
	require.NoError(t, err)
	require.Equal(t, "remote_authoritative", connected.Resolution)

	rowID := uuid.NewString()
	request := []PushRequestRow{{
		Schema: schema, Table: "users", Key: SyncKey{"id": rowID}, Op: OpInsert,
		Payload: json.RawMessage(fmt.Sprintf(`{"id":%q,"name":"Bob"}`, rowID)),
	}}
	committed, _, err := pushRowsWithGeneration(t, ctx, service, oldActor, oldProof, 1, request)
	require.NoError(t, err)
	require.Equal(t, int64(1), committed.RowCount)
	require.Positive(t, committed.ByteCount)

	requestHash, err := computeGenerationPinnedPushRequestHash(oldProof, request)
	require.NoError(t, err)
	replay, err := service.PushOneShot(ctx, oldActor, oldProof, &OneShotPushRequest{
		SourceBundleID: 1, CanonicalRequestHash: requestHash, Rows: request,
	})
	require.NoError(t, err)
	require.NotNil(t, replay.Outcome)
	require.Equal(t, "committed", replay.Outcome.Status)
	require.Equal(t, committed.BundleSeq, replay.Outcome.Committed.BundleSeq)
	require.Equal(t, committed.RowCount, replay.Outcome.Committed.RowCount)
	require.Equal(t, committed.BundleHash, replay.Outcome.Committed.BundleHash)
	acknowledgeRetainedOutcomeForTest(t, ctx, service, oldActor, oldProof)

	oldPull, err := service.ProcessPull(ctx, oldActor, oldProof, 0, 10, 0)
	require.NoError(t, err)
	require.Len(t, oldPull.Bundles, 1)
	require.Equal(t, committed.ByteCount, oldPull.Bundles[0].ByteCount)
	require.Len(t, oldPull.Bundles[0].Rows, 1)
	require.Empty(t, oldPull.Bundles[0].CanonicalRequestHash)
	require.JSONEq(t, fmt.Sprintf(`{"id":%q,"name":"BOB"}`, rowID), string(oldPull.Bundles[0].Rows[0].Payload))

	newPull, err := service.ProcessPull(ctx, newActor, newProof, 0, 10, 0)
	require.NoError(t, err)
	require.Len(t, newPull.Bundles, 1)
	require.Equal(t, int64(2), newPull.Bundles[0].RowCount)
	require.Len(t, newPull.Bundles[0].Rows, 2)
	rowsByTable := make(map[string]BundleRow, 2)
	for _, row := range newPull.Bundles[0].Rows {
		rowsByTable[row.Table] = row
	}
	require.Contains(t, string(rowsByTable["users"].Payload), `"email":"hidden@example.com"`)
	require.Equal(t, "audit_events", rowsByTable["audit_events"].Table)

	for _, snapshotCase := range []struct {
		actor    Actor
		proof    SchemaGenerationProof
		wantRows int64
	}{
		{actor: oldActor, proof: oldProof, wantRows: 1},
		{actor: newActor, proof: newProof, wantRows: 2},
	} {
		session, err := service.CreateSnapshotSession(ctx, snapshotCase.actor, snapshotCase.proof)
		require.NoError(t, err)
		require.Equal(t, snapshotCase.wantRows, session.RowCount)
		chunk, err := service.GetSnapshotChunk(ctx, snapshotCase.actor, snapshotCase.proof, session.SnapshotID, 0, 100, 1<<20)
		require.NoError(t, err)
		require.Len(t, chunk.Rows, int(snapshotCase.wantRows))
	}

	// A stale write through the older view receives only that view's
	// authoritative columns. The generation-2-only email cannot escape through
	// the exported typed error or its JSON representation.
	_, _, err = pushRowsWithGeneration(t, ctx, service, oldActor, oldProof, 2, []PushRequestRow{{
		Schema: schema, Table: "users", Key: SyncKey{"id": rowID}, Op: OpUpdate, BaseRowVersion: 0,
		Payload: json.RawMessage(fmt.Sprintf(`{"id":%q,"name":"stale"}`, rowID)),
	}})
	var conflictErr *PushConflictError
	require.ErrorAs(t, err, &conflictErr)
	require.NotContains(t, string(conflictErr.Conflict.ServerRow), "email")
	require.JSONEq(t, fmt.Sprintf(`{"id":%q,"name":"BOB"}`, rowID), string(conflictErr.Conflict.ServerRow))
	conflictWire, err := json.Marshal(conflictErr)
	require.NoError(t, err)
	require.NotContains(t, string(conflictWire), "hidden@example.com")
	acknowledgeRetainedOutcomeForTest(t, ctx, service, oldActor, oldProof)

	// A generation-2-only audit write becomes an explicit empty envelope for
	// the old client and still advances its stable canonical checkpoint.
	auditID := uuid.NewString()
	_, _, err = pushRowsWithGeneration(t, ctx, service, newActor, newProof, 1, []PushRequestRow{{
		Schema: schema, Table: "audit_events", Key: SyncKey{"id": auditID}, Op: OpInsert,
		Payload: json.RawMessage(fmt.Sprintf(`{"detail":"new-only","id":%q}`, auditID)),
	}})
	require.NoError(t, err)
	emptyPull, err := service.ProcessPull(ctx, oldActor, oldProof, committed.BundleSeq, 10, 0)
	require.NoError(t, err)
	require.Len(t, emptyPull.Bundles, 1)
	require.Equal(t, int64(0), emptyPull.Bundles[0].RowCount)
	require.Equal(t, int64(2), emptyPull.Bundles[0].ByteCount)
	require.Empty(t, emptyPull.Bundles[0].Rows)
	require.Equal(t, emptyPull.StableBundleSeq, emptyPull.Bundles[0].BundleSeq)

	// The suppressing trigger creates an unrelated hidden effect. Coverage is
	// checked inside the one-shot transaction, so both it and the user mutation
	// roll back without publishing a replayable committed bundle.
	suppressedID := uuid.NewString()
	suppressed := []PushRequestRow{{
		Schema: schema, Table: "users", Key: SyncKey{"id": suppressedID}, Op: OpInsert,
		Payload: json.RawMessage(fmt.Sprintf(`{"id":%q,"name":"SUPPRESS"}`, suppressedID)),
	}}
	hash, err := computeGenerationPinnedPushRequestHash(oldProof, suppressed)
	require.NoError(t, err)
	_, err = service.PushOneShot(ctx, oldActor, oldProof, &OneShotPushRequest{SourceBundleID: 3, CanonicalRequestHash: hash, Rows: suppressed})
	var coverageErr *PushEffectCoverageError
	require.ErrorAs(t, err, &coverageErr)

	var users, rolledBackAudit int
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.users WHERE _sync_scope_id=$1 AND id=$2`, schemaID), userID, suppressedID).Scan(&users))
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.audit_events WHERE _sync_scope_id=$1 AND id=$2`, schemaID), userID, suppressedID).Scan(&rolledBackAudit))
	require.Zero(t, users)
	require.Zero(t, rolledBackAudit)

	// Restarting the server does not create a staged-upload recovery path. The
	// same rejected one-shot remains non-committed and is evaluated afresh.
	require.NoError(t, service.Close(ctx))
	restarted := newBootstrappedIntegrationService(t, ctx, pool, serviceConfig(), nil)
	_, err = restarted.PushOneShot(ctx, oldActor, oldProof, &OneShotPushRequest{SourceBundleID: 3, CanonicalRequestHash: hash, Rows: suppressed})
	require.ErrorAs(t, err, &coverageErr)
	var committedCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM sync.bundle_log
		WHERE user_pk=(SELECT user_pk FROM sync.user_state WHERE user_id=$1)
		  AND source_id=$2 AND source_bundle_id=3
	`, userID, oldActor.SourceID).Scan(&committedCount))
	require.Zero(t, committedCount)
}

// This supporting protocol case deliberately omits a visible wire member. SDK
// full-row encoders do not provide an equivalent omitted-visible-field test.
func TestGenerationVisibleDataPlane_OmittedVisibleFieldsPreserveValuesAndUseDefaults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := newIntegrationTestPool(t, ctx)
	schema := "generation_omission_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	require.NoError(t, resetTestBusinessSchema(ctx, pool, schema))
	t.Cleanup(func() { _ = dropTestSchema(context.Background(), pool, schema) })
	schemaID := pgx.Identifier{schema}.Sanitize()
	_, err := pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s.users ALTER COLUMN email SET DEFAULT 'visible-default'`, schemaID))
	require.NoError(t, err)
	table := GenerationTable{Schema: schema, Table: "users", Columns: []GenerationColumn{{Name: "id", WireType: GenerationWireTypeUUID}, {Name: "name", WireType: GenerationWireTypeText}, {Name: "email", WireType: GenerationWireTypeText}}}
	service := newBootstrappedIntegrationService(t, ctx, pool, &ServiceConfig{RegisteredTables: []RegisteredTable{{Schema: schema, Table: "users", SyncKeyColumns: []string{"id"}}}, RegisteredTableCatalogRevision: 1, SchemaGenerations: []SchemaGeneration{{ID: 1, Tables: []GenerationTable{table}}}, AppName: "visible-omission"}, nil)
	proof := proofForGeneration(t, service, 1)
	actor := Actor{UserID: "visible-omission", SourceID: "wire-client"}
	_, err = service.Connect(ctx, actor, proof, &ConnectRequest{HasLocalPendingRows: false})
	require.NoError(t, err)
	id := uuid.NewString()
	first, _, err := pushRowsWithGeneration(t, ctx, service, actor, proof, 1, []PushRequestRow{{Schema: schema, Table: "users", Key: SyncKey{"id": id}, Op: OpInsert, Payload: json.RawMessage(fmt.Sprintf(`{"id":%q,"name":"insert"}`, id))}})
	require.NoError(t, err)
	var name, email string
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT name,email FROM %s.users WHERE id=$1`, schemaID), id).Scan(&name, &email))
	require.Equal(t, "insert", name)
	require.Equal(t, "visible-default", email)
	acknowledgeRetainedOutcomeForTest(t, ctx, service, actor, proof)
	_, _, err = pushRowsWithGeneration(t, ctx, service, actor, proof, 2, []PushRequestRow{{Schema: schema, Table: "users", Key: SyncKey{"id": id}, Op: OpUpdate, BaseRowVersion: first.BundleSeq, Payload: json.RawMessage(fmt.Sprintf(`{"id":%q,"email":"explicit"}`, id))}})
	require.NoError(t, err)
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`SELECT name,email FROM %s.users WHERE id=$1`, schemaID), id).Scan(&name, &email))
	require.Equal(t, "insert", name)
	require.Equal(t, "explicit", email)
}
