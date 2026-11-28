// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// A client whose upload source the server can no longer sequence keeps its pending edits, requests a
// replacement source together with its recovery snapshot, and uploads those edits as the replacement's
// first bundle. These tests pin the server half of that exit.

func reuploadUserPayload(id uuid.UUID, name string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"id":%q,"name":%q,"email":"%s@example.com"}`, id, name, strings.ToLower(strings.ReplaceAll(name, " ", "-"))))
}

func (f *snapshotSessionFixture) userName(t *testing.T, id uuid.UUID) (string, bool) {
	t.Helper()
	var name string
	err := f.pool.QueryRow(f.ctx, fmt.Sprintf(`SELECT name FROM %s.users WHERE id = $1`, pgx.Identifier{f.schemaName}.Sanitize()), id).Scan(&name)
	if err == pgx.ErrNoRows {
		return "", false
	}
	require.NoError(t, err)
	return name, true
}

func TestSourceRecoveryReupload_ReplacementAcceptsCarriedIntentAfterRefusedResolve(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "recovery_reupload", snapshotSessionFixtureOptions{})
	proof := testSchemaGenerationProofForService(fixture.svc)
	oldActor := fixture.actor("reupload-old")
	newActor := fixture.actor("reupload-new")
	editedID, createdID := uuid.New(), uuid.New()
	fixture.pushUserAs(t, oldActor, 1, editedID, "Alpha")

	// The client froze source bundle 3, but the server only acknowledged bundle 1: the frozen request
	// never arrived and the sequence cannot continue. Resolve refuses without inventing an outcome.
	frozenRows := []PushRequestRow{
		{Schema: fixture.schemaName, Table: "users", Key: SyncKey{"id": editedID.String()}, Op: OpUpdate, BaseRowVersion: 1, Payload: reuploadUserPayload(editedID, "Edited offline")},
		{Schema: fixture.schemaName, Table: "users", Key: SyncKey{"id": createdID.String()}, Op: OpInsert, Payload: reuploadUserPayload(createdID, "Created offline")},
	}
	frozenHash, err := computeGenerationPinnedPushRequestHash(proof, frozenRows)
	require.NoError(t, err)
	frozen := PushOutcomeResolveRequest{SourceBundleID: 3, CanonicalRequestHash: frozenHash}
	_, err = fixture.svc.ResolvePushOutcome(fixture.ctx, oldActor, proof, frozen)
	outcomeCode(t, err, "source_sequence_out_of_order")
	require.Zero(t, fixture.countRows(t, `SELECT count(*) FROM sync.push_outcomes WHERE source_id = $1`, oldActor.SourceID),
		"a refused resolve leaves no terminal evidence behind")

	// The replacement is admitted because the old source holds no unacknowledged outcome.
	session, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, oldActor, proof, &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{PreviousSourceID: oldActor.SourceID, NewSourceID: newActor.SourceID, Reason: "source_sequence_out_of_order"},
	})
	require.NoError(t, err)
	snapshotRows, _ := collectSnapshotChunkRows(t, fixture.ctx, fixture.svc, newActor, session.SnapshotID, 10)
	require.Len(t, snapshotRows, 1)
	require.Equal(t, editedID.String(), snapshotRows[0].Key["id"])
	installedVersion := snapshotRows[0].RowVersion

	// The retired source can never settle the frozen request or acknowledge anything again, so the
	// client is right to release that request once its replacement snapshot exists. The frozen
	// sequence stays out of order, and even the next sequence in order is refused as retired.
	_, err = fixture.svc.ResolvePushOutcome(fixture.ctx, oldActor, proof, frozen)
	outcomeCode(t, err, "source_sequence_out_of_order")
	var retired *SourceRetiredError
	_, err = fixture.svc.ResolvePushOutcome(fixture.ctx, oldActor, proof, PushOutcomeResolveRequest{SourceBundleID: 2, CanonicalRequestHash: frozenHash})
	require.ErrorAs(t, err, &retired)
	require.Equal(t, newActor.SourceID, retired.ReplacedBySourceID)
	require.Zero(t, fixture.countRows(t, `SELECT count(*) FROM sync.push_outcomes WHERE source_id = $1`, oldActor.SourceID))
	require.Error(t, fixture.svc.AcknowledgePushOutcome(fixture.ctx, oldActor, proof, PushOutcomeAckRequest{
		SourceBundleID: 3, CanonicalRequestHash: frozenHash, OutcomeHash: strings.Repeat("a", 64),
	}))
	name, found := fixture.userName(t, editedID)
	require.True(t, found)
	require.Equal(t, "Alpha", name, "nothing from the refused request reached business state")

	// The carried edits are rebased onto the installed snapshot versions and become the replacement
	// source's first bundle.
	carried := []PushRequestRow{
		{Schema: fixture.schemaName, Table: "users", Key: SyncKey{"id": editedID.String()}, Op: OpUpdate, BaseRowVersion: installedVersion, Payload: reuploadUserPayload(editedID, "Edited offline")},
		{Schema: fixture.schemaName, Table: "users", Key: SyncKey{"id": createdID.String()}, Op: OpInsert, Payload: reuploadUserPayload(createdID, "Created offline")},
	}
	bundle, err := pushRowsOneShot(t, fixture.ctx, fixture.svc, newActor, 1, carried)
	require.NoError(t, err)
	require.NotNil(t, bundle)
	acknowledgeRetainedOutcomeForTest(t, fixture.ctx, fixture.svc, newActor, proof)

	name, found = fixture.userName(t, editedID)
	require.True(t, found)
	require.Equal(t, "Edited offline", name)
	name, found = fixture.userName(t, createdID)
	require.True(t, found)
	require.Equal(t, "Created offline", name)

	userPK, err := lookupUserPK(fixture.ctx, fixture.pool, fixture.userID)
	require.NoError(t, err)
	var state string
	var maxCommitted int64
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `SELECT state, max_committed_source_bundle_id FROM sync.source_state WHERE user_pk = $1 AND source_id = $2`, userPK, newActor.SourceID).Scan(&state, &maxCommitted))
	require.Equal(t, sourceStateActive, state)
	require.Equal(t, int64(1), maxCommitted)
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `SELECT state FROM sync.source_state WHERE user_pk = $1 AND source_id = $2`, userPK, oldActor.SourceID).Scan(&state))
	require.Equal(t, sourceStateRetired, state)
}

func TestSourceRecoveryReupload_CarriedEditOnRowChangedAfterSnapshotConflictsInsteadOfOverwriting(t *testing.T) {
	t.Parallel()
	fixture := newSnapshotSessionFixture(t, "reupload_conflict", snapshotSessionFixtureOptions{})
	proof := testSchemaGenerationProofForService(fixture.svc)
	oldActor := fixture.actor("reupload-conflict-old")
	newActor := fixture.actor("reupload-conflict-new")
	rowID := uuid.New()
	fixture.pushUserAs(t, oldActor, 1, rowID, "Alpha")

	session, err := fixture.svc.CreateSnapshotSessionWithRequest(fixture.ctx, oldActor, proof, &SnapshotSessionCreateRequest{
		SourceReplacement: &SnapshotSourceReplacement{PreviousSourceID: oldActor.SourceID, NewSourceID: newActor.SourceID, Reason: "source_retired"},
	})
	require.NoError(t, err)
	snapshotRows, _ := collectSnapshotChunkRows(t, fixture.ctx, fixture.svc, newActor, session.SnapshotID, 10)
	require.Len(t, snapshotRows, 1)
	installedVersion := snapshotRows[0].RowVersion

	// Another device changes the row after the snapshot boundary.
	_, err = pushRowsOneShot(t, fixture.ctx, fixture.svc, fixture.writer, 1, []PushRequestRow{{
		Schema: fixture.schemaName, Table: "users", Key: SyncKey{"id": rowID.String()}, Op: OpUpdate, BaseRowVersion: installedVersion, Payload: reuploadUserPayload(rowID, "Changed remotely"),
	}})
	require.NoError(t, err)
	acknowledgeRetainedOutcomeForTest(t, fixture.ctx, fixture.svc, fixture.writer, proof)

	// The carried edit still names the snapshot version, so the server reports an ordinary conflict
	// for the client resolver instead of overwriting the newer remote change.
	_, err = pushRowsOneShot(t, fixture.ctx, fixture.svc, newActor, 1, []PushRequestRow{{
		Schema: fixture.schemaName, Table: "users", Key: SyncKey{"id": rowID.String()}, Op: OpUpdate, BaseRowVersion: installedVersion, Payload: reuploadUserPayload(rowID, "Edited offline"),
	}})
	var conflict *PushConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, installedVersion, conflict.Conflict.BaseRowVersion)
	require.Greater(t, conflict.Conflict.ServerRowVersion, installedVersion)
	name, found := fixture.userName(t, rowID)
	require.True(t, found)
	require.Equal(t, "Changed remotely", name)
}
