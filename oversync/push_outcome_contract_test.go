// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// UPDATE_PUSH_OUTCOME_LAYOUT emits the new objects only from the authored fresh
// schema. Existing manifest facts are not rewritten from test failures.
func TestOutcomeLayoutManifest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := newIntegrationTestPool(t, ctx)
	svc := &SyncService{config: &ServiceConfig{MaxPushOutcomeDecodedBytes: int64(defaultMaxBytesPerBundle) + 8192}, logger: integrationTestLogger(-4)}
	require.NoError(t, pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error { return svc.createFreshSyncLayoutInTx(ctx, tx) }))
	facts, err := loadManagedLayoutFacts(ctx, pool)
	require.NoError(t, err)
	var selected []managedLayoutFact
	for _, fact := range facts {
		if strings.HasPrefix(fact.Identity, "sync.push_") || strings.Contains(fact.Identity, ".push_outcome_lifecycle_guard") {
			selected = append(selected, fact)
		}
	}
	selected, err = normalizeManagedLayoutFacts(selected)
	require.NoError(t, err)
	if path := os.Getenv("UPDATE_PUSH_OUTCOME_LAYOUT"); path != "" {
		raw, err := json.MarshalIndent(selected, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, append(raw, '\n'), 0644))
		t.Log("emitted new authored outcome layout facts", len(selected))
		return
	}
	expected, err := normalizeManagedLayoutFacts(pushOutcomeManagedLayoutFacts)
	require.NoError(t, err)
	require.Equal(t, expected, selected)
}

// Acknowledged terminal operations must remain consumed across every write owner.
func TestOutcomeWrapperConsumedSequence(t *testing.T) {
	outcomeImages(t, func(t *testing.T, ctx context.Context, f *outcomeFixture) {
		req := f.request(t, 1, f.userRow(uuid.New(), "cancelled"))
		cancelled, err := f.svc.ResolvePushOutcome(ctx, f.writer, testSchemaGenerationProofForService(f.svc), pushOperationRequest(req))
		require.NoError(t, err)
		f.ack(t, ctx, f.writer, cancelled.Outcome)
		invoked := false
		id := uuid.New()
		err = f.svc.WithinSyncBundle(ctx, f.writer, BundleSource{SourceID: f.writer.SourceID, SourceBundleID: 1, CanonicalRequestHash: req.CanonicalRequestHash}, retryableBundleWriteOptionsForTest(), func(ctx context.Context, tx DatabaseWriteTx) error {
			invoked = true
			_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.users (id,name,email) VALUES ($1,'must-not-run',$2)`, pgx.Identifier{f.schemaName}.Sanitize()), id, id.String()+"@example.invalid")
			return err
		})
		require.Error(t, err, "trusted wrapper must not commit an acknowledged cancelled operation")
		require.False(t, invoked, "cross-owner sequence check must precede the callback")
	})
}

// TRUNCATE injects corruption only into this run-owned database. It leaves the
// managed schema/guards intact; normal DELETE is deliberately forbidden. The
// actual replacement API must detect the missing marker before any mutation.
func TestOutcomeReplacementOwnershipCorruption(t *testing.T) {
	outcomeImages(t, func(t *testing.T, ctx context.Context, f *outcomeFixture) {
		proof := testSchemaGenerationProofForService(f.svc)
		req := &SnapshotSessionCreateRequest{SourceReplacement: &SnapshotSourceReplacement{PreviousSourceID: f.writer.SourceID, NewSourceID: "replacement", Reason: "history_pruned"}}
		facts, err := loadManagedLayoutFacts(ctx, f.pool)
		require.NoError(t, err)
		f.query(t, ctx, `TRUNCATE sync.push_source_progress CASCADE`)
		assertRejected := func() {
			t.Helper()
			before := f.durablePushState(t, ctx, f.writer)
			var stateBefore string
			require.NoError(t, f.pool.QueryRow(ctx, `SELECT jsonb_build_array((SELECT jsonb_agg(s ORDER BY source_id) FROM sync.source_state s),(SELECT jsonb_agg(b ORDER BY source_id) FROM sync.source_generation_bindings b),(SELECT jsonb_agg(p ORDER BY source_id) FROM sync.push_source_progress p),(SELECT jsonb_agg(s ORDER BY snapshot_id) FROM sync.snapshot_sessions s))::text`).Scan(&stateBefore))
			_, err := f.svc.CreateSnapshotSessionWithRequest(ctx, f.writer, proof, req)
			var corrupt *SourceGenerationError
			require.ErrorAs(t, err, &corrupt)
			require.Equal(t, SourceGenerationStateCorrupt, corrupt.Code)
			require.Equal(t, before, f.durablePushState(t, ctx, f.writer))
			var stateAfter string
			require.NoError(t, f.pool.QueryRow(ctx, `SELECT jsonb_build_array((SELECT jsonb_agg(s ORDER BY source_id) FROM sync.source_state s),(SELECT jsonb_agg(b ORDER BY source_id) FROM sync.source_generation_bindings b),(SELECT jsonb_agg(p ORDER BY source_id) FROM sync.push_source_progress p),(SELECT jsonb_agg(s ORDER BY snapshot_id) FROM sync.snapshot_sessions s))::text`).Scan(&stateAfter))
			require.Equal(t, stateBefore, stateAfter)
		}
		assertRejected()
		// Restore the exact zero marker, make a legal replacement, then corrupt
		// only its successor marker to exercise the existing replacement replay.
		f.query(t, ctx, `INSERT INTO sync.push_source_progress(user_pk,source_id) SELECT user_pk,source_id FROM sync.source_state`)
		_, err = f.svc.CreateSnapshotSessionWithRequest(ctx, f.writer, proof, req)
		require.NoError(t, err)
		f.query(t, ctx, `TRUNCATE sync.push_source_progress CASCADE; INSERT INTO sync.push_source_progress(user_pk,source_id) SELECT user_pk,source_id FROM sync.source_state WHERE source_id <> 'replacement'`)
		assertRejected()
		after, err := loadManagedLayoutFacts(ctx, f.pool)
		require.NoError(t, err)
		require.Equal(t, facts, after, "corruption injection must not alter schema or disable preflight guards")
	})
}

func TestRecoveryProtocol_OutcomeConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name    string
		limit   int64
		invalid bool
	}{
		{"default", 0, false}, {"minimum", 16384 + 8192, false}, {"maximum", int64(maxMaxBytesPerBundle) + 8192, false}, {"below complete envelope allowance", 16384 + 8191, true}, {"above ceiling", int64(maxMaxBytesPerBundle) + 8193, true}, {"negative", -1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &ServiceConfig{MaxBytesPerBundle: 16384, MaxPushOutcomeDecodedBytes: tc.limit}
			err := normalizeHistoryProjectionConfig(cfg)
			if tc.invalid {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.GreaterOrEqual(t, cfg.MaxPushOutcomeDecodedBytes, int64(16384+8192))
			}
		})
	}
	for _, tc := range []struct {
		name, sqlState, category string
		invalid                  bool
	}{
		{"declared application", "P0001", "application_rejected", false},
		{"conflict category needs real conflict", "P0001", "push_conflict", true},
		{"infrastructure is not application rejection", "53100", "storage_full", true},
		{"private category invalid", "P0001", "private details", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateApplicationWriteRejections([]ApplicationWriteRejection{{SQLState: tc.sqlState, Category: tc.category}})
			if tc.invalid {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Explicitly acknowledge only where a test intends to start a successor. Keeping
// this outside upload helpers prevents old tests from hiding missing handshakes.
func acknowledgeRetainedOutcomeForTest(t *testing.T, ctx context.Context, svc *SyncService, actor Actor, proof SchemaGenerationProof) {
	t.Helper()
	var raw []byte
	require.NoError(t, svc.pool.QueryRow(ctx, `SELECT o.envelope FROM sync.push_outcomes o JOIN sync.user_state u USING(user_pk) WHERE u.user_id=$1 AND o.source_id=$2`, actor.UserID, actor.SourceID).Scan(&raw))
	var o PushOutcome
	require.NoError(t, json.Unmarshal(raw, &o))
	require.NoError(t, svc.AcknowledgePushOutcome(ctx, actor, proof, PushOutcomeAckRequest{SourceBundleID: o.SourceBundleID, CanonicalRequestHash: o.CanonicalRequestHash, OutcomeHash: o.OutcomeHash}))
}

func requireSourceOwnershipConflict(t *testing.T, err error) {
	t.Helper()
	var conflict *SourceGenerationError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, SourceGenerationConflict, conflict.Code)
	require.Equal(t, "source_id", conflict.Field)
}

func (f *outcomeFixture) rejectTrustedClientSource(t *testing.T, ctx context.Context, actor Actor) {
	t.Helper()
	before := f.durablePushState(t, ctx, actor)
	called := false
	callback := func(context.Context, DatabaseWriteTx) error {
		called = true
		return fmt.Errorf("cross-owner callback must not run")
	}
	for _, seq := range []int64{1, 2} {
		err := f.svc.WithinSyncBundle(ctx, actor, BundleSource{SourceID: actor.SourceID, SourceBundleID: seq, CanonicalRequestHash: strings.Repeat("a", 64)}, retryableBundleWriteOptionsForTest(), callback)
		requireSourceOwnershipConflict(t, err)
		require.False(t, called)
		require.Equal(t, before, f.durablePushState(t, ctx, actor))
	}
	_, err := NewScopeManager(f.svc, ScopeManagerConfig{}).ExecWrite(ctx, actor.UserID, ScopeWriteOptions{WriterID: actor.SourceID, RetryableWriteOptions: retryableWriteOptionsForTest()}, callback)
	requireSourceOwnershipConflict(t, err)
	require.False(t, called)
	require.Equal(t, before, f.durablePushState(t, ctx, actor))
}

// This runs in the required LifecycleBinding family on both PostgreSQL versions.
func (f *outcomeFixture) checkSourceOwnership(t *testing.T, ctx context.Context) {
	t.Helper()
	proof := testSchemaGenerationProofForService(f.svc)
	var clients []Actor
	for _, status := range []string{"zero", "cancelled", "rejected", "committed"} {
		actor := Actor{UserID: f.writer.UserID, SourceID: "client-" + status}
		resolveConnectForPush(t, ctx, f.svc, actor, false)
		clients = append(clients, actor)
		req := f.request(t, 1, f.userRow(uuid.New(), "owner-"+status))
		var result *RetainedPushOutcome
		switch status {
		case "zero":
			require.Zero(t, f.count(t, ctx, `SELECT acknowledged_sequence FROM sync.push_source_progress WHERE source_id=$1`, actor.SourceID))
		case "cancelled":
			var err error
			result, err = f.svc.ResolvePushOutcome(ctx, actor, proof, pushOperationRequest(req))
			require.NoError(t, err)
		case "rejected":
			req = f.request(t, 1, f.updateUserRow(uuid.New(), "absent", 999))
			r, err := f.svc.PushOneShot(ctx, actor, proof, req)
			require.NoError(t, err)
			result = r.RetainedPushOutcome
		case "committed":
			r, err := f.svc.PushOneShot(ctx, actor, proof, req)
			require.NoError(t, err)
			result = r.RetainedPushOutcome
		}
		if result != nil {
			require.Equal(t, status, result.Outcome.Status)
		}
		f.rejectTrustedClientSource(t, ctx, actor)
		if result != nil {
			f.ack(t, ctx, actor, result.Outcome)
			f.rejectTrustedClientSource(t, ctx, actor)
		}
	}
	// Zero and acknowledged ownership survive ordinary binding cleanup. Neither
	// direct progress cleanup nor source cleanup may turn them into server writers.
	for _, actor := range clients[:2] {
		f.query(t, ctx, `DELETE FROM sync.source_generation_bindings WHERE source_id=$1`, actor.SourceID)
		f.rejectTrustedClientSource(t, ctx, actor)
		_, err := f.pool.Exec(ctx, `DELETE FROM sync.push_source_progress WHERE source_id=$1`, actor.SourceID)
		require.Error(t, err)
		_, err = f.pool.Exec(ctx, `DELETE FROM sync.source_state WHERE source_id=$1`, actor.SourceID)
		require.Error(t, err)
	}
	// Distinct trusted writer IDs still commit once and resolve receipt replay
	// without callbacks. Deleting their receipt never removes source ownership.
	for _, kind := range []string{"within", "manager"} {
		actor := Actor{UserID: f.writer.UserID, SourceID: "trusted-" + kind}
		id := uuid.New()
		calls := 0
		callback := func(ctx context.Context, tx DatabaseWriteTx) error {
			calls++
			_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.users(id,name,email) VALUES($1,$2,$3)`, pgx.Identifier{f.schemaName}.Sanitize()), id, kind, id.String()+"@example.invalid")
			return err
		}
		if kind == "within" {
			source := BundleSource{SourceID: actor.SourceID, SourceBundleID: 1, CanonicalRequestHash: strings.Repeat("a", 64)}
			opts := retryableBundleWriteOptionsForTest()
			require.NoError(t, f.svc.WithinSyncBundle(ctx, actor, source, opts, callback))
			require.NoError(t, f.svc.WithinSyncBundle(ctx, actor, source, opts, callback))
		} else {
			mgr := NewScopeManager(f.svc, ScopeManagerConfig{})
			opts := ScopeWriteOptions{WriterID: actor.SourceID, RetryableWriteOptions: retryableWriteOptionsForTest()}
			first, err := mgr.ExecWrite(ctx, actor.UserID, opts, callback)
			require.NoError(t, err)
			replay, err := mgr.ExecWrite(ctx, actor.UserID, opts, callback)
			require.NoError(t, err)
			require.Equal(t, first, replay)
		}
		require.Equal(t, 1, calls)
		f.query(t, ctx, `DELETE FROM sync.scope_write_receipts WHERE user_pk=(SELECT user_pk FROM sync.user_state WHERE user_id=$1) AND writer_id=$2`, actor.UserID, actor.SourceID)
		before := f.durablePushState(t, ctx, actor)
		_, err := f.svc.Connect(ctx, actor, proof, &ConnectRequest{})
		requireSourceOwnershipConflict(t, err)
		require.Equal(t, before, f.durablePushState(t, ctx, actor))
		_, err = f.pool.Exec(ctx, `DELETE FROM sync.source_state WHERE user_pk=(SELECT user_pk FROM sync.user_state WHERE user_id=$1) AND source_id=$2`, actor.UserID, actor.SourceID)
		require.Error(t, err)
	}
	// A source string in another scope remains independent.
	otherScope := f.writer.UserID + "-other"
	_, err := NewScopeManager(f.svc, ScopeManagerConfig{}).ExecWrite(ctx, otherScope, ScopeWriteOptions{WriterID: clients[0].SourceID, RetryableWriteOptions: retryableWriteOptionsForTest()}, func(ctx context.Context, tx DatabaseWriteTx) error {
		id := uuid.New()
		_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.users(id,name,email) VALUES($1,'independent',$2)`, pgx.Identifier{f.schemaName}.Sanitize()), id, id.String()+"@example.invalid")
		return err
	})
	require.NoError(t, err)
	// Service restart rechecks the authored layout, then reads the same fences.
	require.NoError(t, f.svc.Close(ctx))
	f.svc = outcomeService(t, ctx, f.pool, f.cfg)
	for _, actor := range clients {
		f.rejectTrustedClientSource(t, ctx, actor)
	}
	for _, kind := range []string{"within", "manager"} {
		_, err := f.svc.Connect(ctx, Actor{UserID: f.writer.UserID, SourceID: "trusted-" + kind}, proof, &ConnectRequest{})
		requireSourceOwnershipConflict(t, err)
	}
}
