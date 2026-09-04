package schemaevolution

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func fixtureUpload(t *testing.T, pool *pgxpool.Pool, service *oversync.SyncService, proof oversync.SchemaGenerationProof, key uuid.UUID, operation string, payload map[string]any) (*oversync.OneShotPushResponse, error) {
	t.Helper()
	ctx := context.Background()
	actor := oversync.Actor{UserID: "scope", SourceID: "u-" + uuid.NewString()}
	_, err := service.Connect(ctx, actor, proof, &oversync.ConnectRequest{})
	if err != nil {
		return nil, err
	}
	payload["id"] = key.String()
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	var version int64
	if operation != oversync.OpInsert {
		require.NoError(t, pool.QueryRow(ctx, `SELECT COALESCE(max(r.bundle_seq),0) FROM sync.row_state r JOIN sync.user_state u USING(user_pk) WHERE u.user_id='scope' AND r.key_bytes=$1`, key[:]).Scan(&version))
	}
	row := oversync.PushRequestRow{Schema: "main", Table: "upgrade_people", Key: oversync.SyncKey{"id": key.String()}, Op: operation, BaseRowVersion: version, Payload: data}
	hash, _, err := protocolhash.GenerationPinnedPushRequest(fmt.Sprint(proof.ID), proof.Fingerprint, []protocolhash.PushRow{{Schema: row.Schema, Table: row.Table, Key: map[string]any(row.Key), Op: row.Op, BaseRowVersion: version, Payload: data}})
	require.NoError(t, err)
	return service.PushOneShot(ctx, actor, proof, &oversync.OneShotPushRequest{SourceBundleID: 1, CanonicalRequestHash: hash, Rows: []oversync.PushRequestRow{row}})
}

func TestEvolution_GenerationUploadPolicyAndCapture(t *testing.T) {
	pool, service, key := fixtureMaintaining(t)
	ctx := context.Background()
	cfg, err := Configuration(3, 2, false)
	require.NoError(t, err)
	oldProof := cfg.Readiness.Generations[0].Proof
	committed, err := fixtureUpload(t, pool, service, oldProof, key, oversync.OpUpdate, map[string]any{"legacy_name": "C|D"})
	require.Error(t, err) // private migration readiness never opens ordinary uploads
	service = fixtureCoexistence(t, pool, service)
	committed, err = fixtureUpload(t, pool, service, oldProof, key, oversync.OpUpdate, map[string]any{"legacy_name": "C|D"})
	require.NoError(t, err)
	require.Positive(t, committed.Bundle.RowCount)
	_, err = fixtureUpload(t, pool, service, oldProof, key, oversync.OpUpdate, map[string]any{"legacy_name": "invalid"})
	var rejected *oversync.ApplicationWriteRejectedError
	require.ErrorAs(t, err, &rejected)
	require.Equal(t, "transition_input_invalid", rejected.Category)
	cfg, err = Configuration(4, 3, true)
	require.NoError(t, err)
	newProof := cfg.Readiness.Generations[1].Proof
	for _, payload := range []map[string]any{{"name": "Partial"}, {"name": "Explicit", "new_label": "D"}} {
		_, err := fixtureUpload(t, pool, service, newProof, key, oversync.OpUpdate, payload)
		require.NoError(t, err)
	}
	for _, payload := range []map[string]any{{"name": nil}, {"name": ""}} {
		_, err := fixtureUpload(t, pool, service, newProof, key, oversync.OpUpdate, payload)
		require.Error(t, err)
	}
	_, err = fixtureUpload(t, pool, service, newProof, uuid.New(), oversync.OpInsert, map[string]any{"name": "Partial"})
	require.Error(t, err)
	newKey := uuid.New()
	committed, err = fixtureUpload(t, pool, service, newProof, newKey, oversync.OpInsert, map[string]any{"name": "Whole", "new_label": "Pair"})
	require.NoError(t, err)
	var canonical json.RawMessage
	require.NoError(t, pool.QueryRow(ctx, `SELECT payload_wire FROM sync.bundle_rows WHERE bundle_seq=$1 AND key_bytes=$2`, committed.Bundle.BundleSeq, newKey[:]).Scan(&canonical))
	var row map[string]any
	require.NoError(t, json.Unmarshal(canonical, &row))
	require.Equal(t, "Whole|Pair", row["legacy_name"])
	require.Equal(t, "Whole", row["name"])
	require.Equal(t, "Pair", row["new_label"])
	for _, proof := range []oversync.SchemaGenerationProof{oldProof, newProof} {
		actor := oversync.Actor{UserID: "scope", SourceID: "reader-" + uuid.NewString()}
		_, err := service.Connect(ctx, actor, proof, &oversync.ConnectRequest{})
		require.NoError(t, err)
		pull, err := service.ProcessPull(ctx, actor, proof, committed.Bundle.BundleSeq-1, 1, 0)
		require.NoError(t, err)
		require.Len(t, pull.Bundles, 1)
		require.Len(t, pull.Bundles[0].Rows, 1)
		var visible map[string]any
		require.NoError(t, json.Unmarshal(pull.Bundles[0].Rows[0].Payload, &visible))
		if proof.ID == 1 {
			require.Equal(t, "Whole|Pair", visible["legacy_name"])
			require.NotContains(t, visible, "name")
		} else {
			require.Equal(t, "Whole", visible["name"])
			require.NotContains(t, visible, "legacy_name")
		}
	}
	// PostgreSQL still performs this UPDATE; core captures its coherent physical
	// after-image. Transition acceptance does not invent alternate no-op semantics.
	unchanged, err := fixtureUpload(t, pool, service, newProof, newKey, oversync.OpUpdate, map[string]any{"name": "Whole", "new_label": "Pair"})
	require.NoError(t, err)
	require.EqualValues(t, 1, unchanged.Bundle.RowCount)
}
