package schemaevolution

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/mobiletoly/go-oversync/internal/protocolhash"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type tableCase struct {
	Request    tablePushEnvelope       `json:"push_request"`
	Headers    map[string]string       `json:"headers"`
	ID         string                  `json:"id"`
	Generation int                     `json:"generation"`
	Method     string                  `json:"method"`
	Path       string                  `json:"path"`
	Tables     []string                `json:"expected_tables"`
	Payloads   []json.RawMessage       `json:"expected_payloads"`
	Row        oversync.PushRequestRow `json:"row"`
	Project    *string                 `json:"expected_project_id"`
	Views      []tableSetNullView      `json:"views"`
}

type tablePushEnvelope struct {
	SourceBundleID       int64  `json:"source_bundle_id"`
	CanonicalRequestHash string `json:"canonical_request_hash"`
}

func (r tablePushEnvelope) oneShot(rows []oversync.PushRequestRow) oversync.OneShotPushRequest {
	return oversync.OneShotPushRequest{SourceBundleID: r.SourceBundleID, CanonicalRequestHash: r.CanonicalRequestHash, Rows: rows}
}

type tableCases struct {
	Format     string      `json:"format"`
	Scope      string      `json:"scope"`
	Person     string      `json:"person"`
	Project    string      `json:"project"`
	Attachment string      `json:"attachment"`
	Cases      []tableCase `json:"cases"`
}

func readTableCases(t *testing.T) tableCases {
	t.Helper()
	b, err := os.ReadFile("../../conformance/table-evolution/v1/cases.json")
	require.NoError(t, err)
	var v tableCases
	require.NoError(t, json.Unmarshal(b, &v))
	require.Equal(t, "oversync.table-evolution.cases.v1", v.Format)
	return v
}
func TestTableEvolution_ProtocolVectors(t *testing.T) {
	v := readTableCases(t)
	ctx := context.Background()
	pool, old := tableBaseline(t)
	_, err := CreateScope(ctx, pool, v.Scope)
	require.NoError(t, err)
	person := uuid.MustParse(v.Person)
	require.NoError(t, fixtureWrite(t, old, v.Scope, `INSERT INTO main.people VALUES($1,$2,'Person')`, v.Scope, person))
	proof, cleanup := tableCleanupStart(t, pool, old, tableTestBounds())
	s := tableBackfillStart(t, pool, proof, cleanup)
	w := &TableWorker{Pool: pool, Service: s, Capacity: tableTestCapacity}
	require.NoError(t, w.Populate(ctx))
	require.NoError(t, s.Close(ctx))
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	tablePublish(t, a, TablesServing)
	s = tableTestService(t, pool, TablesServing)
	require.NoError(t, ReopenTableServing(ctx, pool, []*oversync.SyncService{s}))
	cfg, _ := TableEvolutionConfiguration(TablesServing)
	// Old pre-expansion history has no project_id. Target pull cannot invent NULL.
	targetActor := oversync.Actor{UserID: v.Scope, SourceID: uuid.NewString()}
	_, err = s.Connect(ctx, targetActor, cfg.Readiness.Generations[1].Proof, &oversync.ConnectRequest{})
	require.NoError(t, err)
	_, err = s.ProcessPull(ctx, targetActor, cfg.Readiness.Generations[1].Proof, 0, 100, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "histor")
	for _, c := range v.Cases {
		if c.ID != "added-table-snapshot" && c.ID != "old-hidden-column" {
			continue
		}
		t.Run(c.ID, func(t *testing.T) {
			actor := oversync.Actor{UserID: v.Scope, SourceID: uuid.NewString()}
			g := cfg.Readiness.Generations[c.Generation-1].Proof
			_, err = s.Connect(ctx, actor, g, &oversync.ConnectRequest{})
			require.NoError(t, err)
			snapshot, err := s.CreateSnapshotSession(ctx, actor, g)
			require.NoError(t, err)
			chunk, err := s.GetSnapshotChunk(ctx, actor, g, snapshot.SnapshotID, 0, 100, 1<<20)
			require.NoError(t, err)
			require.Len(t, chunk.Rows, len(c.Tables))
			actual := map[string]json.RawMessage{}
			for _, row := range chunk.Rows {
				actual[row.Table] = row.Payload
			}
			for i, name := range c.Tables {
				require.JSONEq(t, string(c.Payloads[i]), string(actual[name]))
			}
		})
	}
	// Execute exact portable request rows with runtime-assigned base versions.
	for _, id := range []string{"old-update-omission", "explicit-null"} {
		for _, c := range v.Cases {
			if c.ID != id {
				continue
			}
			t.Run(id, func(t *testing.T) {
				actor := oversync.Actor{UserID: v.Scope, SourceID: uuid.NewString()}
				g := cfg.Readiness.Generations[c.Generation-1].Proof
				_, err = s.Connect(ctx, actor, g, &oversync.ConnectRequest{})
				require.NoError(t, err)
				row := c.Row
				var base int64
				require.NoError(t, pool.QueryRow(ctx, `SELECT bundle_seq FROM sync.row_state r JOIN sync.user_state u USING(user_pk) WHERE u.user_id=$1 AND table_id=2 AND key_bytes=$2`, v.Scope, person[:]).Scan(&base))
				require.Equal(t, row.BaseRowVersion, base)
				require.Equal(t, fmt.Sprint(g.ID), c.Headers[oversync.SchemaGenerationIDHeader])
				require.Equal(t, g.Fingerprint, c.Headers[oversync.SchemaGenerationFingerprintHeader])
				hash, _, err := protocolhash.GenerationPinnedPushRequest(fmt.Sprint(g.ID), g.Fingerprint, []protocolhash.PushRow{{Schema: row.Schema, Table: row.Table, Key: map[string]any(row.Key), Op: row.Op, BaseRowVersion: row.BaseRowVersion, Payload: row.Payload}})
				require.NoError(t, err)
				require.Equal(t, c.Request.CanonicalRequestHash, hash)
				request := c.Request.oneShot([]oversync.PushRequestRow{row})
				_, err = s.PushOneShot(ctx, actor, g, &request)
				require.NoError(t, err)
				var value *string
				require.NoError(t, pool.QueryRow(ctx, `SELECT project_id::text FROM main.people WHERE id=$1`, person).Scan(&value))
				require.Equal(t, c.Project, value)
			})
		}
	}
	// New-only seed effects produce an empty old envelope with a real checkpoint.
	var before int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT max(bundle_seq) FROM sync.bundle_log`).Scan(&before))
	require.NoError(t, fixtureWrite(t, s, v.Scope, `INSERT INTO main.attachments VALUES($1,$2,$3,'hidden')`, v.Scope, uuid.MustParse(v.Attachment), person))
	actor := oversync.Actor{UserID: v.Scope, SourceID: uuid.NewString()}
	g := cfg.Readiness.Generations[0].Proof
	_, err = s.Connect(ctx, actor, g, &oversync.ConnectRequest{})
	require.NoError(t, err)
	pulled, err := s.ProcessPull(ctx, actor, g, before, 100, 0)
	require.NoError(t, err)
	require.Len(t, pulled.Bundles, 1)
	require.Empty(t, pulled.Bundles[0].Rows)
	require.NoError(t, pool.QueryRow(ctx, `SELECT max(bundle_seq) FROM sync.bundle_log`).Scan(&before))
	require.NoError(t, fixtureWrite(t, s, v.Scope, `DELETE FROM main.people WHERE _sync_scope_id=$1 AND id=$2`, v.Scope, person))
	pulled, err = s.ProcessPull(ctx, actor, g, before, 100, 0)
	require.NoError(t, err)
	require.Len(t, pulled.Bundles[0].Rows, 1)
	require.Equal(t, "people", pulled.Bundles[0].Rows[0].Table)
	var deleted int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sync.bundle_rows WHERE bundle_seq=$1 AND op_code=3`, pulled.Bundles[0].BundleSeq).Scan(&deleted))
	require.Equal(t, 2, deleted)
}
