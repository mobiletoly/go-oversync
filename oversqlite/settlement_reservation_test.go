package oversqlite

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type settlementReservationCase struct {
	target      int64
	rows, limit int
}

func TestSettlementReservation_PublicCompletion(t *testing.T) {
	testSettlementReservation(t, []settlementReservationCase{
		{1, 125, 40}, {8, 125, 40}, {17, 125, 40}, {1000000, 125, 40}, {1000000, 1000, 1000},
	})
}

// A small semantic regression; the full scale witnesses remain above.
func TestSettlementReservation_Continuation(t *testing.T) {
	testSettlementReservation(t, []settlementReservationCase{
		{1, 9, 4}, {8, 9, 4}, {17, 9, 4}, {1000000, 9, 4},
	})
}

func testSettlementReservation(t *testing.T, cases []settlementReservationCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(fmt.Sprintf("target_%d_rows_%d", tc.target, tc.rows), func(t *testing.T) {
			tableCount := 8
			if tc.rows == 1000 {
				tableCount = 1
			}
			tables := make([]SyncTable, tableCount)
			ddl := make([]string, tableCount)
			for i := range tables {
				name := fmt.Sprintf("items_%d", i)
				tables[i] = SyncTable{TableName: name, SyncKeyColumnName: "id"}
				ddl[i] = "CREATE TABLE " + name + "(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)"
			}
			c, db := newBundleClient(t, "main", tables, ddl...)
			c.config.UploadLimit = tc.limit
			c.config.MaxSyncWorkUnitsPerInvocation = tc.target
			_, err := db.Exec(`CREATE TABLE reservation_witness(peak_entries INTEGER,peak_bytes INTEGER,releases INTEGER); INSERT INTO reservation_witness VALUES(0,0,0);
CREATE TRIGGER reservation_witness_update AFTER UPDATE OF workspace_context ON _sync_operation_state BEGIN
 UPDATE reservation_witness SET peak_entries=max(peak_entries,NEW.reserved_entries),peak_bytes=max(peak_bytes,NEW.reserved_bytes),releases=releases+(OLD.workspace_context<>'' AND NEW.workspace_context=''); END;`)
			require.NoError(t, err)
			_, err = db.Exec(`WITH RECURSIVE n(i) AS(SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?) INSERT INTO items_0 SELECT printf('row-%04d',i),'value' FROM n`, tc.rows)
			require.NoError(t, err)
			server := newMockPushServer(t)
			c.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/sync/pull" {
					return jsonResponse(oversync.PullResponse{StableBundleSeq: server.nextBundleSeq - 1}), nil
				}
				if r.Header.Get("Content-Encoding") == "gzip" {
					decoder, e := gzip.NewReader(r.Body)
					require.NoError(t, e)
					body, e := io.ReadAll(decoder)
					require.NoError(t, e)
					require.NoError(t, decoder.Close())
					require.NoError(t, r.Body.Close())
					r = r.Clone(r.Context())
					r.Body = io.NopCloser(bytes.NewReader(body))
					r.Header.Del("Content-Encoding")
					r.ContentLength = int64(len(body))
				}
				return server.RoundTrip(r)
			})}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			_, err = c.PushPending(ctx)
			t.Logf("after push: stats=%+v peak=%d", c.a3Stats, countA3(t, db, `SELECT peak_entries FROM reservation_witness`))
			require.NoError(t, err, "one call must finish settlement")
			require.Len(t, server.pushRequests, 1)
			require.EqualValues(t, tc.rows-tc.limit, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
			require.EqualValues(t, 1, countA3(t, db, `SELECT releases FROM reservation_witness`), "one outer reservation release")
			peak := countA3(t, db, `SELECT peak_entries FROM reservation_witness`)
			require.LessOrEqual(t, peak, int64(128*tc.limit+33*(64+12*tableCount)+512), "retained buffers depend on simultaneous copies, not slice count")
			if tc.target < 1000000 {
				require.Positive(t, c.a3Stats.workSlices)
			}
			t.Logf("target=%d rows=%d slices=%d reservation_peak_entries=%d bytes=%d", tc.target, tc.rows, c.a3Stats.workSlices, peak, countA3(t, db, `SELECT peak_bytes FROM reservation_witness`))
			// Peak and release are fully asserted for the measured PushPending
			// above. Remove its bookkeeping trigger before the unmeasured drain:
			// its measurement is complete, so later workspace updates no longer
			// need the extra bookkeeping DML. Keep every row and the same target.
			_, err = db.Exec(`DROP TRIGGER reservation_witness_update`)
			require.NoError(t, err)
			_, err = c.Sync(ctx)
			require.NoError(t, err)
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`))
			require.Zero(t, countA3(t, db, `SELECT count(*) FROM _sync_outbox_rows`))
			require.Zero(t, countA3(t, db, `SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state`))
			require.EqualValues(t, tc.rows, countA3(t, db, `SELECT count(*) FROM items_0 WHERE name='value'`))
		})
	}
}
