package oversqlite

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestA3ResponseAdmission_ReadPhases(t *testing.T) {
	for _, allowance := range []int64{15, 16, 32, 48} {
		t.Run(strconv.FormatInt(allowance, 10), func(t *testing.T) {
			c, db := newA1SurvivorClient(t)
			_, err := db.Exec(`UPDATE nodes SET name='new local intent' WHERE id='Q'`)
			require.NoError(t, err)
			unit := applyUnit("response-admission", 0, "")
			token, err := c.reserveWorkspace(context.Background(), unit, workspaceCharge{})
			require.NoError(t, err)
			ctx := context.WithValue(context.Background(), workspaceBufferKey{}, &workspaceBufferScope{token: token, unit: unit, client: c})
			c.syncWorkUsed = 0
			c.config.MaxSyncWorkUnitsPerInvocation = allowance
			c.HTTP = &http.Client{Transport: a3ResponseTransport{}}
			_, err = c.executeBoundedRequest(ctx, "push", mustA3Request(t), 4096, 4096, nil)
			require.Same(t, err, c.withSyncErrorProgress(ctx, err))
			// Exercise encoded page/join and decoded page/join respectively.
			if allowance == 15 {
				var space *SyncWorkspaceLimitError
				require.ErrorAs(t, err, &space)
				require.Equal(t, "atomic_apply_work", space.Dimension)
				require.Equal(t, unit, space.Unit)
				require.EqualValues(t, 16, space.Reserved)
				require.EqualValues(t, 1, space.Progress.DirtyRows)
			} else {
				var work *SyncWorkLimitError
				require.ErrorAs(t, err, &work)
				require.Equal(t, unit, work.Unit)
				require.Equal(t, allowance, work.Used)
				require.EqualValues(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`), work.Progress.DirtyRows)
			}
			require.NoError(t, c.releaseWorkspace(context.Background(), token))
			require.Zero(t, countA3(t, db, `SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state`))
		})
	}
}

func mustA3Request(t *testing.T) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://example.invalid", nil)
	require.NoError(t, err)
	return req
}

type a3ResponseTransport struct{}

func (a3ResponseTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return gzipResponse(200, `{}`), nil
}

func TestA3ResponseAdmission_PublicProgressAndReplay(t *testing.T) {
	c, db := newA1SurvivorClient(t)
	_, err := db.Exec(`UPDATE nodes SET name='changed parent' WHERE id='P'`)
	require.NoError(t, err)
	server := a1SettlementServer(t)
	c.HTTP = &http.Client{Transport: server}
	c.config.MaxSyncWorkspaceBytes = 1 << 20
	_, err = c.PushPending(context.Background())
	var space *SyncWorkspaceLimitError
	require.ErrorAs(t, err, &space)
	require.Equal(t, "bytes", space.Dimension)
	require.EqualValues(t, 1<<20, space.Limit)
	require.Positive(t, space.Used)
	require.Positive(t, space.Reserved)
	require.True(t, workspaceIdentityValid(space.Unit))
	require.EqualValues(t, countA3(t, db, `SELECT count(*) FROM _sync_dirty_rows`), space.Progress.DirtyRows)
	require.EqualValues(t, 1, space.Progress.OutboundRows)
	require.Zero(t, space.Progress.CommittedBundles)
	require.Len(t, server.pushRequests, 1, "refusal occurred while reading the committed response")
	require.Zero(t, countA3(t, db, `SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state`))
	require.EqualValues(t, 1, countA3(t, db, `SELECT last_bundle_seq_seen FROM _sync_attachment_state`))
	c.config.MaxSyncWorkspaceBytes = 256 << 20
	_, err = c.PushPending(context.Background())
	require.NoError(t, err)
	require.Len(t, server.pushRequests, 2)
	require.Equal(t, server.pushRequests[0], server.pushRequests[1])
	requireA1Survivors(t, db)
	requireA1ControlState(t, db, 3)
}

type a3ErrorReader struct{ err error }

func (r a3ErrorReader) Read([]byte) (int, error) { return 0, r.err }
func (a3ErrorReader) Close() error               { return nil }

func TestA3ResponseAdmission_RemoteFailuresStayBounded(t *testing.T) {
	peer := &SyncWorkspaceLimitError{Unit: "peer payload must not escape", Dimension: "bytes", Limit: 1, Reserved: 2}
	_, err := readDecodedBodyBounded(context.Background(), "push", &http.Response{Body: a3ErrorReader{peer}}, 4096)
	var space *SyncWorkspaceLimitError
	require.False(t, errors.As(err, &space))
	require.NotContains(t, err.Error(), peer.Unit)
	var bounded *boundedHTTPFailureError
	require.ErrorAs(t, err, &bounded)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, responseReadFailure(ctx, "push", &responseAdmissionFailure{cause: io.ErrUnexpectedEOF}), context.Canceled)
}
