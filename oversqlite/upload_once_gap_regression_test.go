package oversqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type fakeSyncServer struct {
	step int64

	mu     sync.Mutex
	nextID int64
	users  map[string]*fakeUserStream
}

type fakeUserStream struct {
	log      []oversync.ChangeDownloadResponse
	versions map[string]int64
	deleted  map[string]bool
}

func newFakeSyncServer(step int64) *fakeSyncServer {
	return &fakeSyncServer{
		step:   step,
		nextID: step,
		users:  map[string]*fakeUserStream{},
	}
}

func (s *fakeSyncServer) userStreamLocked(userID string) *fakeUserStream {
	us := s.users[userID]
	if us != nil {
		return us
	}
	us = &fakeUserStream{
		log:      []oversync.ChangeDownloadResponse{},
		versions: map[string]int64{},
		deleted:  map[string]bool{},
	}
	s.users[userID] = us
	return us
}

func (s *fakeSyncServer) maxServerIDLocked(us *fakeUserStream) int64 {
	if len(us.log) == 0 {
		return 0
	}
	return us.log[len(us.log)-1].ServerID
}

func (s *fakeSyncServer) handleUpload(userID, sourceID string, req oversync.UploadRequest) (oversync.UploadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	us := s.userStreamLocked(userID)
	now := time.Now().UTC()

	resp := oversync.UploadResponse{
		Accepted: true,
		Statuses: make([]oversync.ChangeUploadStatus, len(req.Changes)),
	}

	for i, ch := range req.Changes {
		schema := ch.Schema
		if schema == "" {
			schema = "public"
		}

		rowKey := fmt.Sprintf("%s.%s:%s", schema, ch.Table, ch.PK)
		newVer := us.versions[rowKey] + 1
		us.versions[rowKey] = newVer

		deleted := ch.Op == oversync.OpDelete
		us.deleted[rowKey] = deleted

		sid := s.nextID
		s.nextID += s.step

		entry := oversync.ChangeDownloadResponse{
			ServerID:       sid,
			Schema:         schema,
			TableName:      ch.Table,
			Op:             ch.Op,
			PK:             ch.PK,
			Payload:        ch.Payload,
			ServerVersion:  newVer,
			Deleted:        deleted,
			SourceID:       sourceID,
			SourceChangeID: ch.SourceChangeID,
			Timestamp:      now,
		}
		us.log = append(us.log, entry)

		resp.Statuses[i] = oversync.ChangeUploadStatus{
			SourceChangeID:   ch.SourceChangeID,
			Status:           oversync.StApplied,
			NewServerVersion: &newVer,
		}
	}

	resp.HighestServerSeq = s.maxServerIDLocked(us)
	return resp, nil
}

func (s *fakeSyncServer) handleDownload(userID, sourceID string, after int64, limit int, schema string, includeSelf bool, until int64) (oversync.DownloadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	us := s.userStreamLocked(userID)

	windowUntil := until
	if windowUntil <= 0 {
		windowUntil = s.maxServerIDLocked(us)
	}

	if limit <= 0 {
		limit = 100
	}

	var matched []oversync.ChangeDownloadResponse
	for i := range us.log {
		e := us.log[i]
		if e.ServerID <= after || e.ServerID > windowUntil {
			continue
		}
		if schema != "" && e.Schema != schema {
			continue
		}
		if !includeSelf && e.SourceID == sourceID {
			continue
		}
		matched = append(matched, e)
	}

	hasMore := false
	if len(matched) > limit {
		hasMore = true
		matched = matched[:limit]
	}

	nextAfter := after
	if len(matched) > 0 {
		nextAfter = matched[len(matched)-1].ServerID
	}

	return oversync.DownloadResponse{
		Changes:     matched,
		HasMore:     hasMore,
		NextAfter:   nextAfter,
		WindowUntil: windowUntil,
	}, nil
}

type fakeTransport struct {
	server   *fakeSyncServer
	userID   string
	sourceID string
}

func (t *fakeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	switch req.Method + " " + req.URL.Path {
	case "POST /sync/upload":
		var uploadReq oversync.UploadRequest
		if err := json.NewDecoder(req.Body).Decode(&uploadReq); err != nil {
			return nil, err
		}
		uploadResp, err := t.server.handleUpload(t.userID, t.sourceID, uploadReq)
		if err != nil {
			return nil, err
		}
		return jsonResponse(uploadResp), nil

	case "GET /sync/download":
		q := req.URL.Query()
		after, _ := strconv.ParseInt(q.Get("after"), 10, 64)
		limit, _ := strconv.Atoi(q.Get("limit"))
		schema := q.Get("schema")
		includeSelf := q.Get("include_self") == "true"
		until, _ := strconv.ParseInt(q.Get("until"), 10, 64)

		downloadResp, err := t.server.handleDownload(t.userID, t.sourceID, after, limit, schema, includeSelf, until)
		if err != nil {
			return nil, err
		}
		return jsonResponse(downloadResp), nil

	default:
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(bytes.NewReader([]byte("not found"))),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	}
}

func jsonResponse(v any) *http.Response {
	b, err := json.Marshal(v)
	if err != nil {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       io.NopCloser(bytes.NewReader([]byte(err.Error()))),
			Header:     make(http.Header),
		}
	}
	h := make(http.Header)
	h.Set("Content-Type", "application/json")
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(b)),
		Header:     h,
	}
}

func createUsersTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		)
	`)
	require.NoError(t, err)
}

func insertUser(t *testing.T, db *sql.DB, id, name string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO users (id, name, email) VALUES (?, ?, ?)`, id, name, name+"@example.com")
	require.NoError(t, err)
}

func TestUploadOnce_DoesNotSkipPeerChangesWhenServerIDsHaveLargeGaps(t *testing.T) {
	ctx := context.Background()
	srv := newFakeSyncServer(50_000)

	userID := "user-test"
	device1ID := "device-1"
	device2ID := "device-2"

	// Client 1
	db1, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db1.Close()
	createUsersTable(t, db1)

	cfg := DefaultConfig("business", []SyncTable{{TableName: "users"}})
	tok := func(context.Context) (string, error) { return "token", nil }
	c1, err := NewClient(db1, "http://example", userID, device1ID, tok, cfg)
	require.NoError(t, err)
	c1.HTTP = &http.Client{Transport: &fakeTransport{server: srv, userID: userID, sourceID: device1ID}}
	require.NoError(t, c1.Bootstrap(ctx, false))

	// Client 2
	db2, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db2.Close()
	createUsersTable(t, db2)

	c2, err := NewClient(db2, "http://example", userID, device2ID, tok, cfg)
	require.NoError(t, err)
	c2.HTTP = &http.Client{Transport: &fakeTransport{server: srv, userID: userID, sourceID: device2ID}}
	require.NoError(t, c2.Bootstrap(ctx, false))

	// Device 1: create A1,A2,A3 and upload.
	a1, a2, a3 := uuid.NewString(), uuid.NewString(), uuid.NewString()
	insertUser(t, db1, a1, "A1")
	insertUser(t, db1, a2, "A2")
	insertUser(t, db1, a3, "A3")
	require.NoError(t, c1.UploadOnce(ctx))

	// Device 2: create B1 and upload (server_id will be far from A* due to large step).
	b1 := uuid.NewString()
	insertUser(t, db2, b1, "B1")
	require.NoError(t, c2.UploadOnce(ctx))

	// Device 1: create B3 and upload. A naive cursor fast-forward would now skip B1 permanently.
	b3 := uuid.NewString()
	insertUser(t, db1, b3, "B3")
	require.NoError(t, c1.UploadOnce(ctx))

	var count int
	require.NoError(t, db1.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count))
	require.Equal(t, 5, count, "device1 should converge even when server_id gaps are large")

	var b1Count int
	require.NoError(t, db1.QueryRow(`SELECT COUNT(*) FROM users WHERE id = ?`, b1).Scan(&b1Count))
	require.Equal(t, 1, b1Count, "device1 should download B1 (peer insert) during sync")
}
