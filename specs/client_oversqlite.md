<!-- moved to specs -->
# go-oversync — Client-Side Service (SQLite) — Build Instructions

> **Goal**: A Go client service that uses **SQLite** to store business data **and** sync metadata,
> and fully synchronizes with our **Go sync server** (single-user, multi-device, no channels, no
> materializer).
>
> **Auth model**: JWT with `sub` (user) and `did` (device/source).  
> **Transport**: `POST /sync/upload`, `GET /sync/download`.  
> **Conflict control**: optimistic via `server_version`.

---

## 1) Architecture (client)

**Components**

- `Store` — wraps SQLite (DDL, migrations, tx helpers).
- `Tracker` — SQLite triggers & pending queue maintenance for business tables.
- `Serializer` — loads a row by `(table, pk)` and serializes it to JSON payload for upload.
- `Uploader` — builds upload batches from `_sync_pending`, assigns `source_change_id`, handles
  server responses, resolves conflicts.
- `Downloader` — pulls from `/sync/download`, applies server changes, advances cursor.
- `Auth` — holds JWT (Option A), injects `Authorization: Bearer <token>`.
- `SourceID` — persistent UUID for this install (generated once, stored in SQLite).
- `Scheduler` — runs Upload/Download loops with backoff.

**Key invariants**

- **Idempotency** per device: `(user_id, source_id, source_change_id)` unique on the server.
- **Per-row concurrency**: `server_version` in client `_sync_row_meta` must match the server on
  upload; otherwise **conflict**.

---

## 2) SQLite schema (client)

Create a dedicated namespace via table prefixes (SQLite has no schemas).

```sql
-- 2.1 Client/device info (one row)
CREATE TABLE IF NOT EXISTS _sync_client_info (
  user_id                TEXT NOT NULL,          -- from JWT.sub
  source_id              TEXT NOT NULL,          -- locally generated UUIDv4 (persisted)
  next_change_id         INTEGER NOT NULL DEFAULT 1,
  last_server_seq_seen   INTEGER NOT NULL DEFAULT 0,
  apply_mode             INTEGER NOT NULL DEFAULT 0,  -- 0=normal (queue to _sync_pending), 1=server-apply (suppress)
  PRIMARY KEY (user_id)                               -- single signed-in user per DB file
);

-- 2.2 Per-row metadata (deletion flag lives here)
CREATE TABLE IF NOT EXISTS _sync_row_meta (
  table_name     TEXT NOT NULL,
  pk_uuid        TEXT NOT NULL,                 -- UUID string
  server_version INTEGER NOT NULL DEFAULT 0,    -- last known server_version
  deleted        INTEGER NOT NULL DEFAULT 0,    -- 0/1 deleted flag (client-side)
  updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  PRIMARY KEY (table_name, pk_uuid)
);

-- 2.3 Pending queue (coalesced, one row per PK)
CREATE TABLE IF NOT EXISTS _sync_pending (
  table_name     TEXT NOT NULL,
  pk_uuid        TEXT NOT NULL,
  op             TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
  base_version   INTEGER NOT NULL,              -- snapshot of _sync_row_meta.server_version at time of queuing
  queued_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  PRIMARY KEY (table_name, pk_uuid)
);

No _sync_state table is needed client-side; we read current business rows on demand to serialize payloads.

⸻

3) Business tables & triggers (client)

Example business tables:

CREATE TABLE IF NOT EXISTS note (
  id         TEXT PRIMARY KEY,                  -- UUID
  title      TEXT NOT NULL,
  content    TEXT,
  updated_at TEXT NOT NULL                      -- ISO8601
);

CREATE TABLE IF NOT EXISTS task (
  id         TEXT PRIMARY KEY,                  -- UUID
  note_id    TEXT REFERENCES note(id) ON DELETE SET NULL,
  title      TEXT NOT NULL,
  done       INTEGER NOT NULL DEFAULT 0,        -- 0/1
  updated_at TEXT NOT NULL
);

Trigger pattern (coalesce to _sync_pending, track deletions in _sync_row_meta):

For each table T(name...) replace T with concrete name.

-- INSERT → ensure meta exists (sv=0), mark pending INSERT
CREATE TRIGGER IF NOT EXISTS trg_T_ai
AFTER INSERT ON T
WHEN COALESCE((SELECT apply_mode FROM _sync_client_info LIMIT 1), 0) = 0
BEGIN
  INSERT OR IGNORE INTO _sync_row_meta(table_name, pk_uuid, server_version, deleted)
  VALUES ('T', NEW.id, 0, 0);

  INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version)
  VALUES ('T', NEW.id, 'INSERT', (SELECT server_version FROM _sync_row_meta WHERE table_name='T' AND pk_uuid=NEW.id))
  ON CONFLICT(table_name, pk_uuid) DO UPDATE SET
    op='INSERT',
    base_version=excluded.base_version,
    queued_at=strftime('%Y-%m-%dT%H:%M:%fZ','now');
END;

-- UPDATE → mark pending UPDATE (base_version from meta)
CREATE TRIGGER IF NOT EXISTS trg_T_au
AFTER UPDATE ON T
WHEN COALESCE((SELECT apply_mode FROM _sync_client_info LIMIT 1), 0) = 0
BEGIN
  UPDATE _sync_row_meta SET deleted=0, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
   WHERE table_name='T' AND pk_uuid=NEW.id;

  INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version)
  VALUES ('T', NEW.id, 'UPDATE', (SELECT server_version FROM _sync_row_meta WHERE table_name='T' AND pk_uuid=NEW.id))
  ON CONFLICT(table_name, pk_uuid) DO UPDATE SET
    op='UPDATE',
    base_version=excluded.base_version,
    queued_at=strftime('%Y-%m-%dT%H:%M:%fZ','now');
END;

-- DELETE → set deleted flag in meta, queue DELETE
CREATE TRIGGER IF NOT EXISTS trg_T_ad
AFTER DELETE ON T
WHEN COALESCE((SELECT apply_mode FROM _sync_client_info LIMIT 1), 0) = 0
BEGIN
  INSERT OR IGNORE INTO _sync_row_meta(table_name, pk_uuid, server_version, deleted)
  VALUES ('T', OLD.id, 0, 1);

  UPDATE _sync_row_meta SET deleted=1, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
   WHERE table_name='T' AND pk_uuid=OLD.id;

  INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version)
  VALUES ('T', OLD.id, 'DELETE', (SELECT server_version FROM _sync_row_meta WHERE table_name='T' AND pk_uuid=OLD.id))
  ON CONFLICT(table_name, pk_uuid) DO UPDATE SET
    op='DELETE',
    base_version=excluded.base_version,
    queued_at=strftime('%Y-%m-%dT%H:%M:%fZ','now');
END;

Why coalesced queue? Multiple local edits between syncs collapse to one _sync_pending row per PK (latest op). Payload will be recomputed at upload time from the current table row.

⸻

4) Source ID & login
	•	Generate UUIDv4 once and persist in _sync_client_info.source_id.
	•	On sign-in, set:
	•	_sync_client_info.user_id = claims.sub
	•	_sync_client_info.source_id unchanged (or new DB per user)
	•	keep/rotate access token externally.

func EnsureSourceID(db *sql.DB, userID string) (string, error) {
  // if row absent -> insert with new uuid
}


⸻

5) Upload flow (client → server)

5.1 Selecting a batch
	•	Read up to N rows from _sync_pending ordered by queued_at.
	•	For each, set server_version := _sync_pending.base_version.
	•	Build changes[]:

{
  "source_change_id": <allocate sequentially from _sync_client_info.next_change_id>,
  "schema": "app",                // constant string; required by server
  "table": "<table_name>",
  "op": "INSERT|UPDATE|DELETE",
  "pk": "<uuid>",
  "server_version": <base_version>,
  "payload": { /* JSON from Serializer (omit on DELETE) */ }
}



Serializer:
	•	For INSERT/UPDATE: SELECT * FROM <table> WHERE id=? → map to JSON.
	•	For DELETE: payload=null.

5.2 Request

POST /sync/upload
Authorization: Bearer <JWT(sub,did)>
Content-Type: application/json

5.3 Response handling

For each status:
	•	applied:
	•	Update _row_meta.server_version = new_server_version, _row_meta.deleted = 0 or 1 (based on op).
	•	DELETE FROM _sync_pending WHERE (table,pk)=….
	•	conflict:
	•	server_row includes {server_version, deleted, payload?}.
	•	Run merge strategy (see §7). Typically:
	•	If local op is DELETE and server_row.deleted==1 → clear _sync_pending, set meta to server_version.
	•	Else produce merged JSON, write back to the business table, and set _sync_pending.op='UPDATE', base_version = server_row.server_version.
	•	invalid/error: log & retry later (exponential backoff).
	•	Always bump _sync_client_info.next_change_id by the count of items submitted.

⸻

6) Download flow (server → client)

6.1 Request

GET /sync/download?after=<last_server_seq_seen>&limit=1000&schema=app
Authorization: Bearer <JWT>

6.2 Apply order & echo suppression
	•	Iterate in ascending server_id.
	•	Skip own changes where change.source_id == _sync_client_info.source_id (prevents “echo”).
	•	For each remaining change:
	•	If there is no local pending for (table, pk):
	•	DELETE: DELETE FROM <table> WHERE id=? (ignore if missing); set _row_meta.deleted=1, server_version=change.server_version.
	•	INSERT/UPDATE: upsert into <table>, set _row_meta.deleted=0, server_version=change.server_version.
	•	If there is local pending:
	•	Add to conflict queue (client wants to keep local edits). Strategy in §7.
	•	Advance _sync_client_info.last_server_seq_seen = last server_id applied or seen.

⸻

7) Conflict strategy (client)

Provide a pluggable interface with a default:

type Resolver interface {
  // Returns merged JSON to store locally & attempt to upload as UPDATE,
  // or (nil, keepLocal bool=false) to accept server and drop local pending.
  Merge(table string, pk string, server json.RawMessage, local json.RawMessage) (merged json.RawMessage, keepLocal bool, err error)
}

Default:
	•	If server deleted and local pending is UPDATE/INSERT → accept server (drop local), remove row from table, set deleted=1, sv=server_version.
	•	Otherwise, client-wins fieldwise for a whitelist of columns (title, content, done), but always base it on server_version returned. Write merged JSON back to the table; set _sync_pending(op='UPDATE', base_version=server_row.server_version).

(Allow swapping to server-wins or custom merge per table via config.)

⸻

8) Service runner
	•	Goroutine Downloader:
	•	for { download; apply; sleep/backoff }
	•	Goroutine Uploader:
	•	for { if _sync_pending not empty { upload; } sleep/backoff }
	•	Backoff: linear first (e.g., 1s → 5s), then exponential up to 60s on HTTP/5xx/network errors.

Transactions
	•	For each batch apply (upload-result handling and download apply), wrap in a single SQLite transaction so _row_meta and business rows move atomically.

⸻

9) Go API surface (proposed)

type Client struct {
  DB        *sql.DB
  BaseURL   string
  Token     func(context.Context) (string, error) // returns JWT
  SourceID  string
  UserID    string
  Schema    string // "app"
  Resolver  Resolver
  HTTP      *http.Client
  // internals: serializer, log, etc.
}

func NewClient(db *sql.DB, baseURL, userID, sourceID string, tok func(ctx context.Context)(string,error)) *Client

func (c *Client) Start(ctx context.Context) error     // starts uploader+downloader loops
func (c *Client) Stop(ctx context.Context) error
func (c *Client) UploadOnce(ctx context.Context) error
func (c *Client) DownloadOnce(ctx context.Context, limit int) (applied int, nextAfter int64, err error)

Serialization

func (c *Client) SerializeRow(ctx context.Context, table, pk string) (json.RawMessage, error)


⸻

10) HTTP shapes (must match server)

Upload request

{
  "last_server_seq_seen": <int>,           // from _sync_client_info
  "changes": [{
    "source_change_id": <int>,             // allocate sequentially
    "schema": "app",
    "table": "note",
    "op": "INSERT|UPDATE|DELETE",
    "pk": "<uuid>",
    "server_version": <int>,
    "payload": { /* from SerializeRow */ }
  }]
}

Upload response (per item)

{ "status":"applied|conflict|invalid",
  "new_server_version": <int>,       // if applied
  "server_row": { "server_version":<int>, "deleted":<bool>, "payload":{...} } // if conflict
}

Download response

{
  "changes": [{
    "server_id": <int>, "schema": "app", "table":"note",
    "op":"INSERT|UPDATE|DELETE", "pk":"<uuid>",
    "payload": {...} | null, "server_version": <int>, "deleted": <bool>,
    "source_id":"<device-uuid>", "source_change_id": <int>, "ts":"..."}],
  "has_more": <bool>,
  "next_after": <int>
}


⸻

11) Validation & safety
	•	Validate table_name against an allowlist in the client to avoid malformed triggers or serialization queries.
	•	Ensure pk_uuid is a UUIDv4 string (client can generate).
	•	Use SQLite WAL mode for concurrency: PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;
	•	Create deferrable FKs aren’t available in SQLite; use ON DELETE SET NULL as in example.

⸻

12) Tests (client side)
	•	Local queueing: INSERT/UPDATE/DELETE create/override entries in _sync_pending with correct op and base_version.
	•	Upload happy path: applied → meta bump, _sync_pending row removed.
	•	Upload conflict: server newer → resolver invoked; pending converted to UPDATE with new base_version.
	•	Download apply: with/without local pending; echo suppression skips own changes.
	•	Multi-device simulation: run two client instances sharing the same server; ensure both converge.
	•	Crash consistency: kill between queue and upload; on restart, queued items still present and re-uploaded idempotently.

⸻

13) Deliverables
	•	Go package oversyncclient with:
	•	SQLite DDL & migration initializer
	•	Trigger generation for configured tables (note, task sample)
	•	Client struct + uploader/downloader loops
	•	Serializer + default Resolver (client-wins) with hook interface
	•	Auth token hook and HTTP client
	•	Example main:
	•	Creates DB, ensures _sync_client_info row with source_id and user_id
	•	Starts the service
	•	Demonstrates basic CRUD on note and task
	•	Integration tests per §12

⸻

14) Defaults & config

type Config struct {
  Schema        string        // "app"
  Tables        []string      // e.g., []{"note","task"}
  UploadLimit   int           // e.g., 200 per batch
  DownloadLimit int           // e.g., 1000
  BackoffMin    time.Duration // 1s
  BackoffMax    time.Duration // 60s
}

Set sensible defaults; allow overrides.

⸻

TL;DR
	•	Track local edits via SQLite triggers into a coalesced _sync_pending queue and per-row _row_meta with server_version & deleted.
	•	Upload coalesced changes with server_version; handle applied vs conflict by bumping meta or merging via a pluggable resolver.
	•	Download other devices’ changes, skip own, and update business tables & meta atomically.
	•	Persist a source_id per install; use JWT (sub,did) for auth to the server.
