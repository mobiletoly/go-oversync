<!-- moved to specs -->
# Client ↔ Server Sync Flow Example (SQLite client with `_sync_pending`)

This example shows how the client (SQLite) and server collaborate using **`_sync_pending`**, **`_sync_row_meta`**, and the HTTP sync API.  
It demonstrates optimistic concurrency, conflict handling, and echo suppression for **one user** with **two devices**.

- **User:** `U1`
- **Devices:**  
  - Phone → `source_id = P`  
  - Laptop → `source_id = L`
- **Table:** `note(id TEXT PK, title TEXT, content TEXT, updated_at TEXT)`
- **Initial server cursor:** `server_id = 0`

---

## 0) Starting state

**Client DB (both devices)**
```
note           : (empty)
_sync_row_meta      : (empty)
_sync_pending       : (empty)
_sync_client_info   : { user_id: "U1", source_id: <P or L>, next_change_id: 1, last_server_seq_seen: 0 }
```

**Server DB**
```
sync_row_meta      : (empty)
server_change_log  : (empty)
```

---

## 1) Phone creates a note (INSERT)

### 1.1 Phone local write
```sql
INSERT INTO note(id, title, content, updated_at)
VALUES ('K1', 'Hello', 'from phone', '2025-08-09T10:00:00Z');
```

**Triggers produce**
```
_row_meta   += ('note','K1', server_version=0, deleted=0)
_sync_pending    += ('note','K1', op='INSERT', base_version=0)
```

### 1.2 Phone → Server upload
**Request**
```json
POST /sync/upload
{
  "last_server_seq_seen": 0,
  "changes": [{
    "source_change_id": 1,
    "schema": "app",
    "table": "note",
    "op": "INSERT",
    "pk": "K1",
    "server_version": 0,
    "payload": { "id":"K1", "title":"Hello", "content":"from phone", "updated_at":"2025-08-09T10:00:00Z" }
  }]
}
```

**Server handling (per item)**
- Log (idempotent): add row to `server_change_log` → `server_id=101`
- Ensure meta: upsert `(U1,'app','note','K1')`
- Version gate: `0 → 1`, set `deleted=false`
- (No materializer)

**Response**
```json
{
  "accepted": true,
  "highest_server_seq": 101,
  "statuses": [{ "source_change_id": 1, "status": "applied", "new_server_version": 1 }]
}
```

### 1.3 Phone applies upload result
```
_row_meta('note','K1').server_version = 1
DELETE FROM _sync_pending WHERE (table_name='note' AND pk_uuid='K1')
_sync_client_info.last_server_seq_seen = 101
_sync_client_info.next_change_id = 2
```

---

## 2) Laptop downloads the insert

**Request**
```
GET /sync/download?after=0&limit=1000&schema=app   (auth: user U1, device L)
```

**Server response**
```json
{
  "changes": [{
    "server_id": 101, "schema":"app","table":"note","op":"INSERT","pk":"K1",
    "payload": {"id":"K1","title":"Hello","content":"from phone","updated_at":"2025-08-09T10:00:00Z"},
    "server_version": 1, "deleted": false, "source_id": "P", "source_change_id": 1
  }],
  "has_more": false, "next_after": 101
}
```

**Laptop apply**
- Skip-own? **No** (origin is `P`)
- Upsert into `note` from payload
- Set `_row_meta('note','K1') = { server_version=1, deleted=0 }`
- Set `_sync_client_info.last_server_seq_seen = 101`

---

## 3) Both edit concurrently (conflict demo)

### 3.1 Phone updates title
**Phone local**
```sql
UPDATE note SET title='Hello (phone)', updated_at='2025-08-09T10:05:00Z' WHERE id='K1';
```
**Triggers**
```
_sync_pending upsert: ('note','K1', op='UPDATE', base_version=1)
```

**Phone upload**
```json
POST /sync/upload
{
  "last_server_seq_seen": 101,
  "changes": [{
    "source_change_id": 2,
    "schema":"app","table":"note","op":"UPDATE","pk":"K1",
    "server_version": 1,
    "payload": {"id":"K1","title":"Hello (phone)","content":"from phone","updated_at":"2025-08-09T10:05:00Z"}
  }]
}
```
**Server**
- Log → `server_id=102`
- Version gate: `1 → 2` (OK)

**Response**
```json
{ "accepted": true, "highest_server_seq": 102,
  "statuses": [{ "source_change_id": 2, "status":"applied", "new_server_version": 2 }] }
```

**Phone apply result**
```
_row_meta('note','K1').server_version = 2
DELETE _sync_pending('note','K1')
last_server_seq_seen = 102
```

### 3.2 Laptop updates same row (now stale)
**Laptop local**
```sql
UPDATE note SET title='Hello (laptop)', updated_at='2025-08-09T10:05:10Z' WHERE id='K1';
```
**Triggers**
```
_sync_pending upsert: ('note','K1', op='UPDATE', base_version=1)   -- base_version still 1 locally
```

**Laptop upload (stale)**
```json
POST /sync/upload
{
  "last_server_seq_seen": 101,
  "changes": [{
    "source_change_id": 1,
    "schema":"app","table":"note","op":"UPDATE","pk":"K1",
    "server_version": 1,
    "payload": {"id":"K1","title":"Hello (laptop)","content":"from phone","updated_at":"2025-08-09T10:05:10Z"}
  }]
}
```
**Server**
- Log → `server_id=103` (idempotent stream)
- Version gate fails (`current=2`, incoming=1) → **conflict**
- Return authoritative row (sv=2, payload latest)

**Response**
```json
{
  "accepted": true, "highest_server_seq": 103,
  "statuses": [{
    "source_change_id": 1,
    "status": "conflict",
    "server_row": {
      "schema_name":"app","table_name":"note","id":"K1",
      "server_version": 2, "deleted": false,
      "payload": {"id":"K1","title":"Hello (phone)","content":"from phone","updated_at":"2025-08-09T10:05:00Z"}
    }
  }]
}
```

**Laptop conflict handling (default: merge → retry)**
- Resolver merges: choose `title="Hello (laptop)"` (client-wins), base on server sv=2
- Write merged row locally
- Rewrite `_sync_pending('note','K1') = { op='UPDATE', base_version=2 }`
- Allocate new SCID and re-upload

**Laptop retry upload**
```json
{
  "last_server_seq_seen": 103,
  "changes": [{
    "source_change_id": 2,
    "schema":"app","table":"note","op":"UPDATE","pk":"K1",
    "server_version": 2,
    "payload": {"id":"K1","title":"Hello (laptop)","content":"from phone","updated_at":"2025-08-09T10:05:10Z"}
  }]
}
```
**Server**
- Log → `server_id=104`
- Version gate: `2 → 3` (applied)

**Response**
```json
{ "accepted": true, "highest_server_seq": 104,
  "statuses": [{ "source_change_id": 2, "status":"applied", "new_server_version": 3 }] }
```

**Laptop apply result**
```
_row_meta('note','K1').server_version = 3
DELETE _sync_pending('note','K1')
last_server_seq_seen = 104
```

---

## 4) Phone deletes the note

**Phone local**
```sql
DELETE FROM note WHERE id='K1';
```
**Triggers**
```
_row_meta('note','K1').deleted = 1    (if row exists)
_sync_pending upsert: ('note','K1', op='DELETE', base_version=3)
```

**Phone upload**
```json
POST /sync/upload
{
  "last_server_seq_seen": 104,
  "changes": [{
    "source_change_id": 3,
    "schema":"app","table":"note","op":"DELETE","pk":"K1","server_version": 3
  }]
}
```
**Server**
- Log → `server_id=105`
- Version gate: `3 → 4`, set `deleted=true`

**Response**
```json
{ "accepted": true, "highest_server_seq": 105,
  "statuses": [{ "source_change_id": 3, "status":"applied", "new_server_version": 4 }] }
```

**Phone apply**
```
_row_meta('note','K1').server_version = 4, deleted=1
DELETE _sync_pending('note','K1')
last_server_seq_seen = 105
```

**Laptop download after 104**
```
GET /sync/download?after=104&limit=1000&schema=app
```
**Server → Laptop**
```json
{
  "changes": [{
    "server_id": 105, "schema": "app", "table":"note", "op":"DELETE",
    "pk":"K1", "payload": null, "server_version": 4, "deleted": true,
    "source_id": "P", "source_change_id": 3
  }],
  "has_more": false, "next_after": 105
}
```

**Laptop apply**
- No local pending for K1 → apply delete locally, set meta `sv=4, deleted=1`
- `_sync_client_info.last_server_seq_seen = 105`

---

## 5) What `_sync_pending` is doing throughout

- It **coalesced** multiple local edits (e.g., phone updates before upload) to a single row.
- It **carried** the stable `base_version` used as `server_version` in the upload.
- On conflict, it was **rewritten** to `UPDATE` with `base_version` bumped to the server’s `server_version` before retry.
- It never stored the **payload** — that was read from the business table at send time — so there’s no bulky duplication.

---

## TL;DR
1) Local write → trigger marks `_sync_pending` and ensures `_row_meta`.
2) Upload uses `_sync_pending.base_version` and serialized current row; server bumps version or returns conflict.
3) Client applies result: clear `_sync_pending` and bump `_row_meta` **or** merge + rewrite `_sync_pending` and retry.
4) Download applies other devices’ changes, skipping own (`source_id`), updating business rows + `_row_meta`.
