<!-- moved to specs -->
```
# ======================================================================
# Offline-first multi-device sync (SQLite client) — PRODUCTION PSEUDOCODE
# With Guest Mode, Sign-In/Out, CRUD Buttons, Persistent Session
# ======================================================================
# What this solves
# ----------------
# 1) Users can use the app fully offline (guest) and later sign in to sync.
# 2) Auth persists across launches; token refresh handled; 401/403 handled.
# 3) Optimistic concurrency (server_version) + idempotent uploads (user,source,SCID).
# 4) Reinstall-safe via /sync/bootstrap when local counters are untrusted.
# 5) Clear UX: optimistic CRUD, banners, badges, conflict hints.
#
# IMPORTANT SCOPING DECISION:
#   This SQLite database is **per user**. Either:
#   - PER_USER_DB = true: one DB file per user (recommended), OR
#   - PER_USER_DB = false: on sign-in to a different user, hard-reset this DB.
#
# Client tables (SQLite, no user_id columns because DB is per-user)
#   _sync_client_info(user_id TEXT PK, source_id TEXT, next_change_id INT,
#                     last_server_seq_seen INT, hydrated BOOL)
#   _sync_row_meta(table_name TEXT, pk_uuid TEXT, server_version INT,
#                  deleted INT, updated_at TEXT, PRIMARY KEY(table_name, pk_uuid))
#   _sync_pending(table_name TEXT, pk_uuid TEXT, op TEXT, base_version INT,
#                 scid INT NULL, attempts INT DEFAULT 0, queued_at TEXT,
#                 PRIMARY KEY(table_name, pk_uuid))
#
# Coalescing rules for _sync_pending (enforced by triggers/UPSERT):
#   existing \ new   | INSERT | UPDATE | DELETE
#   -----------------+--------+--------+--------
#   none             | INSERT | UPDATE | DELETE
#   INSERT           | INSERT | INSERT | (DROP)   -- insert→delete = cancel
#   UPDATE           | UPDATE | UPDATE | DELETE
#   DELETE           | UPDATE | UPDATE | DELETE   -- delete→insert = resurrect as update
#
# Server endpoints (Option A auth: JWT {sub=user_id, did=source_id})
#   GET  /sync/bootstrap   # when local state missing/untrusted or DID change
#   POST /sync/upload
#   GET  /sync/download?after=<sid>&limit=<n>&schema=app
# ======================================================================


# --------------------------- APP LIFECYCLE ---------------------------

APP.onLaunch():
  DB.open()
  PRAGMA journal_mode=WAL
  PRAGMA foreign_keys=ON
  PRAGMA busy_timeout=5000
  ensureSQLiteSchema()                                        # business + _sync_* + triggers

  Net.watchConnectivity(ON_CONNECTIVITY_CHANGE)
  Auth.watchTokenRefresh(ON_TOKEN_REFRESH)

  # Stable device identity (survives reinstalls ideally)
  did := SecureStore.read("source_id")
  if did == "": did := UUIDv4(); SecureStore.write("source_id", did)

  # Restore session if possible
  session := Auth.restoreSession()                            # {jwt}? or nil
  if session == nil:
    Session.clear()
    UI.setBanner("Offline mode. Sign in to sync.")
    UI.setPendingBadge(DB._sync_pending.count())
    return

  if !Auth.ensureFreshJWT(session):
    Session.clear()
    UI.setBanner("Session expired. Please sign in.")
    UI.setPendingBadge(DB._sync_pending.count())
    return

  # Enforce per-user DB policy
  if PER_USER_DB and DB.boundUser() not in {session.jwt.sub, null} and DB.boundUser() != session.jwt.sub:
    # This DB belongs to a different user; switch DB file.
    DB.close()
    DB := DBManager.openDBForUser(session.jwt.sub)
    ensureSQLiteSchema()

  Session.setJWT(session.jwt)
  Session.setUserAndSource(session.jwt.sub, did)
  maybeBootstrapAndHydrateOnResume()                          # blocking only if needed
  START_SYNC_LOOPS()
  UI.setBanner(Net.isOnline() ? "Syncing…" : "Offline")
  UI.setPendingBadge(DB._sync_pending.count())


APP.onForeground():
  if Session.isActive():
    SIGNAL Downloader wake
    SIGNAL Uploader wake
  UI.setBanner(Net.isOnline() ? "Syncing…" : "Offline")


APP.onBackground():
  if Session.isActive():
    Uploader.runOnce(maxBatch=50, softTimeout=2s)             # flush for perceived speed


APP.onTerminate():
  STOP_SYNC_LOOPS()
  DB.close()


# ------------------------------ BUTTONS ------------------------------

UI.onSignInButtonTapped():
  creds := UI.showSignInDialog()
  if creds == nil: return

  jwt := Auth.exchange(creds)                                 # {sub, did?, exp}
  if jwt == nil: UI.toast("Sign in failed"); return

  localDid := SecureStore.read("source_id")
  if jwt.did == "" or jwt.did != localDid:
    jwt2 := Auth.remintTokenWithDID(localDid)                 # preferred to keep local DID
    if jwt2 != nil: jwt = jwt2
    else:
      # Accept server DID, replace local identity
      newDid := (jwt.did != "" ? jwt.did : localDid)
      SecureStore.write("source_id", newDid)
      localDid = newDid

  # Per-user DB policy
  if PER_USER_DB:
    if DB.boundUser() not in {jwt.sub, null} and DB.boundUser() != jwt.sub:
      DB.close()
      DB := DBManager.openDBForUser(jwt.sub)                  # switch file
      ensureSQLiteSchema()
  else:
    # Single DB mode: on different user sign-in, wipe to avoid mixing data
    if DB._sync_client_info.existsAnyUser() and DB.boundUser() != jwt.sub:
      hardResetLocalDB()                                      # drop business + _sync_* tables
      ensureSQLiteSchema()

  Session.setJWT(jwt)
  Session.setUserAndSource(jwt.sub, localDid)

  DB.tx:
    ci := DB._sync_client_info.get(jwt.sub)
    if ci == nil:
      DB._sync_client_info.insert({ user_id: jwt.sub, source_id: localDid,
                                    next_change_id: 0, last_server_seq_seen: 0, hydrated: false })
      DB.bindUser(jwt.sub)                                    # remember which user owns this DB
    else:
      DB._sync_client_info.updateSource(jwt.sub, localDid)
      DB.bindUser(jwt.sub)

  maybeBootstrapAndHydrateOnFirstSignIn()                     # blocking (spinner)
  START_SYNC_LOOPS()
  UI.toast("Signed in")
  UI.setBanner(Net.isOnline() ? "Syncing…" : "Offline")
  UI.setPendingBadge(DB._sync_pending.count())


UI.onSignOutButtonTapped():
  STOP_SYNC_LOOPS()
  Auth.clear()
  Session.clearJWT()
  if POLICY.clearOnLogout:
    hardResetLocalDB()
    ensureSQLiteSchema()
  UI.toast("Signed out")
  UI.setBanner("Offline mode. Sign in to sync.")
  UI.setPendingBadge(DB._sync_pending.count())


UI.onAddRecord(table, fields):
  pk := UUIDv4()
  now := nowISO8601()
  DB.tx:
    DB.business.insert(table, { id: pk, ...fields, updated_at: now })  # triggers enqueue INSERT
  UI.setPendingBadge(DB._sync_pending.count())
  if Session.isActive() and Net.isOnline(): SIGNAL Uploader wake


UI.onUpdateRecord(table, pk, patch):
  now := nowISO8601()
  DB.tx:
    DB.business.update(table, pk, patch + { updated_at: now })          # triggers enqueue UPDATE
  UI.setPendingBadge(DB._sync_pending.count())
  if Session.isActive() and Net.isOnline(): SIGNAL Uploader wake


UI.onDeleteRecord(table, pk):
  DB.tx:
    DB.business.delete(table, pk)                                       # triggers enqueue DELETE
  UI.setPendingBadge(DB._sync_pending.count())
  if Session.isActive() and Net.isOnline(): SIGNAL Uploader wake


# -------------------------- CONNECTIVITY/TOKEN --------------------------

ON_CONNECTIVITY_CHANGE(online):
  UI.setBanner(online ? "Syncing…" : "Offline")
  if Session.isActive() and online:
    SIGNAL Downloader wake
    SIGNAL Uploader wake

ON_TOKEN_REFRESH(newJWT):
  if newJWT == nil:
    # Refresh failed; loops will hit 401 and stop; prompt sign-in
    return
  Session.setJWT(newJWT)                                               # next HTTP uses fresh token


# ----------------------------- HELPERS (UX) -----------------------------

maybeBootstrapAndHydrateOnFirstSignIn():
  user := Session.userID(); did := Session.sourceID()
  ci := DB._sync_client_info.get(user)
  need := (ci == nil) OR (ci.source_id != did) OR (ci.hydrated == false)

  if !need: return

  bs := BOOTSTRAP(user, did)                                           # { next_scid, hydrate_after, known_source }
  if bs == FAIL: return                                                # handled (401/403) inside

  DB._sync_client_info.setNextChangeID(user, bs.next_scid)
  DB._sync_client_info.setLastServerSeqSeen(user, bs.hydrate_after)
  echo := (bs.known_source ? false : true)

  HYDRATE(user, did, echo)                                             # blocking (spinner)
  UI.toast("Initial sync complete")


maybeBootstrapAndHydrateOnResume():
  user := Session.userID(); did := Session.sourceID()
  ci := DB._sync_client_info.get(user)
  if ci == nil or ci.source_id != did or ci.hydrated == false:
    bs := BOOTSTRAP(user, did)
    if bs == FAIL: return
    DB._sync_client_info.setNextChangeID(user, bs.next_scid)
    DB._sync_client_info.setLastServerSeqSeen(user, bs.hydrate_after)
    HYDRATE(user, did, echoSuppression=(bs.known_source ? false : true))


# ------------------------------ BOOTSTRAP ------------------------------

BOOTSTRAP(user_id, source_id):
  UI.showSpinner("Preparing sync…")
  backoff := 1s + jitter(200ms)
  while true:
    resp := HTTP.GET("/sync/bootstrap", auth=Session.JWT())
    if resp.status == 200 and resp.body.next_scid exists:
      UI.hideSpinner()
      return {
        next_scid:     resp.body.next_scid,
        hydrate_after: resp.body.hydrate_after,          # typically 0
        known_source:  resp.body.known_source
      }
    if resp.status == 401:
      UI.hideSpinner(); UI.setBanner("Session expired. Sign in.")
      STOP_SYNC_LOOPS(); return FAIL
    if resp.status == 403:
      UI.hideSpinner(); UI.setBanner("Access revoked."); STOP_SYNC_LOOPS(); return FAIL
    sleep(backoff); backoff := min(60s, backoff * 2 + jitter(200ms))


# ------------------------------ HYDRATION ------------------------------

# HYDRATE: one-time rebuild of the local DB from the server stream
# - Always **include self** (omit exclude_source_id), so we replay our own history too.
# - Starts from the bootstrap-provided cursor already saved in _sync_client_info (typically 0).
# - Applies pages atomically; updates last_server_seq_seen; marks hydrated=true on success.
# - Robust to 401/403 and transient failures (backoff+jitter).

HYDRATE(user_id, my_source_id):
  ASSERT Session.isActive()

  DB._sync_client_info.setHydrated(user_id, false)

  after := DB._sync_client_info.get(user_id).last_server_seq_seen   # set by /sync/bootstrap (usually 0)
  limit := DOWNLOAD_LIMIT                                           # e.g., 1000
  schema := SCHEMA_NAME                                             # e.g., "app"

  backoff := 1s + jitter(200ms)

  while true:
    url := "/sync/download?after="+after+"&limit="+limit+"&schema="+schema+"&include_self=true"
    resp := HTTP.GET(url, auth=Session.JWT())

    if resp.status == 401:
      UI.setBanner("Session expired. Sign in.")
      RETURN FAIL
    if resp.status == 403:
      UI.setBanner("Access revoked.")
      RETURN FAIL
    if resp.status != 200:
      sleep(backoff); backoff := min(60s, backoff * 2 + jitter(200ms))
      continue
    backoff := 1s + jitter(200ms)  # reset on success

    # Apply page atomically
    DB.tx:
      for change in resp.changes:                      # server_id ascending
        # During HYDRATE we always allow self changes (no echo suppression here).
        key := (change.table, change.pk)
        hasPending := DB._sync_pending.exists(key)

        if hasPending:
          # Conflict-on-download: keep local intent but align to server's version
          DB._sync_row_meta.upsert(key,
                                   server_version=change.server_version,
                                   deleted=change.deleted)
          DB._sync_pending.rewriteToUpdate(key,
                                           base_version=change.server_version)
          # We intentionally do NOT overwrite business tables here to preserve local edits
          continue

        # No local pending → apply server directly
        if change.op == "DELETE" or change.deleted == true:
          DB.business.delete(change.table, change.pk)                  # ignore if missing
          DB._sync_row_meta.upsert(key, server_version=change.server_version, deleted=1)
        else:
          DB.business.upsert(change.table, change.pk, change.payload)  # idempotent upsert
          DB._sync_row_meta.upsert(key, server_version=change.server_version, deleted=0)

      if resp.changes not empty:
        after := resp.next_after
        DB._sync_client_info.setLastServerSeqSeen(user_id, after)

    # Paging complete?
    if resp.has_more == false:
      break
    # else loop to fetch next page immediately

  DB._sync_client_info.setHydrated(user_id, true)
  UI.setBanner(Net.isOnline() ? "All changes saved" : "Offline")
  RETURN OK

# -------------------------------------------------------------
# Download URL builder using include_self=true|false
# -------------------------------------------------------------
buildDownloadURL(after, limit, schema, includeSelf):
  base := "/sync/download?after="+after+"&limit="+limit+"&schema="+schema
  return base + "&include_self=" + (includeSelf ? "true" : "false")

# -------------------------- STEADY-STATE LOOPS --------------------------

START_SYNC_LOOPS():
  Uploader.start()
  Downloader.start()

STOP_SYNC_LOOPS():
  Uploader.stop()
  Downloader.stop()

Downloader.run():
  while running:
    if !Net.isOnline(): SLEEP(2s + jitter(300ms)); continue
    ci := DB._sync_client_info.get(Session.userID())
    after := ci.last_server_seq_seen

    resp := HTTP.GET("/sync/download?after="+after+"&limit=1000&schema=app", auth=Session.JWT())
    if resp.status == 401: UI.setBanner("Session expired. Sign in."); STOP_SYNC_LOOPS(); break
    if resp.status == 403: UI.setBanner("Access revoked."); STOP_SYNC_LOOPS(); break
    if resp.status != 200: WAIT.backoffAndRetry(); continue

    DB.tx:
      for change in resp.changes:
        APPLY_SERVER_CHANGE(change, Session.userID(), Session.sourceID(), echoSuppression=true)
      if resp.changes not empty:
        DB._sync_client_info.setLastServerSeqSeen(Session.userID(), resp.next_after)

    if resp.has_more: continue
    UI.setBanner(Net.isOnline() ? "All changes saved" : "Offline")
    SLEEP(2s + jitter(300ms))

Uploader.run():
  while running:
    if !Net.isOnline(): SLEEP(2s + jitter(300ms)); continue

    batch := DB._sync_pending.selectOldestN(200)
    if batch.empty():
      UI.setBanner(Net.isOnline() ? "All changes saved" : "Offline")
      SLEEP(2s + jitter(300ms)); continue

    # Stage SCIDs for idempotent retry
    DB.tx:
      for item in batch:
        if item.scid is NULL:
          scid := DB._sync_client_info.consumeNextChangeID(Session.userID())
          DB._sync_pending.setSCID(item.key, scid)

    # Build request (robust serializer)
    changes := []
    for item in batch:
      payload := NULL
      if item.op in ["INSERT","UPDATE"]:
        row := DB.business.load(item.table, item.pk)
        if row == nil:
          # Row vanished locally. If meta.deleted=1 → convert to DELETE; else drop with audit.
          meta := DB._sync_row_meta.get(item.table, item.pk)
          if meta != nil and meta.deleted == 1:
            item.op = "DELETE"; payload = NULL
          else:
            _upload_audit.debug("drop-missing-row", item)
            DB._sync_pending.delete(item.key)
            continue
        else:
          payload := SERIALIZE(row)

      changes.append({
        source_change_id: item.scid,
        schema: "app",
        table: item.table,
        op: item.op,
        pk: item.pk,
        server_version: item.base_version,
        payload: payload
      })

    req := { last_server_seq_seen: DB._sync_client_info.get(Session.userID()).last_server_seq_seen,
             changes: changes }

    resp := HTTP.POST("/sync/upload", json=req, auth=Session.JWT())
    if resp.status == 401: UI.setBanner("Session expired. Sign in."); STOP_SYNC_LOOPS(); break
    if resp.status == 403: UI.setBanner("Access revoked."); STOP_SYNC_LOOPS(); break
    if resp.status != 200: WAIT.backoffAndRetry(); continue

    # Apply results
    DB.tx:
      for status in resp.statuses:
        key  := DB._sync_pending.findKeyBySCID(status.source_change_id)
        if key == nil: continue   # already coalesced away
        item := DB._sync_pending.get(key)

        if status.status == "applied":
          DB._sync_row_meta.setVersionAndDeleted(key, status.new_server_version, (item.op == "DELETE") ? 1 : 0)
          DB._sync_pending.delete(key)

        else if status.status == "conflict":
          srv := status.server_row                                # { server_version, deleted, payload? }
          localPayload := (item.op == "DELETE") ? NULL : SERIALIZE(DB.business.load(key))
          merged, keepLocal := RESOLVER.Merge(item.table, item.pk, srv.payload, localPayload)

          if keepLocal == false:
            if srv.deleted:
              DB.business.delete(item.table, item.pk)
              DB._sync_row_meta.setVersionAndDeleted(key, srv.server_version, 1)
            else:
              DB.business.upsert(item.table, item.pk, srv.payload)
              DB._sync_row_meta.setVersionAndDeleted(key, srv.server_version, 0)
            DB._sync_pending.delete(key)
          else:
            DB.business.upsert(item.table, item.pk, merged)
            DB._sync_row_meta.setVersionAndDeleted(key, srv.server_version, 0)
            DB._sync_pending.rewriteToUpdate(key, base_version=srv.server_version)  # keep same SCID

        else if status.status == "invalid":
          DB._sync_pending.incrementAttempts(key)

      if resp.highest_server_seq exists:
        DB._sync_client_info.setLastServerSeqSeen(Session.userID(), resp.highest_server_seq)

    UI.setPendingBadge(DB._sync_pending.count())
    UI.setBanner(Net.isOnline() ? "Syncing…" : "Offline")


# ------------------------------ APPLY CHANGE ------------------------------

APPLY_SERVER_CHANGE(change, user_id, my_source_id, echoSuppression):
  if echoSuppression and change.source_id == my_source_id:
    return  # skip echo except during hydration for known sources

  key := (change.table, change.pk)
  hasPending := DB._sync_pending.exists(key)

  if hasPending:
    # Conflict-on-download: preserve local intent; align base_version to server
    DB._sync_row_meta.upsert(key, server_version=change.server_version, deleted=change.deleted)
    DB._sync_pending.rewriteToUpdate(key, base_version=change.server_version)
    UI.showConflictIndicator(key)
    return

  if change.op == "DELETE" or change.deleted == true:
    DB.business.delete(change.table, change.pk)                        # ignore if missing
    DB._sync_row_meta.upsert(key, server_version=change.server_version, deleted=1)
  else:
    DB.business.upsert(change.table, change.pk, change.payload)
    DB._sync_row_meta.upsert(key, server_version=change.server_version, deleted=0)


# ------------------------------ HELPERS ------------------------------

ensureSQLiteSchema():
  # Create _sync_client_info, _sync_row_meta, _sync_pending, business tables if absent.
  # Create triggers implementing coalescing rules and meta maintenance.
  # Add indexes:
  #   CREATE INDEX _pending_q ON _sync_pending(queued_at);
  #   CREATE INDEX _meta_tbl_pk ON _sync_row_meta(table_name, pk_uuid);

SERIALIZE(row):
  return JSON(row)  # include updated_at; exclude sync meta

RESOLVER.Merge(table, pk, serverPayload, localPayload):
  # Default:
  #   if server deleted → keepLocal=false
  #   else client-wins for whitelisted fields (title, content, done), preserve server-only
  return (mergedPayload, keepLocal)

WAIT.backoffAndRetry():
  sleep(currentBackoff + jitter(300ms))
  currentBackoff := min(60s, currentBackoff * 2)

hardResetLocalDB():
  DROP TABLE IF EXISTS all business tables
  DROP TABLE IF EXISTS _sync_pending, _sync_row_meta, _sync_client_info
  VACUUM
```
