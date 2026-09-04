CREATE SCHEMA IF NOT EXISTS app_evolution;
CREATE TABLE IF NOT EXISTS app_evolution.migrations (
    version bigint PRIMARY KEY,
    identity jsonb NOT NULL,
    state text NOT NULL,
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    failed_at timestamptz
);
CREATE TABLE IF NOT EXISTS app_evolution.deployment (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    revision bigint NOT NULL,
    contract jsonb NOT NULL,
    generations jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS app_evolution.barrier (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    closed boolean NOT NULL DEFAULT true,
    revision bigint NOT NULL DEFAULT 1
);
INSERT INTO app_evolution.barrier(singleton) VALUES(true) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS app_evolution.scope_inventory (
    sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    scope_id text NOT NULL,
    incarnation uuid PRIMARY KEY,
    deleted boolean NOT NULL DEFAULT false
);
CREATE UNIQUE INDEX IF NOT EXISTS scope_inventory_active ON app_evolution.scope_inventory(scope_id) WHERE NOT deleted;
CREATE TABLE IF NOT EXISTS app_evolution.jobs (
    job text PRIMARY KEY,
    revision text NOT NULL,
    algorithm text NOT NULL,
    enumeration_policy text NOT NULL DEFAULT 'keyset-incarnation-v1',
    preparation_policy text NOT NULL DEFAULT 'sql-v3',
    epoch bigint NOT NULL,
    census text NOT NULL,
    transition_revision text NOT NULL,
    phase text NOT NULL,
    watermark bigint NOT NULL,
    cursor_sequence bigint NOT NULL DEFAULT 0,
    cursor_key uuid,
    state text NOT NULL,
    pause_reason text NOT NULL DEFAULT '',
    bounds jsonb NOT NULL,
    scanned bigint NOT NULL DEFAULT 0,
    changed bigint NOT NULL DEFAULT 0,
    skipped bigint NOT NULL DEFAULT 0,
    stale bigint NOT NULL DEFAULT 0,
    failed bigint NOT NULL DEFAULT 0,
    history_bytes bigint NOT NULL DEFAULT 0,
    bundles bigint NOT NULL DEFAULT 0,
    retries bigint NOT NULL DEFAULT 0,
    duration_ns bigint NOT NULL DEFAULT 0,
    lock_wait_ns bigint NOT NULL DEFAULT 0,
    last_capacity jsonb
);
CREATE TABLE IF NOT EXISTS app_evolution.batches (
    operation_id uuid PRIMARY KEY,
    job text NOT NULL,
    ordinal bigint NOT NULL UNIQUE,
    evidence_reserve bigint NOT NULL DEFAULT 0,
    history_reserve bigint NOT NULL DEFAULT 0,
    descriptor bytea NOT NULL,
    spec jsonb NOT NULL,
    hash text NOT NULL,
    state text NOT NULL DEFAULT 'pending',
    outcome jsonb,
    metrics_complete boolean NOT NULL DEFAULT false,
    history_bytes bigint NOT NULL DEFAULT 0,
    bundles bigint NOT NULL DEFAULT 0,
    UNIQUE(job,hash)
);
CREATE UNIQUE INDEX IF NOT EXISTS one_pending_batch ON app_evolution.batches(job) WHERE state='pending';
CREATE TABLE IF NOT EXISTS app_evolution.row_work (
    job text NOT NULL,
    incarnation uuid NOT NULL,
    row_key uuid NOT NULL,
    input_digest text NOT NULL,
    category text NOT NULL,
    PRIMARY KEY(job,incarnation,row_key)
);
CREATE TABLE IF NOT EXISTS app_evolution.certificates (
    certificate_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    certificate jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE app_evolution.maintenance (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 requested boolean NOT NULL DEFAULT true,
 epoch bigint NOT NULL DEFAULT 1,
 worker uuid, worker_pid integer,
 client_policy text NOT NULL DEFAULT '',
 completed_at timestamptz
);
INSERT INTO app_evolution.maintenance(singleton) VALUES(true);
CREATE TABLE app_evolution.worker_transactions (
 backend integer PRIMARY KEY, transaction_id xid8 NOT NULL, epoch bigint NOT NULL, worker uuid NOT NULL
);
CREATE TABLE app_evolution.census_scopes (
 epoch bigint NOT NULL, sequence bigint NOT NULL, scope_id text NOT NULL,
 incarnation uuid NOT NULL, deleted boolean NOT NULL, PRIMARY KEY(epoch,incarnation)
);
CREATE TABLE app_evolution.census_rows (
 epoch bigint NOT NULL, scope_id text NOT NULL, row_key uuid NOT NULL,
 PRIMARY KEY(epoch,scope_id,row_key)
);
-- No runtime role may write maintenance, census, certificates, or worker permits.
REVOKE ALL ON app_evolution.maintenance,app_evolution.worker_transactions,app_evolution.census_scopes,app_evolution.census_rows FROM PUBLIC;
CREATE FUNCTION app_evolution.guard_write() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE closed_now boolean; permitted boolean;
BEGIN
 PERFORM pg_advisory_xact_lock_shared(1869833075,9);
 SELECT requested INTO STRICT closed_now FROM app_evolution.maintenance WHERE singleton;
 IF closed_now THEN
  SELECT EXISTS(SELECT 1 FROM app_evolution.worker_transactions t
   JOIN app_evolution.maintenance m ON m.singleton AND m.epoch=t.epoch AND m.worker=t.worker
   JOIN pg_locks l ON l.pid=m.worker_pid AND l.locktype='advisory' AND l.classid=1869833075 AND l.objid=7 AND l.objsubid=2 AND l.granted
   WHERE t.backend=pg_backend_pid() AND t.transaction_id=pg_current_xact_id()) INTO permitted;
  IF NOT permitted THEN RAISE EXCEPTION 'application_admission_closed'; END IF;
 END IF;
 RETURN NULL;
END
$$;
CREATE TRIGGER aa_app_scope_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON app_evolution.scope_inventory
FOR EACH STATEMENT EXECUTE FUNCTION app_evolution.guard_write();

CREATE TABLE app_evolution.census_progress (
 job text PRIMARY KEY, phase text NOT NULL, sequence bigint NOT NULL DEFAULT 0,
 scope_id text NOT NULL DEFAULT '', row_key uuid, hash_state bytea NOT NULL
);
CREATE INDEX census_scope_sequence ON app_evolution.census_scopes(epoch,sequence);

CREATE TABLE app_evolution.repair_evidence (
 repair_operation uuid PRIMARY KEY, failed_operation uuid NOT NULL,
 attempt_no bigint NOT NULL CHECK(attempt_no > 0), is_current boolean NOT NULL,
 row_key uuid NOT NULL, input_digest text NOT NULL, category text NOT NULL,
 UNIQUE(failed_operation,attempt_no)
);
CREATE UNIQUE INDEX one_current_repair ON app_evolution.repair_evidence(failed_operation) WHERE is_current;
