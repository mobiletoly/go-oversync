CREATE TABLE app_evolution.table_run (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 epoch bigint NOT NULL, plan_id uuid NOT NULL UNIQUE, bounds jsonb NOT NULL,
 source_digest text NOT NULL, watermark bigint NOT NULL,
 phase text NOT NULL DEFAULT 'cleanup', batches bigint NOT NULL DEFAULT 0,
 evidence_bytes bigint NOT NULL DEFAULT 0, history_bytes bigint NOT NULL DEFAULT 0,
 bundles bigint NOT NULL DEFAULT 0, census_cursor bigint NOT NULL DEFAULT 0,
 next_batch_at timestamptz NOT NULL DEFAULT transaction_timestamp(), census_complete boolean NOT NULL DEFAULT false, certificate jsonb,
 validation_cursor jsonb NOT NULL DEFAULT '{}'
);
CREATE TABLE app_evolution.table_cleanup (
 operation_id uuid PRIMARY KEY, epoch bigint NOT NULL, result jsonb NOT NULL
);
CREATE TABLE app_evolution.table_scopes (
 epoch bigint NOT NULL, sequence bigint NOT NULL, scope_id text NOT NULL, incarnation uuid NOT NULL,
 cursor_key uuid, populated boolean NOT NULL DEFAULT false, PRIMARY KEY(epoch,scope_id)
);
CREATE TABLE app_evolution.table_batches (
 operation_id uuid PRIMARY KEY, epoch bigint NOT NULL, descriptor jsonb NOT NULL,
 attempts integer NOT NULL DEFAULT 0, replaces uuid UNIQUE REFERENCES app_evolution.table_batches(operation_id), hash text NOT NULL, state text NOT NULL DEFAULT 'pending', outcome jsonb, error text,
 evidence_reserve bigint NOT NULL, history_reserve bigint NOT NULL
);
CREATE UNIQUE INDEX table_one_pending ON app_evolution.table_batches(epoch) WHERE state='pending';
REVOKE ALL ON app_evolution.table_run,app_evolution.table_cleanup,app_evolution.table_scopes,app_evolution.table_batches FROM PUBLIC;
