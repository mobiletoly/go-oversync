CREATE TABLE IF NOT EXISTS app_evolution.generation_ledger_meta (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 revision bigint NOT NULL CHECK(revision>0),
 maximum_id integer NOT NULL CHECK(maximum_id>0),
 capacity integer NOT NULL CHECK(capacity BETWEEN 1 AND 4096)
);
CREATE TABLE IF NOT EXISTS app_evolution.generation_ledger (
 generation_id integer PRIMARY KEY CHECK(generation_id>0),
 fingerprint text NOT NULL CHECK(fingerprint ~ '^[0-9a-f]{64}$'),
 declaration jsonb NOT NULL,
 allocated_revision bigint NOT NULL UNIQUE CHECK(allocated_revision>0),
 state text NOT NULL CHECK(state IN ('allocated','supported','retired','abandoned')),
 retired_revision bigint,
 CHECK((state IN ('allocated','supported') AND retired_revision IS NULL)
    OR (state IN ('retired','abandoned') AND retired_revision IS NOT NULL AND retired_revision>allocated_revision))
);
CREATE OR REPLACE FUNCTION app_evolution.guard_generation_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory'
  AND classid=1869833075 AND objid=6 AND objsubid=2 AND mode='ExclusiveLock' AND granted)
  OR NOT EXISTS(SELECT 1 FROM app_evolution.maintenance m CROSS JOIN app_evolution.barrier b
    WHERE m.singleton AND b.singleton AND m.requested AND b.closed AND m.epoch=b.revision)
 THEN RAISE EXCEPTION 'generation_authority_required'; END IF;
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'generation_identity_retained'; END IF;
 IF NEW.state='supported' AND (SELECT count(*) FROM app_evolution.generation_ledger WHERE state='supported' AND generation_id<>NEW.generation_id)>=2
 THEN RAISE EXCEPTION 'generation_support_capacity'; END IF;
 IF TG_OP='INSERT' AND EXISTS(SELECT 1 FROM app_evolution.generation_ledger_meta WHERE singleton AND (NEW.generation_id<=maximum_id OR NEW.allocated_revision<>revision+1))
 THEN RAISE EXCEPTION 'generation_allocation_immutable'; END IF;
 IF TG_OP='UPDATE' THEN
  IF NEW.generation_id<>OLD.generation_id OR NEW.fingerprint<>OLD.fingerprint
   OR NEW.declaration<>OLD.declaration OR NEW.allocated_revision<>OLD.allocated_revision
   OR OLD.state IN ('retired','abandoned')
   OR NOT ((OLD.state='allocated' AND NEW.state IN ('supported','abandoned'))
      OR (OLD.state='supported' AND NEW.state='retired'))
  THEN RAISE EXCEPTION 'generation_identity_immutable'; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE OR REPLACE TRIGGER generation_identity_guard BEFORE INSERT OR UPDATE OR DELETE
 ON app_evolution.generation_ledger FOR EACH ROW EXECUTE FUNCTION app_evolution.guard_generation_identity();
REVOKE ALL ON app_evolution.generation_ledger,app_evolution.generation_ledger_meta FROM PUBLIC;
CREATE TABLE IF NOT EXISTS app_evolution.rollout_operations (
 operation_id text PRIMARY KEY,
 digest text NOT NULL,
 plan jsonb NOT NULL,
 report jsonb NOT NULL,
 state text NOT NULL CHECK(state IN ('running','completed')),
 deleted_rows bigint NOT NULL DEFAULT 0,
 attempts integer NOT NULL DEFAULT 0,
 batches integer NOT NULL DEFAULT 0,
 evidence_bytes bigint NOT NULL DEFAULT 0,
 result jsonb,
 created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
 completed_at timestamptz
);
REVOKE ALL ON app_evolution.rollout_operations FROM PUBLIC;

CREATE OR REPLACE FUNCTION app_evolution.guard_rollout_operation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory'
  AND classid=1869833075 AND objid=6 AND objsubid=2 AND mode='ExclusiveLock' AND granted)
  OR NOT EXISTS(SELECT 1 FROM app_evolution.maintenance m CROSS JOIN app_evolution.barrier b
    WHERE m.singleton AND b.singleton AND m.requested AND b.closed AND m.epoch=b.revision)
 THEN RAISE EXCEPTION 'rollout_authority_required'; END IF;
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'rollout_identity_retained'; END IF;
 IF TG_OP='UPDATE' AND (OLD.operation_id<>NEW.operation_id OR OLD.digest<>NEW.digest
  OR OLD.plan<>NEW.plan OR OLD.report<>NEW.report OR OLD.created_at<>NEW.created_at
  OR OLD.state='completed' OR NEW.deleted_rows<OLD.deleted_rows
  OR NEW.attempts<OLD.attempts OR NEW.attempts>OLD.attempts+1
  OR NEW.batches<OLD.batches OR NEW.batches>OLD.batches+1
  OR NEW.evidence_bytes<OLD.evidence_bytes)
 THEN RAISE EXCEPTION 'rollout_identity_immutable'; END IF;
 IF NEW.attempts>(NEW.plan->>'MaxAttempts')::int OR NEW.batches>(NEW.plan->>'MaxBatches')::int
 OR NEW.evidence_bytes>(NEW.plan->>'MaxEvidenceBytes')::bigint
 THEN RAISE EXCEPTION 'rollout_budget_exhausted'; END IF;
 RETURN NEW;
END $$;
CREATE OR REPLACE TRIGGER rollout_operation_guard BEFORE INSERT OR UPDATE OR DELETE
 ON app_evolution.rollout_operations FOR EACH ROW EXECUTE FUNCTION app_evolution.guard_rollout_operation();
CREATE OR REPLACE FUNCTION app_evolution.guard_generation_meta() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory'
  AND classid=1869833075 AND objid=6 AND objsubid=2 AND mode='ExclusiveLock' AND granted)
  OR NOT EXISTS(SELECT 1 FROM app_evolution.maintenance m CROSS JOIN app_evolution.barrier b
    WHERE m.singleton AND b.singleton AND m.requested AND b.closed AND m.epoch=b.revision)
 THEN RAISE EXCEPTION 'generation_authority_required'; END IF;
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'generation_identity_retained'; END IF;
 IF TG_OP='UPDATE' AND (NEW.singleton<>OLD.singleton OR NEW.capacity<>OLD.capacity
   OR NEW.maximum_id<OLD.maximum_id OR NEW.revision<>OLD.revision+1)
 THEN RAISE EXCEPTION 'generation_revision_immutable'; END IF;
 IF NEW.maximum_id<>(SELECT max(generation_id) FROM app_evolution.generation_ledger)
  OR NEW.capacity<(SELECT count(*) FROM app_evolution.generation_ledger)
 THEN RAISE EXCEPTION 'generation_ledger_incomplete'; END IF;
 RETURN NEW;
END $$;
CREATE OR REPLACE TRIGGER generation_meta_guard BEFORE INSERT OR UPDATE OR DELETE
 ON app_evolution.generation_ledger_meta FOR EACH ROW EXECUTE FUNCTION app_evolution.guard_generation_meta();
CREATE TABLE IF NOT EXISTS app_evolution.rollout_certificates (
 deployment_revision bigint PRIMARY KEY,
 operation_id text NOT NULL REFERENCES app_evolution.rollout_operations(operation_id),
 certificate jsonb NOT NULL
);
REVOKE ALL ON app_evolution.rollout_certificates FROM PUBLIC;

CREATE TABLE IF NOT EXISTS app_evolution.rollout_batches (
 operation_id text NOT NULL REFERENCES app_evolution.rollout_operations(operation_id),
 batch_number integer NOT NULL,
 outcome jsonb NOT NULL,
 committed_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
 PRIMARY KEY(operation_id,batch_number)
);
REVOKE ALL ON app_evolution.rollout_batches FROM PUBLIC;
CREATE OR REPLACE FUNCTION app_evolution.guard_rollout_evidence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory'
  AND classid=1869833075 AND objid=6 AND objsubid=2 AND mode='ExclusiveLock' AND granted)
  OR NOT EXISTS(SELECT 1 FROM app_evolution.maintenance m CROSS JOIN app_evolution.barrier b
    WHERE m.singleton AND b.singleton AND m.requested AND b.closed AND m.epoch=b.revision)
 THEN RAISE EXCEPTION 'rollout_authority_required'; END IF;
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'rollout_evidence_immutable'; END IF;
 RETURN NEW;
END $$;
CREATE OR REPLACE TRIGGER rollout_batch_guard BEFORE INSERT OR UPDATE OR DELETE
 ON app_evolution.rollout_batches FOR EACH ROW EXECUTE FUNCTION app_evolution.guard_rollout_evidence();
CREATE OR REPLACE TRIGGER rollout_certificate_guard BEFORE INSERT OR UPDATE OR DELETE
 ON app_evolution.rollout_certificates FOR EACH ROW EXECUTE FUNCTION app_evolution.guard_rollout_evidence();

CREATE TABLE IF NOT EXISTS app_evolution.recovery_gate (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 cut_id text NOT NULL,
 plan_digest text NOT NULL CHECK(plan_digest ~ '^[0-9a-f]{64}$'),
 database_name text NOT NULL,
 authority_id text NOT NULL,
 epoch bigint NOT NULL CHECK(epoch>0),
 state text NOT NULL CHECK(state IN ('required','verified')),
 cut_sha256 text,
 archive_sha256 text,
 verification_sha256 text,
 verification jsonb,
 CHECK((state='required' AND cut_sha256 IS NULL AND archive_sha256 IS NULL AND verification_sha256 IS NULL AND verification IS NULL)
    OR (state='verified' AND cut_sha256 ~ '^[0-9a-f]{64}$' AND archive_sha256 ~ '^[0-9a-f]{64}$'
        AND verification_sha256 ~ '^[0-9a-f]{64}$' AND verification IS NOT NULL))
);
REVOKE ALL ON app_evolution.recovery_gate FROM PUBLIC;
CREATE OR REPLACE FUNCTION app_evolution.guard_recovery_gate() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory'
  AND classid=1869833075 AND objid=6 AND objsubid=2 AND mode='ExclusiveLock' AND granted)
  OR NOT EXISTS(SELECT 1 FROM app_evolution.maintenance m CROSS JOIN app_evolution.barrier b
    WHERE m.singleton AND b.singleton AND m.requested AND b.closed AND m.epoch=b.revision)
 THEN RAISE EXCEPTION 'rollout_authority_required'; END IF;
 IF TG_OP='DELETE' THEN
  IF OLD.state<>'required' THEN RAISE EXCEPTION 'recovery_evidence_immutable'; END IF;
  RETURN OLD;
 END IF;
 IF TG_OP='UPDATE' AND (OLD.state<>'required' OR NEW.state<>'verified'
  OR NEW.singleton<>OLD.singleton OR NEW.cut_id<>OLD.cut_id OR NEW.plan_digest<>OLD.plan_digest
  OR NEW.database_name<>OLD.database_name OR NEW.authority_id<>OLD.authority_id OR NEW.epoch<>OLD.epoch)
 THEN RAISE EXCEPTION 'recovery_evidence_immutable'; END IF;
 RETURN NEW;
END $$;
CREATE OR REPLACE TRIGGER recovery_gate_guard BEFORE INSERT OR UPDATE OR DELETE
 ON app_evolution.recovery_gate FOR EACH ROW EXECUTE FUNCTION app_evolution.guard_recovery_gate();
