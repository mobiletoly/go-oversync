// Copyright 2026 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
)

//go:embed push_outcome_layout_facts.json
var pushOutcomeLayoutJSON []byte

// The manifest is mechanically emitted from the authored fresh-schema DDL by
// TestOutcomeLayoutManifest, then checked on both supported PostgreSQL versions.
var pushOutcomeManagedLayoutFacts = func() []managedLayoutFact {
	var facts []managedLayoutFact
	if err := json.Unmarshal(pushOutcomeLayoutJSON, &facts); err != nil {
		panic(err)
	}
	return facts
}()

// pushOutcomeEnvelopeAllowance is the room r0001 reserves above the push body
// limit for the outcome envelope.
const pushOutcomeEnvelopeAllowance int64 = 8192

// maximumPushOutcomeCeiling is the largest ceiling the store accepts.
const maximumPushOutcomeCeiling = int64(maxMaxBytesPerBundle) + pushOutcomeEnvelopeAllowance

// minimumPushOutcomeCeiling is the smallest ceiling that can hold the outcome
// of a maximum-size push.
func minimumPushOutcomeCeiling(maxBytesPerBundle int) int64 {
	return int64(maxBytesPerBundle) + pushOutcomeEnvelopeAllowance
}

// newStorePushOutcomeCeiling is the ceiling a new push outcome store records:
// the explicit value, or the minimum for the configured bundle limit.
func newStorePushOutcomeCeiling(cfg *ServiceConfig) int64 {
	if cfg.MaxPushOutcomeDecodedBytes != 0 {
		return cfg.MaxPushOutcomeDecodedBytes
	}
	return minimumPushOutcomeCeiling(cfg.MaxBytesPerBundle)
}

// pushOutcomeCeilingBytes returns the ceiling this instance enforces and
// advertises. With a database it is known only after Bootstrap has created or
// read the store; a service without a pool (unit tests) uses the new-store value.
func (s *SyncService) pushOutcomeCeilingBytes() (int64, bool) {
	if ceiling := s.pushOutcomeCeiling.Load(); ceiling > 0 {
		return ceiling, true
	}
	if s.pool == nil {
		return newStorePushOutcomeCeiling(s.config), true
	}
	return 0, false
}

func (s *SyncService) createPushOutcomeStore(ctx context.Context, tx pgx.Tx) error {
	statements := []string{
		`CREATE TABLE sync.push_protocol_store (
 singleton BOOLEAN PRIMARY KEY CHECK(singleton),
 format TEXT COLLATE "C" NOT NULL CHECK(format='durable_outcomes'),
 max_outcome_bytes BIGINT NOT NULL CHECK(max_outcome_bytes BETWEEN 8194 AND 67117056)
 )`,
		`CREATE TABLE sync.push_source_progress (
 user_pk BIGINT NOT NULL,
 source_id TEXT COLLATE "C" NOT NULL CHECK(octet_length(source_id) BETWEEN 1 AND 256),
 acknowledged_sequence BIGINT NOT NULL DEFAULT 0 CHECK(acknowledged_sequence>=0),
 acknowledged_generation_id INTEGER,
 acknowledged_generation_fingerprint TEXT COLLATE "C",
 acknowledged_request_hash TEXT COLLATE "C",
 acknowledged_outcome_hash TEXT COLLATE "C",
 PRIMARY KEY(user_pk,source_id),
 FOREIGN KEY(user_pk,source_id) REFERENCES sync.source_state(user_pk,source_id) ON DELETE CASCADE,
 CHECK((acknowledged_sequence=0 AND acknowledged_generation_id IS NULL AND acknowledged_generation_fingerprint IS NULL AND acknowledged_request_hash IS NULL AND acknowledged_outcome_hash IS NULL)
 OR (acknowledged_sequence>0 AND acknowledged_generation_id IS NOT NULL AND acknowledged_generation_fingerprint IS NOT NULL AND acknowledged_request_hash IS NOT NULL AND acknowledged_outcome_hash IS NOT NULL AND acknowledged_generation_id BETWEEN 1 AND 2147483647 AND acknowledged_generation_fingerprint ~ '^[0-9a-f]{64}$' AND acknowledged_request_hash ~ '^[0-9a-f]{64}$' AND acknowledged_outcome_hash ~ '^[0-9a-f]{64}$'))
 )`,
		`CREATE TABLE sync.push_outcomes (
 user_pk BIGINT NOT NULL,
 source_id TEXT COLLATE "C" NOT NULL,
 source_bundle_id BIGINT NOT NULL CHECK(source_bundle_id>0),
 generation_id INTEGER NOT NULL CHECK(generation_id>0),
 generation_fingerprint TEXT COLLATE "C" NOT NULL CHECK(generation_fingerprint ~ '^[0-9a-f]{64}$'),
 request_hash TEXT COLLATE "C" NOT NULL CHECK(request_hash ~ '^[0-9a-f]{64}$'),
 initialization_id UUID,
 outcome_hash TEXT COLLATE "C" NOT NULL CHECK(outcome_hash ~ '^[0-9a-f]{64}$'),
 envelope BYTEA NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(user_pk,source_id),
 FOREIGN KEY(user_pk,source_id) REFERENCES sync.push_source_progress(user_pk,source_id) ON DELETE CASCADE
 )`,
		`CREATE FUNCTION sync.push_outcome_store_guard() RETURNS TRIGGER LANGUAGE plpgsql AS $$
 BEGIN RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='push outcome store configuration is immutable'; END; $$`,
		`CREATE TRIGGER push_outcome_store_guard BEFORE UPDATE OR DELETE ON sync.push_protocol_store FOR EACH ROW EXECUTE FUNCTION sync.push_outcome_store_guard()`,
		`CREATE FUNCTION sync.push_outcome_insert_guard() RETURNS TRIGGER LANGUAGE plpgsql AS $$
 DECLARE acknowledged BIGINT; ceiling BIGINT;
 BEGIN
 SELECT acknowledged_sequence INTO acknowledged FROM sync.push_source_progress WHERE user_pk=NEW.user_pk AND source_id=NEW.source_id FOR UPDATE;
 IF acknowledged IS NULL OR acknowledged=9223372036854775807 OR NEW.source_bundle_id<>acknowledged+1 THEN
 RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='push outcome sequence invariant'; END IF;
 SELECT max_outcome_bytes INTO ceiling FROM sync.push_protocol_store WHERE singleton;
 IF ceiling IS NULL OR octet_length(NEW.envelope)>ceiling THEN
 RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='push outcome size invariant'; END IF;
 RETURN NEW; END; $$`,
		`CREATE TRIGGER push_outcome_insert_guard BEFORE INSERT ON sync.push_outcomes FOR EACH ROW EXECUTE FUNCTION sync.push_outcome_insert_guard()`,
		`CREATE TRIGGER push_outcome_immutable_guard BEFORE UPDATE ON sync.push_outcomes FOR EACH ROW EXECUTE FUNCTION sync.push_outcome_store_guard()`,
		`CREATE FUNCTION sync.push_outcome_lifecycle_guard() RETURNS TRIGGER LANGUAGE plpgsql AS $$
 BEGIN
 IF EXISTS(SELECT 1 FROM sync.user_state WHERE user_pk=OLD.user_pk) THEN
 IF TG_TABLE_NAME='push_source_progress' THEN RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='client source ownership fence must be retained'; END IF;
 IF EXISTS(SELECT 1 FROM sync.push_outcomes WHERE user_pk=OLD.user_pk AND source_id=OLD.source_id) THEN
 RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='push_outcome_pending'; END IF;
 IF TG_TABLE_NAME='source_state' AND TG_OP='DELETE' THEN
 IF OLD.max_committed_source_bundle_id>0 OR EXISTS(SELECT 1 FROM sync.push_source_progress WHERE user_pk=OLD.user_pk AND source_id=OLD.source_id) THEN
 RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='source ownership and acknowledgment fence must be retained'; END IF; END IF;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW;
 END; $$`,
		`CREATE TRIGGER push_outcome_lifecycle_guard BEFORE UPDATE OF state OR DELETE ON sync.source_state FOR EACH ROW EXECUTE FUNCTION sync.push_outcome_lifecycle_guard()`,
		`CREATE TRIGGER push_outcome_lifecycle_guard BEFORE DELETE ON sync.source_generation_bindings FOR EACH ROW EXECUTE FUNCTION sync.push_outcome_lifecycle_guard()`,
		`CREATE TRIGGER push_outcome_lifecycle_guard BEFORE DELETE ON sync.push_source_progress FOR EACH ROW EXECUTE FUNCTION sync.push_outcome_lifecycle_guard()`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("create fresh push outcome store: %w", err)
		}
	}
	_, err := tx.Exec(ctx, `INSERT INTO sync.push_protocol_store VALUES(TRUE,'durable_outcomes',$1)`, newStorePushOutcomeCeiling(s.config))
	return err
}

// validatePushOutcomeStore checks the existing store and returns its immutable
// ceiling, which this instance then advertises and enforces.
func (s *SyncService) validatePushOutcomeStore(ctx context.Context, tx pgx.Tx) (int64, error) {
	var ceiling int64
	if err := tx.QueryRow(ctx, `SELECT max_outcome_bytes FROM sync.push_protocol_store WHERE singleton AND format='durable_outcomes'`).Scan(&ceiling); err != nil {
		return 0, fmt.Errorf("incompatible push outcome store: %w", err)
	}
	if err := checkStoredPushOutcomeCeiling(ceiling, s.config); err != nil {
		return 0, err
	}
	rows, err := tx.Query(ctx, `SELECT DISTINCT generation_id,generation_fingerprint FROM sync.push_outcomes`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int32
		var fingerprint string
		if err := rows.Scan(&id, &fingerprint); err != nil {
			return 0, err
		}
		contract, ok := s.schemaGenerationRegistry.Generation(SchemaGenerationID(id))
		if !ok || contract.Fingerprint() != fingerprint {
			return 0, &PushOutcomeError{Code: "push_outcome_pending"}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return ceiling, nil
}

// checkStoredPushOutcomeCeiling applies r0001's startup rule to an existing
// store: an explicit configured ceiling must equal the stored one, and the
// stored one must hold the outcome of a maximum-size push. The ceiling has no
// in-place transition, so the error names the two ways out.
func checkStoredPushOutcomeCeiling(stored int64, cfg *ServiceConfig) error {
	if cfg.MaxPushOutcomeDecodedBytes != 0 && stored != cfg.MaxPushOutcomeDecodedBytes {
		return fmt.Errorf("immutable push outcome ceiling mismatch: stored=%d configured=%d; set MaxPushOutcomeDecodedBytes to 0 to adopt the stored value", stored, cfg.MaxPushOutcomeDecodedBytes)
	}
	if required := minimumPushOutcomeCeiling(cfg.MaxBytesPerBundle); stored < required {
		return fmt.Errorf("immutable push outcome ceiling too small: stored=%d, MaxBytesPerBundle=%d needs at least %d; keep MaxBytesPerBundle at or below %d, or recreate the sync store (the ceiling cannot change in place)", stored, cfg.MaxBytesPerBundle, required, stored-pushOutcomeEnvelopeAllowance)
	}
	return nil
}
