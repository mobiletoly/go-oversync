CREATE SCHEMA main;
CREATE TABLE main.people (_sync_scope_id text NOT NULL, id uuid NOT NULL, name text NOT NULL, PRIMARY KEY (_sync_scope_id,id));
CREATE TABLE main.legacy_notes (_sync_scope_id text NOT NULL, id uuid NOT NULL, body text NOT NULL, PRIMARY KEY (_sync_scope_id,id));
CREATE TRIGGER aa_app_write_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON main.people FOR EACH STATEMENT EXECUTE FUNCTION app_evolution.guard_write();
CREATE TRIGGER aa_app_write_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON main.legacy_notes FOR EACH STATEMENT EXECUTE FUNCTION app_evolution.guard_write();
