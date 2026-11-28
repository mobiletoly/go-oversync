CREATE SCHEMA main;
CREATE TABLE main.upgrade_people (
    _sync_scope_id text NOT NULL,
    id uuid NOT NULL,
    legacy_name text NOT NULL,
    PRIMARY KEY (_sync_scope_id, id)
);
CREATE TRIGGER aa_app_write_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON main.upgrade_people
FOR EACH STATEMENT EXECUTE FUNCTION app_evolution.guard_write();
