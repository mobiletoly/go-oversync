CREATE TABLE main.projects (_sync_scope_id text NOT NULL, id uuid NOT NULL, title text NOT NULL, PRIMARY KEY (_sync_scope_id,id));
CREATE TABLE main.attachments (_sync_scope_id text NOT NULL, id uuid NOT NULL, person_id uuid, body text NOT NULL, PRIMARY KEY (_sync_scope_id,id));
ALTER TABLE main.people ADD COLUMN project_id uuid;
CREATE TRIGGER aa_app_write_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON main.projects FOR EACH STATEMENT EXECUTE FUNCTION app_evolution.guard_write();
CREATE TRIGGER aa_app_write_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON main.attachments FOR EACH STATEMENT EXECUTE FUNCTION app_evolution.guard_write();
-- Deliberately no people -> projects FK until the parent has a durable catalog ID.
