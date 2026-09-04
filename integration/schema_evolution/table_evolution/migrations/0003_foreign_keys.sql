ALTER TABLE main.people ADD CONSTRAINT people_project FOREIGN KEY (_sync_scope_id,project_id) REFERENCES main.projects(_sync_scope_id,id) ON DELETE SET NULL (project_id) DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE main.people VALIDATE CONSTRAINT people_project;
ALTER TABLE main.attachments ADD CONSTRAINT attachment_person FOREIGN KEY (_sync_scope_id,person_id) REFERENCES main.people(_sync_scope_id,id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED NOT VALID;
ALTER TABLE main.attachments VALIDATE CONSTRAINT attachment_person;
