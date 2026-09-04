DROP TRIGGER zz_app_person_transition ON main.upgrade_people;
ALTER TABLE main.upgrade_people DROP CONSTRAINT person_encoding;
ALTER TABLE main.upgrade_people DROP COLUMN legacy_name RESTRICT;
DROP FUNCTION app_evolution.transition_person() RESTRICT;
DROP FUNCTION app_evolution.convert_person(text,text,text,boolean) RESTRICT;
ALTER TABLE main.upgrade_people ADD CONSTRAINT person_encoding CHECK (
 name IS NOT NULL AND new_label IS NOT NULL
 AND name COLLATE "C" ~ '^[A-Za-z0-9_]+$'
 AND new_label COLLATE "C" ~ '^[A-Za-z0-9_]+$'
);
