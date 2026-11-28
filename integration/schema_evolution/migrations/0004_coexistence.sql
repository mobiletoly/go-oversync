ALTER TABLE main.upgrade_people ADD CONSTRAINT person_encoding CHECK (
    legacy_name IS NOT NULL AND name IS NOT NULL AND new_label IS NOT NULL
    AND name COLLATE "C" ~ '^[A-Za-z0-9_]+$' AND new_label COLLATE "C" ~ '^[A-Za-z0-9_]+$'
    AND legacy_name = name || '|' || new_label
) NOT VALID;
ALTER TABLE main.upgrade_people VALIDATE CONSTRAINT person_encoding;
ALTER TABLE main.upgrade_people ALTER COLUMN name SET NOT NULL, ALTER COLUMN new_label SET NOT NULL;
DROP TRIGGER zz_app_person_transition ON main.upgrade_people;
CREATE TRIGGER zz_app_person_transition BEFORE INSERT OR UPDATE ON main.upgrade_people
FOR EACH ROW EXECUTE FUNCTION app_evolution.transition_person('coexistence');
