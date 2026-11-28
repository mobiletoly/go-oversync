ALTER TABLE main.upgrade_people ADD COLUMN name text, ADD COLUMN new_label text;
CREATE FUNCTION app_evolution.convert_person(legacy text, person_name text, label text, from_legacy boolean)
RETURNS text[] LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
 IF from_legacy THEN
  IF legacy IS NULL OR legacy COLLATE "C" !~ '^[A-Za-z0-9_]+[|][A-Za-z0-9_]+$' THEN
   RAISE EXCEPTION USING ERRCODE='P6001', MESSAGE='transition_input_invalid';
  END IF;
  RETURN ARRAY[split_part(legacy,'|',1),split_part(legacy,'|',2),legacy];
 END IF;
 IF person_name IS NULL OR label IS NULL OR person_name COLLATE "C" !~ '^[A-Za-z0-9_]+$' OR label COLLATE "C" !~ '^[A-Za-z0-9_]+$' THEN
  RAISE EXCEPTION USING ERRCODE='P6001', MESSAGE='transition_input_invalid';
 END IF;
 RETURN ARRAY[person_name,label,person_name||'|'||label];
END
$$;
CREATE FUNCTION app_evolution.transition_person() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    parts text[];
    old_changed boolean;
    name_changed boolean;
    label_changed boolean;
    pair_valid boolean;
    legacy_valid boolean;
BEGIN
    PERFORM 1 FROM app_evolution.scope_inventory WHERE scope_id=NEW._sync_scope_id AND NOT deleted FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'application_scope_deleted'; END IF;
    legacy_valid := NEW.legacy_name IS NOT NULL AND NEW.legacy_name COLLATE "C" ~ '^[A-Za-z0-9_]+[|][A-Za-z0-9_]+$';
    pair_valid := NEW.name IS NOT NULL AND NEW.new_label IS NOT NULL
        AND NEW.name COLLATE "C" ~ '^[A-Za-z0-9_]+$' AND NEW.new_label COLLATE "C" ~ '^[A-Za-z0-9_]+$';
    IF legacy_valid THEN parts := app_evolution.convert_person(NEW.legacy_name,NULL,NULL,true); END IF;
    IF TG_OP = 'UPDATE' THEN
        old_changed := NEW.legacy_name IS DISTINCT FROM OLD.legacy_name;
        name_changed := NEW.name IS DISTINCT FROM OLD.name;
        label_changed := NEW.new_label IS DISTINCT FROM OLD.new_label;
    END IF;
    IF TG_ARGV[0] = 'maintaining' THEN
        IF NOT legacy_valid THEN RAISE EXCEPTION USING ERRCODE='P6001', MESSAGE='transition_input_invalid'; END IF;
        IF TG_OP = 'INSERT' THEN
            name_changed := NEW.name IS NOT NULL;
            label_changed := NEW.new_label IS NOT NULL;
        END IF;
        IF (name_changed AND NEW.name IS DISTINCT FROM parts[1])
            OR (label_changed AND NEW.new_label IS DISTINCT FROM parts[2]) THEN
            RAISE EXCEPTION USING ERRCODE='P6002', MESSAGE='transition_input_conflict';
        END IF;
        NEW.name := parts[1];
        NEW.new_label := parts[2];
        RETURN NEW;
    END IF;
    IF TG_ARGV[0] <> 'coexistence' THEN RAISE EXCEPTION 'transition_phase_invalid'; END IF;
    IF TG_OP = 'UPDATE' THEN
        IF OLD.legacy_name IS NULL OR OLD.name IS NULL OR OLD.new_label IS NULL
            OR OLD.name COLLATE "C" !~ '^[A-Za-z0-9_]+$' OR OLD.new_label COLLATE "C" !~ '^[A-Za-z0-9_]+$'
            OR OLD.legacy_name IS DISTINCT FROM OLD.name || '|' || OLD.new_label THEN
            RAISE EXCEPTION USING ERRCODE='P6001', MESSAGE='transition_invariant_invalid';
        END IF;
        IF old_changed AND NOT (name_changed OR label_changed) AND legacy_valid THEN
            NEW.name := parts[1]; NEW.new_label := parts[2]; RETURN NEW;
        ELSIF NOT old_changed AND (name_changed OR label_changed) AND pair_valid THEN
            NEW.legacy_name := (app_evolution.convert_person(NULL,NEW.name,NEW.new_label,false))[3]; RETURN NEW;
        END IF;
    ELSE
        IF legacy_valid AND NEW.name IS NULL AND NEW.new_label IS NULL THEN
            NEW.name := parts[1]; NEW.new_label := parts[2]; RETURN NEW;
        ELSIF NEW.legacy_name IS NULL AND pair_valid THEN
            NEW.legacy_name := (app_evolution.convert_person(NULL,NEW.name,NEW.new_label,false))[3]; RETURN NEW;
        END IF;
    END IF;
    IF NOT legacy_valid OR NOT pair_valid THEN
        RAISE EXCEPTION USING ERRCODE='P6001', MESSAGE='transition_input_invalid';
    END IF;
    IF NEW.legacy_name IS DISTINCT FROM NEW.name || '|' || NEW.new_label THEN
        RAISE EXCEPTION USING ERRCODE='P6002', MESSAGE='transition_input_conflict';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER zz_app_person_transition BEFORE INSERT OR UPDATE ON main.upgrade_people
FOR EACH ROW EXECUTE FUNCTION app_evolution.transition_person('maintaining');
