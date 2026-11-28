-- Synthetic qualification fixture for the activity schema; reference revision 3aede22195a7462ee0178bf64f4a054d5440dc01.
-- Schema declarations preserve source SQLite shapes; app_settings remains local-only.
-- Explicit wire mapping: annotated INTEGER booleans -> boolean; other INTEGER -> int64, REAL -> float64, all TEXT -> text (including date/JSON text).
-- Source activity.sql SHA256 9e353dff33d4f044934b9aa5869a1627ed79bc93747ac95e99b4caf230b2cc60
CREATE TABLE activity (
    id TEXT PRIMARY KEY NOT NULL,

    content_origin TEXT NOT NULL CHECK (content_origin IN ('catalog', 'user')),

    group_id TEXT NOT NULL,

    activity_package_id TEXT NOT NULL REFERENCES activity_package(id),

    category_id TEXT NOT NULL REFERENCES activity_category(id),

    program_type TEXT NOT NULL CHECK (program_type IN ('marker', 'simple', 'finite', 'dailyCounter')),

    delete_when_expired INTEGER NOT NULL DEFAULT 0 CHECK (delete_when_expired IN (0,1)),

    days_confirm_required INTEGER NOT NULL DEFAULT 0 CHECK (days_confirm_required IN (0,1)),

    order_ind INTEGER NOT NULL CHECK (order_ind >= 0),

    title TEXT NOT NULL,

    descr TEXT NOT NULL,

    icon TEXT NOT NULL,

    required_unlock_code TEXT,

    priority INTEGER NOT NULL DEFAULT 1 CHECK (priority >= 0),

    unlocked_days INTEGER,

    reporting TEXT NOT NULL DEFAULT 'default' CHECK (reporting IN ('default', 'disabled')),

    semantics_json TEXT NOT NULL DEFAULT '{"tracks":[],"emits":[]}',

    meta_json TEXT,

    sched_mandatory_to_setup INTEGER NOT NULL DEFAULT 0 CHECK (sched_mandatory_to_setup IN (0,1)),

    sched_allowed_repeat_modes TEXT NOT NULL DEFAULT '[]',

    sched_repeat TEXT NOT NULL CHECK (sched_repeat IN ('none', 'weekDays', 'daysOfMonth', 'daysInterval')),

    sched_days_of_week_mask INTEGER NOT NULL DEFAULT 0 CHECK (sched_days_of_week_mask BETWEEN 0 AND 127),

    sched_weeks_mask INTEGER NOT NULL DEFAULT 0 CHECK (sched_weeks_mask BETWEEN 0 AND 15),

    sched_days_of_month_mask INTEGER NOT NULL DEFAULT 0 CHECK (sched_days_of_month_mask BETWEEN 0 AND 2147483647),

    sched_start_at TEXT,

    sched_start_at_eval TEXT,

    sched_read_ref_start_at TEXT,

    sched_write_ref_start_at TEXT,

    sched_start_at_label TEXT,

    sched_repeat_after_days INTEGER NOT NULL
        CHECK (sched_repeat <> 'daysInterval' OR sched_repeat_after_days > 0),

    sched_read_ref_repeat_after_days TEXT,

    sched_write_ref_repeat_after_days TEXT,

    sched_repeat_after_days_label TEXT,

    sched_repeat_after_days_min INTEGER,

    sched_repeat_after_days_max INTEGER,

    sched_days_duration INTEGER NOT NULL
        CHECK (sched_repeat <> 'daysInterval' OR sched_days_duration > 0),

    sched_allow_edit_days_duration INTEGER NOT NULL DEFAULT 0 CHECK (sched_allow_edit_days_duration IN (0,1)),

    sched_read_ref_days_duration TEXT,

    sched_write_ref_days_duration TEXT,

    sched_days_duration_label TEXT,

    sched_days_duration_min INTEGER,

    sched_days_duration_max INTEGER,

    sched_time_points_json TEXT NOT NULL DEFAULT '[]',

    sched_time_range TEXT CHECK (sched_time_range IS NULL OR sched_time_range IN ('morning', 'afternoon', 'evening'))
) WITHOUT ROWID;

CREATE TABLE activity_state (
    activity_id TEXT PRIMARY KEY NOT NULL REFERENCES activity(id) ON DELETE CASCADE,

    enabled INTEGER NOT NULL CHECK (enabled IN (0,1)),

    monthly_glance_view INTEGER NOT NULL CHECK (monthly_glance_view IN (0,1)),

    priority INTEGER NOT NULL DEFAULT 1 CHECK (priority >= 0),

    category_id TEXT NOT NULL REFERENCES activity_category(id),

    program_type TEXT NOT NULL CHECK (program_type IN ('marker', 'simple', 'finite', 'dailyCounter')),

    installed_at TEXT,

    repeat TEXT NOT NULL CHECK (repeat IN ('none', 'weekDays', 'daysOfMonth', 'daysInterval')),

    days_of_week_mask INTEGER NOT NULL DEFAULT 0 CHECK (days_of_week_mask BETWEEN 0 AND 127),

    weeks_mask INTEGER NOT NULL DEFAULT 0 CHECK (weeks_mask BETWEEN 0 AND 15),

    days_of_month_mask INTEGER NOT NULL DEFAULT 0 CHECK (days_of_month_mask BETWEEN 0 AND 2147483647),

    start_at TEXT,

    repeat_after_days INTEGER NOT NULL
        CHECK (repeat <> 'daysInterval' OR repeat_after_days > 0),

    days_duration INTEGER NOT NULL
        CHECK (repeat <> 'daysInterval' OR days_duration > 0),

    time_points_json TEXT NOT NULL DEFAULT '[]',

    time_range TEXT CHECK (time_range IS NULL OR time_range IN ('morning', 'afternoon', 'evening'))
) WITHOUT ROWID;

CREATE INDEX idx_activity_contentOrigin ON activity(content_origin);
CREATE INDEX idx_activity_packageOrder ON activity(activity_package_id, order_ind, title, id);
CREATE INDEX idx_activity_deleteWhenExpired ON activity(delete_when_expired);
CREATE INDEX idx_activity_readRefStartAt ON activity(sched_read_ref_start_at);
CREATE UNIQUE INDEX idx_activity_writeRefStartAt_unique ON activity(sched_write_ref_start_at);
CREATE INDEX idx_activity_readRefRepeatAfterDays ON activity(sched_read_ref_repeat_after_days);
CREATE UNIQUE INDEX idx_activity_writeRefRepeatAfterDays_unique ON activity(sched_write_ref_repeat_after_days);
CREATE INDEX idx_activity_readRefDaysDuration ON activity(sched_read_ref_days_duration);
CREATE UNIQUE INDEX idx_activity_writeRefDaysDuration_unique ON activity(sched_write_ref_days_duration);

CREATE INDEX idx_activityState_enabled ON activity_state(enabled);
CREATE INDEX idx_activityState_repeat ON activity_state(repeat);
CREATE INDEX idx_activityState_monthlyGlanceViewActivityId ON activity_state(monthly_glance_view, activity_id);

-- Source activity_bundle.sql SHA256 d5ababa357720b89e6ff2069f89e160336fba34c08a43e061e68e35291b5832a
CREATE TABLE activity_bundle (
    id TEXT PRIMARY KEY NOT NULL,

    publisher_id TEXT REFERENCES publisher(id),

    content_origin TEXT NOT NULL CHECK (content_origin IN ('catalog', 'user')),

    publisher_title TEXT NOT NULL,

    version INTEGER NOT NULL,

    title TEXT NOT NULL,

    descr TEXT,

    purchase_mode TEXT NOT NULL CHECK (purchase_mode IN ('fullyFree', 'partiallyFree', 'paid')),

    unlock_code TEXT,

    resources_json TEXT NOT NULL,

    icon TEXT NOT NULL,

    promo_image TEXT NOT NULL,

    promo_screens_json TEXT NOT NULL DEFAULT '[]'
) WITHOUT ROWID;

CREATE TABLE bundle_state (
    bundle_id TEXT PRIMARY KEY NOT NULL REFERENCES activity_bundle(id) ON DELETE CASCADE,
    purchased INTEGER NOT NULL DEFAULT 0 CHECK (purchased IN (0,1)),
    installed_at TEXT
) WITHOUT ROWID;

CREATE INDEX idx_activityBundle_titleId ON activity_bundle(title, id);
CREATE INDEX idx_bundleState_installedAt ON bundle_state(installed_at);

-- Source activity_category.sql SHA256 6eeb6acff08e4e26d5a1731dee1c2b9c915379d5a23000c20811216b179be262
CREATE TABLE activity_category (
    id TEXT PRIMARY KEY NOT NULL,

    title TEXT NOT NULL DEFAULT '',

    icon TEXT NOT NULL
) WITHOUT ROWID;

-- Source activity_package.sql SHA256 5a69f63be516443a0ea8550b87ae52bbafa792a190817a227bdd9cd163d99f00
CREATE TABLE activity_package (
    id TEXT PRIMARY KEY NOT NULL,

    content_origin TEXT NOT NULL CHECK (content_origin IN ('catalog', 'user')),

    activity_bundle_id TEXT NOT NULL REFERENCES activity_bundle(id),

    title TEXT NOT NULL,

    descr TEXT,

    pre_start_text TEXT,

    category_id TEXT NOT NULL REFERENCES activity_category(id),

    icon TEXT NOT NULL
) WITHOUT ROWID;

CREATE INDEX idx_activityPackage_bundleTitleId ON activity_package(activity_bundle_id, title, id);

-- Source app_settings.sql SHA256 4d58c4d6b807248c8e4b9f1b27ab18a4511a1f7bcac609b21b5e4a1f50f84e57
CREATE TABLE app_settings (
    id INTEGER PRIMARY KEY NOT NULL CHECK (id = 1),

    bootstrapped_date TEXT NOT NULL DEFAULT '',

    most_recent_sync_date TEXT NOT NULL DEFAULT '',

    bootstrap_version INTEGER NOT NULL DEFAULT 0,

    wizard_completed INTEGER NOT NULL DEFAULT 0
);

-- Source daily_log.sql SHA256 27e265e07d74014da49c79ac4feba549b49aebcb4cec9f4b04acfcc018116905
CREATE TABLE daily_log (
    id TEXT PRIMARY KEY NOT NULL,

    parent_daily_log_id TEXT REFERENCES daily_log(id),

    activity_id TEXT NOT NULL REFERENCES activity(id),

    program_item_id TEXT NOT NULL REFERENCES program_item(id),

    group_id TEXT,

    date TEXT NOT NULL,

    applied_week_ind INTEGER NOT NULL,

    applied_day_ind INTEGER NOT NULL,

    counter INTEGER NOT NULL DEFAULT 0,

    added_manually INTEGER NOT NULL,

    confirmed INTEGER NOT NULL DEFAULT 1,

    inputs_json TEXT NOT NULL DEFAULT '{"values":{}}',

    notes TEXT,

    CHECK (parent_daily_log_id IS NULL OR parent_daily_log_id <> id),
    CHECK (added_manually IN (0,1)),
    CHECK (confirmed IN (0,1))
) WITHOUT ROWID;

CREATE INDEX idx_dailyLog_programItemId ON daily_log(program_item_id);
CREATE INDEX idx_dailyLog_date ON daily_log(date);
CREATE INDEX idx_dailyLog_parentDailyLogId ON daily_log(parent_daily_log_id);
CREATE INDEX idx_dailyLog_activityDateRange ON daily_log(activity_id, date);
CREATE INDEX idx_dailyLog_groupDateId ON daily_log(group_id, date, id);
CREATE INDEX idx_dailyLog_activityConfirmed ON daily_log(activity_id, confirmed);

-- Source profile.sql SHA256 ce04972ff6bcfccc99ed4a52dbf7b0c936ec1c69d892b93cecbbb8e99726a8a6
CREATE TABLE profile (
    name TEXT PRIMARY KEY NOT NULL,

    measure_system TEXT NOT NULL DEFAULT 'metric' CHECK (measure_system IN ('metric', 'US', 'UK')),

    temperature_system TEXT NOT NULL DEFAULT 'F' CHECK (temperature_system IN ('C', 'F')),

    body_weight INTEGER NOT NULL DEFAULT 0,

    body_weight_last_update TEXT NOT NULL DEFAULT '',

    body_height INTEGER NOT NULL DEFAULT 0,

    body_height_last_update TEXT NOT NULL DEFAULT '',

    unlock_codes TEXT NOT NULL DEFAULT '[]',

    essential_plan INTEGER NOT NULL DEFAULT 0 CHECK (essential_plan IN (0,1)),

    gender TEXT NOT NULL DEFAULT 'male' CHECK (gender IN ('male', 'female', 'other')),

    birthday TEXT NOT NULL DEFAULT '1980-01-01',

    time_zone TEXT NOT NULL DEFAULT 'UTC'
) WITHOUT ROWID;

-- Source program_item.sql SHA256 006f734ba4d9377ccff17dfcfd3292f7e7c72eb8243f7c331f55cf6b931aa184
CREATE TABLE program_item (
    id TEXT PRIMARY KEY NOT NULL,

    activity_id TEXT NOT NULL REFERENCES activity(id) ON DELETE CASCADE,

    item_id TEXT NOT NULL,

    title TEXT,

    descr TEXT,

    goal_value INTEGER NOT NULL,

    goal_daily_initial INTEGER NOT NULL,

    goal_direction TEXT NOT NULL CHECK (goal_direction IN ('up', 'down')),

    goal_invert INTEGER NOT NULL CHECK (goal_invert IN (0,1)),

    goal_at_least INTEGER NOT NULL CHECK (goal_at_least IN (0,1)),

    goal_single INTEGER NOT NULL CHECK (goal_single IN (0,1)),

    goal_hide_editor INTEGER NOT NULL CHECK (goal_hide_editor IN (0,1)),

    week_index INTEGER NOT NULL CHECK (week_index >= 0),

    day_index INTEGER NOT NULL CHECK (day_index >= 0),

    pre_start_text TEXT,

    post_complete_text TEXT,

    presentation TEXT CHECK (presentation IS NULL OR presentation IN ('default', 'verticalCounterBars')),

    seq_items_json TEXT NOT NULL DEFAULT '[]',

    required_unlock_code TEXT,

    has_unlocked_seq_items INTEGER NOT NULL CHECK (has_unlocked_seq_items IN (0,1)),

    lock_item_display TEXT NOT NULL CHECK (lock_item_display IN ('default', 'blurred')),

    input_entries TEXT NOT NULL DEFAULT '[]'
) WITHOUT ROWID;

CREATE TABLE program_item_state (
    program_item_id TEXT PRIMARY KEY NOT NULL REFERENCES program_item(id) ON DELETE CASCADE,

    goal_value INTEGER NOT NULL,

    goal_invert INTEGER NOT NULL CHECK (goal_invert IN (0,1)),

    goal_single INTEGER NOT NULL CHECK (goal_single IN (0,1))
) WITHOUT ROWID;

CREATE TABLE program_item_input_goal (
    id TEXT PRIMARY KEY NOT NULL,

    program_item_id TEXT NOT NULL REFERENCES program_item(id) ON DELETE CASCADE,

    input_key TEXT NOT NULL,

    current_revision INTEGER NOT NULL CHECK (current_revision > 0),

    UNIQUE(program_item_id, input_key),
    CHECK (input_key <> '')
) WITHOUT ROWID;

CREATE TABLE program_item_input_goal_revision (
    id TEXT PRIMARY KEY NOT NULL,

    goal_id TEXT NOT NULL REFERENCES program_item_input_goal(id) ON DELETE CASCADE,

    revision INTEGER NOT NULL CHECK (revision > 0),

    effective_day TEXT NOT NULL CHECK (
        length(effective_day) = 10
        AND substr(effective_day, 5, 1) = '-'
        AND substr(effective_day, 8, 1) = '-'
    ),

    goal_kind TEXT NOT NULL CHECK (goal_kind IN ('none', 'range', 'upper_limit', 'lower_limit')),

    policy_json TEXT NOT NULL DEFAULT '{}',

    UNIQUE(goal_id, revision)
) WITHOUT ROWID;

CREATE INDEX idx_programItem_activityWeekDayId ON program_item(activity_id, week_index, day_index, id);
CREATE INDEX idx_programItemInputGoal_programItem
    ON program_item_input_goal(program_item_id, input_key);
CREATE INDEX idx_programItemInputGoalRevision_effective
    ON program_item_input_goal_revision(goal_id, effective_day DESC, revision DESC);

-- Source publisher.sql SHA256 d106ddb400f8aa68094cea22753b6dd64209aeda6d99b26dfa91658802113398
CREATE TABLE publisher (
    id TEXT PRIMARY KEY NOT NULL,
    title TEXT NOT NULL
) WITHOUT ROWID;

-- Source value_registry.sql SHA256 2d953792b643fd8d169c03083186d115c44c3f56b65f7a31619638bbb726fc72
CREATE TABLE value_registry (
    key TEXT PRIMARY KEY NOT NULL,

    value_type TEXT NOT NULL CHECK (value_type IN ('null', 'string', 'numeric', 'date')),

    updated_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00.000000000Z',

    string_value TEXT,

    numeric_value REAL
) WITHOUT ROWID;

CREATE INDEX idx_valueRegistry_valueType ON value_registry(value_type);
CREATE INDEX idx_valueRegistry_updatedAt ON value_registry(updated_at);
