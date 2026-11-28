-- Equivalent isolated PostgreSQL fixture. Scope-qualified keys/FKs and explicit boolean mapping.
CREATE TABLE "business"."activity_category" (
 _sync_scope_id TEXT NOT NULL,
    id TEXT NOT NULL,

    title TEXT NOT NULL DEFAULT '',

    icon TEXT NOT NULL
,
 PRIMARY KEY(_sync_scope_id,id)
);
CREATE TABLE "business"."profile" (
 _sync_scope_id TEXT NOT NULL,
    name TEXT NOT NULL,

    measure_system TEXT NOT NULL DEFAULT 'metric' CHECK (measure_system IN ('metric', 'US', 'UK')),

    temperature_system TEXT NOT NULL DEFAULT 'F' CHECK (temperature_system IN ('C', 'F')),

    body_weight BIGINT NOT NULL DEFAULT 0,

    body_weight_last_update TEXT NOT NULL DEFAULT '',

    body_height BIGINT NOT NULL DEFAULT 0,

    body_height_last_update TEXT NOT NULL DEFAULT '',

    unlock_codes TEXT NOT NULL DEFAULT '[]',

    essential_plan BOOLEAN NOT NULL DEFAULT FALSE CHECK (essential_plan IN (FALSE,TRUE)),

    gender TEXT NOT NULL DEFAULT 'male' CHECK (gender IN ('male', 'female', 'other')),

    birthday TEXT NOT NULL DEFAULT '1980-01-01',

    time_zone TEXT NOT NULL DEFAULT 'UTC'
,
 PRIMARY KEY(_sync_scope_id,name)
);
CREATE TABLE "business"."publisher" (
 _sync_scope_id TEXT NOT NULL,
    id TEXT NOT NULL,
    title TEXT NOT NULL
,
 PRIMARY KEY(_sync_scope_id,id)
);
CREATE TABLE "business"."value_registry" (
 _sync_scope_id TEXT NOT NULL,
    key TEXT NOT NULL,

    value_type TEXT NOT NULL CHECK (value_type IN ('null', 'string', 'numeric', 'date')),

    updated_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00.000000000Z',

    string_value TEXT,

    numeric_value DOUBLE PRECISION
,
 PRIMARY KEY(_sync_scope_id,key)
);
CREATE TABLE "business"."activity_bundle" (
 _sync_scope_id TEXT NOT NULL,
    id TEXT NOT NULL,

    publisher_id TEXT,

    content_origin TEXT NOT NULL CHECK (content_origin IN ('catalog', 'user')),

    publisher_title TEXT NOT NULL,

    version BIGINT NOT NULL,

    title TEXT NOT NULL,

    descr TEXT,

    purchase_mode TEXT NOT NULL CHECK (purchase_mode IN ('fullyFree', 'partiallyFree', 'paid')),

    unlock_code TEXT,

    resources_json TEXT NOT NULL,

    icon TEXT NOT NULL,

    promo_image TEXT NOT NULL,

    promo_screens_json TEXT NOT NULL DEFAULT '[]'
,
 PRIMARY KEY(_sync_scope_id,id),
 FOREIGN KEY(_sync_scope_id,publisher_id) REFERENCES "business"."publisher"(_sync_scope_id,id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE "business"."activity_package" (
 _sync_scope_id TEXT NOT NULL,
    id TEXT NOT NULL,

    content_origin TEXT NOT NULL CHECK (content_origin IN ('catalog', 'user')),

    activity_bundle_id TEXT NOT NULL,

    title TEXT NOT NULL,

    descr TEXT,

    pre_start_text TEXT,

    category_id TEXT NOT NULL,

    icon TEXT NOT NULL
,
 PRIMARY KEY(_sync_scope_id,id),
 FOREIGN KEY(_sync_scope_id,activity_bundle_id) REFERENCES "business"."activity_bundle"(_sync_scope_id,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(_sync_scope_id,category_id) REFERENCES "business"."activity_category"(_sync_scope_id,id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE "business"."bundle_state" (
 _sync_scope_id TEXT NOT NULL,
    bundle_id TEXT NOT NULL,
    purchased BOOLEAN NOT NULL DEFAULT FALSE CHECK (purchased IN (FALSE,TRUE)),
    installed_at TEXT
,
 PRIMARY KEY(_sync_scope_id,bundle_id),
 FOREIGN KEY(_sync_scope_id,bundle_id) REFERENCES "business"."activity_bundle"(_sync_scope_id,id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE "business"."activity" (
 _sync_scope_id TEXT NOT NULL,
    id TEXT NOT NULL,

    content_origin TEXT NOT NULL CHECK (content_origin IN ('catalog', 'user')),

    group_id TEXT NOT NULL,

    activity_package_id TEXT NOT NULL,

    category_id TEXT NOT NULL,

    program_type TEXT NOT NULL CHECK (program_type IN ('marker', 'simple', 'finite', 'dailyCounter')),

    delete_when_expired BOOLEAN NOT NULL DEFAULT FALSE CHECK (delete_when_expired IN (FALSE,TRUE)),

    days_confirm_required BOOLEAN NOT NULL DEFAULT FALSE CHECK (days_confirm_required IN (FALSE,TRUE)),

    order_ind BIGINT NOT NULL CHECK (order_ind >= 0),

    title TEXT NOT NULL,

    descr TEXT NOT NULL,

    icon TEXT NOT NULL,

    required_unlock_code TEXT,

    priority BIGINT NOT NULL DEFAULT 1 CHECK (priority >= 0),

    unlocked_days BIGINT,

    reporting TEXT NOT NULL DEFAULT 'default' CHECK (reporting IN ('default', 'disabled')),

    semantics_json TEXT NOT NULL DEFAULT '{"tracks":[],"emits":[]}',

    meta_json TEXT,

    sched_mandatory_to_setup BOOLEAN NOT NULL DEFAULT FALSE CHECK (sched_mandatory_to_setup IN (FALSE,TRUE)),

    sched_allowed_repeat_modes TEXT NOT NULL DEFAULT '[]',

    sched_repeat TEXT NOT NULL CHECK (sched_repeat IN ('none', 'weekDays', 'daysOfMonth', 'daysInterval')),

    sched_days_of_week_mask BIGINT NOT NULL DEFAULT 0 CHECK (sched_days_of_week_mask BETWEEN 0 AND 127),

    sched_weeks_mask BIGINT NOT NULL DEFAULT 0 CHECK (sched_weeks_mask BETWEEN 0 AND 15),

    sched_days_of_month_mask BIGINT NOT NULL DEFAULT 0 CHECK (sched_days_of_month_mask BETWEEN 0 AND 2147483647),

    sched_start_at TEXT,

    sched_start_at_eval TEXT,

    sched_read_ref_start_at TEXT,

    sched_write_ref_start_at TEXT,

    sched_start_at_label TEXT,

    sched_repeat_after_days BIGINT NOT NULL
        CHECK (sched_repeat <> 'daysInterval' OR sched_repeat_after_days > 0),

    sched_read_ref_repeat_after_days TEXT,

    sched_write_ref_repeat_after_days TEXT,

    sched_repeat_after_days_label TEXT,

    sched_repeat_after_days_min BIGINT,

    sched_repeat_after_days_max BIGINT,

    sched_days_duration BIGINT NOT NULL
        CHECK (sched_repeat <> 'daysInterval' OR sched_days_duration > 0),

    sched_allow_edit_days_duration BOOLEAN NOT NULL DEFAULT FALSE CHECK (sched_allow_edit_days_duration IN (FALSE,TRUE)),

    sched_read_ref_days_duration TEXT,

    sched_write_ref_days_duration TEXT,

    sched_days_duration_label TEXT,

    sched_days_duration_min BIGINT,

    sched_days_duration_max BIGINT,

    sched_time_points_json TEXT NOT NULL DEFAULT '[]',

    sched_time_range TEXT CHECK (sched_time_range IS NULL OR sched_time_range IN ('morning', 'afternoon', 'evening'))
,
 PRIMARY KEY(_sync_scope_id,id),
 FOREIGN KEY(_sync_scope_id,activity_package_id) REFERENCES "business"."activity_package"(_sync_scope_id,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(_sync_scope_id,category_id) REFERENCES "business"."activity_category"(_sync_scope_id,id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE "business"."activity_state" (
 _sync_scope_id TEXT NOT NULL,
    activity_id TEXT NOT NULL,

    enabled BOOLEAN NOT NULL CHECK (enabled IN (FALSE,TRUE)),

    monthly_glance_view BOOLEAN NOT NULL CHECK (monthly_glance_view IN (FALSE,TRUE)),

    priority BIGINT NOT NULL DEFAULT 1 CHECK (priority >= 0),

    category_id TEXT NOT NULL,

    program_type TEXT NOT NULL CHECK (program_type IN ('marker', 'simple', 'finite', 'dailyCounter')),

    installed_at TEXT,

    repeat TEXT NOT NULL CHECK (repeat IN ('none', 'weekDays', 'daysOfMonth', 'daysInterval')),

    days_of_week_mask BIGINT NOT NULL DEFAULT 0 CHECK (days_of_week_mask BETWEEN 0 AND 127),

    weeks_mask BIGINT NOT NULL DEFAULT 0 CHECK (weeks_mask BETWEEN 0 AND 15),

    days_of_month_mask BIGINT NOT NULL DEFAULT 0 CHECK (days_of_month_mask BETWEEN 0 AND 2147483647),

    start_at TEXT,

    repeat_after_days BIGINT NOT NULL
        CHECK (repeat <> 'daysInterval' OR repeat_after_days > 0),

    days_duration BIGINT NOT NULL
        CHECK (repeat <> 'daysInterval' OR days_duration > 0),

    time_points_json TEXT NOT NULL DEFAULT '[]',

    time_range TEXT CHECK (time_range IS NULL OR time_range IN ('morning', 'afternoon', 'evening'))
,
 PRIMARY KEY(_sync_scope_id,activity_id),
 FOREIGN KEY(_sync_scope_id,activity_id) REFERENCES "business"."activity"(_sync_scope_id,id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(_sync_scope_id,category_id) REFERENCES "business"."activity_category"(_sync_scope_id,id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE "business"."program_item" (
 _sync_scope_id TEXT NOT NULL,
    id TEXT NOT NULL,

    activity_id TEXT NOT NULL,

    item_id TEXT NOT NULL,

    title TEXT,

    descr TEXT,

    goal_value BIGINT NOT NULL,

    goal_daily_initial BIGINT NOT NULL,

    goal_direction TEXT NOT NULL CHECK (goal_direction IN ('up', 'down')),

    goal_invert BOOLEAN NOT NULL CHECK (goal_invert IN (FALSE,TRUE)),

    goal_at_least BOOLEAN NOT NULL CHECK (goal_at_least IN (FALSE,TRUE)),

    goal_single BOOLEAN NOT NULL CHECK (goal_single IN (FALSE,TRUE)),

    goal_hide_editor BOOLEAN NOT NULL CHECK (goal_hide_editor IN (FALSE,TRUE)),

    week_index BIGINT NOT NULL CHECK (week_index >= 0),

    day_index BIGINT NOT NULL CHECK (day_index >= 0),

    pre_start_text TEXT,

    post_complete_text TEXT,

    presentation TEXT CHECK (presentation IS NULL OR presentation IN ('default', 'verticalCounterBars')),

    seq_items_json TEXT NOT NULL DEFAULT '[]',

    required_unlock_code TEXT,

    has_unlocked_seq_items BOOLEAN NOT NULL CHECK (has_unlocked_seq_items IN (FALSE,TRUE)),

    lock_item_display TEXT NOT NULL CHECK (lock_item_display IN ('default', 'blurred')),

    input_entries TEXT NOT NULL DEFAULT '[]'
,
 PRIMARY KEY(_sync_scope_id,id),
 FOREIGN KEY(_sync_scope_id,activity_id) REFERENCES "business"."activity"(_sync_scope_id,id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE "business"."daily_log" (
 _sync_scope_id TEXT NOT NULL,
    id TEXT NOT NULL,

    parent_daily_log_id TEXT,

    activity_id TEXT NOT NULL,

    program_item_id TEXT NOT NULL,

    group_id TEXT,

    date TEXT NOT NULL,

    applied_week_ind BIGINT NOT NULL,

    applied_day_ind BIGINT NOT NULL,

    counter BIGINT NOT NULL DEFAULT 0,

    added_manually BOOLEAN NOT NULL,

    confirmed BOOLEAN NOT NULL DEFAULT TRUE,

    inputs_json TEXT NOT NULL DEFAULT '{"values":{}}',

    notes TEXT,

    CHECK (parent_daily_log_id IS NULL OR parent_daily_log_id <> id),
    CHECK (added_manually IN (FALSE,TRUE)),
    CHECK (confirmed IN (FALSE,TRUE))
,
 PRIMARY KEY(_sync_scope_id,id),
 FOREIGN KEY(_sync_scope_id,parent_daily_log_id) REFERENCES "business"."daily_log"(_sync_scope_id,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(_sync_scope_id,activity_id) REFERENCES "business"."activity"(_sync_scope_id,id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(_sync_scope_id,program_item_id) REFERENCES "business"."program_item"(_sync_scope_id,id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE "business"."program_item_input_goal" (
 _sync_scope_id TEXT NOT NULL,
    id TEXT NOT NULL,

    program_item_id TEXT NOT NULL,

    input_key TEXT NOT NULL,

    current_revision BIGINT NOT NULL CHECK (current_revision > 0),

    UNIQUE(_sync_scope_id,program_item_id, input_key),
    CHECK (input_key <> '')
,
 PRIMARY KEY(_sync_scope_id,id),
 FOREIGN KEY(_sync_scope_id,program_item_id) REFERENCES "business"."program_item"(_sync_scope_id,id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE "business"."program_item_state" (
 _sync_scope_id TEXT NOT NULL,
    program_item_id TEXT NOT NULL,

    goal_value BIGINT NOT NULL,

    goal_invert BOOLEAN NOT NULL CHECK (goal_invert IN (FALSE,TRUE)),

    goal_single BOOLEAN NOT NULL CHECK (goal_single IN (FALSE,TRUE))
,
 PRIMARY KEY(_sync_scope_id,program_item_id),
 FOREIGN KEY(_sync_scope_id,program_item_id) REFERENCES "business"."program_item"(_sync_scope_id,id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE "business"."program_item_input_goal_revision" (
 _sync_scope_id TEXT NOT NULL,
    id TEXT NOT NULL,

    goal_id TEXT NOT NULL,

    revision BIGINT NOT NULL CHECK (revision > 0),

    effective_day TEXT NOT NULL CHECK (
        length(effective_day) = 10
        AND substr(effective_day, 5, 1) = '-'
        AND substr(effective_day, 8, 1) = '-'
    ),

    goal_kind TEXT NOT NULL CHECK (goal_kind IN ('none', 'range', 'upper_limit', 'lower_limit')),

    policy_json TEXT NOT NULL DEFAULT '{}',

    UNIQUE(_sync_scope_id,goal_id, revision)
,
 PRIMARY KEY(_sync_scope_id,id),
 FOREIGN KEY(_sync_scope_id,goal_id) REFERENCES "business"."program_item_input_goal"(_sync_scope_id,id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX idx_activity_contentOrigin ON "business"."activity"(_sync_scope_id,content_origin);
CREATE INDEX idx_activity_packageOrder ON "business"."activity"(_sync_scope_id,activity_package_id, order_ind, title, id);
CREATE INDEX idx_activity_deleteWhenExpired ON "business"."activity"(_sync_scope_id,delete_when_expired);
CREATE INDEX idx_activity_readRefStartAt ON "business"."activity"(_sync_scope_id,sched_read_ref_start_at);
CREATE UNIQUE INDEX idx_activity_writeRefStartAt_unique ON "business"."activity"(_sync_scope_id,sched_write_ref_start_at);
CREATE INDEX idx_activity_readRefRepeatAfterDays ON "business"."activity"(_sync_scope_id,sched_read_ref_repeat_after_days);
CREATE UNIQUE INDEX idx_activity_writeRefRepeatAfterDays_unique ON "business"."activity"(_sync_scope_id,sched_write_ref_repeat_after_days);
CREATE INDEX idx_activity_readRefDaysDuration ON "business"."activity"(_sync_scope_id,sched_read_ref_days_duration);
CREATE UNIQUE INDEX idx_activity_writeRefDaysDuration_unique ON "business"."activity"(_sync_scope_id,sched_write_ref_days_duration);
CREATE INDEX idx_activityState_enabled ON "business"."activity_state"(_sync_scope_id,enabled);
CREATE INDEX idx_activityState_repeat ON "business"."activity_state"(_sync_scope_id,repeat);
CREATE INDEX idx_activityState_monthlyGlanceViewActivityId ON "business"."activity_state"(_sync_scope_id,monthly_glance_view, activity_id);
CREATE INDEX idx_activityBundle_titleId ON "business"."activity_bundle"(_sync_scope_id,title, id);
CREATE INDEX idx_bundleState_installedAt ON "business"."bundle_state"(_sync_scope_id,installed_at);
CREATE INDEX idx_activityPackage_bundleTitleId ON "business"."activity_package"(_sync_scope_id,activity_bundle_id, title, id);
CREATE INDEX idx_dailyLog_programItemId ON "business"."daily_log"(_sync_scope_id,program_item_id);
CREATE INDEX idx_dailyLog_date ON "business"."daily_log"(_sync_scope_id,date);
CREATE INDEX idx_dailyLog_parentDailyLogId ON "business"."daily_log"(_sync_scope_id,parent_daily_log_id);
CREATE INDEX idx_dailyLog_activityDateRange ON "business"."daily_log"(_sync_scope_id,activity_id, date);
CREATE INDEX idx_dailyLog_groupDateId ON "business"."daily_log"(_sync_scope_id,group_id, date, id);
CREATE INDEX idx_dailyLog_activityConfirmed ON "business"."daily_log"(_sync_scope_id,activity_id, confirmed);
CREATE INDEX idx_programItem_activityWeekDayId ON "business"."program_item"(_sync_scope_id,activity_id, week_index, day_index, id);
CREATE INDEX idx_programItemInputGoal_programItem
    ON "business"."program_item_input_goal"(_sync_scope_id,program_item_id, input_key);
CREATE INDEX idx_programItemInputGoalRevision_effective
    ON "business"."program_item_input_goal_revision"(_sync_scope_id,goal_id, effective_day DESC, revision DESC);
CREATE INDEX idx_valueRegistry_valueType ON "business"."value_registry"(_sync_scope_id,value_type);
CREATE INDEX idx_valueRegistry_updatedAt ON "business"."value_registry"(_sync_scope_id,updated_at);
