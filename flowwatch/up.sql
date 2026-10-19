PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    payload_hash TEXT NOT NULL,
    task_type TEXT NOT NULL CHECK (task_type IN ('normalize_contact', 'draft_summary')),
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'running', 'done', 'dead')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 10),
    run_after INTEGER NOT NULL,
    lease_until INTEGER,
    lease_token TEXT,
    last_error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    CHECK ((state = 'running' AND lease_until IS NOT NULL AND lease_token IS NOT NULL)
       OR (state <> 'running' AND lease_until IS NULL AND lease_token IS NULL))
);

CREATE INDEX IF NOT EXISTS jobs_ready_idx ON jobs (state, run_after, lease_until);

CREATE TABLE IF NOT EXISTS attempt_log (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    attempt_number INTEGER NOT NULL,
    lease_token TEXT NOT NULL UNIQUE,
    started_at INTEGER NOT NULL,
    finished_at INTEGER,
    outcome TEXT CHECK (outcome IN ('done', 'retry', 'dead', 'lease_expired')),
    error TEXT,
    UNIQUE (job_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS results (
    job_id INTEGER PRIMARY KEY REFERENCES jobs(id),
    output_json TEXT NOT NULL,
    completed_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS operator_actions (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    new_job_id INTEGER NOT NULL UNIQUE REFERENCES jobs(id),
    actor TEXT NOT NULL CHECK (length(actor) BETWEEN 2 AND 100),
    action TEXT NOT NULL CHECK (action = 'requeue_dead'),
    reason TEXT NOT NULL CHECK (length(reason) BETWEEN 5 AND 500),
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS schedules (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE CHECK (length(name) BETWEEN 3 AND 80),
    task_type TEXT NOT NULL CHECK (task_type IN ('normalize_contact', 'draft_summary')),
    payload_json TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL CHECK (interval_seconds BETWEEN 60 AND 86400),
    next_run_at INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 10),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS schedules_due_idx ON schedules (enabled, next_run_at);

CREATE TABLE IF NOT EXISTS schedule_runs (
    id INTEGER PRIMARY KEY,
    schedule_id INTEGER NOT NULL REFERENCES schedules(id),
    due_at INTEGER NOT NULL,
    job_id INTEGER NOT NULL UNIQUE REFERENCES jobs(id),
    created_at INTEGER NOT NULL,
    UNIQUE (schedule_id, due_at)
);

CREATE TRIGGER IF NOT EXISTS schedule_runs_no_update BEFORE UPDATE ON schedule_runs
BEGIN SELECT RAISE(ABORT, 'schedule run is immutable'); END;
CREATE TRIGGER IF NOT EXISTS schedule_runs_no_delete BEFORE DELETE ON schedule_runs
BEGIN SELECT RAISE(ABORT, 'schedule run is immutable'); END;

CREATE TRIGGER IF NOT EXISTS operator_actions_no_update BEFORE UPDATE ON operator_actions
BEGIN SELECT RAISE(ABORT, 'operator action is immutable'); END;
CREATE TRIGGER IF NOT EXISTS operator_actions_no_delete BEFORE DELETE ON operator_actions
BEGIN SELECT RAISE(ABORT, 'operator action is immutable'); END;
