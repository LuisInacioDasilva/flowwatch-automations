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

CREATE TRIGGER IF NOT EXISTS operator_actions_no_update BEFORE UPDATE ON operator_actions
BEGIN SELECT RAISE(ABORT, 'operator action is immutable'); END;
CREATE TRIGGER IF NOT EXISTS operator_actions_no_delete BEFORE DELETE ON operator_actions
BEGIN SELECT RAISE(ABORT, 'operator action is immutable'); END;
