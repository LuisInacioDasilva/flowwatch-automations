-- Destrutivo: faça backup do arquivo SQLite antes do rollback.
DROP TABLE IF EXISTS results;
DROP TRIGGER IF EXISTS schedule_runs_no_update;
DROP TRIGGER IF EXISTS schedule_runs_no_delete;
DROP TABLE IF EXISTS schedule_runs;
DROP TABLE IF EXISTS schedules;
DROP TRIGGER IF EXISTS operator_actions_no_update;
DROP TRIGGER IF EXISTS operator_actions_no_delete;
DROP TABLE IF EXISTS operator_actions;
DROP TABLE IF EXISTS attempt_log;
DROP TABLE IF EXISTS jobs;
