-- Destrutivo: faça backup do arquivo SQLite antes do rollback.
DROP TABLE IF EXISTS results;
DROP TABLE IF EXISTS attempt_log;
DROP TABLE IF EXISTS jobs;
