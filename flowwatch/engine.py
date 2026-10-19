import hashlib
import json
import re
import sqlite3
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent


class IdempotencyConflict(ValueError):
    pass


class StaleLease(RuntimeError):
    pass


class InvalidRequeue(ValueError):
    pass


class Store:
    def __init__(self, path: str | Path = ":memory:"):
        self.db = sqlite3.connect(path, isolation_level=None, timeout=10)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.execute("PRAGMA busy_timeout = 10000")
        self.db.executescript((ROOT / "up.sql").read_text(encoding="utf-8"))

    def close(self):
        self.db.close()

    def enqueue(self, *, key: str, task_type: str, payload: dict, now: int | None = None, max_attempts: int = 3) -> int:
        now = int(time.time()) if now is None else now
        if not key or len(key) > 120 or task_type not in {"normalize_contact", "draft_summary"}:
            raise ValueError("invalid key or task type")
        if not 1 <= max_attempts <= 10 or not isinstance(payload, dict):
            raise ValueError("invalid payload or attempt limit")
        content = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if len(content) > 10000:
            raise ValueError("payload too large")
        digest = hashlib.sha256((task_type + "\0" + content).encode()).hexdigest()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            existing = self.db.execute("SELECT id, payload_hash FROM jobs WHERE idempotency_key = ?", (key,)).fetchone()
            if existing:
                if existing["payload_hash"] != digest:
                    raise IdempotencyConflict("key reused with different payload")
                self.db.execute("COMMIT")
                return existing["id"]
            cursor = self.db.execute(
                """INSERT INTO jobs
                (idempotency_key, payload_hash, task_type, payload_json, max_attempts, run_after, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (key, digest, task_type, content, max_attempts, now, now, now),
            )
            self.db.execute("COMMIT")
            return cursor.lastrowid
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def claim(self, *, now: int | None = None, lease_seconds: int = 60) -> dict | None:
        now = int(time.time()) if now is None else now
        if lease_seconds < 1:
            raise ValueError("lease must be positive")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("""SELECT * FROM jobs
                WHERE (state = 'pending' AND run_after <= ?)
                   OR (state = 'running' AND lease_until <= ?)
                ORDER BY run_after, id LIMIT 1""", (now, now)).fetchone()
            if not row:
                self.db.execute("COMMIT")
                return None
            if row["state"] == "running":
                self.db.execute("""UPDATE attempt_log SET finished_at = ?, outcome = 'lease_expired', error = 'worker lease expired'
                    WHERE job_id = ? AND lease_token = ? AND finished_at IS NULL""", (now, row["id"], row["lease_token"]))
                if row["attempts"] >= row["max_attempts"]:
                    self.db.execute("""UPDATE jobs SET state = 'dead', lease_until = NULL, lease_token = NULL,
                        last_error = 'worker lease expired', updated_at = ? WHERE id = ?""", (now, row["id"]))
                    self.db.execute("COMMIT")
                    return None
            token = uuid.uuid4().hex
            next_attempt = row["attempts"] + 1
            self.db.execute("""UPDATE jobs SET state = 'running', attempts = ?, lease_until = ?,
                lease_token = ?, updated_at = ? WHERE id = ?""", (next_attempt, now + lease_seconds, token, now, row["id"]))
            self.db.execute("""INSERT INTO attempt_log (job_id, attempt_number, lease_token, started_at)
                VALUES (?, ?, ?, ?)""", (row["id"], next_attempt, token, now))
            self.db.execute("COMMIT")
            return {**dict(row), "attempts": next_attempt, "lease_token": token}
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def finish(self, job: dict, output: dict, *, now: int | None = None):
        now = int(time.time()) if now is None else now
        self._complete(job, now, output=output)

    def fail(self, job: dict, error: str, *, now: int | None = None):
        now = int(time.time()) if now is None else now
        self._complete(job, now, error=error[:300])

    def _complete(self, job: dict, now: int, *, output: dict | None = None, error: str | None = None):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            current = self.db.execute("SELECT * FROM jobs WHERE id = ?", (job["id"],)).fetchone()
            if (current is None or current["state"] != "running"
                    or current["lease_token"] != job["lease_token"] or current["lease_until"] <= now):
                raise StaleLease("worker no longer owns this job")
            if output is not None:
                self.db.execute("INSERT INTO results (job_id, output_json, completed_at) VALUES (?, ?, ?)",
                                (job["id"], json.dumps(output, ensure_ascii=False, sort_keys=True), now))
                state, outcome, run_after = "done", "done", current["run_after"]
            else:
                state = "dead" if current["attempts"] >= current["max_attempts"] else "pending"
                outcome = "dead" if state == "dead" else "retry"
                run_after = now + min(300, 5 * (2 ** (current["attempts"] - 1)))
            self.db.execute("""UPDATE jobs SET state = ?, run_after = ?, lease_until = NULL,
                lease_token = NULL, last_error = ?, updated_at = ? WHERE id = ?""",
                (state, run_after, error, now, job["id"]))
            self.db.execute("""UPDATE attempt_log SET finished_at = ?, outcome = ?, error = ?
                WHERE job_id = ? AND lease_token = ?""", (now, outcome, error, job["id"], job["lease_token"]))
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def process_one(self, *, now: int | None = None) -> bool:
        job = self.claim(now=now)
        if job is None:
            return False
        try:
            output = handle(job["task_type"], json.loads(job["payload_json"]))
            self.finish(job, output, now=now)
        except ValueError as exc:
            self.fail(job, str(exc), now=now)
        return True

    def requeue_dead(self, job_id: int, *, actor: str, reason: str, now: int | None = None) -> int:
        """Manual recovery creates a linked job, preserving the original idempotency record."""
        now = int(time.time()) if now is None else now
        actor, reason = actor.strip(), reason.strip()
        if not 2 <= len(actor) <= 100 or not 5 <= len(reason) <= 500:
            raise InvalidRequeue("actor or reason is invalid")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if row is None or row["state"] != "dead":
                raise InvalidRequeue("only a dead job can be requeued")
            cursor = self.db.execute("""INSERT INTO jobs
                (idempotency_key, payload_hash, task_type, payload_json, max_attempts, run_after, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", (
                    f"requeue:{job_id}:{uuid.uuid4().hex}", row["payload_hash"], row["task_type"],
                    row["payload_json"], row["max_attempts"], now, now, now,
                ))
            self.db.execute("""INSERT INTO operator_actions (job_id, new_job_id, actor, action, reason, created_at)
                VALUES (?, ?, ?, 'requeue_dead', ?, ?)""", (job_id, cursor.lastrowid, actor, reason, now))
            self.db.execute("COMMIT")
            return cursor.lastrowid
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def metrics(self, *, now: int | None = None) -> dict:
        now = int(time.time()) if now is None else now
        counts = {row["state"]: row["count"] for row in self.db.execute(
            "SELECT state, count(*) AS count FROM jobs GROUP BY state")}
        oldest = self.db.execute("SELECT min(created_at) FROM jobs WHERE state = 'pending'").fetchone()[0]
        return {
            "jobs": {state: counts.get(state, 0) for state in ("pending", "running", "done", "dead")},
            "oldest_pending_seconds": max(0, now - oldest) if oldest is not None else None,
            "requeues": self.db.execute("SELECT count(*) FROM operator_actions").fetchone()[0],
        }


def handle(task_type: str, payload: dict) -> dict:
    if task_type == "normalize_contact":
        name = str(payload.get("name", "")).strip()
        phone = re.sub(r"\D", "", str(payload.get("phone", "")))
        if not name or not 10 <= len(phone) <= 13:
            raise ValueError("name or phone invalid")
        return {"name": name, "phone_digits": phone}
    if task_type == "draft_summary":
        amounts = payload.get("amounts_cents")
        if not isinstance(amounts, list) or not amounts or len(amounts) > 1000 or any(type(x) is not int or x < 0 for x in amounts):
            raise ValueError("amounts_cents must be a non-empty list of positive integers")
        return {"count": len(amounts), "total_cents": sum(amounts)}
    raise ValueError("unknown task type")
