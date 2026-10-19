import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from flowwatch.engine import IdempotencyConflict, InvalidRequeue, StaleLease, Store
from flowwatch.report import write_report


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "jobs.sqlite"
        self.store = Store(self.path)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_idempotency_and_payload_conflict(self):
        first = self.store.enqueue(key="contact-1", task_type="normalize_contact", payload={"name": "Ana", "phone": "11999998888"}, now=100)
        repeat = self.store.enqueue(key="contact-1", task_type="normalize_contact", payload={"phone": "11999998888", "name": "Ana"}, now=100)
        self.assertEqual(first, repeat)
        with self.assertRaises(IdempotencyConflict):
            self.store.enqueue(key="contact-1", task_type="normalize_contact", payload={"name": "Outra", "phone": "11999998888"}, now=100)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM jobs").fetchone()[0], 1)

    def test_success_is_atomic_with_result_and_attempt(self):
        job_id = self.store.enqueue(key="summary-1", task_type="draft_summary", payload={"amounts_cents": [100, 250]}, now=100)
        self.assertTrue(self.store.process_one(now=100))
        job = self.store.db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        self.assertEqual(job["state"], "done")
        self.assertEqual(job["attempts"], 1)
        output = self.store.db.execute("SELECT output_json FROM results WHERE job_id = ?", (job_id,)).fetchone()[0]
        self.assertEqual(json.loads(output)["total_cents"], 350)
        self.assertEqual(self.store.db.execute("SELECT outcome FROM attempt_log").fetchone()[0], "done")

    def test_retry_backoff_then_dead_letter(self):
        self.store.enqueue(key="invalid-1", task_type="normalize_contact", payload={"name": "Ana", "phone": "12"}, now=100, max_attempts=2)
        self.assertTrue(self.store.process_one(now=100))
        row = self.store.db.execute("SELECT state, run_after FROM jobs").fetchone()
        self.assertEqual((row["state"], row["run_after"]), ("pending", 105))
        self.assertFalse(self.store.process_one(now=104))
        self.assertTrue(self.store.process_one(now=105))
        self.assertEqual(self.store.db.execute("SELECT state FROM jobs").fetchone()[0], "dead")
        self.assertEqual([row[0] for row in self.store.db.execute("SELECT outcome FROM attempt_log ORDER BY id")], ["retry", "dead"])

    def test_expired_lease_is_reclaimed_and_old_worker_is_fenced(self):
        self.store.enqueue(key="contact-2", task_type="normalize_contact", payload={"name": "Ana", "phone": "11999998888"}, now=100)
        old = self.store.claim(now=100, lease_seconds=10)
        with self.assertRaises(StaleLease):
            self.store.finish(old, {"bad": True}, now=110)
        new = self.store.claim(now=110, lease_seconds=10)
        self.assertNotEqual(old["lease_token"], new["lease_token"])
        with self.assertRaises(StaleLease):
            self.store.finish(old, {"bad": True}, now=111)
        self.store.finish(new, {"ok": True}, now=111)
        self.assertEqual(self.store.db.execute("SELECT state FROM jobs").fetchone()[0], "done")
        self.assertEqual([row[0] for row in self.store.db.execute("SELECT outcome FROM attempt_log ORDER BY id")], ["lease_expired", "done"])

    def test_expired_last_attempt_goes_to_dead_letter(self):
        self.store.enqueue(key="contact-3", task_type="normalize_contact", payload={"name": "Ana", "phone": "11999998888"}, now=100, max_attempts=1)
        self.store.claim(now=100, lease_seconds=10)
        self.assertIsNone(self.store.claim(now=110))
        self.assertEqual(self.store.db.execute("SELECT state FROM jobs").fetchone()[0], "dead")

    def test_report_escapes_errors_and_excludes_payload(self):
        self.store.enqueue(key="contact-4", task_type="normalize_contact", payload={"name": "Ana", "phone": "11999998888"}, now=100)
        job = self.store.claim(now=100)
        self.store.fail(job, "<script>alert(1)</script>", now=101)
        report = write_report(self.store, Path(self.temp.name) / "report.html").read_text(encoding="utf-8")
        self.assertIn("&lt;script&gt;", report)
        self.assertNotIn("11999998888", report)

    def test_dead_job_recovery_creates_linked_job_and_immutable_operator_record(self):
        original = self.store.enqueue(key="recovery", task_type="normalize_contact", payload={"name": "Ana", "phone": "12"}, now=100, max_attempts=1)
        self.store.process_one(now=100)
        new_id = self.store.requeue_dead(original, actor="operador", reason="Corrigir dado na origem", now=101)
        self.assertNotEqual(original, new_id)
        self.assertEqual(self.store.db.execute("SELECT state FROM jobs WHERE id = ?", (original,)).fetchone()[0], "dead")
        self.assertEqual(self.store.db.execute("SELECT state FROM jobs WHERE id = ?", (new_id,)).fetchone()[0], "pending")
        self.assertEqual(self.store.db.execute("SELECT new_job_id FROM operator_actions WHERE job_id = ?", (original,)).fetchone()[0], new_id)
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.db.execute("DELETE FROM operator_actions")
        self.assertEqual(self.store.metrics(now=101)["requeues"], 1)
        with self.assertRaises(InvalidRequeue):
            self.store.requeue_dead(new_id, actor="operador", reason="Tentar novamente", now=102)

    def test_metrics_pending_age_is_never_negative(self):
        self.store.enqueue(key="age", task_type="draft_summary", payload={"amounts_cents": [1]}, now=100)
        self.assertEqual(self.store.metrics(now=105)["oldest_pending_seconds"], 5)
        self.assertEqual(self.store.metrics(now=95)["oldest_pending_seconds"], 0)
        self.store.process_one(now=100)
        self.assertIsNone(self.store.metrics(now=105)["oldest_pending_seconds"])

    def test_schedule_materializes_each_due_time_once_and_can_be_paused(self):
        schedule_id = self.store.add_schedule(name="daily-summary", task_type="draft_summary",
            payload={"amounts_cents": [100, 250]}, interval_seconds=60, start_at=100)
        self.assertEqual(self.store.tick(now=99), 0)
        self.assertEqual(self.store.tick(now=100), 1)
        self.assertEqual(self.store.tick(now=100), 0)
        self.assertTrue(self.store.process_one(now=100))
        self.assertEqual(self.store.tick(now=160), 1)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM schedule_runs").fetchone()[0], 2)
        self.store.set_schedule_enabled(schedule_id, False)
        self.assertEqual(self.store.tick(now=220), 0)
        self.store.set_schedule_enabled(schedule_id, True)
        self.assertEqual(self.store.tick(now=220), 1)
        self.assertEqual(self.store.metrics(now=220)["schedules_enabled"], 1)
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.db.execute("DELETE FROM schedule_runs")

    def test_schedule_backfill_is_bounded_and_config_is_idempotent(self):
        first = self.store.add_schedule(name="contact-check", task_type="normalize_contact",
            payload={"name": "Ana", "phone": "11999998888"}, interval_seconds=60, start_at=100)
        again = self.store.add_schedule(name="contact-check", task_type="normalize_contact",
            payload={"phone": "11999998888", "name": "Ana"}, interval_seconds=60, start_at=999)
        self.assertEqual(first, again)
        with self.assertRaises(IdempotencyConflict):
            self.store.add_schedule(name="contact-check", task_type="draft_summary",
                payload={"amounts_cents": [1]}, interval_seconds=60, start_at=100)
        self.assertEqual(self.store.tick(now=400, max_per_schedule=2), 2)
        self.assertEqual(self.store.metrics(now=400)["schedule_backlog"], 1)
        self.assertEqual(self.store.tick(now=400, max_per_schedule=2), 2)
        self.assertEqual(self.store.tick(now=400, max_per_schedule=2), 2)
        self.assertEqual(self.store.tick(now=400, max_per_schedule=2), 0)
        self.assertEqual(self.store.db.execute("SELECT count(DISTINCT idempotency_key) FROM jobs").fetchone()[0], 6)
        other_worker = Store(self.path)
        try:
            self.assertEqual(other_worker.tick(now=400), 0)
        finally:
            other_worker.close()

    def test_schema_enforces_unique_and_rollback(self):
        self.store.enqueue(key="unique", task_type="draft_summary", payload={"amounts_cents": [1]}, now=100)
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.db.execute("INSERT INTO jobs (idempotency_key, payload_hash, task_type, payload_json, run_after, created_at, updated_at) VALUES ('unique','x','draft_summary','{}',100,100,100)")
        self.store.db.executescript((Path(__file__).parents[1] / "flowwatch" / "down.sql").read_text(encoding="utf-8"))
        self.assertEqual(self.store.db.execute("SELECT name FROM sqlite_master WHERE name = 'jobs'").fetchone(), None)
        self.assertEqual(self.store.db.execute("SELECT name FROM sqlite_master WHERE name = 'schedules'").fetchone(), None)


if __name__ == "__main__":
    unittest.main()
