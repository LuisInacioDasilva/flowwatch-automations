import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from flowwatch.engine import IdempotencyConflict, StaleLease, Store
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

    def test_schema_enforces_unique_and_rollback(self):
        self.store.enqueue(key="unique", task_type="draft_summary", payload={"amounts_cents": [1]}, now=100)
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.db.execute("INSERT INTO jobs (idempotency_key, payload_hash, task_type, payload_json, run_after, created_at, updated_at) VALUES ('unique','x','draft_summary','{}',100,100,100)")
        self.store.db.executescript((Path(__file__).parents[1] / "flowwatch" / "down.sql").read_text(encoding="utf-8"))
        self.assertEqual(self.store.db.execute("SELECT name FROM sqlite_master WHERE name = 'jobs'").fetchone(), None)


if __name__ == "__main__":
    unittest.main()
