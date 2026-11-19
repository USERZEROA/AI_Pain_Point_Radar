"""Real PostgreSQL integration checks, using labeled synthetic fixtures only.

Set TEST_DATABASE_URL to a disposable PostgreSQL database to execute. Every test
uses isolated run IDs and removes only its own rows; no database-wide truncation.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import unittest
import uuid

from radar.db import connect, init_db, redacted_payload, reject_event, store_analysis, store_raw
from radar.events import event_identity


def fixture_event(run_id, item_id="1", hour=0, text="I keep hitting the usage limit in ChatGPT."):
    created = datetime(2026, 1, 1, 0, 15, tzinfo=timezone.utc) + timedelta(hours=hour)
    return {
        "event_id": event_identity("hacker_news", item_id, run_id),
        "schema_version": 1, "source_item_id": item_id, "source": "hacker_news",
        "item_type": "comment", "thread_id": "100", "source_created_at": created.isoformat(),
        "emitted_at": datetime.now(timezone.utc).isoformat(), "title": "TEST FIXTURE: AI discussion",
        "text": text, "text_length": len(text), "source_url": f"https://news.ycombinator.com/item?id={item_id}",
        "mode": "replay", "run_id": run_id, "author_hash": hashlib.sha256(b"test-author").hexdigest(),
    }


class DiagnosticTests(unittest.TestCase):
    def test_poison_diagnostics_do_not_copy_arbitrary_secrets(self):
        result = redacted_payload(json.dumps({"event_id": "test", "password": "do-not-copy", "api_key": "secret", "text": "not needed"}))
        self.assertNotIn("do-not-copy", result)
        self.assertNotIn("secret", result)
        self.assertNotIn("not needed", result)
        self.assertIn("sha256", result)


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "TEST_DATABASE_URL is required for PostgreSQL integration checks")
class StorageTests(unittest.TestCase):
    def setUp(self):
        self.conn = connect(os.environ["TEST_DATABASE_URL"])
        init_db(self.conn)
        self.run_id = "test-" + uuid.uuid4().hex

    def tearDown(self):
        self.conn.rollback()
        with self.conn.transaction():
            for table in ("raw_events", "painpoint_results", "painpoint_windows", "sequence_signals"):
                self.conn.execute(f"DELETE FROM {table} WHERE run_id=%s", (self.run_id,))
            self.conn.execute("DELETE FROM rejected_events WHERE kafka_topic=%s", (self.run_id,))
        self.conn.close()

    def test_raw_re_delivery_is_idempotent(self):
        event = fixture_event(self.run_id)
        with self.conn.transaction():
            self.assertTrue(store_raw(self.conn, event, 0, 1))
            self.assertFalse(store_raw(self.conn, event, 0, 1))
        row = self.conn.execute("SELECT count(*) AS n,max(text_length) AS length FROM raw_events WHERE run_id=%s", (self.run_id,)).fetchone()
        self.assertEqual(row["n"], 1)
        self.assertEqual(row["length"], len(event["text"]))

    def test_analytics_can_precede_raw_and_does_not_double_count(self):
        event = fixture_event(self.run_id)
        with self.conn.transaction():
            self.assertTrue(store_analysis(self.conn, event))
            self.assertFalse(store_analysis(self.conn, event))
        row = self.conn.execute("SELECT record_count,thread_count,total_ai_records,pain_share FROM painpoint_windows WHERE run_id=%s AND category='Cost & limits'", (self.run_id,)).fetchone()
        self.assertEqual(dict(row), {"record_count": 1, "thread_count": 1, "total_ai_records": 1, "pain_share": 1.0})
        self.assertEqual(self.conn.execute("SELECT count(*) AS n FROM raw_events WHERE run_id=%s", (self.run_id,)).fetchone()["n"], 0)

    def test_denominator_includes_non_pain_and_distinct_threads_are_not_comments(self):
        with self.conn.transaction():
            store_analysis(self.conn, fixture_event(self.run_id, "1"))
            store_analysis(self.conn, fixture_event(self.run_id, "2"))
            store_analysis(self.conn, fixture_event(self.run_id, "3", text="I use AI tools and they work well."))
        row = self.conn.execute("SELECT * FROM painpoint_windows WHERE run_id=%s AND category='Cost & limits'", (self.run_id,)).fetchone()
        self.assertEqual(row["record_count"], 2)
        self.assertEqual(row["thread_count"], 1)
        self.assertEqual(row["total_ai_records"], 3)
        self.assertAlmostEqual(row["pain_share"], 2/3)

    def test_rollback_does_not_leave_partial_raw_or_derived_rows(self):
        with self.assertRaises(RuntimeError):
            with self.conn.transaction():
                store_raw(self.conn, fixture_event(self.run_id))
                store_analysis(self.conn, fixture_event(self.run_id))
                raise RuntimeError("injected failure")
        self.assertEqual(self.conn.execute("SELECT count(*) AS n FROM raw_events WHERE run_id=%s", (self.run_id,)).fetchone()["n"], 0)
        self.assertEqual(self.conn.execute("SELECT count(*) AS n FROM painpoint_results WHERE run_id=%s", (self.run_id,)).fetchone()["n"], 0)

    def test_late_record_recalculates_affected_forecasts_without_duplicate_inflation(self):
        with self.conn.transaction():
            for hour in range(13):
                store_analysis(self.conn, fixture_event(self.run_id, str(hour + 1), hour))
        before = self.conn.execute("SELECT * FROM sequence_signals WHERE run_id=%s AND category='Cost & limits' ORDER BY window_start", (self.run_id,)).fetchall()
        self.conn.commit()
        self.assertEqual(len(before), 11)
        with self.conn.transaction():
            store_analysis(self.conn, fixture_event(self.run_id, "9000", 9))
        after = self.conn.execute("SELECT * FROM sequence_signals WHERE run_id=%s AND category='Cost & limits' ORDER BY window_start", (self.run_id,)).fetchall()
        self.assertEqual(before[8]["predicted_count"], after[8]["predicted_count"])
        self.assertEqual(after[8]["actual_count"], before[8]["actual_count"] + 1)
        self.assertGreater(after[9]["predicted_count"], before[9]["predicted_count"])

    def test_rejected_event_is_durable_and_idempotent(self):
        with self.conn.transaction():
            self.assertTrue(reject_event(self.conn, "storage", "invalid_event", b"not-json", 0, 42, self.run_id))
            self.assertFalse(reject_event(self.conn, "storage", "invalid_event", b"not-json", 0, 42, self.run_id))
        self.assertEqual(self.conn.execute("SELECT count(*) AS n FROM rejected_events WHERE kafka_topic=%s", (self.run_id,)).fetchone()["n"], 1)

    def test_process_message_is_committed_before_return_and_poison_is_quarantined(self):
        from radar.consumer import process_message

        class Message:
            def __init__(self, value, topic, offset):
                self._value, self._topic, self._offset = value, topic, offset
            def value(self): return self._value
            def partition(self): return 0
            def offset(self): return self._offset
            def topic(self): return self._topic

        inserted, rejected = process_message(self.conn, Message(json.dumps(fixture_event(self.run_id)).encode(), self.run_id, 7), "storage", "test-storage")
        self.assertTrue(inserted)
        self.assertFalse(rejected)
        # A different connection sees this immediately, before any Kafka commit.
        with connect(os.environ["TEST_DATABASE_URL"]) as observer:
            self.assertEqual(observer.execute("SELECT count(*) AS n FROM raw_events WHERE run_id=%s", (self.run_id,)).fetchone()["n"], 1)
        inserted, rejected = process_message(self.conn, Message(b"not-json", self.run_id, 8), "storage", "test-storage")
        self.assertFalse(inserted)
        self.assertTrue(rejected)
        with connect(os.environ["TEST_DATABASE_URL"]) as observer:
            self.assertEqual(observer.execute("SELECT count(*) AS n FROM rejected_events WHERE kafka_topic=%s", (self.run_id,)).fetchone()["n"], 1)


if __name__ == "__main__":
    unittest.main()
