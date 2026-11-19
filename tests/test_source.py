"""Offline synthetic fixtures cover source bounds, replay integrity and ACK recovery."""

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from radar.config import Settings
from radar.events import create_event
from radar.producer import CheckpointStore, DeliveryError, publish_record, run_live, run_replay
from radar.source import AlgoliaClient, CollectionIncomplete, SourceError, clean_html, collect_seed, is_ai_relevant, load_replay, record_from_hit


def hit(item_id="101", text="Synthetic fixture: my LLM response failed.", created=1767225600):
    return {"objectID": item_id, "_tags": ["comment", "story_100"], "story_id": 100,
            "story_title": "Synthetic AI fixture", "comment_text": text,
            "created_at_i": created, "author": "fixture-account"}


class Response:
    def __init__(self, body, status=200, headers=None):
        self.body, self.status_code, self.headers = body, status, headers or {}

    def json(self):
        return self.body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise SourceError("fixture HTTP error")


class HTTPStub:
    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    def get(self, url, params):
        self.calls.append(dict(params))
        return self.handler(params)

    def close(self):
        pass


class KafkaStub:
    def __init__(self, fail_at=None):
        self.messages = []
        self.callback = None
        self.fail_at = fail_at

    def produce(self, topic, key, value, on_delivery):
        self.messages.append((topic, key, json.loads(value)))
        self.callback = on_delivery

    def poll(self, timeout):
        if self.callback:
            callback, self.callback = self.callback, None
            callback("simulated delivery failure" if len(self.messages) == self.fail_at else None, None)


class SourceTests(unittest.TestCase):
    def test_clean_html_preserves_text_and_removes_active_markup(self):
        self.assertEqual(clean_html("<p>A &amp; B</p><script>bad()</script><p>C<br>D</p>"), "A & B\n\nC\nD")

    def test_relevance_does_not_match_almost_or_generic_cursor(self):
        self.assertFalse(is_ai_relevant("Almost all libraries are quiet."))
        self.assertFalse(is_ai_relevant("Move the cursor across the screen."))
        self.assertFalse(is_ai_relevant("The Gemini telescope photographed a star."))
        self.assertTrue(is_ai_relevant("Cursor's coding assistant failed."))
        self.assertTrue(is_ai_relevant("This LLM seems useful."))
        self.assertTrue(is_ai_relevant("It helped me today.", "Using ChatGPT"))

    def test_comment_identity_hash_and_missing_scores(self):
        row = record_from_hit(hit())
        self.assertEqual(row["thread_id"], "100")
        self.assertEqual(row["text_length"], len(row["text"]))
        self.assertNotIn("score", row)
        self.assertNotIn("author", row)
        self.assertEqual(len(row["author_hash"]), 64)

    def test_story_title_is_valid_content_when_body_absent(self):
        row = record_from_hit({"objectID": "100", "_tags": ["story"], "title": "Ask HN: AI reliability?", "created_at_i": 1767225600})
        self.assertEqual(row["item_type"], "story")
        self.assertEqual(row["thread_id"], "100")
        self.assertEqual(row["text"], row["title"])

    def test_exact_search_and_pagination(self):
        http = HTTPStub(lambda p: Response({"hits": [hit(str(101 + p["page"]))], "nbHits": 2, "nbPages": 2}))
        client = AlgoliaClient(http)
        rows = client.fetch_window("LLM", 100, 200)
        self.assertEqual(len(rows), 2)
        self.assertEqual(http.calls[0]["typoTolerance"], "false")
        self.assertEqual(http.calls[0]["queryType"], "prefixNone")
        self.assertEqual(http.calls[0]["numericFilters"], "created_at_i>=100,created_at_i<200")
        self.assertEqual([call["page"] for call in http.calls], [0, 1])

    def test_pagination_cap_splits_contiguous_time_without_gaps(self):
        def handler(params):
            if params["numericFilters"] == "created_at_i>=100,created_at_i<200":
                return Response({"hits": [], "nbHits": 2000, "nbPages": 10})
            return Response({"hits": [hit()], "nbHits": 1, "nbPages": 1})
        http = HTTPStub(handler)
        client = AlgoliaClient(http)
        client.fetch_window("LLM", 100, 200)
        self.assertEqual([(r["start"], r["end"]) for r in client.completed_ranges], [(100, 150), (150, 200)])

    def test_request_budget_failure_is_explicit(self):
        http = HTTPStub(lambda p: Response({"hits": [], "nbHits": 2000, "nbPages": 10}))
        client = AlgoliaClient(http, max_requests=1)
        with self.assertRaisesRegex(CollectionIncomplete, "budget"):
            client.fetch_window("LLM", 100, 200)

    def test_retry_after_is_obeyed_and_bounded(self):
        responses = iter([Response({}, 429, {"Retry-After": "2"}), Response({"hits": [], "nbHits": 0, "nbPages": 0})])
        delays = []
        client = AlgoliaClient(HTTPStub(lambda p: next(responses)), sleep=delays.append)
        self.assertEqual(client.fetch_window("LLM", 100, 200), [])
        self.assertEqual(delays, [2.0])

    def test_collection_keeps_whole_window_above_minimum_and_verifies_hash(self):
        http = HTTPStub(lambda p: Response({"hits": [hit("101"), hit("102")], "nbHits": 2, "nbPages": 1}))
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "fixture.jsonl"
            manifest = collect_seed(output, min_records=1, lookback_days=1, queries=["LLM", "ChatGPT"], client=AlgoliaClient(http), end_timestamp=1767312000)
            self.assertEqual(manifest["record_count"], 2)
            self.assertTrue(manifest["pagination_complete"])
            self.assertEqual(len(load_replay(output)), 2)
            output.write_text(output.read_text() + "\n")
            with self.assertRaisesRegex(SourceError, "SHA256"):
                load_replay(output)

    def test_incomplete_collection_does_not_overwrite_existing_corpus(self):
        http = HTTPStub(lambda p: Response({"hits": [], "nbHits": 0, "nbPages": 0}))
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "fixture.jsonl"
            output.write_text("existing corpus")
            with self.assertRaises(CollectionIncomplete):
                collect_seed(output, min_records=10, lookback_days=1, queries=["LLM"], client=AlgoliaClient(http), end_timestamp=1767312000)
            self.assertEqual(output.read_text(), "existing corpus")
            manifest = json.loads(output.with_suffix(".incomplete.manifest.json").read_text())
            self.assertFalse(manifest["pagination_complete"])


class DeliveryTests(unittest.TestCase):
    def test_success_is_journaled_and_duplicate_suppressed_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.sqlite"
            checkpoint = CheckpointStore(path)
            kafka = KafkaStub()
            row = record_from_hit(hit())
            self.assertTrue(publish_record(kafka, "topic", row, "run-1", "live", checkpoint))
            checkpoint.close()
            checkpoint = CheckpointStore(path)
            self.assertFalse(publish_record(kafka, "topic", row, "run-1", "live", checkpoint))
            self.assertTrue(publish_record(kafka, "topic", row, "run-2", "replay", checkpoint))
            self.assertEqual(len(kafka.messages), 2)
            checkpoint.close()

    def test_failed_ack_does_not_create_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            checkpoint = CheckpointStore(Path(directory) / "checkpoint.sqlite")
            with self.assertRaises(DeliveryError):
                publish_record(KafkaStub(fail_at=1), "topic", record_from_hit(hit()), "run-1", "live", checkpoint)
            self.assertFalse(checkpoint.was_delivered("run-1", "101"))
            checkpoint.close()

    def test_live_watermark_remains_before_partial_delivery_failure(self):
        class FakeSource:
            request_count = 0
            completed_ranges = []
            def fetch_window(self, *args):
                return [hit("101"), hit("102")]
            def close(self):
                pass
        with tempfile.TemporaryDirectory() as directory:
            settings = replace(Settings(), state_dir=Path(directory), run_id="test-live")
            with patch("radar.producer.AlgoliaClient", return_value=FakeSource()), patch.dict("os.environ", {"HN_QUERIES": "LLM"}):
                with self.assertRaises(DeliveryError):
                    run_live(settings, KafkaStub(fail_at=2), threading.Event(), once=True)
            checkpoint = CheckpointStore(Path(directory) / "producer.sqlite3")
            self.assertTrue(checkpoint.was_delivered("test-live", "101"))
            self.assertFalse(checkpoint.was_delivered("test-live", "102"))
            self.assertEqual(checkpoint.watermark("test-live:LLM", -1), -1)
            checkpoint.close()

    def test_replay_restart_resumes_without_mutating_source_time(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.jsonl"
            rows = [record_from_hit(hit(str(1000 + i), created=1767225600 + i)) for i in range(360)]
            data = "".join(json.dumps(row) + "\n" for row in rows).encode()
            path.write_bytes(data)
            path.with_suffix(".manifest.json").write_text(json.dumps({"pagination_complete": True, "record_count": len(rows), "sha256": hashlib.sha256(data).hexdigest()}))
            settings = replace(Settings(), replay_file=path, state_dir=Path(directory) / "state", run_id="test-replay", replay_interval=0, replay_loop=False)
            kafka = KafkaStub()
            self.assertEqual(run_replay(settings, kafka, threading.Event(), max_events=2), 2)
            self.assertEqual(run_replay(settings, kafka, threading.Event(), max_events=2), 2)
            ids = [message[2]["source_item_id"] for message in kafka.messages]
            self.assertEqual(ids, ["1000", "1001", "1002", "1003"])
            self.assertEqual(kafka.messages[0][1], b"100")
            self.assertEqual(kafka.messages[0][2]["source_created_at"], rows[0]["source_created_at"])


if __name__ == "__main__":
    unittest.main()
