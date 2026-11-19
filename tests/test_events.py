"""Synthetic test fixtures only; these are never used as research or demo data."""

import unittest

from radar.events import Event, create_event, validate_event


def record(item_id="101"):
    return {
        "source": "hacker_news", "source_item_id": item_id, "item_type": "comment",
        "thread_id": "100", "source_created_at": "2026-01-02T03:04:05Z",
        "title": "Synthetic fixture about AI", "text": "Synthetic fixture: my AI request failed.",
        "author_hash": "",
    }


class EventTests(unittest.TestCase):
    def test_identity_stable_within_run_and_distinct_across_replay_passes(self):
        first = create_event(record(), "pass-1", "replay")
        retry = create_event(record(), "pass-1", "replay")
        next_pass = create_event(record(), "pass-2", "replay")
        self.assertEqual(first.event_id, retry.event_id)
        self.assertNotEqual(first.event_id, next_pass.event_id)

    def test_original_source_time_is_retained(self):
        event = create_event(record(), "pass-1", "replay", emitted_at="2026-11-19T12:00:00Z")
        self.assertEqual(event.source_created_at, "2026-01-02T03:04:05.000000Z")
        self.assertEqual(event.emitted_at, "2026-11-19T12:00:00.000000Z")
        self.assertEqual(event, Event.from_dict(event.to_dict()))

    def test_missing_required_field_rejected(self):
        data = create_event(record(), "pass-1", "live").to_dict()
        del data["thread_id"]
        with self.assertRaisesRegex(ValueError, "missing"):
            validate_event(data)

    def test_invalid_numeric_field_rejected(self):
        data = create_event(record(), "pass-1", "live").to_dict()
        for value in (True, 0, 3.1, "41"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "text_length"):
                    validate_event({**data, "text_length": value})

    def test_forged_identity_and_source_link_rejected(self):
        data = create_event(record(), "pass-1", "live").to_dict()
        with self.assertRaisesRegex(ValueError, "event_id"):
            validate_event({**data, "event_id": "0" * 64})
        with self.assertRaisesRegex(ValueError, "source_url"):
            validate_event({**data, "source_url": "https://unrelated.example/"})

    def test_naive_timestamp_rejected_and_offset_normalized(self):
        data = create_event(record(), "pass-1", "live").to_dict()
        with self.assertRaisesRegex(ValueError, "timezone"):
            validate_event({**data, "source_created_at": "2026-01-01T10:00:00"})
        event = validate_event({**data, "source_created_at": "2026-01-01T10:00:00+02:00"})
        self.assertEqual(event.source_created_at, "2026-01-01T08:00:00.000000Z")

    def test_unknown_schema_or_fields_rejected(self):
        data = create_event(record(), "pass-1", "live").to_dict()
        with self.assertRaisesRegex(ValueError, "schema_version"):
            validate_event({**data, "schema_version": 2})
        with self.assertRaisesRegex(ValueError, "unsupported event fields"):
            validate_event({**data, "made_up_likes": 100})

    def test_optional_hash_and_categorical_fields(self):
        data = create_event(record(), "pass-1", "live").to_dict()
        del data["author_hash"]
        self.assertEqual(validate_event(data).author_hash, "")
        for field, value in (("mode", "fake"), ("item_type", "purchase"), ("source_item_id", "not-a-number")):
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    validate_event({**data, field: value})


if __name__ == "__main__":
    unittest.main()
