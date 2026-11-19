"""Dashboard safety/semantics tests; fixtures here are explicitly synthetic."""

from __future__ import annotations

import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from dashboard import age_label, safe_source_url, seconds_since, source_lower_bound


class DashboardSemanticsTests(unittest.TestCase):
    def test_source_links_reject_script_and_lookalike_hosts(self):
        self.assertEqual(safe_source_url("https://news.ycombinator.com/item?id=123&foo=bar"),
                         "https://news.ycombinator.com/item?id=123")
        for url in ["javascript:alert(1)", "https://news.ycombinator.com.evil.test/item?id=123",
                    "https://evil.test/item?id=123", "http://news.ycombinator.com/item?id=123",
                    "https://news.ycombinator.com/item?id=123&id=456", None]:
            with self.subTest(url=url):
                self.assertIsNone(safe_source_url(url))

    def test_source_range_is_relative_to_replayed_time_not_today(self):
        first = datetime(2025, 1, 1, 1, 12, tzinfo=timezone.utc)
        last = datetime(2025, 1, 8, 10, 45, tzinfo=timezone.utc)
        self.assertEqual(source_lower_bound(first, last, "Last 24 source hours"),
                         datetime(2025, 1, 7, 11, tzinfo=timezone.utc))
        self.assertEqual(source_lower_bound(first, last, "Entire run"),
                         datetime(2025, 1, 1, 1, tzinfo=timezone.utc))

    def test_future_ingestion_clock_does_not_display_negative_age(self):
        now = datetime(2026, 1, 1, 0, tzinfo=timezone.utc)
        future = datetime(2026, 1, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(seconds_since(future, now), 0)
        self.assertEqual(age_label(None), "No records yet")


class DashboardEmptyStateTests(unittest.TestCase):
    def test_evidence_category_can_be_changed_even_when_current_category_is_empty(self):
        from streamlit.testing.v1 import AppTest

        app = AppTest.from_string('''
from dashboard import Snapshot, render_evidence
render_evidence(Snapshot({}, {}, {}, [], [], [], [], []))
''', default_timeout=20).run()
        self.assertEqual(len(app.exception), 0)
        self.assertEqual(app.radio[0].options, ["Pain reports", "All classifications", "Uncertain"])
        app.radio[0].set_value("All classifications").run()
        self.assertEqual(len(app.exception), 0)
        self.assertEqual(app.radio[0].value, "All classifications")

    def test_missing_database_is_explicit_and_does_not_generate_sample_data(self):
        from streamlit.testing.v1 import AppTest

        path = Path(__file__).resolve().parents[1] / "dashboard.py"
        with patch.dict(os.environ, {"DATABASE_URL": ""}):
            app = AppTest.from_file(str(path), default_timeout=20).run()
        self.assertEqual(len(app.exception), 0)
        self.assertTrue(any("DATABASE_URL" in e.value for e in app.error))
        self.assertEqual(len(app.metric), 0)
        self.assertEqual(len(app.selectbox), 2)

    def test_database_failure_does_not_expose_credentials(self):
        from streamlit.testing.v1 import AppTest

        path = Path(__file__).resolve().parents[1] / "dashboard.py"
        secret = "DO_NOT_RENDER_DATABASE_PASSWORD"
        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://unused/test"}), \
             patch("radar.db.connect", side_effect=RuntimeError(secret)):
            app = AppTest.from_file(str(path), default_timeout=20).run()
        self.assertEqual(len(app.exception), 0)
        self.assertTrue(any("Database unavailable" in e.value for e in app.error))
        visible_text = " ".join(str(e.value) for e in list(app.error) + list(app.caption))
        self.assertNotIn(secret, visible_text)
        self.assertEqual(len(app.metric), 0)

    def test_persisted_result_views_render_charts_and_warmup_without_db(self):
        """Synthetic UI fixtures validate chart encoding, never feed production."""
        from streamlit.testing.v1 import AppTest

        app = AppTest.from_string('''
from datetime import datetime, timedelta, timezone
import streamlit as st
from dashboard import Snapshot, render_overview, render_evidence, render_sequence
st.set_page_config(layout="wide")
base = datetime(2025, 1, 1, tzinfo=timezone.utc)
snapshot = Snapshot(
    run={}, metrics={}, pipeline={},
    windows=[dict(window_start=base + timedelta(hours=i), category="Reliability",
                  record_count=i, thread_count=1, total_ai_records=12, pain_share=i / 12)
             for i in range(2)],
    distribution=[dict(category="Reliability", record_count=3, thread_count=2)],
    heatmap=[dict(product="Test-only product", category="Reliability", record_count=3)],
    evidence=[dict(source_created_at=base, product="Test-only product", category="Reliability",
                   is_pain=True, evidence="TEST FIXTURE: incorrect output", title="TEST FIXTURE",
                   source_url="https://news.ycombinator.com/item?id=1", thread_id="1",
                   method="fixture", rule_version="fixture")],
    signals=[dict(window_start=base + timedelta(hours=i), category="Reliability",
                  actual_count=float(i), predicted_count=float(max(0, i - 1)), residual=1.0,
                  anomaly_score=0.0, is_anomaly=False, history_windows=i, model_version="fixture")
             for i in range(4)],
)
render_overview(snapshot)
render_evidence(snapshot)
render_sequence(snapshot)
''', default_timeout=20).run()
        self.assertEqual(len(app.exception), 0)
        self.assertGreaterEqual(len(app.get("vega_lite_chart")), 4)
        self.assertTrue(any("warming up" in e.value for e in app.warning))
        self.assertEqual(len(app.metric), 3)


if __name__ == "__main__":
    unittest.main()
