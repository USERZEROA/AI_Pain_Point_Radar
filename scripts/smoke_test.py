"""Send a fixture through both consumer groups in a disposable Compose stack."""

from datetime import datetime, timezone
from pathlib import Path
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from radar.config import Settings
from radar.db import connect
from radar.events import create_event
from radar.kafka_client import make_producer
from radar.producer import send_event


def main() -> None:
    settings = Settings.from_env()
    run_id = f"ci-{uuid.uuid4().hex}"
    event = create_event(
        {
            "source_item_id": "1",
            "thread_id": "1",
            "item_type": "comment",
            "source_created_at": datetime.now(timezone.utc).isoformat(),
            "text": "I keep hitting the usage limit in ChatGPT.",
            "title": "CI fixture",
        },
        run_id,
        "replay",
    )
    send_event(make_producer(settings), settings.kafka_topic, event)

    deadline = time.monotonic() + 30
    with connect(settings.database_url, autocommit=True) as conn:
        while time.monotonic() < deadline:
            row = conn.execute(
                """SELECT raw.text_length, result.is_pain, win.record_count
                   FROM raw_events AS raw
                   JOIN painpoint_results AS result USING (event_id)
                   JOIN painpoint_windows AS win
                     ON win.run_id = result.run_id AND win.category = result.category
                   WHERE raw.event_id = %s AND result.category = 'Cost & limits'""",
                (event.event_id,),
            ).fetchone()
            if row == {"text_length": event.text_length, "is_pain": True, "record_count": 1}:
                print("Kafka fixture reached raw storage and the derived PostgreSQL window.")
                return
            time.sleep(0.5)
    raise SystemExit("Kafka fixture did not reach both PostgreSQL outputs within 30 seconds.")


if __name__ == "__main__":
    main()
