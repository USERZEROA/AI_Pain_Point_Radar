"""Continuously publish real AI feedback to Apache Kafka.

SQLite is used only as a local producer checkpoint journal. The submitted
pipeline's event/analytics database is PostgreSQL, populated by its consumers.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
import hashlib
import json
import logging
import os
from pathlib import Path
import signal
import sqlite3
import threading
import time
from typing import Any
import uuid

from radar.config import Settings, configure_logging
from radar.events import Event, create_event, utc_now
from radar.kafka_client import ensure_topic, make_producer
from radar.source import AlgoliaClient, DEFAULT_QUERIES, SourceError, collect_seed, load_replay, record_from_hit

LOG = logging.getLogger(__name__)


class DeliveryError(RuntimeError):
    pass


class CheckpointStore:
    """Durable ack journal: never record successful delivery before Kafka ACK."""

    def __init__(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(str(path), timeout=20)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS delivered (
                run_id TEXT NOT NULL, source_item_id TEXT NOT NULL,
                acknowledged_at TEXT NOT NULL,
                PRIMARY KEY (run_id, source_item_id)
            );
            CREATE TABLE IF NOT EXISTS watermarks (
                query_key TEXT PRIMARY KEY, source_timestamp INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        """)
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def was_delivered(self, run_id: str, item_id: str) -> bool:
        return self.connection.execute("SELECT 1 FROM delivered WHERE run_id=? AND source_item_id=?", (run_id, item_id)).fetchone() is not None

    def mark_delivered(self, run_id: str, item_id: str) -> None:
        self.connection.execute("INSERT OR IGNORE INTO delivered VALUES (?, ?, ?)", (run_id, item_id, utc_now()))
        self.connection.commit()

    def watermark(self, key: str, default: int) -> int:
        row = self.connection.execute("SELECT source_timestamp FROM watermarks WHERE query_key=?", (key,)).fetchone()
        return int(row[0]) if row else default

    def set_watermark(self, key: str, timestamp: int) -> None:
        self.connection.execute("INSERT INTO watermarks VALUES (?, ?) ON CONFLICT(query_key) DO UPDATE SET source_timestamp=excluded.source_timestamp", (key, timestamp))
        self.connection.commit()

    def get_meta(self, key: str, default: str = "") -> str:
        row = self.connection.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return str(row[0]) if row else default

    def set_meta(self, key: str, value: str) -> None:
        self.connection.execute("INSERT INTO metadata VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
        self.connection.commit()


def send_event(producer: Any, topic: str, event: Event, timeout: float = 70.0) -> None:
    """Return only after broker delivery acknowledgement; callers journal next."""
    result: dict[str, Any] = {"done": False, "error": None}

    def delivered(error: Any, message: Any) -> None:
        result.update(done=True, error=error)

    deadline = time.monotonic() + timeout
    payload = json.dumps(event.to_dict(), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    while True:
        try:
            producer.produce(topic, key=event.thread_id.encode("utf-8"), value=payload, on_delivery=delivered)
            break
        except BufferError:
            if time.monotonic() >= deadline:
                raise DeliveryError("producer queue remained full; event not checkpointed")
            producer.poll(0.1)
    while not result["done"]:
        if time.monotonic() >= deadline:
            raise DeliveryError("Kafka delivery acknowledgement timed out; event not checkpointed")
        producer.poll(0.25)
    if result["error"] is not None:
        raise DeliveryError("Kafka rejected the event; event not checkpointed")


def publish_record(producer: Any, topic: str, record: dict[str, Any], run_id: str, mode: str,
                   checkpoint: CheckpointStore) -> bool:
    item_id = str(record["source_item_id"])
    if checkpoint.was_delivered(run_id, item_id):
        return False
    event = create_event(record, run_id, mode)
    send_event(producer, topic, event)
    checkpoint.mark_delivered(run_id, item_id)
    return True


def _new_run(prefix: str) -> str:
    return f"{prefix[:95]}-{uuid.uuid4().hex[:16]}"


def run_replay(settings: Settings, producer: Any, stop: threading.Event, *, once: bool = False,
               max_events: int | None = None) -> int:
    path = Path(settings.replay_file)
    if not path.is_file():
        LOG.info("Downloading real historical feedback before replay; no synthetic fallback")
        collect_seed(path, min_records=max(settings.seed_limit, 360), lookback_days=settings.lookback_days)
    records = load_replay(path)
    if len(records) < 360:
        raise SourceError(f"replay needs at least 360 genuine distinct records, found {len(records)}")
    fingerprint = hashlib.sha256(path.read_bytes()).hexdigest()
    checkpoint = CheckpointStore(Path(settings.state_dir) / "producer.sqlite3")
    active_key = f"replay-active:{fingerprint}:{settings.run_id or 'automatic'}"
    run_id = checkpoint.get_meta(active_key) or settings.run_id or _new_run("replay")
    checkpoint.set_meta(active_key, run_id)
    sent = 0
    try:
        while not stop.is_set():
            LOG.info("REPLAY run_id=%s records=%s interval=%.3fs source_range=%s..%s", run_id, len(records), settings.replay_interval, records[0]["source_created_at"], records[-1]["source_created_at"])
            next_emit = time.monotonic()
            for record in records:
                if stop.is_set():
                    break
                if checkpoint.was_delivered(run_id, str(record["source_item_id"])):
                    continue
                if stop.wait(max(0.0, next_emit - time.monotonic())):
                    break
                if publish_record(producer, settings.kafka_topic, record, run_id, "replay", checkpoint):
                    sent += 1
                    if sent == 1 or sent % 25 == 0:
                        LOG.info("REPLAY acknowledged=%d run_id=%s source_item_id=%s", sent, run_id, record["source_item_id"])
                next_emit = time.monotonic() + settings.replay_interval
                if max_events is not None and sent >= max_events:
                    return sent
            if stop.is_set() or once or not settings.replay_loop:
                break
            run_id = _new_run(settings.run_id or "replay")
            checkpoint.set_meta(active_key, run_id)
            LOG.info("Historical replay pass completed; next pass has a distinct run_id=%s", run_id)
    finally:
        checkpoint.close()
    return sent


def run_live(settings: Settings, producer: Any, stop: threading.Event, *, once: bool = False,
             max_events: int | None = None) -> int:
    checkpoint = CheckpointStore(Path(settings.state_dir) / "producer.sqlite3")
    run_key = "live-active:" + (settings.run_id or "automatic")
    run_id = checkpoint.get_meta(run_key) or settings.run_id or _new_run("live")
    checkpoint.set_meta(run_key, run_id)
    initial_key = "live-start:" + run_id
    initial_start = int(checkpoint.get_meta(initial_key) or str(int(time.time()) - int(os.getenv("LIVE_LOOKBACK_SECONDS", "900"))))
    checkpoint.set_meta(initial_key, str(initial_start))
    overlap = max(60, int(os.getenv("LIVE_OVERLAP_SECONDS", "1800")))
    queries = tuple(value.strip() for value in os.getenv("HN_QUERIES", "|".join(DEFAULT_QUERIES)).split("|") if value.strip())
    if not queries:
        checkpoint.close()
        raise ValueError("HN_QUERIES must contain at least one query")
    source = AlgoliaClient(max_requests=int(os.getenv("LIVE_REQUEST_BUDGET", "120")))
    sent = 0
    failures = 0
    LOG.info("LIVE run_id=%s queries=%d polling=%.1fs overlap=%ds", run_id, len(queries), settings.poll_seconds, overlap)
    try:
        while not stop.is_set():
            source.request_count = 0
            source.completed_ranges.clear()
            upper = int(time.time()) - 5
            cycle_failures = 0
            for query in queries:
                if stop.is_set():
                    break
                key = f"{run_id}:{query}"
                lower = max(initial_start, checkpoint.watermark(key, initial_start) - overlap)
                try:
                    hits = source.fetch_window(query, lower, upper)
                    eligible: dict[str, dict[str, Any]] = {}
                    for hit in hits:
                        try:
                            record = record_from_hit(hit)
                        except (ValueError, TypeError, KeyError, OverflowError):
                            LOG.warning("Skipped malformed source record for query=%r", query)
                            continue
                        if record is not None:
                            eligible[record["source_item_id"]] = record
                    for record in sorted(eligible.values(), key=lambda row: (row["source_created_at"], int(row["source_item_id"]))):
                        if stop.is_set():
                            break
                        if publish_record(producer, settings.kafka_topic, record, run_id, "live", checkpoint):
                            sent += 1
                            if sent == 1 or sent % 25 == 0:
                                LOG.info("LIVE acknowledged=%d run_id=%s", sent, run_id)
                        if max_events is not None and sent >= max_events:
                            return sent
                    # A stopped/interrupted scan is not a fully acknowledged query window.
                    if not stop.is_set():
                        checkpoint.set_watermark(key, upper)
                except SourceError as exc:
                    cycle_failures += 1
                    LOG.warning("LIVE query=%r failed; watermark retained: %s", query, exc)
                # DeliveryError is deliberately fatal: restart will retry deterministic identities.
            failures = failures + 1 if cycle_failures else 0
            if once:
                if cycle_failures:
                    raise SourceError("one or more live queries failed; watermarks retained")
                break
            delay = min(max(settings.poll_seconds, 2 ** min(failures, 5)), 60.0)
            LOG.info("LIVE poll complete; total_acknowledged=%d failed_queries=%d next_poll=%.1fs", sent, cycle_failures, delay)
            stop.wait(delay)
    finally:
        source.close()
        checkpoint.close()
    return sent


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("live", "replay"))
    parser.add_argument("--run-id")
    parser.add_argument("--replay-file", type=Path)
    parser.add_argument("--replay-interval", type=float)
    parser.add_argument("--poll-seconds", type=float)
    parser.add_argument("--once", action="store_true", help="One replay pass or one live polling cycle")
    parser.add_argument("--max-events", type=int, help="Optional verification limit; normal runs are continuous")
    args = parser.parse_args()
    configure_logging()
    settings = Settings.from_env()
    updates = {name: getattr(args, name) for name in ("mode", "run_id", "replay_file", "replay_interval", "poll_seconds") if getattr(args, name) is not None}
    settings = replace(settings, **updates)
    if settings.replay_interval < 0 or settings.poll_seconds < 1 or (args.max_events is not None and args.max_events < 1):
        parser.error("intervals must be nonnegative; poll seconds and max events must be positive")
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda signum, frame: stop.set())
    producer = None
    try:
        ensure_topic(settings)
        producer = make_producer(settings)
        runner = run_live if settings.mode == "live" else run_replay
        sent = runner(settings, producer, stop, once=args.once, max_events=args.max_events)
        LOG.info("Producer stopped cleanly; acknowledged during this process=%d", sent)
        return 0
    except Exception as exc:
        LOG.error("Producer stopped: %s", exc)
        return 1
    finally:
        if producer is not None:
            remaining = producer.flush(10)
            if remaining:
                LOG.warning("%d messages remain unacknowledged; durable checkpoints will retry them", remaining)


if __name__ == "__main__":
    raise SystemExit(main())
