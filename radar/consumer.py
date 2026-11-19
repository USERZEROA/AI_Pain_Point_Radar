"""Kafka consumers: commit durable PostgreSQL work before committing offsets."""
from __future__ import annotations

import argparse
import json
import logging
import signal
import time
from typing import Any

import psycopg
from confluent_kafka import KafkaException, KafkaError

from radar.config import Settings, configure_logging
from radar.db import connect, heartbeat, init_db, reject_event, store_analysis, store_raw
from radar.events import validate_event
from radar.kafka_client import ensure_topic, make_consumer

LOG = logging.getLogger(__name__)


def process_message(conn: psycopg.Connection, message: Any, role: str, group: str) -> tuple[bool, bool]:
    """Return (inserted, rejected) only after a successful database transaction.

    Malformed messages are quarantined and may then be acknowledged. Unexpected
    database/programming failures propagate; their Kafka offset is never advanced.
    """
    try:
        payload = message.value()
        if payload is None:
            raise ValueError("Tombstones are not feedback events")
        event = validate_event(json.loads(payload))
    except (ValueError, TypeError, UnicodeError, KeyError) as exc:
        # Do not interpolate arbitrary invalid payload or secret-bearing values.
        reason = f"invalid_event:{type(exc).__name__}"
        with conn.transaction():
            inserted = reject_event(conn, role, reason, message.value(), message.partition(), message.offset(), message.topic())
            heartbeat(conn, role, group, rejected=int(inserted))
        return False, True
    with conn.transaction():
        inserted = (store_raw(conn, event, message.partition(), message.offset())
                    if role == "storage" else store_analysis(conn, event))
        heartbeat(conn, role, group, processed=int(inserted))
    return inserted, False


def _database(settings: Settings, attempts: int = 15) -> psycopg.Connection:
    for attempt in range(attempts):
        conn = None
        try:
            conn = connect(settings.database_url)
            init_db(conn)
            return conn
        except psycopg.OperationalError as exc:
            if conn is not None:
                conn.close()
            if attempt + 1 == attempts:
                raise RuntimeError("PostgreSQL is unavailable; no Kafka offsets were advanced") from exc
            LOG.warning("Waiting for PostgreSQL (%s/%s)", attempt + 1, attempts)
            time.sleep(min(1 + attempt, 5))
    raise AssertionError("unreachable")


def run_consumer(settings: Settings, role: str, *, max_messages: int | None = None,
                 idle_timeout: float | None = None) -> dict[str, int]:
    if role not in {"storage", "analytics"}:
        raise ValueError("role must be storage or analytics")
    if not settings.database_url:
        raise ValueError("DATABASE_URL is required")
    group = settings.storage_group if role == "storage" else settings.analytics_group
    ensure_topic(settings)
    conn = _database(settings)
    consumer = make_consumer(settings, group)
    stop = False

    def request_stop(_signum: int, _frame: Any) -> None:
        nonlocal stop
        stop = True

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, request_stop)
    consumer.subscribe([settings.kafka_topic])
    counts = {"received": 0, "inserted": 0, "duplicates": 0, "rejected": 0}
    last_received = time.monotonic()
    last_heartbeat = 0.0
    LOG.info("Consumer ready role=%s group=%s topic=%s", role, group, settings.kafka_topic)
    try:
        while not stop and (max_messages is None or counts["received"] < max_messages):
            message = consumer.poll(1.0)
            now = time.monotonic()
            if message is None:
                if now - last_heartbeat >= 10:
                    try:
                        with conn.transaction():
                            heartbeat(conn, role, group)
                    except psycopg.OperationalError:
                        conn.close()
                        conn = _database(settings)
                    last_heartbeat = now
                if idle_timeout is not None and now - last_received >= idle_timeout:
                    break
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())
            last_received = now
            for attempt in range(5):
                try:
                    inserted, rejected = process_message(conn, message, role, group)
                    break
                except (psycopg.OperationalError, psycopg.errors.DeadlockDetected,
                        psycopg.errors.SerializationFailure) as exc:
                    conn.close()
                    if attempt == 4:
                        raise RuntimeError("Database write failed; event remains uncommitted in Kafka") from exc
                    LOG.warning("Retrying uncommitted event role=%s attempt=%s error=%s", role, attempt + 1, type(exc).__name__)
                    time.sleep(min(1 + attempt, 5))
                    conn = _database(settings)
            # If this fails after a durable DB commit, restart/re-delivery is safe.
            consumer.commit(message=message, asynchronous=False)
            counts["received"] += 1
            counts["inserted"] += int(inserted)
            counts["rejected"] += int(rejected)
            counts["duplicates"] += int(not inserted and not rejected)
            if counts["received"] % 25 == 0:
                LOG.info("Consumer progress role=%s %s", role, counts)
        return counts
    finally:
        try:
            if not conn.closed:
                with conn.transaction():
                    heartbeat(conn, role, group, status="stopped")
        except psycopg.Error:
            pass
        consumer.close()
        conn.close()
        LOG.info("Consumer stopped role=%s %s", role, counts)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", required=True, choices=("storage", "analytics"))
    parser.add_argument("--max-messages", type=int, default=None, help="Optional bounded integration-test/demo run")
    parser.add_argument("--idle-timeout", type=float, default=None, help="Exit after N seconds with no records; default is continuous")
    args = parser.parse_args()
    configure_logging()
    result = run_consumer(Settings.from_env(), args.role, max_messages=args.max_messages, idle_timeout=args.idle_timeout)
    print(json.dumps({"role": args.role, **result}))


if __name__ == "__main__":
    main()
