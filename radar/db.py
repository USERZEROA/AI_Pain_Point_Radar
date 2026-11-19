"""Transactional PostgreSQL storage and persistent stream analytics.

Write helpers do not commit: callers own the transaction and only then commit
Kafka offsets. Re-delivery is safe through deterministic source/run identities.
"""
from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

import psycopg
from psycopg.rows import dict_row

from radar.classifier import PAIN_CATEGORIES, Classification, classify_event
from radar.sequence import build_signals, floor_hour

EVENT_COLUMNS = (
    "event_id", "schema_version", "source_item_id", "source", "item_type", "thread_id",
    "source_created_at", "emitted_at", "title", "text", "text_length", "source_url",
    "mode", "run_id", "author_hash",
)
RESULT_EVENT_COLUMNS = (
    "event_id", "run_id", "mode", "source_created_at", "emitted_at", "thread_id",
    "source_item_id", "source_url", "title", "text_length", "author_hash",
)
RESULT_CLASS_COLUMNS = ("product", "category", "is_pain", "evidence", "method", "rule_version")


def connect(database_url: str, **kwargs: Any) -> psycopg.Connection:
    if not database_url:
        raise ValueError("DATABASE_URL is required")
    return psycopg.connect(database_url, row_factory=dict_row, connect_timeout=10, **kwargs)


def init_db(conn: psycopg.Connection) -> None:
    """Serialize idempotent initialization when all containers start together."""
    schema = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(718294301)")
        conn.execute(schema, prepare=False)


def _event_dict(event: Any) -> dict[str, Any]:
    result = dict(event) if isinstance(event, Mapping) else event.to_dict()
    result.setdefault("author_hash", "")
    return result


def store_raw(conn: psycopg.Connection, event: Any, partition: int = 0, offset: int = 0) -> bool:
    record = _event_dict(event)
    values = [record[name] for name in EVENT_COLUMNS] + [partition, offset]
    columns = (*EVENT_COLUMNS, "kafka_partition", "kafka_offset")
    query = f"INSERT INTO raw_events ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) ON CONFLICT DO NOTHING RETURNING event_id"
    return conn.execute(query, values).fetchone() is not None


def _refresh_hour(conn: psycopg.Connection, run_id: str, mode: str, hour: datetime) -> None:
    end = hour + timedelta(hours=1)
    total = conn.execute(
        "SELECT count(*) AS n FROM painpoint_results WHERE run_id=%s AND source_created_at >= %s AND source_created_at < %s",
        (run_id, hour, end),
    ).fetchone()["n"]
    counts = conn.execute(
        "SELECT category, count(*) AS n, count(DISTINCT thread_id) AS threads "
        "FROM painpoint_results WHERE run_id=%s AND source_created_at >= %s AND source_created_at < %s AND is_pain "
        "GROUP BY category", (run_id, hour, end),
    ).fetchall()
    by_category = {row["category"]: row for row in counts}
    values = []
    for category in PAIN_CATEGORIES:
        count = int(by_category.get(category, {}).get("n", 0))
        threads = int(by_category.get(category, {}).get("threads", 0))
        values.append((run_id, mode, hour, category, count, threads, total, count / total if total else 0.0))
    with conn.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO painpoint_windows (run_id,mode,window_start,category,record_count,thread_count,total_ai_records,pain_share) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT(run_id,window_start,category) DO UPDATE SET "
            "record_count=EXCLUDED.record_count, thread_count=EXCLUDED.thread_count, "
            "total_ai_records=EXCLUDED.total_ai_records, pain_share=EXCLUDED.pain_share, updated_at=now()",
            values,
        )


def refresh_sequence(conn: psycopg.Connection, run_id: str) -> int:
    """Deterministic recomputation permits out-of-order/late data without leakage.

    Only changed forecasts are updated. Ordinary records in the open latest
    source-hour don't trigger this; a new hour or late event does.
    """
    bounds = conn.execute(
        "SELECT min(source_created_at) AS first_at,max(source_created_at) AS last_at "
        "FROM painpoint_results WHERE run_id=%s", (run_id,),
    ).fetchone()
    if not bounds["first_at"]:
        return 0
    rows = conn.execute(
        "SELECT window_start,category,record_count FROM painpoint_windows WHERE run_id=%s ORDER BY window_start,category",
        (run_id,),
    ).fetchall()
    signals = build_signals(rows, bounds["first_at"], bounds["last_at"], PAIN_CATEGORIES)
    # Remove boundaries defensively if rebuilding an existing run after edits.
    conn.execute("DELETE FROM sequence_signals WHERE run_id=%s AND (window_start <= %s OR window_start >= %s)",
                 (run_id, floor_hour(bounds["first_at"]), floor_hour(bounds["last_at"])))
    values = [(run_id, s.window_start, s.category, s.actual_count, s.predicted_count, s.residual,
               s.anomaly_score, s.is_anomaly, s.history_windows, s.model_version, s.forecast_ready) for s in signals]
    if values:
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO sequence_signals (run_id,window_start,category,actual_count,predicted_count,residual,anomaly_score,is_anomaly,history_windows,model_version,forecast_ready) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT(run_id,window_start,category) DO UPDATE SET "
                "actual_count=EXCLUDED.actual_count,predicted_count=EXCLUDED.predicted_count,residual=EXCLUDED.residual,"
                "anomaly_score=EXCLUDED.anomaly_score,is_anomaly=EXCLUDED.is_anomaly,history_windows=EXCLUDED.history_windows,"
                "model_version=EXCLUDED.model_version,forecast_ready=EXCLUDED.forecast_ready,updated_at=now() "
                "WHERE (sequence_signals.actual_count,sequence_signals.predicted_count,sequence_signals.residual,"
                "sequence_signals.anomaly_score,sequence_signals.is_anomaly,sequence_signals.history_windows,"
                "sequence_signals.model_version,sequence_signals.forecast_ready) IS DISTINCT FROM "
                "(EXCLUDED.actual_count,EXCLUDED.predicted_count,EXCLUDED.residual,EXCLUDED.anomaly_score,EXCLUDED.is_anomaly,"
                "EXCLUDED.history_windows,EXCLUDED.model_version,EXCLUDED.forecast_ready)", values,
            )
    return len(signals)


def store_analysis(conn: psycopg.Connection, event: Any, classification: Classification | None = None) -> bool:
    record = _event_dict(event)
    result = (classification or classify_event(record)).to_dict()
    run_id = record["run_id"]
    # One logical aggregation writer per run even if the group is scaled out.
    conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (run_id,))
    old_max = conn.execute("SELECT max(source_created_at) AS last_at FROM painpoint_results WHERE run_id=%s", (run_id,)).fetchone()["last_at"]
    columns = (*RESULT_EVENT_COLUMNS, *RESULT_CLASS_COLUMNS)
    values = [record[name] for name in RESULT_EVENT_COLUMNS] + [result[name] for name in RESULT_CLASS_COLUMNS]
    inserted = conn.execute(
        f"INSERT INTO painpoint_results ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) ON CONFLICT DO NOTHING RETURNING event_id", values,
    ).fetchone()
    if not inserted:
        return False
    hour = floor_hour(record["source_created_at"])
    _refresh_hour(conn, run_id, record["mode"], hour)
    if old_max is not None and hour != floor_hour(old_max):
        refresh_sequence(conn, run_id)
    return True


def redacted_payload(payload: bytes | str | None) -> str:
    """Keep poison-event diagnostics useful without persisting arbitrary secrets."""
    raw = payload if isinstance(payload, bytes) else str(payload or "").encode("utf-8", errors="replace")
    summary: dict[str, Any] = {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            for field in ("schema_version", "event_id", "source", "source_item_id", "item_type", "mode", "run_id", "thread_id"):
                if field in parsed:
                    # IDs should be short; ignore unexpected large/structured values.
                    value = parsed[field]
                    if isinstance(value, (str, int)):
                        summary[field] = str(value)[:80]
    except (ValueError, UnicodeError, TypeError):
        pass
    return json.dumps(summary, ensure_ascii=True)


def reject_event(conn: psycopg.Connection, role: str, reason: str, payload: bytes | str | None,
                 partition: int, offset: int, topic: str = "ai_user_feedback") -> bool:
    return conn.execute(
        "INSERT INTO rejected_events(consumer_role,error_reason,payload,kafka_topic,kafka_partition,kafka_offset) "
        "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING id",
        (role, reason[:240], redacted_payload(payload), topic, partition, offset),
    ).fetchone() is not None


def heartbeat(conn: psycopg.Connection, role: str, group_id: str, *, status: str = "running",
              processed: int = 0, rejected: int = 0) -> None:
    conn.execute(
        "INSERT INTO consumer_status(consumer_role,group_id,status,processed_events,rejected_count,last_event_at) "
        "VALUES (%s,%s,%s,%s,%s,CASE WHEN %s > 0 THEN now() ELSE NULL END) "
        "ON CONFLICT(consumer_role) DO UPDATE SET group_id=EXCLUDED.group_id,status=EXCLUDED.status,last_heartbeat=now(),"
        "processed_events=consumer_status.processed_events+EXCLUDED.processed_events,"
        "rejected_count=consumer_status.rejected_count+EXCLUDED.rejected_count,"
        "last_event_at=CASE WHEN %s > 0 THEN now() ELSE consumer_status.last_event_at END",
        (role, group_id, status, processed, rejected, processed, processed),
    )
