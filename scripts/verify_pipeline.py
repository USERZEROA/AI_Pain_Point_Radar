#!/usr/bin/env python3
"""Observe a genuine running pipeline; produce machine-readable rubric evidence."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from radar.db import connect


def snapshot(conn, run_id: str) -> dict:
    raw = conn.execute(
        "SELECT count(*) AS rows, min(emitted_at) AS first_emitted, max(emitted_at) AS last_emitted, "
        "max(received_at) AS last_received FROM raw_events WHERE run_id=%s", (run_id,),
    ).fetchone()
    analysis = conn.execute(
        "SELECT count(*) AS rows, count(*) FILTER (WHERE is_pain) AS pain_rows, "
        "count(DISTINCT thread_id) FILTER (WHERE is_pain) AS pain_threads, "
        "max(classified_at) AS last_classified, "
        "percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM classified_at-emitted_at)) AS latency_p95_seconds "
        "FROM painpoint_results WHERE run_id=%s", (run_id,),
    ).fetchone()
    windows = conn.execute(
        "SELECT count(*) AS rows,max(updated_at) AS last_updated FROM painpoint_windows WHERE run_id=%s", (run_id,),
    ).fetchone()
    signals = conn.execute(
        "SELECT count(*) AS rows,count(*) FILTER (WHERE forecast_ready) AS ready_rows, "
        "count(*) FILTER (WHERE is_anomaly) AS anomalies FROM sequence_signals WHERE run_id=%s", (run_id,),
    ).fetchone()
    return {"observed_at": datetime.now(timezone.utc).isoformat(), "raw": dict(raw),
            "analysis": dict(analysis), "windows": dict(windows), "sequence": dict(signals)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--duration", type=float, default=360)
    parser.add_argument("--interval", type=float, default=30)
    parser.add_argument("--output", type=Path, default=Path("evidence/verification.json"))
    args = parser.parse_args()
    if args.duration < 1 or args.interval < 1:
        parser.error("duration and interval must be positive")
    observations = []
    started = time.monotonic()
    with connect(os.environ.get("DATABASE_URL", ""), autocommit=True) as conn:
        while True:
            item = snapshot(conn, args.run_id)
            item["elapsed_seconds"] = round(time.monotonic() - started, 3)
            observations.append(item)
            print(json.dumps(item, default=str), flush=True)
            remaining = args.duration - (time.monotonic() - started)
            if remaining <= 0:
                break
            time.sleep(min(args.interval, remaining))
        final = observations[-1]
        first, last = final["raw"]["first_emitted"], final["raw"]["last_emitted"]
        span = (last-first).total_seconds() if first and last else 0
        raw_counts = [x["raw"]["rows"] for x in observations]
        derived_updates = {str(x["windows"]["last_updated"]) for x in observations if x["windows"]["last_updated"]}
        checks = {
            "observation_at_least_300_seconds": time.monotonic()-started >= 300,
            "genuine_event_emission_span_at_least_300_seconds": span >= 300,
            "raw_rows_increased": raw_counts[-1] > raw_counts[0],
            "rows_monotonic": all(b >= a for a, b in zip(raw_counts, raw_counts[1:])),
            "persisted_derived_results_updated": len(derived_updates) >= 2,
            "both_consumers_received_records": final["raw"]["rows"] > 0 and final["analysis"]["rows"] > 0,
            "analysis_caught_up": final["raw"]["rows"] == final["analysis"]["rows"],
        }
        report = {"run_id": args.run_id, "event_emission_span_seconds": span,
                  "checks": checks, "observations": observations,
                  "note": "This observes PostgreSQL populated by Kafka consumers; no rows are inserted by this verifier."}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"output": str(args.output), "checks": checks}), flush=True)
    if not all(checks.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
