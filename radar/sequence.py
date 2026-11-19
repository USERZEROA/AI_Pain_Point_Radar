"""Chronological one-step forecasting and anomaly scoring for source-hour counts.

The prediction at t uses only windows strictly before t. Missing *internal*
hours may be zero-filled by the caller; unobserved boundary periods must not be.
Scores describe a keyword-collected stream, not the whole market.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import argparse
import json
import math
from typing import Any, Iterable, Mapping

MODEL_VERSION = "online-ewma-v1"
MIN_HISTORY = 8
DEFAULT_ALPHA = 0.25
ANOMALY_THRESHOLD = 3.0


@dataclass(frozen=True)
class Signal:
    window_start: datetime
    category: str
    actual_count: float
    predicted_count: float
    residual: float
    anomaly_score: float
    is_anomaly: bool
    history_windows: int
    model_version: str = MODEL_VERSION
    forecast_ready: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def floor_hour(value: datetime | str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("Source timestamps must include a timezone")
    return value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def forecast_series(
    points: Iterable[tuple[datetime, float]], category: str,
    *, alpha: float = DEFAULT_ALPHA, min_history: int = MIN_HISTORY,
) -> list[Signal]:
    """EWMA level and exponentially weighted prior-residual variance.

    A Poisson-like variance floor avoids declaring a one-count change a spike.
    Only positive unexpected increases are flagged; at least three observations
    must occur in the tested hour. These are transparent research heuristics,
    not calibrated false-positive probabilities.
    """
    if not 0 < alpha <= 1:
        raise ValueError("alpha must be in (0, 1]")
    if min_history < 2:
        raise ValueError("min_history must be at least 2")
    series = sorted(points, key=lambda point: point[0])
    if len({time for time, _ in series}) != len(series):
        raise ValueError("Each source hour must occur once")
    level = 0.0
    variance = 1.0
    result: list[Signal] = []
    for history, (time, actual) in enumerate(series):
        actual = float(actual)
        if not math.isfinite(actual) or actual < 0:
            raise ValueError("Counts must be finite and nonnegative")
        predicted = level if history else 0.0
        residual = actual - predicted
        ready = history >= min_history
        scale = math.sqrt(max(1.0, variance, predicted))
        score = residual / scale if ready else 0.0
        result.append(Signal(
            window_start=time, category=category, actual_count=actual,
            predicted_count=predicted, residual=residual,
            anomaly_score=score, is_anomaly=ready and score >= ANOMALY_THRESHOLD and actual >= 3,
            history_windows=history, forecast_ready=ready,
        ))
        # Updating comes AFTER constructing the prediction and residual.
        if history == 0:
            level = actual
            variance = max(1.0, actual)
        else:
            level = alpha * actual + (1 - alpha) * level
            variance = alpha * residual * residual + (1 - alpha) * variance
    return result


def build_signals(
    rows: Iterable[Mapping[str, Any]], first_source_at: datetime | str,
    last_source_at: datetime | str, categories: Iterable[str],
) -> list[Signal]:
    """Forecast only complete interior hours; exclude both source boundaries.

    This conservative policy does not infer coverage before/after the observed
    range. Zeros between those boundaries mean zero records in this collection.
    """
    first = floor_hour(first_source_at)
    last = floor_hour(last_source_at)
    if last <= first + timedelta(hours=1):
        return []
    categories = tuple(categories)
    counts = {(floor_hour(row["window_start"]), row["category"]): float(row["record_count"]) for row in rows}
    hours: list[datetime] = []
    cursor = first + timedelta(hours=1)
    while cursor < last:
        hours.append(cursor)
        cursor += timedelta(hours=1)
    output: list[Signal] = []
    for category in categories:
        points = [(hour, counts.get((hour, category), 0.0)) for hour in hours]
        output.extend(forecast_series(points, category))
    return output


def evaluate_signals(rows: Iterable[Mapping[str, Any] | Signal]) -> dict[str, Any]:
    """Walk-forward error on warmed-up windows, with previous-hour baseline.

    The comparison uses the same evaluated windows for both methods. No model
    is fitted on later data, and no improvement is claimed when MAE is higher.
    """
    normalized = [row.to_dict() if isinstance(row, Signal) else dict(row) for row in rows]
    normalized.sort(key=lambda row: (row["category"], row["window_start"]))
    previous: dict[str, float] = {}
    model_errors: list[float] = []
    naive_errors: list[float] = []
    anomaly_count = 0
    for row in normalized:
        category = row["category"]
        actual = float(row["actual_count"])
        ready = bool(row.get("forecast_ready", int(row["history_windows"]) >= MIN_HISTORY))
        if ready and category in previous:
            model_errors.append(abs(actual - float(row["predicted_count"])))
            naive_errors.append(abs(actual - previous[category]))
            anomaly_count += bool(row["is_anomaly"])
        previous[category] = actual
    count = len(model_errors)
    return {
        "model_version": MODEL_VERSION,
        "evaluation": "walk-forward, only prior complete source-hour windows",
        "evaluated_category_hours": count,
        "model_mae": sum(model_errors) / count if count else None,
        "previous_hour_baseline_mae": sum(naive_errors) / count if count else None,
        "anomalous_category_hours": anomaly_count,
        "minimum_history_hours": MIN_HISTORY,
        "scope": "Collected keyword stream only; first/last source hours excluded. Sparse internal hours are zero observations in this collection.",
        "limitation": "Labels are a transparent-rule baseline, not ground-truth demand. No labeled anomaly truth set; anomaly precision/recall is not established.",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate persisted chronological forecasts on a real captured run")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    from radar.config import Settings
    from radar.db import connect
    url = args.database_url or Settings.from_env().database_url
    with connect(url) as conn:
        rows = conn.execute("SELECT * FROM sequence_signals WHERE run_id=%s ORDER BY category, window_start", (args.run_id,)).fetchall()
        bounds = conn.execute("SELECT min(source_created_at) AS first_source_at, max(source_created_at) AS last_source_at, count(*) AS processed_records FROM painpoint_results WHERE run_id=%s", (args.run_id,)).fetchone()
    result = {"run_id": args.run_id, **dict(bounds), **evaluate_signals(rows)}
    rendered = json.dumps(result, indent=2, default=str)
    if args.output:
        from pathlib import Path
        Path(args.output).write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
