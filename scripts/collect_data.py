#!/usr/bin/env python3
"""Download real replay data. Full source text stays outside the submitted archive."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from radar.source import DEFAULT_QUERIES, collect_seed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="data/replay.jsonl")
    parser.add_argument("--limit", type=int, default=600, help="Minimum records; complete source window is not truncated")
    parser.add_argument("--days", type=int, default=30, help="Maximum lookback; starts at 3 days and expands if necessary")
    parser.add_argument("--max-requests", type=int, default=250)
    parser.add_argument("--queries", nargs="+", default=list(DEFAULT_QUERIES))
    parser.add_argument("--end-timestamp", type=int, help="Optional exclusive UTC epoch boundary for reproducibility")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        manifest = collect_seed(args.output, args.limit, args.days, args.queries, args.max_requests, end_timestamp=args.end_timestamp)
    except Exception as exc:
        logging.error("Collection failed: %s", exc)
        return 1
    print(json.dumps({key: manifest[key] for key in ("record_count", "sha256", "window_start", "window_end_exclusive", "pagination_complete", "http_requests")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
