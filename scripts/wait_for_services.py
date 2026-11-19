#!/usr/bin/env python3
"""Initialize the actual database and Kafka topic before starting services."""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse
import time
from radar.config import Settings, configure_logging
from radar.db import connect, init_db
from radar.kafka_client import ensure_topic


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--initialize", action="store_true")
    args = parser.parse_args()
    configure_logging()
    settings = Settings.from_env()
    for attempt in range(30):
        try:
            with connect(settings.database_url) as conn:
                conn.execute("SELECT 1")
                if args.initialize:
                    init_db(conn)
            break
        except Exception:
            if attempt == 29:
                raise RuntimeError("PostgreSQL did not become ready; check the database service logs")
            time.sleep(2)
    ensure_topic(settings)
    print("PostgreSQL schema and Kafka topic are ready.", flush=True)


if __name__ == "__main__":
    main()
