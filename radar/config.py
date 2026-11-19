"""Environment-only configuration; credentials never enter the repository."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    database_url: str = ""
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic: str = "ai_user_feedback"
    mode: str = "replay"
    run_id: str = ""
    poll_seconds: float = 30.0
    replay_interval: float = 1.0
    replay_file: Path = Path("data/replay.jsonl")
    replay_loop: bool = True
    seed_limit: int = 600
    lookback_days: int = 30
    state_dir: Path = Path("data/state")
    storage_group: str = "ai-feedback-storage-v1"
    analytics_group: str = "ai-feedback-analysis-v1"

    @classmethod
    def from_env(cls) -> "Settings":
        settings = cls(
            database_url=os.getenv("DATABASE_URL", ""),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            kafka_topic=os.getenv("KAFKA_TOPIC", "ai_user_feedback"),
            mode=os.getenv("STREAM_MODE", "replay").lower(),
            run_id=os.getenv("RUN_ID", ""),
            poll_seconds=float(os.getenv("POLL_SECONDS", "30")),
            replay_interval=float(os.getenv("REPLAY_INTERVAL", "1")),
            replay_file=Path(os.getenv("REPLAY_FILE", "data/replay.jsonl")),
            replay_loop=_bool("REPLAY_LOOP", True),
            seed_limit=int(os.getenv("SEED_LIMIT", "600")),
            lookback_days=int(os.getenv("LOOKBACK_DAYS", "30")),
            state_dir=Path(os.getenv("STATE_DIR", "data/state")),
            storage_group=os.getenv("STORAGE_GROUP", "ai-feedback-storage-v1"),
            analytics_group=os.getenv("ANALYTICS_GROUP", "ai-feedback-analysis-v1"),
        )
        if settings.mode not in {"live", "replay"}:
            raise ValueError("STREAM_MODE must be live or replay")
        if settings.replay_interval < 0 or settings.poll_seconds < 1:
            raise ValueError("REPLAY_INTERVAL must be >= 0 and POLL_SECONDS >= 1")
        if settings.seed_limit < 1 or settings.lookback_days < 1:
            raise ValueError("SEED_LIMIT and LOOKBACK_DAYS must be positive")
        return settings


def configure_logging() -> None:
    import logging
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
