"""Versioned, validated wire events. Source timestamps are never rewritten."""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
import hashlib
import re
from typing import Any, Mapping


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def as_utc(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("timestamp must be a timezone-aware ISO8601 string")
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError) as exc:
        raise ValueError("timestamp must be valid ISO8601") from exc
    if dt.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def event_identity(source: str, source_item_id: str, run_id: str) -> str:
    return hashlib.sha256(f"{source}:{source_item_id}:{run_id}".encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Event:
    schema_version: int
    event_id: str
    source_item_id: str
    source: str
    item_type: str
    thread_id: str
    source_created_at: str
    emitted_at: str
    title: str
    text: str
    text_length: int
    source_url: str
    mode: str
    run_id: str
    author_hash: str = ""

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Event":
        if not isinstance(data, Mapping):
            raise ValueError("event must be a JSON object")
        expected = {field.name for field in fields(cls)}
        missing = (expected - {"author_hash"}) - set(data)
        if missing:
            raise ValueError("missing event fields: " + ", ".join(sorted(missing)))
        if set(data) - expected:
            raise ValueError("unsupported event fields: " + ", ".join(sorted(set(data) - expected)))
        values = {name: data.get(name, "") for name in expected}
        if type(values["schema_version"]) is not int or values["schema_version"] != 1:
            raise ValueError("unsupported schema_version")
        string_fields = expected - {"schema_version", "text_length"}
        if any(not isinstance(values[name], str) for name in string_fields):
            raise ValueError("all event identifiers, labels and content must be strings")
        if values["source"] != "hacker_news":
            raise ValueError("unsupported source")
        if values["item_type"] not in {"story", "comment"}:
            raise ValueError("item_type must be story or comment")
        if values["mode"] not in {"live", "replay"}:
            raise ValueError("mode must be live or replay")
        for name in ("source_item_id", "thread_id"):
            if not re.fullmatch(r"[0-9]{1,20}", values[name]):
                raise ValueError(f"{name} must be a valid HN item id")
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", values["run_id"]):
            raise ValueError("run_id must contain 1-128 safe identifier characters")
        if not values["text"].strip() or len(values["text"]) > 100_000:
            raise ValueError("text must contain 1-100000 characters")
        if len(values["title"]) > 4096:
            raise ValueError("title exceeds 4096 characters")
        if type(values["text_length"]) is not int or values["text_length"] != len(values["text"]):
            raise ValueError("text_length must exactly match the stored text")
        canonical_url = "https://news.ycombinator.com/item?id=" + values["source_item_id"]
        if values["source_url"] != canonical_url:
            raise ValueError("source_url must be the canonical HN item URL")
        if values["author_hash"] and not re.fullmatch(r"[0-9a-f]{64}", values["author_hash"]):
            raise ValueError("author_hash must be empty or a SHA256 digest")
        expected_id = event_identity(values["source"], values["source_item_id"], values["run_id"])
        if values["event_id"] != expected_id:
            raise ValueError("event_id does not match the source identity and run")
        values["source_created_at"] = as_utc(values["source_created_at"])
        values["emitted_at"] = as_utc(values["emitted_at"])
        return cls(**values)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_event(data: Mapping[str, Any] | Event) -> Event:
    return Event.from_dict(data.to_dict() if isinstance(data, Event) else data)


def create_event(record: Mapping[str, Any], run_id: str, mode: str, emitted_at: str | None = None) -> Event:
    source = str(record.get("source", "hacker_news"))
    item_id = str(record["source_item_id"])
    text = record["text"]
    data = {
        "schema_version": 1,
        "event_id": event_identity(source, item_id, run_id),
        "source_item_id": item_id,
        "source": source,
        "item_type": record["item_type"],
        "thread_id": str(record["thread_id"]),
        "source_created_at": record["source_created_at"],
        "emitted_at": emitted_at or utc_now(),
        "title": record.get("title", ""),
        "text": text,
        "text_length": len(text),
        "source_url": "https://news.ycombinator.com/item?id=" + item_id,
        "mode": mode,
        "run_id": run_id,
        "author_hash": record.get("author_hash", ""),
    }
    return validate_event(data)
