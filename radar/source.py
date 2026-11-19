"""Real Hacker News collection with exact retrieval and complete time windows.

The dataset is a bounded keyword-visible community sample, not all AI users.
No generated comments or invented engagement measurements are used.
"""

from __future__ import annotations

from datetime import datetime, timezone
from html.parser import HTMLParser
import hashlib
import json
import logging
import math
from pathlib import Path
import re
import time
from typing import Any, Callable, Iterable

from radar.events import as_utc, create_event, utc_now

LOGGER = logging.getLogger(__name__)
ENDPOINT = "https://hn.algolia.com/api/v1/search_by_date"
DEFAULT_QUERIES = ("LLM", "ChatGPT", "Claude", "Copilot", "AI agent", "artificial intelligence", "Gemini", "Cursor")
STRONG_AI = re.compile(
    r"\b(?:AI|A\.I\.|LLMs?|ChatGPT|GPT(?:[- ]?[0-9][\w.-]*)?|OpenAI|Anthropic|"
    r"Claude|DeepSeek|artificial intelligence|generative (?:AI|models?)|large language models?)\b",
    re.IGNORECASE,
)
WEAK_PRODUCT = re.compile(r"\b(?:Copilot|Cursor|Gemini)\b", re.IGNORECASE)
PRODUCT_CONTEXT = re.compile(
    r"\b(?:coding|code|IDE|model|prompt|token|assistant|chatbot|agent|inference|subscription|API|autocomplete)\b",
    re.IGNORECASE,
)


class SourceError(RuntimeError):
    """Source failure; callers must not advance a watermark."""


class CollectionIncomplete(SourceError):
    """A request or indexing bound prevented complete retrieval of the window."""


class _TextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.suppressed = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"script", "style"}:
            self.suppressed += 1
        if not self.suppressed and tag in {"p", "br", "div", "li", "pre"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style"}:
            self.suppressed = max(0, self.suppressed - 1)
        elif not self.suppressed and tag in {"p", "div", "li", "pre"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self.suppressed:
            self.parts.append(data)


def clean_html(value: str | None) -> str:
    parser = _TextParser()
    parser.feed(value or "")
    parser.close()
    text = "".join(parser.parts).replace("\x00", "").replace("\r", "")
    text = re.sub(r"[\t \u00a0]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def is_ai_relevant(text: str, title: str = "") -> bool:
    combined = f"{title}\n{text}"
    return bool(STRONG_AI.search(combined) or (WEAK_PRODUCT.search(combined) and PRODUCT_CONTEXT.search(combined)))


def record_from_hit(hit: dict[str, Any]) -> dict[str, Any] | None:
    tags = hit.get("_tags") or []
    if "comment" in tags or hit.get("comment_text") is not None:
        item_type = "comment"
        title = clean_html(hit.get("story_title"))
        text = clean_html(hit.get("comment_text"))
        thread_id = hit.get("story_id")
        if thread_id is None:
            roots = [tag.split("_", 1)[1] for tag in tags if isinstance(tag, str) and tag.startswith("story_")]
            thread_id = roots[0] if roots else None
    elif "story" in tags or hit.get("title"):
        item_type = "story"
        title = clean_html(hit.get("title"))
        text = clean_html(hit.get("story_text")) or title
        thread_id = hit.get("objectID")
    else:
        return None
    if not text or thread_id is None or not is_ai_relevant(text, title):
        return None
    item_id = str(hit["objectID"])
    created = hit.get("created_at")
    if hit.get("created_at_i") is not None:
        created = datetime.fromtimestamp(int(hit["created_at_i"]), timezone.utc).isoformat()
    author = hit.get("author") or ""
    record = {
        "source_item_id": item_id,
        "source": "hacker_news",
        "item_type": item_type,
        "thread_id": str(thread_id),
        "source_created_at": as_utc(created),
        "title": title,
        "text": text,
        "text_length": len(text),
        "source_url": "https://news.ycombinator.com/item?id=" + item_id,
        "author_hash": hashlib.sha256(("hacker_news:" + author).encode()).hexdigest() if author else "",
    }
    create_event(record, "validation", "replay")
    return record


class AlgoliaClient:
    """Bounded retries, pagination and time bisection around Algolia's page cap."""

    def __init__(self, client: Any = None, max_requests: int = 250, retry_attempts: int = 4,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        if client is None:
            import httpx
            client = httpx.Client(timeout=httpx.Timeout(30.0, connect=10.0), follow_redirects=True,
                                  headers={"User-Agent": "AI-Pain-Point-Radar/1.0 (academic streaming project)"})
        self.client = client
        self.max_requests = max_requests
        self.retry_attempts = retry_attempts
        self.sleep = sleep
        self.request_count = 0
        self.completed_ranges: list[dict[str, Any]] = []

    def close(self) -> None:
        self.client.close()

    def _get(self, params: dict[str, Any]) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(self.retry_attempts):
            if self.request_count >= self.max_requests:
                raise CollectionIncomplete(f"request budget {self.max_requests} exhausted; no complete-window claim")
            self.request_count += 1
            try:
                response = self.client.get(ENDPOINT, params=params)
                if response.status_code == 429 or response.status_code >= 500:
                    retry_after = response.headers.get("Retry-After", "")
                    delay = min(float(retry_after), 30.0) if retry_after.replace(".", "", 1).isdigit() else min(2 ** attempt, 8)
                    if attempt + 1 < self.retry_attempts:
                        self.sleep(max(0.1, delay))
                        continue
                response.raise_for_status()
                body = response.json()
                if not isinstance(body, dict) or not isinstance(body.get("hits"), list):
                    raise SourceError("source returned an invalid search result")
                return body
            except CollectionIncomplete:
                raise
            except Exception as exc:
                last_error = exc
                # Configuration and invalid query errors should not be retried.
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status is not None and 400 <= status < 500 and status != 429:
                    raise SourceError(f"HN search rejected request with HTTP {status}") from exc
                if attempt + 1 < self.retry_attempts:
                    self.sleep(min(2 ** attempt, 8))
        raise SourceError(f"HN search failed after {self.retry_attempts} attempts: {type(last_error).__name__}") from last_error

    def fetch_window(self, query: str, start: int, end: int, *, _depth: int = 0) -> list[dict[str, Any]]:
        if end <= start:
            return []
        params = {
            "query": query, "tags": "(story,comment)", "hitsPerPage": 100, "page": 0,
            "numericFilters": f"created_at_i>={start},created_at_i<{end}",
            "typoTolerance": "false", "queryType": "prefixNone", "advancedSyntax": "true",
        }
        body = self._get(params)
        total = int(body.get("nbHits", len(body["hits"])))
        pages = int(body.get("nbPages", math.ceil(total / 100)))
        if total > 1000 or pages > 10:
            if end - start <= 1 or _depth >= 24:
                raise CollectionIncomplete("source pagination cap cannot be resolved at one-second resolution")
            midpoint = start + (end - start) // 2
            return self.fetch_window(query, start, midpoint, _depth=_depth + 1) + self.fetch_window(query, midpoint, end, _depth=_depth + 1)
        hits = list(body["hits"])
        for page in range(1, pages):
            next_body = self._get({**params, "page": page})
            # A moving index can expand the window between pages; resplit rather than silently truncate.
            if int(next_body.get("nbHits", total)) > 1000:
                if end - start <= 1:
                    raise CollectionIncomplete("source index grew beyond the pagination cap")
                midpoint = start + (end - start) // 2
                return self.fetch_window(query, start, midpoint, _depth=_depth + 1) + self.fetch_window(query, midpoint, end, _depth=_depth + 1)
            hits.extend(next_body["hits"])
        self.completed_ranges.append({"query": query, "start": start, "end": end, "pages": max(pages, 1), "returned_hits": len(hits), "reported_hits": total})
        return hits


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def collect_seed(output: str | Path, min_records: int = 600, lookback_days: int = 30,
                 queries: Iterable[str] = DEFAULT_QUERIES, max_requests: int = 250,
                 client: AlgoliaClient | None = None, end_timestamp: int | None = None) -> dict[str, Any]:
    """Collect every visible query result in a continuous bounded window.

    Start with three days; expand backwards only if the minimum is not reached.
    `min_records` is a minimum, never a truncation or per-day sampling quota.
    A failure leaves only an explicitly incomplete manifest, never a replacement corpus.
    """
    if min_records < 1 or lookback_days < 1:
        raise ValueError("min_records and lookback_days must be positive")
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    queries = tuple(dict.fromkeys(query.strip() for query in queries if query.strip()))
    if not queries:
        raise ValueError("at least one query is required")
    owned = client is None
    client = client or AlgoliaClient(max_requests=max_requests)
    started = utc_now()
    # Exclude the latest 120 seconds to reduce churn while paging through a live index.
    end = int(end_timestamp if end_timestamp is not None else time.time() - 120)
    days = min(3, lookback_days)
    start = end - days * 86400
    records: dict[str, dict[str, Any]] = {}
    invalid_hits = 0
    excluded_hits = 0
    raw_hits = 0
    manifest: dict[str, Any] = {
        "format_version": 1, "source": "Hacker News via Algolia public search", "api_endpoint": ENDPOINT,
        "collection_started_at": started, "queries": list(queries), "typo_tolerance": False,
        "prefix_matching": False, "minimum_requested_records": min_records,
        "scope": "Keyword-visible public HN posts/comments; not a representative sample of all AI users.",
        "selection": "Complete query pagination in a continuous time window, then explicit AI relevance filtering and ID deduplication. No daily quotas or count truncation.",
        "completeness_scope": "All returned pages of the configured queries at collection time; search indexing, subsequent edits/deletions, and keyword coverage can omit source content.",
        "partial_boundary_hours": "The start and end UTC hours may be partial; do not treat them as full exposure windows.",
        "pagination_complete": False,
    }
    fetch_end = end
    try:
        while True:
            for query in queries:
                LOGGER.info("Collecting query=%r UTC [%s, %s)", query, datetime.fromtimestamp(start, timezone.utc).isoformat(), datetime.fromtimestamp(fetch_end, timezone.utc).isoformat())
                hits = client.fetch_window(query, start, fetch_end)
                raw_hits += len(hits)
                for hit in hits:
                    try:
                        record = record_from_hit(hit)
                    except (ValueError, TypeError, KeyError, OverflowError):
                        invalid_hits += 1
                        continue
                    if record is None:
                        excluded_hits += 1
                        continue
                    records[record["source_item_id"]] = record
            if len(records) >= min_records:
                break
            if days >= lookback_days:
                raise CollectionIncomplete(f"only {len(records)} eligible distinct records in {days} days; minimum is {min_records}")
            fetch_end = start
            days = min(days * 2, lookback_days)
            start = end - days * 86400
        ordered = sorted(records.values(), key=lambda row: (row["source_created_at"], int(row["source_item_id"])))
        payload = "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in ordered).encode("utf-8")
        temporary = output.with_suffix(output.suffix + ".tmp")
        temporary.write_bytes(payload)
        temporary.replace(output)
        manifest.update({
            "pagination_complete": True, "record_count": len(ordered), "sha256": hashlib.sha256(payload).hexdigest(),
            "file_name": output.name, "source_record_first_at": ordered[0]["source_created_at"],
            "source_record_last_at": ordered[-1]["source_created_at"],
        })
    except Exception as exc:
        manifest.update({"record_count": len(records), "error": str(exc), "error_type": type(exc).__name__})
        raise
    finally:
        manifest.update({
            "collection_finished_at": utc_now(), "window_start": datetime.fromtimestamp(start, timezone.utc).isoformat(),
            "window_end_exclusive": datetime.fromtimestamp(end, timezone.utc).isoformat(),
            "query_pages": client.completed_ranges, "http_requests": client.request_count,
            "retrieved_hits_before_deduplication": raw_hits, "invalid_hits_excluded": invalid_hits,
            "non_ai_or_empty_hits_excluded": excluded_hits,
        })
        manifest_path = output.with_suffix(".manifest.json") if manifest["pagination_complete"] else output.with_suffix(".incomplete.manifest.json")
        _write_json(manifest_path, manifest)
        if owned:
            client.close()
    return manifest


def load_replay(path: str | Path, require_manifest: bool = True) -> list[dict[str, Any]]:
    path = Path(path)
    payload = path.read_bytes()
    if require_manifest:
        manifest_path = path.with_suffix(".manifest.json")
        if not manifest_path.is_file():
            raise SourceError("replay provenance manifest is missing; run scripts/collect_data.py")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("pagination_complete") is not True:
            raise SourceError("refusing an incomplete replay corpus")
        if manifest.get("sha256") != hashlib.sha256(payload).hexdigest():
            raise SourceError("replay SHA256 does not match its provenance manifest")
    records: dict[str, dict[str, Any]] = {}
    for line_number, line in enumerate(payload.decode("utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
            event = create_event(record, "validation", "replay")
        except (TypeError, ValueError, KeyError) as exc:
            raise SourceError(f"invalid replay record at line {line_number}: {exc}") from exc
        if event.source_item_id in records:
            raise SourceError(f"duplicate source record in replay corpus at line {line_number}")
        records[event.source_item_id] = record
    if require_manifest and manifest.get("record_count") != len(records):
        raise SourceError("replay record count does not match provenance manifest")
    return sorted(records.values(), key=lambda row: (row["source_created_at"], int(row["source_item_id"])))
