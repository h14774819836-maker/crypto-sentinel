from __future__ import annotations

import asyncio
import hashlib
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from app.logging import logger


_RSS_DATE_FORMATS = (
    "%a, %d %b %Y %H:%M:%S %z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%SZ",
)

DEFAULT_INTEL_FEEDS: dict[str, list[dict[str, str]]] = {
    "macro": [
        {"name": "Reuters World", "url": "https://feeds.reuters.com/reuters/worldNews"},
        {"name": "BBC World", "url": "https://feeds.bbci.co.uk/news/world/rss.xml"},
        {"name": "NPR News", "url": "https://feeds.npr.org/1001/rss.xml"},
    ],
    "policy": [
        {"name": "Federal Reserve", "url": "https://www.federalreserve.gov/feeds/press_all.xml"},
        {"name": "US Treasury", "url": "https://home.treasury.gov/news/press-releases/rss"},
        {"name": "SEC News", "url": "https://www.sec.gov/news/pressreleases.rss"},
    ],
    "intel": [
        {"name": "CSIS", "url": "https://www.csis.org/analysis/feed"},
        {"name": "Brookings", "url": "https://www.brookings.edu/feed/"},
        {"name": "CFR", "url": "https://www.cfr.org/rss.xml"},
    ],
    "tech": [
        {"name": "MIT Tech Review", "url": "https://www.technologyreview.com/feed/"},
        {"name": "The Verge", "url": "https://www.theverge.com/rss/index.xml"},
    ],
}

ALERT_KEYWORDS = (
    "war", "invasion", "missile", "sanction", "default", "bank run", "liquidity crunch",
    "martial law", "cyber attack", "emergency", "exchange halt", "blackout", "tariff",
)

REGION_KEYWORDS: dict[str, tuple[str, ...]] = {
    "APAC": ("china", "taiwan", "japan", "korea", "indo-pacific", "south china sea"),
    "MENA": ("iran", "israel", "gaza", "saudi", "syria", "iraq", "middle east"),
    "EUROPE": ("eu", "eurozone", "ukraine", "russia", "germany", "france"),
    "AMERICAS": ("united states", "us ", "america", "canada", "brazil", "mexico"),
}

TOPIC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "fed-rates": ("federal reserve", "fomc", "rate hike", "rate cut", "powell"),
    "inflation": ("inflation", "cpi", "ppi", "price pressure"),
    "tariffs": ("tariff", "trade war", "import duty", "customs duty"),
    "crypto": ("bitcoin", "crypto", "ethereum", "stablecoin", "sec crypto"),
    "ai-regulation": ("ai safety", "ai regulation", "model governance", "artificial intelligence law"),
    "geopolitics": ("missile", "troops", "ceasefire", "hostage", "border conflict"),
}

NARRATIVE_PATTERNS: list[dict[str, Any]] = [
    {"id": "fed-pivot", "keywords": ("fed pause", "rate cut", "dovish"), "severity": "emerging"},
    {"id": "dedollarization", "keywords": ("dedollarization", "reserve currency shift", "brics currency"), "severity": "spreading"},
    {"id": "ai-bubble", "keywords": ("ai bubble", "overvalued ai", "ai euphoria"), "severity": "watch"},
    {"id": "energy-shock", "keywords": ("oil spike", "energy shock", "gas shortage"), "severity": "spreading"},
]

PERSON_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bjerome\s+powell\b|\bpowell\b", re.I), "Jerome Powell"),
    (re.compile(r"\bdonald\s+trump\b|\btrump\b", re.I), "Donald Trump"),
    (re.compile(r"\bxi\s*jinping\b|\bxi\b", re.I), "Xi Jinping"),
    (re.compile(r"\belon\s+musk\b|\bmusk\b", re.I), "Elon Musk"),
    (re.compile(r"\bputin\b", re.I), "Vladimir Putin"),
]


@dataclass(slots=True)
class _CacheEntry:
    data: str
    fresh_until: datetime
    stale_until: datetime


@dataclass(slots=True)
class _CircuitState:
    state: str = "CLOSED"
    failure_count: int = 0
    open_until: datetime | None = None


class ResilientHttpClient:
    """Small anti-fragile fetch layer for RSS/news polling jobs."""

    def __init__(
        self,
        http_client: httpx.AsyncClient,
        *,
        ttl_seconds: int = 180,
        stale_seconds: int = 600,
        failure_threshold: int = 3,
        open_seconds: int = 60,
    ) -> None:
        self.http_client = http_client
        self.ttl_seconds = max(30, ttl_seconds)
        self.stale_seconds = max(self.ttl_seconds, stale_seconds)
        self.failure_threshold = max(1, failure_threshold)
        self.open_seconds = max(5, open_seconds)
        self.cache: dict[str, _CacheEntry] = {}
        self.inflight: dict[str, asyncio.Task[str]] = {}
        self.circuits: dict[str, _CircuitState] = {}
        self._lock = asyncio.Lock()

    async def get_text(self, key: str, url: str, *, timeout_seconds: int = 12) -> str:
        now = datetime.now(timezone.utc)
        entry = self.cache.get(key)
        if entry and entry.fresh_until > now:
            return entry.data
        if entry and entry.stale_until > now:
            asyncio.create_task(self._refresh(key, url, timeout_seconds))
            return entry.data
        return await self._refresh(key, url, timeout_seconds)

    async def _refresh(self, key: str, url: str, timeout_seconds: int) -> str:
        async with self._lock:
            task = self.inflight.get(key)
            if task is None:
                task = asyncio.create_task(self._fetch_with_circuit(key, url, timeout_seconds))
                self.inflight[key] = task
        try:
            return await task
        finally:
            async with self._lock:
                if self.inflight.get(key) is task:
                    self.inflight.pop(key, None)

    async def _fetch_with_circuit(self, key: str, url: str, timeout_seconds: int) -> str:
        now = datetime.now(timezone.utc)
        circuit = self.circuits.setdefault(key, _CircuitState())
        if circuit.state == "OPEN" and circuit.open_until and now < circuit.open_until:
            stale = self.cache.get(key)
            if stale:
                return stale.data
            raise RuntimeError(f"circuit_open:{key}")
        if circuit.state == "OPEN":
            circuit.state = "HALF_OPEN"

        try:
            resp = await self.http_client.get(url, timeout=timeout_seconds)
            resp.raise_for_status()
            data = resp.text
        except Exception:
            circuit.failure_count += 1
            if circuit.failure_count >= self.failure_threshold:
                circuit.state = "OPEN"
                circuit.open_until = datetime.now(timezone.utc) + timedelta(seconds=self.open_seconds)
            stale = self.cache.get(key)
            if stale:
                return stale.data
            raise

        circuit.failure_count = 0
        circuit.state = "CLOSED"
        circuit.open_until = None

        ts = datetime.now(timezone.utc)
        self.cache[key] = _CacheEntry(
            data=data,
            fresh_until=ts + timedelta(seconds=self.ttl_seconds),
            stale_until=ts + timedelta(seconds=self.stale_seconds),
        )
        return data


class IntelService:
    def __init__(
        self,
        http_client: httpx.AsyncClient,
        *,
        feeds: dict[str, list[dict[str, str]]] | None = None,
        max_items_per_feed: int = 30,
    ) -> None:
        self.feeds = feeds or DEFAULT_INTEL_FEEDS
        self.max_items_per_feed = max(5, max_items_per_feed)
        self.client = ResilientHttpClient(http_client)

    async def fetch_news_items(self, *, max_items_per_run: int = 300) -> list[dict[str, Any]]:
        sem = asyncio.Semaphore(5)
        tasks: list[asyncio.Task[list[dict[str, Any]]]] = []
        for category, feed_list in self.feeds.items():
            for feed in feed_list:
                tasks.append(asyncio.create_task(self._fetch_feed(sem, category, feed)))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        items: list[dict[str, Any]] = []
        seen_url_hash: set[str] = set()
        for result in results:
            if isinstance(result, Exception):
                logger.warning("intel feed fetch failed: %s", result)
                continue
            for item in result:
                uh = str(item.get("url_hash") or "")
                if not uh or uh in seen_url_hash:
                    continue
                seen_url_hash.add(uh)
                items.append(item)
        items.sort(key=lambda x: x.get("ts_utc") or datetime.now(timezone.utc), reverse=True)
        return items[: max(1, int(max_items_per_run))]

    async def _fetch_feed(self, sem: asyncio.Semaphore, category: str, feed: dict[str, str]) -> list[dict[str, Any]]:
        async with sem:
            name = str(feed.get("name") or category)
            url = str(feed.get("url") or "").strip()
            if not url:
                return []
            raw = await self.client.get_text(f"feed:{name}:{url}", url)
            return self._parse_rss(raw, source=name, category=category)

    def _parse_rss(self, xml_text: str, *, source: str, category: str) -> list[dict[str, Any]]:
        root = ET.fromstring(xml_text)
        items: list[dict[str, Any]] = []

        channel_items = root.findall(".//item")
        atom_entries = root.findall(".//{http://www.w3.org/2005/Atom}entry")
        for node in channel_items + atom_entries:
            title = _safe_text(node.find("title")) or _safe_text(node.find("{http://www.w3.org/2005/Atom}title")) or ""
            if not title.strip():
                continue
            link = _safe_text(node.find("link")) or ""
            if not link:
                atom_link = node.find("{http://www.w3.org/2005/Atom}link")
                if atom_link is not None:
                    link = str(atom_link.attrib.get("href") or "")
            if not link:
                continue
            summary = (
                _safe_text(node.find("description"))
                or _safe_text(node.find("summary"))
                or _safe_text(node.find("{http://www.w3.org/2005/Atom}summary"))
                or ""
            )
            raw_text = f"{title}. {summary}".strip()
            published_text = (
                _safe_text(node.find("pubDate"))
                or _safe_text(node.find("published"))
                or _safe_text(node.find("{http://www.w3.org/2005/Atom}published"))
                or _safe_text(node.find("updated"))
                or _safe_text(node.find("{http://www.w3.org/2005/Atom}updated"))
                or ""
            )
            ts_utc = _parse_datetime(published_text)
            alert_keyword = _contains_alert_keyword(raw_text)
            topics = _detect_topics(raw_text)
            region = _detect_region(raw_text)
            entities = _detect_persons(raw_text)
            severity = _compute_severity(alert_keyword=alert_keyword, topics=topics, region=region)
            items.append(
                {
                    "ts_utc": ts_utc,
                    "source": source,
                    "category": category,
                    "title": _clean_text(title),
                    "title_hash": _sha256(_clean_text(title).lower()),
                    "url": link,
                    "url_hash": _sha256(link.strip()),
                    "summary": _clean_text(summary),
                    "raw_text": _clean_text(raw_text),
                    "region": region,
                    "topics_json": topics,
                    "alert_keyword": alert_keyword,
                    "severity": severity,
                    "entities_json": entities,
                    "metadata_json": {"ingest": "rss"},
                }
            )
            if len(items) >= self.max_items_per_feed:
                break
        return items

    def build_digest(self, items: list[dict[str, Any]], *, lookback_hours: int = 24) -> dict[str, Any]:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, int(lookback_hours)))
        recent: list[dict[str, Any]] = []
        for item in items:
            ts = item.get("ts_utc")
            if not isinstance(ts, datetime):
                continue
            ts_utc = _ensure_utc(ts)
            if ts_utc >= cutoff:
                normalized = dict(item)
                normalized["ts_utc"] = ts_utc
                recent.append(normalized)
        if not recent:
            return {
                "lookback_hours": lookback_hours,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "total_items": 0,
                "risk_temperature": 0,
                "high_risk_count": 0,
                "top_narratives": [],
                "main_characters": [],
                "top_topics": [],
                "top_regions": [],
            }

        now = datetime.now(timezone.utc)
        weighted = 0.0
        high_risk_count = 0
        topic_counts: dict[str, int] = {}
        region_counts: dict[str, int] = {}
        people_counts: dict[str, int] = {}

        for item in recent:
            sev = int(item.get("severity") or 0)
            age_hours = max(0.0, (now - item["ts_utc"]).total_seconds() / 3600.0)
            decay = max(0.2, 1.0 - age_hours / max(lookback_hours, 1))
            weighted += sev * decay
            if sev >= 70:
                high_risk_count += 1
            for topic in item.get("topics_json") or []:
                topic_counts[str(topic)] = topic_counts.get(str(topic), 0) + 1
            if item.get("region"):
                k = str(item["region"])
                region_counts[k] = region_counts.get(k, 0) + 1
            for person in item.get("entities_json") or []:
                p = str(person)
                people_counts[p] = people_counts.get(p, 0) + 1

        risk_temperature = min(100, int(round(weighted / max(len(recent), 1) * 1.5)))
        top_narratives = _build_narratives(recent)

        return {
            "lookback_hours": lookback_hours,
            "generated_at": now.isoformat(),
            "total_items": len(recent),
            "risk_temperature": risk_temperature,
            "high_risk_count": high_risk_count,
            "top_narratives": top_narratives[:3],
            "main_characters": _top_counts(people_counts, limit=3),
            "top_topics": _top_counts(topic_counts, limit=5),
            "top_regions": _top_counts(region_counts, limit=4),
        }


def _build_narratives(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for pattern in NARRATIVE_PATTERNS:
        keywords = [str(k).lower() for k in pattern.get("keywords", ())]
        count = 0
        for item in items:
            text = f"{item.get('title') or ''} {item.get('summary') or ''}".lower()
            if any(k in text for k in keywords):
                count += 1
        if count <= 0:
            continue
        status = "emerging"
        if count >= 6:
            status = "spreading"
        if count >= 10:
            status = "dominant"
        out.append(
            {
                "id": pattern.get("id"),
                "count": count,
                "status": status,
                "severity": pattern.get("severity", "watch"),
            }
        )
    out.sort(key=lambda x: int(x.get("count") or 0), reverse=True)
    return out


def _top_counts(raw: dict[str, int], *, limit: int) -> list[dict[str, Any]]:
    rows = [{"name": k, "count": v} for k, v in raw.items()]
    rows.sort(key=lambda x: int(x["count"]), reverse=True)
    return rows[: max(1, int(limit))]


def _compute_severity(*, alert_keyword: str | None, topics: list[str], region: str | None) -> int:
    score = 20
    if alert_keyword:
        score += 35
    for t in topics:
        if t in {"geopolitics", "fed-rates", "tariffs"}:
            score += 18
        elif t in {"inflation", "crypto"}:
            score += 10
        else:
            score += 6
    if region in {"MENA", "APAC", "EUROPE"}:
        score += 8
    return min(100, score)


def _contains_alert_keyword(text: str) -> str | None:
    lower = text.lower()
    for keyword in ALERT_KEYWORDS:
        if keyword in lower:
            return keyword
    return None


def _detect_region(text: str) -> str | None:
    lower = text.lower()
    for region, words in REGION_KEYWORDS.items():
        if any(w in lower for w in words):
            return region
    return None


def _detect_topics(text: str) -> list[str]:
    lower = text.lower()
    topics: list[str] = []
    for topic, words in TOPIC_KEYWORDS.items():
        if any(w in lower for w in words):
            topics.append(topic)
    return topics


def _detect_persons(text: str) -> list[str]:
    found: list[str] = []
    for pattern, name in PERSON_PATTERNS:
        if pattern.search(text):
            found.append(name)
    return sorted(set(found))


def _parse_datetime(value: str) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    for fmt in _RSS_DATE_FORMATS:
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue
    return datetime.now(timezone.utc)


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _safe_text(node: ET.Element | None) -> str:
    if node is None:
        return ""
    return (node.text or "").strip()


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


_CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _clean_text(text: str) -> str:
    cleaned = _CTRL_RE.sub(" ", text or "")
    return re.sub(r"\s+", " ", cleaned).strip()
