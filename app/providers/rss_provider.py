from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class NewsItem:
    title: str
    link: str
    published_at: str
    summary: str = ""


class RSSProvider:
    """V1 placeholder. RSS aggregation is intentionally not active in V0."""

    async def fetch(self) -> list[NewsItem]:
        return []
