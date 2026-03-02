"""YouTube provider: RSS feed, transcript, audio download, local ASR."""
from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import httpx

from app.logging import logger


@dataclass(slots=True)
class VideoEntry:
    video_id: str
    title: str
    channel_id: str
    channel_title: str
    published_at: datetime
    url: str


# ── RSS Feed ────────────────────────────────────────────────────────

_ATOM_NS = "http://www.w3.org/2005/Atom"
_YT_NS = "http://www.youtube.com/xml/schemas/2015"
_MEDIA_NS = "http://search.yahoo.com/mrss/"
_FEED_URL = "https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
_MAX_RETRIES = 3


async def fetch_channel_feed(channel_id: str, max_entries: int = 15) -> list[VideoEntry]:
    """Fetch latest videos from a YouTube channel Atom RSS feed."""
    url = _FEED_URL.format(channel_id=channel_id)
    xml_text = await _http_get_text(url)
    if not xml_text:
        return []

    entries: list[VideoEntry] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logger.error("RSS XML parse error for %s: %s", channel_id, exc)
        return []

    # Channel title from feed
    feed_title_el = root.find(f"{{{_ATOM_NS}}}title")
    feed_title = feed_title_el.text if feed_title_el is not None and feed_title_el.text else channel_id

    for entry in root.findall(f"{{{_ATOM_NS}}}entry")[:max_entries]:
        vid_el = entry.find(f"{{{_YT_NS}}}videoId")
        title_el = entry.find(f"{{{_ATOM_NS}}}title")
        pub_el = entry.find(f"{{{_ATOM_NS}}}published")

        if vid_el is None or vid_el.text is None:
            continue

        video_id = vid_el.text.strip()
        title = title_el.text.strip() if title_el is not None and title_el.text else ""
        published_str = pub_el.text.strip() if pub_el is not None and pub_el.text else ""

        try:
            published_at = datetime.fromisoformat(published_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            published_at = datetime.now(timezone.utc)

        entries.append(VideoEntry(
            video_id=video_id,
            title=title,
            channel_id=channel_id,
            channel_title=feed_title,
            published_at=published_at,
            url=f"https://www.youtube.com/watch?v={video_id}",
        ))

    return entries


# ── Transcript (subtitles) ──────────────────────────────────────────

def fetch_transcript(video_id: str, langs: list[str] | None = None) -> tuple[str, str] | None:
    """Try to get subtitles via youtube_transcript_api. Returns (text, lang) or None."""
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
    except ImportError:
        logger.warning("youtube_transcript_api not installed, skipping transcript")
        return None

    if langs is None:
        langs = ["zh-Hans", "zh-Hant", "en"]

    try:
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
        transcript = transcript_list.find_transcript(langs)
        segments = transcript.fetch()
        text = " ".join(seg.text for seg in segments)
        lang = transcript.language_code
        return (text, lang)
    except Exception as exc:
        logger.debug("No transcript for %s: %s", video_id, type(exc).__name__)
        return None


# ── Audio Download (yt-dlp) ─────────────────────────────────────────

def download_audio(
    video_id: str,
    cache_dir: str = "data/audio",
    progress_hook: Callable | None = None,
) -> str | None:
    """Download audio-only with yt-dlp. Returns path or None.

    Args:
        progress_hook: optional callable(dict) receiving yt-dlp progress events
                       with keys like status, downloaded_bytes, total_bytes, speed, eta, filename.
    """
    try:
        import yt_dlp
    except ImportError:
        logger.warning("yt-dlp not installed, cannot download audio")
        return None

    os.makedirs(cache_dir, exist_ok=True)
    output_template = os.path.join(cache_dir, f"{video_id}.%(ext)s")

    # Check if already downloaded
    for existing in Path(cache_dir).glob(f"{video_id}.*"):
        if existing.stat().st_size > 0:
            logger.info("Audio already cached: %s", existing)
            return str(existing)

    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": output_template,
        "quiet": True,
        "no_warnings": True,
        "extract_flat": False,
        # Skip post-processing to avoid ffmpeg requirement
        "postprocessors": [],
    }

    if progress_hook:
        ydl_opts["progress_hooks"] = [progress_hook]

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=True)
            if info:
                ext = info.get("ext", "webm")
                filepath = os.path.join(cache_dir, f"{video_id}.{ext}")
                if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                    return filepath
                # Try to find the file by glob
                for f in Path(cache_dir).glob(f"{video_id}.*"):
                    if f.stat().st_size > 0:
                        return str(f)
    except Exception as exc:
        logger.error("yt-dlp download failed for %s: %s", video_id, exc)

    return None


# ── Local ASR (faster-whisper) ──────────────────────────────────────

_whisper_model_cache: dict[str, Any] = {}


def transcribe_local(
    audio_path: str,
    model_name: str = "small",
    device: str = "cuda",
    compute_type: str = "float16",
    vad_filter: bool = True,
) -> tuple[str, str] | None:
    """Transcribe audio with faster-whisper. Returns (text, lang) or None.

    Falls back from GPU(fp16) to CPU(int8) on failure.
    """
    try:
        from faster_whisper import WhisperModel
    except ImportError:
        logger.warning("faster-whisper not installed, cannot transcribe locally")
        return None

    cache_key = f"{model_name}_{device}_{compute_type}"

    if cache_key not in _whisper_model_cache:
        try:
            logger.info("Loading Whisper model: %s (device=%s, compute=%s)", model_name, device, compute_type)
            _whisper_model_cache[cache_key] = WhisperModel(model_name, device=device, compute_type=compute_type)
        except Exception as exc:
            logger.warning("GPU Whisper init failed (%s), falling back to CPU int8: %s", exc, type(exc).__name__)
            device = "cpu"
            compute_type = "int8"
            cache_key = f"{model_name}_{device}_{compute_type}"
            if cache_key not in _whisper_model_cache:
                try:
                    _whisper_model_cache[cache_key] = WhisperModel(model_name, device=device, compute_type=compute_type)
                except Exception as exc2:
                    logger.error("CPU Whisper init also failed: %s", exc2)
                    return None

    model = _whisper_model_cache[cache_key]

    try:
        segments, info = model.transcribe(audio_path, vad_filter=vad_filter)
        text_parts = [seg.text for seg in segments]
        full_text = " ".join(text_parts).strip()
        lang = info.language if info else "unknown"
        if not full_text:
            return None
        logger.info("Transcribed %s: lang=%s, chars=%d", audio_path, lang, len(full_text))
        return (full_text, lang)
    except Exception as exc:
        logger.error("Transcription failed for %s: %s", audio_path, exc)
        return None


# ── Channel URL Resolution ──────────────────────────────────────────

async def resolve_channel_id(url: str) -> str | None:
    """Extract channel_id from various YouTube URL formats.

    Supports:
    - youtube.com/channel/UCxxxx  -> UCxxxx
    - youtube.com/@handle          -> needs HTML scrape for channel_id
    - youtube.com/c/CustomName     -> needs HTML scrape
    """
    url = url.strip()

    # Direct channel ID in URL
    match = re.search(r"youtube\.com/channel/(UC[\w-]+)", url)
    if match:
        return match.group(1)

    # Handle-based or custom URLs — need to fetch page to get channel ID
    if re.search(r"youtube\.com/(@[\w.-]+|c/[\w.-]+|user/[\w.-]+)", url):
        html = await _http_get_text(url, use_browser_headers=True)
        if html:
            # Multiple patterns to find channel ID in YouTube page source
            patterns = [
                r'"channelId"\s*:\s*"(UC[\w-]+)"',
                r'"externalId"\s*:\s*"(UC[\w-]+)"',
                r'"browseId"\s*:\s*"(UC[\w-]+)"',
                r'<meta\s+itemprop="channelId"\s+content="(UC[\w-]+)"',
                r'<link\s+rel="canonical"\s+href="https?://www\.youtube\.com/channel/(UC[\w-]+)"',
                r'/channel/(UC[\w-]+)',
            ]
            for pattern in patterns:
                cid_match = re.search(pattern, html)
                if cid_match:
                    return cid_match.group(1)

    # Maybe it's just a bare channel ID
    if url.startswith("UC") and len(url) == 24:
        return url

    return None


# ── HTTP Helper ─────────────────────────────────────────────────────

_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


async def _http_get_text(url: str, retries: int = _MAX_RETRIES, use_browser_headers: bool = False) -> str | None:
    """GET with simple retry logic."""
    headers = _BROWSER_HEADERS if use_browser_headers else None
    for attempt in range(1, retries + 1):
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True, headers=headers) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                return resp.text
        except Exception as exc:
            logger.warning("HTTP GET %s attempt %d/%d failed: %s", url, attempt, retries, exc)
            if attempt == retries:
                return None
    return None
