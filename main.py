#!/usr/bin/env python3
"""Incrementally crawl VCPedia via MarkItDown and persist page markdown artifacts."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import re
import sys
import threading
import time
import urllib.parse
import urllib.request
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup  # pyright: ignore[reportMissingImports]
from kb_batch_upload import UploadError, build_upload_summary, upload_files_to_kb
from loguru import logger  # pyright: ignore[reportMissingImports]
from markdownify import markdownify as html_to_markdown  # pyright: ignore[reportMissingImports]
from markitdown import MarkItDown  # pyright: ignore[reportMissingImports]
from openai import OpenAI  # pyright: ignore[reportMissingImports]
from tqdm import tqdm  # pyright: ignore[reportMissingImports]

BASE = Path(__file__).resolve().parent
PAGES_DIR = BASE / "research/vcpedia-pages-md"
STATE_FILE = BASE / "research/vcpedia-crawl-state.json"
INDEX_FILE = BASE / "research/vcpedia-pages-index.jsonl"
EXPLORE_FILTER_FILE = BASE / "research/vcpedia-explore-filter.json"
SUMMARY_CACHE_FILE = BASE / "research/vcpedia-summary-cache.json"
SUMMARY_SOURCE_MAX_CHARS = 65536
SUMMARY_MAX_TOKENS = 512
SUMMARY_MIN_CHARS_FOR_LLM = 1024


BAD_PATH_HINTS = (
    "Special:",
    "Template:",
    "Category:",
    "User:",
    "Talk:",
    "File:",
    "VCPedia:",
)

BAD_URL_HINTS = (
    "/load.php",
    "/api.php",
    "/rest.php",
    "index.php?",
    "action=edit",
    "redlink=1",
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch VCPedia pages via MarkItDown")
    p.add_argument("--pages-dir", default=str(PAGES_DIR), help="Directory to store per-page markdown files")
    p.add_argument("--state-file", default=str(STATE_FILE), help="Crawler state json path")
    p.add_argument("--index-file", default=str(INDEX_FILE), help="Index manifest jsonl path for RAG")
    p.add_argument(
        "--explore-filter-json",
        default=str(EXPLORE_FILTER_FILE),
        help=(
            "Manual JSON rules for URL deny filtering. Supported: "
            "list[regex] or {deny_patterns:[regex]}."
        ),
    )
    p.add_argument(
        "--per-run",
        type=int,
        default=100,
        help="Max pages attempted per run",
    )
    p.add_argument(
        "--fetch-workers",
        type=int,
        default=8,
        help="Max parallel workers for page fetch in crawl mode",
    )
    p.add_argument(
        "--fetch-batch-size",
        type=int,
        default=0,
        help="Max URLs submitted in one fetch batch (<=0 means auto: fetch_workers * 2)",
    )
    p.add_argument(
        "--fetch-timeout",
        type=float,
        default=30.0,
        help="Per-request timeout in seconds for MarkItDown URL fetch (<=0 disables timeout)",
    )
    p.add_argument(
        "--wiki-qps",
        type=float,
        default=1.0,
        help="Wiki fetch rate limit in requests per second (<=0 disables throttling)",
    )
    p.add_argument(
        "--target-total",
        type=int,
        default=0,
        help="Stop fetching when visited pages reach this total (<=0 means no cap)",
    )
    p.add_argument("--retry-base-seconds", type=float, default=30.0, help="Backoff base seconds for failed URLs")
    p.add_argument("--retry-max-seconds", type=float, default=3600.0, help="Backoff max seconds for failed URLs")
    p.add_argument(
        "--retry",
        type=int,
        default=0,
        help="Retry up to N previously failed URLs first in current run (0 disables)",
    )
    p.add_argument(
        "--llm-base-url",
        default=os.getenv("OPENAI_BASE_URL", "https://api.cursorai.art/v1"),
        help="OpenAI-compatible base url, e.g. https://api.openai.com/v1",
    )
    p.add_argument(
        "--llm-api-key",
        default=os.getenv("OPENAI_API_KEY", ""),
        help="OpenAI-compatible API key (or set OPENAI_API_KEY)",
    )
    p.add_argument(
        "--llm-model",
        default=os.getenv("OPENAI_MODEL", "gpt-5.4-mini"),
        help="Model name for OpenAI-compatible chat/completions",
    )
    p.add_argument(
        "--llm-qps",
        type=float,
        default=3.0,
        help="AI service rate limit in requests per second (<=0 disables throttling)",
    )
    p.add_argument("--llm-timeout", type=int, default=60, help="LLM request timeout seconds")
    p.add_argument(
        "--kb-id",
        default=os.getenv("ASTRBOT_KB_ID", ""),
        help="AstrBot knowledge base ID; set to auto-upload generated pages after crawl",
    )
    p.add_argument(
        "--kb-base-url",
        default=os.getenv("ASTRBOT_BASE_URL", "http://127.0.0.1:6185"),
        help="AstrBot dashboard base URL for knowledge base upload",
    )
    p.add_argument("--kb-token", default=os.getenv("ASTRBOT_TOKEN", ""), help="JWT token for KB upload")
    p.add_argument("--kb-username", default=os.getenv("ASTRBOT_USERNAME", ""), help="Dashboard username")
    p.add_argument("--kb-password", default=os.getenv("ASTRBOT_PASSWORD", ""), help="Dashboard password")
    p.add_argument(
        "--kb-files-per-task",
        type=int,
        default=20,
        help="How many files to include in one KB upload task",
    )
    p.add_argument("--kb-poll-interval", type=float, default=1.5, help="Seconds between KB upload progress polling")
    p.add_argument("--kb-chunk-size", type=int, default=512, help="KB upload chunk size")
    p.add_argument("--kb-chunk-overlap", type=int, default=50, help="KB upload chunk overlap")
    p.add_argument("--kb-batch-size", type=int, default=32, help="KB upload embedding batch size")
    p.add_argument("--kb-tasks-limit", type=int, default=3, help="KB upload embedding concurrency")
    p.add_argument("--kb-max-retries", type=int, default=3, help="KB upload max retries")
    p.add_argument("--kb-timeout", type=float, default=120.0, help="KB upload HTTP request timeout")
    p.add_argument(
        "--kb-progress-file",
        default=str(BASE / "research/vcpedia-kb-upload-state.json"),
        help="Persistent KB upload progress json file",
    )
    p.add_argument(
        "--serverchan3-sendkey",
        default=os.getenv("SERVERCHAN3_SENDKEY", ""),
        help="ServerChan3 sendkey for end-of-run notification",
    )
    p.add_argument(
        "--serverchan3-endpoint",
        default=os.getenv("SERVERCHAN3_ENDPOINT", "https://sctapi.ftqq.com"),
        help="ServerChan3 API endpoint base url",
    )
    p.add_argument(
        "--notify-on-failure",
        action="store_true",
        help="Send ServerChan3 notification even when run fails",
    )
    p.add_argument("--log-every", type=int, default=10, help="Progress log interval")
    p.add_argument("--verbose", action="store_true", help="Debug logs")
    return p.parse_args()


def setup_logger(verbose: bool) -> Any:
    level = "DEBUG" if verbose else "INFO"
    # Keep one stdout sink and switch detail level via --verbose.
    logger.remove()
    format_str = (
        "<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | {message}"
        if verbose
        else "<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}"
    )
    logger.add(
        sys.stdout,
        level=level,
        colorize=True,
        backtrace=verbose,
        diagnose=verbose,
        format=format_str,
    )
    return logger


def extract_title_from_markdown(markdown_text: str, fallback_url: str) -> str:
    for line in markdown_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            title = stripped.lstrip("#").strip()
            if title:
                return title
    return page_slug_from_url(fallback_url)


def markitdown_convert_url(
    url: str,
    logger: Any,
    wiki_rate_limiter: RequestRateLimiter | None = None,
    retries: int = 3,
    fetch_timeout: float = 30.0,
) -> dict:
    last_err: Exception | None = None
    for i in range(1, retries + 1):
        requests_session: DefaultTimeoutSession | None = None
        try:
            logger.debug(
                f"markitdown convert start | attempt={i}/{retries} | url={url} "
                f"| fetch_timeout={fetch_timeout}"
            )
            if wiki_rate_limiter is not None:
                wiki_rate_limiter.wait()
            requests_session = DefaultTimeoutSession(
                default_timeout=fetch_timeout if fetch_timeout > 0 else None,
            )
            converter = MarkItDown(enable_plugins=True, requests_session=requests_session)
            result = converter.convert(url)
            markdown_text = getattr(result, "text_content", "") or ""
            title = (
                getattr(result, "title", "")
                or getattr(getattr(result, "metadata", None), "title", "")
                or extract_title_from_markdown(markdown_text, url)
            )
            payload = {
                "success": bool(markdown_text.strip()),
                "title": normalize_page_title(title, page_slug_from_url(url)),
                "content": markdown_text,
            }
            logger.debug(
                f"markitdown convert ok | attempt={i}/{retries} | url={url} | "
                f"success={payload['success']} | content_chars={len(markdown_text)}"
            )
            if payload["success"]:
                return payload
            last_err = RuntimeError(f"MarkItDown returned empty content for {url}")
            logger.debug(f"markitdown convert empty | attempt={i}/{retries} | url={url}")
            break
        except Exception as e:
            last_err = e
            logger.debug(f"markitdown convert failed | attempt={i}/{retries} | url={url} | err={e}")
            time.sleep(0.5 * i)
        finally:
            if requests_session is not None:
                requests_session.close()

    try:
        if wiki_rate_limiter is not None:
            wiki_rate_limiter.wait()
        payload = convert_vcpedia_via_api(url, fetch_timeout=fetch_timeout)
        logger.debug(f"mediawiki api fallback ok | url={url} | content_chars={len(payload.get('content') or '')}")
        if payload.get("success"):
            return payload
    except Exception as e:
        last_err = e
        logger.debug(f"mediawiki api fallback failed | url={url} | err={e}")
    if last_err:
        raise last_err
    raise RuntimeError("unreachable")


def clean_url(u: str) -> str:
    u = u.strip().rstrip(").,;!?]>")
    p = urllib.parse.urlparse(u)
    return urllib.parse.urlunparse(("https", "vcpedia.cn", p.path, "", p.query, ""))


def page_visit_key(url: str) -> str:
    """Collapse language variants to the last path segment for dedupe/visited checks."""
    path = urllib.parse.urlparse(url).path.rstrip("/")
    if not path:
        return "/"
    last_segment = path.rsplit("/", 1)[-1]
    return urllib.parse.unquote(last_segment) or "/"


def normalize_page_title(title: str, fallback: str) -> str:
    cleaned = re.sub(r"\s+", " ", (title or "").strip()).strip(" \"'")
    if " - " in cleaned:
        cleaned = cleaned.split(" - ", 1)[0].strip()
    return cleaned or fallback


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_candidate(url: str) -> bool:
    if not url.startswith("https://vcpedia.cn/"):
        return False
    if any(x in url for x in BAD_URL_HINTS):
        return False
    decoded = urllib.parse.unquote(url)
    if any(x in decoded for x in BAD_PATH_HINTS):
        return False
    return True


def extract_internal_links(markdown_text: str) -> list[str]:
    links = set()
    # Absolute URLs in plain text.
    for m in re.finditer(r"https://vcpedia\.cn/[^\s)\]>\"']+", markdown_text):
        u = clean_url(m.group(0))
        if is_candidate(u):
            links.add(u)
    # Markdown links like [title](/%E6%B4%9B%E5%A4%A9%E4%BE%9D "洛天依")
    for m in re.finditer(r"\[[^\]]+\]\(([^)\s]+)", markdown_text):
        href = m.group(1).strip()
        if href.startswith("#"):
            continue
        joined = urllib.parse.urljoin("https://vcpedia.cn/", href)
        u = clean_url(joined)
        if is_candidate(u):
            links.add(u)
    return sorted(links)


def compact_excerpt(md_content: str, max_len: int = 1000000) -> str:
    text = re.sub(r"^---[\s\S]*?---\s*", " ", md_content, count=1)
    text = re.sub(r"\*\s*欢迎加入\[本站QQ群\][^\n]*", " ", text)
    text = re.sub(r"\*\s*若发现内容有误[^\n]*", " ", text)
    text = re.sub(r"\*\s*想要为VCPedia做出更多贡献[^\n]*", " ", text)
    text = re.sub(r"\[跳转到导航\][^\n]*", " ", text)
    text = text.replace("#", " ").replace("*", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_len] + ("..." if len(text) > max_len else "")


def short_summary(md_content: str, max_len: int = 220) -> str:
    return compact_excerpt(md_content, max_len=max_len)


class RequestRateLimiter:
    """Simple process-local rate limiter to protect upstream LLM service."""

    def __init__(self, qps: float) -> None:
        self._qps = float(qps)
        self._lock = threading.Lock()
        self._next_allowed_ts = 0.0

    def wait(self) -> None:
        if self._qps <= 0:
            return

        interval = 1.0 / self._qps
        with self._lock:
            now = time.monotonic()
            wait_seconds = self._next_allowed_ts - now
            if wait_seconds > 0:
                time.sleep(wait_seconds)
                now = time.monotonic()
            self._next_allowed_ts = max(self._next_allowed_ts + interval, now + interval)


class DefaultTimeoutSession(requests.Session):
    """requests.Session with an optional default timeout."""

    def __init__(self, *, default_timeout: float | None) -> None:
        super().__init__()
        self._default_timeout = default_timeout
        self.trust_env = False
        self.headers.update(
            {
                "Accept": "text/markdown, text/html;q=0.9, text/plain;q=0.8, */*;q=0.1",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.6",
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
                ),
            }
        )

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        if self._default_timeout is not None and "timeout" not in kwargs:
            kwargs["timeout"] = self._default_timeout
        return super().request(method, url, **kwargs)


def convert_vcpedia_via_api(url: str, *, fetch_timeout: float) -> dict[str, Any]:
    title = page_slug_from_url(url)
    session = DefaultTimeoutSession(
        default_timeout=fetch_timeout if fetch_timeout > 0 else None,
    )
    try:
        response = session.get(
            "https://vcpedia.cn/api.php",
            params={
                "action": "parse",
                "page": title,
                "prop": "text|displaytitle",
                "format": "json",
                "formatversion": "2",
            },
        )
        response.raise_for_status()
        data = response.json()
    finally:
        session.close()

    parse_data = data.get("parse") or {}
    html = str(parse_data.get("text") or "").strip()
    display_title = str(parse_data.get("displaytitle") or "").strip() or title
    markdown_text = mediawiki_html_to_markdown(html, display_title)
    return {
        "success": bool(markdown_text.strip()),
        "title": display_title.strip() or title,
        "content": markdown_text,
    }


def mediawiki_html_to_markdown(html: str, title: str) -> str:
    if not html.strip():
        return ""

    soup = BeautifulSoup(html, "html.parser")
    for tag_name in ("script", "style", "noscript"):
        for node in soup.find_all(tag_name):
            node.decompose()

    for selector in (
        ".mw-editsection",
        ".reference",
        ".noprint",
        ".toc",
        ".navbox",
        ".metadata",
        ".mw-empty-elt",
    ):
        for node in soup.select(selector):
            node.decompose()

    content_root = soup.select_one(".mw-parser-output") or soup
    markdown_body = html_to_markdown(
        str(content_root),
        heading_style="ATX",
        bullets="-",
        strip=["span"],
    ).strip()
    markdown_body = re.sub(r"\n{3,}", "\n\n", markdown_body)
    markdown_body = re.sub(r"^[ \t]+$", "", markdown_body, flags=re.MULTILINE)
    if not markdown_body:
        return ""
    if markdown_body.startswith(f"# {title}"):
        return markdown_body
    return f"# {title}\n\n{markdown_body}".strip()


@dataclass
class LlmUsage:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


@dataclass
class CrawlRunStats:
    scanned_this_run: int = 0
    fetched_this_run: int = 0
    failed_this_run: int = 0
    deferred_retry_this_run: int = 0
    queue_size: int = 0
    visited_total: int = 0
    failed_total: int = 0
    llm_success_count: int = 0
    llm_fallback_count: int = 0
    llm_cache_hit_count: int = 0
    enqueue_skipped_by_filter: int = 0
    llm_usage: LlmUsage = field(default_factory=LlmUsage)


def content_summary_cache_key(title: str, content: str) -> str:
    h = hashlib.sha1()
    h.update(title.strip().encode("utf-8"))
    h.update(b"\n")
    h.update(content.strip().encode("utf-8"))
    return h.hexdigest()


def load_summary_cache(cache_path: Path, logger: Any) -> dict[str, str]:
    if not cache_path.exists():
        logger.debug(f"summary cache missing | file={cache_path}")
        return {}
    try:
        raw = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"summary cache load failed | file={cache_path} | err={e}")
        return {}
    if not isinstance(raw, dict):
        logger.warning(f"summary cache invalid schema | file={cache_path}")
        return {}
    cache = {
        str(k): str(v).strip()
        for k, v in raw.items()
        if isinstance(k, str) and isinstance(v, str) and v.strip()
    }
    logger.info(f"summary cache loaded | file={cache_path} | entries={len(cache)}")
    return cache


def save_summary_cache(cache_path: Path, cache: dict[str, str], logger: Any) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = cache_path.with_suffix(cache_path.suffix + ".tmp")
    tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(cache_path)
    logger.debug(f"summary cache saved | file={cache_path} | entries={len(cache)}")


def build_summary(
    *,
    title: str,
    content: str,
    logger: Any,
    llm_client: OpenAI | None,
    llm_rate_limiter: RequestRateLimiter,
    llm_model: str,
    summary_cache: dict[str, str],
    summary_cache_lock: threading.Lock | None = None,
) -> tuple[str, LlmUsage, str]:
    summary_cache_key = content_summary_cache_key(title, content)
    if summary_cache_lock is not None:
        with summary_cache_lock:
            cached_summary = summary_cache.get(summary_cache_key, "")
    else:
        cached_summary = summary_cache.get(summary_cache_key, "")
    if cached_summary:
        logger.debug(f"summary cache hit | title={title} | cache_key={summary_cache_key}")
        return cached_summary, LlmUsage(), "cache_hit"

    excerpt = compact_excerpt(content, max_len=SUMMARY_SOURCE_MAX_CHARS)
    if llm_client is None or len(excerpt) < SUMMARY_MIN_CHARS_FOR_LLM:
        summary = short_summary(content)
        logger.debug(
            f"summary local shortcut | title={title} | llm_enabled={llm_client is not None} | summary={summary}"
        )
        return summary, LlmUsage(), "fallback"

    summary, usage = generate_summary_with_llm(
        title=title,
        content=content,
        logger=logger,
        client=llm_client,
        limiter=llm_rate_limiter,
        model=llm_model,
        source_max_chars=SUMMARY_SOURCE_MAX_CHARS,
        max_tokens=SUMMARY_MAX_TOKENS,
    )
    if summary_cache_lock is not None:
        with summary_cache_lock:
            summary_cache[summary_cache_key] = summary
    else:
        summary_cache[summary_cache_key] = summary
    return summary, usage, "llm"


def process_fetched_page(
    *,
    url: str,
    data: dict[str, Any] | None,
    fetch_error: str | None,
    logger: Any,
    retry_base: float,
    retry_max: float,
    pages_dir: Path,
    llm_client: OpenAI | None,
    llm_rate_limiter: RequestRateLimiter,
    llm_model: str,
    summary_cache: dict[str, str],
    summary_cache_lock: threading.Lock | None = None,
) -> dict[str, Any]:
    if not data:
        return {
            "url": url,
            "status": "fetch_failed",
            "error": fetch_error or "unknown_error",
            "failure_meta": {
                "last_error": fetch_error or "unknown_error",
            },
        }

    if not data.get("success"):
        logger.debug(f"markitdown returned empty content | url={url} | keys={sorted(data.keys())}")
        return {
            "url": url,
            "status": "empty_content",
            "error": "api_success_false",
            "failure_meta": {
                "last_error": "api_success_false",
            },
        }

    title = normalize_page_title(data.get("title") or "", page_slug_from_url(url))
    content = data.get("content") or ""
    logger.debug(
        f"page payload ready | url={url} | title={title} | content_chars={len(content)} | "
        f"content_preview={compact_excerpt(content, max_len=200)}"
    )

    llm_usage = LlmUsage()
    summary_mode = "fallback"
    try:
        summary, usage, summary_mode = build_summary(
            title=title,
            content=content,
            logger=logger,
            llm_client=llm_client,
            llm_rate_limiter=llm_rate_limiter,
            llm_model=llm_model,
            summary_cache=summary_cache,
            summary_cache_lock=summary_cache_lock,
        )
        llm_usage = usage
    except Exception as e:
        logger.warning(f"llm summary failed | url={url} | err={e} | fallback=short_summary")
        summary = short_summary(content)
        summary_mode = "fallback"
        logger.debug(f"llm fallback summary | url={url} | summary={summary}")

    out_path = write_page_markdown(pages_dir, title, url, summary, content)
    fetched_at = iso_now()
    logger.debug(
        f"page persist start | url={url} | out_path={out_path} | fetched_at={fetched_at} | "
        f"summary_chars={len(summary)}"
    )
    extracted_links = extract_internal_links(content)
    logger.debug(
        f"link extraction done | url={url} | extracted_count={len(extracted_links)} | "
        f"sample={extracted_links[:10]}"
    )
    return {
        "url": url,
        "status": "ok",
        "title": title,
        "content": content,
        "summary": summary,
        "summary_mode": summary_mode,
        "llm_usage": llm_usage,
        "out_path": out_path,
        "fetched_at": fetched_at,
        "extracted_links": extracted_links,
        "failure_meta": {},
    }


def generate_summary_with_llm(
    *,
    title: str,
    content: str,
    logger: Any,
    client: OpenAI,
    limiter: RequestRateLimiter,
    model: str,
    source_max_chars: int,
    max_tokens: int,
) -> tuple[str, LlmUsage]:
    source_text = compact_excerpt(content, max_len=max(500, source_max_chars))

    # Throttle outbound LLM calls to reduce 429 and protect upstream capacity.
    logger.debug(
        f"llm summary start | model={model} | title={title} | source_chars={len(source_text)} "
        f"| max_tokens={max_tokens} | qps_limit={limiter._qps}"
    )
    limiter.wait()
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": "你负责把 wiki 页面压缩成简洁中文摘要。",
            },
            {
                "role": "user",
                "content": (
                    "请基于页面内容生成中文摘要，要求：\n"
                    "1) 60-100字；\n"
                    "2) 客观、中性；\n"
                    "3) 不输出项目符号；\n"
                    "4) 不包含\"本文\"\"该页面\"等指代；\n"
                    "5) 优先概括人物/作品/事件/组织的身份与关键特征。\n\n"
                    f"标题：{title}\n\n内容：\n{source_text}"
                ),
            },
        ],
    )

    choice = response.choices[0] if response.choices else None
    message = choice.message if choice else None
    text = message.content if message else None
    if not isinstance(text, str) or not text.strip():
        logger.debug(f"llm summary invalid response | title={title} | response_has_choices={bool(response.choices)}")
        raise RuntimeError("invalid llm response: empty message content")

    usage = getattr(response, "usage", None)
    prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
    completion_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
    total_tokens = int(getattr(usage, "total_tokens", 0) or 0)

    summary_text = re.sub(r"\s+", " ", text).strip()
    logger.debug(
        f"llm summary ok | title={title} | summary_chars={len(summary_text)} | "
        f"prompt_tokens={prompt_tokens} | completion_tokens={completion_tokens} | total_tokens={total_tokens}"
    )

    return summary_text, LlmUsage(
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
    )


def build_run_summary_message(args: argparse.Namespace, stats: CrawlRunStats, status: str, error: str = "") -> str:
    lines = [
        f"状态: {status}",
        f"模型: {args.llm_model}",
        f"扫描: {stats.scanned_this_run}",
        f"抓取成功: {stats.fetched_this_run}",
        f"抓取失败: {stats.failed_this_run}",
        f"重试延后: {stats.deferred_retry_this_run}",
        f"已访问总量: {stats.visited_total}",
        f"失败总量: {stats.failed_total}",
        f"队列剩余: {stats.queue_size}",
        f"LLM成功: {stats.llm_success_count}",
        f"LLM回退: {stats.llm_fallback_count}",
        f"LLM缓存命中: {stats.llm_cache_hit_count}",
        f"入队过滤: {stats.enqueue_skipped_by_filter}",
        f"LLM prompt_tokens: {stats.llm_usage.prompt_tokens}",
        f"LLM completion_tokens: {stats.llm_usage.completion_tokens}",
        f"LLM total_tokens: {stats.llm_usage.total_tokens}",
    ]
    if error:
        lines.append(f"错误: {error}")
    lines.append(f"时间: {iso_now()}")
    return "\n".join(lines)


def build_kb_upload_message(result: dict[str, Any]) -> str:
    return build_upload_summary(result)


def send_serverchan3_notification(
    *,
    logger: Any,
    sendkey: str,
    endpoint_base: str,
    title: str,
    desp: str,
) -> None:
    if not sendkey.strip():
        logger.info("serverchan3 skipped: missing sendkey")
        return

    endpoint = endpoint_base.rstrip("/") + f"/{sendkey}.send"
    body = urllib.parse.urlencode({"title": title, "desp": desp}).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        raw = r.read().decode("utf-8", errors="ignore")
    logger.info(f"serverchan3 delivered | endpoint={endpoint} | response={raw[:200]}")


def safe_name(name: str, fallback: str = "untitled") -> str:
    name = re.sub(r"[\\/:*?\"<>|]+", "_", (name or "").strip())
    name = re.sub(r"\s+", " ", name).strip(" .")
    if not name:
        return fallback
    return name[:80]


def page_slug_from_url(url: str) -> str:
    path = urllib.parse.urlparse(url).path
    slug = urllib.parse.unquote(path.rstrip("/").rsplit("/", 1)[-1])
    return safe_name(slug, fallback="page")


def build_page_filename(title: str, url: str) -> str:
    base_name = safe_name(title, fallback=page_slug_from_url(url))
    suffix = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
    return f"{base_name}-{suffix}.md"


def load_crawl_state(state_path: Path) -> dict:
    if state_path.exists():
        raw = json.loads(state_path.read_text(encoding="utf-8"))
        queue = [clean_url(u) for u in raw.get("queue", []) if isinstance(u, str) and u.strip()]
        visited = [clean_url(u) for u in raw.get("visited", []) if isinstance(u, str) and u.strip()]
        failed = [clean_url(u) for u in raw.get("failed", []) if isinstance(u, str) and u.strip()]
        failure_meta_raw = raw.get("failure_meta", {})
        failure_meta: dict[str, dict[str, Any]] = {}
        if isinstance(failure_meta_raw, dict):
            for k, v in failure_meta_raw.items():
                if not isinstance(k, str) or not isinstance(v, dict):
                    continue
                u = clean_url(k)
                failure_meta[u] = {
                    "count": int(v.get("count", 0) or 0),
                    "next_retry_at": str(v.get("next_retry_at") or ""),
                    "last_error": str(v.get("last_error") or ""),
                }
        return {
            "queue": queue,
            "visited": visited,
            "failed": failed,
            "failure_meta": failure_meta,
            "created_at": raw.get("created_at") or iso_now(),
            "updated_at": raw.get("updated_at") or iso_now(),
        }

    return {
        "queue": [],
        "visited": [],
        "failed": [],
        "failure_meta": {},
        "created_at": iso_now(),
        "updated_at": iso_now(),
    }


def save_crawl_state(state_path: Path, state: dict) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = iso_now()
    tmp = state_path.with_suffix(state_path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(state_path)


def write_page_markdown(pages_dir: Path, title: str, url: str, summary: str, content: str) -> Path:
    pages_dir.mkdir(parents=True, exist_ok=True)
    file_name = build_page_filename(title, url)
    out_path = pages_dir / file_name
    fetched_at = iso_now()

    lines = [
        "---",
        f"title: {title}",
        f"source_url: {url}",
        f"fetched_at: {fetched_at}",
        f"summary: {summary}",
        "---",
        "",
        f"# {title}",
        "",
        f"- source: {url}",
        f"- fetched_at: {fetched_at}",
        "",
        "## Summary",
        "",
        summary,
        "",
        "## Content",
        "",
        content.strip(),
        "",
    ]
    out_path.write_text("\n".join(lines), encoding="utf-8")
    return out_path


def upsert_index_record(index_file: Path, record: dict[str, Any]) -> None:
    index_file.parent.mkdir(parents=True, exist_ok=True)
    by_url: dict[str, dict[str, Any]] = {}

    if index_file.exists():
        for line in index_file.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            url = obj.get("url")
            if isinstance(url, str) and url:
                by_url[url] = obj

    by_url[record["url"]] = record
    with index_file.open("w", encoding="utf-8") as f:
        for url in sorted(by_url.keys()):
            f.write(json.dumps(by_url[url], ensure_ascii=False) + "\n")


def parse_iso_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def compute_next_retry_at(count: int, base_seconds: float, max_seconds: float) -> str:
    if count <= 0:
        return iso_now()
    delay = min(max_seconds, base_seconds * (2 ** (count - 1)))
    ts = datetime.now(timezone.utc).timestamp() + delay
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def load_explore_filter_rules(
    filter_path: Path,
    logger: Any,
) -> list[re.Pattern[str]]:
    """Load regex-based deny rules from JSON.

    Supported schema:
    1) ["regex1", "regex2"]
    2) {"deny_patterns": ["regex1", "regex2"]}

    Backward compatibility:
    - older exact-url fields are converted into escaped regex patterns.
    """
    if not filter_path.exists():
        logger.debug(f"explore filter missing | file={filter_path}")
        return []

    raw = json.loads(filter_path.read_text(encoding="utf-8"))
    raw_patterns: list[str] = []

    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, str) and item.strip():
                raw_patterns.append(item.strip())
    elif isinstance(raw, dict):
        for item in raw.get("deny_patterns", []):
            if isinstance(item, str) and item.strip():
                raw_patterns.append(item.strip())
        for legacy_key in ("stop_expand_pages", "all_blocked", "block_links"):
            for item in raw.get(legacy_key, []):
                if isinstance(item, str) and item.strip():
                    raw_patterns.append(re.escape(clean_url(item)))
        by_source = raw.get("block_links_by_source", {})
        if isinstance(by_source, dict):
            for source, targets in by_source.items():
                if isinstance(source, str) and source.strip():
                    raw_patterns.append(re.escape(clean_url(source)))
                if isinstance(targets, list):
                    for target in targets:
                        if isinstance(target, str) and target.strip():
                            raw_patterns.append(re.escape(clean_url(target)))
    else:
        raise ValueError(f"unsupported explore-filter-json schema: {filter_path}")

    patterns: list[re.Pattern[str]] = []
    invalid_patterns: list[str] = []
    for pattern in raw_patterns:
        try:
            patterns.append(re.compile(pattern))
        except re.error:
            invalid_patterns.append(pattern)

    logger.info(
        f"explore filter loaded | file={filter_path} | deny_patterns={len(patterns)} "
        f"| invalid_patterns={len(invalid_patterns)}"
    )
    if invalid_patterns:
        logger.warning(f"explore filter has invalid regex patterns | patterns={invalid_patterns}")
    logger.debug(f"explore filter detail | patterns={[p.pattern for p in patterns]}")
    return patterns


def match_deny_pattern(url: str, patterns: list[re.Pattern[str]]) -> str | None:
    for pattern in patterns:
        if pattern.search(url):
            return pattern.pattern
    return None


def run_incremental_crawl_mode(args: argparse.Namespace, logger: Any) -> CrawlRunStats:
    pages_dir = Path(args.pages_dir)
    state_path = Path(args.state_file)
    index_file = Path(args.index_file)
    state = load_crawl_state(state_path)

    queue = list(state["queue"])
    queued = set(queue)
    visited = set(state["visited"])
    visited_keys = {page_visit_key(url) for url in visited}
    queued_key_counts = Counter(page_visit_key(url) for url in queue)
    failed = set(state["failed"])
    failure_meta: dict[str, dict[str, Any]] = dict(state.get("failure_meta", {}))
    deny_patterns = load_explore_filter_rules(Path(args.explore_filter_json), logger)
    forced_retry_urls: set[str] = set()

    retry_quota = max(0, int(args.retry))
    if retry_quota > 0 and failed:
        retry_candidates: list[str] = []
        seen_retry: set[str] = set()

        for url in state.get("failed", []):
            if isinstance(url, str) and url.strip():
                normalized = clean_url(url)
                normalized_key = page_visit_key(normalized)
                if (
                    normalized not in seen_retry
                    and normalized in failed
                    and normalized not in visited
                    and normalized_key not in visited_keys
                ):
                    retry_candidates.append(normalized)
                    seen_retry.add(normalized)

        for url in queue:
            url_key = page_visit_key(url)
            if url not in seen_retry and url in failed and url not in visited and url_key not in visited_keys:
                retry_candidates.append(url)
                seen_retry.add(url)

        valid_retry_candidates: list[str] = []
        for url in retry_candidates:
            denied_by = match_deny_pattern(url, deny_patterns)
            if denied_by:
                logger.debug(f"retry candidate denied by filter | url={url} | pattern={denied_by}")
                continue
            valid_retry_candidates.append(url)

        valid_retry_candidates.sort(
            key=lambda u: (
                -int((failure_meta.get(u) or {}).get("count", 0) or 0),
                str((failure_meta.get(u) or {}).get("next_retry_at") or ""),
                u,
            )
        )

        prioritized = valid_retry_candidates[:retry_quota]
        if prioritized:
            prioritized_set = set(prioritized)
            queue = [url for url in queue if url not in prioritized_set]
            queue = prioritized + queue
            queued = set(queue)
            queued_key_counts = Counter(page_visit_key(url) for url in queue)
            forced_retry_urls = prioritized_set
            logger.info(
                f"retry prioritize enabled | retry={retry_quota} | prioritized={len(prioritized)} "
                f"| sample={prioritized[:5]}"
            )
        else:
            logger.info(f"retry prioritize enabled | retry={retry_quota} | prioritized=0")

    summary_cache_path = SUMMARY_CACHE_FILE
    summary_cache = load_summary_cache(summary_cache_path, logger)
    summary_cache_lock = threading.Lock()

    llm_client: OpenAI | None = None
    if args.llm_api_key.strip():
        llm_client = OpenAI(
            api_key=args.llm_api_key,
            base_url=args.llm_base_url.rstrip("/"),
            timeout=max(1, int(args.llm_timeout)),
        )
    else:
        logger.info("llm summary disabled: missing api key, will use local short_summary")
    llm_rate_limiter = RequestRateLimiter(args.llm_qps)
    wiki_rate_limiter = RequestRateLimiter(args.wiki_qps)

    target_total = args.target_total if args.target_total > 0 else None
    if target_total is not None and len(visited) >= target_total:
        logger.info(f"target reached | visited={len(visited)} | target_total={target_total}")
        return CrawlRunStats(queue_size=len(queue), visited_total=len(visited), failed_total=len(failed))

    per_run = max(1, args.per_run)
    run_goal = per_run if target_total is None else min(per_run, max(0, target_total - len(visited)))
    fetch_batch_size = (
        max(1, int(args.fetch_batch_size))
        if int(args.fetch_batch_size) > 0
        else max(1, int(args.fetch_workers) * 2)
    )
    retry_base = max(0.1, float(args.retry_base_seconds))
    retry_max = max(retry_base, float(args.retry_max_seconds))
    fetched_this_run = 0
    scanned_this_run = 0
    failed_this_run = 0
    deferred_retry_this_run = 0
    llm_success_count = 0
    llm_fallback_count = 0
    llm_cache_hit_count = 0
    enqueue_skipped_by_filter = 0
    llm_usage = LlmUsage()

    logger.info(
        f"crawl start | queue={len(queue)} | visited={len(visited)} | failed={len(failed)} "
        f"| per_run={per_run} | target_total={target_total if target_total is not None else 'none'}"
    )
    logger.debug(
        f"fetch config | workers={args.fetch_workers} | batch_size={fetch_batch_size} "
        f"| timeout={args.fetch_timeout}"
    )
    if not queue:
        logger.warning(f"没有待抓取 URL，请手动编辑 {state_path} 的 state.queue")
    logger.debug(
        f"crawl state loaded | queue_head={queue[:5]} | visited_sample={sorted(list(visited))[:5]} | "
        f"failed_sample={sorted(list(failed))[:5]} | state_file={state_path}"
    )

    progress_bar = tqdm(
        total=run_goal,
        desc="crawl overall",
        unit="page",
        dynamic_ncols=True,
        disable=run_goal <= 0,
    )
    try:
        while queue and scanned_this_run < run_goal:
            if target_total is not None and len(visited) >= target_total:
                break

            remaining_quota = run_goal - scanned_this_run
            if target_total is not None:
                remaining_quota = min(remaining_quota, target_total - len(visited))
            if remaining_quota <= 0:
                break

            # Step 1: pick URLs for this batch, skipping entries still in backoff.
            batch_urls: list[str] = []
            batch_selected: set[str] = set()
            batch_target_size = min(remaining_quota, fetch_batch_size)
            inspect_budget = len(queue)
            logger.debug(
                f"batch selection start | queue_size={len(queue)} | inspect_budget={inspect_budget} "
                f"| scanned_this_run={scanned_this_run} | remaining_quota={remaining_quota} "
                f"| batch_target_size={batch_target_size}"
            )
            for _ in range(inspect_budget):
                if len(batch_urls) >= batch_target_size:
                    break
                url = queue.pop(0)
                queued.discard(url)
                url_key = page_visit_key(url)
                queued_key_counts[url_key] -= 1
                if queued_key_counts[url_key] <= 0:
                    queued_key_counts.pop(url_key, None)

                if url in batch_selected:
                    logger.debug(f"batch selection skip duplicate in same batch | url={url}")
                    continue

                if url in visited or url_key in visited_keys:
                    logger.debug(f"batch selection skip visited variant | url={url} | visit_key={url_key}")
                    continue

                denied_by = match_deny_pattern(url, deny_patterns)
                if denied_by:
                    enqueue_skipped_by_filter += 1
                    logger.debug(f"url denied before fetch | url={url} | pattern={denied_by}")
                    continue

                meta = failure_meta.get(url) or {}
                next_retry_at = parse_iso_datetime(str(meta.get("next_retry_at") or ""))
                if url in forced_retry_urls:
                    forced_retry_urls.discard(url)
                    logger.debug(f"batch selection retry override backoff | url={url}")
                elif next_retry_at and datetime.now(timezone.utc) < next_retry_at:
                    queue.append(url)
                    queued.add(url)
                    queued_key_counts[url_key] += 1
                    deferred_retry_this_run += 1
                    logger.debug(
                        f"batch selection deferred | url={url} | next_retry_at={next_retry_at.isoformat()}"
                    )
                    continue

                batch_urls.append(url)
                batch_selected.add(url)
                logger.debug(f"batch selection picked | url={url}")

            if not batch_urls:
                logger.info("all pending URLs are in backoff window, stop this run")
                break

            scanned_this_run += len(batch_urls)

            logger.debug(f"batch ready | batch_size={len(batch_urls)} | batch_urls={batch_urls}")
            # Step 2: fetch page markdown in parallel (network-bound work).
            fetch_results: dict[str, dict[str, Any] | None] = {}
            fetch_errors: dict[str, str] = {}
            max_workers = max(1, min(max(1, args.fetch_workers), len(batch_urls)))
            logger.debug(f"batch fetch start | max_workers={max_workers} | batch_size={len(batch_urls)}")
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_url = {
                    executor.submit(
                        markitdown_convert_url,
                        url,
                        logger,
                        wiki_rate_limiter,
                        3,
                        float(args.fetch_timeout),
                    ): url
                    for url in batch_urls
                }
                for future in concurrent.futures.as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        fetch_results[url] = future.result()
                        logger.debug(f"batch fetch ok | url={url}")
                    except Exception as e:
                        fetch_results[url] = None
                        fetch_errors[url] = str(e)
                        logger.debug(f"batch fetch error | url={url} | err={e}")

            # Step 3: process each fetched page in parallel, then merge crawl state sequentially.
            logger.debug(f"batch process start | max_workers={max_workers} | batch_size={len(batch_urls)}")
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_url = {
                    executor.submit(
                        process_fetched_page,
                        url=url,
                        data=fetch_results.get(url),
                        fetch_error=fetch_errors.get(url),
                        logger=logger,
                        retry_base=retry_base,
                        retry_max=retry_max,
                        pages_dir=pages_dir,
                        llm_client=llm_client,
                        llm_rate_limiter=llm_rate_limiter,
                        llm_model=args.llm_model,
                        summary_cache=summary_cache,
                        summary_cache_lock=summary_cache_lock,
                    ): url
                    for url in batch_urls
                }
                for future in concurrent.futures.as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        result = future.result()
                    except Exception as e:
                        logger.warning(f"page process crashed | url={url} | err={e}")
                        result = {
                            "url": url,
                            "status": "fetch_failed",
                            "error": str(e),
                            "failure_meta": {"last_error": str(e)},
                        }

                    status = result.get("status")
                    if status != "ok":
                        err = str(result.get("error") or "unknown_error")
                        logger.warning(f"page process failed | url={url} | status={status} | err={err}")
                        prev_count = int((failure_meta.get(url) or {}).get("count", 0) or 0)
                        count = prev_count + 1
                        failure_meta[url] = {
                            "count": count,
                            "next_retry_at": compute_next_retry_at(count, retry_base, retry_max),
                            "last_error": err,
                        }
                        failed.add(url)
                        failed_this_run += 1
                    else:
                        summary_mode = str(result.get("summary_mode") or "fallback")
                        usage = result.get("llm_usage") or LlmUsage()
                        if summary_mode == "llm":
                            llm_success_count += 1
                            llm_usage.prompt_tokens += usage.prompt_tokens
                            llm_usage.completion_tokens += usage.completion_tokens
                            llm_usage.total_tokens += usage.total_tokens
                        elif summary_mode == "cache_hit":
                            llm_cache_hit_count += 1
                        else:
                            llm_fallback_count += 1

                        out_path = Path(str(result["out_path"]))
                        fetched_at = str(result["fetched_at"])
                        title = str(result["title"])
                        summary = str(result["summary"])
                        extracted_links = list(result.get("extracted_links") or [])

                        upsert_index_record(
                            index_file,
                            {
                                "url": url,
                                "title": title,
                                "summary": summary,
                                "file_path": str(out_path),
                                "file_name": out_path.name,
                                "fetched_at": fetched_at,
                            },
                        )
                        visited.add(url)
                        visited_keys.add(page_visit_key(url))
                        failed.discard(url)
                        failure_meta.pop(url, None)
                        fetched_this_run += 1

                        for link in extracted_links:
                            link_key = page_visit_key(link)
                            denied_by = match_deny_pattern(link, deny_patterns)
                            if denied_by:
                                enqueue_skipped_by_filter += 1
                                logger.debug(
                                    f"link filtered out | source={url} | target={link} | reason=deny_pattern | pattern={denied_by}"
                                )
                                continue
                            if (
                                link in visited
                                or link_key in visited_keys
                                or link in queued
                                or queued_key_counts.get(link_key, 0) > 0
                            ):
                                logger.debug(
                                    f"link skipped as duplicate | source={url} | target={link} | "
                                    f"visited={link in visited or link_key in visited_keys} "
                                    f"| queued={link in queued or queued_key_counts.get(link_key, 0) > 0}"
                                )
                                continue
                            queue.append(link)
                            queued.add(link)
                            queued_key_counts[link_key] += 1
                            logger.debug(f"link enqueued | source={url} | target={link} | new_queue_size={len(queue)}")

                        logger.info(f"saved page | visited={len(visited)} | path={out_path.name} | url={url}")

                    if not progress_bar.disable:
                        remain = max(0, run_goal - int(progress_bar.n))
                        progress_bar.update(min(1, remain))
                        progress_bar.set_postfix(queue=len(queue), visited=len(visited), failed=len(failed), refresh=True)

            if scanned_this_run % args.log_every == 0:
                logger.info(
                    f"progress | scanned_this_run={scanned_this_run} | fetched_this_run={fetched_this_run} "
                    f"| queue={len(queue)} | visited={len(visited)} | failed={len(failed)}"
                )
    finally:
        progress_bar.close()

    state["queue"] = queue
    state["visited"] = sorted(visited)
    state["failed"] = sorted(failed)
    state["failure_meta"] = failure_meta
    save_crawl_state(state_path, state)
    save_summary_cache(summary_cache_path, summary_cache, logger)
    logger.debug(
        f"state saved | queue_size={len(queue)} | visited_size={len(visited)} | failed_size={len(failed)} | "
        f"state_file={state_path}"
    )

    logger.info(
        f"crawl done | fetched_this_run={fetched_this_run} | scanned_this_run={scanned_this_run} "
        f"| failed_this_run={failed_this_run} | deferred_retry_this_run={deferred_retry_this_run} "
        f"| queue={len(queue)} | visited={len(visited)} | failed={len(failed)} "
        f"| state={state_path} | index={index_file}"
    )

    logger.info(
        f"llm usage summary | model={args.llm_model} | llm_success={llm_success_count} "
        f"| llm_fallback={llm_fallback_count} | llm_cache_hit={llm_cache_hit_count} "
        f"| enqueue_filtered={enqueue_skipped_by_filter} "
        f"| prompt_tokens={llm_usage.prompt_tokens} "
        f"| completion_tokens={llm_usage.completion_tokens} | total_tokens={llm_usage.total_tokens}"
    )
    logger.debug(
        f"crawl summary detail | scanned={scanned_this_run} | fetched={fetched_this_run} | "
        f"failed={failed_this_run} | deferred={deferred_retry_this_run} | "
        f"visited_total={len(visited)} | queue_remaining={len(queue)} | failed_total={len(failed)}"
    )

    return CrawlRunStats(
        scanned_this_run=scanned_this_run,
        fetched_this_run=fetched_this_run,
        failed_this_run=failed_this_run,
        deferred_retry_this_run=deferred_retry_this_run,
        queue_size=len(queue),
        visited_total=len(visited),
        failed_total=len(failed),
        llm_success_count=llm_success_count,
        llm_fallback_count=llm_fallback_count,
        llm_cache_hit_count=llm_cache_hit_count,
        enqueue_skipped_by_filter=enqueue_skipped_by_filter,
        llm_usage=llm_usage,
    )


def main() -> None:
    args = parse_args()
    logger = setup_logger(args.verbose)
    logger.debug(
        f"runtime config | per_run={args.per_run} | target_total={args.target_total} | "
        f"fetch_workers={args.fetch_workers} | wiki_qps={args.wiki_qps} | "
        f"llm_model={args.llm_model} | llm_qps={args.llm_qps} | verbose={args.verbose} | "
        f"filter_json={args.explore_filter_json}"
    )
    stats = CrawlRunStats()
    upload_result: dict[str, Any] | None = None
    title = "VCPedia Crawl 完成"
    try:
        stats = run_incremental_crawl_mode(args, logger)
        desp = build_run_summary_message(args, stats, status="成功")

        if args.kb_id.strip():
            upload_result = upload_files_to_kb(
                base_url=args.kb_base_url.rstrip("/"),
                kb_id=args.kb_id.strip(),
                paths=[args.pages_dir],
                recursive=False,
                ext=[".md"],
                token=args.kb_token,
                username=args.kb_username,
                password=args.kb_password,
                progress_file=args.kb_progress_file,
                files_per_task=args.kb_files_per_task,
                poll_interval=args.kb_poll_interval,
                chunk_size=args.kb_chunk_size,
                chunk_overlap=args.kb_chunk_overlap,
                batch_size=args.kb_batch_size,
                tasks_limit=args.kb_tasks_limit,
                max_retries=args.kb_max_retries,
                timeout=args.kb_timeout,
            )
            desp = desp + "\n\n" + build_kb_upload_message(upload_result)
            if int(upload_result.get("total_failed", 0) or 0) > 0:
                title = "VCPedia Crawl + KB Upload 失败"
                raise UploadError(
                    f"Knowledge base upload finished with {upload_result['total_failed']} failed file(s)"
                )
    except Exception as e:
        title = "VCPedia Crawl 失败" if upload_result is None else "VCPedia Crawl + KB Upload 失败"
        desp = build_run_summary_message(args, stats, status="失败", error=str(e))
        if upload_result is not None:
            desp = desp + "\n\n" + build_kb_upload_message(upload_result)
        if args.notify_on_failure:
            try:
                send_serverchan3_notification(
                    logger=logger,
                    sendkey=args.serverchan3_sendkey,
                    endpoint_base=args.serverchan3_endpoint,
                    title=title,
                    desp=desp,
                )
            except Exception as notify_err:
                logger.warning(f"serverchan3 failed after crawl error | err={notify_err}")
        raise

    try:
        send_serverchan3_notification(
            logger=logger,
            sendkey=args.serverchan3_sendkey,
            endpoint_base=args.serverchan3_endpoint,
            title=title,
            desp=desp,
        )
    except Exception as notify_err:
        logger.warning(f"serverchan3 notify failed | err={notify_err}")


if __name__ == "__main__":
    main()
