#!/usr/bin/env python3
"""Incrementally crawl VCPedia via markdown.new and persist page markdown artifacts."""

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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger  # pyright: ignore[reportMissingImports]
from openai import OpenAI  # pyright: ignore[reportMissingImports]

BASE = Path(__file__).resolve().parent
PAGES_DIR = BASE / "research/vcpedia-pages-md"
STATE_FILE = BASE / "research/vcpedia-crawl-state.json"
INDEX_FILE = BASE / "research/vcpedia-pages-index.jsonl"
EXPLORE_FILTER_FILE = BASE / "research/vcpedia-explore-filter.json"

API = "https://markdown.new"
DEFAULT_START_URL = "https://vcpedia.cn/zh-hans/Template:%E6%B4%9B%E5%A4%A9%E4%BE%9D"


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
    p = argparse.ArgumentParser(description="Fetch VCPedia pages via markdown.new")
    p.add_argument("--start-url", default=DEFAULT_START_URL, help="Seed VCPedia URL")
    p.add_argument("--pages-dir", default=str(PAGES_DIR), help="Directory to store per-page markdown files")
    p.add_argument("--state-file", default=str(STATE_FILE), help="Crawler state json path")
    p.add_argument("--index-file", default=str(INDEX_FILE), help="Index manifest jsonl path for RAG")
    p.add_argument(
        "--explore-filter-json",
        default=str(EXPLORE_FILTER_FILE),
        help=(
            "Manual JSON rules for enqueue filtering. Supported: "
            "list[url] (source pages that stop expansion) or "
            "{stop_expand_pages, block_links, block_links_by_source}."
        ),
    )
    p.add_argument(
        "--per-run",
        type=int,
        default=20,
        help="Max pages fetched per run and max parallel workers in crawl mode",
    )
    p.add_argument(
        "--target-total",
        type=int,
        default=500,
        help="Stop fetching when visited pages reach this total (<=0 means no cap)",
    )
    p.add_argument("--retry-base-seconds", type=float, default=30.0, help="Backoff base seconds for failed URLs")
    p.add_argument("--retry-max-seconds", type=float, default=3600.0, help="Backoff max seconds for failed URLs")
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
        default=os.getenv("OPENAI_MODEL", "gpt-5-nano"),
        help="Model name for OpenAI-compatible chat/completions",
    )
    p.add_argument(
        "--llm-qps",
        type=float,
        default=1.0,
        help="AI service rate limit in requests per second (<=0 disables throttling)",
    )
    p.add_argument("--llm-timeout", type=int, default=60, help="LLM request timeout seconds")
    p.add_argument("--summary-context-chars", type=int, default=6000, help="Max source chars sent to LLM summary")
    p.add_argument("--summary-max-tokens", type=int, default=180, help="LLM max_tokens for summary generation")
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


def mdnew_json(url: str, logger: Any, retries: int = 3) -> dict:
    endpoint = f"{API}/{url}?format=json"
    req = urllib.request.Request(endpoint, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    last_err: Exception | None = None
    for i in range(1, retries + 1):
        try:
            logger.debug(f"mdnew request start | attempt={i}/{retries} | url={url} | endpoint={endpoint}")
            with urllib.request.urlopen(req, timeout=180) as r:
                payload = json.loads(r.read().decode("utf-8", errors="ignore"))
                logger.debug(
                    f"mdnew request ok | attempt={i}/{retries} | url={url} | "
                    f"success={payload.get('success')} | keys={sorted(payload.keys())}"
                )
                return payload
        except Exception as e:
            last_err = e
            logger.debug(f"mdnew request failed | attempt={i}/{retries} | url={url} | err={e}")
            time.sleep(0.5 * i)
    if last_err:
        raise last_err
    raise RuntimeError("unreachable")


def clean_url(u: str) -> str:
    u = u.strip().rstrip(").,;!?]>")
    p = urllib.parse.urlparse(u)
    return urllib.parse.urlunparse(("https", "vcpedia.cn", p.path, "", p.query, ""))


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
    enqueue_skipped_by_filter: int = 0
    llm_usage: LlmUsage = field(default_factory=LlmUsage)


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
        temperature=0.2,
        max_tokens=max(16, int(max_tokens)),
        messages=[
            {
                "role": "system",
                "content": "You summarize wiki-like pages into concise Chinese text for JSON fields.",
            },
            {
                "role": "user",
                "content": (
                    "请基于页面内容生成中文摘要，要求：\n"
                    "1) 80-140字；\n"
                    "2) 客观、中性；\n"
                    "3) 不输出项目符号；\n"
                    "4) 不包含\"本文\"\"该页面\"等指代。\n\n"
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
        f"入队过滤: {stats.enqueue_skipped_by_filter}",
        f"LLM prompt_tokens: {stats.llm_usage.prompt_tokens}",
        f"LLM completion_tokens: {stats.llm_usage.completion_tokens}",
        f"LLM total_tokens: {stats.llm_usage.total_tokens}",
    ]
    if error:
        lines.append(f"错误: {error}")
    lines.append(f"时间: {iso_now()}")
    return "\n".join(lines)


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


def load_crawl_state(state_path: Path, start_url: str) -> dict:
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
            "start_url": raw.get("start_url") or clean_url(start_url),
            "queue": queue,
            "visited": visited,
            "failed": failed,
            "failure_meta": failure_meta,
            "created_at": raw.get("created_at") or iso_now(),
            "updated_at": raw.get("updated_at") or iso_now(),
        }

    seed = clean_url(start_url)
    return {
        "start_url": seed,
        "queue": [seed],
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
) -> tuple[set[str], set[str], dict[str, set[str]]]:
    """Load manual enqueue filtering rules from JSON.

    Supported schema:
    1) ["https://vcpedia.cn/..."]
       -> source pages that stop expansion entirely.
    2) {
         "stop_expand_pages": [...],
            "all_blocked": [...],
         "block_links": [...],
         "block_links_by_source": {"<source_url>": ["<target_url>"]}
       }
    """
    if not filter_path.exists():
        logger.debug(f"explore filter missing | file={filter_path}")
        return set(), set(), {}

    raw = json.loads(filter_path.read_text(encoding="utf-8"))
    stop_expand_pages: set[str] = set()
    all_blocked_pages: set[str] = set()
    global_block_links: set[str] = set()
    block_links_by_source: dict[str, set[str]] = {}

    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, str) and item.strip():
                stop_expand_pages.add(clean_url(item))
    elif isinstance(raw, dict):
        for item in raw.get("stop_expand_pages", []):
            if isinstance(item, str) and item.strip():
                stop_expand_pages.add(clean_url(item))
        for item in raw.get("all_blocked", []):
            if isinstance(item, str) and item.strip():
                all_blocked_pages.add(clean_url(item))
        for item in raw.get("block_links", []):
            if isinstance(item, str) and item.strip():
                global_block_links.add(clean_url(item))
        by_source = raw.get("block_links_by_source", {})
        if isinstance(by_source, dict):
            for source, targets in by_source.items():
                if not isinstance(source, str) or not source.strip() or not isinstance(targets, list):
                    continue
                normalized_source = clean_url(source)
                normalized_targets: set[str] = set()
                for target in targets:
                    if isinstance(target, str) and target.strip():
                        normalized_targets.add(clean_url(target))
                if normalized_targets:
                    block_links_by_source[normalized_source] = normalized_targets
    else:
        raise ValueError(f"unsupported explore-filter-json schema: {filter_path}")

    stop_expand_pages.update(all_blocked_pages)

    logger.info(
        f"explore filter loaded | file={filter_path} | stop_expand_pages={len(stop_expand_pages)} "
        f"| all_blocked={len(all_blocked_pages)} | global_block_links={len(global_block_links)} "
        f"| block_links_by_source={len(block_links_by_source)}"
    )
    logger.debug(
        f"explore filter detail | stop_expand_pages={sorted(stop_expand_pages)} | "
        f"global_block_links={sorted(global_block_links)} | "
        f"block_links_by_source={{{', '.join(f'{k}: {sorted(v)}' for k, v in block_links_by_source.items())}}}"
    )
    return stop_expand_pages, global_block_links, block_links_by_source


def run_incremental_crawl_mode(args: argparse.Namespace, logger: Any) -> CrawlRunStats:
    pages_dir = Path(args.pages_dir)
    state_path = Path(args.state_file)
    index_file = Path(args.index_file)
    state = load_crawl_state(state_path, args.start_url)

    queue = list(state["queue"])
    queued = set(queue)
    visited = set(state["visited"])
    failed = set(state["failed"])
    failure_meta: dict[str, dict[str, Any]] = dict(state.get("failure_meta", {}))
    stop_expand_pages, global_block_links, block_links_by_source = load_explore_filter_rules(
        Path(args.explore_filter_json),
        logger,
    )

    if not args.llm_api_key.strip():
        raise RuntimeError("missing --llm-api-key (or OPENAI_API_KEY)")

    llm_client = OpenAI(
        api_key=args.llm_api_key,
        base_url=args.llm_base_url.rstrip("/"),
        timeout=max(1, int(args.llm_timeout)),
    )
    llm_rate_limiter = RequestRateLimiter(args.llm_qps)

    target_total = args.target_total if args.target_total > 0 else None
    if target_total is not None and len(visited) >= target_total:
        logger.info(f"target reached | visited={len(visited)} | target_total={target_total}")
        return CrawlRunStats(queue_size=len(queue), visited_total=len(visited), failed_total=len(failed))

    per_run = max(1, args.per_run)
    retry_base = max(0.1, float(args.retry_base_seconds))
    retry_max = max(retry_base, float(args.retry_max_seconds))
    fetched_this_run = 0
    scanned_this_run = 0
    failed_this_run = 0
    deferred_retry_this_run = 0
    llm_success_count = 0
    llm_fallback_count = 0
    enqueue_skipped_by_filter = 0
    llm_usage = LlmUsage()

    logger.info(
        f"crawl start | queue={len(queue)} | visited={len(visited)} | failed={len(failed)} "
        f"| per_run={per_run} | target_total={target_total if target_total is not None else 'none'}"
    )
    logger.debug(
        f"crawl state loaded | queue_head={queue[:5]} | visited_sample={sorted(list(visited))[:5]} | "
        f"failed_sample={sorted(list(failed))[:5]} | state_file={state_path}"
    )

    while queue and fetched_this_run < per_run:
        if target_total is not None and len(visited) >= target_total:
            break

        remaining_quota = per_run - fetched_this_run
        if target_total is not None:
            remaining_quota = min(remaining_quota, target_total - len(visited))
        if remaining_quota <= 0:
            break

        # Step 1: pick URLs for this batch, skipping entries still in backoff.
        batch_urls: list[str] = []
        inspect_budget = len(queue)
        logger.debug(
            f"batch selection start | queue_size={len(queue)} | inspect_budget={inspect_budget} "
            f"| fetched_this_run={fetched_this_run} | remaining_quota={remaining_quota}"
        )
        for _ in range(inspect_budget):
            if len(batch_urls) >= remaining_quota:
                break
            url = queue.pop(0)
            queued.discard(url)

            if url in visited:
                continue

            meta = failure_meta.get(url) or {}
            next_retry_at = parse_iso_datetime(str(meta.get("next_retry_at") or ""))
            if next_retry_at and datetime.now(timezone.utc) < next_retry_at:
                queue.append(url)
                queued.add(url)
                deferred_retry_this_run += 1
                logger.debug(
                    f"batch selection deferred | url={url} | next_retry_at={next_retry_at.isoformat()}"
                )
                continue

            batch_urls.append(url)
            logger.debug(f"batch selection picked | url={url}")

        if not batch_urls:
            logger.info("all pending URLs are in backoff window, stop this run")
            break

        scanned_this_run += len(batch_urls)
        logger.debug(f"batch ready | batch_size={len(batch_urls)} | batch_urls={batch_urls}")
        # Step 2: fetch markdown.new payloads in parallel (network-bound work).
        fetch_results: dict[str, dict[str, Any] | None] = {}
        fetch_errors: dict[str, str] = {}
        max_workers = max(1, min(per_run, len(batch_urls)))
        logger.debug(f"batch fetch start | max_workers={max_workers} | batch_size={len(batch_urls)}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(mdnew_json, url, logger): url for url in batch_urls}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    fetch_results[url] = future.result()
                    logger.debug(f"batch fetch ok | url={url}")
                except Exception as e:
                    fetch_results[url] = None
                    fetch_errors[url] = str(e)
                    logger.debug(f"batch fetch error | url={url} | err={e}")

        # Step 3: apply crawl side effects sequentially to keep state/index writes predictable.
        for url in batch_urls:
            data = fetch_results.get(url)
            if not data:
                err = fetch_errors.get(url, "unknown_error")
                logger.warning(f"fetch failed | url={url} | err={err}")
                prev_count = int((failure_meta.get(url) or {}).get("count", 0) or 0)
                count = prev_count + 1
                failure_meta[url] = {
                    "count": count,
                    "next_retry_at": compute_next_retry_at(count, retry_base, retry_max),
                    "last_error": err,
                }
                failed.add(url)
                failed_this_run += 1
                continue

            if not data.get("success"):
                logger.debug(f"api returned success=false | url={url} | keys={sorted(data.keys())}")
                prev_count = int((failure_meta.get(url) or {}).get("count", 0) or 0)
                count = prev_count + 1
                failure_meta[url] = {
                    "count": count,
                    "next_retry_at": compute_next_retry_at(count, retry_base, retry_max),
                    "last_error": "api_success_false",
                }
                failed.add(url)
                failed_this_run += 1
                continue

            title = (data.get("title") or "").strip() or page_slug_from_url(url)
            content = data.get("content") or ""
            logger.debug(
                f"page payload ready | url={url} | title={title} | content_chars={len(content)} | "
                f"content_preview={compact_excerpt(content, max_len=200)}"
            )
            try:
                summary, usage = generate_summary_with_llm(
                    title=title,
                    content=content,
                    logger=logger,
                    client=llm_client,
                    limiter=llm_rate_limiter,
                    model=args.llm_model,
                    source_max_chars=args.summary_context_chars,
                    max_tokens=args.summary_max_tokens,
                )
                llm_success_count += 1
                llm_usage.prompt_tokens += usage.prompt_tokens
                llm_usage.completion_tokens += usage.completion_tokens
                llm_usage.total_tokens += usage.total_tokens
            except Exception as e:
                logger.warning(f"llm summary failed | url={url} | err={e} | fallback=short_summary")
                summary = short_summary(content)
                llm_fallback_count += 1
                logger.debug(f"llm fallback summary | url={url} | summary={summary}")
            out_path = write_page_markdown(pages_dir, title, url, summary, content)
            fetched_at = iso_now()
            logger.debug(
                f"page persist start | url={url} | out_path={out_path} | fetched_at={fetched_at} | "
                f"summary_chars={len(summary)}"
            )
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
            failed.discard(url)
            failure_meta.pop(url, None)
            fetched_this_run += 1

            extracted_links = extract_internal_links(content)
            logger.debug(
                f"link extraction done | url={url} | extracted_count={len(extracted_links)} | "
                f"sample={extracted_links[:10]}"
            )
            if url in stop_expand_pages:
                enqueue_skipped_by_filter += len(extracted_links)
                logger.debug(
                    f"expansion blocked by stop_expand_pages/all_blocked | source={url} | "
                    f"blocked_count={len(extracted_links)}"
                )
            else:
                source_block_set = block_links_by_source.get(url, set())
                for link in extracted_links:
                    if link in global_block_links or link in source_block_set:
                        enqueue_skipped_by_filter += 1
                        logger.debug(
                            f"link filtered out | source={url} | target={link} | "
                            f"reason={'global_block_links' if link in global_block_links else 'block_links_by_source'}"
                        )
                        continue
                    if link in visited or link in queued:
                        logger.debug(
                            f"link skipped as duplicate | source={url} | target={link} | "
                            f"visited={link in visited} | queued={link in queued}"
                        )
                        continue
                    queue.append(link)
                    queued.add(link)
                    logger.debug(f"link enqueued | source={url} | target={link} | new_queue_size={len(queue)}")

            logger.info(f"saved page | visited={len(visited)} | path={out_path.name} | url={url}")

        if scanned_this_run % args.log_every == 0:
            logger.info(
                f"progress | scanned_this_run={scanned_this_run} | fetched_this_run={fetched_this_run} "
                f"| queue={len(queue)} | visited={len(visited)} | failed={len(failed)}"
            )

    state["queue"] = queue
    state["visited"] = sorted(visited)
    state["failed"] = sorted(failed)
    state["failure_meta"] = failure_meta
    save_crawl_state(state_path, state)
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
        f"| llm_fallback={llm_fallback_count} | enqueue_filtered={enqueue_skipped_by_filter} "
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
        enqueue_skipped_by_filter=enqueue_skipped_by_filter,
        llm_usage=llm_usage,
    )


def main() -> None:
    args = parse_args()
    logger = setup_logger(args.verbose)
    logger.debug(
        f"runtime config | start_url={args.start_url} | per_run={args.per_run} | target_total={args.target_total} | "
        f"llm_model={args.llm_model} | llm_qps={args.llm_qps} | verbose={args.verbose} | "
        f"filter_json={args.explore_filter_json}"
    )
    stats = CrawlRunStats()
    title = "VCPedia Crawl 完成"
    try:
        stats = run_incremental_crawl_mode(args, logger)
        desp = build_run_summary_message(args, stats, status="成功")
    except Exception as e:
        title = "VCPedia Crawl 失败"
        desp = build_run_summary_message(args, stats, status="失败", error=str(e))
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
