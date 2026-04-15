#!/usr/bin/env python3
"""Batch upload local documents to an AstrBot knowledge base.

Examples:
  uv run python scripts/kb_batch_upload.py \
    --base-url http://127.0.0.1:6185 \
    --kb-id YOUR_KB_ID \
    --path ./docs \
    --recursive \
    --username astrbot \
    --password astrbot

  uv run python scripts/kb_batch_upload.py \
    --base-url http://127.0.0.1:6185 \
    --kb-id YOUR_KB_ID \
    --path ./docs ./notes/a.md \
    --token YOUR_JWT_TOKEN
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import time
from pathlib import Path
from typing import Any, Iterable, Sequence

import httpx


DEFAULT_EXTS = {".txt", ".md", ".pdf", ".docx", ".xls", ".xlsx"}
DEFAULT_PROGRESS_FILE = Path("research/vcpedia-kb-upload-state.json")


class UploadError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch upload files to AstrBot knowledge base")

    parser.add_argument("--base-url", default="http://127.0.0.1:6185", help="AstrBot dashboard base URL")
    parser.add_argument("--kb-id", required=True, help="Target knowledge base ID")
    parser.add_argument(
        "--path",
        nargs="+",
        required=True,
        help="One or more files/directories to upload",
    )
    parser.add_argument("--recursive", action="store_true", help="Recursively scan directories")
    parser.add_argument(
        "--ext",
        nargs="*",
        default=sorted(DEFAULT_EXTS),
        help="Allowed extensions, default: .txt .md .pdf .docx .xls .xlsx",
    )

    parser.add_argument("--token", default=os.getenv("ASTRBOT_TOKEN", ""), help="JWT token")
    parser.add_argument("--username", default=os.getenv("ASTRBOT_USERNAME", ""), help="Dashboard username")
    parser.add_argument("--password", default=os.getenv("ASTRBOT_PASSWORD", ""), help="Dashboard password")

    parser.add_argument("--files-per-task", type=int, default=20, help="How many files to include in one upload task")
    parser.add_argument("--poll-interval", type=float, default=1.5, help="Seconds between progress polling")

    parser.add_argument("--chunk-size", type=int, default=512, help="Chunk size")
    parser.add_argument("--chunk-overlap", type=int, default=50, help="Chunk overlap")
    parser.add_argument("--batch-size", type=int, default=32, help="Embedding batch size")
    parser.add_argument("--tasks-limit", type=int, default=3, help="Embedding concurrency")
    parser.add_argument("--max-retries", type=int, default=3, help="Max retries")

    parser.add_argument("--timeout", type=float, default=120.0, help="HTTP request timeout")
    parser.add_argument(
        "--progress-file",
        default=str(DEFAULT_PROGRESS_FILE),
        help="Persistent upload progress json file used to skip already uploaded files",
    )
    return parser.parse_args()


def normalize_exts(raw_exts: Sequence[str]) -> set[str]:
    exts: set[str] = set()
    for ext in raw_exts:
        e = ext.strip().lower()
        if not e:
            continue
        if not e.startswith("."):
            e = f".{e}"
        exts.add(e)
    return exts


def iter_candidate_files(paths: Iterable[str], recursive: bool) -> list[Path]:
    results: list[Path] = []
    for raw in paths:
        p = Path(raw).expanduser().resolve()
        if not p.exists():
            print(f"[WARN] Path does not exist, skip: {p}")
            continue
        if p.is_file():
            results.append(p)
            continue
        if p.is_dir():
            iterator = p.rglob("*") if recursive else p.glob("*")
            for child in iterator:
                if child.is_file():
                    results.append(child.resolve())
            continue
        print(f"[WARN] Unsupported path type, skip: {p}")
    deduped = sorted(set(results))
    return deduped


def filter_files(files: Iterable[Path], allowed_exts: set[str]) -> list[Path]:
    filtered: list[Path] = []
    for p in files:
        if p.suffix.lower() in allowed_exts:
            filtered.append(p)
    return filtered


def chunk_list(items: Sequence[Path], chunk_size: int) -> list[list[Path]]:
    if chunk_size <= 0:
        raise ValueError("files-per-task must be > 0")
    return [list(items[i : i + chunk_size]) for i in range(0, len(items), chunk_size)]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_upload_progress(progress_file: Path) -> dict[str, Any]:
    if not progress_file.exists():
        return {
            "version": 1,
            "updated_at": "",
            "files": {},
        }

    try:
        raw = json.loads(progress_file.read_text(encoding="utf-8"))
    except Exception:
        return {
            "version": 1,
            "updated_at": "",
            "files": {},
        }

    files = raw.get("files") if isinstance(raw, dict) else {}
    if not isinstance(files, dict):
        files = {}

    return {
        "version": int(raw.get("version", 1) or 1) if isinstance(raw, dict) else 1,
        "updated_at": str(raw.get("updated_at") or "") if isinstance(raw, dict) else "",
        "files": files,
    }


def save_upload_progress(progress_file: Path, state: dict[str, Any]) -> None:
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    tmp = progress_file.with_suffix(progress_file.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(progress_file)


def file_state_key(path: Path) -> str:
    return str(path.resolve())


def build_file_fingerprint(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "sha256": sha256_file(path),
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


def unwrap_api_response(resp: httpx.Response) -> dict:
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "ok":
        raise UploadError(data.get("message") or data.get("error") or "Unknown API error")
    return data.get("data") or {}


def login_and_get_token(client: httpx.Client, base_url: str, username: str, password: str) -> str:
    if not username or not password:
        raise UploadError("No token provided; username/password are required for login")
    resp = client.post(
        f"{base_url}/api/auth/login",
        json={"username": username, "password": password},
    )
    data = unwrap_api_response(resp)
    token = data.get("token")
    if not token:
        raise UploadError("Login succeeded but token is missing")
    return token


def start_upload_task(
    client: httpx.Client,
    base_url: str,
    token: str,
    kb_id: str,
    files: list[Path],
    *,
    chunk_size: int,
    chunk_overlap: int,
    batch_size: int,
    tasks_limit: int,
    max_retries: int,
) -> str:
    headers = {"Authorization": f"Bearer {token}"}
    form_data = {
        "kb_id": kb_id,
        "chunk_size": str(chunk_size),
        "chunk_overlap": str(chunk_overlap),
        "batch_size": str(batch_size),
        "tasks_limit": str(tasks_limit),
        "max_retries": str(max_retries),
    }

    multipart_files = []
    file_handlers = []
    try:
        for i, path in enumerate(files):
            fh = path.open("rb")
            file_handlers.append(fh)
            multipart_files.append((f"file{i}", (path.name, fh, "application/octet-stream")))

        resp = client.post(
            f"{base_url}/api/kb/document/upload",
            headers=headers,
            data=form_data,
            files=multipart_files,
        )
        payload = unwrap_api_response(resp)
        task_id = payload.get("task_id")
        if not task_id:
            raise UploadError("Upload task created but task_id is missing")
        return str(task_id)
    finally:
        for fh in file_handlers:
            fh.close()


def poll_upload_task(
    client: httpx.Client,
    base_url: str,
    token: str,
    task_id: str,
    poll_interval: float,
) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    last_line = ""

    while True:
        resp = client.get(
            f"{base_url}/api/kb/document/upload/progress",
            headers=headers,
            params={"task_id": task_id},
        )
        data = unwrap_api_response(resp)

        status = data.get("status", "unknown")
        progress = data.get("progress") or {}

        if status == "processing":
            file_index = int(progress.get("file_index", 0)) + 1
            file_total = int(progress.get("file_total", 0))
            stage = progress.get("stage", "")
            current = float(progress.get("current", 0) or 0)
            total = float(progress.get("total", 100) or 100)
            percent = 0.0 if total <= 0 else max(0.0, min(100.0, (current / total) * 100))
            line = f"[TASK {task_id}] {file_index}/{file_total} stage={stage} {percent:.1f}%"
            if line != last_line:
                print(line)
                last_line = line
            time.sleep(max(poll_interval, 0.2))
            continue

        if status == "completed":
            result = data.get("result") or {}
            print(
                f"[TASK {task_id}] completed: "
                f"success={result.get('success_count', 0)} "
                f"failed={result.get('failed_count', 0)}"
            )
            return result

        if status == "failed":
            raise UploadError(f"Task {task_id} failed: {data.get('error')}")

        time.sleep(max(poll_interval, 0.2))


def build_upload_summary(result: dict[str, Any]) -> str:
    lines = [
        "知识库上传完成",
        f"候选文件: {result.get('total_candidates', result.get('total_files', 0))}",
        f"待上传: {result.get('pending_files', result.get('total_files', 0))}",
        f"已跳过: {result.get('skipped_files', 0)}",
        f"任务数: {result.get('batches', 0)}",
        f"成功: {result.get('total_success', 0)}",
        f"失败: {result.get('total_failed', 0)}",
    ]
    failed_items = result.get("failed_items") or []
    if isinstance(failed_items, list) and failed_items:
        lines.append(f"失败条目: {len(failed_items)}")
    return "\n".join(lines)


def upload_files_to_kb(
    *,
    base_url: str,
    kb_id: str,
    paths: Iterable[str],
    recursive: bool,
    ext: Sequence[str],
    token: str = "",
    username: str = "",
    password: str = "",
    progress_file: str | Path | None = None,
    files_per_task: int = 20,
    poll_interval: float = 1.5,
    chunk_size: int = 512,
    chunk_overlap: int = 50,
    batch_size: int = 32,
    tasks_limit: int = 3,
    max_retries: int = 3,
    timeout: float = 120.0,
) -> dict[str, Any]:
    progress_path = Path(progress_file).expanduser().resolve() if progress_file else None
    allowed_exts = normalize_exts(ext)

    candidates = iter_candidate_files(paths, recursive=recursive)
    files = filter_files(candidates, allowed_exts)

    if not files:
        raise UploadError("No files matched. Check --path and --ext.")

    progress_state = load_upload_progress(progress_path) if progress_path else {"version": 1, "updated_at": "", "files": {}}
    progress_files = progress_state.setdefault("files", {})

    pending_files: list[Path] = []
    skipped_files = 0

    for path in files:
        fingerprint = build_file_fingerprint(path)
        key = file_state_key(path)
        record = progress_files.get(key)
        already_uploaded = (
            isinstance(record, dict)
            and record.get("status") == "success"
            and record.get("sha256") == fingerprint["sha256"]
        )
        if already_uploaded:
            skipped_files += 1
        else:
            pending_files.append(path)

    if progress_path:
        progress_state["version"] = 1
        progress_state.setdefault("files", progress_files)
        save_upload_progress(progress_path, progress_state)

    if not pending_files:
        summary = {
            "total_candidates": len(files),
            "pending_files": 0,
            "skipped_files": skipped_files,
            "batches": 0,
            "total_success": 0,
            "total_failed": 0,
            "failed_items": [],
            "progress_file": str(progress_path) if progress_path else "",
        }
        print(f"Collected {len(files)} files, 0 task(s) to submit.")
        print("All candidate files are already marked as uploaded.")
        print("\n=== Upload Summary ===")
        print(build_upload_summary(summary))
        return summary

    batches = chunk_list(pending_files, files_per_task)

    print(f"Collected {len(files)} files, {len(batches)} task(s) to submit.")
    print(f"Skipping {skipped_files} already uploaded file(s).")
    for idx, batch in enumerate(batches, start=1):
        print(f"  batch {idx}: {len(batch)} files")

    timeout_obj = httpx.Timeout(timeout)
    total_success = 0
    total_failed = 0
    all_failed_items: list[dict[str, Any]] = []
    uploaded_keys: set[str] = set()
    failed_keys: set[str] = set()

    def mark_batch_result(batch_files: list[Path], result: dict[str, Any]) -> None:
        failed_names: set[str] = set()
        failed_items = result.get("failed") or []
        if isinstance(failed_items, list):
            for item in failed_items:
                if isinstance(item, dict):
                    name = item.get("file_name")
                    if isinstance(name, str) and name:
                        failed_names.add(name)

        for path in batch_files:
            stat = path.stat()
            fingerprint = build_file_fingerprint(path)
            key = file_state_key(path)
            record = {
                "key": key,
                "path": str(path),
                "name": path.name,
                "sha256": fingerprint["sha256"],
                "size": int(stat.st_size),
                "mtime_ns": int(stat.st_mtime_ns),
                "last_attempt_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            if path.name in failed_names:
                record["status"] = "failed"
                if isinstance(failed_items, list):
                    matched_error = ""
                    for item in failed_items:
                        if isinstance(item, dict) and item.get("file_name") == path.name:
                            matched_error = str(item.get("error") or "")
                            break
                    if matched_error:
                        record["error"] = matched_error
                progress_files[key] = record
                failed_keys.add(key)
            else:
                record["status"] = "success"
                record["uploaded_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                progress_files[key] = record
                uploaded_keys.add(key)

        if progress_path:
            progress_state["files"] = progress_files
            save_upload_progress(progress_path, progress_state)

    with httpx.Client(timeout=timeout_obj) as client:
        token = token.strip()
        if not token:
            token = login_and_get_token(
                client,
                base_url,
                username.strip(),
                password,
            )
            print("Login succeeded, token acquired.")

        for i, batch in enumerate(batches, start=1):
            print(f"\nSubmitting task {i}/{len(batches)} with {len(batch)} files...")
            task_id = start_upload_task(
                client,
                base_url,
                token,
                kb_id,
                batch,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                batch_size=batch_size,
                tasks_limit=tasks_limit,
                max_retries=max_retries,
            )
            print(f"Task created: {task_id}")

            result = poll_upload_task(
                client,
                base_url,
                token,
                task_id,
                poll_interval,
            )
            succ = int(result.get("success_count", 0) or 0)
            fail = int(result.get("failed_count", 0) or 0)
            total_success += succ
            total_failed += fail
            failed_items = result.get("failed") or []
            if isinstance(failed_items, list):
                all_failed_items.extend(failed_items)
            mark_batch_result(batch, result)

    summary = {
        "total_candidates": len(files),
        "pending_files": len(pending_files),
        "skipped_files": skipped_files,
        "batches": len(batches),
        "total_success": total_success,
        "total_failed": total_failed,
        "failed_items": all_failed_items,
        "uploaded_files": len(uploaded_keys),
        "failed_files": len(failed_keys),
        "progress_file": str(progress_path) if progress_path else "",
    }
    if progress_path:
        progress_state["summary"] = {
            "total_candidates": len(files),
            "pending_files": len(pending_files),
            "skipped_files": skipped_files,
            "batches": len(batches),
            "uploaded_files": len(uploaded_keys),
            "failed_files": len(failed_keys),
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        save_upload_progress(progress_path, progress_state)
    print("\n=== Upload Summary ===")
    print(build_upload_summary(summary))

    if all_failed_items:
        print("\nFailed items:")
        for item in all_failed_items:
            name = item.get("file_name", "<unknown>")
            err = item.get("error", "")
            print(f"- {name}: {err}")

    return summary


def main() -> int:
    args = parse_args()
    result = upload_files_to_kb(
        base_url=args.base_url.rstrip("/"),
        kb_id=args.kb_id,
        paths=args.path,
        recursive=args.recursive,
        ext=args.ext,
        token=args.token,
        username=args.username,
        password=args.password,
        progress_file=args.progress_file,
        files_per_task=args.files_per_task,
        poll_interval=args.poll_interval,
        chunk_size=args.chunk_size,
        chunk_overlap=args.chunk_overlap,
        batch_size=args.batch_size,
        tasks_limit=args.tasks_limit,
        max_retries=args.max_retries,
        timeout=args.timeout,
    )

    return 0 if int(result.get("total_failed", 0) or 0) == 0 else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        raise SystemExit(130)
    except UploadError as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1)
    except httpx.HTTPError as exc:
        print(f"[HTTP ERROR] {exc}")
        raise SystemExit(1)
