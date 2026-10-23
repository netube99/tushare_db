"""一次维护的日志上下文与事件计数，不持久化调度状态。"""
from collections import Counter
from contextlib import contextmanager
from contextvars import ContextVar
from hashlib import sha256
import json


_context: ContextVar[dict] = ContextVar("maintenance_context", default={})
_counts: ContextVar[Counter | None] = ContextVar("maintenance_counts", default=None)


def safe_params(params: dict) -> dict:
    redacted = {}
    for key, value in params.items():
        if any(part in key.lower() for part in
               ("token", "password", "secret", "authorization", "proxy")):
            value = "[REDACTED]"
        elif isinstance(value, dict):
            value = safe_params(value)
        redacted[key] = value
    return redacted


def task_id(table: str, date_val: str) -> str:
    return sha256(json.dumps([table, date_val]).encode()).hexdigest()[:20]


@contextmanager
def log_context(**fields):
    token = _context.set({**_context.get(), **fields})
    try:
        yield
    finally:
        _context.reset(token)


@contextmanager
def run_context(run_id: str):
    token = _counts.set(Counter())
    try:
        with log_context(run_id=run_id):
            yield
    finally:
        _counts.reset(token)


def context_fields() -> dict:
    return dict(_context.get())


def count(name: str, amount: int = 1) -> None:
    counts = _counts.get()
    if counts is not None:
        counts[name] += amount


def record_event(event: dict) -> None:
    if event.get("event") == "pull":
        if event.get("module") == "client":
            count("cache_hits" if event.get("cache_hit") else "fetched_pages")
        elif "ok" in event:
            count({0: "tasks_failed", 1: "tasks_succeeded", 2: "tasks_empty", 3: "tasks_abandoned"}[event["ok"]])


def run_counts() -> dict:
    counts = _counts.get() or {}
    return {key: counts.get(key, 0) for key in (
        "http_attempts", "fetched_pages", "cache_hits", "tasks_succeeded",
        "tasks_empty", "tasks_failed", "tasks_abandoned", "tasks_cooled_down")}
