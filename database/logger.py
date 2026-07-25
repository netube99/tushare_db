"""JSON Lines 运行日志，按日自动轮转."""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

from database.utils import beijing_now


class JsonLogger:
    """线程安全的 JSON Lines 日志写入器，按日切换文件."""

    def __init__(self, log_dir: str = "logs"):
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._current_date: str = ""
        self._file = None

    def _rotate(self, today: str) -> None:
        if self._file and today == self._current_date:
            return
        if self._file:
            self._file.close()
        path = self._log_dir / f"maintain_{today}.log"
        self._file = open(path, "a", encoding="utf-8")
        self._current_date = today

    def write(self, data: dict[str, Any]) -> None:
        ts = beijing_now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        entry = {"ts": ts, **data}
        line = json.dumps(entry, ensure_ascii=False) + "\n"
        try:
            with self._lock:
                today = beijing_now().strftime("%Y%m%d")
                self._rotate(today)
                self._file.write(line)
                self._file.flush()
        except Exception:
            import sys
            print(line, end="", file=sys.stderr, flush=True)


# 模块级单例，首次调用时初始化
_logger: JsonLogger | None = None


def get_json_logger() -> JsonLogger:
    global _logger
    if _logger is None:
        _logger = JsonLogger()
    return _logger
