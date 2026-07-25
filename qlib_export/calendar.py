"""交易日历管理 — CalendarSync + date_to_cal_index."""

import bisect
import sqlite3
from pathlib import Path


def format_date(raw: str) -> str:
    """YYYYMMDD → YYYY-MM-DD（取前 8 位；不足 8 位原样返回）."""
    if len(raw) >= 8:
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    return raw


def date_to_cal_index(date_str: str, calendar: list[str]) -> int | None:
    """将 YYYYMMDD 映射到日历索引 (forward-fill).

    如果 date_str 不在日历中（非交易日/公告日），
    使用 bisect_left 找到最近的 >= 交易日（forward-fill）。
    """
    if not date_str or len(date_str) < 8:
        return None
    target = format_date(date_str)
    if not calendar or target < calendar[0]:
        return None
    idx = bisect.bisect_left(calendar, target)
    if idx >= len(calendar):
        return None
    return idx


class CalendarSync:
    """维护 calendars/day.txt，提供日期→索引映射."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.calendar: list[str] = []
        self._index: dict[str, int] = {}

    def load_old_calendar(self) -> list[str] | None:
        """加载旧日历内容（用于检测非追加变更）."""
        path = self.output_dir / "calendars" / "day.txt"
        if not path.exists():
            return None
        return path.read_text(encoding="utf-8").strip().splitlines()

    def full_init(self, conn: sqlite3.Connection) -> None:
        """从 trade_cal 全量构建日历."""
        rows = conn.execute(
            "SELECT cal_date FROM trade_cal WHERE exchange='SSE' AND is_open=1 ORDER BY cal_date"
        ).fetchall()
        self.calendar = [format_date(r[0]) for r in rows]
        self._build_index()
        self._write()

    def _build_index(self) -> None:
        """构建日期→索引映射."""
        self._index = {d: i for i, d in enumerate(self.calendar)}

    def _write(self) -> None:
        """写入 calendars/day.txt."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        path = self.output_dir / "calendars" / "day.txt"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(self.calendar) + "\n", encoding="utf-8")

    def load(self) -> None:
        """从已有 day.txt 加载日历."""
        path = self.output_dir / "calendars" / "day.txt"
        if not path.exists():
            return
        self.calendar = path.read_text(encoding="utf-8").strip().splitlines()
        self._build_index()

    def date_to_index(self, date_str: str) -> int | None:
        """将 YYYYMMDD 映射到日历索引 (forward-fill)."""
        return date_to_cal_index(date_str, self.calendar)

    @property
    def calendar_range(self) -> tuple[str, str] | None:
        """返回日历范围."""
        if not self.calendar:
            return None
        return (self.calendar[0], self.calendar[-1])

    @property
    def n_days(self) -> int:
        return len(self.calendar)
