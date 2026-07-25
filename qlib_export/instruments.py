"""品种清单管理 — InstrumentSync + IndexConstituentSync + get_instruments_for_table."""

import sqlite3
from pathlib import Path

from qlib_export.calendar import format_date as _format_date_raw
from qlib_export.specs import INSTRUMENT_SOURCES, VIRTUAL_INSTRUMENTS


def format_date(raw: str) -> str:
    """YYYYMMDD → YYYY-MM-DD，空/过短返回 ""."""
    if not raw or len(raw) < 8:
        return ""
    return _format_date_raw(raw)


def ts_code_to_qlib_instrument(ts_code: str, inst_type: str = "stock") -> str:
    """Tushare ts_code → qlib instrument 名称.

    通用规则: 000001.SZ → SZ000001
    例外: 申万行业指数 (sw_sector) 保持原样 (801010.SI)
    """
    if inst_type == "sw_sector":
        return ts_code.upper()
    if inst_type == "market":
        return ts_code
    parts = ts_code.split(".")
    if len(parts) == 2:
        code, exchange = parts
        return f"{exchange}{code}".upper()
    return ts_code.upper()


def qlib_to_ts_code(inst: str, inst_type: str) -> str:
    """qlib instrument → tushare ts_code."""
    if inst_type == "sw_sector":
        return inst.upper()
    if inst.startswith("_"):
        return inst
    exchange = inst[:2].upper()
    code = inst[2:]
    return f"{code}.{exchange}"


def _query_delisted_stock_ranges(conn: sqlite3.Connection) -> list[tuple]:
    """从 stk_factor_pro 查所有 ts_code 的日期范围（含退市股）."""
    return conn.execute(
        "SELECT ts_code, MIN(trade_date), MAX(trade_date) "
        "FROM stk_factor_pro WHERE ts_code LIKE '%.SZ' OR ts_code LIKE '%.SH' OR ts_code LIKE '%.BJ' "
        "GROUP BY ts_code"
    ).fetchall()


INDEX_CONSTITUENT_MAP = {
    "000300.SH": "csi300",
    "000905.SH": "csi500",
    "000852.SH": "csi1000",
    "000985.SH": "csiall",
    "399006.SZ": "chinext",
    "000906.SH": "csi800",
    "399303.SZ": "gz2000",
}


class InstrumentSync:
    """维护 instruments/all.txt."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir

    def full_init(self, conn: sqlite3.Connection) -> None:
        """全量构建 instruments/all.txt."""
        entries = []

        for inst_type, src in INSTRUMENT_SOURCES.items():
            if src is None:
                continue
            table = src["table"]
            code_col = src["code_col"]
            list_col = src["list_col"]
            delist_col = src["delist_col"]
            range_mode = src.get("range_mode")

            if range_mode == "minmax":
                rows = conn.execute(
                    f'SELECT "{code_col}", MIN("{list_col}"), MAX("{list_col}") '
                    f'FROM "{table}" GROUP BY "{code_col}"'
                ).fetchall()
            else:
                cols = f'"{code_col}"'
                if list_col:
                    cols += f', "{list_col}"'
                if delist_col:
                    cols += f', "{delist_col}"'
                else:
                    cols += ', NULL'
                rows = conn.execute(f"SELECT {cols} FROM \"{table}\"").fetchall()

            for row in rows:
                code = row[0]
                if not code:
                    continue
                inst = ts_code_to_qlib_instrument(code, inst_type)
                start = format_date(row[1]) if len(row) > 1 and row[1] else ""
                end = format_date(row[2]) if len(row) > 2 and row[2] else ""
                entries.append((inst, start, end))

        for inst, (table, date_col) in VIRTUAL_INSTRUMENTS.items():
            row = conn.execute(
                f'SELECT MIN("{date_col}"), MAX("{date_col}") FROM "{table}"'
            ).fetchone()
            start = format_date(row[0]) if row[0] else ""
            end = format_date(row[1]) if row[1] else ""
            entries.append((inst, start, end))

        existing_insts = {e[0] for e in entries}
        for row in _query_delisted_stock_ranges(conn):
            code = row[0]
            if not code:
                continue
            inst = ts_code_to_qlib_instrument(code, "stock")
            if inst not in existing_insts:
                start = format_date(row[1])
                end = format_date(row[2])
                entries.append((inst, start, end))

        self._write(entries)

    def _write(self, entries: list[tuple[str, str, str]]) -> None:
        path = self.output_dir / "instruments" / "all.txt"
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = []
        for inst, start, end in entries:
            if not end:
                end = "2099-12-31"
            if not start:
                start = "1990-01-01"
            lines.append(f"{inst}\t{start}\t{end}")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class IndexConstituentSync:
    """维护 instruments/<name>.txt — 各指数成分股存续期清单.

    每行格式: INSTRUMENT<TAB>START<TAB>END
    同一 instrument 多次调入调出 → 多行，每行一段存续期。
    """

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.instruments_dir = output_dir / "instruments"

    def full_sync(self, conn: sqlite3.Connection) -> None:
        """全量生成所有指数的成分股 TSV 文件."""
        self.instruments_dir.mkdir(parents=True, exist_ok=True)

        for index_code, name in INDEX_CONSTITUENT_MAP.items():
            rows = conn.execute(
                """SELECT con_code, trade_date FROM "index_weight"
                   WHERE index_code = ? ORDER BY con_code, trade_date""",
                (index_code,),
            ).fetchall()

            if not rows:
                continue

            max_date = max(r[1] for r in rows)

            entries = []
            current_code = None
            current_dates = []
            for con_code, trade_date in rows:
                if con_code != current_code:
                    if current_dates:
                        entries.extend(
                            self._build_periods(current_code, current_dates, max_date)
                        )
                    current_code = con_code
                    current_dates = [trade_date]
                else:
                    current_dates.append(trade_date)
            if current_dates:
                entries.extend(
                    self._build_periods(current_code, current_dates, max_date)
                )

            self._write_file(name, entries)

    def _build_periods(
        self, con_code: str, dates: list[str], max_date: str
    ) -> list[tuple[str, str, str]]:
        """从排序后的日期列表检测连续存续期，返回 [(inst, start, end), ...]."""
        inst = ts_code_to_qlib_instrument(con_code, "stock")
        periods = []
        period_start = dates[0]
        prev = dates[0]

        for d in dates[1:]:
            if self._days_between(prev, d) > 40:
                end = format_date(prev)
                if end == format_date(max_date):
                    end = "2099-12-31"
                periods.append((inst, format_date(period_start), end))
                period_start = d
            prev = d

        end = format_date(prev)
        if end == format_date(max_date):
            end = "2099-12-31"
        periods.append((inst, format_date(period_start), end))
        return periods

    @staticmethod
    def _days_between(d1: str, d2: str) -> int:
        """计算两个 YYYYMMDD 之间的天数."""
        from datetime import datetime
        dt1 = datetime(int(d1[:4]), int(d1[4:6]), int(d1[6:8]))
        dt2 = datetime(int(d2[:4]), int(d2[4:6]), int(d2[6:8]))
        return (dt2 - dt1).days

    def _write_file(
        self, name: str, entries: list[tuple[str, str, str]]
    ) -> None:
        """写单个指数成分股 TSV 文件."""
        sorted_entries = sorted(entries, key=lambda x: (x[0], x[1]))
        lines = []
        for inst, start, end in sorted_entries:
            if not end:
                end = "2099-12-31"
            if not start:
                start = "1990-01-01"
            lines.append(f"{inst}\t{start}\t{end}")

        path = self.instruments_dir / f"{name}.txt"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def get_instruments_for_table(conn: sqlite3.Connection, table_cfg: dict) -> list[str]:
    """返回某表应处理的所有 qlib instrument 名称."""
    if table_cfg.get("virtual_inst"):
        return [table_cfg["virtual_inst"]]

    inst_type = table_cfg["inst_type"]
    src = INSTRUMENT_SOURCES.get(inst_type)
    if src is None:
        return []

    table = src["table"]
    code_col = src["code_col"]

    rows = conn.execute(
        f'SELECT DISTINCT "{code_col}" FROM "{table}"'
    ).fetchall()

    instruments = [ts_code_to_qlib_instrument(r[0], inst_type) for r in rows if r[0]]

    if inst_type == "stock":
        existing = set(instruments)
        for code, *_ in _query_delisted_stock_ranges(conn):
            if not code:
                continue
            inst = ts_code_to_qlib_instrument(code, inst_type)
            if inst not in existing:
                instruments.append(inst)
                existing.add(inst)

    return instruments
