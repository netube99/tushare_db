"""全量特征转换引擎 — FeatureSync（全量转换 + 中断续转）."""

import numpy as np
import sqlite3
from collections import defaultdict
from pathlib import Path

from qlib_export.specs import _collect_tushare_cols
from qlib_export.sync_log import is_synced, upsert_sync_log, get_partial_records
from qlib_export.binio import write_bin
from qlib_export.instruments import get_instruments_for_table, qlib_to_ts_code
from qlib_export.calendar import CalendarSync


class FeatureSync:
    """全量转换引擎，支持中断续转."""

    def __init__(self, output_dir: Path, calendar: CalendarSync, conn: sqlite3.Connection,
                 since: str | None = None):
        self.output_dir = output_dir
        self.calendar = calendar
        self.conn = conn
        self.since = since

    def full_convert(self, conversion_tables: list[dict], quiet: bool = False) -> dict:
        """全量转换所有表.

        Returns:
            stats: {"total_instruments": N, "total_written": N, "total_skipped": N}
        """
        total_instruments = 0
        total_written = 0
        total_skipped = 0

        overall_total = sum(len(get_instruments_for_table(self.conn, t)) for t in conversion_tables)
        overall_done = 0

        for table_cfg in conversion_tables:
            source_table = table_cfg["source_table"]
            instruments = get_instruments_for_table(self.conn, table_cfg)
            field_names = [f["bin_name"] for f in table_cfg["fields"]]

            n_total = len(instruments)
            for i, inst in enumerate(instruments):
                total_instruments += 1
                overall_done += 1
                if is_synced(self.conn, inst, source_table, field_names):
                    total_skipped += 1
                    continue

                self._cleanup_partial_bins(inst, field_names)

                upsert_sync_log(self.conn, inst, source_table, status="partial",
                                fields=field_names)

                try:
                    row_count = self._convert_instrument(inst, table_cfg)
                    last_date = self._get_date_bound(inst, table_cfg, "MAX")
                    first_date = self._get_date_bound(inst, table_cfg, "MIN")

                    upsert_sync_log(self.conn, inst, source_table,
                                    status="done", last_date=last_date,
                                    first_date=first_date,
                                    row_count=row_count, fields=field_names)
                    total_written += 1
                except Exception as e:
                    upsert_sync_log(self.conn, inst, source_table,
                                    status="error", error_msg=str(e),
                                    fields=field_names)
                    if not quiet:
                        print(f"  [ERROR] {inst} ← {source_table}: {e}")

                if not quiet and (i + 1) % 100 == 0:
                    print(f"  [{source_table}] {i+1}/{n_total}  overall {overall_done}/{overall_total}")

        from database.logger import get_json_logger
        get_json_logger().write({
            "level": "INFO", "module": "convert_to_qlib", "event": "convert",
            "mode": "full",
            "total_instruments": total_instruments,
            "total_written": total_written,
            "total_skipped": total_skipped,
        })

        return {
            "total_instruments": total_instruments,
            "total_written": total_written,
            "total_skipped": total_skipped,
        }

    def _convert_instrument(self, inst: str, table_cfg: dict,
                            since: str | None = None) -> int:
        """转换单个 instrument 的一张表的所有字段.

        since 为 None 时取 self.since；传 "" 则不做日期过滤（新 instrument 全量首转）.
        """
        source_table = table_cfg["source_table"]
        inst_col = table_cfg["inst_col"]
        date_col = table_cfg["date_col"]
        fields = table_cfg["fields"]
        agg = table_cfg.get("agg")
        encode = table_cfg.get("encode")

        rows, col_names = self._query_instrument(inst, table_cfg, since=since)

        if not rows:
            return 0

        if agg:
            rows, col_names = self._apply_aggregation(rows, col_names, table_cfg)

        if encode:
            rows, col_names = self._apply_encoding(rows, col_names, table_cfg)

        arrays = self._align_to_calendar(rows, col_names, fields, date_col)

        inst_dir = self.output_dir / "features" / inst.lower()
        inst_dir.mkdir(parents=True, exist_ok=True)

        for j, fdef in enumerate(fields):
            field_name = fdef["bin_name"]
            bin_path = inst_dir / f"{field_name}.day.bin"
            write_bin(bin_path, arrays[j])

        return len(rows)

    def _query_instrument(self, inst: str, table_cfg: dict,
                          since: str | None = None) -> tuple[list[tuple], list[str]]:
        """查询单个 instrument 的原始数据.

        since 语义同 _convert_instrument：None 取 self.since，"" 不过滤.
        """
        source_table = table_cfg["source_table"]
        inst_col = table_cfg["inst_col"]
        date_col = table_cfg["date_col"]
        virtual_inst = table_cfg.get("virtual_inst")
        inst_filter = table_cfg.get("inst_filter")

        tushare_cols = _collect_tushare_cols(table_cfg)
        cols_sql = ", ".join(f'"{c}"' for c in tushare_cols)

        if since is None:
            since = self.since
        date_filter = ""
        date_params: tuple = ()
        if since:
            date_filter = f' AND "{date_col}" >= ?'
            date_params = (since,)

        if virtual_inst:
            if inst_filter:
                query = (f'SELECT {cols_sql} FROM "{source_table}"'
                         f' WHERE {inst_filter}{date_filter} ORDER BY "{date_col}"')
            else:
                query = (f'SELECT {cols_sql} FROM "{source_table}"'
                         f' WHERE 1=1{date_filter} ORDER BY "{date_col}"')
            rows = self.conn.execute(query, date_params).fetchall()
        else:
            ts_code = qlib_to_ts_code(inst, table_cfg["inst_type"])
            query = (f'SELECT {cols_sql} FROM "{source_table}"'
                     f' WHERE "{inst_col}" = ?{date_filter} ORDER BY "{date_col}"')
            rows = self.conn.execute(query, (ts_code,) + date_params).fetchall()

        return [tuple(r) for r in rows], tushare_cols

    def _apply_aggregation(self, rows: list[tuple], col_names: list[str],
                           table_cfg: dict) -> tuple[list[tuple], list[str]]:
        """对多对一数据进行聚合."""
        agg_type = table_cfg["agg"]

        if agg_type == "split_by_type":
            return self._agg_split_by_type(rows, col_names, table_cfg)
        elif agg_type in ("sum_count", "sum_count_weighted"):
            return self._agg_sum_count(rows, col_names, table_cfg)
        elif agg_type == "count":
            return self._agg_count(rows, col_names, table_cfg)

        return rows, col_names

    def _agg_split_by_type(self, rows, col_names, table_cfg):
        """按 type 列拆分为多个字段 (stock_hsgt)."""
        date_col = table_cfg["date_col"]
        fields = table_cfg["fields"]
        agg_col = table_cfg.get("agg_col", "type")

        date_idx = col_names.index(date_col)
        type_idx = col_names.index(agg_col) if agg_col in col_names else -1

        by_date: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for row in rows:
            d = row[date_idx]
            t = str(row[type_idx]) if type_idx >= 0 else ""
            by_date[d][t] += 1

        HSGT_TYPE_MAP = {"HK_SH": "SH", "HK_SZ": "SZ", "1": "SH", "2": "SZ"}
        result = []
        for d in sorted(by_date.keys()):
            counts = by_date[d]
            mapped = defaultdict(int)
            for raw_type, cnt in counts.items():
                mapped[HSGT_TYPE_MAP.get(raw_type, raw_type)] += cnt
            vals = [d]
            for fdef in fields:
                computed = fdef.get("computed", "")
                if computed == "type_SH":
                    vals.append(1 if mapped.get("SH", 0) > 0 else 0)
                elif computed == "type_SZ":
                    vals.append(1 if mapped.get("SZ", 0) > 0 else 0)
                elif computed == "count":
                    vals.append(sum(mapped.values()))
                else:
                    vals.append(0)
            result.append(tuple(vals))

        new_cols = [date_col] + [f["bin_name"] for f in fields]
        return result, new_cols

    def _agg_sum_count(self, rows, col_names, table_cfg):
        """对数值列 SUM + COUNT 聚合."""
        date_col = table_cfg["date_col"]
        fields = table_cfg["fields"]
        agg_weighted = table_cfg.get("agg_weighted_cols", {})
        agg_count_col = table_cfg.get("agg_count_col", "")

        date_idx = col_names.index(date_col)

        col_idx = {c: i for i, c in enumerate(col_names)}

        by_date = defaultdict(list)
        for row in rows:
            d = row[date_idx]
            by_date[d].append(row)

        result = []
        new_cols = [date_col]

        for fdef in fields:
            new_cols.append(fdef["bin_name"])

        for d in sorted(by_date.keys()):
            group = by_date[d]
            vals = [d]
            for fdef in fields:
                tcol = fdef.get("tushare_col")
                computed = fdef.get("computed")
                is_weighted = fdef.get("agg_weighted")
                weight_col = fdef.get("weight_col")

                if computed == "count":
                    vals.append(len(group))
                elif is_weighted and tcol and weight_col in col_idx:
                    w_idx = col_idx[weight_col]
                    tc_idx = col_idx[tcol]
                    total_w = sum(abs(float(r[w_idx]) if r[w_idx] is not None else 0) for r in group)
                    if total_w > 0:
                        weighted_sum = sum(
                            (float(r[tc_idx]) if r[tc_idx] is not None else 0) *
                            abs(float(r[w_idx]) if r[w_idx] is not None else 0)
                            for r in group
                        )
                        vals.append(weighted_sum / total_w)
                    else:
                        vals.append(np.nan)
                elif tcol and tcol in col_idx:
                    tc_idx = col_idx[tcol]
                    s = sum(float(r[tc_idx]) if r[tc_idx] is not None else 0 for r in group)
                    vals.append(s)
                else:
                    vals.append(np.nan)
            result.append(tuple(vals))

        return result, new_cols

    def _agg_count(self, rows, col_names, table_cfg):
        """对 COUNT 聚合（kpl_concept_cons）."""
        date_col = table_cfg["date_col"]
        fields = table_cfg["fields"]
        date_idx = col_names.index(date_col)

        by_date = defaultdict(list)
        for row in rows:
            d = row[date_idx]
            by_date[d].append(row)

        result = []
        new_cols = [date_col] + [f["bin_name"] for f in fields]

        for d in sorted(by_date.keys()):
            group = by_date[d]
            vals = [d]
            for fdef in fields:
                computed = fdef.get("computed")
                agg_sum_col = fdef.get("agg_sum_col")
                if computed == "count":
                    vals.append(len(group))
                elif computed == "sum" and agg_sum_col:
                    sum_idx = col_names.index(agg_sum_col) if agg_sum_col in col_names else -1
                    if sum_idx >= 0:
                        vals.append(sum(float(r[sum_idx]) if r[sum_idx] is not None else 0 for r in group))
                    else:
                        vals.append(np.nan)
                else:
                    vals.append(np.nan)
            result.append(tuple(vals))

        return result, new_cols

    def _apply_encoding(self, rows, col_names, table_cfg):
        """文本列编码为 0/1 (stock_st)."""
        encode_type = table_cfg["encode"]
        date_col = table_cfg["date_col"]
        fields = table_cfg["fields"]
        encode_col = table_cfg.get("encode_col", "type")

        date_idx = col_names.index(date_col)
        type_idx = col_names.index(encode_col) if encode_col in col_names else -1

        result = []
        new_cols = [date_col] + [f["bin_name"] for f in fields]

        for row in rows:
            vals = [row[date_idx]]
            for fdef in fields:
                computed = fdef.get("computed")
                if computed == "encode_is_st" and type_idx >= 0:
                    t = str(row[type_idx]).upper() if row[type_idx] else ""
                    vals.append(1 if "S" in t else 0)
                else:
                    vals.append(0)
            result.append(tuple(vals))

        return result, new_cols

    def _align_to_calendar(self, rows, col_names, fields, date_col):
        """将 DB 行转为按日历索引对齐的 float32 数组."""
        n_cal = len(self.calendar.calendar)
        n_fields = len(fields)

        arrays = [np.full(n_cal, np.nan, dtype=np.float32) for _ in range(n_fields)]

        date_idx = col_names.index(date_col)

        name_to_idx = {c: i for i, c in enumerate(col_names)}

        for row in rows:
            date_val = row[date_idx]
            if not date_val:
                continue
            idx = self.calendar.date_to_index(str(date_val))
            if idx is None:
                continue

            for j, fdef in enumerate(fields):
                computed = fdef.get("computed")
                tcol = fdef.get("tushare_col")

                if computed:
                    if fdef["bin_name"] in col_names:
                        c_idx = col_names.index(fdef["bin_name"])
                        val = row[c_idx]
                    else:
                        continue
                elif tcol and tcol in name_to_idx:
                    val = row[name_to_idx[tcol]]
                elif fdef["bin_name"] in name_to_idx:
                    val = row[name_to_idx[fdef["bin_name"]]]
                else:
                    continue

                if val is not None:
                    try:
                        arrays[j][idx] = float(val)
                    except (ValueError, TypeError):
                        pass

        for j, fdef in enumerate(fields):
            computed = fdef.get("computed", "")
            if computed == "vwap":
                amount_j = vol_j = None
                for k, f2 in enumerate(fields):
                    if f2.get("tushare_col") == "amount":
                        amount_j = k
                    if f2.get("tushare_col") == "vol":
                        vol_j = k
                if amount_j is not None and vol_j is not None:
                    mask = (arrays[vol_j] > 0) & (~np.isnan(arrays[amount_j]))
                    arrays[j][mask] = arrays[amount_j][mask] * 10.0 / arrays[vol_j][mask]

        return arrays

    def _cleanup_partial_bins(self, inst: str, field_names: list[str]) -> None:
        """清除中断残留的 bin 文件."""
        inst_dir = self.output_dir / "features" / inst.lower()
        if not inst_dir.exists():
            return
        for fname in field_names:
            bin_path = inst_dir / f"{fname}.day.bin"
            if bin_path.exists():
                bin_path.unlink()

    def _get_date_bound(self, inst: str, table_cfg: dict, agg_fn: str) -> str:
        """获取该 instrument 数据的日期边界（agg_fn = "MAX" 或 "MIN"）."""
        source_table = table_cfg["source_table"]
        date_col = table_cfg["date_col"]
        virtual_inst = table_cfg.get("virtual_inst")
        inst_filter = table_cfg.get("inst_filter")

        if virtual_inst:
            if inst_filter:
                query = f'SELECT {agg_fn}("{date_col}") FROM "{source_table}" WHERE {inst_filter}'
            else:
                query = f'SELECT {agg_fn}("{date_col}") FROM "{source_table}"'
            row = self.conn.execute(query).fetchone()
        else:
            ts_code = qlib_to_ts_code(inst, table_cfg["inst_type"])
            inst_col = table_cfg["inst_col"]
            row = self.conn.execute(
                f'SELECT {agg_fn}("{date_col}") FROM "{source_table}" WHERE "{inst_col}" = ?',
                (ts_code,)
            ).fetchone()

        return row[0] if row and row[0] else ""

    def resume_check(self) -> list[dict]:
        """检查是否有中断任务需要续转."""
        return get_partial_records(self.conn)
