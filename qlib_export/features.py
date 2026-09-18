"""全量特征转换引擎 — FeatureSync（全量转换 + 中断续转）."""

import numpy as np
from collections import defaultdict
from pathlib import Path

from database.engine import Connection
from qlib_export.specs import _collect_tushare_cols
from qlib_export.sync_log import (load_synced_fields, upsert_sync_log,
                                  get_partial_records)
from qlib_export.binio import write_bin
from qlib_export.instruments import get_instruments_for_table, qlib_to_ts_code
from qlib_export.calendar import CalendarSync


class FeatureSync:
    """全量转换引擎，支持中断续转."""

    def __init__(self, output_dir: Path, calendar: CalendarSync, conn: Connection,
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
            desired = set(field_names)
            synced_map = load_synced_fields(self.conn, source_table)

            n_total = len(instruments)
            for i, inst in enumerate(instruments):
                total_instruments += 1
                overall_done += 1
                if desired.issubset(synced_map.get(inst, set())):
                    total_skipped += 1
                    continue

                self._cleanup_partial_bins(inst, field_names)

                upsert_sync_log(self.conn, inst, source_table, status="partial",
                                fields=field_names)

                try:
                    row_count, first_date, last_date = self._convert_instrument(
                        inst, table_cfg)

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
                            since: str | None = None) -> tuple[int, str, str]:
        """转换单个 instrument 的一张表的所有字段.

        since 为 None 时取 self.since；传 "" 则不做日期过滤（新 instrument 全量首转）.
        Returns: (row_count, first_date, last_date)，日期取自已拉取的排好序行，
        省去每 instrument 两次 MIN/MAX 全表扫描。
        """
        fields = table_cfg["fields"]

        rows, col_names = self._query_instrument(inst, table_cfg, since=since)

        if not rows:
            return 0, "", ""

        raw_count = len(rows)
        date_idx = col_names.index(table_cfg["date_col"])
        first_date = str(rows[0][date_idx] or "")
        last_date = str(rows[-1][date_idx] or "")

        arrays = self._rows_to_arrays(rows, col_names, table_cfg, fields)

        inst_dir = self.output_dir / "features" / inst.lower()
        inst_dir.mkdir(parents=True, exist_ok=True)

        for j, fdef in enumerate(fields):
            field_name = fdef["bin_name"]
            bin_path = inst_dir / f"{field_name}.day.bin"
            write_bin(bin_path, arrays[j])

        return raw_count, first_date, last_date

    def _inst_where(self, table_cfg: dict, inst: str,
                    date_cond: tuple[str, str] | None = None) -> tuple[str, tuple]:
        """构造 WHERE 子句: instrument 定位 + 可选 (op, value) 日期条件."""
        conds: list[str] = []
        params: list[str] = []
        if table_cfg.get("virtual_inst"):
            flt = table_cfg.get("inst_filter")
            if flt:
                conds.append(flt)
        else:
            conds.append(f'"{table_cfg["inst_col"]}" = ?')
            params.append(qlib_to_ts_code(inst, table_cfg["inst_type"]))
        if date_cond is not None:
            op, val = date_cond
            conds.append(f'"{table_cfg["date_col"]}" {op} ?')
            params.append(val)
        if not conds:
            return "", ()
        return " WHERE " + " AND ".join(conds), tuple(params)

    def _rows_to_arrays(self, rows: list[tuple], col_names: list[str],
                        table_cfg: dict, fields: list[dict]) -> list:
        """按需聚合/编码后对齐日历，返回各字段数组."""
        if table_cfg.get("agg"):
            rows, col_names = self._apply_aggregation(rows, col_names, table_cfg)
        if table_cfg.get("encode"):
            rows, col_names = self._apply_encoding(rows, col_names, table_cfg)
        return self._align_to_calendar(rows, col_names, fields, table_cfg["date_col"])

    def _query_instrument(self, inst: str, table_cfg: dict,
                          since: str | None = None) -> tuple[list[tuple], list[str]]:
        """查询单个 instrument 的原始数据.

        since 语义同 _convert_instrument：None 取 self.since，"" 不过滤.
        """
        if since is None:
            since = self.since
        tushare_cols = _collect_tushare_cols(table_cfg)
        cols_sql = ", ".join(f'"{c}"' for c in tushare_cols)
        where, params = self._inst_where(
            table_cfg, inst, (">=", since) if since else None
        )
        query = (f'SELECT {cols_sql} FROM "{table_cfg["source_table"]}"'
                 f'{where} ORDER BY "{table_cfg["date_col"]}"')
        rows = self.conn.execute(query, params).fetchall()
        return [tuple(r) for r in rows], tushare_cols

    def _apply_aggregation(self, rows: list[tuple], col_names: list[str],
                           table_cfg: dict) -> tuple[list[tuple], list[str]]:
        """对多对一数据进行聚合."""
        agg_type = table_cfg["agg"]

        if agg_type == "split_by_type":
            return self._agg_split_by_type(rows, col_names, table_cfg)
        if agg_type in ("sum_count", "sum_count_weighted", "count"):
            return self._agg_sum_count(rows, col_names, table_cfg)

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
        """按日期分组聚合: COUNT / computed sum / 加权均值 / 列 SUM."""
        date_col = table_cfg["date_col"]
        fields = table_cfg["fields"]
        agg_weighted = table_cfg.get("agg_weighted_cols", {})

        date_idx = col_names.index(date_col)

        col_idx = {c: i for i, c in enumerate(col_names)}

        by_date: dict[str, list[tuple]] = defaultdict(list)
        for row in rows:
            d = row[date_idx]
            by_date[d].append(row)

        result = []
        new_cols = [date_col] + [f["bin_name"] for f in fields]

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
                elif computed == "sum" and fdef.get("agg_sum_col"):
                    sum_idx = col_idx.get(fdef["agg_sum_col"], -1)
                    if sum_idx >= 0:
                        vals.append(sum(float(r[sum_idx]) if r[sum_idx] is not None else 0 for r in group))
                    else:
                        vals.append(np.nan)
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
                if amount_j is None and "amount" in col_names:
                    amount_j = ("raw", col_names.index("amount"))
                if vol_j is None and "vol" in col_names:
                    vol_j = ("raw", col_names.index("vol"))
                if amount_j is not None and vol_j is not None:
                    amount_arr = arrays[amount_j] if isinstance(amount_j, int) \
                        else self._raw_col_to_calendar(rows, amount_j[1], date_idx, n_cal)
                    vol_arr = arrays[vol_j] if isinstance(vol_j, int) \
                        else self._raw_col_to_calendar(rows, vol_j[1], date_idx, n_cal)
                    mask = (vol_arr > 0) & (~np.isnan(amount_arr))
                    arrays[j][mask] = amount_arr[mask] * 10.0 / vol_arr[mask]

        return arrays

    def _raw_col_to_calendar(self, rows, col_idx, date_idx, n_cal):
        arr = np.full(n_cal, np.nan, dtype=np.float32)
        for row in rows:
            date_val = row[date_idx]
            if not date_val:
                continue
            idx = self.calendar.date_to_index(str(date_val))
            if idx is None:
                continue
            val = row[col_idx]
            if val is not None:
                try:
                    arr[idx] = float(val)
                except (ValueError, TypeError):
                    pass
        return arr

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
        date_col = table_cfg["date_col"]
        where, params = self._inst_where(table_cfg, inst)
        row = self.conn.execute(
            f'SELECT {agg_fn}("{date_col}") FROM "{table_cfg["source_table"]}"{where}',
            params,
        ).fetchone()
        return row[0] if row and row[0] else ""

    def resume_check(self) -> list[dict]:
        """检查是否有中断任务需要续转."""
        return get_partial_records(self.conn)
