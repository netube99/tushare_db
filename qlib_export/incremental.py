"""增量同步引擎 — IncrementalSync + FieldRebuilder."""

import json
import sqlite3
from pathlib import Path

from qlib_export.specs import _collect_tushare_cols
from qlib_export.sync_log import upsert_sync_log, get_sync_records
from qlib_export.binio import write_bin, append_bin
from qlib_export.instruments import (
    get_instruments_for_table, InstrumentSync,
)
from qlib_export.calendar import CalendarSync
from qlib_export.features import FeatureSync


class IncrementalSync:
    """每日增量追加引擎."""

    def __init__(self, output_dir: Path, calendar: CalendarSync,
                 conn: sqlite3.Connection, feature_sync: FeatureSync):
        self.output_dir = output_dir
        self.calendar = calendar
        self.conn = conn
        self.feature_sync = feature_sync

    def daily_sync(self, conversion_tables: list[dict], quiet: bool = False,
                   old_calendar: list[str] | None = None) -> dict:
        """每日增量同步.

        Returns:
            stats: {"new_instruments": N, "updated_records": N}
        """
        stats = {"new_instruments": 0, "updated_records": 0}

        if old_calendar is None:
            old_calendar = self.calendar.load_old_calendar()

        self.calendar.full_init(self.conn)

        if old_calendar and self.calendar.calendar:
            if (len(old_calendar) > len(self.calendar.calendar)
                    or self.calendar.calendar[:len(old_calendar)] != old_calendar):
                print("  [WARNING] 日历发生非追加变更（历史交易日被修改），建议 --reset 全量重转")

        InstrumentSync(self.output_dir).full_init(self.conn)

        for table_cfg in conversion_tables:
            source_table = table_cfg["source_table"]
            desired_field_names = [f["bin_name"] for f in table_cfg["fields"]]

            records = get_sync_records(self.conn, source_table)
            existing_insts = {r["instrument"] for r in records}
            all_insts = get_instruments_for_table(self.conn, table_cfg)

            for i, inst in enumerate(all_insts):
                if inst not in existing_insts:
                    # 全量首转必须忽略 since（--since 仅供全量调试截断用）
                    self._convert_and_log(inst, table_cfg, source_table,
                                          desired_field_names, stats,
                                          "new_instruments", "new", quiet)
                if not quiet and (i + 1) % 100 == 0:
                    print(f"  [{source_table}] scan {i+1}/{len(all_insts)}")

            for j, record in enumerate(records):
                if not quiet and (j + 1) % 100 == 0:
                    print(f"  [{source_table}] sync {j+1}/{len(records)}")
                self._sync_record(record, table_cfg, desired_field_names,
                                  stats, quiet)

        from database.logger import get_json_logger
        get_json_logger().write({
            "level": "INFO", "module": "convert_to_qlib", "event": "convert",
            "mode": "daily",
            "total_cal_days": self.calendar.n_days,
            "new_instruments": stats["new_instruments"],
            "updated_records": stats["updated_records"],
        })

        return stats

    def _sync_record(self, record: dict, table_cfg: dict,
                     desired_field_names: list[str], stats: dict,
                     quiet: bool) -> None:
        """处理单条已同步记录: 字段回填 / 缺口检测 / 增量追加."""
        source_table = table_cfg["source_table"]
        inst = record["instrument"]
        last_date = record["last_date"]
        first_date = record.get("first_date", "")
        synced_fields = set(json.loads(record["fields_json"]))

        new_fields_set = set(desired_field_names) - synced_fields
        if new_fields_set:
            self._backfill_fields(inst, table_cfg, list(new_fields_set))
            synced_fields = set(desired_field_names)
            upsert_sync_log(self.conn, inst, source_table,
                            status="done", last_date=last_date,
                            first_date=first_date,
                            row_count=record.get("row_count") or 0,
                            fields=list(synced_fields))

        reason = self._backfill_reason(record, inst, table_cfg,
                                       first_date, last_date)
        if reason:
            if not quiet:
                print(f"  [backfill] {inst} ← {source_table}: {reason}")
            self._convert_and_log(inst, table_cfg, source_table,
                                  desired_field_names, stats,
                                  "updated_records", "reconvert", quiet,
                                  cleanup=True)
            return

        rows, col_names = self._query_rows(inst, table_cfg,
                                           set(synced_fields), last_date)
        if not rows:
            return

        arrays = self.feature_sync._rows_to_arrays(
            rows, col_names, table_cfg, table_cfg["fields"]
        )

        date_col = table_cfg["date_col"]
        inst_dir = self.output_dir / "features" / inst.lower()
        for j, fdef in enumerate(table_cfg["fields"]):
            fname = fdef["bin_name"]
            if fname not in synced_fields:
                continue
            append_bin(inst_dir / f"{fname}.day.bin", arrays[j])

        new_last = rows[-1][col_names.index(date_col)]
        new_row_count = (record.get("row_count") or 0) + len(rows)
        upsert_sync_log(self.conn, inst, source_table,
                        status="done", last_date=str(new_last),
                        first_date=first_date,
                        row_count=new_row_count,
                        fields=list(synced_fields))
        stats["updated_records"] += 1

    def _backfill_reason(self, record: dict, inst: str, table_cfg: dict,
                         first_date: str, last_date: str) -> str | None:
        """检测需要全量重转的情形，返回原因描述或 None."""
        current_first = self.feature_sync._get_date_bound(inst, table_cfg, "MIN")
        if first_date and current_first and current_first < first_date:
            return f"first_date {first_date} → {current_first}"

        stored_count = record.get("row_count") or 0
        where, params = self.feature_sync._inst_where(
            table_cfg, inst, ("<=", last_date)
        )
        hist_count = self.conn.execute(
            f'SELECT COUNT(*) FROM "{table_cfg["source_table"]}"{where}', params
        ).fetchone()[0]
        if hist_count > stored_count:
            return f"row_count {stored_count} → {hist_count}"

        if not last_date:
            return "last_date 为空，全量重转"
        return None

    def _convert_and_log(self, inst: str, table_cfg: dict, source_table: str,
                         field_names: list[str], stats: dict, stat_key: str,
                         label: str, quiet: bool, cleanup: bool = False) -> None:
        """全量重转单个 instrument（忽略 since）并更新 sync_log."""
        if cleanup:
            self.feature_sync._cleanup_partial_bins(inst, field_names)
        try:
            row_count = self.feature_sync._convert_instrument(inst, table_cfg, since="")
            upsert_sync_log(self.conn, inst, source_table, status="done",
                            last_date=self.feature_sync._get_date_bound(inst, table_cfg, "MAX"),
                            first_date=self.feature_sync._get_date_bound(inst, table_cfg, "MIN"),
                            row_count=row_count, fields=field_names)
            stats[stat_key] += 1
        except Exception as e:
            upsert_sync_log(self.conn, inst, source_table, status="error",
                            error_msg=str(e), fields=field_names)
            if not quiet:
                print(f"  [ERROR] {label} {inst} ← {source_table}: {e}")

    def _query_rows(self, inst: str, table_cfg: dict, field_filter: set[str],
                    after_date: str | None = None) -> tuple[list[tuple], list[str]]:
        """查询 instrument 数据（field_filter 限定字段，after_date 非 None 时仅取其后的行）."""
        tushare_cols = _collect_tushare_cols(table_cfg, field_filter=field_filter)
        cols_sql = ", ".join(f'"{c}"' for c in tushare_cols)
        date_cond = (">", after_date) if after_date is not None else None
        where, params = self.feature_sync._inst_where(table_cfg, inst, date_cond)
        query = (f'SELECT {cols_sql} FROM "{table_cfg["source_table"]}"'
                 f'{where} ORDER BY "{table_cfg["date_col"]}"')
        rows = self.conn.execute(query, params).fetchall()
        return [tuple(r) for r in rows], tushare_cols

    def _backfill_fields(self, inst: str, table_cfg: dict, new_fields: list[str]) -> None:
        """为新字段回填全量历史数据."""
        all_rows, col_names = self._query_rows(inst, table_cfg, set(new_fields))
        if not all_rows:
            return

        new_field_defs = [f for f in table_cfg["fields"] if f["bin_name"] in new_fields]
        arrays = self.feature_sync._rows_to_arrays(
            all_rows, col_names, table_cfg, new_field_defs
        )

        inst_dir = self.output_dir / "features" / inst.lower()
        for j, fdef in enumerate(new_field_defs):
            write_bin(inst_dir / f"{fdef['bin_name']}.day.bin", arrays[j])


class FieldRebuilder:
    """按字段维度重建."""

    def __init__(self, output_dir: Path, conn: sqlite3.Connection,
                 feature_sync: FeatureSync):
        self.output_dir = output_dir
        self.conn = conn
        self.feature_sync = feature_sync

    def rebuild_fields(self, conversion_tables: list[dict],
                       field_names_to_rebuild: list[str]) -> None:
        """仅重建指定字段（用于新增字段后回填）."""
        for table_cfg in conversion_tables:
            source_table = table_cfg["source_table"]
            desired_fields = {f["bin_name"] for f in table_cfg["fields"]}
            targets = set(field_names_to_rebuild) & desired_fields
            if not targets:
                continue

            instruments = get_instruments_for_table(self.conn, table_cfg)
            for i, inst in enumerate(instruments):
                if (i + 1) % 100 == 0:
                    print(f"  [{source_table}] rebuild {i+1}/{len(instruments)}")

                inst_dir = self.output_dir / "features" / inst.lower()
                for fname in targets:
                    bin_path = inst_dir / f"{fname}.day.bin"
                    if bin_path.exists():
                        bin_path.unlink()

                all_rows, col_names = self.feature_sync._query_instrument(inst, table_cfg)
                if not all_rows:
                    continue

                target_field_defs = [f for f in table_cfg["fields"] if f["bin_name"] in targets]
                arrays = self.feature_sync._rows_to_arrays(
                    all_rows, col_names, table_cfg, target_field_defs
                )

                for j, fdef in enumerate(target_field_defs):
                    write_bin(inst_dir / f"{fdef['bin_name']}.day.bin", arrays[j])

            print(f"  [{source_table}] 重建了 {len(targets)} 个字段")
