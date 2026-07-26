"""增量同步引擎 — IncrementalSync + FieldRebuilder."""

import json
import sqlite3
from pathlib import Path

from qlib_export.specs import _collect_tushare_cols
from qlib_export.sync_log import upsert_sync_log, get_sync_records
from qlib_export.binio import write_bin, append_bin
from qlib_export.instruments import (
    get_instruments_for_table, qlib_to_ts_code, InstrumentSync,
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
            date_col = table_cfg["date_col"]
            fields = table_cfg["fields"]
            desired_field_names = [f["bin_name"] for f in fields]
            inst_col = table_cfg["inst_col"]
            virtual_inst = table_cfg.get("virtual_inst")
            inst_filter = table_cfg.get("inst_filter")

            records = get_sync_records(self.conn, source_table)
            existing_insts = {r["instrument"] for r in records}
            all_insts = get_instruments_for_table(self.conn, table_cfg)

            n_all = len(all_insts)
            for i, inst in enumerate(all_insts):
                if inst not in existing_insts:
                    try:
                        # 全量首转必须忽略 since（--since 仅供全量调试截断用）
                        row_count = self.feature_sync._convert_instrument(
                            inst, table_cfg, since="")
                        last_date = self.feature_sync._get_date_bound(inst, table_cfg, "MAX")
                        first_date = self.feature_sync._get_date_bound(inst, table_cfg, "MIN")
                        upsert_sync_log(self.conn, inst, source_table,
                                       status="done", last_date=last_date,
                                       first_date=first_date,
                                       row_count=row_count, fields=desired_field_names)
                        stats["new_instruments"] += 1
                    except Exception as e:
                        upsert_sync_log(self.conn, inst, source_table,
                                       status="error", error_msg=str(e),
                                       fields=desired_field_names)
                        if not quiet:
                            print(f"  [ERROR] new {inst} ← {source_table}: {e}")

                if not quiet and (i + 1) % 100 == 0:
                    print(f"  [{source_table}] scan {i+1}/{n_all}")

            n_records = len(records)
            for j, record in enumerate(records):
                if not quiet and (j + 1) % 100 == 0:
                    print(f"  [{source_table}] sync {j+1}/{n_records}")

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

                needs_reconvert = False
                ts_code = (None if virtual_inst
                          else qlib_to_ts_code(inst, table_cfg["inst_type"]))

                current_first = self.feature_sync._get_date_bound(inst, table_cfg, "MIN")
                if (first_date and current_first and current_first < first_date):
                    needs_reconvert = True
                    if not quiet:
                        print(f"  [backfill] {inst} ← {source_table}: "
                              f"first_date {first_date} → {current_first}")

                if not needs_reconvert and not table_cfg.get("agg"):
                    if virtual_inst:
                        if inst_filter:
                            hist_count = self.conn.execute(
                                f'SELECT COUNT(*) FROM "{source_table}" '
                                f'WHERE {inst_filter} AND "{date_col}" <= ?',
                                (last_date,)
                            ).fetchone()[0]
                        else:
                            hist_count = self.conn.execute(
                                f'SELECT COUNT(*) FROM "{source_table}" '
                                f'WHERE "{date_col}" <= ?', (last_date,)
                            ).fetchone()[0]
                    else:
                        hist_count = self.conn.execute(
                            f'SELECT COUNT(*) FROM "{source_table}" '
                            f'WHERE "{inst_col}" = ? AND "{date_col}" <= ?',
                            (ts_code, last_date)
                        ).fetchone()[0]
                    stored_count = record.get("row_count") or 0
                    if hist_count > stored_count:
                        needs_reconvert = True
                        if not quiet:
                            print(f"  [backfill] {inst} ← {source_table}: "
                                  f"row_count {stored_count} → {hist_count}")

                if not last_date and not needs_reconvert:
                    needs_reconvert = True
                    if not quiet:
                        print(f"  [backfill] {inst} ← {source_table}: last_date 为空，全量重转")

                if needs_reconvert:
                    self._reconvert_full(inst, table_cfg, source_table,
                                         desired_field_names, stats, quiet)
                    continue

                rows, col_names = self._query_incremental(
                    inst, table_cfg, last_date, synced_fields
                )
                if not rows:
                    continue

                agg = table_cfg.get("agg")
                encode = table_cfg.get("encode")
                if agg:
                    rows, col_names = self.feature_sync._apply_aggregation(rows, col_names, table_cfg)
                if encode:
                    rows, col_names = self.feature_sync._apply_encoding(rows, col_names, table_cfg)

                arrays = self.feature_sync._align_to_calendar(rows, col_names, fields, date_col)

                inst_dir = self.output_dir / "features" / inst.lower()
                for j, fdef in enumerate(fields):
                    fname = fdef["bin_name"]
                    if fname not in synced_fields:
                        continue
                    bin_path = inst_dir / f"{fname}.day.bin"
                    append_bin(bin_path, arrays[j])

                new_last = rows[-1][col_names.index(date_col)] if date_col in col_names else last_date
                new_row_count = (record.get("row_count") or 0) + len(rows)
                upsert_sync_log(self.conn, inst, source_table,
                               status="done", last_date=str(new_last),
                               first_date=first_date,
                               row_count=new_row_count,
                               fields=list(synced_fields))
                stats["updated_records"] += 1

        from database.logger import get_json_logger
        get_json_logger().write({
            "level": "INFO", "module": "convert_to_qlib", "event": "convert",
            "mode": "daily",
            "total_cal_days": self.calendar.n_days,
            "new_instruments": stats["new_instruments"],
            "updated_records": stats["updated_records"],
        })

        return stats

    def _reconvert_full(self, inst, table_cfg, source_table,
                        desired_field_names, stats, quiet) -> None:
        """清理残留并全量重转单个 instrument，更新 sync_log.

        全量重转同样忽略 since（写全量 bin，不能被 --since 截断）.
        """
        self.feature_sync._cleanup_partial_bins(inst, desired_field_names)
        try:
            row_count = self.feature_sync._convert_instrument(inst, table_cfg, since="")
            new_last = self.feature_sync._get_date_bound(inst, table_cfg, "MAX")
            new_first = self.feature_sync._get_date_bound(inst, table_cfg, "MIN")
            upsert_sync_log(self.conn, inst, source_table,
                           status="done", last_date=new_last,
                           first_date=new_first,
                           row_count=row_count, fields=desired_field_names)
            stats["updated_records"] += 1
        except Exception as e:
            upsert_sync_log(self.conn, inst, source_table,
                           status="error", error_msg=str(e),
                           fields=desired_field_names)
            if not quiet:
                print(f"  [ERROR] reconvert {inst} ← {source_table}: {e}")

    def _query_incremental(self, inst, table_cfg, last_date, synced_fields):
        """查询增量数据."""
        source_table = table_cfg["source_table"]
        date_col = table_cfg["date_col"]
        inst_col = table_cfg["inst_col"]
        virtual_inst = table_cfg.get("virtual_inst")
        inst_filter = table_cfg.get("inst_filter")

        tushare_cols = _collect_tushare_cols(table_cfg, field_filter=set(synced_fields))
        cols_sql = ", ".join(f'"{c}"' for c in tushare_cols)

        if virtual_inst:
            if inst_filter:
                query = f'SELECT {cols_sql} FROM "{source_table}" WHERE {inst_filter} AND "{date_col}" > ? ORDER BY "{date_col}"'
            else:
                query = f'SELECT {cols_sql} FROM "{source_table}" WHERE "{date_col}" > ? ORDER BY "{date_col}"'
            rows = self.conn.execute(query, (last_date,)).fetchall()
        else:
            ts_code = qlib_to_ts_code(inst, table_cfg["inst_type"])
            query = f'SELECT {cols_sql} FROM "{source_table}" WHERE "{inst_col}" = ? AND "{date_col}" > ? ORDER BY "{date_col}"'
            rows = self.conn.execute(query, (ts_code, last_date)).fetchall()

        return [tuple(r) for r in rows], tushare_cols

    def _backfill_fields(self, inst, table_cfg, new_fields):
        """为新字段回填全量历史数据."""
        source_table = table_cfg["source_table"]
        inst_col = table_cfg["inst_col"]
        date_col = table_cfg["date_col"]
        virtual_inst = table_cfg.get("virtual_inst")
        inst_filter = table_cfg.get("inst_filter")

        tushare_cols = _collect_tushare_cols(table_cfg, field_filter=set(new_fields))
        cols_sql = ", ".join(f'"{c}"' for c in tushare_cols)

        if virtual_inst:
            if inst_filter:
                query = f'SELECT {cols_sql} FROM "{source_table}" WHERE {inst_filter} ORDER BY "{date_col}"'
            else:
                query = f'SELECT {cols_sql} FROM "{source_table}" ORDER BY "{date_col}"'
            rows = self.conn.execute(query).fetchall()
        else:
            ts_code = qlib_to_ts_code(inst, table_cfg["inst_type"])
            query = f'SELECT {cols_sql} FROM "{source_table}" WHERE "{inst_col}" = ? ORDER BY "{date_col}"'
            rows = self.conn.execute(query, (ts_code,)).fetchall()

        all_rows = [tuple(r) for r in rows]
        if not all_rows:
            return

        agg = table_cfg.get("agg")
        encode = table_cfg.get("encode")
        if agg:
            all_rows, tushare_cols = self.feature_sync._apply_aggregation(all_rows, tushare_cols, table_cfg)
        if encode:
            all_rows, tushare_cols = self.feature_sync._apply_encoding(all_rows, tushare_cols, table_cfg)

        new_field_defs = [f for f in table_cfg["fields"] if f["bin_name"] in new_fields]

        arrays = self.feature_sync._align_to_calendar(all_rows, tushare_cols, new_field_defs, date_col)

        inst_dir = self.output_dir / "features" / inst.lower()
        for j, fdef in enumerate(new_field_defs):
            bin_path = inst_dir / f"{fdef['bin_name']}.day.bin"
            write_bin(bin_path, arrays[j])


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
            n_total = len(instruments)
            for i, inst in enumerate(instruments):
                if (i + 1) % 100 == 0:
                    print(f"  [{source_table}] rebuild {i+1}/{n_total}")

                inst_dir = self.output_dir / "features" / inst.lower()
                for fname in targets:
                    bin_path = inst_dir / f"{fname}.day.bin"
                    if bin_path.exists():
                        bin_path.unlink()

                all_rows, col_names = self.feature_sync._query_instrument(inst, table_cfg)
                if not all_rows:
                    continue

                date_col = table_cfg["date_col"]
                agg = table_cfg.get("agg")
                encode = table_cfg.get("encode")
                if agg:
                    all_rows, col_names = self.feature_sync._apply_aggregation(all_rows, col_names, table_cfg)
                if encode:
                    all_rows, col_names = self.feature_sync._apply_encoding(all_rows, col_names, table_cfg)

                target_field_defs = [f for f in table_cfg["fields"] if f["bin_name"] in targets]
                arrays = self.feature_sync._align_to_calendar(all_rows, col_names, target_field_defs, date_col)

                for j, fdef in enumerate(target_field_defs):
                    bin_path = inst_dir / f"{fdef['bin_name']}.day.bin"
                    write_bin(bin_path, arrays[j])

            print(f"  [{source_table}] 重建了 {len(targets)} 个字段")
