#!/usr/bin/env python3
"""tushare_db → Qlib Bin 数据转换引擎.

用法:
    python scripts/convert_to_qlib.py                  # 全量转换（中断自动续转）
    python scripts/convert_to_qlib.py --daily           # 每日增量同步
    python scripts/convert_to_qlib.py --reset           # 清除所有 bin 及同步状态，从头全量
    python scripts/convert_to_qlib.py --dry-run         # 仅扫描，显示待转换统计
    python scripts/convert_to_qlib.py --fields <F1,F2>  # 仅重建指定字段
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.utils import get_conn, beijing_now
from qlib_export import (
    build_field_map,
    init_sync_log, is_synced, clear_all_sync_log,
    CalendarSync,
    InstrumentSync, IndexConstituentSync, get_instruments_for_table,
    FeatureSync, IncrementalSync, FieldRebuilder,
)

# ---------------------------------------------------------------------------
# 路径常量
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "market.db"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "qlib_bin" / "cn_data"


# ---------------------------------------------------------------------------
# features_manifest.json 生成
# ---------------------------------------------------------------------------

def build_features_manifest(conversion_tables: list[dict],
                            calendar: CalendarSync) -> dict:
    """生成 features_manifest.json."""
    manifest: dict[str, Any] = {
        "_meta": {
            "version": "1.0.0",
            "generated_at": beijing_now().isoformat(),
            "source_db": "tushare_db",
        },
        "instruments": {},
        "features": [],
        "categories": {"OHLCV": 0, "valuation": 0, "liquidity": 0, "technical": 0,
                       "moneyflow": 0, "margin": 0, "chip": 0, "event": 0,
                       "risk": 0, "macro": 0, "fundamental": 0},
    }

    if calendar.calendar_range:
        manifest["_meta"]["calendar_range"] = list(calendar.calendar_range)
        manifest["_meta"]["calendar_days"] = calendar.n_days

    inst_types = {
        "stock": "stock", "etf": "etf", "index": "index",
        "sw_sector": "sw_sector", "cb": "convertible_bond", "market": "market_virtual"
    }
    type_counts: dict[str, int] = {v: 0 for v in inst_types.values()}

    all_features = []
    for table_cfg in conversion_tables:
        source_table = table_cfg["source_table"]
        inst_type = table_cfg["inst_type"]
        applies_to = inst_types.get(inst_type, inst_type)
        adjustment = "none"
        if inst_type == "stock" and source_table == "stk_factor_pro":
            adjustment = "hfq"
        elif inst_type == "cb" and source_table == "cb_factor_pro":
            adjustment = "bfq"

        for fdef in table_cfg["fields"]:
            cat = _categorize_field(fdef["bin_name"], source_table)
            all_features.append({
                "name": fdef["bin_name"],
                "source_table": source_table,
                "applies_to": [applies_to],
                "category": cat,
                "adjustment": adjustment,
            })
            manifest["categories"][cat] = manifest["categories"].get(cat, 0) + 1

    manifest["features"] = all_features
    manifest["_meta"]["total_fields"] = len(all_features)

    return manifest


def _categorize_field(field_name: str, source_table: str) -> str:
    """根据字段名和来源表推断类别."""
    name_lower = field_name.lower()
    ohlcv_patterns = ["open", "high", "low", "close", "volume", "amount",
                       "vwap", "pre_close", "change", "pct_chg", "pct_change"]
    for p in ohlcv_patterns:
        if name_lower.endswith(p) or name_lower == p:
            return "OHLCV"
    val_patterns = ["pe", "pb", "ps", "mv", "total_share", "float_share",
                    "free_share", "dv_ratio", "dv_ttm", "adj_factor"]
    for p in val_patterns:
        if p in name_lower:
            return "valuation"
    liq_patterns = ["turnover", "volume_ratio"]
    for p in liq_patterns:
        if p in name_lower:
            return "liquidity"
    mf_patterns = ["buy_", "sell_", "net_mf", "net_amount"]
    for p in mf_patterns:
        if p in name_lower:
            return "moneyflow"
    if any(p in name_lower for p in ["rzye", "rqye", "rzmre", "rzche", "rqmcl",
                                       "rzrqye", "rqyl", "rqchl"]):
        return "margin"
    if any(p in name_lower for p in ["cyq_", "winner", "cost_"]):
        return "chip"
    evt_patterns = ["limit", "st_is_st", "pledge", "repurchase", "block_trade",
                    "hsgt", "kpl_", "top_list", "top_inst", "holder"]
    for p in evt_patterns:
        if p in name_lower:
            return "event"
    if source_table in ("gz_index", "moneyflow_hsgt"):
        return "macro"
    fund_patterns = ["eps", "bvps", "gpr", "npr", "rev_yoy", "profit_yoy",
                     "total_assets", "liquid_assets", "fixed_assets",
                     "reserved", "undp", "holder_num"]
    for p in fund_patterns:
        if p in name_lower:
            return "fundamental"
    return "technical"


def write_manifest(output_dir: Path, manifest: dict) -> None:
    """写入 features_manifest.json."""
    path = output_dir / "features_manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="tushare_db → Qlib Bin 数据转换引擎",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python scripts/convert_to_qlib.py                  # 全量转换（中断自动续转）
  python scripts/convert_to_qlib.py --daily           # 每日增量同步
  python scripts/convert_to_qlib.py --reset           # 清除所有 bin 及同步状态，从头全量
  python scripts/convert_to_qlib.py --dry-run         # 仅扫描，显示待转换统计
  python scripts/convert_to_qlib.py --fields open,high  # 仅重建指定字段
        """,
    )
    parser.add_argument("--daily", action="store_true",
                       help="每日增量同步")
    parser.add_argument("--reset", action="store_true",
                       help="清除所有 bin 及同步状态，从头全量转换")
    parser.add_argument("--dry-run", action="store_true",
                       help="仅扫描统计，不实际转换")
    parser.add_argument("--fields", type=str, default=None,
                       help="仅重建指定字段（逗号分隔）")
    parser.add_argument("--table", type=str, default=None,
                       help="仅转换指定表（逗号分隔的 source_table 名）")
    parser.add_argument("--since", type=str, default=None,
                       help="起始日期 YYYYMMDD，仅转换该日期之后的数据（测试用）")
    parser.add_argument("--output", type=str, default=None,
                       help=f"bin 输出目录（默认: {DEFAULT_OUTPUT_DIR}）")
    parser.add_argument("--db", type=str, default=None,
                       help=f"market.db 路径（默认: {DB_PATH}）")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    t_start = time.time()

    output_dir = Path(args.output) if args.output else DEFAULT_OUTPUT_DIR
    db_path = str(Path(args.db) if args.db else DB_PATH)

    mode = ('daily' if args.daily else 'reset' if args.reset
            else 'dry-run' if args.dry_run else 'fields' if args.fields else 'full')
    print(f"Qlib Converter — market.db → {output_dir}")
    print(f"模式: {mode}")

    conn = get_conn(db_path)
    init_sync_log(conn)

    from database.logger import get_json_logger
    logger = get_json_logger()
    logger.write({"level": "INFO", "module": "convert_to_qlib", "event": "run_start",
                  "mode": mode, "output_dir": str(output_dir)})

    try:
        print("构建字段映射...")
        conversion_tables = build_field_map(conn)

        if args.table:
            table_filter = {t.strip() for t in args.table.split(",")}
            conversion_tables = [t for t in conversion_tables
                                 if t["source_table"] in table_filter]
            print(f"  --table 过滤: {table_filter}")

        total_fields = sum(len(t["fields"]) for t in conversion_tables)
        print(f"  共 {len(conversion_tables)} 张表, {total_fields} 个字段")
        if args.since:
            print(f"  日期过滤: >= {args.since}")

        if args.reset and not args.dry_run:
            print("清除所有 bin 文件和同步状态...")
            if output_dir.exists():
                import shutil
                shutil.rmtree(output_dir)
            clear_all_sync_log(conn)

        print("[1/3] 初始化日历和品种清单...")
        calendar = CalendarSync(output_dir)
        old_calendar = calendar.load_old_calendar() if args.daily else None
        if args.daily:
            # 日历由 daily_sync 全量重建并写盘（先 load 旧日历供比对）
            calendar.load()
        elif args.dry_run:
            # dry-run 只读：优先复用已有日历文件，缺失时才从 DB 构建
            calendar.load()
            if not calendar.calendar:
                calendar.full_init(conn)
        else:
            calendar.full_init(conn)
        if calendar.calendar:
            print(f"  日历: {calendar.n_days} 天 ({calendar.calendar_range[0]} ~ {calendar.calendar_range[1]})")

        inst_sync = InstrumentSync(output_dir)

        feature_sync = FeatureSync(output_dir, calendar, conn, since=args.since)

        if args.dry_run:
            _cmd_dry_run(conn, conversion_tables, calendar, output_dir)
        elif args.fields:
            field_names = [f.strip() for f in args.fields.split(",")]
            print(f"重建字段: {field_names}")
            rebuilder = FieldRebuilder(output_dir, conn, feature_sync)
            rebuilder.rebuild_fields(conversion_tables, field_names)
        elif args.daily:
            _cmd_daily(conn, conversion_tables, calendar, inst_sync, feature_sync, output_dir,
                       old_calendar=old_calendar)
        else:
            _cmd_full(conn, conversion_tables, calendar, inst_sync, feature_sync, output_dir)

    finally:
        elapsed = time.time() - t_start
        logger.write({"level": "INFO", "module": "convert_to_qlib", "event": "run_end",
                      "elapsed_sec": round(elapsed, 1)})
        conn.close()


def _cmd_dry_run(conn, conversion_tables, calendar, output_dir):
    """扫描并显示统计."""
    print("\n=== 扫描报告 ===\n")
    print(f"日历: {calendar.n_days} 天")
    print(f"输出目录: {output_dir}")

    total_insts = 0
    total_fields = 0
    total_pending = 0

    for table_cfg in conversion_tables:
        source_table = table_cfg["source_table"]
        instruments = get_instruments_for_table(conn, table_cfg)
        fields = table_cfg["fields"]
        field_names = [f["bin_name"] for f in fields]

        synced = sum(1 for inst in instruments
                    if is_synced(conn, inst, source_table, field_names))
        pending = len(instruments) - synced

        total_insts += len(instruments)
        total_fields += len(fields)
        total_pending += pending

        agg_note = f" [{table_cfg.get('agg', '1:1')}]" if table_cfg.get("agg") else ""
        status = "✓ 全部完成" if pending == 0 else f"{pending} 待转换"
        print(f"  {source_table}{agg_note}: {len(instruments)} instruments × {len(fields)} fields → {status}")

    print(f"\n合计: {total_insts} instruments, {total_fields} fields, {total_pending} 待转换")


def _cmd_full(conn, conversion_tables, calendar, inst_sync, feature_sync, output_dir):
    """全量转换（含中断续转）."""
    partials = feature_sync.resume_check()
    if partials:
        print(f"\n! 发现 {len(partials)} 个未完成同步任务，将重新执行:")
        for p in partials:
            print(f"  {p['instrument']} ← {p['source_table']}")
        for p in partials:
            field_names = json.loads(p["fields_json"])
            feature_sync._cleanup_partial_bins(p["instrument"], field_names)

    print("[1/3] 初始化品种清单...")
    inst_sync.full_init(conn)

    print("生成指数成分股清单...")
    idx_constituent_sync = IndexConstituentSync(output_dir)
    idx_constituent_sync.full_sync(conn)

    t_start = time.time()
    print(f"\n[2/3] 开始全量转换 ({len(conversion_tables)} 张表)...")
    stats = feature_sync.full_convert(conversion_tables)
    elapsed = time.time() - t_start

    print(f"\n全量转换完成: {elapsed:.0f}s")
    print(f"  instruments: {stats['total_instruments']} (written: {stats['total_written']}, skipped: {stats['total_skipped']})")

    print("[3/3] 生成 features_manifest.json...")
    manifest = build_features_manifest(conversion_tables, calendar)
    manifest["_meta"]["total_instruments"] = stats["total_written"]
    write_manifest(output_dir, manifest)
    print(f"  字段: {manifest['_meta']['total_fields']} 个")


def _cmd_daily(conn, conversion_tables, calendar, inst_sync, feature_sync, output_dir,
               old_calendar=None):
    """每日增量同步（日历由 daily_sync 全量重建）."""
    print("[2/3] 执行增量同步...")
    inc_sync = IncrementalSync(output_dir, calendar, conn, feature_sync)
    t_start = time.time()
    stats = inc_sync.daily_sync(conversion_tables, old_calendar=old_calendar)
    elapsed = time.time() - t_start

    print(f"\n增量同步完成: {elapsed:.0f}s")
    print(f"  交易日: {calendar.n_days} 天")
    print(f"  新 instruments: {stats['new_instruments']}")
    print(f"  更新记录: {stats['updated_records']}")

    idx_constituent_sync = IndexConstituentSync(output_dir)
    idx_constituent_sync.full_sync(conn)

    print("[3/3] 更新 features_manifest.json...")
    manifest = build_features_manifest(conversion_tables, calendar)
    write_manifest(output_dir, manifest)


if __name__ == "__main__":
    main()
