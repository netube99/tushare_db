#!/usr/bin/env python3
"""重建 pension_float_daily — 基本养老保险基金十大流通持仓 → 日频对齐表.

来源：top10_floatholders（holder_type='基本养老保险基金'，即"基本养老保险基金XXX组合"）。
排除噪音：holder_name 含"养老"但属于养老产业公募基金 / 外国养老金 / 养老服务公司
（holder_type 为开放式投资基金/一般企业等），不纳入核心口径。

C6 财报对齐契约：引擎只消费 (交易日, 代码) 日频网格上的列，不做季度频率推断。
本表在数据层按公告日(ann_date)对齐成日频列：披露日起前向携带持有状态至下一披露日，
超过 STALE_DAYS 个自然日未再披露则过期（=养老组合退出十大流通股东后信号失效）。

物理表（非 VIEW）：SQL 侧 window CTE + 范围 JOIN 计划不可控，用 pandas 组装后落库。
top10_floatholders 增量更新后需重跑本脚本刷新。

用法: python scripts/refresh_pension_daily.py [--db PATH]
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts._derived_tables import (
    DEFAULT_DB,
    SOURCE_TABLES,
    atomic_replace,
    carry_expand,
    final_span,
    require_tables,
)

STALE_DAYS = 400  # 自然日；覆盖一个完整披露周期（季度）+ 迟披余量

COLUMNS = [
    "ts_code", "trade_date", "pen_cnt", "pen_ratio_sum", "pen_float_ratio_sum",
    "pen_chg_sum", "pen_amount_sum", "pen_age", "pen_ann_date",
]

METRIC_COLS = [
    "pen_cnt", "pen_ratio_sum", "pen_float_ratio_sum", "pen_chg_sum", "pen_amount_sum",
]

FINAL_TABLE = "pension_float_daily"
TMP_TABLE = "pension_float_daily__new"

CREATE_SQL = """
CREATE TABLE {table} (
    ts_code    TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    pen_cnt         INTEGER,
    pen_ratio_sum   REAL,
    pen_float_ratio_sum REAL,
    pen_chg_sum     REAL,
    pen_amount_sum  REAL,
    pen_age         INTEGER,
    pen_ann_date    TEXT,
    PRIMARY KEY (ts_code, trade_date)
)
"""

EVENTS_SQL = """
    SELECT ts_code, ann_date,
           COUNT(DISTINCT holder_name) AS pen_cnt,
           SUM(hold_ratio)             AS pen_ratio_sum,
           SUM(hold_float_ratio)       AS pen_float_ratio_sum,
           SUM(hold_change)            AS pen_chg_sum,
           SUM(hold_amount)            AS pen_amount_sum
    FROM top10_floatholders
    WHERE ann_date IS NOT NULL AND holder_type = '基本养老保险基金'
    GROUP BY ts_code, ann_date
    ORDER BY ts_code, ann_date
"""


def build(conn: sqlite3.Connection) -> pd.DataFrame:
    return carry_expand(
        conn, pd.read_sql_query(EVENTS_SQL, conn), METRIC_COLS, "pen", STALE_DAYS, COLUMNS
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DEFAULT_DB)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        require_tables(conn, SOURCE_TABLES)
        out = build(conn)
        atomic_replace(conn, TMP_TABLE, FINAL_TABLE, CREATE_SQL, out, chunksize=100)
        stocks, lo, hi = final_span(conn, FINAL_TABLE)
        print(f"pension_float_daily: {len(out)} 行, {stocks} 只股票, {lo} ~ {hi} (STALE_DAYS={STALE_DAYS})")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
