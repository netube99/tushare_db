#!/usr/bin/env python3
"""重建 national_team_daily — 国家队（汇金/证金/外汇局系）十大流通持仓 → 日频对齐表.

来源：top10_floatholders，国家队名单（holder_name 模式匹配）：
  - 汇金系: '%中央汇金%'（中央汇金投资 / 中央汇金资产管理 / 汇金资管单一资管计划）
  - 证金系: '%中国证券金融%'（证金公司）+ '%中证金融资产管理%'（十大基金资管计划，
    全半角连字符变体均覆盖；"中证金融科技"等基金名不匹配该模式）
  - 外汇局系: '%梧桐树%'（梧桐树投资平台）+ '%北京凤山%' + '%北京坤藤'

与 pension_float_daily 的关键差异：**显式退出事件**。国家队"退出十大流通"
是跟卖信号，不能靠过期兜底——按报告期对齐：某股某报告期披露了十大流通
但无国家队在榜 → 在该期首公告日生成退出事件（nt_state=0），携带立即清零。

C6 财报对齐契约：按公告日(ann_date)对齐成 (ts_code, trade_date) 日频列，
事件间携带当前状态。hold_change=NULL 即新进（2015 救市样本验证）。

物理表（非 VIEW）：与 pension_float_daily 同理。top10_floatholders 增量更新后
需重跑本脚本刷新。

用法: python scripts/refresh_national_team_daily.py [--db PATH] [--stale-days N]
"""
from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import engine
from database.engine import Connection

from scripts._derived_tables import (
    DEFAULT_DB,
    SOURCE_TABLES,
    atomic_replace,
    carry_expand,
    final_span,
    require_tables,
)

NT_WHERE = """
    holder_name LIKE '%中央汇金%'
    OR holder_name LIKE '%中国证券金融%'
    OR holder_name LIKE '%中证金融资产管理%'
    OR holder_name LIKE '%梧桐树%'
    OR holder_name LIKE '%北京凤山%'
    OR holder_name LIKE '%北京坤藤%'
"""

COLUMNS = [
    "ts_code", "trade_date", "nt_state", "nt_cnt", "nt_ratio_sum", "nt_float_ratio_sum",
    "nt_chg_sum", "nt_amount_sum", "nt_new_cnt", "nt_age", "nt_ann_date",
]

METRIC_COLS = [
    "nt_state", "nt_cnt", "nt_ratio_sum", "nt_float_ratio_sum",
    "nt_chg_sum", "nt_amount_sum", "nt_new_cnt",
]

FINAL_TABLE = "national_team_daily"
TMP_TABLE = "national_team_daily__new"

CREATE_SQL = """
CREATE TABLE {table} (
    ts_code    VARCHAR NOT NULL,
    trade_date VARCHAR NOT NULL,
    nt_state        BIGINT,
    nt_cnt          BIGINT,
    nt_ratio_sum    DOUBLE,
    nt_float_ratio_sum DOUBLE,
    nt_chg_sum      DOUBLE,
    nt_amount_sum   DOUBLE,
    nt_new_cnt      BIGINT,
    nt_age          BIGINT,
    nt_ann_date     VARCHAR,
    PRIMARY KEY (ts_code, trade_date)
)
"""


def load_events(conn: Connection) -> pd.DataFrame:
    """构造事件表：在榜事件（聚合，nt_state=1）+ 退出事件（仅持股状态 1→0 时）.

    退出语义：国家队在榜后，某报告期披露无国家队 → 退出事件（state 1→0）。
    从未被持有的股票的披露期不是退出事件；已退出后的后续披露期也不再重复
    生成退出（state 已为 0）。重新进入 = 新的在榜事件。
    """
    snapshots = conn.execute(
        f"""
        WITH named AS (
            SELECT *, ({NT_WHERE}) AS is_nt
            FROM top10_floatholders WHERE ann_date IS NOT NULL
        ), per_period AS (
            SELECT ts_code, ann_date, end_date,
                   COUNT(DISTINCT CASE WHEN is_nt THEN holder_name END) AS nt_cnt,
                   SUM(CASE WHEN is_nt THEN hold_ratio END) AS nt_ratio_sum,
                   SUM(CASE WHEN is_nt THEN hold_float_ratio END) AS nt_float_ratio_sum,
                   SUM(CASE WHEN is_nt THEN hold_change END) AS nt_chg_sum,
                   SUM(CASE WHEN is_nt THEN hold_amount END) AS nt_amount_sum,
                   SUM(CASE WHEN is_nt AND hold_change IS NULL THEN 1 ELSE 0 END) AS nt_new_cnt
            FROM named GROUP BY ts_code, ann_date, end_date
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY ts_code, ann_date ORDER BY end_date DESC NULLS LAST
            ) AS rn FROM per_period
        ), latest AS (
            SELECT *, MAX(end_date) OVER (
                PARTITION BY ts_code ORDER BY ann_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS known_period FROM ranked WHERE rn=1
        )
        SELECT ts_code, ann_date, nt_cnt,
               CASE WHEN nt_cnt=0 THEN 0 ELSE nt_ratio_sum END AS nt_ratio_sum,
               CASE WHEN nt_cnt=0 THEN 0 ELSE nt_float_ratio_sum END AS nt_float_ratio_sum,
               CASE WHEN nt_cnt=0 THEN 0 ELSE nt_chg_sum END AS nt_chg_sum,
               CASE WHEN nt_cnt=0 THEN 0 ELSE nt_amount_sum END AS nt_amount_sum, nt_new_cnt,
               CASE WHEN nt_cnt > 0 THEN 1 ELSE 0 END AS nt_state
        FROM latest WHERE end_date=known_period OR known_period IS NULL
        ORDER BY ts_code, ann_date
        """
    ).df()
    # 同日只用最新报告期；迟到的旧报告不覆盖已知的新期持仓。
    previous = snapshots.groupby("ts_code")["nt_state"].shift(fill_value=0)
    return snapshots.loc[(snapshots["nt_state"] == 1) | (previous == 1)].reset_index(drop=True)


def build(conn: Connection, stale_days: int) -> pd.DataFrame:
    return carry_expand(conn, load_events(conn), METRIC_COLS, "nt", stale_days, COLUMNS)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--stale-days", type=int, default=400)
    args = ap.parse_args()

    with engine.db_lock(args.db, exclusive=True):
        conn = engine.connect(args.db)
        try:
            require_tables(conn, SOURCE_TABLES)
            out = build(conn, args.stale_days)
            atomic_replace(conn, TMP_TABLE, FINAL_TABLE, CREATE_SQL, out)
            stocks, lo, hi = final_span(conn, FINAL_TABLE)
            present_days = conn.execute(
                f"SELECT COUNT(*) FROM {FINAL_TABLE} WHERE nt_state=1"
            ).fetchone()[0]
            print(
                f"national_team_daily: {len(out)} 行（在榜 {present_days}）, {stocks} 只股票, "
                f"{lo} ~ {hi} (stale_days={args.stale_days})"
            )
        finally:
            conn.close()


if __name__ == "__main__":
    main()
