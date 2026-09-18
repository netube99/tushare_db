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
    present = conn.execute(
        f"""
        SELECT ts_code, ann_date,
               COUNT(DISTINCT holder_name)                                    AS nt_cnt,
               SUM(hold_ratio)                                                AS nt_ratio_sum,
               SUM(hold_float_ratio)                                          AS nt_float_ratio_sum,
               SUM(hold_change)                                               AS nt_chg_sum,
               SUM(hold_amount)                                               AS nt_amount_sum,
               SUM(CASE WHEN hold_change IS NULL THEN 1 ELSE 0 END)           AS nt_new_cnt,
               1                                                              AS nt_state
        FROM top10_floatholders
        WHERE ann_date IS NOT NULL AND {NT_WHERE}
        GROUP BY ts_code, ann_date
        ORDER BY ts_code, ann_date
        """
    ).df()
    disc = conn.execute(
        """
        SELECT ts_code, end_date, MIN(ann_date) AS ann_date
        FROM top10_floatholders
        WHERE ann_date IS NOT NULL AND end_date IS NOT NULL
        GROUP BY ts_code, end_date
        """
    ).df()

    present_keys = set(zip(present["ts_code"], present["ann_date"]))
    present_anns: dict[str, set[str]] = {}
    for ts, ann in present_keys:
        present_anns.setdefault(ts, set()).add(ann)

    disc_anns: dict[str, set[str]] = {}
    for ts, ann in zip(disc["ts_code"], disc["ann_date"]):
        disc_anns.setdefault(ts, set()).add(ann)

    exit_rows = []
    for ts in sorted(set(disc_anns) | set(present_anns)):
        anns = sorted(disc_anns.get(ts, set()) | present_anns.get(ts, set()))
        state = 0
        for ann in anns:
            if (ts, ann) in present_keys:
                state = 1
            elif state == 1:
                state = 0
                exit_rows.append(
                    {
                        "ts_code": ts,
                        "ann_date": ann,
                        "nt_cnt": 0,
                        "nt_ratio_sum": 0.0,
                        "nt_float_ratio_sum": 0.0,
                        "nt_chg_sum": pd.NA,
                        "nt_amount_sum": 0.0,
                        "nt_new_cnt": 0,
                        "nt_state": 0,
                    }
                )

    return pd.concat(
        [present, pd.DataFrame(exit_rows, columns=present.columns)], ignore_index=True
    ).drop_duplicates(subset=["ts_code", "ann_date"], keep="first")


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
