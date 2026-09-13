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

DEFAULT_DB = os.environ.get(
    "DDUP_MARKET_DB", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "market.db")
)
STALE_DAYS = 400  # 自然日；覆盖一个完整披露周期（季度）+ 迟披余量

COLUMNS = [
    "ts_code", "trade_date", "pen_cnt", "pen_ratio_sum", "pen_float_ratio_sum",
    "pen_chg_sum", "pen_amount_sum", "pen_age", "pen_ann_date",
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


def _require_tables(conn: sqlite3.Connection, tables: tuple[str, ...]) -> None:
    missing = []
    for t in tables:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)
        ).fetchone()
        if row is None:
            missing.append(t)
    if missing:
        raise SystemExit(
            f"缺少源表: {', '.join(missing)}；请先运行 scripts/maintain.py 拉取数据后重试"
        )


def build(conn: sqlite3.Connection) -> pd.DataFrame:
    ev = pd.read_sql_query(
        """
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
        """,
        conn,
    )
    cal = pd.read_sql_query(
        "SELECT cal_date FROM trade_cal WHERE exchange='SSE' AND is_open=1 ORDER BY cal_date", conn
    )
    cal_days = cal["cal_date"].to_numpy()
    cal_int = cal_days.astype("int64")

    # 下一披露日（按股票分组后移一位）；组末为 None → 无限携带，由 STALE_DAYS 兜底
    ev["next_ann"] = ev.groupby("ts_code")["ann_date"].shift(-1)

    frames = []
    ann = ev["ann_date"].to_numpy()
    nxt = ev["next_ann"].to_numpy()
    # 过期上限：真日历算术（YYYYMMDD 整数直接 +400 会跨年错位，如
    # 20211027+400=20211427 被误判为早于 2022-01-01，携带在年末全截断）
    ann_int = ann.astype("int64")
    ann_dt = pd.to_datetime(ann, format="%Y%m%d")
    stale_int = (ann_dt + pd.Timedelta(days=STALE_DAYS)).strftime("%Y%m%d").astype("int64").to_numpy()
    for i in range(len(ev)):
        start = cal_int.searchsorted(ann_int[i], side="left")
        end_int = int(nxt[i]) if nxt[i] is not None and not pd.isna(nxt[i]) else 99999999
        end_int = min(end_int, int(stale_int[i]))
        end = cal_int.searchsorted(end_int, side="left")
        if end <= start:
            continue
        n = end - start
        row = ev.iloc[i]
        df = pd.DataFrame(
            {
                "ts_code": row["ts_code"],
                "trade_date": cal_days[start:end],
                "pen_cnt": row["pen_cnt"],
                "pen_ratio_sum": row["pen_ratio_sum"],
                "pen_float_ratio_sum": row["pen_float_ratio_sum"],
                "pen_chg_sum": row["pen_chg_sum"],
                "pen_amount_sum": row["pen_amount_sum"],
                "pen_age": cal_int[start:end] - int(ann_int[i]),
                "pen_ann_date": row["ann_date"],
            }
        )
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMNS)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DEFAULT_DB)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        _require_tables(conn, ("top10_floatholders", "trade_cal"))
        out = build(conn)
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {TMP_TABLE}")
        cur.execute(CREATE_SQL.format(table=TMP_TABLE))
        try:
            out.to_sql(TMP_TABLE, conn, if_exists="append", index=False, method="multi", chunksize=100)
        except Exception:
            cur.execute(f"DROP TABLE IF EXISTS {TMP_TABLE}")
            conn.commit()
            raise
        cur.execute("BEGIN")
        cur.execute(f"DROP TABLE IF EXISTS {FINAL_TABLE}")
        cur.execute(f"ALTER TABLE {TMP_TABLE} RENAME TO {FINAL_TABLE}")
        conn.commit()
        stocks, lo, hi = cur.execute(
            f"SELECT COUNT(DISTINCT ts_code), MIN(trade_date), MAX(trade_date) FROM {FINAL_TABLE}"
        ).fetchone()
        print(f"pension_float_daily: {len(out)} 行, {stocks} 只股票, {lo} ~ {hi} (STALE_DAYS={STALE_DAYS})")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
