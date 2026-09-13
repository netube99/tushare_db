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
import sqlite3
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DEFAULT_DB = os.environ.get(
    "DDUP_MARKET_DB", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "market.db")
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

FINAL_TABLE = "national_team_daily"
TMP_TABLE = "national_team_daily__new"

CREATE_SQL = """
CREATE TABLE {table} (
    ts_code    TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    nt_state        INTEGER,
    nt_cnt          INTEGER,
    nt_ratio_sum    REAL,
    nt_float_ratio_sum REAL,
    nt_chg_sum      REAL,
    nt_amount_sum   REAL,
    nt_new_cnt      INTEGER,
    nt_age          INTEGER,
    nt_ann_date     TEXT,
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


def load_events(conn: sqlite3.Connection) -> pd.DataFrame:
    """构造事件表：在榜事件（聚合）+ 退出事件（仅持股状态 1→0 时）.

    退出语义：国家队在榜后，某报告期披露无国家队 → 退出事件（state 1→0）。
    从未被持有的股票的披露期不是退出事件；已退出后的后续披露期也不再重复
    生成退出（state 已为 0）。重新进入 = 新的在榜事件。
    """
    present = pd.read_sql_query(
        f"""
        SELECT ts_code, ann_date,
               COUNT(DISTINCT holder_name)                                    AS nt_cnt,
               SUM(hold_ratio)                                                AS nt_ratio_sum,
               SUM(hold_float_ratio)                                          AS nt_float_ratio_sum,
               SUM(hold_change)                                               AS nt_chg_sum,
               SUM(hold_amount)                                               AS nt_amount_sum,
               SUM(CASE WHEN hold_change IS NULL THEN 1 ELSE 0 END)           AS nt_new_cnt
        FROM top10_floatholders
        WHERE ann_date IS NOT NULL AND {NT_WHERE}
        GROUP BY ts_code, ann_date
        ORDER BY ts_code, ann_date
        """,
        conn,
    )
    disc = pd.read_sql_query(
        """
        SELECT ts_code, end_date, MIN(ann_date) AS ann_date
        FROM top10_floatholders
        WHERE ann_date IS NOT NULL AND end_date IS NOT NULL
        GROUP BY ts_code, end_date
        """,
        conn,
    )

    present_keys = set(zip(present["ts_code"], present["ann_date"]))

    disc_anns: dict[str, set[str]] = {}
    for ts, ann in zip(disc["ts_code"], disc["ann_date"]):
        disc_anns.setdefault(ts, set()).add(ann)

    exit_rows = []
    for ts in sorted(set(disc_anns) | {ts for ts, _ in present_keys}):
        anns = sorted(disc_anns.get(ts, set()) | {a for t, a in present_keys if t == ts})
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
                    }
                )

    events = pd.concat(
        [present, pd.DataFrame(exit_rows, columns=present.columns)], ignore_index=True
    )
    events = events.drop_duplicates(subset=["ts_code", "ann_date"], keep="first")
    return events.sort_values(["ts_code", "ann_date"]).reset_index(drop=True)


def build(conn: sqlite3.Connection, stale_days: int) -> pd.DataFrame:
    events = load_events(conn)
    cal = pd.read_sql_query(
        "SELECT cal_date FROM trade_cal WHERE exchange='SSE' AND is_open=1 ORDER BY cal_date", conn
    )
    cal_days = cal["cal_date"].to_numpy()
    cal_int = cal_days.astype("int64")

    events["next_ann"] = events.groupby("ts_code")["ann_date"].shift(-1)
    # 过期上限：真日历算术（YYYYMMDD 整数直接 +N 会跨年错位，见 pension 脚本教训）
    ann_dt = pd.to_datetime(events["ann_date"], format="%Y%m%d")
    stale_int = (ann_dt + pd.Timedelta(days=stale_days)).dt.strftime("%Y%m%d").astype("int64").to_numpy()

    ann_int = events["ann_date"].astype("int64").to_numpy()
    nxt = events["next_ann"].to_numpy()

    frames = []
    for i in range(len(events)):
        start = cal_int.searchsorted(ann_int[i], side="left")
        end_int = int(nxt[i]) if nxt[i] is not None and not pd.isna(nxt[i]) else 99999999
        end_int = min(end_int, int(stale_int[i]))
        end = cal_int.searchsorted(end_int, side="left")
        if end <= start:
            continue
        row = events.iloc[i]
        df = pd.DataFrame(
            {
                "ts_code": row["ts_code"],
                "trade_date": cal_days[start:end],
                "nt_state": 1 if row["nt_cnt"] > 0 else 0,
                "nt_cnt": row["nt_cnt"],
                "nt_ratio_sum": row["nt_ratio_sum"],
                "nt_float_ratio_sum": row["nt_float_ratio_sum"],
                "nt_chg_sum": row["nt_chg_sum"],
                "nt_amount_sum": row["nt_amount_sum"],
                "nt_new_cnt": row["nt_new_cnt"],
                "nt_age": cal_int[start:end] - int(ann_int[i]),
                "nt_ann_date": row["ann_date"],
            }
        )
        frames.append(df)
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMNS)
    # 最后一个事件为退出(state=0)时其后无携带行；为在榜时仅 stale_days 内有效
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--stale-days", type=int, default=400)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        _require_tables(conn, ("top10_floatholders", "trade_cal"))
        out = build(conn, args.stale_days)
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {TMP_TABLE}")
        cur.execute(CREATE_SQL.format(table=TMP_TABLE))
        try:
            out.to_sql(TMP_TABLE, conn, if_exists="append", index=False, method="multi", chunksize=90)
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
        present_days = cur.execute(
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
