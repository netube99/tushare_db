"""national_team_daily / pension_float_daily 重建脚本共享骨架.

流程：源表检查 → 事件聚合（脚本自定义 SQL）→ carry_expand 按公告日对齐到
交易日网格（前向携带至 min(下一披露日, 公告日+stale)）→ tmp 表写入后
DROP+RENAME 原子替换。过期上限必须用真日历算术：YYYYMMDD 整数直接 +N
会跨年错位（如 20211027+400=20211427 被误判为早于 2022-01-01），须转
datetime 加 Timedelta 再回编。
"""
from __future__ import annotations

import os
from collections.abc import Sequence

import pandas as pd

from database import engine
from database.engine import Connection

DEFAULT_DB = os.environ.get("DDUP_MARKET_DB", engine.DEFAULT_DB_PATH)

SOURCE_TABLES = ("top10_floatholders", "trade_cal")

CAL_SQL = "SELECT cal_date FROM trade_cal WHERE exchange='SSE' AND is_open=1 ORDER BY cal_date"


def require_tables(conn: Connection, tables: tuple[str, ...]) -> None:
    missing = [t for t in tables if not engine.table_exists(conn, t)]
    if missing:
        raise SystemExit(
            f"缺少源表: {', '.join(missing)}；请先运行 scripts/maintain.py 拉取数据后重试"
        )


def carry_expand(
    conn: Connection,
    events: pd.DataFrame,
    metric_cols: Sequence[str],
    age_prefix: str,
    stale_days: int,
    out_columns: Sequence[str],
) -> pd.DataFrame:
    """事件行 (ts_code, ann_date, 指标列) 展开为 (ts_code, trade_date) 日频行.

    每个事件占据 [ann_date, min(next_ann, ann_date+stale_days)) 内的交易日，
    指标逐日前向携带，age = 交易日与公告日的自然日差（datetime 差，非
    YYYYMMDD 整数差——整数差仅在"同月"内恰好正确，跨月/跨年会被放大，
    如 20260630→20260810 整数差 180 实为 41 自然日）。
    """
    cal = conn.execute(CAL_SQL).df()
    cal_days = cal["cal_date"].to_numpy()
    cal_int = cal_days.astype("int64")
    cal_dt = pd.to_datetime(cal["cal_date"], format="%Y%m%d").to_numpy()

    events = events.sort_values(["ts_code", "ann_date"]).reset_index(drop=True)
    events["next_ann"] = events.groupby("ts_code")["ann_date"].shift(-1)
    ann_int = events["ann_date"].astype("int64").to_numpy()
    ann_dt = pd.to_datetime(events["ann_date"], format="%Y%m%d")
    ann_dt_np = ann_dt.to_numpy()
    stale_int = (ann_dt + pd.Timedelta(days=stale_days)).dt.strftime("%Y%m%d").astype("int64").to_numpy()
    nxt = events["next_ann"].to_numpy()

    age_col = f"{age_prefix}_age"
    ann_col = f"{age_prefix}_ann_date"
    frames = []
    for i in range(len(events)):
        start = cal_int.searchsorted(ann_int[i], side="left")
        end_int = int(nxt[i]) if nxt[i] is not None and not pd.isna(nxt[i]) else 99999999
        end_int = min(end_int, int(stale_int[i]))
        end = cal_int.searchsorted(end_int, side="left")
        if end <= start:
            continue
        row = events.iloc[i]
        data: dict[str, object] = {"ts_code": row["ts_code"], "trade_date": cal_days[start:end]}
        data.update({col: row[col] for col in metric_cols})
        data[age_col] = (
            (cal_dt[start:end] - ann_dt_np[i]).astype("timedelta64[D]").astype("int64")
        )
        data[ann_col] = row["ann_date"]
        frames.append(pd.DataFrame(data))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=out_columns)


def atomic_replace(
    conn: Connection,
    tmp_table: str,
    final_table: str,
    create_sql: str,
    df: pd.DataFrame,
) -> None:
    """写 tmp 表后 BEGIN + DROP 旧表 + RENAME 原子替换；写失败清理 tmp 保留旧表."""
    conn.execute(f"DROP TABLE IF EXISTS {tmp_table}")
    conn.execute(create_sql.format(table=tmp_table))
    try:
        conn.register("_derived_df", df)
        try:
            conn.execute(f"INSERT INTO {tmp_table} BY NAME SELECT * FROM _derived_df")
        finally:
            conn.unregister("_derived_df")
        conn.commit()
    except Exception:
        conn.execute(f"DROP TABLE IF EXISTS {tmp_table}")
        conn.commit()
        raise
    conn.execute("BEGIN")
    try:
        conn.execute(f"DROP TABLE IF EXISTS {final_table}")
        conn.execute(f"ALTER TABLE {tmp_table} RENAME TO {final_table}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def final_span(conn: Connection, final_table: str) -> tuple[int, str | None, str | None]:
    return conn.execute(
        f"SELECT COUNT(DISTINCT ts_code), MIN(trade_date), MAX(trade_date) FROM {final_table}"
    ).fetchone()
