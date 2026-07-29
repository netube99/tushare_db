"""ETL 层 — REGISTRY 驱动 API → 表映射."""
from __future__ import annotations

import sqlite3

# 自动生成，勿手工编辑。运行 scripts/generate_schema.py 重新生成
REGISTRY = [
    {"api": "etf_basic", "table": "etf_basic"},
    {"api": "fund_adj", "table": "fund_adj", "date_col": "trade_date"},
    {"api": "cb_factor_pro", "table": "cb_factor_pro", "date_col": "trade_date"},
    {"api": "cb_basic", "table": "cb_basic"},
    {"api": "fund_factor_pro", "table": "fund_factor_pro", "date_col": "trade_date"},
    {"api": "gz_index", "table": "gz_index"},
    {"api": "daily_info", "table": "daily_info", "date_col": "trade_date"},
    {"api": "index_classify", "table": "index_classify", "default_params": {"src": "SW2021"}},
    {"api": "sz_daily_info", "table": "sz_daily_info", "date_col": "trade_date"},
    {"api": "index_dailybasic", "table": "index_dailybasic", "date_col": "trade_date"},
    {"api": "index_weight", "table": "index_weight", "date_col": "trade_date", "driver": {"values": ["000016.SH", "000300.SH", "000852.SH", "000903.SH", "000905.SH", "000906.SH", "399001.SZ", "399006.SZ", "399303.SZ", "399330.SZ", "399673.SZ", "000985.CSI"], "date_mode": "monthly"}},
    {"api": "ci_index_member", "table": "ci_index_member"},
    {"api": "idx_factor_pro", "table": "idx_factor_pro", "date_col": "trade_date"},
    {"api": "index_member_all", "table": "index_member_all"},
    {"api": "sw_daily", "table": "sw_daily", "date_col": "trade_date"},
    {"api": "index_basic", "table": "index_basic"},
    {"api": "margin_detail", "table": "margin_detail", "date_col": "trade_date"},
    {"api": "margin", "table": "margin", "date_col": "trade_date"},
    {"api": "repurchase", "table": "repurchase", "date_col": "ann_date"},
    {"api": "block_trade", "table": "block_trade", "date_col": "trade_date"},
    {"api": "stk_holdernumber", "table": "stk_holdernumber", "date_col": "ann_date"},
    {"api": "stk_holdertrade", "table": "stk_holdertrade", "date_col": "ann_date"},
    {"api": "pledge_detail", "table": "pledge_detail", "date_col": "ann_date"},
    {"api": "stock_hsgt", "table": "stock_hsgt", "date_col": "trade_date"},
    {"api": "stock_st", "table": "stock_st", "date_col": "trade_date"},
    {"api": "trade_cal", "table": "trade_cal"},
    {"api": "stock_basic", "table": "stock_basic"},
    {"api": "bak_basic", "table": "bak_basic", "date_col": "trade_date"},
    {"api": "limit_list_d", "table": "limit_list_d", "date_col": "trade_date"},
    {"api": "hm_list", "table": "hm_list"},
    {"api": "kpl_list", "table": "kpl_list", "date_col": "trade_date"},
    {"api": "top_inst", "table": "top_inst", "date_col": "trade_date"},
    {"api": "kpl_concept_cons", "table": "kpl_concept_cons", "date_col": "trade_date"},
    {"api": "top_list", "table": "top_list", "date_col": "trade_date"},
    {"api": "cyq_perf", "table": "cyq_perf", "date_col": "trade_date"},
    {"api": "stk_factor_pro", "table": "stk_factor_pro", "date_col": "trade_date"},
    {"api": "bak_daily", "table": "bak_daily", "date_col": "trade_date"},
    {"api": "stk_limit", "table": "stk_limit", "date_col": "trade_date"},
    {"api": "dividend", "table": "dividend", "date_col": "ann_date"},
    {"api": "moneyflow_hsgt", "table": "moneyflow_hsgt", "date_col": "trade_date"},
    {"api": "moneyflow", "table": "moneyflow", "date_col": "trade_date"},
    {"api": "moneyflow_dc", "table": "moneyflow_dc", "date_col": "trade_date"},
]

def log_pull(conn: sqlite3.Connection, table: str, date_val: str, ok: int,
             api: str = "", rows: int = 0, strategy: str = "") -> None:
    """记录拉取结果到 pull_log，同时写 JSON 日志.

    ok: 0=失败需重试, 1=成功, 2=确认空不重试, 3=重试超限放弃。
    ON CONFLICT 仅更新 ok，保留 retry_count/last_try（修复循环的重试计数）。
    """
    if ok not in (0, 1, 2, 3):
        raise ValueError(f"非法 ok 值: {ok}")
    conn.execute(
        "INSERT INTO pull_log (table_name, date_val, ok, last_try) "
        "VALUES (?, ?, ?, datetime('now', 'localtime')) "
        "ON CONFLICT(table_name, date_val) DO UPDATE SET ok=excluded.ok, "
        "last_try=excluded.last_try",
        (table, date_val, ok),
    )
    conn.commit()
    if api:
        from database.logger import get_json_logger
        level = "ERROR" if ok == 0 else "INFO"
        get_json_logger().write({
            "level": level, "module": "maintain", "event": "pull",
            "api": api, "table": table, "date_val": date_val,
            "strategy": strategy, "rows": rows, "ok": ok,
        })
