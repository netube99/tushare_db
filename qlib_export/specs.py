"""TABLE_SPECS 声明 + 字段映射生成 + instrument 来源定义."""

import sqlite3
from typing import Any

# ---------------------------------------------------------------------------
# TABLE_SPECS — 紧凑配置 + 通用推导
# ---------------------------------------------------------------------------

# 每张表只有真正不同的决策才在此声明，字段列表从 DB schema 自动推导。
TABLE_SPECS: list[dict[str, Any]] = [
    # ── 股票核心 ──
    {"table": "stk_factor_pro",   "inst_col": "ts_code", "date_col": "trade_date", "type": "stock",
     "ohlcv_hfq": True,
     "computed": [("vwap", "vwap")]},
    {"table": "bak_daily",        "inst_col": "ts_code", "date_col": "trade_date", "type": "stock", "prefix": "bd"},
    {"table": "bak_basic",        "inst_col": "ts_code", "date_col": "trade_date", "type": "stock", "prefix": "bb"},

    # ── 股票资金/事件/风控 ──
    {"table": "moneyflow",        "inst_col": "ts_code", "date_col": "trade_date", "type": "stock", "prefix": "mf"},
    {"table": "moneyflow_dc",     "inst_col": "ts_code", "date_col": "trade_date", "type": "stock", "prefix": "mdc"},
    {"table": "margin_detail",    "inst_col": "ts_code", "date_col": "trade_date", "type": "stock", "prefix": "md"},
    {"table": "stock_hsgt",       "inst_col": "ts_code", "date_col": "trade_date", "type": "stock",
     "agg": "split_by_type", "agg_split_col": "type",
     "computed": [("hsgt_sh", "type_SH"), ("hsgt_sz", "type_SZ"), ("hsgt_cnt", "count")]},
    {"table": "stk_limit",        "inst_col": "ts_code", "date_col": "trade_date", "type": "stock", "prefix": "sl"},
    {"table": "limit_list_d",     "inst_col": "ts_code", "date_col": "trade_date", "type": "stock", "prefix": "lld"},
    {"table": "cyq_perf",         "inst_col": "ts_code", "date_col": "trade_date", "type": "stock", "prefix": "cyq"},
    {"table": "stk_holdernumber", "inst_col": "ts_code", "date_col": "ann_date",   "type": "stock", "prefix": "shn"},
    {"table": "stk_holdertrade",  "inst_col": "ts_code", "date_col": "ann_date",   "type": "stock", "prefix": "sht",
     "agg": "sum_count", "agg_count": "sht_trade_cnt",
     "agg_weighted": {"avg_price": "change_vol"}},
    {"table": "pledge_detail",    "inst_col": "ts_code", "date_col": "ann_date",   "type": "stock", "prefix": "pd",
     "agg": "sum_count", "agg_count": "pd_pledge_cnt"},
    {"table": "repurchase",       "inst_col": "ts_code", "date_col": "ann_date",   "type": "stock", "prefix": "rp",
     "agg": "sum_count", "agg_count": "rp_cnt"},
    {"table": "block_trade",      "inst_col": "ts_code", "date_col": "trade_date", "type": "stock", "prefix": "bt",
     "agg": "sum_count_weighted", "agg_count": "bt_trade_cnt",
     "agg_weighted": {"price": "vol"}},
    {"table": "top_list",         "inst_col": "ts_code", "date_col": "trade_date", "type": "stock", "prefix": "tl"},
    {"table": "top_inst",         "inst_col": "ts_code", "date_col": "trade_date", "type": "stock", "prefix": "ti",
     "agg": "sum_count", "agg_count": "ti_inst_cnt"},
    {"table": "kpl_list",         "inst_col": "ts_code", "date_col": "trade_date", "type": "stock", "prefix": "kpl"},
    {"table": "kpl_concept_cons", "inst_col": "ts_code", "date_col": "trade_date", "type": "stock",
     "agg": "count", "agg_count": "kcc_concept_cnt", "no_raw_fields": True,
     "computed": [("kcc_hot_num_total", "sum", "hot_num")]},
    {"table": "stock_st",         "inst_col": "ts_code", "date_col": "trade_date", "type": "stock",
     "encode": "is_st", "encode_col": "type",
     "computed": [("st_is_st", "encode_is_st")]},

    # ── 指数（不复权）──
    {"table": "idx_factor_pro",   "inst_col": "ts_code", "date_col": "trade_date", "type": "index",
     "tech_prefix": "idx",
     "computed": [("vwap", "vwap")]},
    {"table": "index_dailybasic", "inst_col": "ts_code", "date_col": "trade_date", "type": "index", "prefix": "idb"},
    {"table": "daily_info",       "inst_col": "ts_code", "date_col": "trade_date", "type": "index", "prefix": "di"},
    {"table": "sz_daily_info",    "inst_col": "ts_code", "date_col": "trade_date", "type": "index", "prefix": "sdi"},

    # ── 申万行业 ──
    {"table": "sw_daily",         "inst_col": "ts_code", "date_col": "trade_date", "type": "sw_sector", "prefix": "sw"},

    # ── ETF（不复权）──
    {"table": "fund_factor_pro",  "inst_col": "ts_code", "date_col": "trade_date", "type": "etf",
     "tech_prefix": "etf",
     "computed": [("vwap", "vwap")]},
    {"table": "fund_adj",         "inst_col": "ts_code", "date_col": "trade_date", "type": "etf", "prefix": "fa"},

    # ── 可转债 ──
    {"table": "cb_factor_pro",    "inst_col": "ts_code", "date_col": "trade_date", "type": "cb",
     "tech_prefix": "cb",
     "computed": [("vwap", "vwap")]},

    # ── 市场级虚拟 instrument ──
    {"table": "gz_index",         "inst_col": None,      "date_col": "date",       "type": "market",
     "prefix": "gz", "virtual_inst": "_market_gz"},
    {"table": "margin",           "inst_col": "exchange_id", "date_col": "trade_date", "type": "market",
     "prefix": "mg", "virtual_inst": "_market_margin", "inst_filter": "exchange_id='SSE'"},
    {"table": "moneyflow_hsgt",   "inst_col": None,      "date_col": "trade_date", "type": "market",
     "prefix": "hsgtflow", "virtual_inst": "_market_hsgt"},
]

# OHLCV 列名集合（不含复权后缀）
_OHLCV_BASE = {"open", "high", "low", "close", "pre_close", "change", "pct_chg", "pct_change", "vol", "amount"}
# stk_factor_pro 中属于非复权估值列的字段
_SFP_VALUATION = {"pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "dv_ttm",
                  "total_share", "float_share", "free_share", "total_mv", "circ_mv",
                  "turnover_rate", "turnover_rate_f", "volume_ratio"}


def build_field_map(conn: sqlite3.Connection) -> list[dict]:
    """从 TABLE_SPECS + 实际 DB 结构动态生成 CONVERSION_TABLES.

    每张表的字段列表完全由 DB schema 决定，TABLE_SPECS 只声明命名规则和特殊策略。
    """
    tables: list[dict] = []

    for spec in TABLE_SPECS:
        tbl = spec["table"]
        inst_col = spec["inst_col"]
        date_col = spec["date_col"]
        inst_type = spec["type"]
        prefix = spec.get("prefix")
        ohlcv_hfq = spec.get("ohlcv_hfq", False)
        tech_prefix = spec.get("tech_prefix")
        agg = spec.get("agg")
        encode = spec.get("encode")
        computed_specs = spec.get("computed", [])

        info = conn.execute(f'PRAGMA table_info("{tbl}")').fetchall()
        numeric_cols = {r[1] for r in info if r[2] in ("REAL", "INTEGER")}
        all_cols = {r[1] for r in info}

        fields: list[dict] = []

        if ohlcv_hfq:
            fields = _derive_stk_factor_fields(numeric_cols, computed_specs)
        elif tech_prefix:
            fields = _derive_factor_fields(numeric_cols, tech_prefix, computed_specs)
        elif encode:
            fields = _derive_encoded_fields(spec)
        elif agg:
            fields = _derive_agg_fields(numeric_cols, prefix, spec)
        else:
            fields = _derive_standard_fields(numeric_cols, prefix, computed_specs)

        entry: dict[str, Any] = {
            "source_table": tbl,
            "inst_col": inst_col,
            "date_col": date_col,
            "inst_type": inst_type,
            "fields": fields,
        }
        for opt in ("agg", "agg_count", "agg_weighted", "agg_split_col",
                     "encode", "encode_col", "virtual_inst", "inst_filter"):
            val = spec.get(opt)
            if val is not None:
                key_map = {
                    "agg_count": "agg_count_col",
                    "agg_weighted": "agg_weighted_cols",
                    "agg_split_col": "agg_col",
                }
                entry[key_map.get(opt, opt)] = val

        tables.append(entry)

    return tables


def _collect_tushare_cols(table_cfg: dict, field_filter: set[str] | None = None) -> list[str]:
    """收集查询所需的 tushare 列名（含聚合/编码依赖列 + computed 字段依赖列）."""
    date_col = table_cfg["date_col"]
    fields = table_cfg["fields"]
    agg = table_cfg.get("agg")
    encode = table_cfg.get("encode")

    tushare_cols = [date_col]
    need_amount_vol = False

    for fdef in fields:
        if field_filter is not None and fdef["bin_name"] not in field_filter:
            continue
        tcol = fdef.get("tushare_col")
        if tcol and tcol not in tushare_cols:
            tushare_cols.append(tcol)
        computed = fdef.get("computed", "")
        if computed == "vwap":
            need_amount_vol = True
        if computed == "sum":
            asc = fdef.get("agg_sum_col")
            if asc and asc not in tushare_cols:
                tushare_cols.append(asc)

    if need_amount_vol:
        for dep_col in ["amount", "vol"]:
            if dep_col not in tushare_cols:
                tushare_cols.append(dep_col)

    if agg in ("sum_count", "sum_count_weighted"):
        for wcol in table_cfg.get("agg_weighted_cols", {}).values():
            if wcol not in tushare_cols:
                tushare_cols.append(wcol)
    if agg == "split_by_type":
        agg_col = table_cfg.get("agg_col", "type")
        if agg_col not in tushare_cols:
            tushare_cols.append(agg_col)
    if encode == "is_st":
        encode_col = table_cfg.get("encode_col", "type")
        if encode_col not in tushare_cols:
            tushare_cols.append(encode_col)

    return tushare_cols


def _derive_stk_factor_fields(numeric_cols: set[str], computed_specs: list) -> list[dict]:
    """stk_factor_pro: OHLCV 显式映射 hfq → 标准名, adj_factor → factor, 估值直接导出, 技术指标去 _hfq."""
    fields = []

    ohlcv_map = [
        ("open", "open_hfq"), ("high", "high_hfq"), ("low", "low_hfq"),
        ("close", "close_hfq"),
    ]
    for bin_name, col in ohlcv_map:
        if col in numeric_cols:
            fields.append({"bin_name": bin_name, "tushare_col": col})

    for col in ["pre_close", "change", "pct_chg"]:
        if col in numeric_cols:
            fields.append({"bin_name": col, "tushare_col": col})

    if "vol" in numeric_cols:
        fields.append({"bin_name": "volume", "tushare_col": "vol"})
    if "amount" in numeric_cols:
        fields.append({"bin_name": "amount", "tushare_col": "amount"})

    if "adj_factor" in numeric_cols:
        fields.append({"bin_name": "factor", "tushare_col": "adj_factor"})

    for col in sorted(_SFP_VALUATION & numeric_cols):
        fields.append({"bin_name": col, "tushare_col": col})

    _OHLCV_HFQ = {"open_hfq", "high_hfq", "low_hfq", "close_hfq"}
    hfq_cols = sorted(c for c in numeric_cols if "_hfq" in c and c not in _OHLCV_HFQ)
    for col in hfq_cols:
        bin_name = col.replace("_hfq", "")
        fields.append({"bin_name": bin_name, "tushare_col": col})

    for item in computed_specs:
        fdef: dict = {"bin_name": item[0], "computed": item[1]}
        if len(item) > 2:
            fdef["agg_sum_col"] = item[2]
        fields.append(fdef)

    return fields


def _derive_factor_fields(numeric_cols: set[str], tech_prefix: str,
                          computed_specs: list) -> list[dict]:
    """idx/fund/cb_factor_pro: OHLCV 无前缀, 技术指标加前缀+去 _bfq, vwap 计算."""
    fields = []

    ohlcv_in_db = sorted(_OHLCV_BASE & numeric_cols)
    for col in ohlcv_in_db:
        bin_name = "volume" if col == "vol" else col
        fields.append({"bin_name": bin_name, "tushare_col": col})

    tech_cols = sorted((numeric_cols - _OHLCV_BASE))
    for col in tech_cols:
        if col.endswith("_bfq"):
            base = col[:-4]
        else:
            base = col
        fields.append({"bin_name": f"{tech_prefix}_{base}", "tushare_col": col})

    for item in computed_specs:
        fdef: dict = {"bin_name": item[0], "computed": item[1]}
        if len(item) > 2:
            fdef["agg_sum_col"] = item[2]
        fields.append(fdef)

    return fields


def _derive_standard_fields(numeric_cols: set[str], prefix: str | None,
                            computed_specs: list) -> list[dict]:
    """标准表: 所有数值列加前缀."""
    fields = []
    for col in sorted(numeric_cols):
        bin_name = f"{prefix}_{col}" if prefix else col
        fields.append({"bin_name": bin_name, "tushare_col": col})
    for item in computed_specs:
        fields.append({"bin_name": item[0], "computed": item[1]})
    return fields


def _derive_agg_fields(numeric_cols: set[str], prefix: str | None,
                       spec: dict) -> list[dict]:
    """聚合表: 所有数值列加前缀 + COUNT 列."""
    weighted = spec.get("agg_weighted", {})
    fields = []
    if not spec.get("no_raw_fields"):
        for col in sorted(numeric_cols):
            bin_name = f"{prefix}_{col}" if prefix else col
            fdef: dict = {"bin_name": bin_name, "tushare_col": col}
            if col in weighted:
                fdef["agg_weighted"] = True
                fdef["weight_col"] = weighted[col]
            fields.append(fdef)
    count_name = spec.get("agg_count")
    if count_name:
        fields.append({"bin_name": count_name, "computed": "count"})
    for item in spec.get("computed", []):
        fdef: dict = {"bin_name": item[0], "computed": item[1]}
        if len(item) > 2:
            fdef["agg_sum_col"] = item[2]
        fields.append(fdef)
    return fields


def _derive_encoded_fields(spec: dict) -> list[dict]:
    """编码表: 无原生数值列，从 computed specs 生成."""
    fields = []
    for item in spec.get("computed", []):
        fields.append({"bin_name": item[0], "computed": item[1]})
    return fields


# ---------------------------------------------------------------------------
# instrument 来源定义
# ---------------------------------------------------------------------------
INSTRUMENT_SOURCES = {
    "stock":     {"table": "stock_basic", "code_col": "ts_code", "list_col": "list_date", "delist_col": "delist_date"},
    "etf":       {"table": "etf_basic",   "code_col": "ts_code", "list_col": "list_date", "delist_col": None},
    "index":     {"table": "index_basic", "code_col": "ts_code", "list_col": "list_date", "delist_col": None},
    "cb":        {"table": "cb_basic",    "code_col": "ts_code", "list_col": "list_date", "delist_col": None},
    "sw_sector": {"table": "sw_daily",    "code_col": "ts_code", "list_col": "trade_date", "delist_col": None, "range_mode": "minmax"},
    "market":    None,
}

VIRTUAL_INSTRUMENTS = {
    "_market_gz":     ("gz_index",       "date"),
    "_market_margin": ("margin",         "trade_date"),
    "_market_hsgt":   ("moneyflow_hsgt", "trade_date"),
}
