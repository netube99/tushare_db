#!/usr/bin/env python3
"""从 api_index.json 自动生成 database/schema.sql 和 REGISTRY."""
from __future__ import annotations

import json as _json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from database.utils import atomic_write_text, load_api_registry


# ── 纯函数 ──

def sql_type(tushare_type: str | None) -> str:
    """Tushare 类型 → DuckDB 类型（int→BIGINT 防 int32 溢出，float→DOUBLE 保精度）."""
    t = str(tushare_type).lower() if tushare_type is not None else ""
    if t in ("int", "integer"):
        return "BIGINT"
    if t == "float":
        return "DOUBLE"
    return "VARCHAR"


def infer_pk(api: dict, driver: dict | None = None) -> str | None:
    """从 api 定义推断主键."""
    proj = api.get("_project") or {}
    if proj.get("no_pk"):
        return None
    if proj.get("pk_override"):
        return proj["pk_override"]
    names = {p["name"] for p in api.get("output_params", [])}
    has_ts = "ts_code" in names
    has_td = "trade_date" in names

    if driver:
        force_req = proj.get("param_fixes", {}).get("force_required", [])
        skip = {"trade_date", "start_date", "end_date", "ann_date",
                "freq", "offset", "limit"}
        api_name = api.get("api_name", "")
        param_name = next((p["name"] for p in api.get("input_params", [])
                           if (p.get("required")
                               or api_name + "." + p["name"] in force_req)
                           and p["name"] not in skip), None)
        if param_name and param_name in names:
            pk_cols = [param_name]
            if "con_code" in names:
                pk_cols.append("con_code")
            if has_td:
                pk_cols.append("trade_date")
            elif "ann_date" in names:
                pk_cols.append("ann_date")
            return "(" + ", ".join(pk_cols) + ")"

    if has_ts and has_td:
        return "(ts_code, trade_date)"
    if "index_code" in names and has_td and not has_ts:
        return "(index_code, trade_date)"
    if has_ts and "ann_date" in names:
        return "(ts_code, ann_date)"
    if "exchange_id" in names and has_td and not has_ts:
        return "(exchange_id, trade_date)"
    if has_td and not (has_ts or "index_code" in names):
        return "(trade_date)"
    if has_ts:
        return "(ts_code)"
    if "index_code" in names:
        return "(index_code)"
    if "con_code" in names:
        return "(con_code)"
    if "date" in names:
        return "(date)"
    return None


_SQL_KEYWORDS = {
    "abort", "action", "add", "after", "all", "alter", "always", "analyze",
    "and", "as", "asc", "attach", "autoincrement", "before", "begin",
    "between", "by", "cascade", "case", "cast", "check", "collate",
    "column", "commit", "conflict", "constraint", "create", "cross",
    "current", "current_date", "current_time", "current_timestamp",
    "database", "default", "deferrable", "deferred", "delete", "desc",
    "detach", "distinct", "do", "drop", "each", "else", "end", "escape",
    "except", "exclusive", "exists", "explain", "fail", "for", "foreign",
    "from", "full", "glob", "group", "having", "if", "ignore", "immediate",
    "in", "index", "indexed", "initially", "inner", "insert", "instead",
    "intersect", "into", "is", "isnull", "join", "key", "left", "like",
    "limit", "match", "natural", "no", "not", "nothing", "notnull", "null",
    "of", "offset", "on", "or", "order", "outer", "plan", "pragma",
    "primary", "query", "raise", "recursive", "references", "regexp",
    "reindex", "release", "rename", "replace", "restrict", "returning",
    "right", "rollback", "row", "rows", "savepoint", "select", "set",
    "table", "temp", "temporary", "then", "ties", "to", "transaction",
    "trigger", "unbounded", "union", "unique", "update", "using", "vacuum",
    "values", "view", "virtual", "when", "where", "window", "with",
    "without", "true", "false", "rowid",
}


def _load_duckdb_reserved() -> set[str]:
    """运行时读取 DuckDB 保留字，避免版本升级后新增关键字导致 DDL 断裂."""
    try:
        import duckdb
        con = duckdb.connect()
        try:
            rows = con.execute(
                "SELECT keyword_name FROM duckdb_keywords() "
                "WHERE keyword_category = 'reserved'").fetchall()
        finally:
            con.close()
        return {str(r[0]).lower() for r in rows}
    except Exception:
        return set()


_SQL_KEYWORDS |= _load_duckdb_reserved()


def _quote_name(name: str) -> str:
    """如果列名需要引号则加双引号（SQL保留字/数字开头/含特殊字符）."""
    if name.lower() in _SQL_KEYWORDS:
        return '"' + name.replace('"', '""') + '"'
    if name[0].isdigit() or not name.replace("_", "").isalnum():
        return '"' + name.replace('"', '""') + '"'
    return name


def generate_table_ddl(api: dict, driver: dict | None = None) -> str:
    """为单个 API 生成 CREATE TABLE 语句."""
    fields = api.get("output_params", [])
    if not fields:
        return ""

    pk = infer_pk(api, driver)
    col_defs = [f"    {_quote_name(p['name'])} {sql_type(p.get('type'))}"
                for p in fields]
    if pk:
        col_defs.append(f"    PRIMARY KEY {pk}")

    body = ",\n".join(col_defs)
    return f'CREATE TABLE IF NOT EXISTS "{api["api_name"]}" (\n{body}\n);'


# ── 基础设施表 DDL ──

INFRA_DDL = """-- 交易日历（时间锚点）
CREATE TABLE IF NOT EXISTS trade_cal (
    exchange       VARCHAR NOT NULL,
    cal_date       VARCHAR NOT NULL,
    is_open        BIGINT NOT NULL,
    pretrade_date  VARCHAR,
    PRIMARY KEY (exchange, cal_date)
);

-- 拉取日志（驱动回填判断）
CREATE TABLE IF NOT EXISTS pull_log (
    table_name  VARCHAR NOT NULL,
    date_val    VARCHAR NOT NULL,
    ok          BIGINT NOT NULL,
    retry_count BIGINT NOT NULL DEFAULT 0,
    last_try    VARCHAR DEFAULT NULL,
    PRIMARY KEY (table_name, date_val)
);
"""


# ── 派生视图 DDL（下游适配器消费的稳定契约）──
# 视图从物理表聚合出 (ts_code, 日期) 唯一键，供 ddup adapters/tushare.py 读取。
# 用 DROP VIEW IF EXISTS + CREATE VIEW（而非 IF NOT EXISTS）保证 init_schema
# 每次执行都把 DB 中的旧定义替换为仓库当前定义。
DERIVED_VIEWS_DDL = """-- 现金分红事件（ex_date 作 trade_date，仅除息日有值）
DROP VIEW IF EXISTS dividend_grid;
CREATE VIEW dividend_grid AS
  SELECT ts_code, ex_date AS trade_date, SUM(cash_div) AS cash_div
  FROM dividend
  WHERE div_proc='实施' AND cash_div > 0 AND ex_date IS NOT NULL
  GROUP BY ts_code, ex_date;

-- 股东人数（同公告取 MAX 披露口径）
DROP VIEW IF EXISTS stk_holdernumber_agg;
CREATE VIEW stk_holdernumber_agg AS
  SELECT ts_code, ann_date, MAX(holder_num) AS holder_num
  FROM stk_holdernumber WHERE holder_num IS NOT NULL GROUP BY ts_code, ann_date;

-- 股东增减持（change_vol/change_ratio 按 in_de 带符号，DE=负；
-- change_ratio 为绝对变动股数加权的带符号平均比例，分母仍为绝对股数之和）
DROP VIEW IF EXISTS stk_holdertrade_agg;
CREATE VIEW stk_holdertrade_agg AS
  SELECT ts_code, ann_date,
         SUM(CASE WHEN in_de = 'DE' THEN -change_vol ELSE change_vol END) AS change_vol,
         SUM(CASE WHEN in_de = 'DE' THEN -change_vol * change_ratio
                  ELSE change_vol * change_ratio END)
             / NULLIF(SUM(change_vol), 0) AS change_ratio
  FROM stk_holdertrade
  GROUP BY ts_code, ann_date;

-- 股权质押（同公告取 MAX 披露比例，保守口径）
DROP VIEW IF EXISTS pledge_detail_agg;
CREATE VIEW pledge_detail_agg AS
  SELECT ts_code, ann_date,
         MAX(p_total_ratio) AS p_total_ratio,
         MAX(h_total_ratio) AS h_total_ratio
  FROM pledge_detail
  GROUP BY ts_code, ann_date;

-- 龙虎榜机构席位聚合（top_inst 为逐席位多行，2026-09 起不再折叠：
-- 原 PK(ts_code, trade_date) 会把多席位 INSERT OR REPLACE 成任意单行）。
-- 机构口径 = exalter='机构专用'；inst_buy_rate = 机构买入额 / 全体上榜席位买入额。
DROP VIEW IF EXISTS top_inst_agg;
CREATE VIEW top_inst_agg AS
  SELECT ts_code, trade_date,
         SUM(CASE WHEN exalter = '机构专用' THEN net_buy ELSE 0 END) AS inst_net_buy,
         SUM(CASE WHEN exalter = '机构专用' THEN buy ELSE 0 END) AS inst_buy,
         SUM(buy) AS total_buy,
         SUM(CASE WHEN exalter = '机构专用' THEN buy ELSE 0 END)
             / NULLIF(SUM(buy), 0) AS inst_buy_rate
  FROM top_inst
  GROUP BY ts_code, trade_date;
"""


# ── I/O ──

def generate_schema(api_list: list[dict]) -> str:
    blocks = [INFRA_DDL.rstrip()]
    seen: set[str] = set()
    for api in api_list:
        if api["api_name"] in seen:
            continue
        seen.add(api["api_name"])
        ddl = generate_table_ddl(api, (api.get("_project") or {}).get("driver"))
        if ddl:
            blocks.append(f"-- {api.get('title', api['api_name'])}\n{ddl}")
    blocks.append(DERIVED_VIEWS_DDL.rstrip())
    return "\n\n".join(blocks) + "\n"


def generate_registry(api_list: list[dict]) -> str:
    """生成 REGISTRY — 仅规则 1/2 且 classification.usable=true 接口."""
    seen: set[str] = set()
    entries = []
    for api in api_list:
        proj = api.get("_project") or {}
        clf = proj.get("classification", {})
        if clf.get("rule") not in (1, 2) or not clf.get("usable"):
            continue
        table = api["api_name"]
        if table in seen:
            continue
        seen.add(table)

        pk = infer_pk(api, proj.get("driver"))
        pk_set = set(pk.strip("()").split(", ")) if pk else set()
        if "trade_date" in pk_set:
            date_col = "trade_date"
        elif "ann_date" in pk_set:
            date_col = "ann_date"
        elif proj.get("date_col"):
            date_col = proj["date_col"]
        else:
            date_col = None

        upsert = proj.get("upsert", {})
        frags = [f'    {{"api": "{table}", "table": "{table}"']
        if date_col:
            frags.append(f', "date_col": "{date_col}"')
        if proj.get("driver"):
            frags.append(f', "driver": {_json.dumps(proj["driver"])}')
        if upsert.get("null_pk_keep"):
            frags.append(', "null_pk_keep": True')
        if upsert.get("partition_key"):
            frags.append(f', "partition_key": "{upsert["partition_key"]}"')
        if upsert.get("dedupe_cols"):
            frags.append(f', "dedupe_cols": {_json.dumps(upsert["dedupe_cols"])}')
        if proj.get("default_params"):
            frags.append(
                f', "default_params": {_json.dumps(proj["default_params"])}')
        frags.append("},")
        entries.append("".join(frags))

    return "\n".join([
        "# 自动生成，勿手工编辑。运行 scripts/generate_schema.py 重新生成",
        "REGISTRY = [",
        *entries,
        "]",
    ]) + "\n"


def inject_registry(etl_path: str, registry_code: str) -> None:
    """将 REGISTRY 代码注入 etl.py，替换 REGISTRY = [...] 段（原子写）."""
    with open(etl_path) as f:
        content = f.read()

    try:
        compile(registry_code, "<registry>", "exec")
    except SyntaxError as e:
        raise ValueError(f"registry_code 语法非法: {e}")

    start = content.find("REGISTRY = [")
    if start == -1:
        raise ValueError("etl.py 中未找到 REGISTRY = [")

    for marker in ("# ---------- REGISTRY ----------", "# 自动生成，勿手工编辑"):
        comment_line = content.rfind(marker, 0, start)
        if comment_line != -1:
            break
    if comment_line == -1:
        raise ValueError("etl.py 中未找到 REGISTRY 注释标记")

    end = content.find("\n\n# --", start)
    if end == -1:
        end = content.find("\ndef ", start)
    if end == -1:
        end = content.find("\nclass ", start)
    if end == -1:
        raise ValueError("找不到 REGISTRY 块结束位置")

    new_content = content[:comment_line] + registry_code + "\n" + content[end + 1:]
    try:
        compile(new_content, etl_path, "exec")
    except SyntaxError as e:
        raise ValueError(f"注入后 etl.py 语法非法: {e}")
    tmp_path = etl_path + ".tmp"
    with open(tmp_path, "w") as f:
        f.write(new_content)
    os.replace(tmp_path, etl_path)


def main():
    api_list = [api for api in load_api_registry()
                if (clf := (api.get("_project") or {}).get("classification", {})).get("usable")
                and clf.get("rule") in (1, 2)]

    schema_sql = generate_schema(api_list)
    schema_path = PROJECT_ROOT / "database" / "schema.sql"
    atomic_write_text(schema_path, schema_sql)
    print(f"[schema] 写入 {schema_path} ({len(api_list)} APIs)")

    registry_code = generate_registry(api_list)
    etl_path = str(PROJECT_ROOT / "database" / "etl.py")
    inject_registry(etl_path, registry_code)
    print(f"[registry] 注入 {etl_path}  ({len(api_list)} APIs)")


if __name__ == "__main__":
    main()
