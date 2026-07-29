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
    """Tushare 类型 → SQLite 类型."""
    if tushare_type is None:
        return "TEXT"
    t = str(tushare_type).lower()
    if t in ("float",):
        return "REAL"
    if t in ("int", "integer"):
        return "INTEGER"
    return "TEXT"


def infer_pk(api: dict, driver: dict | None = None) -> str | None:
    """从 api 定义推断主键."""
    output_params = api.get("output_params", [])
    names = {p["name"] for p in output_params}
    has_ts = "ts_code" in names
    has_td = "trade_date" in names
    has_ad = "ann_date" in names
    has_ic = "index_code" in names
    has_cc = "con_code" in names
    has_ei = "exchange_id" in names
    has_dt = "date" in names

    if driver:
        param_name = None
        for p in api.get("input_params", []):
            force_req = api.get("_project", {}).get("param_fixes", {}).get("force_required", [])
            api_fq = api.get("api_name", "") + "." + p["name"]
            if (p.get("required") or api_fq in force_req) and p["name"] not in (
                "trade_date", "start_date", "end_date", "ann_date",
                "freq", "offset", "limit",
            ):
                param_name = p["name"]
                break
        if param_name and param_name in names:
            pk_cols = [param_name]
            if "con_code" in names:
                pk_cols.append("con_code")
            if has_td:
                pk_cols.append("trade_date")
            return "(" + ", ".join(pk_cols) + ")"

    if has_ts and has_td:
        return "(ts_code, trade_date)"
    if has_ic and has_td and not has_ts:
        return "(index_code, trade_date)"
    if has_ts and has_ad:
        return "(ts_code, ann_date)"
    if has_ei and has_td and not has_ts:
        return "(exchange_id, trade_date)"
    if has_td and not (has_ts or has_ic):
        return "(trade_date)"
    if has_ts:
        return "(ts_code)"
    if has_ic:
        return "(index_code)"
    if has_cc:
        return "(con_code)"
    if has_dt:
        return "(date)"
    return None


_SQL_KEYWORDS = {
    "on", "limit", "order", "group", "select", "from", "where", "and", "or",
    "not", "null", "true", "false", "index", "table", "view", "trigger",
    "primary", "key", "foreign", "references", "check", "default", "unique",
    "alter", "add", "drop", "create", "insert", "update", "delete", "set",
    "into", "values", "between", "like", "in", "is", "exists", "having",
    "asc", "desc", "offset", "union", "except", "intersect", "all", "any",
    "case", "when", "then", "else", "end", "as", "cast", "distinct",
    "join", "left", "right", "inner", "outer", "cross", "using",
    "commit", "rollback", "begin", "transaction", "abort", "replace",
    "recursive", "without", "rowid", "vacuum", "pragma",
}


def _quote_name(name: str) -> str:
    """如果列名需要引号则加双引号（SQL保留字/数字开头/含特殊字符）."""
    if name.lower() in _SQL_KEYWORDS:
        return f'"{name}"'
    if name[0].isdigit() or not name.replace("_", "").isalnum():
        return f'"{name}"'
    return name


def generate_table_ddl(api: dict, driver: dict | None = None) -> str:
    """为单个 API 生成 CREATE TABLE 语句."""
    table_name = api["api_name"]
    fields = api.get("output_params", [])
    if not fields:
        return ""

    pk = infer_pk(api, driver)

    col_defs = []
    for p in fields:
        col = f"    {_quote_name(p['name'])}"
        col += f" {sql_type(p.get('type'))}"
        col_defs.append(col)
    if pk:
        col_defs.append(f"    PRIMARY KEY {pk}")

    body = ",\n".join(col_defs)
    return f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n{body}\n);'


# ── 基础设施表 DDL ──

INFRA_DDL = """-- 交易日历（时间锚点）
CREATE TABLE IF NOT EXISTS trade_cal (
    exchange       TEXT NOT NULL,
    cal_date       TEXT NOT NULL,
    is_open        INTEGER NOT NULL,
    pretrade_date  TEXT,
    PRIMARY KEY (exchange, cal_date)
);

-- 拉取日志（驱动回填判断）
CREATE TABLE IF NOT EXISTS pull_log (
    table_name  TEXT NOT NULL,
    date_val    TEXT NOT NULL,
    ok          INTEGER NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_try    TEXT DEFAULT NULL,
    PRIMARY KEY (table_name, date_val)
);
"""


# ── I/O ──

def generate_schema(api_list: list[dict]) -> str:
    parts = [INFRA_DDL.rstrip()]
    seen = set()
    for api in api_list:
        if api["api_name"] in seen:
            continue
        seen.add(api["api_name"])
        driver = api.get("_project", {}).get("driver")
        ddl = generate_table_ddl(api, driver)
        if ddl:
            parts.append("")
            parts.append(f"-- {api.get('title', api['api_name'])}")
            parts.append(ddl)
    return "\n".join(parts) + "\n"


def generate_registry(api_list: list[dict]) -> str:
    """生成 REGISTRY — 仅规则 1/2 且 classification.usable=true 接口."""
    seen = set()
    entries = []
    for api in api_list:
        clf = api.get("_project", {}).get("classification", {})
        if clf.get("rule") not in (1, 2) or not clf.get("usable"):
            continue
        table = api["api_name"]
        if table in seen:
            continue
        seen.add(table)
        fields = api.get("output_params", [])
        driver = api.get("_project", {}).get("driver")
        pk = infer_pk(api, driver)
        pk_set = set(pk.strip("()").split(", ")) if pk else set()
        date_col = None
        if "trade_date" in pk_set:
            date_col = "trade_date"
        elif "ann_date" in pk_set:
            date_col = "ann_date"

        entry = '    {"api": "' + table + '", "table": "' + table + '"'
        if date_col:
            entry += ', "date_col": "' + date_col + '"'
        if driver:
            entry += ', "driver": ' + _json.dumps(driver)
        default_params = api.get("_project", {}).get("default_params")
        if default_params:
            entry += ', "default_params": ' + _json.dumps(default_params)
        entry += "},"
        entries.append(entry)

    lines = [
        "# 自动生成，勿手工编辑。运行 scripts/generate_schema.py 重新生成",
        "REGISTRY = [",
    ]
    lines.extend(entries)
    lines.append("]")
    return "\n".join(lines) + "\n"


def inject_registry(etl_path: str, registry_code: str) -> None:
    """将 REGISTRY 代码注入 etl.py，替换 REGISTRY = [...] 段（原子写）."""
    with open(etl_path) as f:
        content = f.read()

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
    tmp_path = etl_path + ".tmp"
    with open(tmp_path, "w") as f:
        f.write(new_content)
    os.replace(tmp_path, etl_path)


def main():
    api_list = [api for api in load_api_registry()
                if api.get("_project", {}).get("classification", {}).get("usable")
                and api.get("_project", {}).get("classification", {}).get("rule") in (1, 2)]

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
