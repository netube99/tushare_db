"""schema 生成链 TDD 审查 — classify_apis.py / generate_schema.py."""

import ast
import json

import pytest

import scripts.classify_apis as classify_apis
from database.engine import connect, execute_script
from scripts.classify_apis import _parse_rate, classify
from scripts.generate_schema import (
    DERIVED_VIEWS_DDL,
    _quote_name,
    generate_registry,
    generate_schema,
    generate_table_ddl,
    infer_pk,
    inject_registry,
    sql_type,
)


# ── classify 分级规则 ──

def _base_api(**kw):
    api = {"api_name": "demo", "min_points": 2000, "rate_limit": None,
           "is_premium": False, "input_params": []}
    api.update(kw)
    return api


def test_classify_rule1_standard_freq():
    c = classify(_base_api(), 5000, 500)
    assert c["_rule"] == 1
    assert c["_usable"] is True
    assert c["_effective_rate"] == 500
    assert c["_recommended_interval_ms"] == 150


def test_classify_rule2_low_freq():
    c = classify(_base_api(rate_limit="20次/分钟"), 5000, 500)
    assert c["_rule"] == 2
    assert c["_effective_rate"] == 20
    assert c["_note"]


def test_classify_rule3_points_insufficient():
    c = classify(_base_api(min_points=8000), 5000, 500)
    assert c["_rule"] == 3
    assert c["_effective_rate"] is None


def test_classify_rule6_premium_takes_priority_over_rule3():
    c = classify(_base_api(is_premium=True, min_points=99999), 5000, 500)
    assert c["_rule"] == 6
    assert c["_usable"] is False


def test_classify_hour_day_limit_unusable():
    c = classify(_base_api(rate_limit="1次/小时"), 5000, 500)
    assert c["_usable"] is False
    assert c["_recommended_interval_ms"] is None


def test_classify_idempotent_on_own_output():
    api = _base_api(rate_limit="20次/分钟")
    once = classify(api, 5000, 500)
    twice = classify(once, 5000, 500)
    for key in ("_rule", "_rule_label", "_usable", "_effective_rate",
                "_recommended_interval_ms", "_note"):
        assert once[key] == twice[key]


def test_parse_rate_variants():
    assert _parse_rate(None) is None
    assert _parse_rate(500) == 500
    assert _parse_rate("500") == 500
    assert _parse_rate("30次/分钟") == 30
    assert _parse_rate("1次/小时") is None
    assert _parse_rate("2次/天") is None
    assert _parse_rate("不限") is None


# ── classify main() 写回（tmp 隔离） ──

@pytest.fixture
def index_env(tmp_path, monkeypatch):
    idx_file = tmp_path / "api_index.json"
    idx_file.write_text(json.dumps([
        _base_api(api_name="ok_api"),
        _base_api(api_name="poor_api", min_points=99999),
        _base_api(api_name="paid_api", is_premium=True),
        _base_api(api_name="slow_api", rate_limit="1次/小时"),
        _base_api(api_name="manual_api"),
        _base_api(api_name="tscoded_api",
                  input_params=[{"name": "ts_code", "required": True}]),
        {"api_name": "overdraft_api", "min_points": 99999, "rate_limit": None,
         "is_premium": False, "input_params": [],
         "_project": {"classification": {
             "rule": 2, "rule_label": "overdraft", "usable": True,
             "effective_rate": 1, "recommended_interval_ms": 86400000,
             "note": "跨积分", "overdraft": True, "max_retries": 1}}},
    ], ensure_ascii=False, indent=2), encoding="utf-8")
    monkeypatch.setattr(classify_apis, "INDEX_FILE", idx_file)
    monkeypatch.setattr(classify_apis, "load_config",
                        lambda: {"tushare_points": 5000,
                                 "tushare_rate_limit": 500,
                                 "exclude_apis": ["manual_api"]})
    return idx_file


def test_classify_main_writeback_and_idempotent(index_env):
    classify_apis.main()
    data1 = json.loads(index_env.read_text(encoding="utf-8"))
    by_name = {a["api_name"]: a for a in data1}

    assert by_name["ok_api"]["_project"]["classification"]["rule"] == 1
    assert "classification" not in by_name["poor_api"]["_project"]
    assert "classification" not in by_name["paid_api"]["_project"]
    assert "classification" not in by_name["slow_api"]["_project"]
    assert "classification" not in by_name["manual_api"]["_project"]
    assert "classification" not in by_name["tscoded_api"]["_project"]

    od = by_name["overdraft_api"]["_project"]["classification"]
    assert od.get("overdraft") is True

    assert not any(k.startswith("_") and k != "_project"
                   for a in data1 for k in a)
    assert not list(index_env.parent.glob("*.bak.json"))

    classify_apis.main()
    assert json.loads(index_env.read_text(encoding="utf-8")) == data1


# ── sql_type ──

def test_sql_type_mapping():
    assert sql_type("str") == "VARCHAR"
    assert sql_type(None) == "VARCHAR"
    assert sql_type("datetime") == "VARCHAR"
    assert sql_type("float") == "DOUBLE"
    assert sql_type("int") == "BIGINT"
    assert sql_type("integer") == "BIGINT"
    assert sql_type("unknown_type") == "VARCHAR"
    assert sql_type("FLOAT") == "DOUBLE"


# ── infer_pk 优先级（pk_override / no_pk / driver 路径） ──

def test_infer_pk_pk_override_beats_pattern():
    api = {"output_params": [{"name": "ts_code"}, {"name": "trade_date"}],
           "_project": {"pk_override": "(ts_code, trade_date, extra)"}}
    assert infer_pk(api) == "(ts_code, trade_date, extra)"


def test_infer_pk_no_pk_beats_override_and_pattern():
    api = {"output_params": [{"name": "ts_code"}, {"name": "trade_date"}],
           "_project": {"no_pk": True,
                        "pk_override": "(ts_code, trade_date)"}}
    assert infer_pk(api) is None


def test_infer_pk_driver_force_required_prefix():
    api = {
        "api_name": "dividend",
        "output_params": [{"name": "ts_code"}, {"name": "ann_date"}],
        "input_params": [{"name": "ts_code", "required": False}],
        "_project": {"param_fixes": {"force_required": ["dividend.ts_code"]}},
    }
    assert infer_pk(api, {"date_mode": "once"}) == "(ts_code, ann_date)"


# ── _quote_name / 关键字覆盖 ──

def test_quote_name_sqlite_keywords_unquoted_break_sql():
    for kw in ("to", "collate", "constraint", "escape", "notnull",
               "returning", "window", "match", "natural", "if"):
        assert _quote_name(kw).startswith('"'), kw


def test_quote_name_case_insensitive_keywords():
    assert _quote_name("Order") == '"Order"'
    assert _quote_name("GROUP") == '"GROUP"'


def test_quote_name_embedded_double_quote():
    assert _quote_name('a"b') == '"a""b"'


def test_generate_table_ddl_keyword_column_executes():
    api = {"api_name": "t1", "output_params": [{"name": "to", "type": "str"},
                                               {"name": "v", "type": "float"}]}
    ddl = generate_table_ddl(api)
    assert 'IF NOT EXISTS' in ddl
    conn = connect(":memory:")
    execute_script(conn, ddl)
    conn.execute('INSERT INTO "t1" ("to", v) VALUES (?, ?)', ("x", 1.0))
    conn.close()


def test_generate_table_ddl_no_output_params_empty():
    assert generate_table_ddl({"api_name": "t", "output_params": []}) == ""


# ── generate_registry 键与 maintain.py 消费对齐 ──

def _entry_value(entry_code: str, key: str):
    node = ast.parse(entry_code.strip().rstrip(",")).body[0].value
    return {k.value: ast.literal_eval(v) for k, v in zip(node.keys, node.values)}[key]


def _registry_entries(code: str) -> list[str]:
    lines = [ln for ln in code.splitlines() if ln.startswith("    {")]
    assert code.startswith("# 自动生成")
    assert code.splitlines()[1] == "REGISTRY = ["
    assert code.splitlines()[-1] == "]"
    return lines


def test_registry_driver_partition_null_pk_keep_passthrough():
    api = {
        "api_name": "stk_holdernumber",
        "output_params": [{"name": "ts_code"}, {"name": "ann_date"}],
        "_project": {
            "classification": {"rule": 1, "usable": True},
            "no_pk": True,
            "date_col": "ann_date",
            "driver": {"source_table": "stk_factor_pro",
                       "source_column": "ts_code", "date_mode": "once"},
            "upsert": {"partition_key": "ts_code"},
        },
    }
    code = generate_registry([api])
    entry = _registry_entries(code)[0]
    assert _entry_value(entry, "date_col") == "ann_date"
    assert _entry_value(entry, "driver") == {"source_table": "stk_factor_pro",
                                             "source_column": "ts_code",
                                             "date_mode": "once"}
    assert _entry_value(entry, "partition_key") == "ts_code"


def test_registry_date_col_from_ann_date_pk():
    api = {
        "api_name": "repurchase",
        "output_params": [{"name": "ts_code"}, {"name": "ann_date"}],
        "_project": {"classification": {"rule": 1, "usable": True}},
    }
    code = generate_registry([api])
    entry = _registry_entries(code)[0]
    assert _entry_value(entry, "date_col") == "ann_date"


def test_registry_null_pk_keep_and_default_params():
    api = {
        "api_name": "dividend",
        "output_params": [{"name": "ts_code"}, {"name": "ann_date"},
                          {"name": "div_proc"}],
        "_project": {
            "classification": {"rule": 1, "usable": True},
            "pk_override": "(ts_code, ann_date, div_proc)",
            "upsert": {"null_pk_keep": True},
        },
    }
    code = generate_registry([api])
    entry = _registry_entries(code)[0]
    assert _entry_value(entry, "null_pk_keep") is True
    assert _entry_value(entry, "date_col") == "ann_date"


def test_registry_skips_rule3_6_and_unusable():
    apis = [
        {"api_name": "r3", "output_params": [{"name": "ts_code"}],
         "_project": {"classification": {"rule": 3, "usable": True}}},
        {"api_name": "r6", "output_params": [{"name": "ts_code"}],
         "_project": {"classification": {"rule": 6, "usable": False}}},
        {"api_name": "r2u", "output_params": [{"name": "ts_code"}],
         "_project": {"classification": {"rule": 2, "usable": False}}},
     ]
    code = generate_registry(apis)
    assert _registry_entries(code) == []


def test_schema_and_registry_dedup_by_api_name():
    api = {"api_name": "dup", "output_params": [{"name": "ts_code"}],
           "_project": {"classification": {"rule": 1, "usable": True}}}
    sql = generate_schema([api, dict(api)])
    assert sql.count('CREATE TABLE IF NOT EXISTS "dup"') == 1
    reg = generate_registry([api, dict(api)])
    assert len(_registry_entries(reg)) == 1


# ── 派生视图 DDL（defect 3：stk_holdertrade_agg 带符号 + 契约持久化） ──

VIEW_NAMES = ("dividend_grid", "stk_holdernumber_agg",
              "stk_holdertrade_agg", "pledge_detail_agg", "top_inst_agg")


def test_generate_schema_embeds_derived_views():
    sql = generate_schema([])
    for view in VIEW_NAMES:
        assert f"DROP VIEW IF EXISTS {view};" in sql
        assert f"CREATE VIEW {view} AS" in sql
    assert "CASE WHEN in_de = 'DE' THEN -change_vol" in sql


@pytest.fixture()
def view_conn():
    conn = connect(":memory:")
    execute_script(
        conn,
        """CREATE TABLE stk_holdertrade (
             ts_code VARCHAR, ann_date VARCHAR, in_de VARCHAR,
             change_vol DOUBLE, change_ratio DOUBLE);
           CREATE TABLE pledge_detail (
             ts_code VARCHAR, ann_date VARCHAR, p_total_ratio DOUBLE, h_total_ratio DOUBLE);
           CREATE TABLE stk_holdernumber (
             ts_code VARCHAR, ann_date VARCHAR, holder_num DOUBLE);
           CREATE TABLE dividend (
             ts_code VARCHAR, div_proc VARCHAR, cash_div DOUBLE, ex_date VARCHAR);
           CREATE TABLE top_inst (
             ts_code VARCHAR, trade_date VARCHAR, exalter VARCHAR, side VARCHAR,
             buy DOUBLE, sell DOUBLE, net_buy DOUBLE);"""
    )
    execute_script(conn, DERIVED_VIEWS_DDL)
    yield conn
    conn.close()


def test_top_inst_agg_institution_rows_only(view_conn):
    """多席位：机构口径只取 exalter='机构专用'，买入占比=机构买入/全体席位买入。"""
    view_conn.execute(
        "INSERT INTO top_inst VALUES ('000001.SZ','20240605','机构专用','0',300.0,100.0,200.0)")
    view_conn.execute(
        "INSERT INTO top_inst VALUES ('000001.SZ','20240605','某营业部','0',100.0,400.0,-300.0)")
    net, rate = view_conn.execute(
        "SELECT inst_net_buy, inst_buy_rate FROM top_inst_agg "
        "WHERE ts_code='000001.SZ'").fetchone()
    assert net == 200.0
    assert rate == pytest.approx(0.75)  # 300 / (300+100)


def test_stk_holdertrade_agg_signs_de_as_negative(view_conn):
    view_conn.execute(
        "INSERT INTO stk_holdertrade VALUES ('600267.SH','20230520','DE',100.0,47.10)")
    view_conn.execute(
        "INSERT INTO stk_holdertrade VALUES ('000001.SZ','20240101','IN',200.0,3.0)")
    vol, ratio = view_conn.execute(
        "SELECT change_vol, change_ratio FROM stk_holdertrade_agg "
        "WHERE ts_code='600267.SH'").fetchone()
    assert vol == -100.0
    assert ratio == pytest.approx(-47.10)


def test_stk_holdertrade_agg_mixed_direction_net(view_conn):
    view_conn.execute(
        "INSERT INTO stk_holdertrade VALUES ('000002.SZ','20240101','IN',100.0,2.0)")
    view_conn.execute(
        "INSERT INTO stk_holdertrade VALUES ('000002.SZ','20240101','DE',100.0,4.0)")
    vol, ratio = view_conn.execute(
        "SELECT change_vol, change_ratio FROM stk_holdertrade_agg "
        "WHERE ts_code='000002.SZ'").fetchone()
    assert vol == 0.0
    assert ratio == pytest.approx(-1.0)   # (100*2 - 100*4) / 200


# ── top_inst：逐席位多行（defect 5），no_pk + 交易日分区替换 ──

def test_top_inst_no_pk_allows_multi_seat_rows():
    api = {
        "api_name": "top_inst",
        "output_params": [{"name": "trade_date"}, {"name": "ts_code"},
                          {"name": "exalter"}, {"name": "side"},
                          {"name": "buy"}, {"name": "net_buy"}],
        "_project": {
            "classification": {"rule": 1, "usable": True},
            "no_pk": True,
            "date_col": "trade_date",
            "upsert": {"partition_key": "trade_date"},
        },
    }
    assert infer_pk(api) is None
    assert "PRIMARY KEY" not in generate_table_ddl(api)
    entry = _registry_entries(generate_registry([api]))[0]
    assert _entry_value(entry, "date_col") == "trade_date"
    assert _entry_value(entry, "partition_key") == "trade_date"


def test_real_top_inst_schema_is_multi_seat():
    from database.utils import load_api_registry
    api = next(a for a in load_api_registry() if a["api_name"] == "top_inst")
    assert api["_project"].get("no_pk") is True
    assert "PRIMARY KEY" not in generate_table_ddl(api)


# ── inject_registry（tmp 副本，绝不触碰真实 database/etl.py） ──

ETL_TEMPLATE = '''"""ETL."""
from __future__ import annotations

from database.engine import Connection

# ---------- REGISTRY ----------
# 自动生成，勿手工编辑。运行 scripts/generate_schema.py 重新生成
REGISTRY = [
    {{"api": "old", "table": "old"}},
]

def log_pull(conn, table, date_val, ok):
    pass
'''


def _write_etl(tmp_path, content=None):
    p = tmp_path / "etl.py"
    p.write_text(ETL_TEMPLATE.format() if content is None else content,
                 encoding="utf-8")
    return str(p)


def test_inject_registry_replaces_block_and_compiles(tmp_path):
    p = _write_etl(tmp_path)
    reg = generate_registry([
        {"api_name": "new_api", "output_params": [{"name": "ts_code"},
                                                  {"name": "trade_date"}],
         "_project": {"classification": {"rule": 1, "usable": True}}},
    ])
    inject_registry(p, reg)
    src = open(p, encoding="utf-8").read()
    compile(src, p, "exec")
    assert '"new_api"' in src
    assert '"old"' not in src
    assert "def log_pull" in src


def test_inject_registry_idempotent(tmp_path):
    p = _write_etl(tmp_path)
    reg = generate_registry([
        {"api_name": "a1", "output_params": [{"name": "ts_code"}],
         "_project": {"classification": {"rule": 1, "usable": True}}},
    ])
    inject_registry(p, reg)
    first = open(p, encoding="utf-8").read()
    inject_registry(p, reg)
    assert open(p, encoding="utf-8").read() == first


def test_inject_registry_missing_marker_raises_and_file_untouched(tmp_path):
    p = _write_etl(tmp_path, 'REGISTRY = [\n]\n')
    before = open(p, encoding="utf-8").read()
    with pytest.raises(ValueError):
        inject_registry(p, "REGISTRY = [\n]\n")
    assert open(p, encoding="utf-8").read() == before


def test_inject_registry_invalid_python_rejected_file_untouched(tmp_path):
    p = _write_etl(tmp_path)
    before = open(p, encoding="utf-8").read()
    with pytest.raises(ValueError):
        inject_registry(p, 'REGISTRY = [{"api": "x"\n')
    assert open(p, encoding="utf-8").read() == before


# ── 端到端：真实 api_index.json 产物稳定性（只读） ──

def _real_classified():
    from database.utils import load_api_registry
    idx = load_api_registry()
    return [api for api in idx
            if api.get("_project", {}).get("classification", {}).get("usable")
            and api.get("_project", {}).get("classification", {}).get("rule")
            in (1, 2)]


def test_real_products_reproducible():
    from database.utils import load_api_registry
    root = classify_apis.PROJECT_ROOT
    api_list = _real_classified()

    on_disk_sql = (root / "database" / "schema.sql").read_text(encoding="utf-8")
    assert generate_schema(api_list) == on_disk_sql

    etl_src = (root / "database" / "etl.py").read_text(encoding="utf-8")
    start = etl_src.index("REGISTRY = [")
    end = etl_src.index("\n\n", start)
    on_disk_reg = etl_src[start:end + 1].rstrip("\n")
    gen_reg = generate_registry(api_list).split("\n", 1)[1].rstrip("\n")
    assert gen_reg == on_disk_reg

    entry_keys = set()
    for api in api_list:
        entry_keys.add(api["api_name"])
    reg_names = set()
    for node in ast.parse(etl_src).body:
        if isinstance(node, ast.Assign) and any(
                getattr(t, "id", None) == "REGISTRY" for t in node.targets):
            for el in node.value.elts:
                kw = {k.value for k in el.keys}
                assert "api" in kw and "table" in kw
                reg_names.add(ast.literal_eval(
                    next(v for k, v in zip(el.keys, el.values)
                         if k.value == "api")))
    assert reg_names == entry_keys


def test_real_classification_idempotent():
    from database.utils import load_api_registry
    idx = load_api_registry()
    exclude = set(classify_apis.load_config().get("exclude_apis", []))
    points = classify_apis.load_config().get("tushare_points", 2100)
    rate = classify_apis.load_config().get("tushare_rate_limit", 200)
    for api in idx:
        old = api.get("_project", {}).get("classification")
        if old and old.get("overdraft"):
            continue
        c = classify(api, points, rate)
        if (not c["_usable"] or c["_rule"] == 3 or api["api_name"] in exclude
                or any(p["name"] == "ts_code" and p.get("required")
                       for p in api.get("input_params", []))):
            expected = None
        else:
            expected = {"rule": c["_rule"], "rule_label": c["_rule_label"],
                        "usable": c["_usable"],
                        "effective_rate": c["_effective_rate"],
                        "recommended_interval_ms":
                            c["_recommended_interval_ms"]}
            if c.get("_note"):
                expected["note"] = c["_note"]
        assert old == expected, api["api_name"]
