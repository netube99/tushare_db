"""2026-10-12 审查证据；编号和关闭条件见 docs/code-quality-baseline.md。"""
from __future__ import annotations

import json
import struct
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from database.client import DataClient, TushareError
from database.engine import Connection, connect
from database.etl import REGISTRY
from database.utils import upsert_df
from qlib_export import CalendarSync, FeatureSync, IncrementalSync, init_sync_log
from qlib_export.specs import build_field_map
from scripts import maintain as m
from scripts import refresh_national_team_daily as nt
from scripts.generate_schema import INFRA_DDL, generate_table_ddl
from database.engine import execute_script

ROOT = Path(__file__).resolve().parents[1]


def known_issue(issue: str):
    return pytest.mark.xfail(
        strict=True, raises=AssertionError,
        reason=f"{issue}: 已确认未修复；修复后移除标记并更新审查台账",
    )


@pytest.fixture(autouse=True)
def quiet_json_log(monkeypatch):
    monkeypatch.setattr(
        "database.logger.get_json_logger",
        lambda: SimpleNamespace(write=lambda entry: None),
    )


@pytest.fixture
def conn():
    with connect(":memory:") as db:
        yield db


def api_spec(name: str) -> dict:
    apis = json.loads((ROOT / "api_index.json").read_text(encoding="utf-8"))
    return next(api for api in apis if api["api_name"] == name)


def registry_entry(name: str) -> dict:
    return next(dict(entry) for entry in REGISTRY if entry["api"] == name)


@pytest.mark.parametrize("previous_ok", [1, 2])
def test_existing_stock_announcements_eventually_refresh(conn, monkeypatch, previous_ok):
    execute_script(conn, INFRA_DDL)
    conn.execute("CREATE TABLE stk_factor_pro (ts_code VARCHAR, trade_date VARCHAR)")
    conn.execute("INSERT INTO stk_factor_pro VALUES ('000001.SZ', '20261009')")
    conn.execute(generate_table_ddl(api_spec("dividend")))
    if previous_ok == 1:
        conn.execute("""
            INSERT INTO dividend (ts_code,ann_date,div_proc)
            VALUES ('000001.SZ','20260801','实施')
        """)
    conn.execute(
        "INSERT INTO pull_log (table_name,date_val,ok,last_try) VALUES (?,?,?,?)",
        ("dividend", "000001.SZ__once__", previous_ok, "2026-08-01 00:00:00"),
    )
    monkeypatch.setattr(m, "REGISTRY", [registry_entry("dividend")])
    monkeypatch.setattr(m, "_verify", lambda db: None)
    monkeypatch.setattr(m, "_write_run_end", lambda *args: None)

    dc = SimpleNamespace(
        _daily_cooldown_until={},
        dividend=lambda **params: pd.DataFrame([
            {"ts_code": "000001.SZ", "ann_date": "20260801", "div_proc": "实施"},
            {"ts_code": "000001.SZ", "ann_date": "20261008", "div_proc": "实施"},
        ]),
    )
    m._cmd_daily(
        conn, dc, SimpleNamespace(since="20200101", until="20261009"),
        {"backfill_since": "20200101"}, "audit", 0.0,
    )
    assert conn.execute("SELECT MAX(ann_date) FROM dividend").fetchone()[0] == "20261008"


@pytest.mark.parametrize("table,records", [
    ("block_trade", [
        {"ts_code": "000001.SZ", "trade_date": "20261009", "price": 10.0, "vol": 100.0},
        {"ts_code": "000001.SZ", "trade_date": "20261009", "price": 11.0, "vol": 200.0},
    ]),
    ("kpl_concept_cons", [
        {"ts_code": "000365.KP", "trade_date": "20261009", "con_code": "600266.SH"},
        {"ts_code": "000365.KP", "trade_date": "20261009", "con_code": "600549.SH"},
    ]),
    ("top_list", [
        {"ts_code": "000001.SZ", "trade_date": "20261009", "reason": "单日涨幅"},
        {"ts_code": "000001.SZ", "trade_date": "20261009", "reason": "三日累计涨幅"},
    ]),
])
def test_source_detail_grain_is_preserved(conn, table, records):
    conn.execute(generate_table_ddl(api_spec(table)))
    upsert_df(conn, table, pd.DataFrame(records))
    assert conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] == 2


def test_incomplete_pagination_cannot_replace_a_complete_partition(conn, monkeypatch):
    execute_script(conn, INFRA_DDL)
    conn.execute(generate_table_ddl(api_spec("top_inst")))
    conn.executemany(
        "INSERT INTO top_inst (trade_date,ts_code,exalter) VALUES ('20261009',?,?)",
        [("000001.SZ", "原席位1"), ("000002.SZ", "原席位2"), ("000003.SZ", "原席位3")],
    )
    entry = registry_entry("top_inst")
    monkeypatch.setattr(m, "REGISTRY", [entry])
    client = DataClient.__new__(DataClient)

    def fetch(api: str, params: dict, force_refresh: bool) -> pd.DataFrame:
        if params["offset"]:
            raise TushareError(-1, "3次重试全部失败")
        return pd.DataFrame([
            {"trade_date": "20261009", "ts_code": "000001.SZ", "exalter": "新席位1"},
            {"trade_date": "20261009", "ts_code": "000002.SZ", "exalter": "新席位2"},
        ])

    client._request_single = fetch
    dc = SimpleNamespace(top_inst=lambda **params: client._paginate_offset(
        "top_inst", params, 2, False,
    ))
    m._pull_once(conn, dc, entry, "20261009", "trade_date", "", trade_date="20261009")
    status = conn.execute("SELECT ok FROM pull_log").fetchone()[0]
    remaining = conn.execute("SELECT exalter FROM top_inst ORDER BY exalter").fetchall()
    original = [
        ("原席位1",), ("原席位2",), ("原席位3",),
    ]
    assert status != 1 and remaining == original, f"{status=}, {remaining=}"


def holder_schema(conn: Connection) -> None:
    conn.execute(generate_table_ddl(api_spec("top10_floatholders")))


def add_holder(conn: Connection, ann: str | None, end: str, amount: float,
               name: str = "中央汇金投资有限责任公司") -> None:
    conn.execute(
        "INSERT INTO top10_floatholders "
        "(ts_code,ann_date,end_date,holder_name,hold_amount,hold_ratio) VALUES (?,?,?,?,?,?)",
        ("000001.SZ", ann, end, name, amount, amount / 10),
    )


def test_national_team_does_not_sum_two_reporting_periods(conn):
    holder_schema(conn)
    add_holder(conn, "20260430", "20251231", 10.0)
    add_holder(conn, "20260430", "20260331", 20.0)
    event = nt.load_events(conn).iloc[0]
    assert event["nt_amount_sum"] == 20.0
    assert event["nt_ratio_sum"] == 2.0
    assert event["nt_cnt"] == 1


def test_all_national_team_names_obey_null_announcement_filter(conn):
    holder_schema(conn)
    add_holder(conn, None, "20260331", 20.0, "中国证券金融股份有限公司")
    assert nt.load_events(conn).empty


def export_fixture(conn: Connection, output: Path, dates: tuple[str, ...]) -> tuple:
    conn.execute("CREATE TABLE trade_cal (exchange VARCHAR,cal_date VARCHAR,is_open BIGINT)")
    conn.executemany("INSERT INTO trade_cal VALUES ('SSE',?,1)", [(d,) for d in dates])
    conn.execute("CREATE TABLE stock_basic (ts_code VARCHAR,list_date VARCHAR,delist_date VARCHAR)")
    conn.execute("INSERT INTO stock_basic VALUES ('000001.SZ','19910403',NULL)")
    conn.execute("CREATE TABLE stk_factor_pro (ts_code VARCHAR,trade_date VARCHAR)")
    conn.execute("CREATE TABLE audit_prices (ts_code VARCHAR,trade_date VARCHAR,val DOUBLE)")
    init_sync_log(conn)
    cfg = {
        "source_table": "audit_prices", "inst_col": "ts_code", "date_col": "trade_date",
        "inst_type": "stock", "fields": [{"bin_name": "audit_val", "tushare_col": "val"}],
    }
    cal = CalendarSync(output)
    cal.full_init(conn)
    fs = FeatureSync(output, cal, conn)
    return cfg, cal, fs, IncrementalSync(output, cal, conn, fs)


def bin_value(path: Path, calendar: list[str], date: str) -> float | None:
    """独立用 struct 解码；预期数值不调用被测计算函数推导。"""
    if not path.exists():
        return None
    data = path.read_bytes()
    start = int(struct.unpack_from("<f", data)[0])
    offset = 4 * (calendar.index(date) - start + 1)
    if offset < 4 or offset + 4 > len(data):
        return None
    return struct.unpack_from("<f", data, offset)[0]


@known_issue("QL-001")
def test_daily_sync_reflects_same_count_historical_correction(conn, tmp_path):
    cfg, cal, fs, inc = export_fixture(conn, tmp_path, ("20261009",))
    conn.execute("INSERT INTO audit_prices VALUES ('000001.SZ','20261009',1)")
    fs.full_convert([cfg], quiet=True)
    conn.execute("UPDATE audit_prices SET val=9")
    inc.daily_sync([cfg], quiet=True)
    path = tmp_path / "features/sz000001/audit_val.day.bin"
    assert bin_value(path, cal.calendar, "2026-10-09") == 9.0


@known_issue("QL-001")
def test_later_weekend_announcement_updates_existing_monday_slot(conn, tmp_path):
    cfg, cal, fs, inc = export_fixture(conn, tmp_path, ("20261009", "20261012"))
    conn.execute("INSERT INTO audit_prices VALUES ('000001.SZ','20261010',1)")
    fs.full_convert([cfg], quiet=True)
    conn.execute("INSERT INTO audit_prices VALUES ('000001.SZ','20261011',2)")
    inc.daily_sync([cfg], quiet=True)
    path = tmp_path / "features/sz000001/audit_val.day.bin"
    assert bin_value(path, cal.calendar, "2026-10-12") == 2.0


@known_issue("QL-002")
def test_non_append_calendar_change_keeps_dates_attached_to_values(conn, tmp_path):
    cfg, cal, fs, inc = export_fixture(conn, tmp_path, ("20261008", "20261012"))
    conn.executemany("INSERT INTO audit_prices VALUES ('000001.SZ',?,?)", [
        ("20261008", 1.0), ("20261012", 3.0),
    ])
    fs.full_convert([cfg], quiet=True)
    cal_path = tmp_path / "calendars/day.txt"
    old_calendar = cal_path.read_bytes()
    path = tmp_path / "features/sz000001/audit_val.day.bin"
    old_bin = path.read_bytes()
    conn.execute("INSERT INTO trade_cal VALUES ('SSE','20261009',1)")
    try:
        inc.daily_sync([cfg], quiet=True)
    except (ValueError, RuntimeError) as error:
        if not any(word in str(error).lower() for word in ("日历", "calendar")):
            raise
        assert cal_path.read_bytes() == old_calendar
        assert path.read_bytes() == old_bin
        return
    assert bin_value(path, cal.calendar, "2026-10-12") == 3.0
    assert bin_value(path, cal.calendar, "2026-10-09") != 3.0


def vwap_array(conn: Connection, tmp_path: Path, table: str, inst_type: str,
               amount: float, vol: float) -> float:
    conn.execute(f'CREATE TABLE "{table}" (ts_code VARCHAR,trade_date VARCHAR,amount DOUBLE,vol DOUBLE)')
    cfg = next(t for t in build_field_map(conn) if t["source_table"] == table)
    assert cfg["inst_type"] == inst_type
    cal = CalendarSync(tmp_path)
    cal.calendar = ["2026-10-09"]
    fs = FeatureSync(tmp_path, cal, conn)
    arrays = fs._rows_to_arrays(
        [("20261009", amount, vol)], ["trade_date", "amount", "vol"], cfg, cfg["fields"],
    )
    index = next(i for i, field in enumerate(cfg["fields"]) if field["bin_name"] == "vwap")
    return float(arrays[index][0])


@known_issue("QL-003")
def test_convertible_bond_vwap_uses_its_own_units(conn, tmp_path):
    # 一手 10 张，每张 100 元；总成交额 1000 元 = 0.1 万元，均价为 100 元。
    assert vwap_array(conn, tmp_path, "cb_factor_pro", "cb", 0.1, 1.0) == pytest.approx(100.0)


@known_issue("QL-004")
def test_new_output_directory_cannot_inherit_other_directory_completion(conn, tmp_path):
    cfg, cal, fs, inc = export_fixture(conn, tmp_path / "a", ("20261009",))
    conn.execute("INSERT INTO audit_prices VALUES ('000001.SZ','20261009',1)")
    fs.full_convert([cfg], quiet=True)
    other_cal = CalendarSync(tmp_path / "b")
    other_cal.full_init(conn)
    other_fs = FeatureSync(tmp_path / "b", other_cal, conn)
    try:
        other_fs.full_convert([cfg], quiet=True)
    except (ValueError, RuntimeError) as error:
        if not any(word in str(error).lower() for word in ("输出", "目录", "output")):
            raise
        return
    path = tmp_path / "b/features/sz000001/audit_val.day.bin"
    assert bin_value(path, other_cal.calendar, "2026-10-09") == 1.0


def test_stock_raw_vwap_unit_control(conn, tmp_path):
    # 一手 100 股，每股 10 元；成交额 1000 元 = 1 千元。
    assert vwap_array(conn, tmp_path, "stk_factor_pro", "stock", 1.0, 1.0) == 10.0


def test_national_team_single_reporting_period_control(conn):
    holder_schema(conn)
    add_holder(conn, "20260430", "20260331", 20.0)
    event = nt.load_events(conn).iloc[0]
    assert (event["nt_amount_sum"], event["nt_ratio_sum"], event["nt_cnt"]) == (20.0, 2.0, 1)


def test_regular_daily_append_control(conn, tmp_path):
    cfg, cal, fs, inc = export_fixture(conn, tmp_path, ("20261008", "20261009"))
    conn.execute("INSERT INTO audit_prices VALUES ('000001.SZ','20261008',1)")
    fs.full_convert([cfg], quiet=True)
    conn.execute("INSERT INTO audit_prices VALUES ('000001.SZ','20261009',2)")
    inc.daily_sync([cfg], quiet=True)
    path = tmp_path / "features/sz000001/audit_val.day.bin"
    assert bin_value(path, cal.calendar, "2026-10-08") == 1.0
    assert bin_value(path, cal.calendar, "2026-10-09") == 2.0


def test_derived_sql_views_arithmetic_control(conn):
    execute_script(conn, (ROOT / "database/schema.sql").read_text(encoding="utf-8"))
    conn.execute("""
        INSERT INTO dividend (ts_code,ex_date,div_proc,cash_div) VALUES
        ('000001.SZ','20261009','实施',0.1),
        ('000001.SZ','20261009','实施',0.2),
        ('000001.SZ','20261009','股东大会通过',0.8)
    """)
    conn.execute("""
        INSERT INTO top_inst (ts_code,trade_date,exalter,buy,net_buy) VALUES
        ('000001.SZ','20261009','机构专用',12,8),
        ('000001.SZ','20261009','其他席位',3,-1)
    """)
    assert conn.execute("SELECT cash_div FROM dividend_grid").fetchone()[0] == pytest.approx(0.3)
    # stk_holdertrade_agg 段 2026-10-24 随视图退役移除（带符号算术控制
    # 迁至 ddup tests/test_tushare_align.py::test_same_day_merge_semantics）
    institution = conn.execute("SELECT inst_net_buy,inst_buy_rate FROM top_inst_agg").fetchone()
    assert tuple(institution) == pytest.approx((8.0, 0.8))
