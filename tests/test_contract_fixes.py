"""DB-001～005 的修复验收：新鲜度预算、明细幂等、分页失败、报告期与旧库恢复。"""
from __future__ import annotations

import copy
import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from database import engine
from database.client import DataClient, DailyLimitError, IncompleteFetchError, TushareError
from database.etl import REGISTRY
from scripts import maintain as m
from scripts import repair_data_contracts as repair
from scripts import refresh_national_team_daily as nt
from scripts.generate_schema import INFRA_DDL, generate_table_ddl

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 12, 21, tzinfo=ZoneInfo("Asia/Shanghai"))


def spec(name: str) -> dict:
    return next(a for a in json.loads((ROOT / "api_index.json").read_text()) if a["api_name"] == name)


def entry(name: str) -> dict:
    return copy.deepcopy(next(e for e in REGISTRY if e["api"] == name))


@pytest.fixture(autouse=True)
def log_and_clock(monkeypatch):
    monkeypatch.setattr("database.logger.get_json_logger", lambda: SimpleNamespace(write=lambda _: None))
    monkeypatch.setattr(m, "beijing_now", lambda: NOW)
    monkeypatch.setattr(m, "_verify", lambda _: None)
    monkeypatch.setattr(m, "_write_run_end", lambda *args: None)


@pytest.fixture
def conn():
    with engine.connect(":memory:") as db:
        engine.execute_script(db, INFRA_DDL)
        yield db


def once_setup(conn, monkeypatch, codes: list[str], budget: int = 1) -> dict:
    conn.execute("CREATE TABLE stk_factor_pro (ts_code VARCHAR,trade_date VARCHAR)")
    conn.executemany("INSERT INTO stk_factor_pro VALUES (?,'20261009')", [(c,) for c in codes])
    conn.execute(generate_table_ddl(spec("dividend")))
    e = entry("dividend")
    e["driver"].update(refresh_days=7, refresh_batch_size=budget)
    monkeypatch.setattr(m, "REGISTRY", [e])
    return e


def state(conn, code: str, ok: int, last_try: str | None, retries: int = 0):
    conn.execute("INSERT INTO pull_log VALUES ('dividend',?,?,?,?)",
                 (f"{code}__once__", ok, retries, last_try))


def run_daily(conn, dc):
    m._cmd_daily(conn, dc, SimpleNamespace(since="20200101", until="20261009"),
                 {"backfill_since": "20200101"}, "test", 0.0)


def test_refresh_budget_oldest_first_and_recent_stock_skipped(conn, monkeypatch):
    codes = ["000001.SZ", "000002.SZ", "000003.SZ"]
    once_setup(conn, monkeypatch, codes)
    state(conn, codes[0], 1, "2026-08-02 00:00:00")
    state(conn, codes[1], 2, "2026-08-01T00:00:00+08:00")
    state(conn, codes[2], 1, "2026-10-11 00:00:00")
    calls = []
    dc = SimpleNamespace(_daily_cooldown_until={},
                         dividend=lambda **kw: calls.append(kw) or pd.DataFrame())
    run_daily(conn, dc)
    assert calls == [{"ts_code": codes[1], "force_refresh": True}]
    run_daily(conn, dc)
    assert calls[-1] == {"ts_code": codes[0], "force_refresh": True}
    assert len(calls) == 2
    run_daily(conn, dc)
    assert len(calls) == 2


def test_backfill_keeps_resume_semantics_for_completed_once(conn, monkeypatch):
    e = once_setup(conn, monkeypatch, ["000001.SZ"])
    state(conn, "000001.SZ", 1, "2026-08-01 00:00:00")
    calls = []
    dc = SimpleNamespace(dividend=lambda **kw: calls.append(kw) or pd.DataFrame())
    m._run_domain_strategy(conn, dc, e, "20200101", "20261009")
    assert calls == []


def test_empty_refresh_clears_only_requested_stock_partition(conn, monkeypatch):
    codes = ["000001.SZ", "000002.SZ"]
    once_setup(conn, monkeypatch, codes)
    for code, date in zip(codes, ["2026-08-01 00:00:00", "2026-10-11 00:00:00"]):
        state(conn, code, 1, date)
        conn.execute("INSERT INTO dividend (ts_code,ann_date,div_proc) VALUES (?,'20260801','实施')", (code,))
    dc = SimpleNamespace(_daily_cooldown_until={}, dividend=lambda **kw: pd.DataFrame())
    run_daily(conn, dc)
    assert conn.execute("SELECT ts_code FROM dividend").fetchall() == [(codes[1],)]
    assert conn.execute("SELECT ok FROM pull_log WHERE date_val=?", (codes[0] + "__once__",)).fetchone()[0] == 2


def test_failed_refresh_preserves_data_and_is_not_retried_twice_in_one_daily(conn, monkeypatch):
    once_setup(conn, monkeypatch, ["000001.SZ"])
    state(conn, "000001.SZ", 1, "2026-08-01 00:00:00", retries=5)
    conn.execute("INSERT INTO dividend (ts_code,ann_date,div_proc) VALUES ('000001.SZ','20260801','实施')")
    calls = []

    def fail(**kw):
        calls.append(kw)
        raise TushareError(-1, "失败")

    run_daily(conn, SimpleNamespace(_daily_cooldown_until={}, dividend=fail))
    assert len(calls) == 1
    assert conn.execute("SELECT ann_date FROM dividend").fetchall() == [("20260801",)]
    assert tuple(conn.execute("SELECT ok,retry_count FROM pull_log").fetchone()) == (0, 0)
    run_daily(conn, SimpleNamespace(_daily_cooldown_until={}, dividend=fail))
    assert len(calls) == 2
    assert tuple(conn.execute("SELECT ok,retry_count FROM pull_log").fetchone()) == (0, 1)


def test_cooldown_preserves_completion_and_retry_budget(conn, monkeypatch):
    once_setup(conn, monkeypatch, ["000001.SZ"])
    state(conn, "000001.SZ", 1, "2026-08-01 00:00:00", retries=2)

    def cool(**kw):
        raise DailyLimitError(40203, "冷却")

    before = tuple(conn.execute("SELECT * FROM pull_log").fetchone())
    run_daily(conn, SimpleNamespace(_daily_cooldown_until={}, dividend=cool))
    assert tuple(conn.execute("SELECT * FROM pull_log").fetchone()) == before


def test_retry_cap_abandons_and_continues_other_domains(conn, monkeypatch):
    e = once_setup(conn, monkeypatch, ["000001.SZ", "000002.SZ"])
    state(conn, "000001.SZ", 0, "2026-08-01 00:00:00", retries=5)
    calls = []
    dc = SimpleNamespace(dividend=lambda **kw: calls.append(kw) or pd.DataFrame())
    m._run_domain_strategy(conn, dc, e, "20200101", "20261009", refresh_done=True)
    assert calls == [{"ts_code": "000002.SZ"}]
    assert conn.execute("SELECT ok FROM pull_log WHERE date_val='000001.SZ__once__'").fetchone()[0] == 3


@pytest.mark.parametrize("table", ["block_trade", "top_list"])
def test_identical_detail_rows_preserved_and_repeat_pull_idempotent(conn, monkeypatch, table):
    conn.execute(generate_table_ddl(spec(table)))
    e = entry(table)
    monkeypatch.setattr(m, "REGISTRY", [e])
    df = pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20261009"}] * 2)
    dc = SimpleNamespace(**{table: lambda **kw: df})
    for _ in range(2):
        assert m._pull_once(conn, dc, e, "20261009", "trade_date", "", trade_date="20261009")
        assert conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] == 2


def test_response_from_other_partition_cannot_delete_requested_partition(conn, monkeypatch):
    conn.execute(generate_table_ddl(spec("block_trade")))
    conn.execute("INSERT INTO block_trade (ts_code,trade_date) VALUES ('000001.SZ','20261009')")
    e = entry("block_trade")
    monkeypatch.setattr(m, "REGISTRY", [e])
    dc = SimpleNamespace(block_trade=lambda **kw: pd.DataFrame([
        {"ts_code": "000002.SZ", "trade_date": "20261008"},
    ]))
    assert not m._pull_once(conn, dc, e, "20261009", "trade_date", "", trade_date="20261009")
    assert conn.execute("SELECT ts_code FROM block_trade").fetchall() == [("000001.SZ",)]


def test_old_pk_contract_is_rejected_before_any_replacement(conn, monkeypatch):
    api = spec("block_trade")
    api["_project"].pop("no_pk")
    conn.execute(generate_table_ddl(api))
    conn.execute("INSERT INTO block_trade (ts_code,trade_date,vol) VALUES ('000001.SZ','20261009',100)")
    monkeypatch.setattr(m, "REGISTRY", [entry("block_trade")])
    assert not m._pull_and_store(conn, "block_trade", pd.DataFrame([
        {"ts_code": "000001.SZ", "trade_date": "20261009", "vol": 200},
    ]), "20261009", "block_trade", "trade_date")
    assert conn.execute("SELECT vol FROM block_trade").fetchone()[0] == 100


def test_same_first_row_is_not_enough_to_claim_duplicate_page():
    client = DataClient.__new__(DataClient)
    pages = iter([
        pd.DataFrame({"v": [1, 2]}), pd.DataFrame({"v": [1, 3]}), pd.DataFrame(),
    ])
    client._request_single = lambda *args: next(pages)
    assert client._paginate_offset("fake", {}, 2, False)["v"].tolist() == [1, 2, 1, 3]


def test_full_last_page_at_page_cap_is_incomplete():
    client = DataClient.__new__(DataClient)
    client._request_single = lambda api, kw, fresh: pd.DataFrame({"v": [kw["offset"]]})
    with pytest.raises(IncompleteFetchError, match="200 页") as error:
        client._paginate_offset("fake", {}, 1, False)
    assert len(error.value.partial_df) == 200


def add_holder(conn, ann: str, end: str, name: str, amount: float):
    conn.execute("INSERT INTO top10_floatholders (ts_code,ann_date,end_date,holder_name,hold_amount) "
                 "VALUES ('000001.SZ',?,?,?,?)", (ann, end, name, amount))


def test_latest_same_day_report_can_exit_despite_older_report_presence(conn):
    conn.execute(generate_table_ddl(spec("top10_floatholders")))
    add_holder(conn, "20260130", "20250930", "中央汇金投资有限责任公司", 10)
    add_holder(conn, "20260430", "20251231", "中央汇金投资有限责任公司", 15)
    add_holder(conn, "20260430", "20260331", "其他基金", 20)
    events = nt.load_events(conn)
    assert events[["ann_date", "nt_state"]].to_records(index=False).tolist() == [
        ("20260130", 1), ("20260430", 0),
    ]
    assert events.iloc[-1]["nt_amount_sum"] == 0


def test_late_old_report_cannot_overwrite_newer_period(conn):
    conn.execute(generate_table_ddl(spec("top10_floatholders")))
    add_holder(conn, "20260430", "20260331", "中央汇金投资有限责任公司", 20)
    add_holder(conn, "20260501", "20251231", "其他基金", 10)
    assert nt.load_events(conn)["ann_date"].tolist() == ["20260430"]


def test_national_team_missing_metrics_stay_null_and_exit_metrics_are_zero(conn):
    conn.execute(generate_table_ddl(spec("top10_floatholders")))
    add_holder(conn, "20260430", "20260331", "中央汇金投资有限责任公司", 20)
    add_holder(conn, "20260430", "20260331", "其他基金", 10)
    add_holder(conn, "20260830", "20260630", "其他基金", 10)
    events = nt.load_events(conn)
    assert events["nt_state"].tolist() == [1, 0]
    assert pd.isna(events.iloc[0]["nt_ratio_sum"])
    assert pd.isna(events.iloc[0]["nt_float_ratio_sum"])
    assert pd.isna(events.iloc[0]["nt_chg_sum"])
    assert events.iloc[0]["nt_amount_sum"] == 20
    assert events.iloc[1][nt.METRIC_COLS].tolist() == [0] * len(nt.METRIC_COLS)


def legacy_detail(conn, table: str = "block_trade") -> dict:
    api = spec(table)
    legacy = copy.deepcopy(api)
    legacy["_project"].pop("no_pk", None)
    legacy["_project"].pop("pk_override", None)
    conn.execute(generate_table_ddl(legacy))
    conn.execute(f'INSERT INTO "{table}" (ts_code,trade_date) VALUES (\'000001.SZ\',\'20261009\')')
    conn.execute("INSERT INTO pull_log (table_name,date_val,ok) VALUES (?,'20261009',1)", (table,))
    return api


def cache_page(root: Path, api: dict, date: str, df: pd.DataFrame, offset: int = 0):
    params = {"trade_date": date, "limit": api["max_rows"], "offset": offset}
    key = DataClient.__new__(DataClient)._make_key({"api_name": api["api_name"], "params": params, "fields": ""})
    target = root / api["api_name"] / f"{key}.pkl"
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(target)


def test_old_schema_migration_and_complete_cache_restore_preserves_duplicate_trades(conn, tmp_path):
    api = legacy_detail(conn)
    # 无法从缓存恢复的日期保留旧数据，并明确登记待重拉。
    conn.execute("INSERT INTO block_trade (ts_code,trade_date) VALUES ('000002.SZ','20261008')")
    conn.execute("INSERT INTO pull_log (table_name,date_val,ok) VALUES ('block_trade','20261008',1)")
    df = pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20261009", "vol": 100.0}] * 2)
    cache_page(tmp_path, api, "20261009", df)
    plan = repair.build_plan(conn, [api], None, tmp_path)[0]
    assert plan["cached_days"] == {"20261009": 2}
    assert plan["pending_days"] == ["20261008"]
    result = repair.apply_table(conn, api, plan, tmp_path)
    assert result["recovered_rows"] == 2
    assert engine.primary_key_cols(conn, "block_trade") == []
    assert conn.execute("SELECT COUNT(*) FROM block_trade").fetchone()[0] == 3
    assert conn.execute("SELECT date_val,ok FROM pull_log ORDER BY date_val").fetchall() == [
        ("20261008", 0), ("20261009", 1),
    ]


def test_kpl_pk_migration_and_two_members_restore(conn, tmp_path):
    api = legacy_detail(conn, "kpl_concept_cons")
    df = pd.DataFrame([
        {"ts_code": "000365.KP", "trade_date": "20261009", "con_code": code}
        for code in ["600266.SH", "600549.SH"]
    ])
    cache_page(tmp_path, api, "20261009", df)
    plan = repair.build_plan(conn, [api], None, tmp_path)[0]
    repair.apply_table(conn, api, plan, tmp_path)
    assert engine.primary_key_cols(conn, "kpl_concept_cons") == ["ts_code", "con_code", "trade_date"]
    assert conn.execute("SELECT con_code FROM kpl_concept_cons ORDER BY con_code").fetchall() == [
        ("600266.SH",), ("600549.SH",),
    ]


def test_migration_registers_uncached_history_without_pull_log_for_retry(conn):
    api = legacy_detail(conn)
    conn.execute("DELETE FROM pull_log WHERE table_name='block_trade'")
    plan = repair.build_plan(conn, [api], None)[0]
    assert plan["pending_days"] == ["20261009"]
    repair.apply_table(conn, api, plan, None)
    assert conn.execute("SELECT date_val,ok,retry_count,last_try FROM pull_log").fetchall() == [
        ("20261009", 0, 0, None),
    ]
    assert conn.execute("SELECT COUNT(*) FROM block_trade").fetchone()[0] == 1


def test_migration_failure_rolls_back_structure_rows_and_pull_state(conn, tmp_path):
    api = legacy_detail(conn, "kpl_concept_cons")
    # 旧库没有 con_code，新唯一键不接受 NULL；必须失败而非静默丢行。
    plan = repair.build_plan(conn, [api], None)[0]
    before = tuple(conn.execute("SELECT * FROM pull_log").fetchone())
    with pytest.raises(Exception, match="NOT NULL"):
        repair.apply_table(conn, api, plan, None)
    assert engine.primary_key_cols(conn, "kpl_concept_cons") == ["ts_code", "trade_date"]
    assert conn.execute("SELECT COUNT(*) FROM kpl_concept_cons").fetchone()[0] == 1
    assert tuple(conn.execute("SELECT * FROM pull_log").fetchone()) == before
    assert not engine.table_exists(conn, "__contract_repair_kpl_concept_cons")


def test_full_cached_page_without_next_page_cannot_be_used(conn, tmp_path):
    api = spec("block_trade")
    api["max_rows"] = 2
    df = pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20261009"}] * 2)
    cache_page(tmp_path, api, "20261009", df)
    assert repair.complete_cached_day(api, "20261009", tmp_path) is None
    cache_page(tmp_path, api, "20261009", df.iloc[:1], offset=2)
    assert len(repair.complete_cached_day(api, "20261009", tmp_path)) == 3


def test_cache_from_other_day_is_rejected(tmp_path):
    api = spec("block_trade")
    cache_page(tmp_path, api, "20261009", pd.DataFrame([
        {"ts_code": "000001.SZ", "trade_date": "20261008"},
    ]))
    assert repair.complete_cached_day(api, "20261009", tmp_path) is None


def test_backup_restores_original_constraint_data_and_state(conn, tmp_path):
    api = legacy_detail(conn)
    plan = repair.build_plan(conn, [api], None)[0]
    backup = tmp_path / "backup"
    repair.backup_tables(conn, [plan], backup)
    repair.apply_table(conn, api, plan, None)
    assert engine.primary_key_cols(conn, "block_trade") == []
    engine.execute_script(conn, (backup / "restore.sql").read_text())
    assert engine.primary_key_cols(conn, "block_trade") == ["ts_code", "trade_date"]
    assert conn.execute("SELECT COUNT(*) FROM block_trade").fetchone()[0] == 1
    assert conn.execute("SELECT ok FROM pull_log").fetchone()[0] == 1


def test_preview_main_does_not_write_database_or_create_backup(tmp_path, monkeypatch):
    path = tmp_path / "market.duckdb"
    with engine.connect(str(path)) as db:
        engine.execute_script(db, INFRA_DDL)
        api = legacy_detail(db)
    before = path.read_bytes()
    backup = tmp_path / "backup"
    monkeypatch.setattr(repair, "load_api_registry", lambda: [api])
    monkeypatch.setattr("sys.argv", ["repair", "--db", str(path), "--backup-dir", str(backup)])
    repair.main()
    assert path.read_bytes() == before
    assert not backup.exists()


def test_changed_cache_aborts_migration_and_keeps_old_data(conn, tmp_path):
    api = legacy_detail(conn)
    df = pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20261009"}] * 2)
    cache_page(tmp_path, api, "20261009", df)
    plan = repair.build_plan(conn, [api], None, tmp_path)[0]
    cache_page(tmp_path, api, "20261009", df.iloc[:1])
    with pytest.raises(RuntimeError, match="缓存已改变"):
        repair.apply_table(conn, api, plan, tmp_path)
    assert engine.primary_key_cols(conn, "block_trade") == ["ts_code", "trade_date"]
    assert conn.execute("SELECT COUNT(*) FROM block_trade").fetchone()[0] == 1
    assert conn.execute("SELECT ok FROM pull_log").fetchone()[0] == 1
