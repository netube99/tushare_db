"""核心整改的离线行为验收；预期值不由被测实现生成。"""
from collections import Counter
from contextlib import nullcontext
import json
import shutil
from pathlib import Path
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from database import engine
from database.client import DataClient
from database.etl import REGISTRY
from database.logger import JsonLogger
from scripts import maintain as m
from scripts.generate_schema import INFRA_DDL


@pytest.fixture
def conn():
    with engine.connect(":memory:") as db:
        engine.execute_script(db, INFRA_DDL)
        yield db


@pytest.fixture(autouse=True)
def isolated_logs(tmp_path, monkeypatch):
    log = JsonLogger(str(tmp_path / "logs"))
    monkeypatch.setattr("database.logger.get_json_logger", lambda: log)
    yield log
    if log._file:
        log._file.close()


def test_refresh_fetches_same_count_correction_but_resume_reuses_cache(conn, tmp_path, monkeypatch):
    entry = next(e for e in REGISTRY if e["api"] == "stk_factor_pro")
    conn.execute("CREATE TABLE stk_factor_pro(ts_code VARCHAR,trade_date VARCHAR,close DOUBLE,"
                 "PRIMARY KEY(ts_code,trade_date))")
    conn.execute("INSERT INTO trade_cal VALUES ('SSE','20261009',1,NULL)")
    monkeypatch.setattr(m, "REGISTRY", [entry])
    monkeypatch.setattr(m, "_write_run_end", lambda *a: None)
    dc = DataClient("test", cache_path=str(tmp_path / "cache"))
    dc._rule_config["stk_factor_pro"]["interval"] = 0
    dc._global_interval = 0
    calls = []

    def fetch(*args):
        calls.append(1)
        return pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20261009",
                              "close": 1.0 if len(calls) == 1 else 9.0}]), 1

    monkeypatch.setattr(dc, "_fetch_with_retry", fetch)
    m._run_trade_date_strategy(conn, dc, entry, "20261009", "20261009")
    conn.execute("DELETE FROM pull_log")  # 模拟写入状态前中断；续跑允许缓存
    m._run_trade_date_strategy(conn, dc, entry, "20261009", "20261009")
    assert len(calls) == 1
    m._cmd_refresh(conn, dc, SimpleNamespace(refresh=["stk_factor_pro", "20261009"]), "r", 0)
    assert conn.execute("SELECT close FROM stk_factor_pro").fetchone()[0] == 9.0
    assert len(calls) == 2


def test_completed_domain_history_uses_one_state_query(conn):
    entry = next(e for e in REGISTRY if e["api"] == "index_weight")
    months = [f"{y}{mo:02}" for y in range(2018, 2027) for mo in range(1, 13)
              if f"{y}{mo:02}" <= "202610"]
    conn.executemany("INSERT INTO trade_cal VALUES ('SSE',?,1,NULL)",
                     [(mo + "02",) for mo in months])
    conn.executemany("INSERT INTO pull_log(table_name,date_val,ok) VALUES ('index_weight',?,1)",
                     [(f"{code}_{mo}",) for code in entry["driver"]["values"] for mo in months])
    counts = Counter()
    execute = conn.execute

    def measured(sql, params=None):
        if "SELECT" in sql.upper() and "FROM pull_log" in sql:
            counts["state_reads"] += 1
        return execute(sql, params)

    conn.execute = measured
    m._run_domain_strategy(conn, object(), entry, "20180101", "20261024")
    assert counts["state_reads"] == 1


def test_domain_latest_empty_retry_and_completed_history_control(conn, monkeypatch):
    entry = {"api": "index_weight", "table": "index_weight", "date_col": "trade_date",
             "driver": {"values": ["000300.SH", "000905.SH"], "date_mode": "monthly"}}
    conn.executemany("INSERT INTO trade_cal VALUES ('SSE',?,1,NULL)",
                     [("20260901",), ("20261009",)])
    conn.executemany("INSERT INTO pull_log(table_name,date_val,ok) VALUES ('index_weight',?,?)",
                     [("000300.SH_202609", 2), ("000300.SH_202610", 2),
                      ("000905.SH_202609", 1), ("000905.SH_202610", 1)])
    calls = []
    monkeypatch.setattr(m, "_pull_once", lambda *a, **k: calls.append((a[3], k)) or True)
    m._run_domain_strategy(conn, object(), entry, "20260901", "20261024")
    assert calls == [("000300.SH_202610", {"index_code": "000300.SH",
                                         "start_date": "20261001", "end_date": "20261031"})]


def test_sparse_once_report_uses_domain_state_not_announcement_dates(conn, monkeypatch):
    entry = {"api": "dividend", "table": "dividend", "date_col": "ann_date",
             "driver": {"values": ["A", "B", "C", "D", "E"], "date_mode": "once",
                        "refresh_days": 7, "refresh_batch_size": 1000}}
    conn.execute("CREATE TABLE dividend(ts_code VARCHAR,ann_date VARCHAR)")
    conn.execute("INSERT INTO dividend VALUES ('A','20100101')")
    conn.executemany("INSERT INTO pull_log VALUES ('dividend',?,?,0,?)",
                     [("A__once__", 1, "2026-10-23 00:00:00"),
                      ("B__once__", 2, "2026-10-01 00:00:00"),
                      ("C__once__", 0, "2026-10-23 00:00:00"),
                      ("D__once__", 3, "2026-10-23 00:00:00"),
                      ("OLD__once__", 1, "2026-10-23 00:00:00")])
    monkeypatch.setattr(m, "REGISTRY", [entry])
    monkeypatch.setattr(m, "load_config", lambda: {})
    monkeypatch.setattr(m, "beijing_now", lambda: datetime(2026, 10, 24, tzinfo=ZoneInfo("Asia/Shanghai")))
    result = m._verify(conn)
    assert result["big_gap"] == 0
    report = result["freshness"]["dividend"]
    assert {k: report[k] for k in ("domains", "completed", "due", "failed", "abandoned", "pending")} == {
        "domains": 5, "completed": 2, "due": 1, "failed": 1, "abandoned": 1, "pending": 1}
    assert report["oldest_success"] == "2026-10-01T00:00:00+08:00"


def test_request_and_store_logs_share_task_and_run_without_secrets(conn, tmp_path, monkeypatch):
    from database.run_context import run_context
    entry = {"api": "daily", "table": "daily", "date_col": "trade_date"}
    conn.execute("CREATE TABLE daily(ts_code VARCHAR,trade_date VARCHAR)")
    monkeypatch.setattr(m, "REGISTRY", [entry])
    dc = DataClient("never-log-this", cache_path=str(tmp_path / "cache"))
    dc._rule_config["daily"] = {"rule": 1, "max_rows": 2, "split_by": "offset"}
    dc._global_interval = 0
    response = SimpleNamespace(status_code=200, json=lambda: {"code": 0, "data": {
        "fields": ["ts_code", "trade_date"], "items": [["A", "20261009"]]}})
    monkeypatch.setattr(dc._session, "post", lambda *a, **k: response)
    conn.execute("INSERT INTO pull_log(table_name,date_val,ok) VALUES ('old','old',1)")
    with run_context("run-test"):
        assert m._pull_once(conn, dc, entry, "20261009", "trade_date", "", trade_date="20261009")
        m._write_run_end(conn, "run-test", 0)
    raw = "".join(f.read_text() for f in (tmp_path / "logs").glob("*.log"))
    records = [json.loads(line) for line in raw.splitlines()]
    pulls = [r for r in records if r["event"] == "pull"]
    assert len(pulls) == 2
    assert {r["run_id"] for r in pulls} == {"run-test"}
    assert len({r["task_id"] for r in pulls}) == 1
    assert pulls[0]["request_params"] == {"trade_date": "20261009", "limit": 2, "offset": 0}
    assert "never-log-this" not in raw
    summary = records[-1]
    assert summary["pull_log_total"] == 2
    assert summary["run_stats"]["http_attempts"] == 1
    assert summary["run_stats"]["tasks_succeeded"] == 1
    assert summary["run_stats"]["cache_hits"] == 0


@pytest.fixture
def runtime_tree(tmp_path):
    root = Path(__file__).resolve().parents[1]
    (tmp_path / "database").mkdir()
    for name in ("api_index.json", "database/schema.sql", "database/etl.py"):
        shutil.copy2(root / name, tmp_path / name)
    return tmp_path


def test_runtime_snapshot_keeps_current_artifacts_and_skips_writes(runtime_tree):
    from scripts._runtime import prepare_runtime
    from database.utils import load_config
    names = ("api_index.json", "database/schema.sql", "database/etl.py")
    before = {n: ((runtime_tree / n).read_bytes(), (runtime_tree / n).stat().st_mtime_ns) for n in names}
    snapshot = prepare_runtime(load_config(), runtime_tree)
    assert snapshot.registry == REGISTRY
    assert snapshot.schema_sql == (runtime_tree / "database/schema.sql").read_text()
    assert snapshot.changed_files == []
    assert {n: ((runtime_tree / n).read_bytes(), (runtime_tree / n).stat().st_mtime_ns) for n in names} == before


def test_runtime_invalid_candidate_does_not_publish_any_file(runtime_tree):
    from scripts._runtime import prepare_runtime
    from database.utils import load_config
    path = runtime_tree / "api_index.json"
    specs = json.loads(path.read_text())
    next(a for a in specs if a["api_name"] == "block_trade")["_project"]["upsert"]["partition_key"] = "missing_column"
    path.write_text(json.dumps(specs))
    before = {p: p.read_bytes() for p in (path, runtime_tree / "database/schema.sql", runtime_tree / "database/etl.py")}
    with pytest.raises(ValueError, match="missing_column"):
        prepare_runtime(load_config(), runtime_tree)
    assert {p: p.read_bytes() for p in before} == before


def test_runtime_publish_failure_restores_artifacts(runtime_tree, monkeypatch):
    from scripts import _runtime
    from database.utils import load_config
    paths = [runtime_tree / n for n in ("api_index.json", "database/schema.sql", "database/etl.py")]
    before = {p: p.read_bytes() for p in paths}
    config = load_config()
    config["exclude_apis"] = [*config.get("exclude_apis", []), "moneyflow_dc"]
    write = _runtime.atomic_write_text
    calls = []

    def fail_second(path, value):
        calls.append(path)
        if len(calls) == 2:
            raise OSError("injected publish error")
        return write(path, value)

    monkeypatch.setattr(_runtime, "atomic_write_text", fail_second)
    with pytest.raises(OSError, match="injected"):
        _runtime.prepare_runtime(config, runtime_tree)
    assert {p: p.read_bytes() for p in paths} == before


@pytest.mark.parametrize("strategy,entry,expected", [
    ({"strategy": "trade_date"}, {"api": "daily", "table": "daily", "date_col": "trade_date"},
     [{"trade_date": "20261009", "force_refresh": True}]),
    ({"strategy": "date_range"}, {"api": "gz_index", "table": "gz_index"},
     [{"label": "gz_index 2026", "start_date": "20260101", "end_date": "20261231", "force_refresh": True}]),
    ({"strategy": "once"}, {"api": "etf_basic", "table": "etf_basic"},
     [{"force_refresh": True}]),
    ({"strategy": "freq", "freq_values": ["W", "M"]}, {"api": "freq_api", "table": "freq_api"},
     [{"trade_date": "20261009", "freq": "W", "force_refresh": True},
      {"trade_date": "20261009", "freq": "M", "force_refresh": True}]),
    ({"strategy": "domain"}, {"api": "index_weight", "table": "index_weight",
      "driver": {"values": ["000300.SH"], "date_mode": "monthly"}},
     [{"index_code": "000300.SH", "start_date": "20261001", "end_date": "20261031", "force_refresh": True}]),
])
def test_explicit_refresh_reaches_every_strategy(conn, monkeypatch, strategy, entry, expected):
    conn.execute("INSERT INTO trade_cal VALUES ('SSE','20261009',1,NULL)")
    calls = []
    monkeypatch.setattr(m, "_pull_once", lambda *a, **k: calls.append(k) or True)
    m._dispatch_strategy(conn, object(), entry, strategy, "20261009", "20261009", force_refresh=True)
    assert calls == expected
    assert "freq_values" not in entry


def test_invalid_runtime_stops_main_before_opening_database(monkeypatch, isolated_logs):
    import sys
    monkeypatch.setattr(sys, "argv", ["maintain.py", "--daily"])
    monkeypatch.setattr(m, "load_config", lambda: {"tushare_token": "test"})
    monkeypatch.setattr(m, "DataClient", lambda **kw: object())
    monkeypatch.setattr(engine, "db_lock", lambda *a, **kw: nullcontext(True))
    monkeypatch.setattr(m, "get_conn", lambda **kw: pytest.fail("must not open database"))

    def fail(*args):
        raise ValueError("invalid candidate")

    monkeypatch.setattr(m, "prepare_runtime", fail)
    with pytest.raises(ValueError, match="invalid candidate"):
        m.main()
    log = isolated_logs._log_dir
    event = json.loads(next(log.glob("*.log")).read_text().splitlines()[-1])
    assert event["status"] == "fail"
    assert event["step"] == "runtime_config"
    assert event["run_id"]


def test_runtime_inputs_pin_config_and_specs_and_restore_after_exit(tmp_path, monkeypatch):
    from database import utils
    monkeypatch.setattr(utils, "PROJECT_ROOT", str(tmp_path))
    monkeypatch.setattr(utils, "_registry_cache", None)
    (tmp_path / "user_config.yaml").write_text("tushare_rate_limit: 1\n")
    (tmp_path / "api_index.json").write_text('[{"api_name":"changed_on_disk"}]')
    api_list = [{"api_name": "snapshot_api", "input_params": [], "max_rows": None,
                 "_project": {"classification": {"rule": 1, "usable": True}}}]
    with utils.runtime_inputs({"tushare_rate_limit": 500}, api_list):
        dc = DataClient("test", cache_path=str(tmp_path / "cache"))
        assert dc._global_interval == 0.12
        assert set(dc._rule_config) == {"snapshot_api"}
        assert utils.load_config()["tushare_rate_limit"] == 500
    assert utils.load_config()["tushare_rate_limit"] == 1
    assert utils.load_api_registry() == [{"api_name": "changed_on_disk"}]


def test_snapshot_report_empty_source_and_unknown_timestamps(conn):
    from database.maintenance_report import snapshot_freshness
    now = datetime(2026, 10, 24, tzinfo=ZoneInfo("Asia/Shanghai"))
    conn.execute("INSERT INTO pull_log VALUES ('dividend','A__once__',1,0,NULL)")
    report = snapshot_freshness(conn, "dividend", {"values": ["A"], "refresh_days": 7}, now)
    assert report["oldest_success"] is None
    assert report["unknown_success_time"] == report["due"] == 1
    empty = snapshot_freshness(conn, "dividend", {"values": []}, now)
    assert empty["domains"] == empty["completed"] == 0


@pytest.mark.parametrize("filters,expected", [
    ({"status": ["active"], "exchange": ["SH", "SZ"]}, ["A", "C"]),
    ({"status": []}, []),
])
def test_scheduling_and_freshness_use_same_filtered_domains(conn, monkeypatch, filters, expected):
    from database.maintenance_report import snapshot_freshness
    conn.execute("CREATE TABLE source_codes(ts_code VARCHAR,status VARCHAR,exchange VARCHAR)")
    conn.executemany("INSERT INTO source_codes VALUES (?,?,?)", [
        ("C", "active", "SZ"), ("A", "active", "SH"), ("A", "active", "SH"),
        ("B", "inactive", "SH"), ("D", "active", "BJ"),
    ])
    entry = {"api": "dividend", "table": "dividend", "driver": {
        "source_table": "source_codes", "source_column": "ts_code",
        "filters": filters, "date_mode": "once"}}
    calls = []
    monkeypatch.setattr(m, "REGISTRY", [entry])
    monkeypatch.setattr(m, "_pull_once", lambda *a, **kw: calls.append(kw["ts_code"]) or True)
    m._run_domain_strategy(conn, object(), entry, until="20261024")
    report = snapshot_freshness(conn, "dividend", entry["driver"], m.beijing_now())
    assert calls == expected
    assert report["domains"] == report["pending"] == len(expected)


def test_once_refresh_preserves_default_parameters(conn, monkeypatch):
    entry = {"api": "index_classify", "table": "index_classify", "default_params": {"src": "SW2021"}}
    calls = []
    dc = SimpleNamespace(index_classify=lambda **kw: calls.append(kw) or pd.DataFrame())
    monkeypatch.setattr(m, "REGISTRY", [entry])
    m._run_once_strategy(conn, dc, entry, force_refresh=True)
    assert calls == [{"src": "SW2021", "force_refresh": True}]
    assert entry["default_params"] == {"src": "SW2021"}


def test_log_redaction_preserves_business_parameters_and_input():
    from database.run_context import safe_params
    params = {"trade_date": "20261009", "fields": "close", "access_token": "secret",
              "nested": {"PASSWORD": "secret", "limit": 100}}
    assert safe_params(params) == {
        "trade_date": "20261009", "fields": "close", "access_token": "[REDACTED]",
        "nested": {"PASSWORD": "[REDACTED]", "limit": 100}}
    assert params["access_token"] == params["nested"]["PASSWORD"] == "secret"
