"""maintain.py TDD 审查测试 — 疑似缺陷暴露 + 契约守护回归.

覆盖: partition 替换原子性 / domain-once MAX 门禁 / ok=2 复验窗口格式 /
infra 日历补拉 / --refresh freq 与 date_range 边界 / _disabled 参数标记 /
_resolve_until 边界 / 策略判定顺序 / 续跑 ok 状态 / 质检断供标注 / cleanup / main 分派.
"""

import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

import database.logger
import database.utils
import scripts.maintain as m
from database.engine import connect
from database.etl import REGISTRY

CST = ZoneInfo("Asia/Shanghai")


def make_conn():
    conn = connect(":memory:")
    conn.execute(
        "CREATE TABLE pull_log (table_name VARCHAR NOT NULL, date_val VARCHAR NOT NULL, "
        "ok BIGINT NOT NULL, retry_count BIGINT NOT NULL DEFAULT 0, "
        "last_try VARCHAR DEFAULT NULL, PRIMARY KEY (table_name, date_val))")
    conn.execute("CREATE TABLE trade_cal (cal_date VARCHAR, exchange VARCHAR, is_open BIGINT)")
    return conn


def add_cal(conn, dates):
    conn.executemany("INSERT INTO trade_cal VALUES (?, 'SSE', 1)", [(d,) for d in dates])


def pl(conn, table, date_val, ok, last_try=None, retry_count=0):
    conn.execute("INSERT INTO pull_log VALUES (?,?,?,?,?)",
                 (table, date_val, ok, retry_count, last_try))


class _Args:
    def __init__(self, since=None, until=None, api=None, refresh=None,
                 vacuum=False, hard=False):
        self.since = since
        self.until = until
        self.api = api
        self.refresh = refresh
        self.vacuum = vacuum
        self.hard = hard


class FakeDC:
    _daily_cooldown_until = {}

    def __init__(self, responses=None):
        self.calls = []
        self._responses = responses or {}

    def load_rules(self):
        pass

    def clear_cache(self):
        self.calls.append(("__clear_cache__", {}))

    def __getattr__(self, name):
        def _f(**kw):
            self.calls.append((name, kw))
            df = self._responses.get(name)
            return df if df is not None else pd.DataFrame()
        return _f


@pytest.fixture(autouse=True)
def quiet_jlog(monkeypatch):
    writes = []

    class Dummy:
        def write(self, entry):
            writes.append(entry)

    monkeypatch.setattr(database.logger, "get_json_logger", lambda: Dummy())
    return writes


@pytest.fixture
def daily_env(monkeypatch):
    conn = make_conn()
    monkeypatch.setattr(m, "_verify", lambda conn: None)
    monkeypatch.setattr(m, "_write_run_end", lambda *a, **k: None)
    return conn


# ── D1: partition_key 分区替换 DELETE 与 INSERT 的原子性 ──


def test_partition_replace_atomic_on_upsert_failure(monkeypatch):
    conn = make_conn()
    conn.execute(
        "CREATE TABLE t_part (ts_code VARCHAR, "
        "ann_date VARCHAR CHECK (ann_date <> '20250101'))")
    conn.execute("INSERT INTO t_part VALUES ('000001.SZ', '20200101')")
    conn.commit()
    entry = {"api": "stk_holdernumber", "table": "t_part", "date_col": "ann_date",
             "partition_key": "ts_code"}
    monkeypatch.setattr(m, "REGISTRY", [entry])

    df = pd.DataFrame([{"ts_code": "000001.SZ", "ann_date": "20250101"}])
    ok = m._pull_and_store(conn, "t_part", df, "000001.SZ__once__",
                           "stk_holdernumber", "domain")

    assert ok is False
    n = conn.execute("SELECT COUNT(*) FROM t_part").fetchone()[0]
    assert n == 1
    row = conn.execute(
        "SELECT ok FROM pull_log WHERE table_name='t_part'").fetchone()
    assert row is not None and row[0] == 0


def test_partition_replace_updates_only_pulled_codes(monkeypatch):
    conn = make_conn()
    conn.execute("CREATE TABLE t_part (ts_code VARCHAR, ann_date VARCHAR)")
    conn.execute("INSERT INTO t_part VALUES ('000001.SZ', '20200101')")
    conn.execute("INSERT INTO t_part VALUES ('000002.SZ', '20200101')")
    entry = {"api": "stk_holdernumber", "table": "t_part", "date_col": "ann_date",
             "partition_key": "ts_code"}
    monkeypatch.setattr(m, "REGISTRY", [entry])

    df = pd.DataFrame([{"ts_code": "000001.SZ", "ann_date": "20250101"},
                       {"ts_code": "000001.SZ", "ann_date": "20250102"}])
    ok = m._pull_and_store(conn, "t_part", df, "000001.SZ__once__",
                           "stk_holdernumber", "domain")

    assert ok is True
    rows = {tuple(r) for r in conn.execute("SELECT ts_code, ann_date FROM t_part")}
    assert rows == {("000001.SZ", "20250101"), ("000001.SZ", "20250102"),
                    ("000002.SZ", "20200101")}
    state = conn.execute(
        "SELECT ok FROM pull_log WHERE table_name='t_part' AND date_val='000001.SZ__once__'"
    ).fetchone()
    assert state is not None and state[0] == 1


# ── D2: domain-once 表的 MAX(date_col) 门禁应被绕过 ──


def test_daily_domain_once_bypasses_max_date_gate(daily_env, monkeypatch):
    conn = daily_env
    conn.execute("CREATE TABLE stk_factor_pro (ts_code VARCHAR, trade_date VARCHAR)")
    conn.execute("CREATE TABLE dividend (ts_code VARCHAR, ann_date VARCHAR, div_proc VARCHAR)")
    conn.executemany("INSERT INTO stk_factor_pro (ts_code) VALUES (?)",
                     [("000001.SZ",), ("000002.SZ",)])
    conn.execute("INSERT INTO dividend VALUES ('000001.SZ', '20260901', '实施')")
    pl(conn, "dividend", "000001.SZ__once__", 1, last_try=m.beijing_now().isoformat())
    conn.commit()

    entry = next(e for e in REGISTRY if e["table"] == "dividend")
    monkeypatch.setattr(m, "REGISTRY", [entry])
    dc = FakeDC()

    m._cmd_daily(conn, dc, _Args(since="20180101", until="20260801"),
                 {"backfill_since": "20180101"}, "run", 0.0)

    pulled = [kw["ts_code"] for (api, kw) in dc.calls if api == "dividend"]
    assert pulled == ["000002.SZ"]


# ── D5: ok=2 复验窗口 last_try 格式不一致 ──


def test_daily_ok2_window_respects_seven_days(daily_env, monkeypatch):
    conn = daily_env
    conn.execute("CREATE TABLE stk_factor_pro (ts_code VARCHAR, trade_date VARCHAR)")
    monkeypatch.setattr(m, "REGISTRY",
                        [next(e for e in REGISTRY if e["table"] == "stk_factor_pro")])
    fixed = datetime(2026, 9, 13, 20, 30, 0, tzinfo=CST)
    monkeypatch.setattr(m, "beijing_now", lambda: fixed)

    last_try = "2026-09-06 21:00:00"
    pl(conn, "stk_factor_pro", "20240101", 2, last_try=last_try)
    conn.commit()
    dc = FakeDC()

    m._cmd_daily(conn, dc, _Args(since="20180101"), {"backfill_since": "20180101"},
                 "run", 0.0)

    row = conn.execute(
        "SELECT last_try FROM pull_log WHERE table_name='stk_factor_pro' "
        "AND date_val='20240101'").fetchone()
    assert row is not None and row[0] == last_try
    assert ("stk_factor_pro", {"trade_date": "20240101"}) not in dc.calls


# ── D3: infra 日历落后于当前日期时应补缺口 ──


def test_infra_fills_calendar_gap_when_behind(monkeypatch):
    conn = make_conn()
    add_cal(conn, ["20260101", "20260105"])
    monkeypatch.setattr(m, "_run_infra_script", lambda *a, **k: True)
    monkeypatch.setattr(m, "beijing_today", lambda: date(2026, 9, 13))
    dc = FakeDC()

    m._run_infrastructure({}, conn, dc)

    cal_calls = [kw for (api, kw) in dc.calls if api == "trade_cal"]
    assert any(kw.get("start_date") == "20260106" and kw.get("end_date") == "20260913"
               for kw in cal_calls)


def test_infra_extends_calendar_when_near_end(monkeypatch):
    conn = make_conn()
    add_cal(conn, ["20260105", "20260919"])
    monkeypatch.setattr(m, "_run_infra_script", lambda *a, **k: True)
    monkeypatch.setattr(m, "beijing_today", lambda: date(2026, 9, 13))
    dc = FakeDC()

    m._run_infrastructure({}, conn, dc)

    cal_calls = [kw for (api, kw) in dc.calls if api == "trade_cal"]
    assert any(kw.get("start_date") == "20270101" and kw.get("end_date") == "20271231"
               for kw in cal_calls)
    assert not any(kw.get("start_date") == "20260914" for kw in cal_calls)


# ── D4/D29: --refresh 参数解析与 pull_log 重置 ──


def _freq_api_index():
    return [{"api_name": "fake_freq_api",
             "input_params": [{"name": "freq", "required": True},
                              {"name": "trade_date", "required": False}]}]


def test_refresh_freq_clears_and_repulls_all_freq_keys(monkeypatch):
    conn = make_conn()
    add_cal(conn, ["20250102"])
    conn.execute("CREATE TABLE t_freq (ts_code VARCHAR, trade_date VARCHAR, freq VARCHAR)")
    entry = {"api": "fake_freq_api", "table": "t_freq", "date_col": "trade_date"}
    monkeypatch.setattr(m, "REGISTRY", [entry])
    monkeypatch.setattr(database.utils, "load_api_registry", _freq_api_index)
    pl(conn, "t_freq", "20250102_W", 1)
    pl(conn, "t_freq", "20250102_M", 1)
    conn.commit()
    dc = FakeDC()

    m._cmd_refresh(conn, dc, _Args(refresh=["fake_freq_api", "20250102"]), "run", 0.0)

    freq_calls = [kw for (api, kw) in dc.calls if api == "fake_freq_api"]
    assert sorted(kw["freq"] for kw in freq_calls) == ["M", "W"]
    rows = {r[0] for r in conn.execute(
        "SELECT date_val FROM pull_log WHERE table_name='t_freq'")}
    assert rows == {"20250102_W", "20250102_M"}


def test_auto_fix_bounds_date_range_truncates_to_year():
    s, u, fdv = m._auto_fix_bounds({"strategy": "date_range"}, "20250601")
    assert (s, u, fdv) == ("20250101", "20251231", None)


def test_refresh_date_range_with_full_date(monkeypatch):
    conn = make_conn()
    add_cal(conn, ["20250102", "20250103"])
    conn.execute("CREATE TABLE gz_index (date VARCHAR)")
    entry = next(e for e in REGISTRY if e["table"] == "gz_index")
    monkeypatch.setattr(m, "REGISTRY", [entry])
    pl(conn, "gz_index", "2025", 1)
    conn.commit()
    dc = FakeDC(responses={"gz_index": pd.DataFrame([{"date": "20250102"}])})

    m._cmd_refresh(conn, dc, _Args(refresh=["gz_index", "20250601"]), "run", 0.0)

    dr_calls = [kw for (api, kw) in dc.calls if api == "gz_index"]
    assert dr_calls == [{"start_date": "20250101", "end_date": "20251231"}]


# ── D6: _disabled 参数标记 ──


def _disabled_api_index():
    return [{"api_name": "fake_api",
             "input_params": [
                 {"name": "ts_code", "required": False},
                 {"name": "trade_date", "required": False, "_disabled": True},
                 {"name": "start_date", "required": False},
                 {"name": "end_date", "required": False},
             ]}]


def test_param_fixes_exclude_disabled_params():
    api = _disabled_api_index()[0]
    fixes = m._apply_param_fixes(api)
    assert "trade_date" not in fixes["active_params"]
    assert fixes["is_required"]("trade_date") is False
    assert fixes["is_required"]("ts_code") is False


def test_get_date_params_disabled_trade_date_falls_to_date_range(monkeypatch):
    monkeypatch.setattr(database.utils, "load_api_registry", _disabled_api_index)
    entry = {"api": "fake_api", "table": "t", "date_col": "trade_date"}
    assert m._get_date_params(entry)["strategy"] == "date_range"


# ── 守护：_resolve_until 门禁边界 ──


@pytest.mark.parametrize("pull_after,now_dt,expected", [
    (None, datetime(2026, 9, 13, 20, 30), "20260913"),
    (None, datetime(2026, 9, 13, 20, 29), "20260912"),
    (None, datetime(2026, 9, 13, 7, 0), "20260912"),
    ("22:00", datetime(2026, 9, 13, 21, 0), "20260912"),
    ("22:00", datetime(2026, 9, 13, 22, 0), "20260913"),
    ("bad", datetime(2026, 9, 13, 20, 30), "20260913"),
])
def test_resolve_until_gate(pull_after, now_dt, expected, monkeypatch):
    monkeypatch.setattr(m, "beijing_now", lambda: now_dt.replace(tzinfo=CST))
    cfg = {} if pull_after is None else {"pull_after": pull_after}
    monkeypatch.setattr(m, "load_config", lambda: cfg)
    assert m._resolve_until() == expected


# ── 守护：_get_date_params 判定顺序 ──


def test_get_date_params_order_rules(monkeypatch):
    api_list = [
        {"api_name": "freq_required", "input_params": [
            {"name": "freq", "required": True},
            {"name": "trade_date", "required": False}]},
        {"api_name": "freq_optional", "input_params": [
            {"name": "freq", "required": False},
            {"name": "trade_date", "required": False}]},
        {"api_name": "has_trading", "input_params": [
            {"name": "trade_date", "required": False},
            {"name": "start_date", "required": False},
            {"name": "end_date", "required": False}]},
        {"api_name": "has_ann", "input_params": [{"name": "ann_date", "required": False}]},
        {"api_name": "range_only", "input_params": [
            {"name": "start_date", "required": False},
            {"name": "end_date", "required": False}]},
        {"api_name": "no_dates", "input_params": []},
    ]
    monkeypatch.setattr(database.utils, "load_api_registry", lambda: api_list)

    def strat(api):
        return m._get_date_params({"api": api, "table": api})["strategy"]

    assert strat("freq_required") == "freq"
    assert strat("freq_optional") == "trade_date"
    assert strat("has_trading") == "trade_date"
    assert strat("has_ann") == "trade_date"
    assert strat("range_only") == "date_range"
    assert strat("no_dates") == "once"
    assert strat("unknown_api") == "once"
    assert m._get_date_params(
        {"api": "freq_required", "table": "x", "driver": {"date_mode": "daily"}}
    )["strategy"] == "domain"


def test_all_registry_entries_resolve_known_strategy():
    known = {"trade_date", "date_range", "once", "freq", "domain"}
    for entry in REGISTRY:
        s = m._get_date_params(entry)
        assert s["strategy"] in known, entry["api"]
        if s["strategy"] == "domain":
            assert "driver" in s


# ── 守护：trade_date 续跑 ok 状态 ──


def test_trade_date_strategy_resumes_by_ok_state(monkeypatch):
    conn = make_conn()
    days = ["20250102", "20250103", "20250106", "20250107", "20250108"]
    add_cal(conn, days)
    conn.execute("CREATE TABLE t_td (ts_code VARCHAR, trade_date VARCHAR)")
    pl(conn, "t_td", "20250102", 1)
    pl(conn, "t_td", "20250103", 2)
    pl(conn, "t_td", "20250106", 0)
    pl(conn, "t_td", "20250107", 3)
    conn.commit()
    entry = {"api": "fake_td", "table": "t_td", "date_col": "trade_date"}
    monkeypatch.setattr(m, "REGISTRY", [entry])
    dc = FakeDC(responses={"fake_td": pd.DataFrame([{"ts_code": "000001.SZ"}])})

    strategy = {"strategy": "trade_date", "date_col": "trade_date",
                "iter_mode": "trading"}
    m._dispatch_strategy(conn, dc, entry, strategy, "20250102", "20250108")

    pulled = [kw["trade_date"] for (api, kw) in dc.calls if api == "fake_td"]
    assert pulled == ["20250108", "20250107", "20250106"]
    states = dict(conn.execute(
        "SELECT date_val, ok FROM pull_log WHERE table_name='t_td'"))
    assert states == {"20250102": 1, "20250103": 2, "20250106": 1,
                      "20250107": 1, "20250108": 1}


# ── 守护：date_range 按年切分边界 ──


def test_date_range_strategy_year_split(monkeypatch):
    conn = make_conn()
    add_cal(conn, ["20241230", "20241231", "20250102", "20250103"])
    conn.execute("CREATE TABLE gz_index (date VARCHAR)")
    entry = next(e for e in REGISTRY if e["table"] == "gz_index")
    monkeypatch.setattr(m, "REGISTRY", [entry])
    dc = FakeDC(responses={"gz_index": pd.DataFrame([{"date": "20250102"}])})

    m._run_backfill(conn, dc, "gz_index", "20241230", "20250103")

    dr_calls = [kw for (api, kw) in dc.calls if api == "gz_index"]
    assert dr_calls == [
        {"start_date": "20250101", "end_date": "20251231"},
        {"start_date": "20240101", "end_date": "20241231"},
    ]
    states = dict(conn.execute(
        "SELECT date_val, ok FROM pull_log WHERE table_name='gz_index'"))
    assert states == {"2025": 1, "2024": 1}


# ── 守护：质检报告 ──


def test_verify_flags_over_100_day_gap(monkeypatch):
    conn = make_conn()
    weekdays = []
    d = date(2020, 1, 1)
    while d < date(2020, 8, 1):
        if d.weekday() < 5:
            weekdays.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    add_cal(conn, weekdays)
    conn.execute("CREATE TABLE t_gap (ts_code VARCHAR, trade_date VARCHAR)")
    for d in weekdays[:5]:
        conn.execute("INSERT INTO t_gap VALUES ('000001.SZ', ?)", (d,))
    entry = {"api": "fake_gap", "table": "t_gap", "date_col": "trade_date"}
    monkeypatch.setattr(m, "REGISTRY", [entry])
    monkeypatch.setattr(m, "load_config", lambda: {})

    result = m._verify(conn)

    assert result["big_gap"] == 1
    assert any("可能Tushare断供" in s for s in result["design_issues"])


def test_verify_domain_monthly_coverage(monkeypatch):
    conn = make_conn()
    add_cal(conn, ["20250102", "20250103", "20250205", "20250206",
                   "20250303", "20250304"])
    conn.execute("CREATE TABLE index_weight (index_code VARCHAR, con_code VARCHAR, "
                 "trade_date VARCHAR)")
    for td in ("20250102", "20250205"):
        conn.execute("INSERT INTO index_weight VALUES ('000300.SH', '000001.SZ', ?)",
                     (td,))
    entry = next(e for e in REGISTRY if e["table"] == "index_weight")
    monkeypatch.setattr(m, "REGISTRY", [entry])
    monkeypatch.setattr(m, "load_config", lambda: {})

    result = m._verify(conn)

    assert result["big_gap"] == 1
    assert any(t == "index_weight" and "1域 × 2/3周期" in detail
               for t, _reason, detail in result["anomalies"])


# ── 守护：cleanup 孤儿表 ──


def test_cleanup_drops_orphans_and_stale_pull_log(monkeypatch):
    conn = make_conn()
    conn.execute("CREATE TABLE etf_basic (ts_code VARCHAR)")
    conn.execute("CREATE TABLE zzz_orphan (a VARCHAR)")
    pl(conn, "zzz_orphan", "20250102", 1)
    pl(conn, "etf_basic", "20250102", 1)
    conn.commit()
    monkeypatch.setattr(m, "REGISTRY", [{"api": "etf_basic", "table": "etf_basic"}])
    dc = FakeDC()

    m._cmd_cleanup(conn, dc, _Args(vacuum=False, hard=False), "run", 0.0)

    tables = {r[0] for r in conn.execute(
        "SELECT table_name FROM duckdb_tables()")}
    assert "zzz_orphan" not in tables
    assert {"pull_log", "trade_cal", "etf_basic"} <= tables
    left = {r[0] for r in conn.execute("SELECT table_name FROM pull_log")}
    assert left == {"etf_basic"}


def test_cleanup_keeps_derived_physical_tables(monkeypatch):
    conn = make_conn()
    conn.execute("CREATE TABLE national_team_daily (ts_code VARCHAR, trade_date VARCHAR)")
    conn.execute("CREATE TABLE pension_float_daily (ts_code VARCHAR, trade_date VARCHAR)")
    conn.execute("CREATE TABLE zzz_orphan (a VARCHAR)")
    monkeypatch.setattr(m, "REGISTRY", [])

    m._cmd_cleanup(conn, FakeDC(), _Args(vacuum=False, hard=False), "run", 0.0)

    tables = {r[0] for r in conn.execute("SELECT table_name FROM duckdb_tables()")}
    assert {"national_team_daily", "pension_float_daily"} <= tables
    assert "zzz_orphan" not in tables


def test_dry_run_does_not_open_database(monkeypatch):
    monkeypatch.setattr(m, "load_config",
                        lambda: {"tushare_token": "x", "backfill_since": "20200101"})
    monkeypatch.setattr(m, "DataClient", lambda token: object())

    def boom(*args, **kwargs):
        raise AssertionError("dry-run 不应连接数据库或取锁")

    monkeypatch.setattr(m, "get_conn", boom)
    monkeypatch.setattr(database.engine, "db_lock", boom)
    monkeypatch.setattr(sys, "argv", ["maintain.py", "--dry-run"])
    m.main()


def test_cleanup_hard_clears_cache(monkeypatch):
    conn = make_conn()
    monkeypatch.setattr(m, "REGISTRY", [])
    dc = FakeDC()

    m._cmd_cleanup(conn, dc, _Args(vacuum=False, hard=True), "run", 0.0)

    assert ("__clear_cache__", {}) in dc.calls


# ── 守护：main() 命令分派 ──


def test_main_dispatch(monkeypatch):
    calls = []

    class FakeConn:
        def close(self):
            pass

    monkeypatch.setattr(m, "load_config",
                        lambda: {"tushare_token": "x", "backfill_since": "20200101"})
    monkeypatch.setattr(m, "DataClient", lambda token: object())
    monkeypatch.setattr(m, "get_conn", lambda *a, **k: FakeConn())
    for name in ("_cmd_verify", "_cmd_cleanup", "_cmd_dry_run", "_cmd_refresh",
                 "_cmd_daily", "_cmd_backfill"):
        monkeypatch.setattr(m, name, (lambda _n: lambda *a: calls.append(_n))(name))
    monkeypatch.setattr(m, "_run_infrastructure", lambda *a: calls.append("infra"))
    monkeypatch.setattr(m, "_log_run_start", lambda *a: None)

    def run(argv):
        calls.clear()
        monkeypatch.setattr(sys, "argv", ["maintain.py"] + argv)
        m.main()
        return list(calls)

    assert run(["--verify"]) == ["_cmd_verify"]
    assert run(["--cleanup"]) == ["_cmd_cleanup"]
    assert run(["--dry-run"]) == ["_cmd_dry_run"]
    assert run(["--refresh", "daily", "20250102"]) == ["_cmd_refresh"]
    assert run(["--daily"]) == ["infra", "_cmd_daily"]
    assert run([]) == ["infra", "_cmd_backfill"]


def test_refresh_unknown_api_exits(monkeypatch):
    conn = make_conn()
    monkeypatch.setattr(m, "REGISTRY", [])
    with pytest.raises(SystemExit) as exc:
        m._cmd_refresh(conn, FakeDC(), _Args(refresh=["daily", "20250102"]),
                       "run", 0.0)
    assert exc.value.code == 1


# ── 守护：integrity_check 仅 --verify 触发 ──


def test_verify_integrity_check_flag(monkeypatch, capsys):
    conn = make_conn()
    monkeypatch.setattr(m, "REGISTRY",
                        [{"api": "fake", "table": "t_x", "date_col": "trade_date"}])
    monkeypatch.setattr(m, "load_config", lambda: {})

    result = m._verify(conn, integrity_check=True)

    out = capsys.readouterr().out
    assert result["tables"] == 1
    assert "ok" in out


def test_verify_default_skips_integrity_check(monkeypatch, capsys):
    conn = make_conn()
    monkeypatch.setattr(m, "REGISTRY",
                        [{"api": "fake", "table": "t_x", "date_col": "trade_date"}])
    monkeypatch.setattr(m, "load_config", lambda: {})

    m._verify(conn)

    assert "integrity_check" not in capsys.readouterr().out


# ── 守护：infra 子脚本失败降级不阻断 ──


def test_infra_script_failure_degrades(monkeypatch, quiet_jlog):
    import subprocess

    def fail(*a, **k):
        raise subprocess.CalledProcessError(1, "script")

    monkeypatch.setattr(subprocess, "run", fail)

    class Dummy:
        def write(self, entry):
            quiet_jlog.append(entry)

    assert m._run_infra_script("classify_apis", "classify_apis.py", Dummy()) is False
    assert quiet_jlog[-1]["event"] == "infra"
    assert quiet_jlog[-1]["status"] == "fail"


# ── 守护：_write_run_end 边界 ──


def test_write_run_end_without_conn_skips_pull_log_stats(quiet_jlog):
    m._write_run_end(None, "run1", 0.0)
    entry = quiet_jlog[-1]
    assert entry["event"] == "run_end"
    assert entry["run_id"] == "run1"
    assert "total_pulls" not in entry


# ── 集成点：infra 重生成 schema.sql 后当轮重新初始化表结构 ──


def test_infra_reinits_schema_after_regeneration(monkeypatch):
    conn = make_conn()
    monkeypatch.setattr(m, "_run_infra_script", lambda *a, **k: True)
    real_load = database.utils.load_schema_sql
    monkeypatch.setattr(
        database.utils, "load_schema_sql",
        lambda: real_load() + "\nCREATE TABLE IF NOT EXISTS zz_new_table(a VARCHAR);")

    m._run_infrastructure({}, conn, FakeDC())

    names = {r[0] for r in conn.execute(
        "SELECT table_name FROM duckdb_tables()")}
    assert "zz_new_table" in names
