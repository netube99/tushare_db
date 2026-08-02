"""A5-3: _cmd_daily ok=2 超期复验 — 删除后按表重拉，而非删除后永不复查.

回归背景: 旧逻辑删除 7 天前的 ok=2 记录后，daily 只从 MAX(date_col) 补尾部，
被删的历史空日永远不再复验（上游后补公告会永久漏掉）。修复后按表聚合重拉.
用 trade_date 策略表 stk_factor_pro 作为被测对象（dividend 已转 domain 策略）.
"""

import sqlite3
from datetime import datetime, timedelta

import pandas as pd
import pytest

import scripts.maintain as m
from database.etl import REGISTRY


@pytest.fixture
def scratch(monkeypatch):
    """内存库 + 仅 stk_factor_pro 的 REGISTRY + 记录调用的 FakeDC."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE pull_log (table_name TEXT NOT NULL, date_val TEXT NOT NULL, "
        "ok INTEGER NOT NULL, retry_count INTEGER NOT NULL DEFAULT 0, "
        "last_try TEXT DEFAULT NULL, PRIMARY KEY (table_name, date_val))")
    conn.execute("CREATE TABLE trade_cal (cal_date TEXT, exchange TEXT, is_open INTEGER)")
    conn.execute("CREATE TABLE stk_factor_pro (ts_code TEXT, trade_date TEXT)")

    entry = next(e for e in REGISTRY if e["table"] == "stk_factor_pro")
    monkeypatch.setattr(m, "REGISTRY", [entry])
    monkeypatch.setattr(m, "_verify", lambda conn: None)
    monkeypatch.setattr(m, "_write_run_end", lambda *a, **k: None)

    class FakeDC:
        _daily_cooldown_until = {}

        def __init__(self):
            self.calls = []

        def trade_cal(self, **kw):
            return pd.DataFrame(
                [{"cal_date": "20240101", "exchange": "SSE", "is_open": 1}])

        def __getattr__(self, name):
            def _f(**kw):
                self.calls.append((name, kw))
                return pd.DataFrame()
            return _f

    return conn, FakeDC()


class _Args:
    since = "20180101"
    until = "20260802"
    api = None


def _run_daily(conn, dc):
    m._cmd_daily(conn, dc, _Args(), {"backfill_since": "20180101"}, "test", 0.0)


def test_daily_revalidates_stale_ok2(scratch):
    conn, dc = scratch
    now = m.beijing_now()
    stale_ts = (now - timedelta(days=10)).isoformat()
    fresh_ts = now.isoformat()
    conn.execute("INSERT INTO pull_log VALUES ('stk_factor_pro','20240101',2,0,?)", (stale_ts,))
    conn.execute("INSERT INTO pull_log VALUES ('stk_factor_pro','20240102',2,0,?)", (fresh_ts,))
    conn.commit()

    _run_daily(conn, dc)

    # 超期空日被重新拉取，pull_log 记录保留且 last_try 刷新
    row = conn.execute(
        "SELECT ok, last_try FROM pull_log WHERE table_name='stk_factor_pro' AND date_val='20240101'"
    ).fetchone()
    assert row is not None
    assert row[0] == 2
    last_dt = datetime.fromisoformat(row[1].replace(" ", "T")).replace(tzinfo=now.tzinfo)
    assert last_dt >= now - timedelta(minutes=1)
    assert ("stk_factor_pro", {"trade_date": "20240101"}) in dc.calls

    # 7 天内的 ok=2 不受影响（不删除、不重拉）
    fresh_row = conn.execute(
        "SELECT last_try FROM pull_log WHERE table_name='stk_factor_pro' AND date_val='20240102'"
    ).fetchone()
    assert fresh_row[0] == fresh_ts
    assert ("stk_factor_pro", {"trade_date": "20240102"}) not in dc.calls


def test_daily_ok2_deletion_never_loses_rows(scratch):
    """修复前: 删除后不复验 → pull_log 永久丢失该日期记录."""
    conn, dc = scratch
    conn.execute(
        "INSERT INTO pull_log VALUES ('stk_factor_pro','20240101',2,0,?)",
        ((m.beijing_now() - timedelta(days=10)).isoformat(),))
    conn.commit()

    _run_daily(conn, dc)

    n = conn.execute(
        "SELECT COUNT(*) FROM pull_log WHERE table_name='stk_factor_pro' AND date_val='20240101'"
    ).fetchone()[0]
    assert n == 1


def test_daily_domain_once_refreshes_new_domain_values(monkeypatch):
    """domain-once 表（dividend）：daily 直接逐域 dispatch，
    已拉域值（ok=1/2）跳过，未拉域值补齐 — 新股分红不被 MAX(date_col) 门禁遗漏."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE pull_log (table_name TEXT NOT NULL, date_val TEXT NOT NULL, "
        "ok INTEGER NOT NULL, retry_count INTEGER NOT NULL DEFAULT 0, "
        "last_try TEXT DEFAULT NULL, PRIMARY KEY (table_name, date_val))")
    conn.execute("CREATE TABLE trade_cal (cal_date TEXT, exchange TEXT, is_open INTEGER)")
    conn.execute("CREATE TABLE stk_factor_pro (ts_code TEXT, trade_date TEXT)")
    conn.execute("CREATE TABLE dividend (ts_code TEXT, ann_date TEXT, div_proc TEXT)")
    conn.executemany("INSERT INTO stk_factor_pro (ts_code) VALUES (?)",
                     [("000001.SZ",), ("000002.SZ",), ("000003.SZ",)])
    conn.execute("INSERT INTO pull_log VALUES ('dividend','000001.SZ__once__',1,0,?)",
                 (m.beijing_now().isoformat(),))
    conn.commit()

    entry = next(e for e in REGISTRY if e["table"] == "dividend")
    monkeypatch.setattr(m, "REGISTRY", [entry])
    monkeypatch.setattr(m, "_verify", lambda conn: None)
    monkeypatch.setattr(m, "_write_run_end", lambda *a, **k: None)

    class FakeDC:
        _daily_cooldown_until = {}

        def __init__(self):
            self.calls = []

        def __getattr__(self, name):
            def _f(**kw):
                self.calls.append((name, kw))
                return pd.DataFrame()
            return _f

    dc = FakeDC()
    m._cmd_daily(conn, dc, _Args(), {"backfill_since": "20180101"}, "test", 0.0)

    pulled = [kw["ts_code"] for (api, kw) in dc.calls if api == "dividend"]
    assert pulled == ["000002.SZ", "000003.SZ"]
    assert "000001.SZ" not in pulled
