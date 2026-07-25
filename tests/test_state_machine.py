"""A5-2: 状态机单测 — upsert_df (pk NaN 过滤/列过滤) + log_pull ok 流转."""

import sqlite3
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from database.utils import upsert_df
from database.etl import log_pull


# ── helpers ──

@pytest.fixture
def conn():
    """内存数据库，含测试表."""
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=10000")
    return db


def _ensure_table(conn, table, ddl):
    conn.execute(ddl)
    conn.commit()


# ── upsert_df ──

def test_upsert_empty_df(conn):
    _ensure_table(conn, "test", 'CREATE TABLE test (ts_code TEXT PRIMARY KEY, val REAL)')
    df = pd.DataFrame()
    assert upsert_df(conn, "test", df) == 0


def test_upsert_basic_insert(conn):
    _ensure_table(conn, "test", 'CREATE TABLE test (ts_code TEXT PRIMARY KEY, val REAL)')
    df = pd.DataFrame({"ts_code": ["A", "B"], "val": [1.0, 2.0]})
    n = upsert_df(conn, "test", df)
    assert n == 2
    rows = conn.execute("SELECT * FROM test ORDER BY ts_code").fetchall()
    assert rows[0]["ts_code"] == "A"
    assert rows[1]["val"] == 2.0


def test_upsert_replace_on_conflict(conn):
    _ensure_table(conn, "test", 'CREATE TABLE test (ts_code TEXT, trade_date TEXT, val REAL, PRIMARY KEY (ts_code, trade_date))')
    df1 = pd.DataFrame({"ts_code": ["A", "A"], "trade_date": ["20200101", "20200102"], "val": [1.0, 2.0]})
    upsert_df(conn, "test", df1)
    df2 = pd.DataFrame({"ts_code": ["A"], "trade_date": ["20200101"], "val": [99.0]})
    upsert_df(conn, "test", df2)
    rows = conn.execute("SELECT * FROM test ORDER BY trade_date").fetchall()
    assert rows[0]["val"] == 99.0
    assert rows[1]["val"] == 2.0
    assert len(rows) == 2


def test_upsert_drop_nan_pk(conn):
    _ensure_table(conn, "test", 'CREATE TABLE test (ts_code TEXT, trade_date TEXT, val REAL, PRIMARY KEY (ts_code, trade_date))')
    df = pd.DataFrame({
        "ts_code": ["A", None, "C"],
        "trade_date": ["20200101", "20200102", "20200103"],
        "val": [1.0, 2.0, 3.0],
    })
    n = upsert_df(conn, "test", df)
    assert n == 2
    rows = conn.execute("SELECT ts_code FROM test").fetchall()
    assert {r["ts_code"] for r in rows} == {"A", "C"}


def test_upsert_drop_all_nan_row(conn):
    _ensure_table(conn, "test", 'CREATE TABLE test (ts_code TEXT PRIMARY KEY, val REAL)')
    df = pd.DataFrame({"ts_code": ["A", "B"], "val": [1.0, np.nan]})
    df.loc[1] = [np.nan, np.nan]  # row 1: both NaN → dropna(how="all") drops it
    # remaining: row 0 (A, 1.0), row 1 (B, NaN) → pk filter drops B/NaN row
    n = upsert_df(conn, "test", df)
    assert n == 1  # only A survives: all-NaN row dropped + NaN pk row dropped


def test_upsert_filter_unknown_columns(conn):
    _ensure_table(conn, "test", 'CREATE TABLE test (ts_code TEXT PRIMARY KEY, val REAL)')
    df = pd.DataFrame({"ts_code": ["A"], "val": [1.0], "extra_col": [999]})
    n = upsert_df(conn, "test", df)
    assert n == 1
    col_names = {r[1] for r in conn.execute('PRAGMA table_info("test")').fetchall()}
    assert "extra_col" not in col_names


def test_upsert_no_pk_table(conn):
    _ensure_table(conn, "test", 'CREATE TABLE test (name TEXT, value REAL)')
    df1 = pd.DataFrame({"name": ["A", "B"], "value": [1.0, 2.0]})
    upsert_df(conn, "test", df1)
    df2 = pd.DataFrame({"name": ["C"], "value": [3.0]})
    upsert_df(conn, "test", df2)
    rows = conn.execute("SELECT * FROM test").fetchall()
    assert len(rows) == 1
    assert rows[0]["name"] == "C"


def test_upsert_all_nan_after_dropna(conn):
    _ensure_table(conn, "test", 'CREATE TABLE test (ts_code TEXT PRIMARY KEY, val REAL)')
    df = pd.DataFrame({"ts_code": [np.nan], "val": [np.nan]})
    n = upsert_df(conn, "test", df)
    assert n == 0


# ── log_pull ok 流转 ──

def _init_pull_log(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pull_log (
            table_name TEXT NOT NULL,
            date_val   TEXT NOT NULL,
            ok         INTEGER NOT NULL,
            retry_count INTEGER NOT NULL DEFAULT 0,
            last_try   TEXT DEFAULT NULL,
            PRIMARY KEY (table_name, date_val)
        )
    """)
    conn.commit()


def test_log_pull_insert_ok1(conn):
    _init_pull_log(conn)
    log_pull(conn, "test_table", "20200101", 1, api="daily")
    row = conn.execute(
        "SELECT ok FROM pull_log WHERE table_name='test_table' AND date_val='20200101'"
    ).fetchone()
    assert row["ok"] == 1


def test_log_pull_insert_ok0(conn):
    _init_pull_log(conn)
    log_pull(conn, "test_table", "20200101", 0, api="daily")
    row = conn.execute(
        "SELECT ok FROM pull_log WHERE table_name='test_table' AND date_val='20200101'"
    ).fetchone()
    assert row["ok"] == 0


def test_log_pull_insert_ok2(conn):
    _init_pull_log(conn)
    log_pull(conn, "test_table", "20200101", 2, api="daily")
    row = conn.execute(
        "SELECT ok FROM pull_log WHERE table_name='test_table' AND date_val='20200101'"
    ).fetchone()
    assert row["ok"] == 2


def test_log_pull_insert_ok3(conn):
    _init_pull_log(conn)
    log_pull(conn, "test_table", "20200101", 3, api="daily")
    row = conn.execute(
        "SELECT ok FROM pull_log WHERE table_name='test_table' AND date_val='20200101'"
    ).fetchone()
    assert row["ok"] == 3


def test_log_pull_invalid_ok(conn):
    _init_pull_log(conn)
    with pytest.raises(ValueError, match="非法 ok 值"):
        log_pull(conn, "test_table", "20200101", 99)


def test_log_pull_update_ok_transition(conn):
    _init_pull_log(conn)
    log_pull(conn, "test_table", "20200101", 0, api="daily")
    log_pull(conn, "test_table", "20200101", 1, api="daily")
    row = conn.execute(
        "SELECT ok FROM pull_log WHERE table_name='test_table' AND date_val='20200101'"
    ).fetchone()
    assert row["ok"] == 1


def test_log_pull_last_try_updated(conn):
    _init_pull_log(conn)
    log_pull(conn, "test_table", "20200101", 0, api="daily")
    row1 = conn.execute(
        "SELECT last_try FROM pull_log WHERE table_name='test_table'"
    ).fetchone()
    assert row1["last_try"] is not None
    import time
    time.sleep(1.1)  # ensure datetime('now','localtime') ticks to next second
    log_pull(conn, "test_table", "20200101", 1, api="daily")
    row2 = conn.execute(
        "SELECT last_try FROM pull_log WHERE table_name='test_table'"
    ).fetchone()
    assert row2["last_try"] is not None
    assert row2["last_try"] != row1["last_try"]


def test_log_pull_multiple_tables(conn):
    _init_pull_log(conn)
    log_pull(conn, "table_a", "20200101", 1)
    log_pull(conn, "table_b", "20200101", 0)
    rows = conn.execute("SELECT table_name, ok FROM pull_log ORDER BY table_name").fetchall()
    assert len(rows) == 2
    assert rows[0]["table_name"] == "table_a"
    assert rows[0]["ok"] == 1
    assert rows[1]["table_name"] == "table_b"
    assert rows[1]["ok"] == 0
