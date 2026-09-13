"""database 存储层 TDD 审查 — 缺陷暴露 + 守护回归测试."""

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

import database.utils as utils
from database.utils import (
    PROJECT_ROOT,
    atomic_write_text,
    get_conn,
    init_schema,
    load_api_registry,
    load_config,
    upsert_df,
    invalidate_registry_cache,
)
from database.etl import log_pull


# ── fixtures ──

@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=10000")
    return db


def _table(conn, ddl):
    conn.execute(ddl)
    conn.commit()


# ── upsert_df: 表不存在 / 列完全不匹配 ──

def test_upsert_missing_table_raises(conn):
    _table(conn, "CREATE TABLE other (a TEXT)")
    df = pd.DataFrame({"a": ["A"]})
    with pytest.raises(ValueError, match="nope"):
        upsert_df(conn, "nope", df)


def test_upsert_all_columns_unknown_raises(conn):
    _table(conn, "CREATE TABLE t (a TEXT PRIMARY KEY)")
    df = pd.DataFrame({"x": ["A"], "y": ["B"]})
    with pytest.raises(ValueError, match="t"):
        upsert_df(conn, "t", df)


# ── upsert_df: 主键列整列缺失 ──

def test_upsert_pk_column_missing_raises(conn):
    _table(conn, "CREATE TABLE t (a TEXT PRIMARY KEY, b TEXT)")
    df = pd.DataFrame({"b": ["x", "y"]})
    with pytest.raises(ValueError, match="主键"):
        upsert_df(conn, "t", df)


def test_upsert_partial_pk_missing_raises(conn):
    _table(conn, "CREATE TABLE t (a TEXT, b TEXT, val REAL, PRIMARY KEY (a, b))")
    df = pd.DataFrame({"a": ["A"], "val": [1.0]})
    with pytest.raises(ValueError, match="主键"):
        upsert_df(conn, "t", df)


# ── upsert_df: 不可绑定标量（Timestamp/NaT/pd.NA/np 标量） ──

def test_upsert_datetime64_column(conn):
    _table(conn, "CREATE TABLE t (a TEXT PRIMARY KEY, b TEXT)")
    df = pd.DataFrame({"a": pd.to_datetime(["2020-01-01"]), "b": ["x"]})
    n = upsert_df(conn, "t", df)
    assert n == 1
    row = conn.execute("SELECT a FROM t").fetchone()
    assert row["a"] and str(row["a"]).startswith("2020-01-01")


def test_upsert_nat_in_datetime64_column(conn):
    _table(conn, "CREATE TABLE t (a TEXT PRIMARY KEY, b TEXT)")
    df = pd.DataFrame({"a": ["A", "B"], "b": pd.to_datetime(pd.Series(["2020-01-01", None]))})
    n = upsert_df(conn, "t", df)
    assert n == 2
    vals = {r["b"] for r in conn.execute("SELECT b FROM t").fetchall()}
    assert any(v and str(v).startswith("2020-01-01") for v in vals)
    assert None in vals


def test_upsert_pd_na_object_column(conn):
    _table(conn, "CREATE TABLE t (a TEXT PRIMARY KEY, b TEXT)")
    df = pd.DataFrame({"a": ["A"], "b": pd.array([None], dtype="Int64").astype(object)})
    df.loc[0, "b"] = pd.NA
    n = upsert_df(conn, "t", df)
    assert n == 1
    assert conn.execute("SELECT b FROM t").fetchone()["b"] is None


def test_upsert_np_generic_in_object_column(conn):
    _table(conn, "CREATE TABLE t (a INTEGER PRIMARY KEY, b REAL)")
    df = pd.DataFrame({"a": pd.Series([np.int64(1)], dtype=object),
                       "b": pd.Series([np.float64(1.5)], dtype=object)})
    n = upsert_df(conn, "t", df)
    assert n == 1
    row = conn.execute("SELECT a, typeof(a) FROM t").fetchone()
    assert row["a"] == 1


def test_upsert_nullable_int64_with_na(conn):
    _table(conn, "CREATE TABLE t (a TEXT PRIMARY KEY, b INTEGER)")
    df = pd.DataFrame({"a": ["A"], "b": pd.array([None], dtype="Int64")})
    df.loc[0, "b"] = pd.NA
    assert upsert_df(conn, "t", df) == 1
    assert conn.execute("SELECT b FROM t").fetchone()["b"] is None


def test_upsert_nullable_int64_value(conn):
    _table(conn, "CREATE TABLE t (a TEXT PRIMARY KEY, b INTEGER)")
    df = pd.DataFrame({"a": ["A"], "b": pd.array([5], dtype="Int64")})
    assert upsert_df(conn, "t", df) == 1
    row = conn.execute("SELECT b, typeof(b) AS t FROM t").fetchone()
    assert row["b"] == 5
    assert row["t"] == "integer"


# ── upsert_df: 事务边界守护 ──

def test_upsert_no_pk_table_delete_rolls_back_on_failure(conn):
    class Unbindable:
        pass

    _table(conn, "CREATE TABLE t (a TEXT, b TEXT)")
    conn.execute("INSERT INTO t VALUES ('old', 'x')")
    conn.commit()
    df = pd.DataFrame({"a": ["A"], "b": [Unbindable()]})
    with pytest.raises(Exception):
        upsert_df(conn, "t", df)
    n = conn.execute("SELECT count(*) FROM t").fetchone()[0]
    assert n == 1
    assert conn.execute("SELECT a FROM t").fetchone()["a"] == "old"


def test_upsert_column_order_mismatch(conn):
    _table(conn, "CREATE TABLE t (a TEXT PRIMARY KEY, b REAL, c TEXT)")
    df = pd.DataFrame({"c": ["x"], "a": ["A"], "b": [1.5]})
    assert upsert_df(conn, "t", df) == 1
    row = dict(conn.execute("SELECT * FROM t").fetchone())
    assert row == {"a": "A", "b": 1.5, "c": "x"}


def test_upsert_subset_columns_fill_null(conn):
    _table(conn, "CREATE TABLE t (a TEXT PRIMARY KEY, b REAL, c TEXT)")
    df = pd.DataFrame({"a": ["A"], "b": [2.0]})
    assert upsert_df(conn, "t", df) == 1
    row = dict(conn.execute("SELECT * FROM t").fetchone())
    assert row["c"] is None


def test_upsert_nan_to_null(conn):
    _table(conn, "CREATE TABLE t (a TEXT PRIMARY KEY, b REAL, c TEXT)")
    df = pd.DataFrame({"a": ["A"], "b": [float("nan")], "c": [float("nan")]})
    upsert_df(conn, "t", df)
    row = conn.execute("SELECT b IS NULL AS bnull, c IS NULL AS cnull FROM t").fetchone()
    assert row["bnull"] == 1
    assert row["cnull"] == 1


def test_upsert_int_date_into_text_col(conn):
    _table(conn, "CREATE TABLE t (trade_date TEXT PRIMARY KEY, val REAL)")
    df = pd.DataFrame({"trade_date": [20200101], "val": [1.0]})
    upsert_df(conn, "t", df)
    row = conn.execute("SELECT trade_date, typeof(trade_date) AS t FROM t").fetchone()
    assert row["trade_date"] == "20200101"
    assert row["t"] == "text"


def test_upsert_double_quote_column_name(conn):
    _table(conn, 'CREATE TABLE t ("a""b" TEXT PRIMARY KEY, c TEXT)')
    df = pd.DataFrame({"a\"b": ["x"], "c": ["y"]})
    assert upsert_df(conn, "t", df) == 1
    assert conn.execute('SELECT "a""b" FROM t').fetchone()[0] == "x"


# ── atomic_write_text: 失败清理 ──

def test_atomic_write_text_cleanup_on_replace_failure(tmp_path, monkeypatch):
    p = tmp_path / "f.txt"
    atomic_write_text(p, "hello")

    def bad_replace(src, dst):
        raise OSError("rename failed")

    monkeypatch.setattr(os, "replace", bad_replace)
    with pytest.raises(OSError, match="rename failed"):
        atomic_write_text(p, "new content")
    monkeypatch.undo()
    assert not os.path.exists(str(p) + ".tmp")
    assert p.read_text(encoding="utf-8") == "hello"


def test_atomic_write_text_cleanup_on_write_failure(tmp_path, monkeypatch):
    p = tmp_path / "f.txt"
    atomic_write_text(p, "hello")

    real_open = open

    class Boom:
        def __enter__(self):
            raise OSError("disk full")

        def __exit__(self, *a):
            return False

    def bad_open(path, mode="r", *a, **k):
        if str(path).endswith(".tmp") and "w" in str(mode):
            return Boom()
        return real_open(path, mode, *a, **k)

    monkeypatch.setattr("builtins.open", bad_open)
    with pytest.raises(OSError, match="disk full"):
        atomic_write_text(p, "new content")
    monkeypatch.undo()
    assert not os.path.exists(str(p) + ".tmp")
    assert p.read_text(encoding="utf-8") == "hello"


def test_atomic_write_text_success(tmp_path):
    p = tmp_path / "f.txt"
    atomic_write_text(p, "数据")
    assert p.read_text(encoding="utf-8") == "数据"
    assert not os.path.exists(str(p) + ".tmp")


# ── JsonLogger ──

def test_logger_non_serializable_payload_falls_back(tmp_path):
    from database.logger import JsonLogger
    lg = JsonLogger(log_dir=str(tmp_path))
    lg.write({"bad": object(), "event": "test"})
    log_file = tmp_path / f"maintain_{utils.beijing_today():%Y%m%d}.log"
    entry = json.loads(log_file.read_text(encoding="utf-8"))
    assert isinstance(entry["bad"], str)
    assert entry["event"] == "test"


def test_logger_rotation_and_json_lines(tmp_path, monkeypatch):
    import database.logger as lgmod
    from database.utils import beijing_now
    from datetime import timedelta

    lg = lgmod.JsonLogger(log_dir=str(tmp_path))
    base = datetime(2026, 1, 1, 23, 59, 59, tzinfo=ZoneInfo("Asia/Shanghai"))
    real = beijing_now
    state = {"n": 0}

    def fake_now():
        return base + timedelta(seconds=state["n"])

    monkeypatch.setattr(lgmod, "beijing_now", fake_now)
    lg.write({"event": "a"})
    state["n"] = 2
    lg.write({"event": "b"})
    monkeypatch.setattr(lgmod, "beijing_now", real)

    files = sorted(os.listdir(tmp_path))
    assert files == ["maintain_20260101.log", "maintain_20260102.log"]
    for fn in files:
        for line in (tmp_path / fn).read_text(encoding="utf-8").splitlines():
            entry = json.loads(line)
            assert entry["ts"]
    d1 = (tmp_path / "maintain_20260101.log").read_text(encoding="utf-8").splitlines()
    d2 = (tmp_path / "maintain_20260102.log").read_text(encoding="utf-8").splitlines()
    assert json.loads(d1[0])["event"] == "a"
    assert json.loads(d2[0])["event"] == "b"


def test_logger_creates_missing_dir(tmp_path):
    from database.logger import JsonLogger
    sub = tmp_path / "deep" / "nested"
    lg = JsonLogger(log_dir=str(sub))
    lg.write({"event": "x"})
    assert (sub / f"maintain_{utils.beijing_today():%Y%m%d}.log").exists()


def test_logger_circular_ref_falls_back_to_stderr(tmp_path, capsys):
    from database.logger import JsonLogger
    lg = JsonLogger(log_dir=str(tmp_path))
    d: dict = {}
    d["self"] = d
    lg.write(d)
    assert "{'self'" in capsys.readouterr().err


def test_logger_singleton_thread_safe(monkeypatch):
    import database.logger as lgmod

    real_init = lgmod.JsonLogger.__init__

    def slow_init(self, log_dir="logs"):
        import time
        time.sleep(0.05)
        real_init(self, log_dir=log_dir)

    monkeypatch.setattr(lgmod.JsonLogger, "__init__", slow_init)
    monkeypatch.setattr(lgmod, "_logger", None)

    barrier = threading.Barrier(2)
    results = []

    def worker():
        barrier.wait()
        results.append(lgmod.get_json_logger())

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert results[0] is results[1]
    assert len(results) == 2
    monkeypatch.setattr(lgmod, "_logger", None)


# ── load_config ──

def test_load_config_yaml_error_has_path(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("a: [1, 2\n", encoding="utf-8")
    with pytest.raises(Exception) as exc_info:
        load_config(str(p))
    assert str(p) in str(exc_info.value)


def test_load_config_empty_file(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("", encoding="utf-8")
    assert load_config(str(p)) == {}


def test_load_config_missing_message(tmp_path):
    with pytest.raises(FileNotFoundError, match="template"):
        load_config(str(tmp_path / "nope.yaml"))


# ── get_conn ──

def test_get_conn_wal_busy_timeout_makedirs(tmp_path):
    p = tmp_path / "sub" / "x.db"
    c = get_conn(str(p))
    try:
        assert os.path.exists(p)
        assert c.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert c.execute("PRAGMA busy_timeout").fetchone()[0] == 10000
        assert c.execute("SELECT 1").fetchone()[0] == 1
    finally:
        c.close()


def test_get_conn_memory_stays_memory():
    c = get_conn(":memory:")
    try:
        assert c.execute("PRAGMA database_list").fetchall()[0][2] == ""
    finally:
        c.close()


# ── init_schema ──

def test_init_schema_idempotent(conn):
    init_schema(conn)
    init_schema(conn)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(pull_log)").fetchall()]
    assert cols == ["table_name", "date_val", "ok", "retry_count", "last_try"]


def test_init_schema_migrates_legacy_pull_log():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.execute(
        "CREATE TABLE pull_log (table_name TEXT NOT NULL, date_val TEXT NOT NULL, "
        "ok INTEGER NOT NULL, PRIMARY KEY (table_name, date_val))"
    )
    init_schema(c)
    cols = [r[1] for r in c.execute("PRAGMA table_info(pull_log)").fetchall()]
    assert cols == ["table_name", "date_val", "ok", "retry_count", "last_try"]


def test_init_schema_empty_sql_raises(conn, monkeypatch):
    monkeypatch.setattr(utils, "load_schema_sql", lambda: "")
    with pytest.raises(RuntimeError, match="schema"):
        init_schema(conn)


def test_init_schema_reads_fresh_from_disk(conn, monkeypatch, tmp_path):
    sql_file = tmp_path / "schema.sql"
    sql_file.write_text("CREATE TABLE IF NOT EXISTS fresh_t (a TEXT);", encoding="utf-8")
    monkeypatch.setattr(utils, "load_schema_sql", lambda: sql_file.read_text(encoding="utf-8"))
    init_schema(conn)
    assert conn.execute(
        "SELECT name FROM sqlite_master WHERE name='fresh_t'"
    ).fetchone() is not None
    sql_file.write_text(
        "CREATE TABLE IF NOT EXISTS fresh_t (a TEXT);\n"
        "CREATE TABLE IF NOT EXISTS fresh_u (b TEXT);",
        encoding="utf-8",
    )
    init_schema(conn)
    assert conn.execute(
        "SELECT name FROM sqlite_master WHERE name='fresh_u'"
    ).fetchone() is not None


# ── schema.sql pull_log DDL 与 AGENTS.md 契约一致 ──

def test_schema_sql_pull_log_ddl_matches_contract():
    from database.schema import load_schema_sql
    schema_sql = load_schema_sql()
    idx = schema_sql.index("CREATE TABLE IF NOT EXISTS pull_log")
    snippet = schema_sql[idx:schema_sql.index(";", idx)]
    for field in ("table_name", "date_val", "ok", "retry_count", "last_try"):
        assert field in snippet
    assert "PRIMARY KEY (table_name, date_val)" in snippet
    assert "retry_count INTEGER NOT NULL DEFAULT 0" in snippet


# ── load_api_registry 缓存 ──

def test_registry_cache_and_invalidate(tmp_path, monkeypatch):
    monkeypatch.setattr(utils, "_registry_cache", None)
    fake = tmp_path / "api_index.json"
    fake.write_text(json.dumps([{"api_name": "daily"}]), encoding="utf-8")
    monkeypatch.setattr(utils, "PROJECT_ROOT", str(tmp_path))

    first = load_api_registry()
    fake.write_text(json.dumps([{"api_name": "daily"}, {"api_name": "x"}]), encoding="utf-8")
    cached = load_api_registry()
    assert cached is first
    invalidate_registry_cache()
    reloaded = load_api_registry()
    assert [a["api_name"] for a in reloaded] == ["daily", "x"]
    monkeypatch.setattr(utils, "_registry_cache", None)


def test_registry_cache_singleton_hit():
    invalidate_registry_cache()
    a = load_api_registry()
    b = load_api_registry()
    assert a is b
    invalidate_registry_cache()


# ── beijing_now / beijing_today ──

def test_beijing_now_offset_utc8():
    from database.utils import beijing_now
    b = beijing_now()
    u = datetime.now(timezone.utc)
    delta = (b.replace(tzinfo=None) - u.replace(tzinfo=None)).total_seconds()
    assert 28795 <= delta <= 28805


def test_beijing_today_is_date():
    from datetime import date as date_type
    from database.utils import beijing_today
    assert isinstance(beijing_today(), date_type)


# ── log_pull 守护（etl.py 契约，只测不改） ──

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


def test_log_pull_preserves_retry_count(conn):
    _init_pull_log(conn)
    log_pull(conn, "t", "20200101", 0, api="x")
    conn.execute("UPDATE pull_log SET retry_count=5 WHERE table_name='t'")
    conn.commit()
    log_pull(conn, "t", "20200101", 1, api="x")
    row = conn.execute("SELECT ok, retry_count FROM pull_log WHERE table_name='t'").fetchone()
    assert row["ok"] == 1
    assert row["retry_count"] == 5
