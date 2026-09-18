"""DuckDB 迁移回归 — 引擎语义 / 事务 / 锁 / dividend 可空主键 / 脚本幂等."""

import multiprocessing as mp

import pandas as pd
import pytest

from database import engine
from database.engine import connect
from database.etl import log_pull
from database.utils import get_conn, init_schema, upsert_df


# ── Row / Result ──


def test_row_index_key_and_dict_access():
    conn = connect(":memory:")
    conn.execute("CREATE TABLE t (a BIGINT, b VARCHAR)")
    conn.execute("INSERT INTO t VALUES (1, 'x')")
    row = conn.execute("SELECT a, b FROM t").fetchone()
    assert row[0] == 1
    assert row["a"] == 1
    assert dict(row) == {"a": 1, "b": "x"}
    assert tuple(row) == (1, "x")
    assert row.get("nope", "d") == "d"


def test_result_interleaved_execute_raises_not_silent_wrong_rows():
    conn = connect(":memory:")
    conn.execute("CREATE TABLE t (a BIGINT, b VARCHAR)")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [(1, "one"), (2, "two")])
    r1 = conn.execute("SELECT a, b FROM t ORDER BY a")
    conn.execute("SELECT b FROM t ORDER BY a")
    with pytest.raises(RuntimeError, match="覆盖"):
        r1.fetchall()


def test_row_missing_key_raises_index_error_and_equality():
    conn = connect(":memory:")
    conn.execute("CREATE TABLE t (a BIGINT, b VARCHAR)")
    conn.execute("INSERT INTO t VALUES (1, 'x')")
    row = conn.execute("SELECT a, b FROM t").fetchone()
    with pytest.raises(IndexError):
        row["nope"]
    assert row == (1, "x")
    assert row != (9, "x")


def test_table_columns_handles_embedded_quote_name():
    conn = connect(":memory:")
    conn.execute('CREATE TABLE "weird""name" (a BIGINT, b VARCHAR)')
    cols = engine.table_columns(conn, 'weird"name')
    assert set(cols) == {"a", "b"}


def test_primary_key_cols_scoped_to_current_database():
    conn = connect(":memory:")
    conn.execute("CREATE TABLE t (a BIGINT, PRIMARY KEY (a))")
    conn.execute("ATTACH ':memory:' AS other")
    conn.execute("CREATE TABLE other.t (b VARCHAR, c BIGINT, PRIMARY KEY (b, c))")
    assert engine.primary_key_cols(conn, "t") == ["a"]


def test_result_iteration_and_empty_fetchone():
    conn = connect(":memory:")
    conn.execute("CREATE TABLE t (a BIGINT)")
    conn.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
    assert [r["a"] for r in conn.execute("SELECT a FROM t ORDER BY a")] == [1, 2]
    assert conn.execute("SELECT a FROM t WHERE a=99").fetchone() is None
    assert conn.execute("SELECT a FROM t WHERE a=99").fetchall() == []


def test_execute_script_runs_all_statements():
    conn = connect(":memory:")
    engine.execute_script(conn, "CREATE TABLE s1 (a BIGINT); CREATE TABLE s2 (b BIGINT);")
    assert engine.table_exists(conn, "s1")
    assert engine.table_exists(conn, "s2")
    assert conn.execute("SELECT COUNT(*) FROM duckdb_tables()").fetchone()[0] == 2


def test_init_schema_idempotent_with_generated_sql():
    conn = connect(":memory:")
    init_schema(conn)
    first = conn.execute("SELECT COUNT(*) FROM duckdb_tables()").fetchone()[0]
    init_schema(conn)
    assert conn.execute("SELECT COUNT(*) FROM duckdb_tables()").fetchone()[0] == first
    assert first > 40


# ── upsert 语义 ──


def test_upsert_batch_duplicate_pk_last_wins():
    conn = connect(":memory:")
    conn.execute("CREATE TABLE t (ts_code VARCHAR, d VARCHAR, v DOUBLE, PRIMARY KEY (ts_code, d))")
    df = pd.DataFrame({"ts_code": ["A", "A"], "d": ["20240101", "20240101"], "v": [1.0, 2.0]})
    upsert_df(conn, "t", df)
    assert conn.execute("SELECT v FROM t").fetchone()[0] == 2.0
    assert conn.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 1


def test_upsert_no_pk_plain_insert_replace_all():
    conn = connect(":memory:")
    conn.execute("CREATE TABLE t (a VARCHAR, b DOUBLE)")
    upsert_df(conn, "t", pd.DataFrame({"a": ["x"], "b": [1.0]}))
    upsert_df(conn, "t", pd.DataFrame({"a": ["y"], "b": [2.0]}))
    assert conn.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 1
    assert conn.execute("SELECT a FROM t").fetchone()[0] == "y"


def test_upsert_pre_delete_partition_atomic():
    conn = connect(":memory:")
    conn.execute("CREATE TABLE t (ts_code VARCHAR, d VARCHAR CHECK (d <> 'bad'))")
    upsert_df(conn, "t", pd.DataFrame({"ts_code": ["A", "B"], "d": ["20240101", "20240102"]}),
              replace_all=False)
    with pytest.raises(Exception):
        upsert_df(conn, "t", pd.DataFrame({"ts_code": ["A"], "d": ["bad"]}),
                  replace_all=False, pre_delete=("ts_code", ["A"]))
    rows = {tuple(r) for r in conn.execute("SELECT ts_code, d FROM t")}
    assert rows == {("A", "20240101"), ("B", "20240102")}


def test_upsert_try_cast_tolerates_empty_string_numeric():
    conn = connect(":memory:")
    conn.execute("CREATE TABLE t (d VARCHAR, v DOUBLE)")
    upsert_df(conn, "t", pd.DataFrame({"d": ["20240101"], "v": [""]}), replace_all=False)
    assert conn.execute("SELECT v FROM t").fetchone()[0] is None


def test_upsert_missing_table_raises_value_error():
    conn = connect(":memory:")
    with pytest.raises(ValueError, match="不存在"):
        upsert_df(conn, "nope", pd.DataFrame({"a": ["x"]}))


# ── dividend 无主键 + 可空 ann_date ──


def test_dividend_null_ann_date_roundtrip_and_partition_replace():
    conn = connect(":memory:")
    init_schema(conn)
    row = {"ts_code": "000001.SZ", "ann_date": None, "div_proc": "预案", "cash_div": 0.0}
    upsert_df(conn, "dividend", pd.DataFrame([row]), replace_all=False)
    assert conn.execute(
        "SELECT COUNT(*) FROM dividend WHERE ann_date IS NULL").fetchone()[0] == 1
    upsert_df(conn, "dividend", pd.DataFrame([row]), replace_all=False,
              dedupe_cols=["ts_code", "ann_date", "div_proc"],
              pre_delete=("ts_code", ["000001.SZ"]))
    assert conn.execute("SELECT COUNT(*) FROM dividend").fetchone()[0] == 1


def test_dividend_registry_has_dedupe_and_partition():
    from database.etl import REGISTRY
    entry = next(e for e in REGISTRY if e["table"] == "dividend")
    assert entry.get("partition_key") == "ts_code"
    assert entry.get("dedupe_cols") == ["ts_code", "ann_date", "div_proc"]


# ── log_pull ──


def test_log_pull_last_try_is_beijing_time():
    conn = connect(":memory:")
    conn.execute("CREATE TABLE pull_log (table_name VARCHAR, date_val VARCHAR, ok BIGINT, "
                 "retry_count BIGINT DEFAULT 0, last_try VARCHAR, PRIMARY KEY (table_name, date_val))")
    log_pull(conn, "t", "20240101", 1, api="t")
    from database.utils import beijing_now
    row = conn.execute("SELECT last_try FROM pull_log").fetchone()
    assert row[0] == beijing_now().strftime("%Y-%m-%d %H:%M:%S")


# ── db_lock ──


def test_db_lock_exclusive_conflicts_shared():
    with engine.db_lock("/tmp/opencode/locktest.duckdb", exclusive=True, timeout=0) as a:
        assert a is True
        with engine.db_lock("/tmp/opencode/locktest.duckdb", exclusive=True, timeout=0) as b:
            assert b is False
        with engine.db_lock("/tmp/opencode/locktest.duckdb", exclusive=False, timeout=0) as c:
            assert c is False


def test_db_lock_shared_allows_shared():
    with engine.db_lock("/tmp/opencode/locktest2.duckdb", exclusive=False, timeout=0) as a:
        assert a is True
        with engine.db_lock("/tmp/opencode/locktest2.duckdb", exclusive=False, timeout=0) as b:
            assert b is True


def test_db_lock_memory_is_noop():
    with engine.db_lock(":memory:", exclusive=True, timeout=0) as a:
        assert a is True


def _hold_lock(db_path: str, started) -> None:
    import time
    with engine.db_lock(db_path, exclusive=True):
        started.set()
        time.sleep(2)


def test_db_lock_blocks_other_process(tmp_path):
    db = str(tmp_path / "x.duckdb")
    ctx = mp.get_context("spawn")
    started = ctx.Event()
    proc = ctx.Process(target=_hold_lock, args=(db, started))
    proc.start()
    try:
        started.wait(10)
        with engine.db_lock(db, exclusive=True, timeout=0) as acquired:
            assert acquired is False
    finally:
        proc.join(10)


# ── 连接保护 ──


def test_connect_file_and_read_only(tmp_path):
    p = str(tmp_path / "m.duckdb")
    c = connect(p)
    c.execute("CREATE TABLE t (a BIGINT)")
    c.execute("INSERT INTO t VALUES (1)")
    c.close()
    ro = connect(p, read_only=True)
    assert ro.execute("SELECT a FROM t").fetchone()[0] == 1
    with pytest.raises(Exception):
        ro.execute("INSERT INTO t VALUES (2)")
    ro.close()


def test_connect_rejects_legacy_sqlite_file(tmp_path):
    p = tmp_path / "legacy.db"
    p.write_bytes(b"SQLite format 3\x00" + b"\x00" * 100)
    with pytest.raises(RuntimeError, match="SQLite"):
        connect(str(p))


def test_connect_memory_read_only_is_rejected():
    with pytest.raises(ValueError, match="read_only"):
        connect(":memory:", read_only=True)


def test_connect_read_only_missing_file_friendly_error(tmp_path):
    with pytest.raises(FileNotFoundError, match="数据库不存在"):
        connect(str(tmp_path / "missing.duckdb"), read_only=True)


def test_get_conn_read_only_skips_ddl(tmp_path):
    p = str(tmp_path / "ro.duckdb")
    conn = connect(p)
    conn.execute("CREATE TABLE t (a BIGINT)")
    conn.close()
    ro = get_conn(p, read_only=True)
    assert not engine.table_exists(ro, "pull_log")
    ro.close()


# ── Engine CHECKPOINT / 元数据 ──


def test_checkpoint_and_database_size():
    conn = connect(":memory:")
    conn.execute("CREATE TABLE t (a BIGINT)")
    engine.checkpoint(conn)
    assert engine.database_size(conn) is not None


def test_metadata_helpers():
    conn = connect(":memory:")
    conn.execute("CREATE TABLE t (a VARCHAR, b DOUBLE, PRIMARY KEY (a))")
    cols = engine.table_columns(conn, "t")
    assert set(cols) == {"a", "b"}
    assert cols["a"]["pk"] is True
    assert engine.primary_key_cols(conn, "t") == ["a"]
    assert engine.unique_constraints(conn, "t") == []
    assert "t" in engine.list_tables(conn)


# ── schema.sql 可直接被 DuckDB 执行 ──


def test_generated_schema_sql_runs_on_duckdb():
    from database.schema import load_schema_sql
    conn = connect(":memory:")
    engine.execute_script(conn, load_schema_sql())
    views = {r[0] for r in conn.execute(
        "SELECT view_name FROM duckdb_views() WHERE schema_name='main'")}
    assert {"dividend_grid", "stk_holdernumber_agg", "stk_holdertrade_agg",
            "pledge_detail_agg", "top_inst_agg"} <= views
