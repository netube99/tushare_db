"""DuckDB 引擎层 — 连接封装、行访问、schema 元数据、跨进程锁."""
from __future__ import annotations

import fcntl
import os
import time
from contextlib import contextmanager
from typing import Any, Iterator, Sequence

import duckdb
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB_NAME = "market.duckdb"
DEFAULT_DB_PATH = os.path.join(PROJECT_ROOT, "data", DEFAULT_DB_NAME)
DEFAULT_LOCK_PATH = os.path.join(PROJECT_ROOT, "data", ".market.lock")

_DEFAULT_THREADS = 4
_DEFAULT_CHECKPOINT_THRESHOLD = "1GB"


class Row:
    """sqlite3.Row 替代：支持下标与列名双访问，可被 dict()/tuple() 消费."""

    __slots__ = ("_columns", "_values")

    def __init__(self, columns: Sequence[str], values: Sequence[Any]):
        self._columns = tuple(columns)
        self._values = tuple(values)

    def __getitem__(self, key):
        if isinstance(key, (int, slice)):
            return self._values[key]
        try:
            return self._values[self._columns.index(key)]
        except ValueError:
            raise IndexError(f"no such column: {key}") from None

    def get(self, key: str, default=None):
        try:
            return self[key]
        except (IndexError, ValueError):
            return default

    def __eq__(self, other) -> bool:
        if isinstance(other, Row):
            return self._values == other._values
        if isinstance(other, tuple):
            return self._values == other
        return NotImplemented

    def keys(self) -> tuple[str, ...]:
        return self._columns

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        pairs = ", ".join(f"{c}={v!r}" for c, v in zip(self._columns, self._values))
        return f"Row({pairs})"


class Result:
    """execute() 返回值：fetchone/fetchall 产出 Row，df() 产出 DataFrame.

    DuckDB 连接即游标：后续 execute 会覆盖未消费结果。Result 记录发起时的
    序号，跨 execute 再消费会显式报错而不是静默返回错误行。
    """

    __slots__ = ("_conn", "_columns", "_seq")

    def __init__(self, conn: "Conn", columns: Sequence[str], seq: int):
        self._conn = conn
        self._columns = tuple(columns)
        self._seq = seq

    def _check(self) -> None:
        if self._conn._seq != self._seq:
            raise RuntimeError(
                "Result 已被后续 execute 覆盖（DuckDB 连接即游标）；"
                "请在 execute 后立即 fetchall/fetchone/df")

    @property
    def columns(self) -> tuple[str, ...]:
        self._check()
        return self._columns

    @property
    def description(self):
        self._check()
        return self._conn.raw.description

    def fetchone(self) -> Row | None:
        self._check()
        values = self._conn.raw.fetchone()
        if values is None:
            return None
        return Row(self._columns, values)

    def fetchall(self) -> list[Row]:
        self._check()
        return [Row(self._columns, v) for v in self._conn.raw.fetchall()]

    def df(self) -> pd.DataFrame:
        self._check()
        return self._conn.raw.df()

    def __iter__(self) -> Iterator[Row]:
        return iter(self.fetchall())


class Conn:
    """DuckDB 连接薄封装：保留 sqlite3.Connection 的常用调用形态."""

    def __init__(self, raw: duckdb.DuckDBPyConnection, path: str | None = None,
                 read_only: bool = False):
        self._raw = raw
        self.path = path
        self.read_only = read_only
        self._closed = False
        self._seq = 0

    @property
    def raw(self) -> duckdb.DuckDBPyConnection:
        return self._raw

    def _columns(self) -> tuple[str, ...]:
        desc = self._raw.description
        if not desc:
            return ()
        return tuple(d[0] for d in desc)

    def execute(self, sql, params=None) -> Result:
        if params is None:
            self._raw.execute(sql)
        else:
            self._raw.execute(sql, params)
        self._seq += 1
        return Result(self, self._columns(), self._seq)

    def executemany(self, sql: str, rows) -> None:
        self._raw.executemany(sql, rows)

    def register(self, name: str, obj) -> None:
        self._raw.register(name, obj)

    def unregister(self, name: str) -> None:
        self._raw.unregister(name)

    def cursor(self) -> "Conn":
        return Conn(self._raw.cursor(), self.path, self.read_only)

    def commit(self) -> None:
        self._raw.commit()

    def rollback(self) -> None:
        try:
            self._raw.rollback()
        except duckdb.TransactionException:
            pass

    def close(self) -> None:
        if not self._closed:
            self._raw.close()
            self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> "Conn":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def connect(db_path: str | None = None, read_only: bool = False,
            threads: int | None = None, memory_limit: str | None = None,
            checkpoint_threshold: str | None = None) -> Conn:
    """建立 DuckDB 连接（文件库或内存库），应用节流/内存配置."""
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    config: dict[str, Any] = {}
    config["threads"] = threads or _DEFAULT_THREADS
    if memory_limit:
        config["memory_limit"] = memory_limit
    config["checkpoint_threshold"] = checkpoint_threshold or _DEFAULT_CHECKPOINT_THRESHOLD

    if db_path == ":memory:":
        if read_only:
            raise ValueError("内存库不支持 read_only")
        raw = duckdb.connect(":memory:", config=config)
        return Conn(raw, db_path, read_only=False)
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    if not os.path.exists(db_path):
        if read_only:
            raise FileNotFoundError(
                f"只读打开失败：数据库不存在 {db_path}；请先运行 scripts/maintain.py 建库")
    else:
        with open(db_path, "rb") as f:
            header = f.read(16)
        if header.startswith(b"SQLite format 3"):
            raise RuntimeError(
                f"{db_path} 是 SQLite 文件，DuckDB 无法打开；"
                f"请先运行 scripts/migrate_sqlite_to_duckdb.py 迁移")
    raw = duckdb.connect(db_path, read_only=read_only, config=config)
    return Conn(raw, db_path, read_only=read_only)


def execute_script(conn: Conn, sql: str) -> None:
    """执行多语句 SQL 脚本（executescript 替代），失败向上抛."""
    for statement in duckdb.extract_statements(sql):
        conn.execute(statement)


def table_columns(conn: Conn, table: str) -> dict[str, dict]:
    """返回 {列名: {type, notnull, default, pk}}；表不存在返回空 dict."""
    rows = conn.execute(
        "SELECT column_name, data_type, is_nullable, column_default "
        "FROM duckdb_columns() "
        "WHERE database_name = current_database() AND table_name = ? "
        "ORDER BY column_index",
        (table,),
    ).fetchall()
    if not rows:
        return {}
    pk_cols = set(primary_key_cols(conn, table))
    return {
        r["column_name"]: {
            "type": r["data_type"],
            "notnull": not bool(r["is_nullable"]),
            "default": r["column_default"],
            "pk": r["column_name"] in pk_cols,
        }
        for r in rows
    }


def primary_key_cols(conn: Conn, table: str) -> list[str]:
    """主键列；无主键返回 []."""
    try:
        row = conn.execute(
            "SELECT constraint_column_names FROM duckdb_constraints() "
            "WHERE database_name = current_database() "
            "AND table_name = ? AND constraint_type = 'PRIMARY KEY'",
            (table,),
        ).fetchone()
    except duckdb.Error:
        return []
    if row is None:
        return []
    return [str(c) for c in row[0]]


def unique_constraints(conn: Conn, table: str) -> list[list[str]]:
    """UNIQUE 约束列组列表."""
    try:
        rows = conn.execute(
            "SELECT constraint_column_names FROM duckdb_constraints() "
            "WHERE database_name = current_database() "
            "AND table_name = ? AND constraint_type = 'UNIQUE'",
            (table,),
        ).fetchall()
    except duckdb.Error:
        return []
    return [[str(c) for c in r[0]] for r in rows]


def table_exists(conn: Conn, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM duckdb_tables() "
        "WHERE table_name = ? AND database_name = current_database()",
        (table,),
    ).fetchone()
    return row is not None


def list_tables(conn: Conn) -> list[str]:
    return [r[0] for r in conn.execute(
        "SELECT table_name FROM duckdb_tables() "
        "WHERE database_name = current_database() ORDER BY table_name").fetchall()]


def checkpoint(conn: Conn) -> None:
    conn.execute("CHECKPOINT")


def database_size(conn: Conn) -> Row | None:
    return conn.execute("PRAGMA database_size").fetchone()


def _lock_path(db_path: str | None) -> str | None:
    if db_path is None:
        return DEFAULT_LOCK_PATH
    if db_path == ":memory:":
        return None
    return os.path.join(os.path.dirname(os.path.abspath(db_path)), ".market.lock")


@contextmanager
def db_lock(db_path: str | None = None, exclusive: bool = True,
            timeout: float | None = None) -> Iterator[bool]:
    """跨进程库锁：写用排他、读用共享；DuckDB 不允许跨进程读写并存.

    timeout=None 无限等待；timeout=0 立即返回 False；timeout>0 轮询到超时。
    不可重入：同进程对同一路径嵌套获取（即使 timeout=0）会失败，禁止嵌套调用。
    """
    path = _lock_path(db_path)
    if path is None:
        yield True
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
    flags = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
    acquired = False
    try:
        if timeout is None:
            fcntl.flock(fd, flags)
            acquired = True
        else:
            deadline = time.monotonic() + timeout
            acquired = False
            while True:
                try:
                    fcntl.flock(fd, flags | fcntl.LOCK_NB)
                    acquired = True
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        break
                    time.sleep(0.2)
        yield acquired
    finally:
        if acquired:
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


Connection = Conn
