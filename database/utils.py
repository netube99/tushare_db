"""数据库核心工具 — 连接、upsert、配置加载."""
from __future__ import annotations

import os
import sqlite3
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import yaml

from database.schema import SCHEMA_SQL

# 项目根目录，所有默认路径相对此处解析
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 北京时间时区
_CST = ZoneInfo("Asia/Shanghai")

# api_index.json 内存缓存
_registry_cache: list[dict] | None = None


def beijing_now() -> datetime:
    """返回北京时间当前时刻."""
    return datetime.now(_CST)


def atomic_write_text(path, text: str) -> None:
    """原子写文本文件：tmp 文件 + os.replace，防 SIGKILL 截断."""
    path = str(path)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp_path, path)


def beijing_today() -> date:
    """返回北京时间的今天日期."""
    return beijing_now().date()


def init_schema(conn: sqlite3.Connection) -> None:
    """初始化数据库表结构（幂等，CREATE IF NOT EXISTS）."""
    if not SCHEMA_SQL.strip():
        raise RuntimeError(
            "database/schema.sql 缺失或为空，请先运行 scripts/generate_schema.py 生成"
        )
    conn.executescript(SCHEMA_SQL)
    # 迁移：为已有 pull_log 表添加新列
    for col, col_def in [
        ("retry_count", "INTEGER NOT NULL DEFAULT 0"),
        ("last_try", "TEXT DEFAULT NULL"),
    ]:
        try:
            conn.execute(f"ALTER TABLE pull_log ADD COLUMN {col} {col_def}")
        except sqlite3.OperationalError:
            pass
    conn.commit()


def get_conn(db_path: str | None = None) -> sqlite3.Connection:
    """获取数据库连接，自动启用 WAL 并初始化 schema.

    check_same_thread=False: SQLite 连接可在多线程间共享（Python GIL 保证
    execute 原子性，WAL 模式下读写不互斥）。上层调用方自行确保不在同一连接
    上并发执行。

    Args:
        db_path: 数据库文件路径。None 或 ":memory:" 则使用项目默认路径。
    """
    if db_path is None:
        db_path = os.path.join(PROJECT_ROOT, "data", "market.db")
    if db_path != ":memory:":
        dir_path = os.path.dirname(os.path.abspath(db_path))
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    init_schema(conn)
    return conn


def load_config(config_path: str | None = None) -> dict:
    """加载 user_config.yaml 配置.

    Args:
        config_path: 配置文件路径。None 则使用项目根目录下的 user_config.yaml。
    """
    if config_path is None:
        config_path = os.path.join(PROJECT_ROOT, "user_config.yaml")
    try:
        with open(config_path) as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        raise FileNotFoundError(
            f"配置文件不存在: {config_path}\n"
            f"请复制 user_config.template.yaml 为 user_config.yaml 并填写配置"
        )


def upsert_df(conn: sqlite3.Connection, table: str, df: pd.DataFrame) -> int:
    """将 DataFrame 写入 SQLite 表，主键冲突时 REPLACE."""
    if df.empty:
        return 0
    df = df.dropna(how="all")
    if df.empty:
        return 0

    # 只保留表中存在的列（过滤 Tushare 未文档化的字段）
    table_info = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    table_cols = {row[1] for row in table_info}
    pk_cols = {row[1] for row in table_info if row[5]}  # row[5] = pk flag
    valid_cols = [c for c in df.columns if c in table_cols]
    df = df[valid_cols]
    if df.empty:
        return 0

    # 丢弃主键列为 NaN 的脏行（NULL pk 不触发 REPLACE，会堆积重复行）
    nan_pk_cols = list(pk_cols & set(df.columns))
    if nan_pk_cols:
        df = df.dropna(subset=nan_pk_cols)
        if df.empty:
            return 0

    columns = df.columns.tolist()
    placeholders = ", ".join(["?"] * len(columns))
    cols_quoted = ", ".join(f'"{c}"' for c in columns)
    sql = f'INSERT OR REPLACE INTO "{table}" ({cols_quoted}) VALUES ({placeholders})'

    rows = [tuple(row) for row in df.itertuples(index=False)]
    try:
        # 无 pk 表：整表替换（仅剩 once 策略快照表适用）；与 INSERT 同事务，失败整体回滚
        if not pk_cols:
            conn.execute(f'DELETE FROM "{table}"')
        conn.executemany(sql, rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return len(rows)


def load_api_registry() -> list[dict]:
    """加载 api_index.json——项目配置的唯一真相源."""
    global _registry_cache
    if _registry_cache is not None:
        return _registry_cache
    import json
    path = os.path.join(PROJECT_ROOT, "api_index.json")
    with open(path) as f:
        _registry_cache = json.load(f)
    return _registry_cache


def invalidate_registry_cache() -> None:
    """api_index.json 被外部修改（如 classify_apis.py）后使缓存失效."""
    global _registry_cache
    _registry_cache = None
