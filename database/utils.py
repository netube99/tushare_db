"""数据库核心工具 — 连接、upsert、配置加载."""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

from database import engine
from database.engine import Connection
from database.schema import load_schema_sql

# 项目根目录，所有默认路径相对此处解析
PROJECT_ROOT = engine.PROJECT_ROOT

# 北京时间时区
_CST = ZoneInfo("Asia/Shanghai")

# api_index.json 内存缓存
_registry_cache: list[dict] | None = None

# 已告警过的 (表, 被丢弃列)：避免每次拉取重复刷日志
_dropped_warned: set[tuple[str, tuple[str, ...]]] = set()


def beijing_now() -> datetime:
    """返回北京时间当前时刻."""
    return datetime.now(_CST)


def atomic_write_text(path, text: str) -> None:
    """原子写文本文件：tmp 文件 + os.replace，防 SIGKILL 截断；失败时清理 tmp."""
    path = str(path)
    tmp_path = path + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def beijing_today() -> date:
    """返回北京时间的今天日期."""
    return beijing_now().date()


def init_schema(conn: Connection) -> None:
    """初始化数据库表结构（幂等，CREATE IF NOT EXISTS，每次调用读取最新 schema.sql）."""
    schema_sql = load_schema_sql()
    if not schema_sql.strip():
        raise RuntimeError(
            "database/schema.sql 缺失或为空，请先运行 scripts/generate_schema.py 生成"
        )
    engine.execute_script(conn, schema_sql)
    _sync_columns(conn, schema_sql)
    conn.commit()


_COLUMN_RE = re.compile(
    r'^\s{4}(?:"([^"]+)"|([A-Za-z_][A-Za-z0-9_]*))\s+'
    r'([A-Za-z][A-Za-z0-9_]*(?:\s*\([^)]*\))?)(.*?),?\s*$')
_DEFAULT_RE = re.compile(r"DEFAULT\s+('[^']*'|[^\s,]+)", re.IGNORECASE)


def _sync_columns(conn: Connection, schema_sql: str) -> None:
    """按当前 schema.sql 为已存在表补新列（DuckDB ADD COLUMN 不接受约束）.

    生成器只在表不存在时 CREATE；接口新增字段后已有表需要这里补列，
    否则 upsert_df 会静默丢弃该列的数据。补列后按 DDL 恢复 DEFAULT/NOT NULL。
    """
    for block in re.finditer(
            r'CREATE TABLE IF NOT EXISTS (?:"([^"]+)"|([A-Za-z_][A-Za-z0-9_]*)) '
            r'\((.*?)\n\);', schema_sql, re.S):
        table, body = block.group(1) or block.group(2), block.group(3)
        existing = engine.table_columns(conn, table)
        if not existing:
            continue
        for line in body.splitlines():
            upper = line.strip().upper()
            if upper.startswith(("PRIMARY KEY", "UNIQUE", "CHECK", "CONSTRAINT")):
                continue
            m = _COLUMN_RE.match(line)
            if m is None:
                continue
            name = m.group(1) or m.group(2)
            if name in existing:
                continue
            col_type, rest = m.group(3), m.group(4) or ""
            qt, qc = engine._quote_ident(table), engine._quote_ident(name)
            conn.execute(f"ALTER TABLE {qt} ADD COLUMN {qc} {col_type}")
            dm = _DEFAULT_RE.search(rest)
            default = dm.group(1) if dm else None
            if default is not None and default.upper() != "NULL":
                conn.execute(f"UPDATE {qt} SET {qc} = {default} WHERE {qc} IS NULL")
                conn.execute(f"ALTER TABLE {qt} ALTER COLUMN {qc} SET DEFAULT {default}")
            if "NOT NULL" in upper and default is not None:
                conn.execute(f"ALTER TABLE {qt} ALTER COLUMN {qc} SET NOT NULL")


def get_conn(db_path: str | None = None, read_only: bool = False) -> Connection:
    """获取 DuckDB 连接并初始化 schema.

    db_path: 数据库文件路径。None 则使用 data/market.duckdb，":memory:" 为内存库。
    read_only: 只读连接不执行 DDL（用于 convert/verify 等读者）。
    文件库为单写者模型，跨进程并发需配合 engine.db_lock。
    """
    if db_path is None:
        db_path = engine.DEFAULT_DB_PATH
    try:
        config = load_config()
    except (FileNotFoundError, yaml.YAMLError):
        config = {}
    conn = engine.connect(
        db_path,
        read_only=read_only,
        threads=config.get("duckdb_threads"),
        memory_limit=config.get("duckdb_memory_limit"),
        checkpoint_threshold=config.get("duckdb_checkpoint_threshold"),
    )
    if not read_only:
        init_schema(conn)
    return conn


def upsert_df(conn: Connection, table: str, df: pd.DataFrame,
              drop_null_pk: bool = True, replace_all: bool = True,
              conflict_cols: list[str] | None = None,
              dedupe_cols: list[str] | None = None,
              pre_delete: tuple[str, list] | None = None) -> int:
    """将 DataFrame 写入 DuckDB 表（DELETE/INSERT 同事务，失败整体回滚）.

    drop_null_pk: 丢弃主键/冲突列为 NULL 的行（DuckDB 主键 NOT NULL，NULL 会
    直接报错）。仅作用于有 PK/UNIQUE 冲突目标的表；dedupe_cols 允许 NULL。
    replace_all: 无冲突目标表默认整表替换（once 快照表适用）；分区替换场景
    （partition_key，如 pledge_detail）配合 pre_delete 置 False。
    conflict_cols: 显式 SQL 冲突列；缺省用表主键。
    dedupe_cols: 仅做批内去重（keep=last），用于无约束表。
    pre_delete: (列, 值列表)，先删目标分区再插入，保证幂等与原子性。
    """
    df = df.dropna(how="all")
    if df.empty:
        return 0

    table_cols = engine.table_columns(conn, table)
    if not table_cols:
        raise ValueError(f"upsert_df: 表 {table} 不存在")
    valid_cols = [c for c in df.columns if c in table_cols]
    if not valid_cols:
        raise ValueError(
            f"upsert_df: DataFrame 列 {list(df.columns)} 与表 {table} 列完全不匹配"
        )
    dropped = [c for c in df.columns if c not in table_cols]
    if dropped:
        key = (table, tuple(sorted(dropped)))
        if key not in _dropped_warned:
            _dropped_warned.add(key)
            logger.warning(
                f"upsert_df: 表 {table} 不存在列 {dropped}，已丢弃；"
                f"若为新增字段请先运行 scripts/generate_schema.py + init_schema")
    df = df[valid_cols]

    conflict = list(conflict_cols) if conflict_cols else engine.primary_key_cols(conn, table)

    if conflict:
        missing_pk = set(conflict) - set(df.columns)
        if missing_pk:
            raise ValueError(f"upsert_df: 表 {table} 主键列 {sorted(missing_pk)} 不在 DataFrame 中")
        if drop_null_pk:
            df = df.dropna(subset=conflict)
        # DuckDB 批内重复键保留首行；SQLite REPLACE 为 last-wins，入库前去重对齐
        df = df.drop_duplicates(subset=conflict, keep="last")
    elif dedupe_cols:
        missing = set(dedupe_cols) - set(df.columns)
        if missing:
            raise ValueError(f"upsert_df: 表 {table} 去重列 {sorted(missing)} 不在 DataFrame 中")
        df = df.drop_duplicates(subset=dedupe_cols, keep="last")

    if df.empty:
        return 0

    col_sql = ", ".join(engine._quote_ident(c) for c in df.columns)
    select_sql = ", ".join(_cast_expr(c, table_cols[c]["type"]) for c in df.columns)
    verb = "INSERT OR REPLACE INTO" if conflict else "INSERT INTO"
    quoted_table = engine._quote_ident(table)

    conn.register("_incoming_df", df)
    try:
        conn.execute("BEGIN TRANSACTION")
        if pre_delete is not None:
            col, values = pre_delete
            conn.execute(
                f'DELETE FROM {quoted_table} '
                f'WHERE {engine._quote_ident(col)} IN (SELECT unnest(?))',
                [list(values)],
            )
        elif not conflict and replace_all:
            conn.execute(f"DELETE FROM {quoted_table}")
        conn.execute(
            f'{verb} {quoted_table} ({col_sql}) '
            f"SELECT {select_sql} FROM _incoming_df"
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.unregister("_incoming_df")
    return len(df)


def _cast_expr(col: str, col_type: str) -> str:
    """生成写入表达式：VARCHAR 用 CAST，数值/时间用 TRY_CAST 宽容空串/NaN."""
    quoted = engine._quote_ident(col)
    if str(col_type).upper().startswith("VARCHAR"):
        return f"CAST({quoted} AS VARCHAR)"
    return f"TRY_CAST({quoted} AS {col_type})"


def load_api_registry() -> list[dict]:
    """加载 api_index.json——项目配置的唯一真相源."""
    global _registry_cache
    if _registry_cache is not None:
        return _registry_cache
    path = os.path.join(PROJECT_ROOT, "api_index.json")
    with open(path) as f:
        _registry_cache = json.load(f)
    return _registry_cache


def invalidate_registry_cache() -> None:
    """api_index.json 被外部修改（如 classify_apis.py）后使缓存失效."""
    global _registry_cache
    _registry_cache = None


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
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"配置文件 YAML 解析错误: {config_path}\n{e}") from e


__all__ = [
    "PROJECT_ROOT", "Connection", "beijing_now", "beijing_today",
    "atomic_write_text", "init_schema", "get_conn", "upsert_df",
    "load_config", "load_api_registry", "invalidate_registry_cache",
]
