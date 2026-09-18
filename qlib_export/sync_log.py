"""bin_sync_log 状态机 — 同步状态管理."""

import json

from database import engine
from database.engine import Connection
from database.utils import beijing_now


def init_sync_log(conn: Connection) -> None:
    """创建 bin_sync_log 表（幂等）."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bin_sync_log (
            instrument    VARCHAR NOT NULL,
            source_table  VARCHAR NOT NULL,
            last_date     VARCHAR NOT NULL,
            first_date    VARCHAR NOT NULL DEFAULT '',
            fields_json   VARCHAR NOT NULL,
            row_count     BIGINT,
            status        VARCHAR DEFAULT 'done',
            error_msg     VARCHAR,
            updated_at    VARCHAR,
            PRIMARY KEY (instrument, source_table)
        )
    """)
    cols = engine.table_columns(conn, "bin_sync_log")
    if "first_date" not in cols:
        conn.execute("ALTER TABLE bin_sync_log ADD COLUMN first_date VARCHAR")
        conn.execute("UPDATE bin_sync_log SET first_date='' WHERE first_date IS NULL")
    conn.commit()


def is_synced(conn: Connection, instrument: str, source_table: str,
              desired_fields: list[str]) -> bool:
    """检查 (instrument, table) 是否已完成同步."""
    row = conn.execute(
        "SELECT status, fields_json FROM bin_sync_log WHERE instrument=? AND source_table=?",
        (instrument, source_table)
    ).fetchone()
    if row is None or row["status"] != "done":
        return False
    synced_fields = set(json.loads(row["fields_json"]))
    return set(desired_fields).issubset(synced_fields)


def load_synced_fields(conn: Connection, source_table: str) -> dict[str, set[str]]:
    """一次取该表全部已完成 instrument 的字段集，替代逐 instrument 点查."""
    rows = conn.execute(
        "SELECT instrument, fields_json FROM bin_sync_log "
        "WHERE source_table=? AND status='done'",
        (source_table,),
    ).fetchall()
    return {r["instrument"]: set(json.loads(r["fields_json"])) for r in rows}


def upsert_sync_log(conn: Connection, instrument: str, source_table: str,
                    status: str = "partial", last_date: str = "",
                    first_date: str = "",
                    row_count: int = 0, fields: list[str] | None = None,
                    error_msg: str | None = None) -> None:
    """插入或更新同步状态."""
    now = beijing_now().isoformat()
    fields_json = json.dumps(fields or [], ensure_ascii=False)
    conn.execute(
        """INSERT OR REPLACE INTO bin_sync_log
           (instrument, source_table, last_date, first_date, fields_json,
            row_count, status, error_msg, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (instrument, source_table, last_date, first_date, fields_json,
         row_count, status, error_msg, now)
    )
    conn.commit()


def get_sync_records(conn: Connection, source_table: str) -> list[dict]:
    """获取某表的所有同步记录."""
    rows = conn.execute(
        "SELECT * FROM bin_sync_log WHERE source_table=? AND status='done'",
        (source_table,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_partial_records(conn: Connection) -> list[dict]:
    """获取所有未完成（partial / error）的同步记录."""
    rows = conn.execute(
        "SELECT * FROM bin_sync_log WHERE status IN ('partial', 'error')"
    ).fetchall()
    return [dict(r) for r in rows]


def clear_all_sync_log(conn: Connection) -> None:
    """清除所有同步状态（--reset 使用）."""
    conn.execute("DELETE FROM bin_sync_log")
    conn.commit()
