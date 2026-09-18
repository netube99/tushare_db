#!/usr/bin/env python3
"""SQLite market.db → DuckDB market.duckdb 一次性迁移.

用法:
    python scripts/migrate_sqlite_to_duckdb.py --dry-run
    python scripts/migrate_sqlite_to_duckdb.py                 # 全量迁移
    python scripts/migrate_sqlite_to_duckdb.py --tables trade_cal,stock_basic
    python scripts/migrate_sqlite_to_duckdb.py --resume        # 断点续迁

特性:
    - 源库以 READ_ONLY 附加（sqlite 扩展），绝不改动源文件
    - 目标 schema 由 database/schema.sql 生成，类型/约束为 DuckDB 原生
    - 按表记录进度，失败/中断可 --resume
    - 迁移后逐表比对源/目标行数，输出校验报告
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import engine
from database.engine import Connection
from database.utils import _cast_expr, atomic_write_text, init_schema
from qlib_export.sync_log import init_sync_log

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SOURCE = PROJECT_ROOT / "data" / "market.db"
DEFAULT_TARGET = PROJECT_ROOT / "data" / "market.duckdb"


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", default=str(DEFAULT_SOURCE))
    ap.add_argument("--target", default=str(DEFAULT_TARGET))
    ap.add_argument("--tables", default=None, help="仅迁移指定表（逗号分隔）")
    ap.add_argument("--exclude", default=None, help="排除指定表（逗号分隔）")
    ap.add_argument("--resume", action="store_true", help="跳过进度文件中已完成的表")
    ap.add_argument("--dry-run", action="store_true", help="仅列出表与行数")
    ap.add_argument("--force", action="store_true", help="目标文件已存在时覆盖")
    ap.add_argument("--memory-limit", default="16GB")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--keep-going", action="store_true", help="单表失败继续迁移其余表")
    return ap.parse_args()


def _attach_source(conn: Connection, source: str) -> None:
    conn.execute("INSTALL sqlite")
    conn.execute("LOAD sqlite")
    conn.execute(f"ATTACH '{source}' AS sdb (TYPE sqlite, READ_ONLY)")


def _source_table_names(conn: Connection) -> list[str]:
    rows = conn.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name='sdb'").fetchall()
    return sorted(r[0] for r in rows)


def _count(conn: Connection, table: str, source: bool = False) -> int:
    prefix = "sdb." if source else ""
    return conn.execute(
        f'SELECT COUNT(*) FROM {prefix}{engine._quote_ident(table)}').fetchone()[0]


def _target_columns(conn: Connection, table: str) -> dict[str, dict]:
    return engine.table_columns(conn, table)


def _progress_path(target: str) -> Path:
    """进度文件跟随目标库路径，避免不同 target 互相污染."""
    p = Path(target)
    return p.with_name(f".{p.stem}.migrate_progress.json")


def _load_progress(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _save_progress(path: Path, progress: dict) -> None:
    atomic_write_text(path, json.dumps(progress, ensure_ascii=False, indent=1))


def _is_done(conn: Connection, table: str, entry: dict) -> bool:
    """进度完成判定必须与目标库实际状态一致，防止目标重建后误跳过."""
    if entry.get("status") != "done":
        return False
    if not engine.table_exists(conn, table):
        return False
    return _count(conn, table) == entry.get("rows")


def migrate_table(conn: Connection, table: str) -> int:
    """单表 INSERT SELECT（显式列 + 目标类型转换），返回写入行数."""
    target_cols = _target_columns(conn, table)
    src_cols = {r[0] for r in conn.execute(
        "SELECT column_name FROM duckdb_columns() "
        "WHERE database_name='sdb' AND table_name=?", (table,)).fetchall()}
    cols = [c for c in target_cols if c in src_cols]
    if not cols:
        raise RuntimeError(f"{table}: 源/目标无同名列")
    col_sql = ", ".join(engine._quote_ident(c) for c in cols)
    select_sql = ", ".join(_cast_expr(c, target_cols[c]["type"]) for c in cols)
    conn.execute(
        f'INSERT INTO {engine._quote_ident(table)} ({col_sql}) '
        f'SELECT {select_sql} FROM sdb.{engine._quote_ident(table)}')
    return _count(conn, table)


def main() -> None:
    args = parse_args()
    source = str(Path(args.source).resolve())
    target = str(Path(args.target).resolve())

    if not os.path.exists(source):
        print(f"源库不存在: {source}")
        sys.exit(1)
    target_exists = os.path.exists(target)
    if target_exists and not args.force and not args.resume:
        print(f"目标已存在: {target}（--force 覆盖 / --resume 续迁）")
        sys.exit(1)
    if args.force and target_exists:
        os.remove(target)
        target_exists = False
        for suffix in (".wal", "-wal", ".tmp", "-tmp"):
            p = target + suffix
            if os.path.exists(p):
                os.remove(p)

    with engine.db_lock(target, exclusive=True):
        _migrate(args, source, target, target_exists)


def _migrate(args: argparse.Namespace, source: str, target: str,
             target_exists: bool) -> None:
    if not target_exists:
        conn = engine.connect(target)
        init_schema(conn)
        init_sync_log(conn)
        print(f"[1/4] 目标 schema 已建: {target}")
    else:
        conn = engine.connect(target)
        print(f"[1/4] 复用已有目标库: {target}")

    conn.execute(f"SET threads={args.threads}")
    conn.execute(f"SET memory_limit='{args.memory_limit}'")
    conn.execute("SET preserve_insertion_order=false")

    _attach_source(conn, source)
    src_names = _source_table_names(conn)
    print(f"      源库 {len(src_names)} 张表")

    wanted = set(src_names)
    if args.tables:
        wanted &= {t.strip() for t in args.tables.split(",")}
    if args.exclude:
        wanted -= {t.strip() for t in args.exclude.split(",")}

    target_tables = set(engine.list_tables(conn))
    migratable = sorted(wanted & target_tables)
    skipped = sorted(wanted - target_tables)
    if skipped:
        print(f"      跳过（目标无此表）: {', '.join(skipped)}")

    if args.dry_run:
        print(f"\n[2/4] dry-run: 待迁移 {len(migratable)} 张表")
        total = 0
        for t in migratable:
            n = _count(conn, t, source=True)
            total += n
            print(f"  {t:24s} {n:>12,} 行")
        print(f"  合计 {total:,} 行")
        conn.close()
        return

    progress_path = _progress_path(target)
    progress = _load_progress(progress_path) if args.resume else {}
    done: set[str] = set()
    stale: list[str] = []
    for t, v in progress.items():
        if v.get("status") != "done":
            continue
        if _is_done(conn, t, v):
            done.add(t)
        else:
            stale.append(t)
    if done:
        print(f"      进度文件已完成 {len(done)} 张表，跳过")
    if stale:
        print(f"      进度失效（目标缺失或行数不符），重迁: {', '.join(sorted(stale))}")

    print(f"\n[2/4] 开始迁移 {len(migratable)} 张表…")
    t0 = time.time()
    failed = []
    for i, table in enumerate(migratable, 1):
        if table in done:
            continue
        t_start = time.time()
        try:
            n = migrate_table(conn, table)
            conn.commit()
            progress[table] = {"status": "done", "rows": n}
            _save_progress(progress_path, progress)
            dt = time.time() - t_start
            print(f"  [{i}/{len(migratable)}] {table:24s} {n:>12,} 行  {dt:6.1f}s")
        except Exception as e:
            conn.rollback()
            progress[table] = {"status": "error", "error": str(e)[:300]}
            _save_progress(progress_path, progress)
            failed.append((table, str(e)[:200]))
            print(f"  [{i}/{len(migratable)}] {table:24s} FAILED: {str(e)[:160]}")
            if not args.keep_going:
                break

    if failed:
        print(f"\n[3/4] 迁移失败 {len(failed)} 张，修正后 --resume 重跑:")
        for t, e in failed:
            print(f"  {t}: {e}")
    else:
        print(f"\n[3/4] 迁移完成，耗时 {time.time() - t0:.0f}s")

    print("[4/4] 行数校验…")
    mismatches = []
    for table in migratable:
        if progress.get(table, {}).get("status") != "done":
            continue
        src_n = _count(conn, table, source=True)
        dst_n = _count(conn, table)
        if src_n != dst_n:
            mismatches.append((table, src_n, dst_n))
    for t, s, d in mismatches:
        print(f"  MISMATCH {t}: 源 {s} vs 目标 {d}")
    ok = not mismatches and not failed
    if ok:
        print("  全部一致")
    conn.execute("CHECKPOINT")
    conn.close()
    if ok:
        print("完成。验收后可保留 market.db 作为备份。")
    else:
        print("存在失败/不一致，退出码 1；修正后 --resume 重跑。")
        sys.exit(1)


if __name__ == "__main__":
    main()
