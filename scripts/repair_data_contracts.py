#!/usr/bin/env python3
"""预览/迁移声明的主键契约，按完整请求缓存恢复历史日分区；不调用 Tushare。"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import engine
from database.client import DataClient
from database.utils import _cast_expr, atomic_write_text, beijing_now, load_api_registry
from scripts.generate_schema import generate_table_ddl, infer_pk

ROOT = Path(engine.PROJECT_ROOT)


def contract_pk(api: dict) -> list[str]:
    pk = infer_pk(api, api.get("_project", {}).get("driver"))
    return [c.strip().strip('"') for c in pk.strip("()").split(",")] if pk else []


def complete_cached_day(api: dict, date: str, cache_root: Path) -> pd.DataFrame | None:
    """只有与全市场逐日请求精确匹配、且可确认分页结束的缓存才可替换分区。"""
    max_rows = api.get("max_rows")
    rule = api.get("_project", {}).get("classification", {}).get("rule")
    if (not max_rows or rule not in (1, 2)
            or any(p["name"] == "exchange" for p in api.get("input_params", []))):
        return None
    frames = []
    client = DataClient.__new__(DataClient)
    for page in range(200):
        params = {"trade_date": date}
        if rule == 1:
            params.update(limit=max_rows, offset=page * max_rows)
        key = client._make_key({"api_name": api["api_name"], "params": params, "fields": ""})
        path = cache_root / api["api_name"] / f"{key}.pkl"
        if not path.exists():
            return None
        try:
            df = pd.read_pickle(path)
        except Exception:
            return None
        if (not isinstance(df, pd.DataFrame) or "trade_date" not in df.columns
                or len(df) > max_rows or df.empty
                or df["trade_date"].isna().any()
                or not df["trade_date"].astype(str).eq(date).all()):
            return None
        pk = contract_pk(api)
        if any(c not in df.columns or df[c].isna().any() for c in pk):
            return None
        if frames and df.reset_index(drop=True).equals(frames[0].reset_index(drop=True)):
            return None
        frames.append(df)
        if len(df) < max_rows:
            result = pd.concat(frames, ignore_index=True)
            return result.drop_duplicates(pk, keep="last") if pk else result
        if rule == 2:
            return None
    return None


def build_plan(conn: engine.Connection, apis: list[dict], tables: set[str] | None,
               cache_root: Path | None = None) -> list[dict]:
    plans = []
    for api in apis:
        name, proj = api["api_name"], api.get("_project", {})
        if tables is not None and name not in tables:
            continue
        if not (proj.get("no_pk") or proj.get("pk_override")) or not engine.table_exists(conn, name):
            continue
        clf = proj.get("classification", {})
        if clf.get("rule") not in (1, 2) or not clf.get("usable"):
            continue
        expected, actual = contract_pk(api), engine.primary_key_cols(conn, name)
        mismatch = set(actual) != set(expected)
        daily = ("trade_date" in {p["name"] for p in api.get("input_params", [])
                                 if not p.get("_disabled")}
                 and not proj.get("driver"))
        dates = []
        if daily:
            # 已匹配契约的自动扫描只恢复已登记失败日；显式选表可检查全部历史。
            if mismatch or tables is not None:
                dates = [r[0] for r in conn.execute(
                    f"SELECT DISTINCT trade_date FROM {engine._quote_ident(name)} "
                    "WHERE trade_date IS NOT NULL ORDER BY trade_date").fetchall()]
            states = "ok IN (0,1,3)" if mismatch or tables is not None else "ok=0"
            dates = sorted(set(dates) | {
                r[0] for r in conn.execute(
                    f"SELECT date_val FROM pull_log WHERE table_name=? AND {states}", (name,),
                ).fetchall() if len(r[0]) == 8 and r[0].isdigit()
            })
        if not mismatch and (cache_root is None or not dates):
            continue
        cached = {}
        if cache_root is not None and daily:
            for date in dates:
                df = complete_cached_day(api, date, cache_root)
                if df is not None:
                    cached[date] = len(df)
        if not mismatch and not cached:
            continue
        rows = conn.execute(f"SELECT COUNT(*) FROM {engine._quote_ident(name)}").fetchone()[0]
        unsupported = [r["constraint_text"] for r in engine.table_constraints(conn, name)
                       if r["constraint_type"] not in ("PRIMARY KEY", "NOT NULL")]
        unsupported.extend(r["sql"] for r in engine.table_indexes(conn, name))
        plans.append({
            "table": name, "actual_pk": actual, "expected_pk": expected,
            "migrate": mismatch, "rows_before": rows,
            "cached_days": cached,
            "pending_days": [d for d in dates if d not in cached] if mismatch else [],
            "unsupported_constraints": unsupported if mismatch else [],
        })
    return plans


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def backup_tables(conn: engine.Connection, plans: list[dict], path: Path) -> None:
    path.mkdir(parents=True, exist_ok=False)
    restore = ["BEGIN TRANSACTION;"]
    for plan in plans:
        name = plan["table"]
        target = _sql_string(str(path / f"{name}.parquet"))
        conn.execute(f"COPY {engine._quote_ident(name)} TO {target} (FORMAT PARQUET)")
        log_target = _sql_string(str(path / f"{name}.pull_log.parquet"))
        conn.execute(
            "COPY (SELECT * FROM pull_log WHERE table_name=" + _sql_string(name)
            + f") TO {log_target} (FORMAT PARQUET)")
        columns = engine.table_columns(conn, name)
        defs = []
        for col, meta in columns.items():
            definition = f"{engine._quote_ident(col)} {meta['type']}"
            if meta["notnull"]:
                definition += " NOT NULL"
            if meta["default"] is not None:
                definition += f" DEFAULT {meta['default']}"
            defs.append(definition)
        pk = engine.primary_key_cols(conn, name)
        if pk:
            defs.append("PRIMARY KEY (" + ",".join(engine._quote_ident(c) for c in pk) + ")")
        qt = engine._quote_ident(name)
        restore.extend([
            f"DROP TABLE {qt};", f"CREATE TABLE {qt} (" + ",".join(defs) + ");",
            f"INSERT INTO {qt} SELECT * FROM read_parquet({target});",
            f"DELETE FROM pull_log WHERE table_name={_sql_string(name)};",
            f"INSERT INTO pull_log SELECT * FROM read_parquet({log_target});",
        ])
    atomic_write_text(path / "plan.json", json.dumps(plans, ensure_ascii=False, indent=2))
    atomic_write_text(path / "restore.sql", "\n".join(restore + ["COMMIT;"]) + "\n")


def _insert_frame(conn: engine.Connection, table: str, df: pd.DataFrame) -> None:
    columns = engine.table_columns(conn, table)
    valid = [c for c in df.columns if c in columns]
    missing = set(engine.primary_key_cols(conn, table)) - set(valid)
    if missing or not valid:
        raise ValueError(f"{table} 缓存列不符合契约: missing_pk={sorted(missing)}")
    names = ",".join(engine._quote_ident(c) for c in valid)
    casts = ",".join(_cast_expr(c, columns[c]["type"]) for c in valid)
    conn.register("_contract_repair_df", df[valid])
    try:
        conn.execute(f"INSERT INTO {engine._quote_ident(table)} ({names}) "
                     f"SELECT {casts} FROM _contract_repair_df")
    finally:
        conn.unregister("_contract_repair_df")


def apply_table(conn: engine.Connection, api: dict, plan: dict,
                cache_root: Path | None) -> dict:
    name = api["api_name"]
    if set(engine.primary_key_cols(conn, name)) != set(plan["actual_pk"]):
        raise RuntimeError(f"{name} 主键已改变，请重新预览")
    if plan.get("unsupported_constraints"):
        raise RuntimeError(f"{name} 有额外约束/索引，需要单独迁移")
    stage = f"__contract_repair_{name}"
    target = stage if plan["migrate"] else name
    qt = engine._quote_ident(target)
    try:
        conn.execute("BEGIN TRANSACTION")
        if plan["migrate"]:
            if engine.table_exists(conn, stage):
                raise RuntimeError(f"暂存表 {stage} 已存在，拒绝覆盖")
            ddl = generate_table_ddl(api).replace(
                f'CREATE TABLE IF NOT EXISTS "{name}"', f"CREATE TABLE {qt}", 1,
            )
            conn.execute(ddl)
            old_cols = engine.table_columns(conn, name)
            new_cols = engine.table_columns(conn, stage)
            extras = {c: meta for c, meta in old_cols.items() if c not in new_cols}
            for col, meta in extras.items():
                conn.execute(f"ALTER TABLE {qt} ADD COLUMN {engine._quote_ident(col)} {meta['type']}")
            columns = ",".join(engine._quote_ident(c) for c in old_cols)
            select = f"SELECT {columns} FROM {engine._quote_ident(name)}"
            if plan["cached_days"]:
                # 完整缓存将替换这些日，旧行可能没有新增唯一键，不能先插入旧行。
                select += " WHERE trade_date IS NULL OR trade_date NOT IN (SELECT unnest(?))"
                conn.execute(f"INSERT INTO {qt} ({columns}) {select}", [list(plan["cached_days"])])
            else:
                conn.execute(f"INSERT INTO {qt} ({columns}) {select}")
            for col, meta in extras.items():
                qc = engine._quote_ident(col)
                if meta["default"] is not None:
                    conn.execute(f"ALTER TABLE {qt} ALTER COLUMN {qc} SET DEFAULT {meta['default']}")
                if meta["notnull"] and not meta["pk"]:
                    conn.execute(f"ALTER TABLE {qt} ALTER COLUMN {qc} SET NOT NULL")
            conn.execute(
                "UPDATE pull_log SET ok=0,retry_count=0,last_try=NULL WHERE table_name=? AND ok IN (0,1,3)",
                (name,),
            )
            for date in plan["pending_days"]:
                conn.execute(
                    "INSERT INTO pull_log (table_name,date_val,ok,retry_count,last_try) "
                    "VALUES (?,?,0,0,NULL) ON CONFLICT(table_name,date_val) "
                    "DO UPDATE SET ok=0,retry_count=0,last_try=NULL",
                    (name, date),
                )

        recovered_rows = 0
        for date, expected_rows in plan["cached_days"].items():
            df = complete_cached_day(api, date, cache_root)
            if df is None or len(df) != expected_rows:
                raise RuntimeError(f"{name} {date} 缓存已改变/不完整，请重新预览")
            conn.execute(f"DELETE FROM {qt} WHERE trade_date=?", (date,))
            _insert_frame(conn, target, df)
            restored = conn.execute(f"SELECT COUNT(*) FROM {qt} WHERE trade_date=?", (date,)).fetchone()[0]
            if restored != expected_rows:
                raise RuntimeError(f"{name} {date} 行数核对失败: {restored} != {expected_rows}")
            conn.execute(
                "INSERT INTO pull_log (table_name,date_val,ok,retry_count,last_try) VALUES (?,?,1,0,?) "
                "ON CONFLICT(table_name,date_val) DO UPDATE SET ok=1,retry_count=0,last_try=excluded.last_try",
                (name, date, beijing_now().strftime("%Y-%m-%d %H:%M:%S")),
            )
            recovered_rows += restored
        if plan["migrate"]:
            conn.execute(f"DROP TABLE {engine._quote_ident(name)}")
            conn.execute(f"ALTER TABLE {qt} RENAME TO {engine._quote_ident(name)}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"table": name, "migrated": plan["migrate"],
            "recovered_days": len(plan["cached_days"]), "recovered_rows": recovered_rows,
            "pending_days": len(plan["pending_days"])}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=engine.DEFAULT_DB_PATH)
    parser.add_argument("--tables", help="逗号分隔；默认检查声明了主键覆盖的表")
    parser.add_argument("--restore-cache", action="store_true", help="使用可确认完整的缓存恢复历史日分区")
    parser.add_argument("--cache", default=str(ROOT / "cache/tushare"))
    parser.add_argument("--backup-dir", help="执行时保存旧表及拉取状态的目录，必须不存在")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--apply", action="store_true", help="备份后执行；默认只读预览")
    modes.add_argument("--dry-run", action="store_true", help="只读预览，不调用 API、不改变文件或库")
    args = parser.parse_args()
    tables = {t.strip() for t in args.tables.split(",")} if args.tables else None
    apis = load_api_registry()
    if tables is not None and tables - {a["api_name"] for a in apis}:
        parser.error("--tables 含未知接口")
    cache_root = Path(args.cache) if args.restore_cache else None
    with engine.db_lock(args.db, exclusive=args.apply, timeout=0) as acquired:
        if not acquired:
            raise SystemExit("数据库忙，稍后重新执行")
        with engine.connect(args.db, read_only=not args.apply) as conn:
            plans = build_plan(conn, apis, tables, cache_root)
            print(json.dumps(plans, ensure_ascii=False, indent=2))
            if not args.apply or not plans:
                return
            backup_dir = Path(args.backup_dir).resolve() if args.backup_dir else (
                Path(args.db).resolve().parent / "contract_repairs" / beijing_now().strftime("%Y%m%dT%H%M%S%f")
            )
            if any(p["unsupported_constraints"] for p in plans):
                raise SystemExit("存在需单独处理的约束/索引，未执行迁移")
            backup_tables(conn, plans, backup_dir)
            results = []
            atomic_write_text(backup_dir / "result.json", "[]")
            for plan in plans:
                api = next(a for a in apis if a["api_name"] == plan["table"])
                results.append(apply_table(conn, api, plan, cache_root))
                atomic_write_text(backup_dir / "result.json", json.dumps(results, ensure_ascii=False, indent=2))
            print(f"备份和执行记录: {backup_dir}")
            print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
