"""域快照的只读运行状态；不以事件日期密度推断完整性。"""
from datetime import datetime, timedelta

from database.engine import _quote_ident


def domain_values(conn, driver: dict) -> list:
    if "values" in driver:
        return list(driver["values"])
    query = (f'SELECT DISTINCT {_quote_ident(driver["source_column"])} '
             f'FROM {_quote_ident(driver["source_table"])}')
    clauses, params = [], []
    for col, values in driver.get("filters", {}).items():
        clauses.append(f"{_quote_ident(col)} IN (SELECT unnest(?))")
        params.append(values)
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    return [r[0] for r in conn.execute(query + " ORDER BY 1", params).fetchall()]


def snapshot_freshness(conn, table: str, driver: dict, now: datetime) -> dict:
    domains = domain_values(conn, driver)
    states = {r["date_val"]: r for r in conn.execute(
        "SELECT date_val,ok,last_try FROM pull_log WHERE table_name=?", (table,)
    ).fetchall()}
    result = dict(domains=len(domains), completed=0, due=0, failed=0,
                  abandoned=0, pending=0, unknown_success_time=0, oldest_success=None)
    days = driver.get("refresh_days")
    cutoff = now - timedelta(days=days) if days else None
    successes = []
    for domain in domains:
        record = states.get(f"{domain}__once__")
        if record is None:
            result["pending"] += 1
        elif record["ok"] in (1, 2):
            result["completed"] += 1
            try:
                ts = datetime.fromisoformat(record["last_try"])
                ts = ts if ts.tzinfo else ts.replace(tzinfo=now.tzinfo)
            except (TypeError, ValueError):
                result["unknown_success_time"] += 1
                if cutoff:
                    result["due"] += 1
            else:
                successes.append(ts)
                if cutoff and ts <= cutoff:
                    result["due"] += 1
        else:
            result["failed" if record["ok"] == 0 else "abandoned"] += 1
            # 失败后 last_try 已覆盖成功时间，不能据其声称数据新鲜。
            result["unknown_success_time"] += 1
    if successes:
        result["oldest_success"] = min(successes).isoformat()
    return result
