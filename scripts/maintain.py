#!/usr/bin/env python3
"""数据库维护 — 建库、每日更新、孤儿表清理.

用法:
    python scripts/maintain.py                  # 全量建库（backfill_since~今天/昨天，收盘后自动到今天）
    python scripts/maintain.py --daily          # 每日盘后更新（自动重分类+重生成+拉最新）
    python scripts/maintain.py --cleanup        # 清理不在 REGISTRY 的孤儿表
    python scripts/maintain.py --api daily      # 只维护单个接口
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time

sys.path.insert(0, '.')
from datetime import date, timedelta
from database import get_conn, load_config, DataClient
from database.etl import REGISTRY, log_pull
import database.etl as _etl_module
from database.utils import upsert_df, beijing_now, beijing_today
from database.client import TushareError, DailyLimitError

logger = logging.getLogger("maintain")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

# 基础设施 once 表：infra 第 4 步每次运行已刷新，daily once 循环不再重复拉取
INFRA_ONCE = ["stock_basic", "index_basic"]

# 基础设施表：非 REGISTRY 系统表 + 常驻表，cleanup 时保留
INFRA_TABLES = {"trade_cal", "pull_log", "stock_basic"}

DEFAULT_BACKFILL_SINCE = "20200101"
DEFAULT_FREQ_VALUES = ["W", "M"]


def _make_pfx(progress: tuple[int, int] | None) -> str:
    """日志前缀：有进度时带 [i/total]."""
    if progress:
        return f"[maintain] [{progress[0]}/{progress[1]}] "
    return "[maintain] "


def _write_run_end(conn, run_id: str, t_start: float) -> None:
    """写入 run_end 日志条目。conn 为 None 时跳过 pull_log 统计."""
    from database.logger import get_json_logger
    jlog = get_json_logger()
    entry = {
        "level": "INFO", "module": "maintain", "event": "run_end",
        "run_id": run_id,
        "elapsed_sec": round(time.time() - t_start, 1),
    }
    if conn is not None:
        stats = conn.execute(
            "SELECT ok, COUNT(*) FROM pull_log GROUP BY ok").fetchall()
        ok_counts = {r[0]: r[1] for r in stats}
        entry.update({
            "total_pulls": sum(ok_counts.values()),
            "ok_1": ok_counts.get(1, 0),
            "ok_2": ok_counts.get(2, 0),
            "ok_0": ok_counts.get(0, 0),
        })
    jlog.write(entry)


def _resolve_until() -> str:
    """根据 pull_after 门禁返回最新拉取日期（收盘后=今天，收盘前=昨天）."""
    config = load_config()
    pull_hour, pull_min = 20, 30
    try:
        parts = config.get("pull_after", "20:30").strip().split(":")
        pull_hour, pull_min = int(parts[0]), int(parts[1])
    except Exception:
        logger.warning(f"[maintain] pull_after 解析失败，使用默认值 {pull_hour:02d}:{pull_min:02d}")
    bj = beijing_now()
    after_close = (bj.hour > pull_hour or (bj.hour == pull_hour and bj.minute >= pull_min))
    until_date = bj.date() if after_close else (bj.date() - timedelta(days=1))
    return until_date.strftime("%Y%m%d")


def _get_date_params(entry: dict) -> dict:
    """通过 input_params 判断日期参数类型，支持 param_fixes 和 domain 策略."""
    from database.utils import load_api_registry
    api_list = load_api_registry()

    for api in api_list:
        if api["api_name"] == entry["api"]:
            fixes = _apply_param_fixes(api)
            active_params = fixes["active_params"]
            is_required = fixes["is_required"]

            driver = entry.get("driver")
            if driver:
                return {"strategy": "domain",
                        "date_col": entry.get("date_col"),
                        "driver": driver}

            if any(p["name"] == "freq" and is_required(p["name"])
                   for p in api.get("input_params", [])):
                return {"strategy": "freq", "date_col": "trade_date",
                        "freq_values": DEFAULT_FREQ_VALUES}
            if "trade_date" in active_params:
                return {"strategy": "trade_date", "date_col": "trade_date",
                        "iter_mode": "trading"}
            if "ann_date" in active_params:
                return {"strategy": "trade_date", "date_col": "ann_date",
                        "iter_mode": "calendar"}
            if "start_date" in active_params and "end_date" in active_params:
                return {"strategy": "date_range",
                        "date_col": entry.get("date_col", "trade_date")}

    return {"strategy": "once", "date_col": None}


def _pull_and_store(conn, table: str, df, date_val: str,
                    api_name: str, strategy: str, log_prefix: str = "") -> bool:
    """统一拉取结果写入：upsert + pull_log。

    Returns: True=成功, False=需重试.
    """
    if df is not None and not df.empty:
        try:
            entry = next((e for e in REGISTRY if e.get("table") == table), None)
            pkey = entry.get("partition_key") if entry else None
            if pkey and pkey in df.columns:
                # 分区替换：先删本域旧行，再插入（无主键表避免重复堆积）
                codes = tuple(str(c) for c in df[pkey].unique())
                placeholders = ",".join(["?"] * len(codes))
                conn.execute(
                    f'DELETE FROM "{table}" WHERE "{pkey}" IN ({placeholders})', codes)
                conn.commit()
            n = upsert_df(conn, table, df,
                          drop_null_pk=not bool(entry and entry.get("null_pk_keep")),
                          replace_all=not bool(entry and entry.get("partition_key")))
            if log_prefix:
                logger.info(f"{log_prefix}{table}: {n} rows")
            else:
                logger.info(f"[maintain] {table}: {n} rows")
            log_pull(conn, table, date_val, 1, api=api_name, rows=n, strategy=strategy)
            return True
        except Exception as e:
            logger.error(f"[maintain] {api_name} 写入异常: {e}")
            log_pull(conn, table, date_val, 0, api=api_name, strategy=strategy)
            return False
    else:
        log_pull(conn, table, date_val, 2, api=api_name, strategy=strategy)
        return True


def _cal_query(conn, select="cal_date", since=None, until=None,
               distinct=False):
    """交易日历查询帮手。统一 since/until 过滤 + ORDER BY."""
    d = "DISTINCT " if distinct else ""
    query = f"SELECT {d}{select} FROM trade_cal WHERE is_open=1"
    params = []
    if since:
        query += " AND cal_date >= ?"; params.append(since)
    if until:
        query += " AND cal_date <= ?"; params.append(until)
    query += " ORDER BY 1 DESC"
    return conn.execute(query, params).fetchall()


def _dispatch_strategy(conn, dc, entry, strategy=None, since=None, until=None,
                       *, progress=None, filter_date_val=None):
    """统一策略分派器。since/until 为 None 时由各策略函数自行决定."""
    if strategy is None:
        strategy = _get_date_params(entry)
    if strategy["strategy"] == "freq":
        entry["freq_values"] = strategy.get("freq_values", DEFAULT_FREQ_VALUES)
        _run_freq_strategy(conn, dc, entry, since, until, progress=progress)
    elif strategy["strategy"] == "trade_date":
        _run_trade_date_strategy(conn, dc, entry, since, until, progress=progress,
                                 iter_mode=strategy.get("iter_mode", "trading"))
    elif strategy["strategy"] == "date_range":
        _run_date_range_strategy(conn, dc, entry, since, until, progress=progress)
    elif strategy["strategy"] == "domain":
        _run_domain_strategy(conn, dc, entry, since, until, progress=progress,
                            filter_date_val=filter_date_val)
    else:
        _run_once_strategy(conn, dc, entry, progress=progress)


def _auto_fix_bounds(strategy: dict, date_val: str) -> tuple[str | None, str | None, str | None]:
    """从 pull_log date_val 解析各策略的 since/until/(filter_date_val).
    返回 (since, until, filter_date_val).
    """
    strat_name = strategy["strategy"]
    if strat_name == "trade_date":
        return date_val, date_val, None
    elif strat_name == "date_range":
        return f"{date_val}0101", f"{date_val}1231", None
    elif strat_name == "freq":
        td, _, _ = date_val.partition("_")
        return td, td, None
    elif strat_name == "domain":
        return None, _resolve_until(), date_val
    else:
        return None, None, None


def _run_trade_date_strategy(conn, dc, entry, since: str | None = None, until: str | None = None,
                            progress: tuple[int, int] | None = None,
                            iter_mode: str = "trading"):
    """逐交易日/自然日迭代拉取（最新→最早）.

    iter_mode: "trading" → 交易日历迭代, "calendar" → 自然日迭代（ann_date 用）.
    """
    api_name = entry["api"]
    table = entry["table"]
    date_col = entry.get("date_col", "trade_date")
    _pfx = _make_pfx(progress)

    if iter_mode == "calendar":
        if not since:
            since = DEFAULT_BACKFILL_SINCE
        if not until:
            until = beijing_now().strftime("%Y%m%d")
        from datetime import date as dt_date
        sy = int(since[:4]); sm = int(since[4:6]); sd = int(since[6:8])
        ey = int(until[:4]); em = int(until[4:6]); ed = int(until[6:8])
        start_d = dt_date(sy, sm, sd)
        end_d = dt_date(ey, em, ed)
        total_days = (end_d - start_d).days + 1
        iter_dates = []
        for d_idx in range(total_days):
            d = start_d + timedelta(days=d_idx)
            iter_dates.append((d.strftime("%Y%m%d"),))
        iter_dates.reverse()
    else:
        iter_dates = _cal_query(conn, since=since, until=until)
        if not iter_dates:
            logger.warning(f"{_pfx}交易日历为空，先拉取日历")
            df = dc.trade_cal()
            if not df.empty:
                upsert_df(conn, "trade_cal", df)
            iter_dates = _cal_query(conn, since=since, until=until)

    # 查出已完成(ok=1)和确认空(ok=2)的日期，都不重拉
    done_dates = set()
    for row in conn.execute(
        'SELECT date_val FROM pull_log WHERE table_name=? AND ok IN (1,2)', (table,)
    ).fetchall():
        done_dates.add(row[0])

    api_func = getattr(dc, api_name)
    total = len(iter_dates)
    filled = 0

    for i, (td,) in enumerate(iter_dates):
        if td in done_dates:
            filled += 1
            continue

        logger.info(f"{_pfx}{api_name}({date_col}={td}) → {table} [{i+1}/{total}]")
        try:
            df = api_func(**{date_col: td})
        except DailyLimitError as e:
            logger.error(f"{_pfx}{api_name} 天级限流: {e}")
            break
        except TushareError as e:
            logger.error(f"{_pfx}{api_name} Tushare错误: {e}")
            log_pull(conn, table, td, 0, api=api_name, strategy="trade_date")
            continue
        except Exception as e:
            logger.error(f"{_pfx}{api_name} 调用异常: {e}")
            log_pull(conn, table, td, 0, api=api_name, strategy="trade_date")
            continue

        _pull_and_store(conn, table, df, td, api_name, "trade_date", _pfx)
        filled += 1

    logger.info(f"{_pfx}{api_name}: {filled}/{total} 完成")


def _run_date_range_strategy(conn, dc, entry, since: str | None = None, until: str | None = None,
                            progress: tuple[int, int] | None = None):
    """按年批次拉取（最新→最早）."""
    api_name = entry["api"]
    table = entry["table"]
    _pfx = _make_pfx(progress)

    years = [r[0] for r in _cal_query(
        conn, select="substr(cal_date,1,4)", since=since, until=until, distinct=True)]
    if not years:
        logger.warning(f"{_pfx}{api_name}: 无交易日历")
        return

    api_func = getattr(dc, api_name)
    for year in years:
        start = f"{year}0101"
        end = f"{year}1231"

        # 检查是否已完成
        done = conn.execute(
            'SELECT COUNT(*) FROM pull_log WHERE table_name=? AND date_val=? AND ok IN (1,2)',
            (table, year)
        ).fetchone()[0]
        if done:
            logger.info(f"{_pfx}{api_name} {year}: 已完成，跳过")
            continue

        logger.info(f"{_pfx}{api_name}(start_date={start}, end_date={end}) → {table}")
        try:
            df = api_func(start_date=start, end_date=end)
        except DailyLimitError as e:
            logger.error(f"{_pfx}{api_name} {year} 天级限流: {e}")
            break
        except TushareError as e:
            logger.error(f"{_pfx}{api_name} {year} Tushare错误: {e}")
            log_pull(conn, table, year, 0, api=api_name, strategy="date_range")
            continue
        except Exception as e:
            logger.error(f"{_pfx}{api_name} {year} 调用异常: {e}")
            log_pull(conn, table, year, 0, api=api_name, strategy="date_range")
            continue

        _pull_and_store(conn, table, df, year, api_name, "date_range", _pfx)


def _run_once_strategy(conn, dc, entry, progress: tuple[int, int] | None = None):
    """一次性拉全量."""
    api_name = entry["api"]
    table = entry["table"]
    _pfx = _make_pfx(progress)

    done = conn.execute(
        'SELECT COUNT(*) FROM pull_log WHERE table_name=? AND date_val=? AND ok IN (1,2)',
        (table, "__once__")
    ).fetchone()[0]
    if done:
        logger.info(f"{_pfx}{api_name}: 已完成（一次性），跳过")
        return

    logger.info(f"{_pfx}{api_name} → {table}")
    api_func = getattr(dc, api_name)
    kwargs = entry.get("default_params", {})
    try:
        df = api_func(**kwargs)
    except DailyLimitError as e:
        logger.error(f"{_pfx}{api_name} 天级限流: {e}")
        return
    except TushareError as e:
        logger.error(f"{_pfx}{api_name} Tushare错误: {e}")
        log_pull(conn, table, "__once__", 0, api=api_name, strategy="once")
        return
    except Exception as e:
        logger.error(f"{_pfx}{api_name} 调用异常: {e}")
        log_pull(conn, table, "__once__", 0, api=api_name, strategy="once")
        return

    _pull_and_store(conn, table, df, "__once__", api_name, "once", _pfx)


def _run_freq_strategy(conn, dc, entry, since=None, until=None,
                      progress: tuple[int, int] | None = None):
    """逐交易日 × freq 值迭代拉取."""
    api_name = entry["api"]
    table = entry["table"]
    freq_values = entry.get("freq_values", DEFAULT_FREQ_VALUES)
    _pfx = _make_pfx(progress)

    trading_days = _cal_query(conn, since=since, until=until)

    api_func = getattr(dc, api_name)
    total = len(trading_days) * len(freq_values)
    filled = 0

    for td_idx, (td,) in enumerate(trading_days):
        for fv_idx, fv in enumerate(freq_values):
            seq = td_idx * len(freq_values) + fv_idx + 1
            date_key = f"{td}_{fv}"
            done = conn.execute(
                'SELECT COUNT(*) FROM pull_log WHERE table_name=? AND date_val=? AND ok IN (1,2)',
                (table, date_key)
            ).fetchone()[0]
            if done:
                filled += 1
                continue

            logger.info(f"{_pfx}{api_name}(trade_date={td}, freq={fv}) → {table} [{seq}/{total}]")
            try:
                df = api_func(trade_date=td, freq=fv)
            except DailyLimitError as e:
                logger.error(f"{_pfx}{api_name} 天级限流: {e}")
                return
            except TushareError as e:
                logger.error(f"{_pfx}{api_name} Tushare错误: {e}")
                log_pull(conn, table, date_key, 0, api=api_name, strategy="freq")
                continue
            except Exception as e:
                logger.error(f"{_pfx}{api_name} 调用异常: {e}")
                log_pull(conn, table, date_key, 0, api=api_name, strategy="freq")
                continue

            _pull_and_store(conn, table, df, date_key, api_name, "freq", _pfx)
            filled += 1

    logger.info(f"{_pfx}{api_name}: {filled}/{total} 完成")


def _get_date_periods(conn, date_mode: str, since: str | None, until: str | None):
    """从 trade_cal 推导日期周期列表，按 date_mode 聚合.

    Returns: list of (period_key, start_date_or_None, end_date_or_None)
    """
    import calendar

    if date_mode == "once":
        return [("__once__", None, None)]

    if date_mode == "daily":
        return [(r[0],) for r in _cal_query(conn, since=since, until=until)]

    if date_mode == "monthly":
        months = [r[0] for r in _cal_query(
            conn, select="substr(cal_date,1,6)", since=since, until=until, distinct=True)]
        periods = []
        for m in months:
            y, mo = int(m[:4]), int(m[4:6])
            last_day = calendar.monthrange(y, mo)[1]
            periods.append((m, f"{y}{mo:02d}01", f"{y}{mo:02d}{last_day}"))
        return periods

    if date_mode == "yearly":
        years = [r[0] for r in _cal_query(
            conn, select="substr(cal_date,1,4)", since=since, until=until, distinct=True)]
        return [(y, f"{y}0101", f"{y}1231") for y in years]

    raise ValueError(f"未知 date_mode: {date_mode}")


def _find_domain_param(entry: dict) -> str | None:
    """找出唯一的 required 非日期/分页参数名（含 force_required 覆盖）."""
    DATE_PAGINATION = {
        "trade_date", "start_date", "end_date", "ann_date",
        "freq", "offset", "limit", "fields",
    }
    from database.utils import load_api_registry
    api_list = load_api_registry()
    for api in api_list:
        if api["api_name"] == entry["api"]:
            fixes = _apply_param_fixes(api)
            candidates = [p["name"] for p in api.get("input_params", [])
                         if fixes["is_required"](p["name"]) and p["name"] not in DATE_PAGINATION]
            return candidates[0] if candidates else None
    return None


def _apply_param_fixes(api: dict) -> dict:
    """应用 _project.param_fixes，返回修正后的 input_params 视图.

    Returns: {"active_params": set, "is_required": callable}
    """
    fixes = api.get("_project", {}).get("param_fixes", {})
    force_required = set(fixes.get("force_required", []))
    force_disabled = set(fixes.get("force_disabled", []))
    key_prefix = api["api_name"] + "."

    active = set()
    for p in api.get("input_params", []):
        full_name = key_prefix + p["name"]
        if full_name in force_disabled:
            continue
        active.add(p["name"])

    def is_required(param_name: str) -> bool:
        full_name = key_prefix + param_name
        if full_name in force_required:
            return True
        for p in api.get("input_params", []):
            if p["name"] == param_name:
                return p.get("required", False)
        return False

    return {"active_params": active, "is_required": is_required}


def _run_domain_strategy(conn, dc, entry, since=None, until=None,
                         progress: tuple[int, int] | None = None,
                         filter_date_val: str | None = None):
    """domain 策略：逐域值 × 逐日期周期拉取.

    Args:
        since: 起始边界，None 则不限（daily 利用 pull_log 实现增量）
        until: 结束边界，None 则由 _resolve_until 自动判定
        filter_date_val: 非 None 时仅处理匹配的单个 date_val（点修复）
    """
    driver = entry["driver"]
    api_name = entry["api"]
    table = entry["table"]
    date_mode = driver["date_mode"]
    _pfx = _make_pfx(progress)

    # 1. 获取域列表（优先静态 values，否则从驱动表查询）
    if "values" in driver:
        domain_vals = driver["values"]
    else:
        source_table = driver["source_table"]
        source_column = driver["source_column"]
        filters = driver.get("filters", {})
        query = f'SELECT DISTINCT "{source_column}" FROM "{source_table}"'
        filter_params = []
        if filters:
            clauses = []
            for col, values in filters.items():
                placeholders = ",".join(["?"] * len(values))
                clauses.append(f'"{col}" IN ({placeholders})')
                filter_params.extend(values)
            query += " WHERE " + " AND ".join(clauses)
        query += f' ORDER BY "{source_column}"'
        try:
            domain_vals = [r[0] for r in conn.execute(query, filter_params).fetchall()]
        except Exception:
            logger.warning(f"{_pfx}{api_name}: 驱动表 {source_table} 不可用，尝试拉取")
            drv_entry = next((e for e in REGISTRY if e["table"] == source_table), None)
            if drv_entry:
                _run_once_strategy(conn, dc, drv_entry)
                domain_vals = [r[0] for r in conn.execute(query, filter_params).fetchall()]
            else:
                domain_vals = []
                logger.warning(f"{_pfx}{api_name}: 驱动表 {source_table} 不在 REGISTRY，无法拉取")
            if not domain_vals:
                logger.error(f"{_pfx}{api_name}: 驱动表 {source_table} 无数据，跳过")
                return

    # 2. 获取日期周期
    if until is None:
        until = _resolve_until()
    periods = _get_date_periods(conn, date_mode, since, until)
    if not periods:
        logger.warning(f"{_pfx}{api_name}: 无可用周期，跳过")
        return

    # 3. 找域参数名
    param_name = _find_domain_param(entry)
    if not param_name:
        logger.error(f"{_pfx}{api_name}: 无法确定域参数名")
        return

    api_func = getattr(dc, api_name)
    total = len(domain_vals) * len(periods)
    filled = 0
    skipped = 0
    # 最新周期（当前月/年）不因 ok=2 跳过，允许每日重试等数据就绪
    latest_period_key = periods[0][0] if periods else None

    for dv in domain_vals:
        for period in periods:
            period_key = period[0]  # YYYYMM / YYYY / YYYYMMDD

            if date_mode == "once":
                date_val = f"{dv}__once__"
            else:
                date_val = f"{dv}_{period_key}"

            # 点修复模式：跳过不匹配的 date_val
            if filter_date_val is not None and date_val != filter_date_val:
                continue

            # 5. 跳过已完成
            if date_mode == "once" or period_key != latest_period_key:
                # once 模式 ok=2 也跳过；非最新周期 ok=2 也跳过
                done = conn.execute(
                    "SELECT COUNT(*) FROM pull_log WHERE table_name=? AND date_val=? AND ok IN (1,2)",
                    (table, date_val)
                ).fetchone()[0]
            else:
                # 最新周期仅跳过 ok=1，ok=2 每日重试
                done = conn.execute(
                    "SELECT COUNT(*) FROM pull_log WHERE table_name=? AND date_val=? AND ok=1",
                    (table, date_val)
                ).fetchone()[0]
            if done:
                skipped += 1
                continue

            # 6. 组装参数并拉取
            kwargs = {param_name: dv}
            if date_mode == "monthly":
                kwargs["start_date"] = period[1]
                kwargs["end_date"] = period[2]
            elif date_mode == "yearly":
                kwargs["start_date"] = period[1]
                kwargs["end_date"] = period[2]
            elif date_mode == "daily":
                kwargs["trade_date"] = period[0]
            # once: 不传日期参数

            logger.info(f"{_pfx}{api_name}({param_name}={dv}, period={period_key}) → {table}")

            try:
                df = api_func(**kwargs)
            except DailyLimitError as e:
                logger.error(f"{_pfx}{api_name} 天级限流: {e}")
                logger.info(f"{_pfx}{api_name}: {filled} 次拉取, {skipped} 跳过, "
                            f"共 {len(domain_vals)} 域 × {len(periods)} 周期（限流中断）")
                return
            except TushareError as e:
                logger.error(f"{_pfx}{api_name} Tushare错误: {e}")
                log_pull(conn, table, date_val, 0, api=api_name, strategy="domain")
                continue
            except Exception as e:
                logger.error(f"{_pfx}{api_name} 调用异常: {e}")
                log_pull(conn, table, date_val, 0, api=api_name, strategy="domain")
                continue

            _pull_and_store(conn, table, df, date_val, api_name, "domain", _pfx)
            filled += 1

    logger.info(f"{_pfx}{api_name}: {filled} 次拉取, {skipped} 跳过, "
                f"共 {len(domain_vals)} 域 × {len(periods)} 周期")


def _run_backfill(conn, dc, target_api: str | None = None, since: str | None = None, until: str | None = None):
    """对 REGISTRY 执行三种策略拉取（最新→最早）.

    Args:
        target_api: 指定单个 API 名，None 则全量
        since: 历史边界 YYYYMMDD，None 则不设限
        until: 最新边界 YYYYMMDD，None 则根据 pull_after 自动判定（收盘后=今天，收盘前=昨天）
    """
    if until is None:
        until = _resolve_until()
    entries = REGISTRY  # REGISTRY 本身已是规则 1/2

    logger.info(f"[maintain] 开始回填，{len(entries)} 个接口，until={until}")

    if target_api:
        entries = [e for e in entries if e["api"] == target_api]
        if not entries:
            logger.error(f"API {target_api} 不在 REGISTRY (规则1/2) 中")
            return

    total = len(entries)
    for i, entry in enumerate(entries):
        strategy = _get_date_params(entry)
        p = (i + 1, total)
        logger.info(f"[maintain] [{p[0]}/{p[1]}] {entry['api']}: 策略={strategy['strategy']}")
        _dispatch_strategy(conn, dc, entry, strategy, since, until, progress=p)
        logger.info(f"[maintain] [{p[0]}/{p[1]}] {entry['api']}: 完成")


_INFRA_FALLBACK = {
    "classify_apis": "沿用既有 api_index.json 分类",
    "generate_schema": "沿用既有 schema.sql/etl.py",
}


def _run_infra_script(step_name: str, script_path: str, jlog) -> bool:
    """运行 infra 子脚本（重分类/重生成）；失败时记录日志并沿用既有产物."""
    import subprocess
    fallback = _INFRA_FALLBACK.get(step_name, "沿用既有产物")
    script_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logger.info(f"[infra] {step_name}…")
    try:
        subprocess.run([sys.executable, script_path],
                       check=True, timeout=300, cwd=script_root)
        jlog.write({"level": "INFO", "module": "infra", "event": "infra",
                    "step": step_name, "status": "ok"})
        return True
    except subprocess.TimeoutExpired:
        logger.error(f"[infra] {step_name} 超时 (timeout=300s)，{fallback}")
        jlog.write({"level": "ERROR", "module": "infra", "event": "infra",
                    "step": step_name, "status": "fail", "reason": "timeout"})
    except subprocess.CalledProcessError as e:
        logger.error(f"[infra] {step_name} 失败 (rc={e.returncode})，{fallback}")
        jlog.write({"level": "ERROR", "module": "infra", "event": "infra",
                    "step": step_name, "status": "fail", "rc": e.returncode})
    except OSError as e:
        logger.error(f"[infra] {step_name} 系统错误: {e}，{fallback}")
        jlog.write({"level": "ERROR", "module": "infra", "event": "infra",
                    "step": step_name, "status": "fail", "reason": str(e)[:200]})
    return False


def _run_infrastructure(config, conn, dc):
    """所有拉取模式前自动执行."""
    from database.logger import get_json_logger
    jlog = get_json_logger()

    _script_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # 1. 重分类
    _run_infra_script("classify_apis",
                      os.path.join(_script_root, "scripts", "classify_apis.py"), jlog)

    # 2. 重生成
    _run_infra_script("generate_schema",
                      os.path.join(_script_root, "scripts", "generate_schema.py"), jlog)

    # 2.5 重载 DC 规则（api_index.json 已被 subprocess 更新，先清缓存）
    from database.utils import load_api_registry, invalidate_registry_cache
    invalidate_registry_cache()
    dc.load_rules()

    # 重载 etl 模块（generate_schema 已重写 etl.py），重新绑定 REGISTRY
    import importlib
    importlib.reload(_etl_module)
    global REGISTRY
    REGISTRY = _etl_module.REGISTRY

    # 3. 日历补拉
    cal_count = conn.execute("SELECT COUNT(*) FROM trade_cal").fetchone()[0]
    if cal_count == 0:
        logger.info("[infra] 日历为空，拉取…")
        df = dc.trade_cal()
        if not df.empty:
            upsert_df(conn, "trade_cal", df)
    else:
        max_cal = conn.execute(
            "SELECT MAX(cal_date) FROM trade_cal WHERE is_open=1"
        ).fetchone()[0]
        if max_cal:
            max_dt = date(int(max_cal[:4]), int(max_cal[4:6]), int(max_cal[6:]))
            if (max_dt - beijing_today()).days <= 7:
                next_year = max_dt.year + 1
                logger.info(f"[infra] 日历快到底，补拉 {next_year}")
                df = dc.trade_cal(
                    start_date=f"{next_year}0101", end_date=f"{next_year}1231")
                if not df.empty:
                    upsert_df(conn, "trade_cal", df)

    # 4. 刷新基础设施 once 表
    total_stocks = 0
    for api_name in INFRA_ONCE:
        entry = next((e for e in REGISTRY if e["api"] == api_name), None)
        if not entry:
            logger.warning(f"[infra] {api_name} 不在 REGISTRY 中，跳过")
            continue
        logger.info(f"[infra] 刷新 {api_name}…")
        api_func = getattr(dc, api_name)
        kwargs = entry.get("default_params", {})
        try:
            df = api_func(**kwargs)
        except TushareError as e:
            logger.error(f"[infra] {api_name} Tushare错误: {e}")
            continue
        except Exception as e:
            logger.error(f"[infra] {api_name} 调用异常: {e}")
            continue
        if df is not None and not df.empty:
            upsert_df(conn, entry["table"], df)
            logger.info(f"[infra] {api_name}: {len(df)} 行")
            if api_name == "stock_basic":
                total_stocks = len(df)
    jlog.write({"level": "INFO", "module": "infra", "event": "infra",
                "step": "stock_basic", "stocks": total_stocks})


def _count_table(conn, table: str) -> int:
    """表行数；表不存在等异常按 0 计."""
    try:
        return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    except Exception:
        return 0


def _classify_empty_table(conn, table: str) -> tuple[str, int]:
    """空表分类：无 pull_log 记录为 untouched，否则 empty（返回 pull_log 条数）."""
    pl = conn.execute(
        'SELECT COUNT(*) FROM pull_log WHERE table_name=?', (table,)
    ).fetchone()[0]
    return ("untouched", 0) if pl == 0 else ("empty", pl)


def _verify(conn, report_issues=True, integrity_check=False) -> dict:
    """生成质检报告，只读不写.

    Args:
        integrity_check: True 时走 PRAGMA integrity_check（全扫，30-60s）；
                         False 时跳过（建库/日更默认，43GB 上需数十秒且末尾已有覆盖度报告）。
    """
    if integrity_check:
        print("integrity_check 扫描中（43GB ~30-60s）…", flush=True)
        try:
            result = conn.execute("PRAGMA integrity_check").fetchone()
            if result and result[0] != "ok":
                print(f"  ⚠ 数据库损坏: {result[0]}")
            else:
                print("  ok")
        except KeyboardInterrupt:
            print("  skipped")
        except Exception as e:
            print(f"  error: {e}")

    config = load_config()
    cal_since = config.get("backfill_since", DEFAULT_BACKFILL_SINCE)
    trading_days = {
        r[0] for r in _cal_query(conn, since=cal_since,
                                 until=beijing_today().strftime("%Y%m%d"))
    }

    perfect, small_gap, big_gap, empty, untouched = 0, 0, 0, 0, 0
    total_rows = 0
    anomalies = []

    for entry in REGISTRY:
        table = entry["table"]
        dc_col = entry.get("date_col")

        # domain 表：按 date_mode 做覆盖质检
        driver = entry.get("driver")
        if driver:
            param_name = _find_domain_param(entry)
            dm = driver["date_mode"]
            domain_date_col = entry.get("date_col") or "trade_date"
            cnt = _count_table(conn, table)
            total_rows += cnt

            if cnt == 0:
                status, pl = _classify_empty_table(conn, table)
                if status == "untouched":
                    untouched += 1
                else:
                    empty += 1
                    anomalies.append((table, "空表", f"pull_log={pl}"))
                continue

            # 按 date_mode 计算周期覆盖
            if dm == "monthly":
                expected = {r[0] for r in conn.execute(
                    "SELECT DISTINCT substr(cal_date,1,6) FROM trade_cal "
                    "WHERE is_open=1 AND cal_date >= ? AND cal_date <= ?",
                    (cal_since, beijing_today().strftime("%Y%m%d"),)
                ).fetchall()}
                actual = {r[0] for r in conn.execute(
                    f'SELECT DISTINCT substr("{domain_date_col}",1,6) FROM "{table}"'
                ).fetchall()}
            elif dm == "yearly":
                expected = {r[0] for r in conn.execute(
                    "SELECT DISTINCT substr(cal_date,1,4) FROM trade_cal "
                    "WHERE is_open=1 AND cal_date >= ? AND cal_date <= ?",
                    (cal_since, beijing_today().strftime("%Y%m%d"),)
                ).fetchall()}
                actual = {r[0] for r in conn.execute(
                    f'SELECT DISTINCT substr("{domain_date_col}",1,4) FROM "{table}"'
                ).fetchall()}
            else:  # daily
                expected = trading_days
                actual = {r[0] for r in conn.execute(
                    f'SELECT DISTINCT "{domain_date_col}" FROM "{table}"'
                ).fetchall()}

            if not param_name:
                anomalies.append((table, "无域参数", ""))
                continue

            codes = conn.execute(
                f'SELECT COUNT(DISTINCT "{param_name}") FROM "{table}"').fetchone()[0]
            coverage = len(actual) / len(expected) * 100 if expected else 0

            if coverage >= 99.9:
                perfect += 1
            elif coverage >= 95:
                small_gap += 1
                anomalies.append((table, f"{coverage:.1f}%",
                                  f"{codes}域 × {len(actual)}/{len(expected)}周期"))
            else:
                big_gap += 1
                anomalies.append((table, f"{coverage:.1f}%",
                                  f"{codes}域 × {len(actual)}/{len(expected)}周期"))

            # 逐 code 异常检测（月度以上才做，避免日频性能爆炸）
            if dm in ("monthly", "yearly"):
                for code_row in conn.execute(
                    f'SELECT DISTINCT "{param_name}" FROM "{table}"').fetchall():
                    code = code_row[0]
                    code_periods = conn.execute(
                        f'SELECT COUNT(DISTINCT substr("{domain_date_col}",1,{6 if dm=="monthly" else 4})) '
                        f'FROM "{table}" WHERE "{param_name}"=?', (code,)).fetchone()[0]
                    if len(expected) > 12 and code_periods < len(expected) * 0.3:
                        anomalies.append((table, f"{code}: {code_periods}/{len(expected)}周期",
                                          "可能退市或停更"))
            continue

        if dc_col:
            cnt = _count_table(conn, table)
            total_rows += cnt
            if cnt == 0:
                status, pl = _classify_empty_table(conn, table)
                if status == "untouched":
                    untouched += 1
                else:
                    empty += 1
                    anomalies.append((table, "空表", f"pull_log={pl}"))
                continue

            try:
                actual = {r[0] for r in conn.execute(
                    f'SELECT DISTINCT "{dc_col}" FROM "{table}"'
                ).fetchall()}
            except Exception:
                actual = set()
            fill = len(actual & trading_days) / len(trading_days) * 100 if trading_days else 0
            missing = len(trading_days - actual)

            if fill >= 99.9:
                perfect += 1
            elif fill >= 95:
                small_gap += 1
                anomalies.append((table, f"{fill:.1f}%", f"缺{missing}天"))
            else:
                big_gap += 1
                anomalies.append((table, f"{fill:.1f}%", f"缺{missing}天"))
        else:
            cnt = _count_table(conn, table)
            total_rows += cnt
            if cnt == 0:
                status, pl = _classify_empty_table(conn, table)
                if status == "untouched":
                    untouched += 1
                else:
                    empty += 1
                    anomalies.append((table, "空表", f"pull_log={pl}"))

    pl_ok = conn.execute("SELECT COUNT(*) FROM pull_log WHERE ok=1").fetchone()[0]
    pl_empty = conn.execute("SELECT COUNT(*) FROM pull_log WHERE ok=2").fetchone()[0]
    pl_fail = conn.execute("SELECT COUNT(*) FROM pull_log WHERE ok=0").fetchone()[0]

    design_issues = []
    if pl_fail > 0:
        design_issues.append(f"pull_log ok=0: {pl_fail} 条需重试")
    for t, reason, detail in anomalies:
        if "缺" in detail:
            miss = int(detail.split("缺")[1].replace("天", ""))
            if miss > 100:
                design_issues.append(f"{t}:{reason} ({detail}) — 可能Tushare断供")

    from database.utils import PROJECT_ROOT
    try:
        size_gb = os.path.getsize(os.path.join(PROJECT_ROOT, "data", "market.db")) / 1024**3
    except Exception:
        size_gb = 0

    result = {
        "tables": len(REGISTRY), "size_gb": size_gb, "total_rows": total_rows,
        "perfect": perfect, "small_gap": small_gap, "big_gap": big_gap,
        "empty": empty, "untouched": untouched,
        "anomalies": anomalies,
        "pl_ok": pl_ok, "pl_empty": pl_empty, "pl_fail": pl_fail,
        "design_issues": design_issues,
    }

    if report_issues:
        _print_report(result)

    from database.logger import get_json_logger
    get_json_logger().write({
        "level": "INFO", "module": "maintain", "event": "summary",
        "tables": result["tables"], "total_rows": result["total_rows"],
        "perfect": result["perfect"], "small_gap": result["small_gap"],
        "big_gap": result["big_gap"], "empty": result["empty"],
        "untouched": result["untouched"],
        "ok_1": result["pl_ok"], "ok_2": result["pl_empty"],
        "ok_0": result["pl_fail"],
        "design_issues": len(result["design_issues"]),
    })

    return result


def _print_report(r: dict):
    print(f"\n{'='*40}")
    print(f"  质检报告  {beijing_today()}")
    print(f"{'='*40}")
    print(f"\nREGISTRY: {r['tables']} 张表, {r['size_gb']:.1f}GB, {r['total_rows']:,} 行\n")
    print(f"┌─ 完整 (≥99.9%): {r['perfect']} 张")
    print(f"├─ 小缺口 (≥95%): {r['small_gap']} 张")
    print(f"├─ 大缺口: {r['big_gap']} 张")
    print(f"├─ 空表: {r['empty']} 张")
    print(f"├─ 未触及: {r['untouched']} 张")

    if r["anomalies"]:
        print(f"\n异常明细:")
        for t, reason, detail in r["anomalies"]:
            print(f"  {t:30s} {reason:10s} {detail}")

    print(f"\npull_log: ok=1:{r['pl_ok']} / ok=2:{r['pl_empty']} / ok=0:{r['pl_fail']}")

    if r["design_issues"]:
        print(f"\n设计外事件: {len(r['design_issues'])} 个")
        for issue in r["design_issues"]:
            print(f"  ⚠ {issue}")


def _cmd_verify(conn, run_id, t_start):
    """--verify: 只读质检（全量 integrity_check）."""
    _verify(conn, integrity_check=True)
    _write_run_end(conn, run_id, t_start)


def _cmd_cleanup(conn, dc, args, run_id, t_start):
    """--cleanup: 删除不在 REGISTRY 的孤儿表及 pull_log 残留."""
    keep = {e["table"] for e in REGISTRY} | INFRA_TABLES
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    orphan = tables - keep
    if orphan:
        print(f"删除 {len(orphan)} 张不在 REGISTRY 的表:")
        for t in sorted(orphan):
            print(f"  DROP {t}")
            conn.execute(f'DROP TABLE IF EXISTS "{t}"')
            conn.execute("DELETE FROM pull_log WHERE table_name=?", (t,))

    stale_pl = conn.execute(
        "SELECT DISTINCT table_name FROM pull_log"
    ).fetchall()
    stale_count = 0
    for (t,) in stale_pl:
        if t not in keep:
            conn.execute("DELETE FROM pull_log WHERE table_name=?", (t,))
            stale_count += 1

    if orphan or stale_count:
        conn.commit()
        if orphan and args.vacuum:
            conn.execute("VACUUM")
        elif orphan:
            print(f"提示: {len(orphan)} 张表已删除，加 --vacuum 回收磁盘空间")
        print(f"cleanup 完成（{len(orphan)} 表 + {stale_count} pull_log 残留）")
    else:
        print("无需清理")

    if args.hard:
        dc.clear_cache()
        print("缓存已清理")
    _write_run_end(conn, run_id, t_start)


def _cmd_daily(conn, dc, args, config, run_id, t_start):
    """--daily: 逐表补缺口 + ok=2 超期重试 + ok=0 自动修复 + 质检."""
    target_str = args.until or _resolve_until()
    logger.info(f"[daily] 目标日期: {target_str}")

    dated_entries = [e for e in REGISTRY if e.get("date_col") or e.get("driver")]
    once_entries = [e for e in REGISTRY if not e.get("date_col") and not e.get("driver")
                    and e["api"] not in INFRA_ONCE]
    total = len(dated_entries) + len(once_entries)
    idx = 0

    for entry in dated_entries:
        idx += 1
        p = (idx, total)
        table = entry["table"]
        dc_col = entry.get("date_col")
        strategy = _get_date_params(entry)

        if entry.get("driver") and strategy.get("date_mode") == "once":
            # once 域表（如 dividend 逐股全量）：MAX(date_col) 无法反映新域值，
            # 直接全量 dispatch，未拉过的域值由 pull_log done 检查自动补齐
            logger.info(f"[daily] [{p[0]}/{p[1]}] {table}: 逐域刷新")
            _dispatch_strategy(conn, dc, entry, strategy, None, target_str, progress=p)
            logger.info(f"[daily] [{p[0]}/{p[1]}] {table}: 完成")
            continue

        last = conn.execute(f'SELECT MAX("{dc_col}") FROM "{table}"').fetchone()[0]
        if last is None:
            since = args.since or config.get("backfill_since", DEFAULT_BACKFILL_SINCE)
        elif last >= target_str:
            logger.info(f"[daily] [{p[0]}/{p[1]}] {table}: 已是最新 ({last})")
            continue
        else:
            since = last

        logger.info(f"[daily] [{p[0]}/{p[1]}] {table}: {since} → {target_str}")
        if strategy["strategy"] == "date_range":
            current_year = target_str[:4]
            conn.execute(
                "DELETE FROM pull_log WHERE table_name=? AND date_val=?",
                (table, current_year),
            )
            conn.commit()
        ds = None if strategy["strategy"] == "domain" else since
        _dispatch_strategy(conn, dc, entry, strategy, ds, target_str, progress=p)
        logger.info(f"[daily] [{p[0]}/{p[1]}] {table}: 完成")

    for entry in once_entries:
        idx += 1
        p = (idx, total)
        logger.info(f"[daily] [{p[0]}/{p[1]}] {entry['table']}: 刷新...")
        conn.execute(
            "DELETE FROM pull_log WHERE table_name=? AND date_val='__once__'",
            (entry["table"],),
        )
        conn.commit()
        _run_once_strategy(conn, dc, entry, progress=p)
        logger.info(f"[daily] [{p[0]}/{p[1]}] {entry['table']}: 完成")

    _verify(conn)

    # ok=2 超期重试
    OK2_RETRY_DAYS = 7
    ok2_cutoff = (beijing_now() - timedelta(days=OK2_RETRY_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
    ok2_retry = conn.execute(
        "SELECT table_name, date_val FROM pull_log WHERE ok=2 AND last_try < ?",
        (ok2_cutoff,)
    ).fetchall()
    if ok2_retry:
        logger.info(f"[daily] 发现 {len(ok2_retry)} 条超期 ok=2 记录，删除并复验…")
        by_table: dict[str, list[str]] = {}
        for (table, date_val) in ok2_retry:
            by_table.setdefault(table, []).append(date_val)
        conn.execute(
            "DELETE FROM pull_log WHERE ok=2 AND last_try < ?", (ok2_cutoff,))
        conn.commit()
        for table, dates in by_table.items():
            entry = next((e for e in REGISTRY if e["table"] == table), None)
            if not entry:
                logger.warning(f"[daily] {table} 不在 REGISTRY，跳过复验")
                continue
            strategy = _get_date_params(entry)
            if strategy["strategy"] != "trade_date":
                logger.info(
                    f"[daily] {table}: {len(dates)} 条空日非日频策略，交由全量回补复检")
                continue
            s, u = min(dates), max(dates)
            logger.info(f"[daily] {table}: 复验 {len(dates)} 个超期空日 [{s}..{u}]")
            _dispatch_strategy(conn, dc, entry, strategy, s, u)

    # 自动修复 ok=0 记录
    MAX_RETRY_ATTEMPTS = 5
    failed = conn.execute(
        "SELECT table_name, date_val, retry_count FROM pull_log WHERE ok=0"
    ).fetchall()
    if failed:
        logger.info(f"[daily] 发现 {len(failed)} 条失败记录，自动修复…")
        for (table, date_val, retry_count) in failed:
            entry = next((e for e in REGISTRY if e["table"] == table), None)
            if not entry:
                logger.warning(f"[daily] {table} 不在 REGISTRY，跳过")
                continue
            if retry_count >= MAX_RETRY_ATTEMPTS:
                logger.warning(f"[daily] {table} {date_val}: 已达最大重试次数 {MAX_RETRY_ATTEMPTS}，标记放弃 (ok=3)")
                conn.execute(
                    "UPDATE pull_log SET ok=3 WHERE table_name=? AND date_val=?",
                    (table, date_val),
                )
                conn.commit()
                continue
            if dc._daily_cooldown_until.get(entry["api"], 0) > time.time():
                logger.info(f"[daily] {entry['api']} 冷却中，跳过修复")
                continue
            conn.execute(
                "UPDATE pull_log SET retry_count=retry_count+1, last_try=? "
                "WHERE table_name=? AND date_val=?",
                (beijing_now().isoformat(), table, date_val),
            )
            conn.commit()
            strategy = _get_date_params(entry)
            s, u, fdv = _auto_fix_bounds(strategy, date_val)
            _dispatch_strategy(conn, dc, entry, strategy, s, u, filter_date_val=fdv)
        _verify(conn)
    _write_run_end(conn, run_id, t_start)


def _cmd_refresh(conn, dc, args, run_id, t_start):
    """--refresh: 单表单日修复."""
    api, date_val = args.refresh
    entry = next((e for e in REGISTRY if e["api"] == api), None)
    if not entry:
        logger.error(f"API {api} 不在 REGISTRY 中")
        sys.exit(1)
    table = entry["table"]
    strategy = _get_date_params(entry)
    if strategy["strategy"] == "date_range":
        log_key = date_val[:4]
    elif strategy["strategy"] == "once":
        log_key = "__once__"
    else:
        log_key = date_val
    conn.execute(
        "DELETE FROM pull_log WHERE table_name=? AND date_val=?", (table, log_key))
    conn.commit()
    logger.info(f"[refresh] {api} {date_val}: pull_log 已清 (key={log_key})")
    s, u, fdv = _auto_fix_bounds(strategy, date_val)
    _dispatch_strategy(conn, dc, entry, strategy, s, u, filter_date_val=fdv)
    logger.info(f"[refresh] {api} {date_val}: 完成")
    _write_run_end(conn, run_id, t_start)


def _cmd_dry_run(conn, args, run_id, t_start):
    """--dry-run: 仅打印策略矩阵，无副作用."""
    entries = REGISTRY
    if args.api:
        entries = [e for e in entries if e["api"] == args.api]
    total = len(entries)
    for i, entry in enumerate(entries):
        strategy = _get_date_params(entry)
        print(f"[{i+1}/{total}] {entry['api']:30s} → {entry['table']:30s} strategy={strategy['strategy']}")
    _write_run_end(None, run_id, t_start)


def _cmd_backfill(conn, dc, args, run_id, t_start):
    """全量建库."""
    since = args.since
    until = args.until or _resolve_until()
    logger.info(f"[maintain] 建库 since={since} until={until}")
    _run_backfill(conn, dc, args.api, since, until)
    _verify(conn)
    _write_run_end(conn, run_id, t_start)


def _log_run_start(jlog, run_id: str, command: str, args) -> None:
    """写入 run_start 日志条目."""
    jlog.write({
        "level": "INFO", "module": "maintain", "event": "run_start",
        "run_id": run_id, "command": command,
        "since": args.since,
        "until": args.until or _resolve_until(),
        "tables": len(REGISTRY),
    })


def main():
    parser = argparse.ArgumentParser(description="数据库维护")
    parser.add_argument("--dry-run", action="store_true", help="仅扫描，不拉取")
    parser.add_argument("--api", type=str, help="只拉取指定 API")
    parser.add_argument("--since", type=str, default=None,
                        help="历史边界 YYYYMMDD，默认读 user_config.yaml backfill_since，兜底 20200101")
    parser.add_argument("--until", type=str, default=None,
                        help="拉取截止日期 YYYYMMDD，默认根据 pull_after 自动判定")
    parser.add_argument("--daily", action="store_true",
                        help="每日更新（逐表补缺口 + 质检）")
    parser.add_argument("--verify", action="store_true",
                        help="质检报告（只读不拉）")
    parser.add_argument("--refresh", type=str, nargs=2,
                        metavar=("API", "DATE"), help="清 pull_log 并重拉单表单日")
    parser.add_argument("--cleanup", action="store_true",
                        help="删除不在 REGISTRY 的孤儿表")
    parser.add_argument("--hard", action="store_true",
                        help="配合 --cleanup，同时清理 pickle 缓存")
    parser.add_argument("--vacuum", action="store_true",
                        help="配合 --cleanup，VACUUM 回收磁盘空间（大库耗时数分钟）")
    args = parser.parse_args()

    config = load_config()
    dc = DataClient(token=config['tushare_token'])
    conn = get_conn()
    args.since = args.since or config.get("backfill_since", DEFAULT_BACKFILL_SINCE)

    from database.logger import get_json_logger
    jlog = get_json_logger()
    run_id = beijing_now().strftime("%Y%m%dT%H%M%S")
    t_start = time.time()

    if args.verify:
        command = "verify"
        _log_run_start(jlog, run_id, command, args)
        _cmd_verify(conn, run_id, t_start)
    elif args.cleanup:
        command = "cleanup"
        _log_run_start(jlog, run_id, command, args)
        _cmd_cleanup(conn, dc, args, run_id, t_start)
    elif args.dry_run:
        command = "dry_run"
        _log_run_start(jlog, run_id, command, args)
        _cmd_dry_run(conn, args, run_id, t_start)
    elif args.refresh:
        command = "refresh"
        _log_run_start(jlog, run_id, command, args)
        _cmd_refresh(conn, dc, args, run_id, t_start)
    elif args.daily:
        command = "daily"
        _log_run_start(jlog, run_id, command, args)
        _run_infrastructure(config, conn, dc)
        _cmd_daily(conn, dc, args, config, run_id, t_start)
    else:
        command = "backfill"
        _log_run_start(jlog, run_id, command, args)
        _run_infrastructure(config, conn, dc)
        _cmd_backfill(conn, dc, args, run_id, t_start)

if __name__ == "__main__":
    main()
