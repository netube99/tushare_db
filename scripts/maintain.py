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
from datetime import datetime, timedelta
from database import get_conn, load_config, DataClient
from database.etl import REGISTRY, log_pull
import database.etl as _etl_module
from database.utils import init_schema, upsert_df, beijing_now, beijing_today
from database.client import TushareError, DailyLimitError

logger = logging.getLogger("maintain")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

# 基础设施 once 表：infra 第 4 步每次运行已刷新，daily once 循环不再重复拉取
INFRA_ONCE = ["stock_basic", "index_basic"]

# 基础设施表：非 REGISTRY 系统表 + 常驻表，cleanup 时保留
# national_team_daily / pension_float_daily：派生物理表（scripts/refresh_*_daily.py 构建）
INFRA_TABLES = {"trade_cal", "pull_log", "stock_basic", "bin_sync_log",
                "national_team_daily", "pension_float_daily"}

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


def _default_backfill_since() -> str:
    """配置中的建库起始日；配置不可用时退回常量."""
    try:
        return load_config().get("backfill_since", DEFAULT_BACKFILL_SINCE)
    except Exception:
        return DEFAULT_BACKFILL_SINCE


def _api_spec(entry: dict) -> dict | None:
    """api_index.json 中该 entry 的原始接口定义."""
    from database.utils import load_api_registry
    return next((a for a in load_api_registry()
                 if a["api_name"] == entry["api"]), None)


def _get_date_params(entry: dict) -> dict:
    """通过 input_params 判断日期参数类型，支持 param_fixes 和 domain 策略."""
    api = _api_spec(entry)
    if api is None:
        return {"strategy": "once", "date_col": None}

    fixes = _apply_param_fixes(api)
    active_params = fixes["active_params"]
    is_required = fixes["is_required"]

    driver = entry.get("driver")
    if driver:
        return {"strategy": "domain",
                "date_col": entry.get("date_col"),
                "date_mode": driver.get("date_mode"),
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


def _find_entry(table: str | None = None, api: str | None = None) -> dict | None:
    """按 table 或 api 在 REGISTRY 中查找 entry."""
    if table is not None:
        return next((e for e in REGISTRY if e.get("table") == table), None)
    return next((e for e in REGISTRY if e["api"] == api), None)


def _pull_and_store(conn, table: str, df, date_val: str,
                    api_name: str, strategy: str, log_prefix: str = "") -> bool:
    """统一拉取结果写入：upsert + pull_log。

    Returns: True=成功, False=需重试.
    """
    if df is not None and not df.empty:
        try:
            entry = _find_entry(table=table)
            pkey = entry.get("partition_key") if entry else None
            # 分区替换：先删本域旧行，再插入（无主键表避免重复堆积）；
            # DELETE 与 INSERT 在 upsert_df 内同事务，失败整体回滚
            pre_delete = None
            if pkey and pkey in df.columns:
                pre_delete = (pkey, [str(c) for c in df[pkey].unique()])
            n = upsert_df(conn, table, df,
                          drop_null_pk=not bool(entry and entry.get("null_pk_keep")),
                          replace_all=not bool(entry and entry.get("partition_key")),
                          dedupe_cols=(entry.get("dedupe_cols") if entry else None),
                          pre_delete=pre_delete)
            logger.info(f"{log_prefix}{table}: {n} rows")
            log_pull(conn, table, date_val, 1, api=api_name, rows=n, strategy=strategy)
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"[maintain] {api_name} 写入异常: {e}")
            log_pull(conn, table, date_val, 0, api=api_name, strategy=strategy)
            return False
    else:
        log_pull(conn, table, date_val, 2, api=api_name, strategy=strategy)
        return True


def _pull_once(conn, dc, entry, date_val: str, strategy: str, pfx: str,
               label: str | None = None, **kwargs) -> bool | None:
    """单次调用 + 错误归类 + 落库.

    Returns: True=已处理, False=错误已记 ok=0, None=天级限流（调用方决定 break/return）.
    """
    api_name = entry["api"]
    table = entry["table"]
    name = label or api_name
    try:
        df = getattr(dc, api_name)(**kwargs)
    except DailyLimitError as e:
        logger.error(f"{pfx}{name} 天级限流: {e}")
        return None
    except TushareError as e:
        logger.error(f"{pfx}{name} Tushare错误: {e}")
        log_pull(conn, table, date_val, 0, api=api_name, strategy=strategy)
        return False
    except Exception as e:
        logger.error(f"{pfx}{name} 调用异常: {e}")
        log_pull(conn, table, date_val, 0, api=api_name, strategy=strategy)
        return False
    return _pull_and_store(conn, table, df, date_val, api_name, strategy, pfx)


def _done_count(conn, table: str, date_val: str,
                ok_filter: str = "ok IN (1,2)") -> int:
    """pull_log 中 (table, date_val) 已完成（命中 ok_filter）的条数，>0 即跳过."""
    return conn.execute(
        f"SELECT COUNT(*) FROM pull_log WHERE table_name=? AND date_val=? AND {ok_filter}",
        (table, date_val),
    ).fetchone()[0]


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

    Returns: (since, until, filter_date_val).
    """
    match strategy["strategy"]:
        case "trade_date":
            return date_val, date_val, None
        case "date_range":
            year = date_val[:4]
            return f"{year}0101", f"{year}1231", None
        case "freq":
            td, _, _ = date_val.partition("_")
            return td, td, None
        case "domain":
            return None, _resolve_until(), date_val
        case _:
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
        start_d = datetime.strptime(since, "%Y%m%d").date()
        end_d = datetime.strptime(until, "%Y%m%d").date()
        days = [(start_d + timedelta(days=n)).strftime("%Y%m%d")
                for n in range((end_d - start_d).days + 1)]
        iter_dates = [(d,) for d in reversed(days)]
    else:
        iter_dates = _cal_query(conn, since=since, until=until)
        if not iter_dates:
            logger.warning(f"{_pfx}交易日历为空，先拉取日历")
            df = dc.trade_cal()
            if not df.empty:
                upsert_df(conn, "trade_cal", df)
            iter_dates = _cal_query(conn, since=since, until=until)

    # 查出已完成(ok=1)和确认空(ok=2)的日期，都不重拉
    done_dates = {r[0] for r in conn.execute(
        'SELECT date_val FROM pull_log WHERE table_name=? AND ok IN (1,2)', (table,)
    ).fetchall()}

    total = len(iter_dates)
    filled = 0
    for i, (td,) in enumerate(iter_dates):
        if td in done_dates:
            filled += 1
            continue
        logger.info(f"{_pfx}{api_name}({date_col}={td}) → {table} [{i+1}/{total}]")
        res = _pull_once(conn, dc, entry, td, "trade_date", _pfx, **{date_col: td})
        if res is None:
            break
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

    for year in years:
        if _done_count(conn, table, year):
            logger.info(f"{_pfx}{api_name} {year}: 已完成，跳过")
            continue
        logger.info(
            f"{_pfx}{api_name}(start_date={year}0101, end_date={year}1231) → {table}")
        res = _pull_once(conn, dc, entry, year, "date_range", _pfx,
                         label=f"{api_name} {year}",
                         start_date=f"{year}0101", end_date=f"{year}1231")
        if res is None:
            break


def _run_once_strategy(conn, dc, entry, progress: tuple[int, int] | None = None):
    """一次性拉全量."""
    api_name = entry["api"]
    table = entry["table"]
    _pfx = _make_pfx(progress)

    if _done_count(conn, table, "__once__"):
        logger.info(f"{_pfx}{api_name}: 已完成（一次性），跳过")
        return
    logger.info(f"{_pfx}{api_name} → {table}")
    _pull_once(conn, dc, entry, "__once__", "once", _pfx,
               **entry.get("default_params", {}))


def _run_freq_strategy(conn, dc, entry, since=None, until=None,
                      progress: tuple[int, int] | None = None):
    """逐交易日 × freq 值迭代拉取."""
    api_name = entry["api"]
    table = entry["table"]
    freq_values = entry.get("freq_values", DEFAULT_FREQ_VALUES)
    _pfx = _make_pfx(progress)

    trading_days = _cal_query(conn, since=since, until=until)
    total = len(trading_days) * len(freq_values)
    filled = 0

    for td_idx, (td,) in enumerate(trading_days):
        for fv_idx, fv in enumerate(freq_values):
            seq = td_idx * len(freq_values) + fv_idx + 1
            date_key = f"{td}_{fv}"
            if _done_count(conn, table, date_key):
                filled += 1
                continue
            logger.info(f"{_pfx}{api_name}(trade_date={td}, freq={fv}) → {table} [{seq}/{total}]")
            res = _pull_once(conn, dc, entry, date_key, "freq", _pfx,
                             trade_date=td, freq=fv)
            if res is None:
                return
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
    api = _api_spec(entry)
    if api is None:
        return None
    fixes = _apply_param_fixes(api)
    candidates = [p["name"] for p in api.get("input_params", [])
                  if fixes["is_required"](p["name"])
                  and p["name"] not in DATE_PAGINATION]
    return candidates[0] if candidates else None


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
        if p.get("_disabled"):
            continue
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
                if p.get("_disabled"):
                    return False
                return p.get("required", False)
        return False

    return {"active_params": active, "is_required": is_required}


def _run_domain_strategy(conn, dc, entry, since=None, until=None,
                         progress: tuple[int, int] | None = None,
                         filter_date_val: str | None = None):
    """domain 策略：逐域值 × 逐日期周期拉取.

    Args:
        since: 起始周期边界；None 不设限（once 模式/点修复用，driver 空表兜底时
               退回配置 backfill_since），daily 传配置 backfill_since
        until: 结束边界，None 则由 _resolve_until 自动判定
        filter_date_val: 非 None 时仅处理匹配的单个 date_val（点修复）
    """
    driver = entry["driver"]
    api_name = entry["api"]
    table = entry["table"]
    date_mode = driver["date_mode"]
    _pfx = _make_pfx(progress)

    if until is None:
        until = _resolve_until()

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
            logger.warning(f"{_pfx}{api_name}: 驱动表 {source_table} 不可用")
            domain_vals = []

        # 空驱动表（全新库首轮常见：驱动表在 REGISTRY 中排在本表之后）：
        # 按驱动表自身策略补拉 since~until 窗口，再重取域列表，避免静默跳过整表
        if not domain_vals:
            logger.warning(f"{_pfx}{api_name}: 驱动表 {source_table} 无数据，尝试拉取")
            drv_entry = _find_entry(table=source_table)
            if drv_entry:
                drv_since = since or _default_backfill_since()
                _dispatch_strategy(conn, dc, drv_entry, None, drv_since, until,
                                   progress=progress)
                domain_vals = [r[0] for r in conn.execute(query, filter_params).fetchall()]
            else:
                logger.warning(f"{_pfx}{api_name}: 驱动表 {source_table} 不在 REGISTRY，无法拉取")
            if not domain_vals:
                logger.error(f"{_pfx}{api_name}: 驱动表 {source_table} 仍无数据，跳过")
                return

    # 2. 获取日期周期
    periods = _get_date_periods(conn, date_mode, since, until)
    if not periods:
        logger.warning(f"{_pfx}{api_name}: 无可用周期，跳过")
        return

    # 3. 找域参数名
    param_name = _find_domain_param(entry)
    if not param_name:
        logger.error(f"{_pfx}{api_name}: 无法确定域参数名")
        return

    filled = 0
    skipped = 0
    # 最新周期（当前月/年）不因 ok=2 跳过，允许每日重试等数据就绪
    latest_period_key = periods[0][0]

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

            # 跳过已完成；once 模式 ok=2 也跳过，最新周期仅 ok=1 跳过（ok=2 每日重试）
            if date_mode == "once" or period_key != latest_period_key:
                ok_filter = "ok IN (1,2)"
            else:
                ok_filter = "ok=1"
            if _done_count(conn, table, date_val, ok_filter):
                skipped += 1
                continue

            kwargs = {param_name: dv}
            if date_mode in ("monthly", "yearly"):
                kwargs["start_date"] = period[1]
                kwargs["end_date"] = period[2]
            elif date_mode == "daily":
                kwargs["trade_date"] = period[0]
            # once: 不传日期参数

            logger.info(f"{_pfx}{api_name}({param_name}={dv}, period={period_key}) → {table}")

            res = _pull_once(conn, dc, entry, date_val, "domain", _pfx, **kwargs)
            if res is None:
                logger.info(f"{_pfx}{api_name}: {filled} 次拉取, {skipped} 跳过, "
                            f"共 {len(domain_vals)} 域 × {len(periods)} 周期（限流中断）")
                return
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


def _run_infra_script(step_name: str, script_name: str, jlog) -> bool:
    """运行 infra 子脚本（重分类/重生成）；失败时记录日志并沿用既有产物."""
    import subprocess
    fallback = _INFRA_FALLBACK.get(step_name, "沿用既有产物")
    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), script_name)
    script_root = os.path.dirname(os.path.dirname(script_path))
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

    # 1. 重分类
    _run_infra_script("classify_apis", "classify_apis.py", jlog)

    # 2. 重生成
    _run_infra_script("generate_schema", "generate_schema.py", jlog)

    # 2.5 重载 DC 规则（api_index.json 已被 subprocess 更新，先清缓存）
    from database.utils import load_api_registry, invalidate_registry_cache
    invalidate_registry_cache()
    dc.load_rules()

    # 重载 etl 模块（generate_schema 已重写 etl.py），重新绑定 REGISTRY
    import importlib
    importlib.reload(_etl_module)
    global REGISTRY
    REGISTRY = _etl_module.REGISTRY

    # schema.sql 已被重生成，当轮重新初始化表结构（幂等，新表立即可用）
    init_schema(conn)

    # 3. 日历补拉
    def pull_cal(**kw) -> None:
        df = dc.trade_cal(**kw)
        if not df.empty:
            upsert_df(conn, "trade_cal", df)

    cal_count = conn.execute("SELECT COUNT(*) FROM trade_cal").fetchone()[0]
    if cal_count == 0:
        logger.info("[infra] 日历为空，拉取…")
        pull_cal()
    else:
        max_cal = conn.execute(
            "SELECT MAX(cal_date) FROM trade_cal WHERE is_open=1"
        ).fetchone()[0]
        if max_cal:
            max_dt = datetime.strptime(max_cal, "%Y%m%d").date()
            today = beijing_today()
            if max_dt < today:
                start = (max_dt + timedelta(days=1)).strftime("%Y%m%d")
                logger.info(f"[infra] 日历落后于当前日期，补拉 {start} → {today}")
                pull_cal(start_date=start, end_date=today.strftime("%Y%m%d"))
            elif (max_dt - today).days <= 7:
                next_year = max_dt.year + 1
                logger.info(f"[infra] 日历快到底，补拉 {next_year}")
                pull_cal(start_date=f"{next_year}0101", end_date=f"{next_year}1231")

    # 4. 刷新基础设施 once 表
    total_stocks = 0
    for api_name in INFRA_ONCE:
        entry = _find_entry(api=api_name)
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


def _verify(conn, integrity_check=False) -> dict:
    """生成质检报告，只读不写.

    Args:
        integrity_check: True 时执行 CHECKPOINT 并输出 database_size；
                         False 时跳过（建库/日更默认，末尾已有覆盖度报告）。
    """
    if integrity_check:
        print("database_size 读取中…", flush=True)
        try:
            from database import engine as _engine
            if not getattr(conn, "read_only", False):
                _engine.checkpoint(conn)
                print("  checkpoint ok")
            row = _engine.database_size(conn)
            if row is not None:
                print(f"  database_size: {row[1]}")
            print("  ok")
        except KeyboardInterrupt:
            print("  skipped")
        except Exception as e:
            print(f"  error: {e}")

    config = load_config()
    cal_since = config.get("backfill_since", DEFAULT_BACKFILL_SINCE)
    today_str = beijing_today().strftime("%Y%m%d")
    trading_days = {r[0] for r in _cal_query(conn, since=cal_since, until=today_str)}

    stats = dict.fromkeys(("perfect", "small_gap", "big_gap", "empty", "untouched"), 0)
    total_rows = 0
    anomalies: list[tuple] = []

    def mark_empty(table: str) -> None:
        status, pl = _classify_empty_table(conn, table)
        if status == "untouched":
            stats["untouched"] += 1
        else:
            stats["empty"] += 1
            anomalies.append((table, "空表", f"pull_log={pl}"))

    def grade(table: str, pct: float, detail: str) -> None:
        if pct >= 99.9:
            stats["perfect"] += 1
        elif pct >= 95:
            stats["small_gap"] += 1
        else:
            stats["big_gap"] += 1
        if pct < 99.9:
            anomalies.append((table, f"{pct:.1f}%", detail))

    for entry in REGISTRY:
        table = entry["table"]
        cnt = _count_table(conn, table)
        total_rows += cnt
        if cnt == 0:
            mark_empty(table)
            continue

        # domain 表：按 date_mode 做覆盖质检
        driver = entry.get("driver")
        if driver:
            param_name = _find_domain_param(entry)
            dm = driver["date_mode"]
            dcol = entry.get("date_col") or "trade_date"
            if dm in ("monthly", "yearly"):
                n = 6 if dm == "monthly" else 4
                expected = {r[0] for r in conn.execute(
                    f"SELECT DISTINCT substr(cal_date,1,{n}) FROM trade_cal "
                    "WHERE is_open=1 AND cal_date >= ? AND cal_date <= ?",
                    (cal_since, today_str)).fetchall()}
                actual = {r[0] for r in conn.execute(
                    f'SELECT DISTINCT substr("{dcol}",1,{n}) FROM "{table}"').fetchall()}
            else:  # daily
                expected = trading_days
                actual = {r[0] for r in conn.execute(
                    f'SELECT DISTINCT "{dcol}" FROM "{table}"').fetchall()}

            if not param_name:
                anomalies.append((table, "无域参数", ""))
                continue

            codes = conn.execute(
                f'SELECT COUNT(DISTINCT "{param_name}") FROM "{table}"').fetchone()[0]
            coverage = len(actual) / len(expected) * 100 if expected else 0
            grade(table, coverage, f"{codes}域 × {len(actual)}/{len(expected)}周期")

            # 逐 code 异常检测（月度以上才做，避免日频性能爆炸）
            if dm in ("monthly", "yearly"):
                for (code,) in conn.execute(
                        f'SELECT DISTINCT "{param_name}" FROM "{table}"'):
                    code_periods = conn.execute(
                        f'SELECT COUNT(DISTINCT substr("{dcol}",1,{n})) '
                        f'FROM "{table}" WHERE "{param_name}"=?',
                        (code,)).fetchone()[0]
                    if len(expected) > 12 and code_periods < len(expected) * 0.3:
                        anomalies.append((table, f"{code}: {code_periods}/{len(expected)}周期",
                                          "可能退市或停更"))
            continue

        dc_col = entry.get("date_col")
        if not dc_col:
            continue
        try:
            actual = {r[0] for r in conn.execute(
                f'SELECT DISTINCT "{dc_col}" FROM "{table}"').fetchall()}
        except Exception:
            actual = set()
        fill = len(actual & trading_days) / len(trading_days) * 100 if trading_days else 0
        grade(table, fill, f"缺{len(trading_days - actual)}天")

    pl_counts = {r[0]: r[1] for r in
                 conn.execute("SELECT ok, COUNT(*) FROM pull_log GROUP BY ok")}
    pl_ok = pl_counts.get(1, 0)
    pl_empty = pl_counts.get(2, 0)
    pl_fail = pl_counts.get(0, 0)

    design_issues = []
    if pl_fail > 0:
        design_issues.append(f"pull_log ok=0: {pl_fail} 条需重试")
    for t, reason, detail in anomalies:
        if "缺" in detail:
            miss = int(detail.split("缺")[1].replace("天", ""))
            if miss > 100:
                design_issues.append(f"{t}:{reason} ({detail}) — 可能Tushare断供")

    from database import engine as _engine
    db_file = getattr(conn, "path", None) or _engine.DEFAULT_DB_PATH
    try:
        size_gb = os.path.getsize(db_file) / 1024**3
    except Exception:
        size_gb = 0

    result = {
        "tables": len(REGISTRY), "size_gb": size_gb, "total_rows": total_rows,
        "perfect": stats["perfect"], "small_gap": stats["small_gap"],
        "big_gap": stats["big_gap"], "empty": stats["empty"],
        "untouched": stats["untouched"],
        "anomalies": anomalies,
        "pl_ok": pl_ok, "pl_empty": pl_empty, "pl_fail": pl_fail,
        "design_issues": design_issues,
    }

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
    from database import engine as _engine
    keep = {e["table"] for e in REGISTRY} | INFRA_TABLES
    tables = set(_engine.list_tables(conn))
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
            conn.execute("CHECKPOINT")
            print("已 CHECKPOINT（DuckDB VACUUM 不回收空间，彻底压缩需 EXPORT/IMPORT 重写）")
        elif orphan:
            print(f"提示: {len(orphan)} 张表已删除，加 --vacuum 触发 CHECKPOINT 回收")
        print(f"cleanup 完成（{len(orphan)} 表 + {stale_count} pull_log 残留）")
    else:
        print("无需清理")

    if args.hard:
        dc.clear_cache()
        print("缓存已清理")
    _write_run_end(conn, run_id, t_start)


def _recheck_ok2_empty(conn, dc, where_sql: str, params: tuple, label: str) -> None:
    """删除并复验命中条件的 ok=2 空日（仅 trade_date 策略批量重拉）.

    近期空日每日复验用于隔日发布接口（如 margin 次日早晨才发布）；
    超期空日复验用于长尾兜底。非日频策略交由全量回补复检。
    未被重拉的记录（日历缺该日/限流中断等）回填原状态，避免 pull_log 状态丢失。
    """
    rows = conn.execute(
        f"SELECT table_name, date_val, last_try, retry_count FROM pull_log "
        f"WHERE ok=2 AND {where_sql}",
        params,
    ).fetchall()
    if not rows:
        return
    logger.info(f"[daily] 发现 {len(rows)} 条{label} ok=2 记录，删除并复验…")
    by_table: dict[str, list[str]] = {}
    backups: list[tuple] = []
    for (table, date_val, last_try, retry_count) in rows:
        by_table.setdefault(table, []).append(date_val)
        backups.append((table, date_val, last_try, retry_count))
    conn.execute(f"DELETE FROM pull_log WHERE ok=2 AND {where_sql}", params)
    conn.commit()
    try:
        for table, dates in by_table.items():
            entry = _find_entry(table=table)
            if not entry:
                logger.warning(f"[daily] {table} 不在 REGISTRY，跳过复验")
                continue
            strategy = _get_date_params(entry)
            if strategy["strategy"] != "trade_date":
                logger.info(
                    f"[daily] {table}: {len(dates)} 条空日非日频策略，交由全量回补复检")
                continue
            s, u = min(dates), max(dates)
            logger.info(f"[daily] {table}: 复验 {len(dates)} 个{label}空日 [{s}..{u}]")
            _dispatch_strategy(conn, dc, entry, strategy, s, u)
    finally:
        conn.executemany(
            "INSERT OR IGNORE INTO pull_log "
            "(table_name, date_val, ok, retry_count, last_try) VALUES (?, ?, 2, ?, ?)",
            [(t, d, rc, lt) for (t, d, lt, rc) in backups],
        )
        conn.commit()


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
            # 直接全量 dispatch，未拉过的域值由 pull_log done 检查自动补齐；
            # since 仅供驱动表为空时兜底补拉使用（once 周期本身与 since 无关）
            logger.info(f"[daily] [{p[0]}/{p[1]}] {table}: 逐域刷新")
            _dispatch_strategy(conn, dc, entry, strategy,
                               config.get("backfill_since", DEFAULT_BACKFILL_SINCE),
                               target_str, progress=p)
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

        if strategy["strategy"] == "domain":
            # domain 按配置建库边界补缺口；传 None 会从日历最早日全历史扫描
            ds = config.get("backfill_since", DEFAULT_BACKFILL_SINCE)
        else:
            ds = since
        logger.info(f"[daily] [{p[0]}/{p[1]}] {table}: {ds} → {target_str}")
        if strategy["strategy"] == "date_range":
            current_year = target_str[:4]
            conn.execute(
                "DELETE FROM pull_log WHERE table_name=? AND date_val=?",
                (table, current_year),
            )
            conn.commit()
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

    # ok=2 复验：近期（默认 3 天）每日重验，覆盖隔日发布接口（如 margin）首拉为空；
    # 超期（>7 天）批量重拉兜底，两者窗口之间留待下一轮扫描
    OK2_RECHECK_DAYS = 3
    OK2_RETRY_DAYS = 7
    now = beijing_now()
    recent_cutoff = (now - timedelta(days=OK2_RECHECK_DAYS)).strftime(
        "%Y-%m-%d %H:%M:%S")
    overdue_cutoff = (now - timedelta(days=OK2_RETRY_DAYS)).strftime(
        "%Y-%m-%d %H:%M:%S")
    _recheck_ok2_empty(conn, dc, "last_try >= ?", (recent_cutoff,), "近期")
    _recheck_ok2_empty(conn, dc, "last_try < ?", (overdue_cutoff,), "超期")

    # 自动修复 ok=0 记录
    MAX_RETRY_ATTEMPTS = 5
    failed = conn.execute(
        "SELECT table_name, date_val, retry_count FROM pull_log WHERE ok=0"
    ).fetchall()
    if failed:
        logger.info(f"[daily] 发现 {len(failed)} 条失败记录，自动修复…")
        for (table, date_val, retry_count) in failed:
            entry = _find_entry(table=table)
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
    entry = _find_entry(api=api)
    if not entry:
        logger.error(f"API {api} 不在 REGISTRY 中")
        sys.exit(1)
    table = entry["table"]
    strategy = _get_date_params(entry)
    if strategy["strategy"] == "date_range":
        log_keys = [date_val[:4]]
    elif strategy["strategy"] == "once":
        log_keys = ["__once__"]
    elif strategy["strategy"] == "freq":
        log_keys = [f"{date_val}_{fv}"
                    for fv in strategy.get("freq_values", DEFAULT_FREQ_VALUES)]
    else:
        log_keys = [date_val]
    for log_key in log_keys:
        conn.execute(
            "DELETE FROM pull_log WHERE table_name=? AND date_val=?", (table, log_key))
    conn.commit()
    logger.info(f"[refresh] {api} {date_val}: pull_log 已清 (keys={log_keys})")
    s, u, fdv = _auto_fix_bounds(strategy, date_val)
    _dispatch_strategy(conn, dc, entry, strategy, s, u, filter_date_val=fdv)
    logger.info(f"[refresh] {api} {date_val}: 完成")
    _write_run_end(conn, run_id, t_start)


def _cmd_dry_run(args, run_id, t_start):
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
                        help="配合 --cleanup，CHECKPOINT 回收磁盘空间（DuckDB VACUUM 不回收）")
    args = parser.parse_args()

    config = load_config()
    dc = DataClient(token=config['tushare_token'])
    args.since = args.since or config.get("backfill_since", DEFAULT_BACKFILL_SINCE)

    from database.logger import get_json_logger
    from database import engine as _engine
    jlog = get_json_logger()
    run_id = beijing_now().strftime("%Y%m%dT%H%M%S")
    t_start = time.time()

    command = ("verify" if args.verify else "cleanup" if args.cleanup
               else "dry_run" if args.dry_run else "refresh" if args.refresh
               else "daily" if args.daily else "backfill")

    if command == "dry_run":
        # 只打印策略矩阵：不连接数据库、不建 schema、不取锁
        _log_run_start(jlog, run_id, command, args)
        _cmd_dry_run(args, run_id, t_start)
        return

    with _engine.db_lock(_engine.DEFAULT_DB_PATH,
                         exclusive=(command != "verify")) as acquired:
        if not acquired:
            logger.error("[maintain] 数据库被其他进程占用（DuckDB 单写者），放弃本次运行")
            return
        conn = get_conn(read_only=(command == "verify"))
        try:
            if command == "verify":
                runner = lambda: _cmd_verify(conn, run_id, t_start)
            elif command == "cleanup":
                runner = lambda: _cmd_cleanup(conn, dc, args, run_id, t_start)
            elif command == "refresh":
                runner = lambda: _cmd_refresh(conn, dc, args, run_id, t_start)
            elif command == "daily":
                runner = lambda: _cmd_daily(conn, dc, args, config, run_id, t_start)
            else:
                runner = lambda: _cmd_backfill(conn, dc, args, run_id, t_start)

            _log_run_start(jlog, run_id, command, args)
            if command in ("daily", "backfill"):
                _run_infrastructure(config, conn, dc)
            runner()
        finally:
            conn.close()

if __name__ == "__main__":
    main()
