"""DataClient — Tushare API 传输 + pickle 缓存层.

__getattr__ 动态路由：dc.daily(...) → dc.post_request("daily", ...)
"""
from __future__ import annotations

import hashlib
import json
import logging
import pickle
import threading
import time
from functools import partial
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)


class TushareError(Exception):
    """Tushare 业务错误（频率超限、权限不足等）."""
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg
        super().__init__(f"Tushare [{code}]: {msg}")


class DailyLimitError(TushareError):
    """Tushare 天级频率超限 (40203). 触发后 24h 内该 API 自动静默跳过."""
    pass


class DataClient:
    """Tushare 数据客户端 + pickle 缓存.

    线程安全：_last_call / _daily_cooldown_until 读-改-写由 _rate_lock 保护，
    缓存文件读写由 _cache_lock 保护，共享 requests.Session。
    """

    def __init__(
        self,
        token: str,
        cache_path: str = "cache/tushare",
        url: str = "https://api.tushare.pro",
        timeout: int = 30,
    ):
        self._token = token
        self._url = url
        self._timeout = timeout
        self.cache_path = Path(cache_path)
        self.cache_path.mkdir(parents=True, exist_ok=True)
        self._cache_lock = threading.Lock()
        self._session = requests.Session()
        self._rate_lock = threading.Lock()  # 保护 _last_call / _daily_cooldown_until 读-改-写
        self.load_rules()
        self._last_call: dict[str, float] = {}
        self._last_call_global: float = 0.0  # 全局 token 级节流（Tushare 限流按 token）
        self._daily_cooldown_until: dict[str, float] = {}  # API → 冷却结束时间戳
        self._cooldown_file = self.cache_path / "_cooldown.json"
        self._load_cooldown()  # 从磁盘恢复冷却状态（跨进程持久化）
        self._setup_proxy()

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return partial(self.post_request, name)

    def _setup_proxy(self) -> None:
        """从 user_config 读取代理设置."""
        from database.utils import load_config
        config = load_config()
        proxy = config.get("tushare_proxy", "__use_system__")
        if proxy is None:
            self._proxies = {"http": None, "https": None}  # 裸连
        elif proxy == "__use_system__":
            self._proxies = None  # 走系统代理
        else:
            self._proxies = {"http": proxy, "https": proxy}

    def _load_cooldown(self) -> None:
        """从磁盘加载天级限流冷却状态（跨进程持久化）."""
        try:
            with open(self._cooldown_file) as f:
                data = json.load(f)
            now = time.time()
            self._daily_cooldown_until = {
                k: v for k, v in data.items() if v > now
            }
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            self._daily_cooldown_until = {}

    def _save_cooldown(self) -> None:
        """持久化冷却状态到磁盘（原子写）."""
        try:
            from database.utils import atomic_write_text
            atomic_write_text(self._cooldown_file, json.dumps(self._daily_cooldown_until))
        except OSError as e:
            logger.warning(f"冷却状态持久化失败: {e}")

    def load_rules(self) -> None:
        """从 api_index.json 的 _project.classification 构建 _rule_config 映射."""
        from database.utils import load_api_registry
        api_list = load_api_registry()

        from database.utils import load_config
        config = load_config()
        rate_limit = config.get("tushare_rate_limit", 200)
        floor_sec = 60.0 / rate_limit  # 450/min → 133ms
        self._global_interval = floor_sec  # 全局 token 级最小间隔
        floor_r2_sec = config.get("tushare_interval_r2_ms", 2000) / 1000

        self._known_api_names = {api["api_name"] for api in api_list}
        self._rule_config = {}
        for api in api_list:
            # 读取 _project.classification 替代原 api["rule"]/api["usable"]
            clf = api.get("_project", {}).get("classification", {})
            if not clf or not clf.get("usable"):
                continue

            rule = clf["rule"]
            if rule not in (1, 2):
                continue

            max_rows = api.get("max_rows")  # 顶层 Tushare 字段，不变

            # 间隔：优先用 classification 的计算值，但不低于硬地板
            rec_ms = clf.get("recommended_interval_ms")
            if rule == 2:
                interval = floor_r2_sec if rec_ms is None else max(floor_r2_sec, rec_ms / 1000)
            else:
                interval = floor_sec if rec_ms is None else max(floor_sec, rec_ms / 1000)

            # split_by 自动检测（input_params 在顶层不变）
            split_by = None
            if rule == 1 and max_rows is not None:
                param_names = [p["name"] for p in api.get("input_params", [])]
                if "exchange" in param_names:
                    split_by = "exchange"
                else:
                    split_by = "offset"

            self._rule_config[api["api_name"]] = {
                "rule": rule,
                "max_rows": max_rows,
                "interval": interval,
                "split_by": split_by,
                "max_retries": clf.get("max_retries", 3),
            }

    def _request_single(
        self, api_name: str, kwargs: dict, force_refresh: bool = False
    ) -> pd.DataFrame:
        """单次 Tushare 请求 + pickle 缓存。"""
        t_start = time.time()
        fields = kwargs.pop("fields", "")
        req_params = {
            "api_name": api_name,
            "token": self._token,
            "params": kwargs,
            "fields": fields,
        }

        api_cache_dir = self.cache_path / api_name
        api_cache_dir.mkdir(parents=True, exist_ok=True)
        cache_key = self._make_key(req_params)
        cache_file = api_cache_dir / f"{cache_key}.pkl"

        if not force_refresh and cache_file.exists():
            # 缓存 7 天过期，防止 Tushare 修正数据后仍取旧货
            age_days = (time.time() - cache_file.stat().st_mtime) / 86400
            if age_days < 7:
                df = self._read_cache(cache_file)
                if df is not None:
                    self._jlog_pull(api_name, len(df),
                                   round((time.time() - t_start) * 1000, 1),
                                   cache_hit=True, attempt=1)
                    return df

        # 天级限流冷却检查 + HTTP 请求级限速（读-改-写，需锁保护）
        with self._rate_lock:
            cooldown_until = self._daily_cooldown_until.get(api_name)
            if cooldown_until and time.time() < cooldown_until:
                remaining_h = (cooldown_until - time.time()) / 3600
                raise DailyLimitError(
                    code=40203,
                    msg=f"天级限流冷却中（剩余 {remaining_h:.1f}h）"
                )

            cfg = self._rule_config.get(api_name, {})
            interval = cfg.get("interval", 0.5)
            max_retries = cfg.get("max_retries", 3)
            # 全局 token 级节流（Tushare 限流按 token，非按 API）
            g_elapsed = time.time() - self._last_call_global
            if g_elapsed < self._global_interval:
                time.sleep(self._global_interval - g_elapsed)
            # per-API 间隔（rule 2 等低频 API 需更长间隔）
            elapsed = time.time() - self._last_call.get(api_name, 0)
            if elapsed < interval:
                time.sleep(interval - elapsed)
            now = time.time()
            self._last_call[api_name] = now
            self._last_call_global = now

        df, attempt = self._fetch_with_retry(req_params, api_name, max_retries)
        elapsed_ms = round((time.time() - t_start) * 1000, 1)
        self._jlog_pull(api_name, len(df), elapsed_ms,
                       cache_hit=False, attempt=attempt)
        if not df.empty:
            self._write_cache(cache_file, df)
        return df

    def _jlog_pull(self, api_name: str, rows: int, elapsed_ms: float,
                  cache_hit: bool = False, attempt: int = 1) -> None:
        """记录单次拉取到 JSON 日志."""
        from database.logger import get_json_logger
        jlog = get_json_logger()
        jlog.write({
            "level": "INFO", "module": "client", "event": "pull",
            "api": api_name, "rows": rows, "elapsed_ms": elapsed_ms,
            "cache_hit": cache_hit, "attempt": attempt,
        })

    def _paginate_offset(
        self, api_name: str, kwargs: dict, max_rows: int,
        force_refresh: bool,
    ) -> pd.DataFrame:
        """offset 递增分页拉取全部数据。"""
        all_dfs = []
        page = 0
        max_pages = 200
        first_page_fingerprint = None
        while page < max_pages:
            page_kwargs = kwargs.copy()
            page_kwargs["limit"] = max_rows
            page_kwargs["offset"] = page * max_rows
            try:
                df = self._request_single(api_name, page_kwargs, force_refresh)
            except TushareError as e:
                # 部分接口 offset 超限返回业务错误（如 pledge_detail 超 10 万行）：
                # 保留已拉数据，避免整表丢弃
                logger.warning(
                    f"[{api_name}] offset={page * max_rows} 分页中断: {e}，"
                    f"保留已拉 {sum(len(d) for d in all_dfs)} 行")
                break
            if df.empty:
                break
            if page == 0:
                first_page_fingerprint = str(df.iloc[0].to_dict())
            elif first_page_fingerprint:
                cur_fp = str(df.iloc[0].to_dict())
                if cur_fp == first_page_fingerprint:
                    logger.warning(f"[{api_name}] offset={page*max_rows} 重复首页数据，终止分页")
                    break
            all_dfs.append(df)
            if len(df) < max_rows:
                break
            page += 1

        if not all_dfs:
            return pd.DataFrame()
        return pd.concat(all_dfs, ignore_index=True)

    def _paginate_exchange(
        self, api_name: str, kwargs: dict, max_rows: int,
        force_refresh: bool,
    ) -> pd.DataFrame:
        """按交易所 SH/SZ/BJ 分批，每个交易所内部再用 offset 翻页。"""
        exchanges = ["SH", "SZ", "BJ"]
        all_dfs = []
        for exg_code in exchanges:
            ex_kwargs = kwargs.copy()
            ex_kwargs["exchange"] = exg_code
            df = self._paginate_offset(
                api_name, ex_kwargs, max_rows, force_refresh
            )
            if not df.empty:
                all_dfs.append(df)
        if not all_dfs:
            return pd.DataFrame()
        return pd.concat(all_dfs, ignore_index=True)

    def post_request(
        self, api_name: str, force_refresh: bool = False, **kwargs
    ) -> pd.DataFrame:
        if not api_name or not isinstance(api_name, str):
            raise ValueError(f"无效 api_name: {api_name}")

        cfg = self._rule_config.get(api_name)

        if cfg is None:
            # 不在规则配置中的接口 → 退化为单次请求（兼容旧行为）
            if api_name not in self._known_api_names:
                logger.warning(f"[{api_name}] 未知 API 名（可能拼写错误），仍尝试请求")
            return self._request_single(api_name, kwargs, force_refresh)

        rule = cfg["rule"]
        max_rows = cfg["max_rows"]
        split_by = cfg["split_by"]

        if rule == 1 and split_by == "exchange":
            return self._paginate_exchange(
                api_name, kwargs, max_rows, force_refresh
            )
        elif rule == 1 and split_by == "offset":
            return self._paginate_offset(
                api_name, kwargs, max_rows, force_refresh
            )
        else:
            # rule 1 (max_rows=None), rule 2, rule 3
            return self._request_single(api_name, kwargs, force_refresh)

    def _make_key(self, req_params: dict) -> str:
        params_no_token = {k: v for k, v in req_params.items() if k != "token"}
        return hashlib.sha256(json.dumps(params_no_token, sort_keys=True).encode()).hexdigest().upper()[:16]

    def _read_cache(self, file: Path) -> pd.DataFrame | None:
        with self._cache_lock:
            try:
                return pd.read_pickle(file)
            except Exception as e:
                logger.warning(f"读取缓存失败: {file}, {e}")
                return None

    def _write_cache(self, file: Path, df: pd.DataFrame) -> None:
        with self._cache_lock:
            try:
                df.to_pickle(file)
            except Exception as e:
                logger.warning(f"写入缓存失败: {file}, {e}")

    def _fetch_with_retry(self, req_params, api_name, max_retries: int = 3) -> tuple:
        for attempt in range(max_retries):
            try:
                res = self._session.post(self._url, json=req_params, timeout=self._timeout,
                                         proxies=self._proxies)
            except requests.RequestException as e:
                logger.warning(f"[{api_name}] 请求异常, 重试 {attempt+1}/{max_retries}: {e}")
                self._log_error(api_name, "RequestException", error_msg=str(e))
                if attempt < max_retries - 1:
                    time.sleep(5)
                continue

            if res.status_code != 200:
                logger.warning(f"[{api_name}] HTTP {res.status_code}, 重试 {attempt+1}/{max_retries}")
                self._log_error(api_name, "HTTPError", http_status=res.status_code,
                                error_msg=f"HTTP {res.status_code}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                continue

            try:
                result = res.json()
            except Exception as e:
                logger.warning(f"[{api_name}] JSON 解析失败, 重试 {attempt+1}/{max_retries}: {e}")
                self._log_error(api_name, "JSONDecodeError", error_msg=str(e))
                if attempt < max_retries - 1:
                    time.sleep(5)
                continue

            if not isinstance(result, dict) or "code" not in result:
                logger.warning(f"[{api_name}] 返回结构异常, 重试 {attempt+1}/{max_retries}")
                self._log_error(api_name, "BadResponse", error_msg=str(result))
                if attempt < max_retries - 1:
                    time.sleep(5)
                continue

            if result["code"] != 0:
                error_code = result.get("code")
                error_msg = result.get("msg", "")
                if error_code == 40203:
                    cooldown_sec = 24 * 3600
                    self._daily_cooldown_until[api_name] = time.time() + cooldown_sec
                    self._save_cooldown()
                    logger.warning(
                        f"[{api_name}] 触发天级限流 (40203)，冷却 24h "
                        f"至 {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() + cooldown_sec))}"
                    )
                    e = DailyLimitError(code=error_code, msg=error_msg)
                else:
                    e = TushareError(code=error_code, msg=error_msg)
                self._log_error(api_name, "TushareError",
                                error_code=e.code, error_msg=e.msg)
                raise e

            data = result.get("data", {})
            if not isinstance(data, dict) or "items" not in data or "fields" not in data:
                logger.warning(f"[{api_name}] data 结构异常, 重试 {attempt+1}/{max_retries}")
                self._log_error(api_name, "BadData", error_msg=str(data))
                if attempt < max_retries - 1:
                    time.sleep(5)
                continue

            try:
                return pd.DataFrame(data["items"], columns=data["fields"]), attempt + 1
            except Exception as e:
                logger.warning(f"[{api_name}] DataFrame 构建失败, 重试 {attempt+1}/{max_retries}: {e}")
                self._log_error(api_name, "DataFrameError", error_msg=str(e))
                if attempt < max_retries - 1:
                    time.sleep(5)
                continue

        raise TushareError(
            code=-1,
            msg=f"{max_retries}次重试全部失败"
        )

    def _log_error(self, api_name: str, error_type: str,
                   error_code=None, error_msg: str = "",
                   http_status=None) -> None:
        """记录异常到 JSON 日志."""
        from database.logger import get_json_logger
        jlog = get_json_logger()
        entry = {
            "level": "ERROR", "module": "client", "event": "error",
            "api": api_name, "error_type": error_type, "error_msg": error_msg,
        }
        if error_code is not None:
            entry["error_code"] = error_code
        if http_status is not None:
            entry["http_status"] = http_status
        jlog.write(entry)

    def clear_cache(self) -> None:
        import shutil
        cooldown_data = None
        if self._cooldown_file.exists():
            cooldown_data = self._cooldown_file.read_bytes()
        with self._cache_lock:
            if self.cache_path.exists():
                shutil.rmtree(self.cache_path)
            self.cache_path.mkdir(parents=True, exist_ok=True)
        if cooldown_data is not None:
            self._cooldown_file.write_bytes(cooldown_data)
