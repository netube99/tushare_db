"""TDD 审查 — DataClient 契约核对.

针对 AGENTS.md「DataClient — Tushare 适配壳」逐条验证：
调度分发 / 分页终止 / 双重节流 / 40203 冷却 / pickle 缓存 / 容错 / 日志字段。
所有网络交互在 _session.post 或 _request_single 层打桩。
"""

import json
import logging
import os
import time
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests

import database.logger as dlog
from database.client import DataClient, TushareError, DailyLimitError


# ── fixtures & helpers ──

@pytest.fixture
def jlog(tmp_path, monkeypatch):
    """把 JsonLogger 单例重定向到临时目录，返回目录路径."""
    monkeypatch.setattr(dlog, "_logger", dlog.JsonLogger(str(tmp_path / "logs")))
    return tmp_path / "logs"


def _read_entries(log_dir: Path) -> list[dict]:
    entries = []
    for f in sorted(Path(log_dir).glob("maintain_*.log")):
        for line in f.read_text(encoding="utf-8").splitlines():
            if line.strip():
                entries.append(json.loads(line))
    return entries


def _ok_response(rows, fields=("ts_code", "trade_date")):
    m = Mock()
    m.status_code = 200
    m.json.return_value = {"code": 0, "data": {"fields": list(fields), "items": rows}}
    return m


def _err_response(code, msg="err"):
    m = Mock()
    m.status_code = 200
    m.json.return_value = {"code": code, "msg": msg}
    return m


def _api_entry(name="fake_api", max_rows=1000, params=None, clf=None):
    return {
        "api_name": name,
        "max_rows": max_rows,
        "input_params": params if params is not None else [],
        "_project": {"classification": clf if clf is not None
                     else {"rule": 1, "usable": True, "recommended_interval_ms": 150}},
    }


def _make_client(monkeypatch, tmp_path, api_list, config=None):
    cfg = {"tushare_rate_limit": 500, "tushare_interval_r2_ms": 2000}
    if config:
        cfg.update(config)
    monkeypatch.setattr("database.utils.load_config", lambda *a, **k: cfg)
    monkeypatch.setattr("database.utils.load_api_registry", lambda *a, **k: api_list)
    return DataClient(token="test_token", cache_path=str(tmp_path / "cache"))


@pytest.fixture
def client(tmp_path):
    return DataClient(token="test_token", cache_path=str(tmp_path / "cache"))


@pytest.fixture
def fake_client(tmp_path, monkeypatch):
    """单一 fake rule-1 offset 分页接口."""
    return _make_client(monkeypatch, tmp_path, [_api_entry()])


# ── A. 调度策略分发 ──

def test_dispatch_rule1_exchange_split_pages_per_exchange(tmp_path, monkeypatch):
    api = _api_entry("fake_ex", max_rows=2,
                     params=[{"name": "exchange"}, {"name": "trade_date"}])
    dc = _make_client(monkeypatch, tmp_path, [api])
    calls = []
    monkeypatch.setattr(dc, "_request_single",
                        lambda api_name, kwargs, force_refresh=False:
                        calls.append(dict(kwargs)) or
                        pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20240102"}]))
    df = dc.post_request("fake_ex", trade_date="20240102")
    assert len(df) == 3
    assert [c["exchange"] for c in calls] == ["SH", "SZ", "BJ"]
    assert all(c["limit"] == 2 and c["offset"] == 0 for c in calls)


def test_dispatch_rule1_offset_split_stops_on_short_page(tmp_path, monkeypatch):
    api = _api_entry("fake_off", max_rows=2, params=[{"name": "trade_date"}])
    dc = _make_client(monkeypatch, tmp_path, [api])
    calls = []

    def fake_rs(api_name, kwargs, force_refresh=False):
        calls.append((kwargs["limit"], kwargs["offset"]))
        rows = 2 if kwargs["offset"] < 4 else 1
        base = kwargs["offset"]
        return pd.DataFrame([{"ts_code": f"{base + i}", "trade_date": "20240102"}
                             for i in range(rows)])

    monkeypatch.setattr(dc, "_request_single", fake_rs)
    df = dc.post_request("fake_off", trade_date="20240102")
    assert calls == [(2, 0), (2, 2), (2, 4)]
    assert len(df) == 5


def test_dispatch_rule1_max_rows_none_single_request(tmp_path, monkeypatch):
    api = _api_entry("fake_once", max_rows=None, params=[{"name": "trade_date"}])
    dc = _make_client(monkeypatch, tmp_path, [api])
    calls = []
    monkeypatch.setattr(dc, "_request_single",
                        lambda api_name, kwargs, force_refresh=False:
                        calls.append(dict(kwargs)) or pd.DataFrame())
    dc.post_request("fake_once", trade_date="20240102")
    assert len(calls) == 1
    assert "limit" not in calls[0] and "offset" not in calls[0]


def test_dispatch_rule2_single_request(tmp_path, monkeypatch):
    api = _api_entry("fake_low", max_rows=1000,
                     clf={"rule": 2, "usable": True, "recommended_interval_ms": 2000})
    dc = _make_client(monkeypatch, tmp_path, [api])
    calls = []
    monkeypatch.setattr(dc, "_request_single",
                        lambda api_name, kwargs, force_refresh=False:
                        calls.append(dict(kwargs)) or pd.DataFrame())
    dc.post_request("fake_low", trade_date="20240102")
    assert len(calls) == 1
    assert "limit" not in calls[0] and "offset" not in calls[0]


def test_dispatch_unknown_api_downgrades_with_warning(client, caplog):
    with patch.object(client._session, "post", return_value=_ok_response([])):
        with caplog.at_level(logging.WARNING, logger="database.client"):
            df = client.post_request("zzz_totally_unknown_api")
    assert df.empty
    assert "zzz_totally_unknown_api" in caplog.text


def test_offset_pagination_detects_duplicate_first_page(fake_client, monkeypatch):
    rows = [{"ts_code": "000001.SZ", "trade_date": "20240102"},
            {"ts_code": "000002.SZ", "trade_date": "20240102"}]
    calls = []
    monkeypatch.setattr(fake_client, "_request_single",
                        lambda api_name, kwargs, force_refresh=False:
                        calls.append(kwargs["offset"]) or pd.DataFrame(rows))
    df = fake_client._paginate_offset("fake_api", {}, 2, False)
    assert calls == [0, 2]
    assert len(df) == 2


# ── B. 分页错误语义（契约：瞬时故障与正常空返回严格区分）──

def test_paginate_offset_first_page_error_reraises(fake_client, monkeypatch):
    monkeypatch.setattr(fake_client, "_request_single",
                        lambda api_name, kwargs, force_refresh=False:
                        (_ for _ in ()).throw(TushareError(50101, "超限")))
    with pytest.raises(TushareError):
        fake_client._paginate_offset("fake_api", {}, 1000, False)


def test_paginate_offset_mid_page_error_keeps_partial(fake_client, monkeypatch):
    def fake_rs(api_name, kwargs, force_refresh=False):
        if kwargs["offset"] == 0:
            return pd.DataFrame([{"ts_code": f"{i}", "trade_date": "20240102"}
                                 for i in range(1000)])
        raise TushareError(50101, "单次请求行数超限")

    monkeypatch.setattr(fake_client, "_request_single", fake_rs)
    df = fake_client._paginate_offset("fake_api", {}, 1000, False)
    assert len(df) == 1000


def test_paginate_offset_mid_page_daily_limit_propagates(fake_client, monkeypatch):
    def fake_rs(api_name, kwargs, force_refresh=False):
        if kwargs["offset"] == 0:
            return pd.DataFrame([{"ts_code": f"{i}", "trade_date": "20240102"}
                                 for i in range(1000)])
        raise DailyLimitError(40203, "天级限流")

    monkeypatch.setattr(fake_client, "_request_single", fake_rs)
    with pytest.raises(DailyLimitError):
        fake_client._paginate_offset("fake_api", {}, 1000, False)


def test_post_request_first_page_business_error_raises(client, jlog):
    with patch.object(client._session, "post", return_value=_err_response(50101, "超限")):
        with pytest.raises(TushareError):
            client.post_request("fund_adj", trade_date="20240102")


# ── C. 天级限流冷却（40203）──

def test_cooldown_restored_from_disk_blocks_request(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True)
    (cache_dir / "_cooldown.json").write_text(
        json.dumps({"fund_adj": time.time() + 3600}))
    dc = DataClient(token="t", cache_path=str(cache_dir))
    with patch.object(dc._session, "post", return_value=_ok_response([])) as mock_post:
        with pytest.raises(DailyLimitError):
            dc.post_request("fund_adj", trade_date="20240102")
        mock_post.assert_not_called()


def test_cooldown_expired_on_disk_allows_request(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True)
    (cache_dir / "_cooldown.json").write_text(
        json.dumps({"fund_adj": time.time() - 10}))
    dc = DataClient(token="t", cache_path=str(cache_dir))
    with patch.object(dc._session, "post", return_value=_ok_response([["000001.SZ", "d"]])):
        df = dc.post_request("fund_adj", trade_date="20240102")
    assert len(df) == 1


def test_40203_sets_cooldown_and_persists(fake_client, monkeypatch, tmp_path):
    sleeps = []
    monkeypatch.setattr("database.client.time.sleep", lambda s: sleeps.append(s))
    with patch.object(fake_client._session, "post",
                      return_value=_err_response(40203, "天级限流")):
        with pytest.raises(DailyLimitError):
            fake_client.post_request("fake_api", trade_date="20240102")
    assert "fake_api" in fake_client._daily_cooldown_until
    assert (tmp_path / "cache" / "_cooldown.json").exists()


def test_active_cooldown_raises_without_network(fake_client):
    fake_client._daily_cooldown_until["fake_api"] = time.time() + 3600
    with patch.object(fake_client._session, "post", return_value=_ok_response([])) as mock_post:
        with pytest.raises(DailyLimitError):
            fake_client.post_request("fake_api", trade_date="20240102")
        mock_post.assert_not_called()


def test_cooldown_save_under_rate_lock(fake_client, monkeypatch, tmp_path):
    orig_save = fake_client._save_cooldown
    observed = {}

    def spy():
        observed["locked"] = fake_client._rate_lock.locked()
        orig_save()

    monkeypatch.setattr(fake_client, "_save_cooldown", spy)
    monkeypatch.setattr("database.client.time.sleep", lambda s: None)
    with patch.object(fake_client._session, "post",
                      return_value=_err_response(40203, "限流")):
        with pytest.raises(DailyLimitError):
            fake_client.post_request("fake_api", trade_date="20240102")
    assert observed.get("locked") is True


def test_load_cooldown_malformed_json_no_crash(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True)
    (cache_dir / "_cooldown.json").write_text("{not valid json")
    dc = DataClient(token="t", cache_path=str(cache_dir))
    assert dc._daily_cooldown_until == {}


def test_load_cooldown_non_dict_no_crash(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True)
    (cache_dir / "_cooldown.json").write_text("[1, 2]")
    dc = DataClient(token="t", cache_path=str(cache_dir))
    assert dc._daily_cooldown_until == {}


def test_load_cooldown_non_numeric_value_no_crash(tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True)
    (cache_dir / "_cooldown.json").write_text(json.dumps({"fund_adj": "soon"}))
    dc = DataClient(token="t", cache_path=str(cache_dir))
    assert dc._daily_cooldown_until == {}


# ── D. load_rules 健壮性 + 间隔契约 ──

def test_load_rules_usable_missing_rule_skips(tmp_path, monkeypatch):
    api = {"api_name": "broken", "max_rows": 1000, "input_params": [],
           "_project": {"classification": {"usable": True}}}
    dc = _make_client(monkeypatch, tmp_path, [api])
    assert "broken" not in dc._rule_config


def test_load_rules_param_without_name_no_crash(tmp_path, monkeypatch):
    api = {"api_name": "p1", "max_rows": 1000,
           "input_params": [{"description": "x"}],
           "_project": {"classification": {"rule": 1, "usable": True}}}
    dc = _make_client(monkeypatch, tmp_path, [api])
    assert dc._rule_config["p1"]["split_by"] == "offset"


def test_load_rules_skips_missing_classification(tmp_path, monkeypatch):
    api = {"api_name": "noclf", "max_rows": 1000, "input_params": []}
    dc = _make_client(monkeypatch, tmp_path, [api])
    assert "noclf" not in dc._rule_config
    assert "noclf" in dc._known_api_names


def test_rule2_interval_floor_2s(tmp_path, monkeypatch):
    api = {"api_name": "r2", "max_rows": 1000, "input_params": [],
           "_project": {"classification": {"rule": 2, "usable": True}}}
    dc = _make_client(monkeypatch, tmp_path, [api])
    assert dc._rule_config["r2"]["interval"] >= 2.0
    assert dc._rule_config["r2"]["split_by"] is None


def test_rule2_interval_respects_rec_ms(tmp_path, monkeypatch):
    api = {"api_name": "r2b", "max_rows": 1000, "input_params": [],
           "_project": {"classification": {"rule": 2, "usable": True,
                                           "recommended_interval_ms": 3000}}}
    dc = _make_client(monkeypatch, tmp_path, [api])
    assert dc._rule_config["r2b"]["interval"] >= 3.0


def test_rule1_interval_rec_and_floor(tmp_path, monkeypatch):
    fast = {"api_name": "r1f", "max_rows": 1000, "input_params": [],
            "_project": {"classification": {"rule": 1, "usable": True,
                                            "recommended_interval_ms": 150}}}
    slow = {"api_name": "r1s", "max_rows": 1000, "input_params": [],
            "_project": {"classification": {"rule": 1, "usable": True,
                                            "recommended_interval_ms": 50}}}
    dc = _make_client(monkeypatch, tmp_path, [fast, slow])
    assert dc._rule_config["r1f"]["interval"] == pytest.approx(0.15)
    assert dc._rule_config["r1s"]["interval"] == pytest.approx(dc._global_interval)


def test_max_retries_from_classification(tmp_path, monkeypatch):
    custom = {"api_name": "mr1", "max_rows": 1000, "input_params": [],
              "_project": {"classification": {"rule": 1, "usable": True,
                                              "max_retries": 5}}}
    default = {"api_name": "mr2", "max_rows": 1000, "input_params": [],
               "_project": {"classification": {"rule": 1, "usable": True}}}
    dc = _make_client(monkeypatch, tmp_path, [custom, default])
    assert dc._rule_config["mr1"]["max_retries"] == 5
    assert dc._rule_config["mr2"]["max_retries"] == 3


def test_unknown_api_interval_uses_global_floor(client, monkeypatch):
    sleeps = []
    monkeypatch.setattr("database.client.time.sleep", lambda s: sleeps.append(s))
    now = time.time()
    client._last_call_global = now
    client._last_call["zzz_totally_unknown_api"] = now
    with patch.object(client._session, "post", return_value=_ok_response([])):
        client.post_request("zzz_totally_unknown_api")
    assert sleeps
    assert all(s <= client._global_interval + 0.05 for s in sleeps)


# ── E. pickle 缓存 ──

def test_cache_hit_skips_network_and_logs(fake_client, monkeypatch, jlog):
    monkeypatch.setattr("database.client.time.sleep", lambda s: None)
    rows = [{"ts_code": "000001.SZ", "trade_date": "20240102"}]
    with patch.object(fake_client._session, "post", return_value=_ok_response(rows)) as mp:
        df1 = fake_client._request_single("fake_api", {"trade_date": "20240102"})
        df2 = fake_client._request_single("fake_api", {"trade_date": "20240102"})
    assert len(df1) == 1 and len(df2) == 1
    assert mp.call_count == 1
    pulls = [e for e in _read_entries(jlog) if e.get("event") == "pull"]
    assert [e["cache_hit"] for e in pulls] == [False, True]
    assert all("rows" in e and "elapsed_ms" in e and "api" in e for e in pulls)


def test_cache_ttl_expiry_repulls(fake_client, monkeypatch, tmp_path):
    monkeypatch.setattr("database.client.time.sleep", lambda s: None)
    rows = [{"ts_code": "000001.SZ", "trade_date": "20240102"}]
    with patch.object(fake_client._session, "post", return_value=_ok_response(rows)) as mp:
        fake_client._request_single("fake_api", {"trade_date": "20240102"})
        cache_file = next((tmp_path / "cache" / "fake_api").glob("*.pkl"))
        old = time.time() - 8 * 86400
        os.utime(cache_file, (old, old))
        fake_client._request_single("fake_api", {"trade_date": "20240102"})
    assert mp.call_count == 2


def test_force_refresh_bypasses_cache(fake_client, monkeypatch):
    monkeypatch.setattr("database.client.time.sleep", lambda s: None)
    rows1 = [{"ts_code": "000001.SZ", "trade_date": "20240102"}]
    rows2 = [{"ts_code": "000002.SZ", "trade_date": "20240102"}]
    with patch.object(fake_client._session, "post",
                      side_effect=[_ok_response(rows1), _ok_response(rows2)]):
        df1 = fake_client._request_single("fake_api", {"trade_date": "20240102"})
        df2 = fake_client._request_single("fake_api", {"trade_date": "20240102"},
                                          force_refresh=True)
    assert df1.iloc[0]["ts_code"] == "000001.SZ"
    assert df2.iloc[0]["ts_code"] == "000002.SZ"


def test_empty_result_not_cached(fake_client, monkeypatch, tmp_path):
    monkeypatch.setattr("database.client.time.sleep", lambda s: None)
    with patch.object(fake_client._session, "post", return_value=_ok_response([])) as mp:
        df1 = fake_client._request_single("fake_api", {"trade_date": "20240102"})
        df2 = fake_client._request_single("fake_api", {"trade_date": "20240102"})
    assert df1.empty and df2.empty
    assert mp.call_count == 2
    assert not list((tmp_path / "cache" / "fake_api").glob("*.pkl"))


# ── F. proxy / 动态路由 / 日志字段 / 重试 ──

@pytest.mark.parametrize("proxy,expected", [
    (None, {"http": None, "https": None}),
    ("__use_system__", None),
    ("http://127.0.0.1:7890", {"http": "http://127.0.0.1:7890",
                               "https": "http://127.0.0.1:7890"}),
])
def test_setup_proxy_variants(tmp_path, monkeypatch, proxy, expected):
    monkeypatch.setattr("database.utils.load_config",
                        lambda *a, **k: {"tushare_proxy": proxy})
    monkeypatch.setattr("database.utils.load_api_registry", lambda *a, **k: [])
    dc = DataClient(token="t", cache_path=str(tmp_path / "cache"))
    assert dc._proxies == expected


def test_getattr_routes_to_post_request(client, monkeypatch):
    calls = []
    monkeypatch.setattr(client, "post_request",
                        lambda api_name, **kw: calls.append((api_name, kw)))
    client.some_api(trade_date="20240102")
    assert calls == [("some_api", {"trade_date": "20240102"})]


def test_getattr_underscore_raises_attributeerror(client):
    with pytest.raises(AttributeError):
        client._private_thing


def test_post_request_invalid_api_name_raises(client):
    with pytest.raises(ValueError):
        client.post_request("")


def test_pull_log_fields(client, jlog, monkeypatch):
    monkeypatch.setattr("database.client.time.sleep", lambda s: None)
    rows = [["000001.SZ", "20240102"], ["000002.SZ", "20240102"],
            ["000003.SZ", "20240102"]]
    with patch.object(client._session, "post", return_value=_ok_response(rows)):
        client.post_request("zzz_unknown_api", trade_date="20240102")
    pulls = [e for e in _read_entries(jlog) if e.get("event") == "pull"]
    assert len(pulls) == 1
    e = pulls[0]
    assert e["api"] == "zzz_unknown_api"
    assert e["rows"] == 3
    assert "elapsed_ms" in e
    assert e["cache_hit"] is False


def test_error_log_fields(client, jlog, monkeypatch):
    monkeypatch.setattr("database.client.time.sleep", lambda s: None)
    with patch.object(client._session, "post", return_value=_err_response(-2001, "boom")):
        with pytest.raises(TushareError):
            client.post_request("zzz_unknown_api", trade_date="20240102")
    errs = [e for e in _read_entries(jlog) if e.get("event") == "error"]
    assert len(errs) == 1
    e = errs[0]
    assert e["error_type"] == "TushareError"
    assert e["error_code"] == -2001
    assert e["error_msg"] == "boom"


def test_retry_sleeps_5s_between_attempts(client, monkeypatch):
    sleeps = []
    monkeypatch.setattr("database.client.time.sleep", lambda s: sleeps.append(s))
    req = {"api_name": "zzz_unknown_api", "token": "t", "params": {}, "fields": ""}
    with patch.object(client._session, "post",
                      side_effect=[requests.ConnectionError("t1"),
                                   requests.ConnectionError("t2"),
                                   _ok_response([["000001.SZ", "20240102"]])]):
        df, attempt = client._fetch_with_retry(req, "zzz_unknown_api", max_retries=3)
    assert attempt == 3
    assert sleeps == [5.0, 5.0]


def test_business_error_not_retried(client, monkeypatch):
    monkeypatch.setattr("database.client.time.sleep", lambda s: None)
    with patch.object(client._session, "post",
                      return_value=_err_response(40101, "权限不足")) as mp:
        with pytest.raises(TushareError):
            req = {"api_name": "zzz_unknown_api", "token": "t", "params": {}, "fields": ""}
            client._fetch_with_retry(req, "zzz_unknown_api", max_retries=3)
    assert mp.call_count == 1
