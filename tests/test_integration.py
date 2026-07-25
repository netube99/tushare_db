"""A5-3: mock 集成测试 — _fetch_with_retry 各分支.

直接覆盖 P0-1: 瞬时故障不应被标 ok=2.
"""

from unittest.mock import Mock, patch

import requests
import pytest

from database.client import DataClient, TushareError, DailyLimitError


@pytest.fixture
def client():
    """返回一个不连接网络的 DataClient（mock 掉 _session.post)."""
    return DataClient(token="test_token")


def _api_params(api_name="daily"):
    return {"api_name": api_name, "token": "test_token",
            "params": {}, "fields": ""}


# ── 正常响应 ──

def test_fetch_success_returns_df(client):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "code": 0,
        "data": {
            "fields": ["ts_code", "trade_date"],
            "items": [["000001.SZ", "20200102"]],
        },
    }
    with patch.object(client._session, "post", return_value=mock_response):
        df, attempt = client._fetch_with_retry(_api_params(), "daily")
        assert attempt == 1
        assert len(df) == 1
        assert list(df.columns) == ["ts_code", "trade_date"]


def test_fetch_empty_items_returns_empty_df(client):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "code": 0,
        "data": {
            "fields": ["ts_code"],
            "items": [],
        },
    }
    with patch.object(client._session, "post", return_value=mock_response):
        df, attempt = client._fetch_with_retry(_api_params(), "daily")
        assert df.empty
        assert attempt == 1


# ── 异常 → 应 raise（非返回空 df）──

def test_fetch_bad_json_raises(client):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.side_effect = ValueError("bad json")
    with patch.object(client._session, "post", return_value=mock_response):
        with pytest.raises(TushareError):
            client._fetch_with_retry(_api_params(), "daily", max_retries=1)


def test_fetch_bad_response_structure_raises(client):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"msg": "no code field"}
    with patch.object(client._session, "post", return_value=mock_response):
        with pytest.raises(TushareError):
            client._fetch_with_retry(_api_params(), "daily", max_retries=1)


def test_fetch_tushare_error_code_raises(client):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"code": -1, "msg": "invalid param"}
    with patch.object(client._session, "post", return_value=mock_response):
        with pytest.raises(TushareError) as exc_info:
            client._fetch_with_retry(_api_params(), "daily", max_retries=1)
        assert exc_info.value.code == -1


def test_fetch_40203_raises_daily_limit(client):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"code": 40203, "msg": "limit exceeded"}
    with patch.object(client._session, "post", return_value=mock_response):
        with pytest.raises(DailyLimitError) as exc_info:
            client._fetch_with_retry(_api_params(), "daily", max_retries=1)
        assert exc_info.value.code == 40203
        assert "daily" in client._daily_cooldown_until


def test_fetch_bad_data_structure_raises_after_retries(client):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "code": 0,
        "data": ["not a dict"],
    }
    with patch.object(client._session, "post", return_value=mock_response):
        with pytest.raises(TushareError):
            client._fetch_with_retry(_api_params(), "daily", max_retries=1)


def test_fetch_dataframe_construction_failure_raises(client):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "code": 0,
        "data": {
            "fields": ["ts_code"],
            "items": [["000001.SZ", "extra_not_in_fields"]],
        },
    }
    with patch.object(client._session, "post", return_value=mock_response):
        with pytest.raises(TushareError):
            client._fetch_with_retry(_api_params(), "daily", max_retries=1)


def test_fetch_http_non_200_retries_then_raises(client):
    mock_response = Mock()
    mock_response.status_code = 500
    with patch.object(client._session, "post", return_value=mock_response):
        with pytest.raises(TushareError):
            client._fetch_with_retry(_api_params(), "daily", max_retries=1)


def test_fetch_network_error_retries_then_raises(client):
    with patch.object(client._session, "post", side_effect=requests.ConnectionError("timeout")):
        with pytest.raises(TushareError):
            client._fetch_with_retry(_api_params(), "daily", max_retries=1)


# ── 重试成功后返回 ──

def test_fetch_retry_then_success(client):
    bad = Mock()
    bad.status_code = 500
    good = Mock()
    good.status_code = 200
    good.json.return_value = {
        "code": 0,
        "data": {"fields": ["ts_code"], "items": [["000001.SZ"]]},
    }
    with patch.object(client._session, "post", side_effect=[bad, good]):
        df, attempt = client._fetch_with_retry(_api_params(), "daily", max_retries=3)
        assert attempt == 2
        assert len(df) == 1
