"""A5-1: 纯函数单测 — infer_pk, _quote_name, date_to_cal_index, _make_key."""

import hashlib
import json

import pytest

from scripts.generate_schema import infer_pk, _quote_name
from qlib_export.calendar import date_to_cal_index


# ── infer_pk ──

def _api_with_outputs(*output_names: str) -> dict:
    return {"output_params": [{"name": n} for n in output_names]}


def test_infer_pk_ts_code_trade_date():
    api = _api_with_outputs("ts_code", "trade_date", "open", "close")
    assert infer_pk(api) == "(ts_code, trade_date)"


def test_infer_pk_index_code_trade_date():
    api = _api_with_outputs("index_code", "trade_date", "pct_chg")
    assert infer_pk(api) == "(index_code, trade_date)"


def test_infer_pk_ts_code_ann_date():
    api = _api_with_outputs("ts_code", "ann_date", "holder_name")
    assert infer_pk(api) == "(ts_code, ann_date)"


def test_infer_pk_trade_date_only():
    api = _api_with_outputs("trade_date", "value")
    assert infer_pk(api) == "(trade_date)"


def test_infer_pk_exchange_id_trade_date():
    api = _api_with_outputs("exchange_id", "trade_date", "rzye")
    assert infer_pk(api) == "(exchange_id, trade_date)"


def test_infer_pk_ts_code_only():
    api = _api_with_outputs("ts_code", "name")
    assert infer_pk(api) == "(ts_code)"


def test_infer_pk_index_code_only():
    api = _api_with_outputs("index_code", "name")
    assert infer_pk(api) == "(index_code)"


def test_infer_pk_con_code_only():
    api = _api_with_outputs("con_code", "name")
    assert infer_pk(api) == "(con_code)"


def test_infer_pk_date_only():
    api = _api_with_outputs("date", "value")
    assert infer_pk(api) == "(date)"


def test_infer_pk_no_match():
    api = _api_with_outputs("name", "value")
    assert infer_pk(api) is None


def test_infer_pk_with_driver():
    api = {
        "output_params": [{"name": "ts_code"}, {"name": "trade_date"}, {"name": "open"}],
        "input_params": [{"name": "ts_code", "required": True}],
    }
    driver = {"date_mode": "daily", "source_table": "stock_basic", "source_column": "ts_code"}
    result = infer_pk(api, driver)
    assert "ts_code" in result
    assert "trade_date" in result


def test_infer_pk_with_driver_con_code():
    api = {
        "output_params": [{"name": "index_code"}, {"name": "con_code"},
                         {"name": "trade_date"}, {"name": "weight"}],
        "input_params": [{"name": "index_code", "required": True}],
    }
    driver = {"date_mode": "monthly"}
    result = infer_pk(api, driver)
    assert "index_code" in result
    assert "con_code" in result
    assert "trade_date" in result


# ── _quote_name ──

def test_quote_normal_name():
    assert _quote_name("ts_code") == "ts_code"
    assert _quote_name("trade_date") == "trade_date"


def test_quote_sql_keyword():
    assert _quote_name("on") == '"on"'
    assert _quote_name("limit") == '"limit"'
    assert _quote_name("order") == '"order"'
    assert _quote_name("index") == '"index"'


def test_quote_numeric_start():
    assert _quote_name("1day") == '"1day"'
    assert _quote_name("52week_high") == '"52week_high"'


def test_quote_special_chars():
    assert _quote_name("a-b") == '"a-b"'
    assert _quote_name("a.b") == '"a.b"'


# ── date_to_cal_index ──

@pytest.fixture
def sample_calendar():
    return ["1991-01-02", "1991-01-03", "1991-01-04", "1991-01-07", "1991-01-08"]


def test_date_exact_match(sample_calendar):
    assert date_to_cal_index("19910103", sample_calendar) == 1


def test_date_non_trading_forward_fill(sample_calendar):
    assert date_to_cal_index("19910106", sample_calendar) == 3


def test_date_before_calendar(sample_calendar):
    assert date_to_cal_index("19900101", sample_calendar) is None


def test_date_after_calendar(sample_calendar):
    assert date_to_cal_index("19920101", sample_calendar) is None


def test_date_empty_string(sample_calendar):
    assert date_to_cal_index("", sample_calendar) is None


def test_date_short_string(sample_calendar):
    assert date_to_cal_index("1991", sample_calendar) is None


def test_date_none(sample_calendar):
    assert date_to_cal_index(None, sample_calendar) is None  # type: ignore[arg-type]


def test_date_empty_calendar():
    assert date_to_cal_index("19910103", []) is None


def test_date_first_in_calendar(sample_calendar):
    assert date_to_cal_index("19910102", sample_calendar) == 0


def test_date_last_in_calendar(sample_calendar):
    assert date_to_cal_index("19910108", sample_calendar) == 4


# ── _make_key (logic test) ──

def make_key(req_params: dict) -> str:
    """Replicating DataClient._make_key logic for isolated testing."""
    params_no_token = {k: v for k, v in req_params.items() if k != "token"}
    return hashlib.sha256(
        json.dumps(params_no_token, sort_keys=True).encode()
    ).hexdigest().upper()[:16]


def test_make_key_deterministic():
    params = {"api_name": "daily", "token": "abc123", "ts_code": "000001.SZ"}
    k1 = make_key(params)
    k2 = make_key(params)
    assert k1 == k2
    assert len(k1) == 16


def test_make_key_excludes_token():
    params_a = {"api_name": "daily", "token": "abc123", "ts_code": "000001.SZ"}
    params_b = {"api_name": "daily", "token": "xyz789", "ts_code": "000001.SZ"}
    assert make_key(params_a) == make_key(params_b)


def test_make_key_order_independent():
    params1 = {"a": "1", "b": "2", "token": "x"}
    params2 = {"b": "2", "a": "1", "token": "y"}
    assert make_key(params1) == make_key(params2)


def test_make_key_different_params():
    params1 = {"api_name": "daily", "ts_code": "000001.SZ"}
    params2 = {"api_name": "daily", "ts_code": "000002.SZ"}
    assert make_key(params1) != make_key(params2)
