"""qlib_export 审查测试 — TDD 证实/证伪疑似缺陷 + 守护回归."""

import sqlite3

import numpy as np
import pytest

from qlib_export import (
    init_sync_log,
    CalendarSync,
    FeatureSync,
    IncrementalSync,
    FieldRebuilder,
    InstrumentSync,
)
from qlib_export.binio import write_bin, append_bin
from qlib_export.calendar import format_date, date_to_cal_index
from qlib_export.instruments import (
    ts_code_to_qlib_instrument,
    qlib_to_ts_code,
    IndexConstituentSync,
)
from qlib_export.sync_log import (
    is_synced,
    get_partial_records,
    upsert_sync_log,
)

CAL = ["20260615", "20260616", "20260617", "20260622"]


@pytest.fixture(autouse=True)
def _stub_logger(monkeypatch):
    import database.logger as dl

    class _Stub:
        def write(self, data):
            pass

    monkeypatch.setattr(dl, "get_json_logger", lambda: _Stub())


@pytest.fixture
def conn():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("CREATE TABLE trade_cal (exchange TEXT, cal_date TEXT, is_open INTEGER)")
    for d in CAL:
        db.execute("INSERT INTO trade_cal VALUES ('SSE', ?, 1)", (d,))
    db.execute("CREATE TABLE stock_basic (ts_code TEXT, list_date TEXT, delist_date TEXT)")
    db.execute("INSERT INTO stock_basic VALUES ('000001.SZ', '19910403', NULL)")
    for tbl in ("etf_basic", "index_basic", "cb_basic"):
        db.execute(f"CREATE TABLE {tbl} (ts_code TEXT, list_date TEXT)")
    db.execute("CREATE TABLE sw_daily (ts_code TEXT, trade_date TEXT)")
    db.execute("CREATE TABLE gz_index (date TEXT)")
    db.execute("CREATE TABLE margin (exchange_id TEXT, trade_date TEXT)")
    db.execute("CREATE TABLE moneyflow_hsgt (trade_date TEXT)")
    db.execute("CREATE TABLE stk_factor_pro (ts_code TEXT, trade_date TEXT)")
    init_sync_log(db)
    return db


@pytest.fixture
def calendar(conn, tmp_path):
    cal = CalendarSync(tmp_path)
    cal.full_init(conn)
    return cal


def read_bin(path):
    return np.fromfile(path, dtype="<f")


# ── binio.write_bin ──


def test_write_bin_header_and_trim(tmp_path):
    p = tmp_path / "f.day.bin"
    vals = np.array([np.nan, 1.0, 2.0, np.nan], dtype=np.float32)
    assert write_bin(p, vals) is True
    data = read_bin(p)
    assert data.dtype == np.float32
    assert data[0] == np.float32(1)
    np.testing.assert_array_equal(data[1:], np.array([1.0, 2.0], dtype=np.float32))


def test_write_bin_all_nan_no_file(tmp_path):
    p = tmp_path / "f.day.bin"
    assert write_bin(p, np.full(4, np.nan)) is False
    assert not p.exists()


# ── binio.append_bin ──


def test_append_bin_pure_append(tmp_path):
    p = tmp_path / "f.day.bin"
    vals = np.array([1.0, 2.0, 3.0] + [np.nan] * 3, dtype=np.float32)
    write_bin(p, vals)
    new = np.array([np.nan] * 3 + [4.0, 5.0, np.nan], dtype=np.float32)
    assert append_bin(p, new) is True
    np.testing.assert_array_equal(read_bin(p)[1:], np.array([1.0, 2.0, 3.0, 4.0, 5.0], dtype=np.float32))


def test_append_bin_noop_when_no_new(tmp_path):
    p = tmp_path / "f.day.bin"
    vals = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    write_bin(p, vals)
    new = np.array([1.0, 2.0, 3.0, np.nan, np.nan, np.nan], dtype=np.float32)
    assert append_bin(p, new) is False
    np.testing.assert_array_equal(read_bin(p)[1:], vals)


def test_append_bin_missing_file_degrades_to_write(tmp_path):
    p = tmp_path / "f.day.bin"
    new = np.array([1.0, np.nan, 2.0], dtype=np.float32)
    assert append_bin(p, new) is True
    data = read_bin(p)
    assert data[0] == 0
    np.testing.assert_array_equal(data[1:], np.array([1.0, np.nan, 2.0], dtype=np.float32))


def test_append_bin_corrupt_rebuilds(tmp_path):
    p = tmp_path / "f.day.bin"
    p.write_bytes(b"\x00\x01\x02")
    new = np.array([1.0, 2.0], dtype=np.float32)
    assert append_bin(p, new) is True
    np.testing.assert_array_equal(read_bin(p)[1:], np.array([1.0, 2.0], dtype=np.float32))


def test_append_bin_overlap_is_append_only(tmp_path):
    p = tmp_path / "f.day.bin"
    vals = np.array([1.0, 2.0, 3.0, np.nan, np.nan, np.nan], dtype=np.float32)
    write_bin(p, vals)
    new = np.array([np.nan, 9.0, np.nan, np.nan, np.nan, 7.0], dtype=np.float32)
    assert append_bin(p, new) is True
    data = read_bin(p)
    assert data[0] == 0
    np.testing.assert_array_equal(
        data[1:], np.array([1.0, 2.0, 3.0, np.nan, np.nan, 7.0], dtype=np.float32)
    )


def test_append_bin_overlap_only_returns_false(tmp_path):
    p = tmp_path / "f.day.bin"
    vals = np.array([1.0, np.nan, 3.0], dtype=np.float32)
    write_bin(p, vals)
    new = np.array([np.nan, 5.0, np.nan], dtype=np.float32)
    assert append_bin(p, new) is False
    np.testing.assert_array_equal(
        read_bin(p)[1:], np.array([1.0, np.nan, 3.0], dtype=np.float32)
    )


def test_append_bin_overlap_all_nan_keeps_append_only(tmp_path):
    p = tmp_path / "f.day.bin"
    vals = np.array([1.0, 2.0, 3.0, np.nan, np.nan, np.nan], dtype=np.float32)
    write_bin(p, vals)
    new = np.array([np.nan] * 3 + [np.nan, np.nan, 7.0], dtype=np.float32)
    assert append_bin(p, new) is True
    np.testing.assert_array_equal(
        read_bin(p)[1:], np.array([1.0, 2.0, 3.0, np.nan, np.nan, 7.0], dtype=np.float32)
    )


# ── calendar ──


def test_format_date_normal():
    assert format_date("19910102") == "1991-01-02"


def test_format_date_already_formatted_passthrough():
    assert format_date("1991-01-02") == "1991-01-02"

def test_format_date_short_passthrough():
    assert format_date("1991") == "1991"
    assert format_date("") == ""


def test_date_to_cal_index_forward_fill():
    cal = ["2026-06-15", "2026-06-16", "2026-06-17"]
    assert date_to_cal_index("20260613", cal) is None
    assert date_to_cal_index("20260615", cal) == 0
    assert date_to_cal_index("20260701", cal) is None


# ── instruments ──


def test_ts_code_roundtrip_all_exchanges():
    for code in ["000001.SZ", "600000.SH", "830799.BJ"]:
        inst = ts_code_to_qlib_instrument(code, "stock")
        assert qlib_to_ts_code(inst, "stock") == code


def test_sw_sector_identity():
    assert ts_code_to_qlib_instrument("801010.SI", "sw_sector") == "801010.SI"
    assert qlib_to_ts_code("801010.SI", "sw_sector") == "801010.SI"


def test_build_periods_gap_split_and_open_end(tmp_path):
    sync = IndexConstituentSync(tmp_path)
    inst = "SZ000001"
    dates = ["20260101", "20260102", "20260320"]
    periods = sync._build_periods(inst, dates, "20260320")
    assert len(periods) == 2
    assert periods[0] == (inst, "2026-01-01", "2026-01-02")
    assert periods[1] == (inst, "2026-03-20", "2099-12-31")


def test_build_periods_closed_end(tmp_path):
    sync = IndexConstituentSync(tmp_path)
    periods = sync._build_periods("SZ000001", ["20260101", "20260102"], "20260601")
    assert periods == [("SZ000001", "2026-01-01", "2026-01-02")]


def test_delisted_stock_in_all_txt(conn, tmp_path):
    conn.execute("INSERT INTO stk_factor_pro VALUES ('999999.SZ', '20150105')")
    conn.execute("INSERT INTO stk_factor_pro VALUES ('999999.SZ', '20180601')")
    conn.commit()
    InstrumentSync(tmp_path).full_init(conn)
    lines = (tmp_path / "instruments" / "all.txt").read_text().splitlines()
    entry = next(l for l in lines if l.startswith("SZ999999"))
    _, start, end = entry.split("\t")
    assert start == "2015-01-05"
    assert end == "2018-06-01"


# ── sync_log ──


def test_init_sync_log_schema_matches_contract(conn):
    cols = {r["name"]: r for r in conn.execute("PRAGMA table_info(bin_sync_log)").fetchall()}
    expected = {
        "instrument": "TEXT",
        "source_table": "TEXT",
        "last_date": "TEXT",
        "first_date": "TEXT",
        "fields_json": "TEXT",
        "row_count": "INTEGER",
        "status": "TEXT",
        "error_msg": "TEXT",
        "updated_at": "TEXT",
    }
    assert set(cols) == set(expected)
    for name, typ in expected.items():
        assert cols[name]["type"] == typ
    assert cols["first_date"]["dflt_value"] == "''"
    assert cols["status"]["dflt_value"] == "'done'"
    ddl = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='bin_sync_log'"
    ).fetchone()[0]
    assert "PRIMARY KEY (instrument, source_table)" in ddl


def test_is_synced_field_subset(conn):
    upsert_sync_log(conn, "SZ000001", "t", status="done", fields=["a", "b"])
    assert is_synced(conn, "SZ000001", "t", ["a"]) is True
    assert is_synced(conn, "SZ000001", "t", ["a", "c"]) is False


def test_get_partial_records_filter(conn):
    upsert_sync_log(conn, "A", "t", status="done", fields=[])
    upsert_sync_log(conn, "B", "t", status="partial", fields=[])
    upsert_sync_log(conn, "C", "t", status="error", fields=[])
    recs = get_partial_records(conn)
    assert {r["instrument"] for r in recs} == {"B", "C"}


# ── FeatureSync 全量转换 ──


def _make_table(conn, name, date_col, extra_cols):
    cols = ", ".join(["ts_code TEXT", f"{date_col} TEXT"] + [f"{c} REAL" for c in extra_cols])
    conn.execute(f"CREATE TABLE {name} ({cols})")


def test_full_convert_basic_and_vwap(conn, calendar, tmp_path):
    _make_table(conn, "t_px", "trade_date", ["amount", "vol"])
    conn.execute("INSERT INTO t_px VALUES ('000001.SZ', '20260615', 1000.0, 100.0)")
    conn.commit()
    cfg = {
        "source_table": "t_px", "inst_col": "ts_code", "date_col": "trade_date",
        "inst_type": "stock",
        "fields": [
            {"bin_name": "amount", "tushare_col": "amount"},
            {"bin_name": "volume", "tushare_col": "vol"},
            {"bin_name": "vwap", "computed": "vwap"},
        ],
    }
    fs = FeatureSync(tmp_path, calendar, conn)
    stats = fs.full_convert([cfg], quiet=True)
    assert stats["total_written"] == 1
    assert not (tmp_path / "features" / "sz000002").exists()
    empty_row = conn.execute(
        "SELECT * FROM bin_sync_log WHERE instrument='SZ000002'"
    ).fetchone()
    assert empty_row is None
    inst_dir = tmp_path / "features" / "sz000001"
    np.testing.assert_allclose(read_bin(inst_dir / "amount.day.bin")[1:], [1000.0])
    np.testing.assert_allclose(read_bin(inst_dir / "vwap.day.bin")[1:], [100.0])
    row = conn.execute(
        "SELECT * FROM bin_sync_log WHERE instrument='SZ000001'"
    ).fetchone()
    assert row["status"] == "done"
    assert row["last_date"] == "20260615"


def test_align_non_trading_ann_date_forward_fill(conn, calendar, tmp_path):
    _make_table(conn, "t_ann", "ann_date", ["val"])
    conn.execute("INSERT INTO t_ann VALUES ('000001.SZ', '20260619', 1.0)")
    conn.commit()
    cfg = {
        "source_table": "t_ann", "inst_col": "ts_code", "date_col": "ann_date",
        "inst_type": "stock",
        "fields": [{"bin_name": "ta_val", "tushare_col": "val"}],
    }
    FeatureSync(tmp_path, calendar, conn).full_convert([cfg], quiet=True)
    data = read_bin(tmp_path / "features" / "sz000001" / "ta_val.day.bin")
    assert data[0] == 3
    np.testing.assert_allclose(data[1:], [1.0])


def test_agg_sum_count_weighted(conn, calendar, tmp_path):
    _make_table(conn, "t_bt", "trade_date", ["price", "vol"])
    conn.execute("INSERT INTO t_bt VALUES ('000001.SZ', '20260615', 10.0, 1.0)")
    conn.execute("INSERT INTO t_bt VALUES ('000001.SZ', '20260615', 20.0, 3.0)")
    conn.execute("INSERT INTO t_bt VALUES ('000001.SZ', '20260616', 30.0, 2.0)")
    conn.commit()
    cfg = {
        "source_table": "t_bt", "inst_col": "ts_code", "date_col": "trade_date",
        "inst_type": "stock", "agg": "sum_count_weighted",
        "fields": [
            {"bin_name": "bt_price", "tushare_col": "price",
             "agg_weighted": True, "weight_col": "vol"},
            {"bin_name": "bt_vol", "tushare_col": "vol"},
            {"bin_name": "bt_cnt", "computed": "count"},
        ],
    }
    FeatureSync(tmp_path, calendar, conn).full_convert([cfg], quiet=True)
    d = tmp_path / "features" / "sz000001"
    np.testing.assert_allclose(read_bin(d / "bt_price.day.bin")[1:], [17.5, 30.0])
    np.testing.assert_allclose(read_bin(d / "bt_vol.day.bin")[1:], [4.0, 2.0])
    np.testing.assert_allclose(read_bin(d / "bt_cnt.day.bin")[1:], [2.0, 1.0])


def test_encoding_st_and_unknown(conn, calendar, tmp_path):
    conn.execute("CREATE TABLE t_st (ts_code TEXT, trade_date TEXT, type TEXT)")
    conn.execute("INSERT INTO t_st VALUES ('000001.SZ', '20260615', 'ST')")
    conn.execute("INSERT INTO t_st VALUES ('000001.SZ', '20260616', 'P')")
    conn.commit()
    cfg = {
        "source_table": "t_st", "inst_col": "ts_code", "date_col": "trade_date",
        "inst_type": "stock", "encode": "is_st", "encode_col": "type",
        "fields": [{"bin_name": "st_is_st", "computed": "encode_is_st"}],
    }
    FeatureSync(tmp_path, calendar, conn).full_convert([cfg], quiet=True)
    d = tmp_path / "features"
    np.testing.assert_allclose(read_bin(d / "sz000001" / "st_is_st.day.bin")[1:], [1.0, 0.0])


def test_full_convert_row_count_is_raw_rows(conn, calendar, tmp_path):
    _make_table(conn, "t_agg", "ann_date", ["num"])
    conn.execute("INSERT INTO t_agg VALUES ('000001.SZ', '20260615', 1.0)")
    conn.execute("INSERT INTO t_agg VALUES ('000001.SZ', '20260615', 2.0)")
    conn.commit()
    cfg = {
        "source_table": "t_agg", "inst_col": "ts_code", "date_col": "ann_date",
        "inst_type": "stock", "agg": "sum_count",
        "fields": [
            {"bin_name": "tg_num", "tushare_col": "num"},
            {"bin_name": "tg_cnt", "computed": "count"},
        ],
    }
    FeatureSync(tmp_path, calendar, conn).full_convert([cfg], quiet=True)
    row = conn.execute(
        "SELECT row_count FROM bin_sync_log WHERE instrument='SZ000001'"
    ).fetchone()
    assert row["row_count"] == 2


def test_interrupted_full_convert_marks_partial_and_resumes(conn, calendar, tmp_path):
    _make_table(conn, "t_res", "trade_date", ["val"])
    conn.execute("INSERT INTO t_res VALUES ('000001.SZ', '20260615', 1.0)")
    conn.commit()
    cfg = {
        "source_table": "t_res", "inst_col": "ts_code", "date_col": "trade_date",
        "inst_type": "stock",
        "fields": [{"bin_name": "tr_val", "tushare_col": "val"}],
    }
    fs = FeatureSync(tmp_path, calendar, conn)
    upsert_sync_log(conn, "SZ000001", "t_res", status="partial",
                    fields=["tr_val"])
    recs = fs.resume_check()
    assert len(recs) == 1 and recs[0]["instrument"] == "SZ000001"
    fs.full_convert([cfg], quiet=True)
    assert fs.resume_check() == []
    assert (tmp_path / "features" / "sz000001" / "tr_val.day.bin").exists()


# ── IncrementalSync ──


def _full_sync(conn, calendar, tmp_path, cfg):
    fs = FeatureSync(tmp_path, calendar, conn)
    fs.full_convert([cfg], quiet=True)
    inc = IncrementalSync(tmp_path, calendar, conn, fs)
    return fs, inc


def test_daily_append_ann_date_collision(conn, calendar, tmp_path):
    _make_table(conn, "t_hold", "ann_date", ["val"])
    conn.execute("INSERT INTO t_hold VALUES ('000001.SZ', '20260615', 1.0)")
    conn.commit()
    cfg = {
        "source_table": "t_hold", "inst_col": "ts_code", "date_col": "ann_date",
        "inst_type": "stock",
        "fields": [{"bin_name": "th_val", "tushare_col": "val"}],
    }
    fs, inc = _full_sync(conn, calendar, tmp_path, cfg)
    bin_path = tmp_path / "features" / "sz000001" / "th_val.day.bin"
    np.testing.assert_allclose(read_bin(bin_path)[1:], [1.0])

    conn.execute("INSERT INTO t_hold VALUES ('000001.SZ', '20260615', 2.0)")
    conn.commit()
    stats = inc.daily_sync([cfg], quiet=True)
    assert stats["updated_records"] == 1
    np.testing.assert_allclose(read_bin(bin_path)[1:], [2.0])


def test_daily_midfill_detection_for_agg_table(conn, calendar, tmp_path):
    _make_table(conn, "t_mid", "ann_date", ["num"])
    conn.execute("INSERT INTO t_mid VALUES ('000001.SZ', '20260615', 1.0)")
    conn.execute("INSERT INTO t_mid VALUES ('000001.SZ', '20260616', 2.0)")
    conn.commit()
    cfg = {
        "source_table": "t_mid", "inst_col": "ts_code", "date_col": "ann_date",
        "inst_type": "stock", "agg": "sum_count",
        "fields": [
            {"bin_name": "tm_num", "tushare_col": "num"},
            {"bin_name": "tm_cnt", "computed": "count"},
        ],
    }
    fs, inc = _full_sync(conn, calendar, tmp_path, cfg)
    bin_path = tmp_path / "features" / "sz000001" / "tm_num.day.bin"
    np.testing.assert_allclose(read_bin(bin_path)[1:], [1.0, 2.0])

    conn.execute("INSERT INTO t_mid VALUES ('000001.SZ', '20260615', 10.0)")
    conn.commit()
    inc.daily_sync([cfg], quiet=True)
    np.testing.assert_allclose(read_bin(bin_path)[1:], [11.0, 2.0])
    row = conn.execute(
        "SELECT row_count FROM bin_sync_log WHERE instrument='SZ000001'"
    ).fetchone()
    assert row["row_count"] == 3


def test_daily_midfill_detection_plain_table(conn, calendar, tmp_path):
    _make_table(conn, "t_plain", "trade_date", ["val"])
    conn.execute("INSERT INTO t_plain VALUES ('000001.SZ', '20260615', 1.0)")
    conn.commit()
    cfg = {
        "source_table": "t_plain", "inst_col": "ts_code", "date_col": "trade_date",
        "inst_type": "stock",
        "fields": [{"bin_name": "tp_val", "tushare_col": "val"}],
    }
    fs, inc = _full_sync(conn, calendar, tmp_path, cfg)
    bin_path = tmp_path / "features" / "sz000001" / "tp_val.day.bin"
    conn.execute("INSERT INTO t_plain VALUES ('000001.SZ', '20260615', 99.0)")
    conn.execute("INSERT INTO t_plain VALUES ('000001.SZ', '20260616', 2.0)")
    conn.commit()
    inc.daily_sync([cfg], quiet=True)
    np.testing.assert_allclose(read_bin(bin_path)[1:], [99.0, 2.0])


def test_daily_new_instrument(conn, calendar, tmp_path):
    _make_table(conn, "t_new", "trade_date", ["val"])
    conn.execute("INSERT INTO t_new VALUES ('000001.SZ', '20260615', 1.0)")
    conn.commit()
    cfg = {
        "source_table": "t_new", "inst_col": "ts_code", "date_col": "trade_date",
        "inst_type": "stock",
        "fields": [{"bin_name": "tn_val", "tushare_col": "val"}],
    }
    fs, inc = _full_sync(conn, calendar, tmp_path, cfg)
    conn.execute("INSERT INTO stock_basic VALUES ('000002.SZ', '20260610', NULL)")
    conn.execute("INSERT INTO t_new VALUES ('000002.SZ', '20260616', 7.0)")
    conn.commit()
    stats = inc.daily_sync([cfg], quiet=True)
    assert stats["new_instruments"] == 1
    np.testing.assert_allclose(
        read_bin(tmp_path / "features" / "sz000002" / "tn_val.day.bin")[1:], [7.0]
    )


def test_daily_new_trading_days_appended(conn, calendar, tmp_path):
    _make_table(conn, "t_cal2", "trade_date", ["val"])
    conn.execute("INSERT INTO t_cal2 VALUES ('000001.SZ', '20260617', 3.0)")
    conn.commit()
    cfg = {
        "source_table": "t_cal2", "inst_col": "ts_code", "date_col": "trade_date",
        "inst_type": "stock",
        "fields": [{"bin_name": "tc_val", "tushare_col": "val"}],
    }
    fs, inc = _full_sync(conn, calendar, tmp_path, cfg)
    conn.execute("INSERT INTO trade_cal VALUES ('SSE', '20260618', 1)")
    conn.execute("INSERT INTO t_cal2 VALUES ('000001.SZ', '20260618', 4.0)")
    conn.commit()
    inc.daily_sync([cfg], quiet=True)
    bin_path = tmp_path / "features" / "sz000001" / "tc_val.day.bin"
    data = read_bin(bin_path)
    assert data[0] == 2
    np.testing.assert_array_equal(data[1:], [3.0, 4.0])


# ── FieldRebuilder ──


def test_rebuild_fields_vwap_keeps_bin(conn, calendar, tmp_path):
    _make_table(conn, "t_rv", "trade_date", ["amount", "vol"])
    conn.execute("INSERT INTO t_rv VALUES ('000001.SZ', '20260615', 1000.0, 100.0)")
    conn.commit()
    cfg = {
        "source_table": "t_rv", "inst_col": "ts_code", "date_col": "trade_date",
        "inst_type": "stock",
        "fields": [
            {"bin_name": "amount", "tushare_col": "amount"},
            {"bin_name": "volume", "tushare_col": "vol"},
            {"bin_name": "vwap", "computed": "vwap"},
        ],
    }
    fs = FeatureSync(tmp_path, calendar, conn)
    fs.full_convert([cfg], quiet=True)
    vwap_path = tmp_path / "features" / "sz000001" / "vwap.day.bin"
    np.testing.assert_allclose(read_bin(vwap_path)[1:], [100.0])

    rb = FieldRebuilder(tmp_path, conn, fs)
    rb.rebuild_fields([cfg], ["vwap"])
    assert vwap_path.exists()
    np.testing.assert_allclose(read_bin(vwap_path)[1:], [100.0])


def test_rebuild_fields_plain_field(conn, calendar, tmp_path):
    _make_table(conn, "t_rp", "trade_date", ["val"])
    conn.execute("INSERT INTO t_rp VALUES ('000001.SZ', '20260615', 1.0)")
    conn.execute("INSERT INTO t_rp VALUES ('000001.SZ', '20260616', 2.0)")
    conn.commit()
    cfg = {
        "source_table": "t_rp", "inst_col": "ts_code", "date_col": "trade_date",
        "inst_type": "stock",
        "fields": [{"bin_name": "trp_val", "tushare_col": "val"}],
    }
    fs = FeatureSync(tmp_path, calendar, conn)
    fs.full_convert([cfg], quiet=True)
    p = tmp_path / "features" / "sz000001" / "trp_val.day.bin"
    p.unlink()
    rb = FieldRebuilder(tmp_path, conn, fs)
    rb.rebuild_fields([cfg], ["trp_val"])
    np.testing.assert_allclose(read_bin(p)[1:], [1.0, 2.0])


def test_rebuild_fields_unknown_field_is_noop(conn, calendar, tmp_path):
    _make_table(conn, "t_del", "trade_date", ["val", "extra"])
    conn.execute("INSERT INTO t_del VALUES ('000001.SZ', '20260615', 1.0, 9.0)")
    conn.commit()
    cfg = {
        "source_table": "t_del", "inst_col": "ts_code", "date_col": "trade_date",
        "inst_type": "stock",
        "fields": [
            {"bin_name": "td_val", "tushare_col": "val"},
            {"bin_name": "td_extra", "tushare_col": "extra"},
        ],
    }
    fs = FeatureSync(tmp_path, calendar, conn)
    fs.full_convert([cfg], quiet=True)
    extra_path = tmp_path / "features" / "sz000001" / "td_extra.day.bin"
    assert extra_path.exists()

    cfg_new = {
        "source_table": "t_del", "inst_col": "ts_code", "date_col": "trade_date",
        "inst_type": "stock",
        "fields": [{"bin_name": "td_val", "tushare_col": "val"}],
    }
    rb = FieldRebuilder(tmp_path, conn, fs)
    rb.rebuild_fields([cfg_new], ["td_extra"])
    assert extra_path.exists()


# ── build_field_map 容错 ──


def test_build_field_map_skips_missing_tables():
    from qlib_export.specs import build_field_map

    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("CREATE TABLE stk_factor_pro (ts_code TEXT, trade_date TEXT, open_hfq REAL)")
    tables = build_field_map(db)
    names = {t["source_table"] for t in tables}
    assert "stk_factor_pro" in names
    assert "bak_daily" not in names
    assert all(len(t["fields"]) > 0 for t in tables)


def test_get_instruments_for_table_missing_source(tmp_path):
    from qlib_export.instruments import get_instruments_for_table

    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    cfg = {"inst_type": "etf", "inst_col": "ts_code", "date_col": "trade_date"}
    assert get_instruments_for_table(db, cfg) == []


# ── CLI ──


def test_cli_parse_args_modes(monkeypatch):
    import scripts.convert_to_qlib as cli

    monkeypatch.setattr("sys.argv", ["x", "--daily"])
    assert cli.parse_args().daily is True

    monkeypatch.setattr("sys.argv", ["x", "--fields", "open,high"])
    args = cli.parse_args()
    assert args.fields == "open,high" and not args.daily

    monkeypatch.setattr("sys.argv", ["x"])
    assert cli.parse_args().fields is None
