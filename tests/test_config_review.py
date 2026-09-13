"""辅助脚本与配置审查 — refresh_national_team_daily / refresh_pension_daily / 模板键一致性."""

from __future__ import annotations

import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest
import yaml

from scripts import refresh_national_team_daily as nt
from scripts import refresh_pension_daily as pen

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATH = PROJECT_ROOT / "user_config.template.yaml"

NT_HOLDER = "中央汇金投资有限责任公司"
PENSION_HOLDER = "基本养老保险基金八零二组合"


@pytest.fixture()
def conn():
    c = sqlite3.connect(":memory:")
    c.execute(
        """CREATE TABLE top10_floatholders (
            ts_code TEXT, ann_date TEXT, end_date TEXT, holder_name TEXT,
            hold_amount REAL, hold_ratio REAL, hold_float_ratio REAL,
            hold_change REAL, holder_type TEXT)"""
    )
    c.execute(
        """CREATE TABLE trade_cal (
            exchange TEXT NOT NULL, cal_date TEXT NOT NULL, is_open INTEGER NOT NULL,
            pretrade_date TEXT, PRIMARY KEY (exchange, cal_date))"""
    )
    yield c
    c.close()


def add_cal(conn: sqlite3.Connection, start: date, end: date) -> None:
    d = start
    rows = []
    while d <= end:
        rows.append((d.strftime("%Y%m%d"),))
        d += timedelta(days=1)
    conn.executemany("INSERT INTO trade_cal VALUES ('SSE', ?, 1, NULL)", rows)


def add_holder(
    conn: sqlite3.Connection,
    ts_code: str,
    ann_date: str,
    holder_name: str,
    holder_type: str = "一般企业",
    change: float | None = None,
    end_date: str = "20231231",
) -> None:
    conn.execute(
        "INSERT INTO top10_floatholders VALUES (?,?,?,?,?,?,?,?,?)",
        (ts_code, ann_date, end_date, holder_name, 100.0, 1.5, 1.5, change, holder_type),
    )


def run_main(mod, db_path: Path, monkeypatch, extra_args: list[str] | None = None) -> None:
    argv = ["prog", "--db", str(db_path)] + (extra_args or [])
    monkeypatch.setattr(sys, "argv", argv)
    mod.main()


# ── refresh_pension_daily.build ──


def test_pension_build_skips_null_ann_date(conn):
    add_cal(conn, date(2024, 1, 1), date(2024, 3, 1))
    add_holder(conn, "000001.SZ", "20240110", PENSION_HOLDER, holder_type="基本养老保险基金")
    conn.execute(
        "INSERT INTO top10_floatholders VALUES (?,?,?,?,?,?,?,?,?)",
        ("000002.SZ", None, "20231231", PENSION_HOLDER, 100.0, 1.0, 1.0, None, "基本养老保险基金"),
    )
    out = pen.build(conn)
    assert not out.empty
    assert out["pen_ann_date"].notna().all()
    assert (out["ts_code"] == "000001.SZ").all()


def test_pension_stale_boundary_exact_n_days_excluded(conn, monkeypatch):
    monkeypatch.setattr(pen, "STALE_DAYS", 5)
    add_cal(conn, date(2024, 1, 1), date(2024, 1, 31))
    add_holder(conn, "000001.SZ", "20240101", PENSION_HOLDER, holder_type="基本养老保险基金")
    out = pen.build(conn)
    days = set(out["trade_date"])
    assert "20240105" in days
    assert "20240106" not in days
    assert len(days) == 5
    assert out["pen_age"].max() == 4


def test_pension_carry_stops_at_next_ann(conn):
    add_cal(conn, date(2024, 1, 1), date(2024, 1, 10))
    add_holder(conn, "000001.SZ", "20240101", PENSION_HOLDER, holder_type="基本养老保险基金")
    add_holder(conn, "000001.SZ", "20240101", "基本养老保险基金八零一组合", holder_type="基本养老保险基金")
    add_holder(conn, "000001.SZ", "20240105", PENSION_HOLDER, holder_type="基本养老保险基金")
    out = pen.build(conn)
    assert not out.duplicated(["ts_code", "trade_date"]).any()
    assert out.loc[out["trade_date"] == "20240104", "pen_cnt"].iloc[0] == 2
    assert out.loc[out["trade_date"] == "20240105", "pen_cnt"].iloc[0] == 1
    assert out.loc[out["trade_date"] == "20240105", "pen_age"].iloc[0] == 0
    assert out["trade_date"].max() == "20240110"


def test_pension_empty_source_tables(conn):
    out = pen.build(conn)
    assert list(out.columns) == pen.COLUMNS
    assert out.empty


# ── refresh_national_team_daily.load_events / build ──


def test_nt_exit_then_reenter_emits_both_exits(conn):
    add_cal(conn, date(2024, 1, 1), date(2025, 6, 30))
    add_holder(conn, "000001.SZ", "20240110", NT_HOLDER, end_date="20231231")
    add_holder(conn, "000001.SZ", "20240110", "中央汇金资产管理有限公司", end_date="20231231")
    add_holder(conn, "000001.SZ", "20240420", "某基金", end_date="20240331")
    add_holder(conn, "000001.SZ", "20250110", NT_HOLDER, end_date="20241231")
    add_holder(conn, "000001.SZ", "20250420", "某基金", end_date="20250331")
    events = nt.load_events(conn)
    exits = set(events.loc[events["nt_cnt"] == 0, "ann_date"])
    assert exits == {"20240420", "20250420"}

    out = nt.build(conn, stale_days=400)
    state = out.set_index("trade_date")["nt_state"]
    assert state.loc["20240110"] == 1
    assert state.loc["20240420"] == 0
    assert state.loc["20241231"] == 0
    assert state.loc["20250110"] == 1
    assert state.loc["20250420"] == 0


def test_nt_same_day_present_wins(conn):
    add_cal(conn, date(2024, 1, 1), date(2024, 2, 1))
    add_holder(conn, "000001.SZ", "20240110", NT_HOLDER)
    add_holder(conn, "000001.SZ", "20240110", "某基金")
    events = nt.load_events(conn)
    assert len(events) == 1
    assert events.iloc[0]["nt_cnt"] == 1
    assert (events["nt_cnt"] > 0).all()


def test_nt_never_held_no_exit(conn):
    add_cal(conn, date(2024, 1, 1), date(2024, 2, 1))
    add_holder(conn, "000001.SZ", "20240110", "某基金")
    add_holder(conn, "000001.SZ", "20240120", "某基金")
    events = nt.load_events(conn)
    assert events.empty


def test_nt_stale_boundary_exact_n_days_excluded(conn):
    add_cal(conn, date(2024, 1, 1), date(2024, 1, 31))
    add_holder(conn, "000001.SZ", "20240101", NT_HOLDER)
    out = nt.build(conn, stale_days=5)
    days = set(out["trade_date"])
    assert "20240105" in days
    assert "20240106" not in days
    assert len(days) == 5


def test_nt_last_event_carries_until_stale(conn):
    add_cal(conn, date(2024, 1, 1), date(2024, 1, 31))
    add_holder(conn, "000001.SZ", "20240101", NT_HOLDER)
    out = nt.build(conn, stale_days=10)
    assert len(out) == 10
    assert out["nt_age"].max() == 9


# ── main(): 幂等 / 原子性 / 缺表报错 ──


@pytest.fixture()
def seeded_db(tmp_path, conn):
    add_cal(conn, date(2024, 1, 1), date(2024, 2, 1))
    add_holder(conn, "000001.SZ", "20240110", NT_HOLDER)
    add_holder(conn, "000002.SZ", "20240110", PENSION_HOLDER, holder_type="基本养老保险基金")
    path = tmp_path / "market.db"
    conn.commit()
    backup = sqlite3.connect(str(path))
    conn.backup(backup)
    backup.close()
    return path


def test_nt_main_rerun_idempotent(seeded_db, monkeypatch):
    run_main(nt, seeded_db, monkeypatch)
    c = sqlite3.connect(seeded_db)
    first = c.execute("SELECT COUNT(*) FROM national_team_daily").fetchone()[0]
    c.close()
    run_main(nt, seeded_db, monkeypatch)
    c = sqlite3.connect(seeded_db)
    second = c.execute("SELECT COUNT(*) FROM national_team_daily").fetchone()[0]
    c.close()
    assert first == second > 0


def test_pension_main_rerun_idempotent(seeded_db, monkeypatch):
    run_main(pen, seeded_db, monkeypatch)
    c = sqlite3.connect(seeded_db)
    first = c.execute("SELECT COUNT(*) FROM pension_float_daily").fetchone()[0]
    c.close()
    run_main(pen, seeded_db, monkeypatch)
    c = sqlite3.connect(seeded_db)
    second = c.execute("SELECT COUNT(*) FROM pension_float_daily").fetchone()[0]
    c.close()
    assert first == second > 0


def test_nt_main_preserves_old_table_on_write_failure(seeded_db, monkeypatch):
    run_main(nt, seeded_db, monkeypatch)
    c = sqlite3.connect(seeded_db)
    expected = c.execute("SELECT COUNT(*) FROM national_team_daily").fetchone()[0]
    c.close()

    def boom(self, *args, **kwargs):
        raise RuntimeError("simulated write failure")

    monkeypatch.setattr(pd.DataFrame, "to_sql", boom)
    with pytest.raises(RuntimeError):
        run_main(nt, seeded_db, monkeypatch)
    c = sqlite3.connect(seeded_db)
    assert c.execute("SELECT COUNT(*) FROM national_team_daily").fetchone()[0] == expected
    c.close()


def test_pension_main_preserves_old_table_on_write_failure(seeded_db, monkeypatch):
    run_main(pen, seeded_db, monkeypatch)
    c = sqlite3.connect(seeded_db)
    expected = c.execute("SELECT COUNT(*) FROM pension_float_daily").fetchone()[0]
    c.close()

    def boom(self, *args, **kwargs):
        raise RuntimeError("simulated write failure")

    monkeypatch.setattr(pd.DataFrame, "to_sql", boom)
    with pytest.raises(RuntimeError):
        run_main(pen, seeded_db, monkeypatch)
    c = sqlite3.connect(seeded_db)
    assert c.execute("SELECT COUNT(*) FROM pension_float_daily").fetchone()[0] == expected
    c.close()


def test_nt_main_missing_source_table_friendly_error(tmp_path, monkeypatch):
    with pytest.raises(SystemExit, match="top10_floatholders"):
        run_main(nt, tmp_path / "empty.db", monkeypatch)


def test_pension_main_missing_source_table_friendly_error(tmp_path, monkeypatch):
    with pytest.raises(SystemExit, match="top10_floatholders"):
        run_main(pen, tmp_path / "empty.db", monkeypatch)


# ── user_config.template.yaml 与代码消费键一致性 ──

CONSUMED_KEYS = {
    "tushare_token",
    "tushare_points",
    "tushare_rate_limit",
    "tushare_interval_r2_ms",
    "tushare_proxy",
    "backfill_since",
    "pull_after",
    "exclude_apis",
}


@pytest.fixture(scope="module")
def template_cfg():
    return yaml.safe_load(TEMPLATE_PATH.read_text(encoding="utf-8"))


def test_template_covers_all_consumed_keys(template_cfg):
    required = CONSUMED_KEYS - {"tushare_proxy"}
    assert required <= set(template_cfg)


def test_template_has_no_ghost_keys(template_cfg):
    assert set(template_cfg) <= CONSUMED_KEYS


def test_template_pull_after_parseable(template_cfg):
    hour, minute = template_cfg["pull_after"].strip().split(":")
    assert 0 <= int(hour) <= 23
    assert 0 <= int(minute) <= 59


def test_template_exclude_apis_strings(template_cfg):
    assert all(isinstance(x, str) for x in template_cfg["exclude_apis"])


def test_template_keeps_top10_floatholders(template_cfg):
    assert "top10_floatholders" not in template_cfg["exclude_apis"]
