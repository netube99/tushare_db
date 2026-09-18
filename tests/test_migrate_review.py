"""迁移脚本纯逻辑回归 — 进度隔离 / 完成判定 / 原子写."""

import json

from database.engine import connect
from scripts import migrate_sqlite_to_duckdb as mig


def test_progress_path_scoped_to_target(tmp_path):
    p = mig._progress_path(str(tmp_path / "market.duckdb"))
    assert p.parent == tmp_path
    assert p.name == ".market.migrate_progress.json"


def test_is_done_requires_matching_target_state():
    conn = connect(":memory:")
    conn.execute("CREATE TABLE t (a BIGINT)")
    conn.execute("INSERT INTO t VALUES (1)")
    assert mig._is_done(conn, "t", {"status": "done", "rows": 1}) is True
    assert mig._is_done(conn, "t", {"status": "done", "rows": 2}) is False
    assert mig._is_done(conn, "missing", {"status": "done", "rows": 0}) is False
    assert mig._is_done(conn, "t", {"status": "error"}) is False


def test_progress_write_is_atomic_and_readable(tmp_path):
    p = tmp_path / ".x.migrate_progress.json"
    mig._save_progress(p, {"t": {"status": "done", "rows": 1}})
    assert not p.with_name(p.name + ".tmp").exists()
    assert json.loads(p.read_text(encoding="utf-8"))["t"]["rows"] == 1
