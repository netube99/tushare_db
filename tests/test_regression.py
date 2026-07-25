"""A5-4: TABLE_SPECS 键名一致性回归测试 — P0-4 根除点.

遍历 TABLE_SPECS 断言所有 opt 键名与 build_field_map 的读取键一致.
"""

from qlib_export.specs import TABLE_SPECS, build_field_map

# build_field_map 中读取的 spec 键名（opt 拷贝循环）
_OPT_KEYS_READ_BY_BUILD = {
    "agg", "agg_count", "agg_weighted", "agg_split_col",
    "encode", "encode_col", "virtual_inst", "inst_filter",
}

# 每个 spec 中声明了这些键时，build_field_map 应能读取到
# 测试: 对所有 spec 中出现的 opt 键，确认它们都在 _OPT_KEYS_READ_BY_BUILD 中
# 反向: 对所有 spec 使用的键，确认 copyloop 不会遗漏


def test_all_spec_opt_keys_covered():
    """验证 TABLE_SPECS 中使用的所有 opt 键都在 build_field_map 的拷贝循环中."""
    for i, spec in enumerate(TABLE_SPECS):
        table = spec["table"]
        for key in spec:
            # 跳过标准键（始终读取）
            if key in ("table", "inst_col", "date_col", "type", "prefix",
                       "ohlcv_hfq", "tech_prefix", "computed", "no_raw_fields"):
                continue
            # agg/encode 等 opt 键
            if key in _OPT_KEYS_READ_BY_BUILD:
                continue
            # 已知非 opt 键
            if key.startswith("_"):
                continue
            raise AssertionError(
                f"TABLE_SPECS[{i}] {table}: key '{key}' "
                f"not in build_field_map opt copy loop! "
                f"Will be silently ignored."
            )


def test_build_field_map_opt_keys_exist_in_specs():
    """反向验证: build_field_map 引用的 opt 键在至少一个 spec 中使用."""
    used_keys = set()
    for spec in TABLE_SPECS:
        used_keys.update(spec.keys())
    for opt_key in _OPT_KEYS_READ_BY_BUILD:
        if opt_key not in used_keys:
            raise AssertionError(
                f"opt key '{opt_key}' is read by build_field_map "
                f"but never used in any TABLE_SPECS. Dead code?"
            )


def test_key_map_consistency():
    """P0-4 回归: agg_count → agg_count_col, agg_weighted → agg_weighted_cols, agg_split_col → agg_col."""
    import sqlite3
    from database.utils import get_conn

    REQ_ENTRY_KEYS = {"agg_count_col", "agg_weighted_cols", "agg_col"}

    spec_keys_with_aliases = set()
    for spec in TABLE_SPECS:
        if spec.get("agg_count"):
            spec_keys_with_aliases.add("agg_count")
        if spec.get("agg_weighted"):
            spec_keys_with_aliases.add("agg_weighted")
        if spec.get("agg_split_col"):
            spec_keys_with_aliases.add("agg_split_col")

    assert spec_keys_with_aliases.issubset(
        {"agg_count", "agg_weighted", "agg_split_col"}
    ), f"Unexpected agg keys in specs: {spec_keys_with_aliases}"

    assert len(spec_keys_with_aliases) > 0, (
        "No specs use agg_count/agg_weighted/agg_split_col. "
        "If intentionally removed, remove from build_field_map too."
    )


def test_agg_key_mapping_applied():
    """验证 build_field_map 正确应用了 key_map 重映射."""
    import sqlite3
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row

    for spec in TABLE_SPECS:
        if spec.get("agg_count"):
            db.execute(f"CREATE TABLE {spec['table']} ({spec['inst_col'] or 'col'} TEXT, {spec['date_col']} TEXT, val REAL)")
    db.commit()

    try:
        conversion_tables = build_field_map(db)
    finally:
        db.close()

    for entry in conversion_tables:
        source_table = entry["source_table"]
        spec = next(s for s in TABLE_SPECS if s["table"] == source_table)

        if spec.get("agg_count"):
            assert entry.get("agg_count_col") == spec["agg_count"], (
                f"{source_table}: agg_count → agg_count_col mapping failed"
            )

        if spec.get("agg_weighted"):
            assert entry.get("agg_weighted_cols") == spec["agg_weighted"], (
                f"{source_table}: agg_weighted → agg_weighted_cols mapping failed"
            )

        if spec.get("agg_split_col"):
            assert entry.get("agg_col") == spec["agg_split_col"], (
                f"{source_table}: agg_split_col → agg_col mapping failed"
            )
