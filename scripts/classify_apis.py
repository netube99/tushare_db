#!/usr/bin/env python3
"""本地分类器 — 读取 api_index.json + user_config.yaml 积分 → 写入 _project.classification.

纯本地逻辑，不调 LLM。积分变化后重跑此脚本即可更新分类。

用法:
    python scripts/classify_apis.py
"""
from __future__ import annotations

import json
import re
import shutil
import sys
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from database.utils import atomic_write_text, load_config

INDEX_FILE = PROJECT_ROOT / "api_index.json"


def _hour_day(raw) -> bool:
    s = str(raw or "")
    return "小时" in s or "天" in s


def _parse_rate(raw) -> int | None:
    """从 rate_limit 提取次/分钟数，非分钟单位返回 None."""
    if raw is None or _hour_day(raw):
        return None
    m = re.search(r"(\d+)", str(raw))
    return int(m.group(1)) if m else None


def _result(api: dict, rule: int, label: str, usable: bool,
            rate: int | None, interval: int | None, note: str | None) -> dict:
    return {**api, "_rule": rule, "_rule_label": label, "_usable": usable,
            "_effective_rate": rate, "_recommended_interval_ms": interval,
            "_note": note}


def classify(api: dict, points: int, default_rate_limit: int) -> dict:
    """对单个 API 分类，返回带 _ 前缀内部字段的字典."""
    min_points = api.get("min_points")
    rate_limit = _parse_rate(api.get("rate_limit"))

    if api.get("is_premium", False):
        return _result(api, 6, "专属付费", False, None, None, "需单独付费解锁")

    if min_points is None or min_points <= points:
        raw_limit = str(api.get("rate_limit") or "")
        if _hour_day(raw_limit):
            return _result(api, 2, "小时/天级限流", False, None, None,
                           f"频率限制 {raw_limit}，不适合批量建库")
        eff_rate = rate_limit if rate_limit else default_rate_limit
        interval = int(60000 / eff_rate / 0.8)
        if rate_limit and rate_limit < default_rate_limit:
            return _result(api, 2, f"{eff_rate}/min", True, eff_rate, interval,
                           f"频率限制{eff_rate}/min（低于标准{default_rate_limit}）")
        return _result(api, 1, f"{eff_rate}/min", True, eff_rate, interval, None)

    return _result(api, 3, "积分不足", True, None, None,
                   f"min_points={min_points}>{points}，积分不足，频率受限")


def main():
    if not INDEX_FILE.exists():
        print(f"错误: {INDEX_FILE} 不存在")
        sys.exit(1)

    config = load_config()
    points = config.get("tushare_points", 2100)
    rate_limit = config.get("tushare_rate_limit", 200)
    print(f"当前积分: {points}，最高频率: {rate_limit}/min")

    index = json.loads(INDEX_FILE.read_text(encoding="utf-8"))
    classified = [classify(api, points, rate_limit) for api in index]
    exclude = set(config.get("exclude_apis", []))

    counts = Counter(api["_rule"] for api in classified)

    for api in classified:
        proj = api.setdefault("_project", {})
        always_drop = (
            api.get("unexpected_error")
            or any(p["name"] == "ts_code" and p.get("required")
                   for p in api.get("input_params", []))
        )
        rule_blocked = (not api["_usable"] or api["_rule"] == 3
                        or api["api_name"] in exclude)
        if always_drop or rule_blocked:
            if always_drop or not proj.get("classification", {}).get("overdraft"):
                proj.pop("classification", None)
            continue

        clf = {
            "rule": api["_rule"],
            "rule_label": api["_rule_label"],
            "usable": api["_usable"],
            "effective_rate": api["_effective_rate"],
            "recommended_interval_ms": api["_recommended_interval_ms"],
        }
        if api.get("_note"):
            clf["note"] = api["_note"]
        proj["classification"] = clf

    written = [a.get("_project", {}).get("classification", {}).get("rule")
               for a in classified]

    # 清理 classify() 产出的临时 _ 前缀字段（保留 _project）
    for api in classified:
        for key in list(api.keys()):
            if key.startswith("_") and key != "_project":
                del api[key]

    # 原子写: tmp + os.replace，防 SIGKILL 截断
    data = json.dumps(classified, ensure_ascii=False, indent=2)
    backup_path = INDEX_FILE.with_suffix(".bak.json")
    shutil.copy2(INDEX_FILE, backup_path)
    try:
        atomic_write_text(INDEX_FILE, data)
    except Exception:
        # 只有写入未成功时才从备份恢复
        print(f"写入失败，已从 {backup_path} 恢复")
        backup_path.rename(INDEX_FILE)
        raise
    try:
        backup_path.unlink()
    except OSError as e:
        print(f"警告: 删除备份 {backup_path} 失败: {e}")

    # 统计输出
    print(f"\n总计: {len(classified)} 个接口")
    for rule, label in [
        (1, "规则1 积分满足+标准频率"),
        (2, "规则2 积分满足+特殊低频"),
        (3, "规则3 积分不足"),
        (6, "规则6 专属付费"),
    ]:
        n = counts.get(rule, 0)
        if n:
            print(f"  {label:30s}: {n}")

    print(f"\n写入 classification: 规则1={written.count(1)} | 规则2={written.count(2)}")
    print(f"输出: {INDEX_FILE}  (_project.classification)")


if __name__ == "__main__":
    main()
