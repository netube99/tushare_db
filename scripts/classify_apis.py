#!/usr/bin/env python3
"""本地分类器 — 读取 api_index.json + user_config.yaml 积分 → 写入 _project.classification.

纯本地逻辑，不调 LLM。积分变化后重跑此脚本即可更新分类。

用法:
    python scripts/classify_apis.py
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from database.utils import atomic_write_text, load_config

INDEX_FILE = PROJECT_ROOT / "api_index.json"


def _parse_rate(raw) -> int | None:
    """从 rate_limit 提取次/分钟数，非分钟单位返回 None."""
    if raw is None:
        return None
    import re
    s = str(raw)
    if '小时' in s or '天' in s:
        return None  # 非分钟级，不适用于建库
    m = re.search(r'(\d+)', s)
    return int(m.group(1)) if m else None


def classify(api: dict, points: int, default_rate_limit: int) -> dict:
    """对单个 API 分类，返回带 _ 前缀内部字段的字典."""
    min_points = api.get("min_points")
    rate_limit = _parse_rate(api.get("rate_limit"))
    is_premium = api.get("is_premium", False)

    # 规则 6: 专属付费
    if is_premium:
        return {
            **api,
            "_rule": 6,
            "_rule_label": "专属付费",
            "_usable": False,
            "_effective_rate": None,
            "_recommended_interval_ms": None,
            "_note": "需单独付费解锁",
        }

    # 规则 1 / 2: 积分满足
    if min_points is None or min_points <= points:
        # 小时/天级限流不适合批量建库
        raw_limit = str(api.get("rate_limit") or "")
        if "小时" in raw_limit or "天" in raw_limit:
            return {
                **api,
                "_rule": 2,
                "_rule_label": "小时/天级限流",
                "_usable": False,
                "_effective_rate": None,
                "_recommended_interval_ms": None,
                "_note": f"频率限制 {raw_limit}，不适合批量建库",
            }
        eff_rate = rate_limit if rate_limit else default_rate_limit
        interval = int(60000 / eff_rate / 0.8)  # 0.8 安全冗余，内置处理
        if rate_limit and rate_limit < default_rate_limit:
            _rule = 2
            _label = f"{eff_rate}/min"
            _note = f"频率限制{eff_rate}/min（低于标准{default_rate_limit}）"
        else:
            _rule = 1
            _label = f"{eff_rate}/min"
            _note = None
        return {
            **api,
            "_rule": _rule,
            "_rule_label": _label,
            "_usable": True,
            "_effective_rate": eff_rate,
            "_recommended_interval_ms": interval,
            "_note": _note,
        }

    # min_points > points — 积分不足
    return {
        **api,
        "_rule": 3,
        "_rule_label": "积分不足",
        "_usable": True,
        "_effective_rate": None,
        "_recommended_interval_ms": None,
        "_note": f"min_points={min_points}>{points}，积分不足，频率受限",
    }


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

    counts: dict[int, int] = {}
    for api in classified:
        _rule = api["_rule"]
        counts[_rule] = counts.get(_rule, 0) + 1

        proj = api.setdefault("_project", {})

        # 排除条件（对应原 main() 跳过逻辑）
        if api.get("unexpected_error"):
            proj.pop("classification", None)
            continue
        if not api["_usable"] or api["_rule"] == 3 or api["api_name"] in exclude:
            # 保留手动标记的 overdraft classification
            existing = proj.get("classification", {})
            if not existing.get("overdraft"):
                proj.pop("classification", None)
            continue
        # ts_code 必填 → 无法批量拉全市场，自动排除
        if any(p["name"] == "ts_code" and p.get("required")
               for p in api.get("input_params", [])):
            proj.pop("classification", None)
            continue

        # 写入 classification
        proj["classification"] = {
            "rule": api["_rule"],
            "rule_label": api["_rule_label"],
            "usable": api["_usable"],
            "effective_rate": api["_effective_rate"],
            "recommended_interval_ms": api["_recommended_interval_ms"],
        }
        if api.get("_note"):
            proj["classification"]["note"] = api["_note"]

    # 计数已写入 classification 的 API
    r1 = sum(1 for a in classified
             if a.get("_project", {}).get("classification", {}).get("rule") == 1)
    r2 = sum(1 for a in classified
             if a.get("_project", {}).get("classification", {}).get("rule") == 2)

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

    print(f"\n写入 classification: 规则1={r1} | 规则2={r2}")
    print(f"输出: {INDEX_FILE}  (_project.classification)")


if __name__ == "__main__":
    main()
