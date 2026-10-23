"""分类、生成与客户端规则共享的本轮候选配置。"""
import ast
from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import Path

from database import engine
from database.utils import atomic_write_text
from scripts.classify_apis import classify_catalog
from scripts.generate_schema import generate_registry, generate_schema, render_registry


@dataclass(frozen=True)
class RuntimeSnapshot:
    api_list: list[dict]
    registry: list[dict]
    schema_sql: str
    fingerprint: str
    changed_files: list[str]


def _validate(api_list: list[dict], registry: list[dict], schema_sql: str) -> None:
    specs = {a["api_name"]: a for a in api_list}
    entries = {e["table"]: e for e in registry}
    for entry in registry:
        name = entry["api"]
        cols = {p["name"] for p in specs[name].get("output_params", [])}
        referenced = [*entry.get("primary_key", []), *entry.get("dedupe_cols", [])]
        referenced += [entry[k] for k in ("partition_key", "date_col") if entry.get(k)]
        missing = set(referenced) - cols
        if missing:
            raise ValueError(f"{name}: 契约引用不存在的字段 {sorted(missing)}")
        driver = entry.get("driver", {})
        if driver:
            if driver.get("date_mode") not in ("once", "daily", "monthly", "yearly"):
                raise ValueError(f"{name}: 非法 date_mode")
            for key in ("refresh_days", "refresh_batch_size"):
                if key in driver and (type(driver[key]) is not int or driver[key] < 1):
                    raise ValueError(f"{name}: {key} 必须为正整数")
            if "refresh_days" in driver and "refresh_batch_size" not in driver:
                raise ValueError(f"{name}: 缺少 refresh_batch_size")
        source = driver.get("source_table")
        if source:
            if source not in entries:
                raise ValueError(f"{name}: 驱动表 {source} 未启用")
            source_cols = {p["name"] for p in specs[source].get("output_params", [])}
            if driver.get("source_column") not in source_cols:
                raise ValueError(f"{name}: 驱动字段不存在")
            if set(driver.get("filters", {})) - source_cols:
                raise ValueError(f"{name}: 驱动过滤字段不存在")
    for name in entries:
        seen = set()
        while name:
            if name in seen:
                raise ValueError(f"驱动依赖存在环: {name}")
            seen.add(name)
            name = entries[name].get("driver", {}).get("source_table")
    # 在临时内存库检查完整 DDL/视图依赖，失败时尚未改动任何产物。
    with engine.connect(":memory:") as conn:
        engine.execute_script(conn, schema_sql)


def prepare_runtime(config: dict, root: Path | None = None) -> RuntimeSnapshot:
    root = Path(root or engine.PROJECT_ROOT)
    source = root / "api_index.json"
    original_source = source.read_text(encoding="utf-8")
    original_apis = json.loads(original_source)
    apis = classify_catalog(original_apis, config)
    active = [a for a in apis if (c := a.get("_project", {}).get("classification", {})).get("usable")
              and c.get("rule") in (1, 2)]
    schema_sql = generate_schema(active)
    registry_code = generate_registry(active)
    registry = ast.literal_eval(ast.parse(registry_code).body[0].value)
    etl_path = root / "database" / "etl.py"
    original_etl = etl_path.read_text(encoding="utf-8")
    etl_code = render_registry(original_etl, registry_code)
    _validate(active, registry, schema_sql)
    candidates = {
        source: original_source if apis == original_apis else json.dumps(apis, ensure_ascii=False, indent=2),
        root / "database" / "schema.sql": schema_sql,
        etl_path: etl_code,
    }
    old = {p: p.read_text(encoding="utf-8") if p.exists() else None for p in candidates}
    changed = [p for p, value in candidates.items() if value != old[p]]
    published = []
    try:
        for path in changed:
            atomic_write_text(path, candidates[path])
            published.append(path)
    except Exception:
        for path in reversed(published):
            if old[path] is None:
                path.unlink()
            else:
                atomic_write_text(path, old[path])
        raise
    settings = {k: config.get(k) for k in ("tushare_rate_limit", "tushare_interval_r2_ms", "backfill_since", "pull_after")}
    digest = sha256(json.dumps([apis, settings], sort_keys=True).encode()).hexdigest()
    return RuntimeSnapshot(apis, registry, schema_sql, digest,
                           [str(p.relative_to(root)) for p in changed])
