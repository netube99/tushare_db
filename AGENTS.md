# AGENTS.md — tushare_db

tushare_db：Tushare_Pro → SQLite → Qlib 数据管道。

- 流水线：`api_index.json` → `classify_apis.py`（分级）→ `generate_schema.py`（DDL + REGISTRY）→ `maintain.py`（拉取落库）
- 导出：`convert_to_qlib.py` 委托 `qlib_export/`，输出 Qlib bin
- 生成物：`schema.sql` 与 `etl.py` 由脚本生成，不要手改
- 源头：接口行为差异修正 `api_index.json`，策略代码不做特判

---

## 命令

```bash
# 全量建库
python scripts/maintain.py

# 每日盘后更新（重分类+拉缺口+修复+质检）
python scripts/maintain.py --daily

# 自定义历史边界
python scripts/maintain.py --since 20200101 --until 20210101

# 单接口维护
python scripts/maintain.py --api stk_factor_pro

# 质检报告（含 PRAGMA integrity_check）
python scripts/maintain.py --verify

# 清理孤儿表 / 缓存 / VACUUM
python scripts/maintain.py --cleanup
python scripts/maintain.py --cleanup --hard
python scripts/maintain.py --cleanup --vacuum

# 重拉单表单日
python scripts/maintain.py --refresh <API> <DATE>

# 预览策略不拉取
python scripts/maintain.py --dry-run

# Qlib 全量转换（中断自动续转）
python scripts/convert_to_qlib.py

# Qlib 每日增量同步
python scripts/convert_to_qlib.py --daily

# Qlib 重建指定字段
python scripts/convert_to_qlib.py --fields open,high

# 重新生成接口分类 / Schema（积分变化后）
python scripts/classify_apis.py
python scripts/generate_schema.py

# 测试（无需真实数据库）
pytest tests/ -v
```

---

## 架构与依赖规则

```
api_index.json (170 接口全集)
       │
       ▼
classify_apis.py ──→ _project.classification 写回 api_index.json
       │
       ▼
generate_schema.py ──→ schema.sql + REGISTRY 注入 etl.py
       │
       ▼
maintain.py ──→ DataClient ──→ Tushare API ──→ market.db (SQLite WAL)
       │
       ▼
convert_to_qlib.py ──→ qlib_export/ ──→ data/qlib_bin/cn_data (Qlib bin)
       │
       ▼
JsonLogger ──→ logs/maintain_YYYYMMDD.log (JSON Lines)
```

`maintain.py` 通过 `_cmd_*` 分派（`_cmd_verify` / `_cmd_cleanup` / `_cmd_daily` / `_cmd_refresh` / `_cmd_dry_run` / `_cmd_backfill`），
策略统一由 `_dispatch_strategy()` 分派到 `_run_*_strategy` 拉取。

依赖规则：

- `classify_apis.py` 读 `api_index.json` → 写 `_project.classification`，不依赖 database/
- `generate_schema.py` 读 classification → 写 `schema.sql` + 重写 `etl.py`，不依赖 client/
- `maintain.py` 通过 subprocess 调用前两者，拉取后 `importlib.reload(etl)` 热加载 REGISTRY
- `convert_to_qlib.py` 是瘦 CLI，委托 `qlib_export/` 子模块
- `qlib_export/` 通过 `get_conn()` 直连 market.db，不经过 DataClient

---

## Tushare 接口分级

`classify_apis.py` 写入 `api_index.json` 的 `_project.classification`。积分变化后重跑即可重新分类。

| 规则 | 含义 | 建表 | 入 REGISTRY | 调度策略 |
|---|------|:---:|:---:|------|
| 1 | 积分满足，标准频率 | ✅ | ✅ | exchange/offset 分页或单次，动态间隔 |
| 2 | 积分满足，低频 (< 200/min) | ✅ | ✅ | 单次请求，最低 2s 间隔 |
| 3 | 积分不足 | ❌ | ❌ | 不入 schema |
| 6 | 专属付费 | ❌ | ❌ | 完全跳过 |

排除逻辑（不写 classification / 不进 REGISTRY）：
- `is_premium=true` → 规则 6
- `min_points > tushare_points` → 规则 3
- 小时/天级限流 → `usable=false`
- `ts_code` 必选 → 无法批量拉全市场
- `exclude_apis` 列表 → 用户手动排除

### api_index.json 参数修正

文档与实际不一致时，直接修正 `api_index.json` 源头数据，不动策略代码：

| 标记 | 用途 |
|------|------|
| `"required": true` | 文档标可选但实际必传 |
| `"_disabled": true` | 参数文档存在但不可用 |

`_get_date_params` 通过 `active_params`（过滤 `_disabled`）判断策略，无硬编码。

---

## DataClient — Tushare 适配壳

`database/client.py` — `dc.xxx(**params)` → `__getattr__` 动态路由 → `post_request("xxx", ...)` → 按 `_rule_config` 分发策略。

### 调度策略

| 规则 | 行为 | 间隔 |
|------|------|------|
| 1 + exchange 参数 | SH/SZ/BJ 分片，每片 offset 翻页 | `max(floor, rec_ms/1000)` |
| 1 + 无 exchange | offset 翻页 | 同上 |
| 1 + max_rows=None | 单次请求 | 同上 |
| 2 | 单次请求 | `max(2s, rec_ms/1000)` |
| 未注册 API | 降级单次请求（警告） | floor_sec |

### 限速（双重节流）

- **全局 token 级**：`_last_call_global`，确保任意两次请求间最小间隔 `60 / tushare_rate_limit`
- **per-API 级**：`_last_call[api_name]`，低频 API 更长间隔
- 间隔计算：`floor = 60 / tushare_rate_limit`，0.8 安全裕度内置于 `recommended_interval_ms`
- 分页终止条件：返回行数 < `max_rows`
- `_rate_lock` 包裹限速临界区，线程安全

### 天级限流冷却（40203）

- 触发 40203 → 该 API 冷却 24h，存内存 + 持久化到 `cache/tushare/_cooldown.json`
- 进程重启后从磁盘恢复，不会立即重试被封
- 冷却中的 API 直接 `raise DailyLimitError`，跳过网络

### pickle 缓存

按请求参数 `json.dumps(sort_keys=True)` 的 SHA256 缓存到 `cache/tushare/<api_name>/<key>.pkl`。TTL 7 天。`force_refresh=True` 绕过。

### 容错

- HTTPS 传输，`requests.Session` 复用
- 重试上限取 `max_retries` 参数，5s 间隔，timeout 30s
- `TushareError` / `DailyLimitError` 区分业务错误与限流
- 瞬时故障（空 df + 错误码）→ `TushareError`，与正常空返回严格区分

---

## Schema 自动生成

```bash
python scripts/generate_schema.py
```

读 `api_index.json` 的 `_project.classification`，生成：

| 产物 | 内容 |
|------|------|
| `database/schema.sql` | DDL（`IF NOT EXISTS`，幂等） |
| `database/etl.py` | `REGISTRY` 列表注入 |

DDL 生成规则：
- 类型映射：str/None/datetime → TEXT，float → REAL，int → INTEGER
- 主键推断：`ts_code+trade_date` > `ts_code+ann_date` > `trade_date` > `ts_code` > 无主键
- SQL 保留字/数字开头/特殊字符自动加双引号
- 基础设施表（`trade_cal`、`pull_log`）手写 DDL，不被覆盖

---

## 数据维护 — maintain.py

### 运行流程

```
main()
  │
  ├─ 加载 config → DataClient(token) → get_conn()
  ├─ run_start 日志（北京时间 run_id）
  ├─ --verify / --cleanup → 提前返回
  │
  └─ _run_infrastructure(config, conn, dc)    ← daily/backfill 前执行
       ├─ 1. subprocess: classify_apis.py
       ├─ 2. subprocess: generate_schema.py
       ├─ 2.5. 清缓存 + dc.load_rules() + importlib.reload(etl)
       ├─ 3. trade_cal 补拉
       └─ 4. stock_basic / index_basic 刷新
  │
  ├─ --daily    → 逐表补缺口 → 刷新 once-only 表 → 质检 → 自动修复 ok=0
  ├─ --refresh  → 单表单日修复
  ├─ --dry-run  → 打印策略矩阵（无副作用）
  │
  └─ 全量建库   → _run_backfill(since, until) → 质检
```

subprocess 容错：classify/generate 失败时记录 error 日志，降级沿用既有产物，不阻断后续拉取。

### 拉取策略（由 `_get_date_params` 判定）

| 策略 | 触发条件 | 粒度 | pull_log 标记 | 续跑 |
|------|---------|------|:---:|------|
| `trade_date` | 含 `trade_date` 或 `ann_date` | 逐交易日/公告日 | `YYYYMMDD` | 跳过 `ok IN (1,2)` |
| `date_range` | 含 `start_date+end_date`，无 trade_date/ann_date | 按年 | `YYYY` | 同上 |
| `once` | 无日期参数 | 单次 | `__once__` | 同上 |
| `freq` | freq 必选 | 逐交易日×freq | `YYYYMMDD_W/M` | 同上 |
| `domain` | entry 含 `driver` 配置 | 逐域值×日期周期 | `域_周期` | 同上 |

### pull_after 时间门禁

`_resolve_until()` — 北京时间 vs `pull_after` 配置（默认 20:30）：
- 收盘后 → `until = 今天`
- 收盘前 → `until = 昨天`

### 质检报告

| 调用场景 | integrity_check | 行为 |
|---------|:---:|------|
| `--verify` | ✅ | `PRAGMA integrity_check` + 覆盖率报告 |
| 建库 / --daily 末尾 | ❌ | 仅覆盖率报告 |

覆盖分析：交易日历基准对齐 `backfill_since`。domain 表按 `date_mode` 聚合。大缺口 >100 天标注为可能 Tushare 断供。

---

## Qlib 数据转换

`scripts/convert_to_qlib.py`（瘦 CLI）→ `qlib_export/`（8 文件），将 market.db 转换为 Qlib 二进制格式。

### 架构

```
market.db (SQLite)
  │
  ├─ CalendarSync            → calendars/day.txt
  ├─ InstrumentSync          → instruments/all.txt（含退市股）
  ├─ IndexConstituentSync    → instruments/{csi300,...}.txt
  ├─ FeatureSync             → features/<inst>/*.day.bin（全量 + 中断续转）
  ├─ IncrementalSync         → 增量追加新交易日
  ├─ FieldRebuilder          → 按字段维度增删重建
  └─ build_features_manifest → features_manifest.json
```

### TABLE_SPECS 配置驱动

31 张表的字段映射由 `TABLE_SPECS` 声明 + DB schema 动态推导：

| 配置键 | 用途 |
|--------|------|
| `prefix` | 字段统一加前缀 |
| `ohlcv_hfq` | OHLCV 取后复权列 + adj_factor → factor |
| `tech_prefix` | 技术指标加前缀 + 去 `_bfq` |
| `agg` | 聚合策略（sum_count / weighted / split_by_type / count） |
| `encode` | 文本编码（如 ST 状态 → 0/1） |
| `computed` | 计算字段 |
| `virtual_inst` | 市场级虚拟 instrument |

### bin_sync_log 同步状态

```sql
CREATE TABLE bin_sync_log (
    instrument    TEXT NOT NULL,
    source_table  TEXT NOT NULL,
    last_date     TEXT NOT NULL,
    first_date    TEXT NOT NULL DEFAULT '',
    fields_json   TEXT NOT NULL,
    row_count     INTEGER,
    status        TEXT DEFAULT 'done',   -- done / partial / error
    error_msg     TEXT,
    updated_at    TEXT,
    PRIMARY KEY (instrument, source_table)
);
```

- `status='done'` — 已完成
- `status='partial'` — 中断，下次自动清理残留并重试
- `status='error'` — 失败，下次自动重试
- 增量同步可检测：历史回填（向后扩张）、中间填充、字段增删

---

## pull_log 拉取状态

```sql
CREATE TABLE pull_log (
    table_name  TEXT NOT NULL,
    date_val    TEXT NOT NULL,
    ok          INTEGER NOT NULL,   -- 0=失败需重试, 1=成功, 2=确认空, 3=超限放弃
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_try    TEXT DEFAULT NULL,
    PRIMARY KEY (table_name, date_val)
);
```

---

## 运行日志

`database/logger.py` — `JsonLogger`，JSON Lines 格式，北京时间轮转至 `logs/maintain_YYYYMMDD.log`。

| event | 来源 | 内容 |
|-------|------|------|
| `run_start` | main() | run_id, command, since/until |
| `run_end` | main() | elapsed_sec, total_pulls |
| `infra` | _run_infrastructure | classify/generate/stock_basic 步骤状态 |
| `pull` | DataClient + 策略函数 | api, rows, elapsed_ms, cache_hit, ok |
| `error` | DataClient | error_type, error_code, error_msg |
| `summary` | _verify | tables, total_rows, perfect/gap/empty |

查询示例：
```bash
grep '"event":"pull"' logs/maintain_YYYYMMDD.log | jq -r '.ok' | sort | uniq -c
```

---

## 测试

```
tests/test_pure.py           — 23 纯函数单测（infer_pk, _quote_name, date_to_cal_index, _make_key）
tests/test_state_machine.py  — 18 状态机单测（upsert_df, log_pull ok 流转）
tests/test_integration.py    — 11 mock 集成测试（_fetch_with_retry 各分支）
tests/test_regression.py     — 4  回归护栏（TABLE_SPECS 键名一致性）
```

61 tests，不需要真实数据库或 Tushare 连接。

---

## 风格

- Python ≥ 3.12，uv 管理依赖
- 类型标注使用，docstring 最小化
- 代码与注释中不使用 emoji
- 修改任何文件后检查是否影响 `AGENTS.md`
