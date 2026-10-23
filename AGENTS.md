# AGENTS.md — tushare_db

tushare_db：Tushare_Pro → DuckDB → Qlib 数据管道。

- 流水线：`api_index.json` → `classify_apis.py`（分级）→ `generate_schema.py`（DDL + REGISTRY）→ `maintain.py`（拉取落库）
- 导出：`convert_to_qlib.py` 委托 `qlib_export/`，输出 Qlib bin
- 生成物：`schema.sql` 与 `etl.py` 由脚本生成，不要手改
- 源头：接口行为差异修正 `api_index.json`，策略代码不做特判

---

## 收敛规则（开发、审查、修复必须遵守）

本项目已经投入使用，默认处于维护阶段。目标是稳定的数据契约和可验证的正确性。
审查基线与问题台账见 [docs/code-quality-baseline.md](docs/code-quality-baseline.md)。
用户目前直接读取 DuckDB；优先级按实际消费路径判断，启用 Qlib 前单独验收其已登记问题。

### 1. 先确定任务边界

- 开始时检查 `git status`、相关台账和消费路径，保留用户已有改动。审查以工作区现状为准，记录基线；不能只审 HEAD 而忽略未提交配置。
- “审查”默认产出证据、风险判断与关闭条件；功能修复按用户授权执行。“优化”必须说明改善的指标和验收标准。
- 未明确要求时，不增加功能、升级依赖、换架构、全仓格式化、批量重构或引入新工具链。
- 每次修复围绕一个根因或紧密相关的一组契约展开。发现旁支问题登记后继续当前任务；有证据的严重问题必须报告，但不能借此无限扩大修改范围。

### 2. 缺陷准入：没有证据，不进入修复清单

每个缺陷必须给出以下信息，缺一则标为“待验证”，不得宣称已确认或自动修复：

1. 具体触发条件与项目内可到达的调用路径。
2. 违反的契约及其依据（接口元数据、现有明确约定、官方资料或用户需求）。
3. 最小复现或实测证据，包括预期值、实际值和代码位置。
4. 对数据/使用者的影响，以及影响的是 DuckDB、派生表还是 Qlib。
5. 固定问题编号、修复范围和可执行的关闭条件。

分级必须与证据一致：

| 级别 | 判断依据 | 处理 |
|---|---|---|
| P0 | 有证据的正在发生的大范围数据破坏或不可恢复损失 | 立即报告，优先控制影响 |
| P1 | 可复现的丢行、错误计算、错误日期对齐、遗漏更新或失败冒充成功 | 在受影响路径的正确性验收前关闭 |
| P2 | 可复现、影响局部且不改变有效数据结果的运行/诊断问题 | 登记，按授权处理 |
| 可选改进 | 风格、偏好、抽象、潜在性能或假设风险 | 用户未要求时不列为缺陷、不实施 |

- 报警文字、静态分析提示、代码“看起来不够优雅”、没有证明可发生的边界，不能单独构成 P0/P1。
- 业务口径未明确时记录“待确认的契约”，先说明几种选择如何改变结果；不得擅自选择口径并包装成修 bug。
- 不设缺陷数量配额；有新证据就报告，不能为了收敛隐藏严重问题。

### 3. 数值和数据完整性是硬契约

- 涉及数值/导出/聚合的改动，必须明确：行粒度、唯一键、字段单位、复权口径、NULL/零的含义、事件有效日期、修订数据如何传播。
- 主键必须符合接口真实行粒度；不能仅凭“有 ts_code 和 trade_date”断言它们唯一。明细的多笔、多理由、多成分行需要保留；没有可靠唯一键时不能猜键或随意去重。
- 聚合必须明确报告期与方向；不能把同一披露日的多个报告期累计成一份当前持仓，也不能把减持直接当增持相加。加权计算需要说明缺值如何进入分子、分母。
- 单位换算必须从源字段单位推导，禁止跨股票、基金、转债机械套常数。原始字段与复权字段必须可区分，不能将一张表所有数值统一标记为同一复权口径。
- `ann_date`、报告期、除权日、交易日分别说明含义；仅有日期不能证明盘前已知。交易策略可用时点由消费契约决定，不得凭日期对齐宣称无未来信息。
- 正常空返回、完整成功、部分返回、失败必须区分。部分分页不能登记为完整成功，不能用不完整结果替换既有完整分区；计数相同不能证明数值未修订。
- 全量、增量、续跑、重复执行在约定的修订范围内应得到一致数据。日历索引变动必须完整重建或在写入前拒绝，不能只警告后继续混用旧 bin。
- 测试通过、`pull_log ok=1`、`bin_sync_log done`、`--verify` 日期覆盖率，均不能单独作为金融计算正确性或全市场完整性的证明。

### 4. 修复必须闭环，禁止顺手改造

- 已确认的正确性问题先写可失败的行为用例，再作最小修复；预期值用手算、固定样本或独立参考推导，不能调用被测实现生成答案。
- 保存正常对照样例，验证修复没有改变已有正确行为。低影响文档/样式修改不堆砌测试。
- 不通过放宽断言、吞异常、提高重试次数、增加 skip/xfail 或降低阈值使修复“通过”。既有测试若维护了错误口径，必须指出契约冲突并同步纠正。
- 接口差异在 `api_index.json` 修正；生成物通过生成器更新。Schema/REGISTRY 一致并不证明主键正确。
- 主键、约束、类型和分区方式改变后，要验证已有库的实际结构；`CREATE IF NOT EXISTS` 和补列不会迁移旧主键。
- 已落库数据受错误影响时，闭环必须包括：受影响表/日期、旧库迁移、定向重拉或恢复、相关派生表/导出重建及新旧样本核对。仅代码修好不得写“历史数据已修复”。
- 审查与本地测试使用内存库/临时目录，不请求真实 Tushare、不重跑生产维护、不清缓存、不 reset、不迁移生产库。必要抽样用共享锁和只读连接，库忙则跳过并报告；生产写入依据用户实际授权，已有授权不重复索取。
- 先跑针对性用例，再跑 `uv run pytest tests/ -q`；涉及生成链时在临时副本或内存中验证产物。通过后不因没有新证据而反复扩大测试或修改。

### 5. 用固定台账管理审查，明确结束条件

- 后续审查先读台账并运行相关用例。相同根因使用同一编号；不得每轮换标题把同一件事当新发现。
- 状态使用“已确认未修复 / 待验证 / 已关闭 / 已接受限制”，同时写适用路径。只有用户明确接受才能标为已接受限制；暂不使用某模块不等于其缺陷消失。
- 重新打开已关闭问题必须提供新触发条件、回归证据或契约变更。没有这些证据，不再要求同一位置重复“优化”。
- 审查范围内可复现问题已登记、验证结果及未覆盖范围已说明，审查任务即结束。一次修复的约定验收通过、相关旧数据处置状态写明后，修复任务即结束，不追加“还能进一步优化”。
- 当前使用路径的 P0/P1 未关闭时，不能宣称该路径已达到正确性验收；不用追求全仓零警告、零建议或对所有假设输入绝对正确。
- 本轮仅审查留下的 `tests/test_audit_contracts.py` 使用 `strict=True, raises=AssertionError` 的 xfail，是未修复证据，不是正常通过或风险接受。修复对应编号后移除标记、更新台账；不得复制此做法掩盖新代码回归。
- 交付说明必须区分：改了什么、实际验证了什么、还未修复/未验证什么。可以如实写“没有新的已确认缺陷”。

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

# 质检报告（只读，含库大小与覆盖率）
python scripts/maintain.py --verify

# 清理孤儿表 / 缓存 / CHECKPOINT
python scripts/maintain.py --cleanup
python scripts/maintain.py --cleanup --hard
python scripts/maintain.py --cleanup --vacuum

# 重拉单表单日
python scripts/maintain.py --refresh <API> <DATE>

# 只读预览明细旧主键迁移与缓存恢复（--apply 才写库，执行前备份）
python scripts/repair_data_contracts.py --tables block_trade,top_list,kpl_concept_cons --restore-cache --dry-run

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
maintain.py ──→ DataClient ──→ Tushare API ──→ market.duckdb (DuckDB)
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
- `maintain.py` 通过 `scripts/_runtime.py` 调用分类/生成纯函数；先校验候选，再按需发布产物，
  本轮使用同一份配置、接口元数据、Schema 和 REGISTRY，不通过 subprocess/reload 加载
- `convert_to_qlib.py` 是瘦 CLI，委托 `qlib_export/` 子模块
- `qlib_export/` 通过 `get_conn()` 直连 market.duckdb，不经过 DataClient
- `database/engine.py` 是唯一的连接/行访问/元数据/跨进程锁层，业务代码不直接 import duckdb

---

## DuckDB 存储与并发

- 单文件 `data/market.duckdb`；DuckDB 是单写者模型，跨进程不允许读写并存。
- `engine.db_lock()` 用 `data/.market.lock` 文件锁串行化：maintain / convert 取排他锁，
  `--verify` 取共享锁且以 `get_conn(read_only=True)` 打开；`--dry-run` 不连库不取锁。
- 类型：str/None/datetime → VARCHAR，int → BIGINT，float → DOUBLE（不要用 REAL，DuckDB 的 REAL 是 float4）。
- `upsert_df` 用 `register(df) + INSERT [OR REPLACE] INTO ... SELECT TRY_CAST(...)`：
  - 批内重复键按 `keep="last"` 去重（DuckDB 原生保留首行，与 SQLite REPLACE 相反）
  - 无 PK/UNIQUE 的表走普通 INSERT；分区表由 `pre_delete` 在事务内先删后插
  - 数值列用 TRY_CAST，空串/NaN 归一为 NULL
- 元数据：`PRAGMA table_info` 可用；表清单用 `duckdb_tables()`，主键用 `duckdb_constraints()`。
- 空间回收用 `CHECKPOINT`（`VACUUM` 不回收）；彻底压实用 EXPORT/IMPORT 重写。
- `ALTER TABLE ADD COLUMN` 不支持带约束，迁移仅补可空列。
- 当前仅支持 DuckDB 数据库文件，无 SQLite 驱动、读写或迁移路径；连接时仅用文件头校验拒绝误传的旧格式文件。

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
- 分页终止条件：返回行数 < `max_rows`；中页业务错误、重复首页或页数上限
  → 抛 IncompleteFetchError，partial_df 仅供诊断，不完整响应不替换旧分区、不登记成功
- `_rate_lock` 包裹限速临界区，线程安全

### 天级限流冷却（40203）

- 触发 40203 → 该 API 冷却 24h，存内存 + 持久化到 `cache/tushare/_cooldown.json`
- 进程重启后从磁盘恢复，不会立即重试被封
- 冷却中的 API 直接 `raise DailyLimitError`，跳过网络

### pickle 缓存

按请求参数 `json.dumps(sort_keys=True)` 的 SHA256 缓存到 `cache/tushare/<api_name>/<key>.pkl`。TTL 7 天。`force_refresh=True` 绕过。
续跑允许缓存；人工 `--refresh`、daily 的 once/年度主动刷新、基础信息刷新、
空返回复验和失败自动修复显式绕过缓存。公告到期刷新沿用已有绕缓存策略。
不增加历史修订扫描窗口；已完成的日频历史分区仍遵循现有续跑语义。

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
- 类型映射：str/None/datetime → VARCHAR，float → DOUBLE，int → BIGINT
- 主键推断：`_project.pk_override` 优先 > `ts_code+trade_date` > `ts_code+ann_date` > `trade_date` > `ts_code` > 无主键
  （DuckDB 主键即 NOT NULL，SQLite 允许主键为 NULL 的旧行为不再成立）
- dividend 的 ann_date 可为空 → `no_pk + partition_key=ts_code + dedupe_cols`，
  按 ts_code 分区替换并批内去重，保住实施/股东提议等 NULL ann_date 行
- `_project.upsert.null_pk_keep=true` 仅对 SQLite 遗留语义有意义，新库不再使用
- SQL 保留字/数字开头/特殊字符自动加双引号
- 基础设施表（`trade_cal`、`pull_log`）手写 DDL，不被覆盖

---

## 数据维护 — maintain.py

### 运行流程

```
main()
  │
  ├─ 加载 config → DataClient(token) → 取锁
  ├─ daily/backfill: prepare_runtime（内存分类/生成/校验，变化时发布）
  ├─ get_conn（daily/backfill 暂不初始化 schema）
  ├─ run_start 日志（北京时间 run_id）
  ├─ --verify / --cleanup → 提前返回
  │
  └─ _run_infrastructure(config, conn, dc)    ← daily/backfill 前执行
       ├─ 1. 启用已校验快照 + dc.load_rules()
       ├─ 2. init_schema(conn, snapshot.schema_sql)
       ├─ 3. trade_cal 补拉
       └─ 4. stock_basic / index_basic 刷新
  │
  ├─ --daily    → 逐表补缺口 → 刷新 once-only 表 → 质检 → ok=2 复验（3 天内每日重验，超期删除后按表重拉）→ 自动修复 ok=0
  ├─ --refresh  → 单表单日修复
  ├─ --dry-run  → 打印策略矩阵（无副作用）
  │
  └─ 全量建库   → _run_backfill(since, until) → 质检
```

配置失败处理：分类、生成或候选校验失败时，在打开生产数据库前记录失败并终止；
不混用旧产物继续拉取。无变化不重写文件。每个文件原子发布，普通发布异常恢复已发布文件；
不宣称多个文件组成文件系统事务，进程被强杀后下次 daily/backfill 会重新构造并校验产物。
显式 classify/generate CLI 继续保留；不要与维护进程并行编辑或生成配置。

### 拉取策略（由 `_get_date_params` 判定）

| 策略 | 触发条件 | 粒度 | pull_log 标记 | 续跑 |
|------|---------|------|:---:|------|
| `trade_date` | 含 `trade_date` 或 `ann_date` | 逐交易日/公告日 | `YYYYMMDD` | 跳过 `ok IN (1,2)` |
| `date_range` | 含 `start_date+end_date`，无 trade_date/ann_date | 按年 | `YYYY` | 同上 |
| `once` | 无日期参数 | 单次 | `__once__` | 同上 |
| `freq` | freq 必选 | 逐交易日×freq | `YYYYMMDD_W/M` | 同上 |
| `domain` | entry 含 `driver` 配置 | 逐域值×日期周期（`date_mode=once` 时逐域单次全量） | `域_周期` / `域__once__` | 同上 |

`_cmd_daily` 对 `date_mode=once` 的域表不做 `MAX(date_col)` 最新性判断，直接全量 dispatch，
未拉域值（如新股）由 pull_log done 检查自动补齐。dividend 即此模式：`driver.source_table=stk_factor_pro`
（覆盖退市股）+ `force_required=["dividend.ts_code"]`（注意带 api 前缀）逐股全量拉取，
才能拿到按 ann_date 逐日拉取永远丢失的多阶段行与 `ann_date=NULL` 行。
`stk_holdernumber`/`stk_holdertrade`/`pledge_detail` 同款：无主键（`_project.no_pk`）+
`partition_key=ts_code` 分区替换（DELETE 本域旧行再插入），保留同日多公告/无唯一列的明细行。
这五张 domain-once 表通过 driver.refresh_days=7、refresh_batch_size=1000 配置，
daily 按上次尝试时间从旧到新处理到期域和未完成域，每表每轮最多 1000 个；
7 天是最小刷新间隔，实际轮转时间还受域数量、运行频率、预算和冷却影响。
刷新已有记录时强制绕过缓存；backfill 仍保留已完成域的续跑语义。
分页中断/重复首页/页数上限抛 IncompleteFetchError，策略登记 ok=0 并保留旧分区。
DB-001～005 的代码已离线验收，本次生产明细迁移、历史恢复、到期公告轮转、
国家队/养老金派生重建及样本核对已完成。上游新空返回的旧增减持快照已归档，
当前快照与历史归档的消费边界、未覆盖范围和证据见审查台账。

domain 任务使用一次表级完成状态读取替代逐域×周期 COUNT，保持最新周期 ok=2 重验语义。
调度与质检共用 `database/maintenance_report.py:domain_values` 获取驱动域；
只有调度允许空驱动补拉。刷新标记统一在 `_pull_once` 处理，保留普通续跑缓存语义。

domain 周期边界：backfill 用 `--since`，daily 传配置 `backfill_since`（不再传 `None` 全历史扫描）。
驱动表为空时（全新库首轮，驱动表在 REGISTRY 中可能排在本表之后）按驱动表自身策略补拉
`since~until` 后重取域列表，仍为空才跳过并记 error，不再静默结束。

### pull_after 时间门禁

`_resolve_until()` — 北京时间 vs `pull_after` 配置（默认 20:30）：
- 收盘后 → `until = 今天`
- 收盘前 → `until = 昨天`

### 派生表刷新脚本

独立于 maintain.py 运行、不写 pull_log（`--verify` 覆盖率不追踪派生表）：

- `scripts/refresh_national_team_daily.py` — 由 top10_floatholders 构建国家队在榜/退出事件表
- `scripts/refresh_pension_daily.py` — 同款构建养老金组合表

两者共享 `scripts/_derived_tables.py`（缺表预检、stale 交易日携带展开、原子替换）；均依赖 `top10_floatholders` 表，`exclude_apis` 不得排除该接口；派生表刷新需在 `top10_floatholders` 增量更新后手动重跑。

### 质检报告

| 调用场景 | 库大小/CHECKPOINT | 行为 |
|---------|:---:|------|
| `--verify` | - | 只读 `PRAGMA database_size` + 覆盖率报告（不写库） |
| 建库 / --daily 末尾 | ❌ | 仅覆盖率报告 |

覆盖分析：交易日历基准对齐 `backfill_since`。domain-once 单独报告当前域的完成、
到期、待处理、失败、放弃及可确认成功时间，不用公告日期密度判定完整。其余日期覆盖
保持现有统计口径，不证明全市场/字段完整；大缺口 >100 天仍为提示，不能单独触发修复。
`last_try` 在失败后无法提供上次成功时间，报告标明未知，不迁移 pull_log。

---

## Qlib 数据转换

`scripts/convert_to_qlib.py`（瘦 CLI）→ `qlib_export/`（8 文件），将 market.duckdb 转换为 Qlib 二进制格式。

### 架构

```
market.duckdb (DuckDB)
  │
  ├─ CalendarSync            → calendars/day.txt
  ├─ InstrumentSync          → instruments/all.txt（含退市股）
  ├─ IndexConstituentSync    → instruments/{csi300,...}.txt
  ├─ FeatureSync             → features/<inst>/*.day.bin（全量 + 中断续转）
  ├─ IncrementalSync         → 增量追加新交易日
  ├─ FieldRebuilder          → 按字段维度重建
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
    instrument    VARCHAR NOT NULL,
    source_table  VARCHAR NOT NULL,
    last_date     VARCHAR NOT NULL,
    first_date    VARCHAR NOT NULL DEFAULT '',
    fields_json   VARCHAR NOT NULL,
    row_count     BIGINT,
    status        VARCHAR DEFAULT 'done',   -- done / partial / error
    error_msg     VARCHAR,
    updated_at    VARCHAR,
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
    table_name  VARCHAR NOT NULL,
    date_val    VARCHAR NOT NULL,
    ok          BIGINT NOT NULL,   -- 0=失败需重试, 1=成功, 2=确认空, 3=超限放弃
    retry_count BIGINT NOT NULL DEFAULT 0,
    last_try    VARCHAR DEFAULT NULL,
    PRIMARY KEY (table_name, date_val)
);
```

---

## 运行日志

`database/logger.py` — `JsonLogger`，JSON Lines 格式，北京时间轮转至 `logs/maintain_YYYYMMDD.log`。

| event | 来源 | 内容 |
|-------|------|------|
| `run_start` | main() | run_id, command, since/until |
| `run_end` | main() | elapsed_sec, run_stats（本轮 HTTP 尝试/页/缓存命中/任务结果）, pull_log_total（历史状态总量） |
| `infra` | _run_infrastructure | runtime_config/stock_basic 步骤状态及配置摘要 |
| `pull` | DataClient + 策略函数 | api, rows, elapsed_ms, cache_hit；ok 仅策略函数/pull_log 侧事件携带 |
| `error` | DataClient | error_type, error_code, error_msg |
| `summary` | _verify | tables, total_rows, 日期覆盖、freshness、ok_3 |

运行内日志自动带 run_id；任务请求、分页和落库通过 task_id 关联，
请求记录含脱敏 task_params/request_params（含分页参数）。tasks_* 统计维护落库任务尝试，
不等于唯一分区数；基础设施请求计入 HTTP/页数，未新增其 pull_log 记录。
旧 total_pulls 字段改名为 pull_log_total，外部日志消费者需要同步字段名。

查询示例：
```bash
grep '"event":"pull"' logs/maintain_YYYYMMDD.log | jq -r '.ok' | sort | uniq -c
```

---

## 测试

```
tests/test_pure.py            — 纯函数单测（infer_pk, _quote_name, date_to_cal_index, _make_key）
tests/test_state_machine.py   — 状态机单测（upsert_df, log_pull ok 流转）
tests/test_integration.py     — mock 集成测试（_fetch_with_retry 各分支）
tests/test_regression.py      — 回归护栏（TABLE_SPECS 键名一致性）
tests/test_daily_ok2.py       — _cmd_daily ok=2 近期/超期复验 + domain-once 逐域补齐回归
tests/test_client_review.py   — DataClient 审查回归（翻页错误传播、冷却、缓存、节流、守护）
tests/test_db_review.py       — 存储层审查回归（upsert_df 边界、schema 加载、原子写、日志器）
tests/test_maintain_review.py — maintain 审查回归（策略矩阵、分区替换原子性、refresh/infra 边界、空驱动表补拉、daily domain 周期边界）
tests/test_qlib_review.py     — qlib_export 审查回归（bin 格式、增量检测、中断续转、字段重建）
tests/test_schema_gen_review.py — schema 生成链审查回归（分级、主键推断、保留字、REGISTRY 注入）
tests/test_config_review.py   — 配置与辅助脚本审查回归（模板键一致性、派生表原子替换）
tests/test_duckdb_review.py   — DuckDB 引擎回归（Row/Result/锁/事务/可空主键/TryCast/元数据）
tests/test_audit_contracts.py — 固定编号审查证据及独立计算对照（含严格 xfail，见台账）
tests/test_contract_fixes.py — DB-001～005 修复验收与旧库迁移/备份恢复回归
tests/test_core_maintenance.py — DB-006、缓存契约、批量状态查询、配置快照、日志与新鲜度验收
```

测试不需要真实数据库或 Tushare 连接（`pytest tests/` 已配置 pythonpath，裸跑可用）。
数量以实际运行结果为准；必须分别报告 passed / failed / xfailed，不把未修复证据算作通过。

---

## 风格

- Python ≥ 3.12，uv 管理依赖
- 类型标注使用，docstring 最小化
- 代码与注释中不使用 emoji
- 修改任何文件后检查是否影响 `AGENTS.md`
