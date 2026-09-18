# SQLite → DuckDB 全面迁移计划（历史记录）

> 本文档记录迁移方案与执行过程；当前行为以 AGENTS.md 与代码为准。

分支：`duckdb-migration`（已创建，工作区未提交改动已随分支携带）
目标数据库：`data/market.duckdb`（格式切换，不建议继续沿用 `.db` 后缀）
预计总工时：5-7 人日 + 60GB 存量数据迁移机时（数小时）

---

## 1. 目标与范围

### 目标

- 运行时（maintain / qlib_export / 派生表脚本 / 测试）完全去掉 `sqlite3` 依赖，改用 `duckdb` Python API。
- `data/market.db`（当前 63.6GB，48 张表）无损迁移为 DuckDB 单文件库。
- 保留 `pull_log` / `bin_sync_log` 状态，避免重新向 Tushare 全量拉取。
- 对外行为契约（命令、日志事件、Qlib bin 输出、派生视图口径）不变。

### 不在范围

- Tushare 拉取策略、分级规则、Qlib bin 文件格式。
- 派生表（national_team_daily / pension_float_daily）业务口径。
- 下游 ddup adapters 内部实现（但接口迁移需要通知，见 §9）。

---

## 2. 版本与依赖决策

| 项 | 决策 | 理由 |
|---|---|---|
| DuckDB 版本 | 固定 `duckdb>=1.5,<2`（当前实测 1.5.5） | v2.0 预计 2026-10 发布，含新默认存储格式与 C API 破坏性变更；先稳定在 1.5.x |
| Python 包 | `pyproject.toml` dependencies 增加 `duckdb>=1.5,<2` | 唯一新增运行时依赖 |
| 扩展 | `sqlite` 扩展仅用于一次性迁移，运行时不依赖 | 迁移脚本内 `INSTALL sqlite; LOAD sqlite;` |
| 数据库路径 | `data/market.duckdb`；`DDUP_MARKET_DB` 环境变量与 `--db` 参数继续生效 | 通过扩展名/魔数区分，避免误开旧 SQLite 文件 |
| 并发策略 | 跨进程串行化（文件锁）+ 进程内多 cursor | 见 §4.1，SQLite WAL 的读写并发语义无法原样保留 |

---

## 3. 现状清单（SQLite 耦合点）

### 3.1 核心层

| 文件 | 耦合点 |
|---|---|
| `database/utils.py` | `import sqlite3`；`get_conn`（WAL/busy_timeout/`row_factory`）；`init_schema`（`executescript` + `ALTER TABLE` 补列）；`upsert_df`（`PRAGMA table_info`、`INSERT OR REPLACE`、`executemany`、`_bind_value`）；`_quote_ident` |
| `database/etl.py` | `import sqlite3` 类型注解；`log_pull` 使用 `datetime('now','localtime')` + `ON CONFLICT DO UPDATE` |
| `database/schema.py` | 只是读 `schema.sql`，无需改 |
| `database/logger.py` | 无 SQLite 依赖 |

### 3.2 生成链

| 文件 | 耦合点 |
|---|---|
| `scripts/generate_schema.py` | `sql_type`（INTEGER/REAL/TEXT）；`INFRA_DDL`；主键推断（DuckDB PK=NOT NULL，dividend 冲突）；`_quote_name` 关键字表是 SQLite 的 |
| `database/schema.sql` | 生成物；含 `CREATE TABLE IF NOT EXISTS`、视图 DDL（`DROP VIEW + CREATE VIEW`） |

### 3.3 调度层

| 文件 | 耦合点 |
|---|---|
| `scripts/maintain.py` | `sqlite_master`（cleanup）；`PRAGMA integrity_check`；`VACUUM`；`os.path.getsize("data/market.db")`；分区替换 `IN (?,?,...)`（5000+ 占位符）；所有 `conn.execute/fetchone/fetchall` 假定 tuple/Row |
| `scripts/_derived_tables.py` | `sqlite3.connect` 类型注解；`sqlite_master` 预检；`df.to_sql`；`BEGIN/DROP/RENAME` 原子替换 |
| `scripts/refresh_pension_daily.py` | `sqlite3.connect(args.db)`；`sqlite3.Connection` 注解 |
| `scripts/refresh_national_team_daily.py` | 同上 |
| `scripts/convert_to_qlib.py` | `get_conn(db_path)`；`DB_PATH` 常量 |

### 3.4 Qlib 导出层

| 文件 | 耦合点 |
|---|---|
| `qlib_export/specs.py` | `_table_exists` 用 `sqlite_master`；`PRAGMA table_info`；数值列判断 `("REAL","INTEGER")` |
| `qlib_export/sync_log.py` | `sqlite3.OperationalError` 捕获；`dict(r)` 依赖 `sqlite3.Row`；`ALTER TABLE ADD COLUMN` |
| `qlib_export/calendar.py` / `instruments.py` / `features.py` / `incremental.py` | 仅 `sqlite3.Connection` 类型注解，SQL 本身兼容 |
| `qlib_export/binio.py` | 无 DB 依赖 |

### 3.5 测试

9 个测试文件含 `sqlite3`：`test_db_review`、`test_maintain_review`、`test_daily_ok2`、`test_config_review`、`test_state_machine`、`test_schema_gen_review`、`test_qlib_review`、`test_regression`。`test_pure` / `test_client_review` / `test_integration` 不受影响（纯函数/mock HTTP）。

### 3.6 文档

`AGENTS.md`（流水线图、命令、质检表）、`README.md`（"SQLite 数据库存储"、`market.db` 连接示例）、`user_config.template.yaml`（如有 SQLite 相关说明）。

---

## 4. 关键技术差异与对策（基于 DuckDB 1.5.5 实测）

### 4.1 并发模型（影响最大）

实测结论：

- 一个进程以 RW 打开后，其他进程 **RW 或 RO 都打不开**（file lock）。
- 无写者时，多个进程可以同时 RO。
- 同一进程内重复打开同文件：仅允许配置相同的连接，RO 与 RW 混开报 `Can't open a connection to same database file with a different configuration`。
- 进程内多线程可共享一个连接，用 `conn.cursor()` 隔离，写冲突由 MVCC/乐观锁处理。

现状问题：SQLite WAL 允许 `maintain.py`（写）与 `convert_to_qlib.py`（读）并行，迁移后这种并行会直接报锁错误。

对策（推荐组合）：

1. 新增 `database/engine.py::db_lock()`：基于 `fcntl.flock(data/.market.lock)` 的跨进程互斥；
   maintain 与 convert 启动时按需获取（读操作获取共享锁，写操作获取排他锁）。
2. 明确文档与 CLI 提示："DuckDB 单写者，maintain 运行期间 convert 必须等待"。
3. 备选：convert 用 `read_only=True` 且仅在无写者时可运行（锁文件保证时序）。
4. 长远的在线并发需求（如 ddup 实时读）以后再评估 DuckLake/Quack，不进入本次范围。

### 4.2 类型系统与 DDL

实测：DuckDB 严格类型；`REAL` 是 4 字节（float4），`INTEGER` 是 int32。

| Tushare 类型 | SQLite（现状） | DuckDB（目标） |
|---|---|---|
| str / None / datetime | TEXT | VARCHAR |
| int | INTEGER | BIGINT（避免 int32 溢出） |
| float | REAL（8 字节） | DOUBLE（不要用 REAL，会丢精度） |

- `specs.build_field_map` 数值列判断从 `{"REAL","INTEGER"}` 改为 `{"DOUBLE","BIGINT","FLOAT","INTEGER","DECIMAL","HUGEINT"}`。
- 空字符串进数值列、`"1.5"` 字符串等 SQLite 宽容场景，统一在写入 SQL 中 `TRY_CAST(NULLIF(col,'') AS ...)`。
- 现有生成 DDL 的 PK 约束在 DuckDB 实测可执行（当前 schema.sql 56 条语句 0 失败）。

### 4.3 主键语义：DuckDB PK = NOT NULL

实测：`INSERT INTO dividend (... ann_date=NULL ...)` 报 `NOT NULL constraint failed`。
SQLite 的 rowid 表允许 PK 列为 NULL，`dividend` 依赖这一点（`null_pk_keep=true`，ann_date 可为空的多阶段行）。

对策：`dividend` 改为「无主键 + 分区替换」：

- `_project.no_pk = true`
- `_project.upsert.partition_key = "ts_code"`
- 生成 REGISTRY 时带 `partition_key`，`_pull_and_store` 已有的"DELETE 本域再 INSERT"逻辑保证幂等。
- `upsert_df` 对无约束表走纯 `INSERT`（见 4.4），并在 pandas 侧按 `(ts_code, ann_date, div_proc)` 去重 `keep='last'`，防止同批多阶段行重复。
- 迁移旧数据时 dividend 直接平铺插入，NULL ann_date 行得以保留。

其他表 PK 列均来自 ts_code/trade_date/exchange/ann_date 等，经检查不存在可空主键（`null_pk_keep` 仅 dividend 使用）。

### 4.4 `INSERT OR REPLACE` 语义差异

实测：

| 行为 | SQLite | DuckDB 1.5.5 |
|---|---|---|
| 无约束表 `INSERT OR REPLACE` | 等价 INSERT | `BinderException`（必须显式 INSERT） |
| 同批次重复 PK | 后写覆盖（last-wins） | **保留第一条（first-wins）** |
| 列清单未包含的列 | 默认值（行被删除重建） | **保留原值**（列未参与更新） |

对策：

- `upsert_df` 分支：
  - 表有 PK/UNIQUE → `INSERT OR REPLACE INTO ... SELECT ...`；
  - 无约束表 → 普通 `INSERT INTO ... SELECT ...`（`replace_all` 时先 `DELETE`，分区表由调用方 DELETE）。
- 入库前按 PK 列 `df.drop_duplicates(subset=pk, keep="last")`，恢复 last-wins 语义。
- `pull_log` 继续 `ON CONFLICT(table_name,date_val) DO UPDATE SET ok,last_try`：实测能保留未更新的 `retry_count`（这正是原注释的意图，甚至比 SQLite 的 REPLACE 更精确）。

### 4.5 批量写入性能

实测/官方结论：`executemany` 逐行 Python 开销巨大（76K 行约 16s vs DataFrame 注册 0.05s）。

对策：`upsert_df` 全部改为

```python
conn.register("_incoming", df)      # DataFrame 零拷贝注册
conn.execute('INSERT OR REPLACE INTO "t" ("a","b") '
             "SELECT TRY_CAST(\"a\" AS BIGINT), TRY_CAST(\"b\" AS VARCHAR) FROM _incoming")
```

必要时分块（如 50 万行/批）控制内存，但保留事务语义（DELETE + INSERT 同事务）。

### 4.6 SQL 方言差异清单

| SQLite | DuckDB 替代 |
|---|---|
| `conn.executescript(sql)` | `for stmt in duckdb.extract_statements(sql): conn.execute(stmt)`（实测 56 语句可用） |
| `sqlite_master WHERE type='table'` | `duckdb_tables()` 或 `information_schema.tables` |
| `PRAGMA table_info(t)` | 实测可用，字段 `(cid,name,type,notnull,dflt_value,pk)`；复杂场景用 `duckdb_columns()` + `duckdb_constraints()` |
| `datetime('now','localtime')` | 不存在；改为 Python `beijing_now().isoformat()` 绑定参数 |
| `timezone('Asia/Shanghai', now())` | 需要 pytz，避免；统一用 Python 时间 |
| `PRAGMA integrity_check` | 不存在；改为 `CHECKPOINT` + `PRAGMA database_size` 报告 |
| `VACUUM`（回收空间） | `VACUUM` 不回收空间；日常用 `CHECKPOINT`（部分回收），彻底压实用 `EXPORT DATABASE` → `IMPORT DATABASE` 重写 |
| `IN (?,?,...5000)` | 列表参数：`IN (SELECT unnest(?))`，Python 传 list（实测可用） |
| `ALTER TABLE ADD COLUMN ... NOT NULL DEFAULT` | 不支持带约束；补列迁移仅允许可空列，或改为重建表/版本化迁移 |
| `row_factory = sqlite3.Row` | 无；`fetchall()` 返回 tuple。新增 `Row` 包装/工具函数，`dict(r)` 调用点替换 |
| `pd.read_sql_query` / `df.to_sql` | 可用但触发 SQLAlchemy 告警；推荐 `conn.execute(sql).df()` 与 `register + CTAS` |
| 标识符大小写不敏感 | DuckDB 同样大小写不敏感，但保留大小写；继续全量双引号引用 |
| `REAL` = 8 字节 | 见 4.2 |

### 4.7 事务与 DDL

实测 `BEGIN` + `DROP TABLE` + `ALTER TABLE ... RENAME TO` + `COMMIT` 可用，`_derived_tables.atomic_replace` 的结构可保留，仅替换 `df.to_sql` 为 `register + CTAS`。

---

## 5. 目标架构

新增 `database/engine.py`（低层，供全项目复用）：

```
database/engine.py
  ├─ connect(db_path, read_only=False, **cfg)   # duckdb.connect + SET threads/memory/checkpoint
  ├─ get_conn(db_path=None)                     # 兼容旧入口，内部调 utils.get_conn
  ├─ DuckRow / rows_to_dicts(cursor)            # sqlite3.Row 替代
  ├─ table_columns(conn, table)                 # PRAGMA table_info 封装
  ├─ primary_key_cols(conn, table)              # duckdb_constraints() 封装
  ├─ list_tables(conn)                          # duckdb_tables() 封装
  ├─ execute_script(conn, sql)                  # extract_statements 循环
  ├─ checkpoint(conn)                           # CHECKPOINT 封装
  └─ db_lock(path, exclusive=True)              # fcntl.flock 跨进程互斥
```

职责边界：

- `database/utils.py` 保留公共 API（`get_conn`/`upsert_df`/`init_schema`/`load_config`），实现迁移到 engine。
- `qlib_export/*` 的类型注解从 `sqlite3.Connection` 改为 `duckdb.DuckDBPyConnection`（或 `engine.Connection` 别名），行为不变。
- 数据库文件：`data/market.duckdb`；`convert_to_qlib.py --db`、`refresh_*.py --db`、`DDUP_MARKET_DB` 全部换默认值。

---

## 6. 分阶段实施计划

### P0 准备（0.5 天）

- [ ] 分支 `duckdb-migration`（已完成）。
- [ ] `pyproject.toml` 增加 `duckdb>=1.5,<2`，`uv lock`。
- [ ] 迁移前对 `data/market.db` 做 `PRAGMA wal_checkpoint(TRUNCATE)` 并确认无进程占用；保留原文件不动。
- [ ] 记录基线：每张表 `COUNT(*)`、`pull_log` 统计、`bin_sync_log` 行数（供迁移校验）。
- [ ] 设计确认（§10）：目标文件名、dividend 方案、并发策略。

### P1 数据层内核（1-1.5 天）

- [ ] 新增 `database/engine.py`（连接工厂、Row、table 元数据、锁、checkpoint）。
- [ ] 重写 `database/utils.py`：
  - `get_conn`：`duckdb.connect(path)`、`SET threads/memory_limit/checkpoint_threshold`、`init_schema`。
  - `init_schema`：`execute_script`；补列迁移改为"仅补可空列"（`ALTER TABLE ADD COLUMN` 不带约束），带约束的补列直接报错提示重建。
  - `upsert_df`：`register + INSERT [OR REPLACE] INTO ... SELECT CAST(...)`；PK 去重 keep=last；无约束表纯 INSERT；保留 `drop_null_pk`/`replace_all` 语义。
  - `_bind_value` 保留但仅用于非 DataFrame 路径（如有）。
- [ ] `database/etl.py`：`log_pull` 改参数绑定 `last_try`，SQL 用 DuckDB `ON CONFLICT`；类型注解换 `duckdb`。
- [ ] `scripts/generate_schema.py`：
  - `sql_type`：int→BIGINT、float→DOUBLE、其余 VARCHAR。
  - `INFRA_DDL` 类型同步。
  - `infer_pk`/REGISTRY：dividend 无主键 + partition_key；新增 `unique_key_override`（如需，如 dividend 去重键）。
  - `_quote_name` 关键字表换 DuckDB 保留字（可用 `duckdb_keywords()` 对照校验）。
- [ ] 重生成 `schema.sql` + `etl.py`，用 DuckDB 内存库执行全部 DDL 验证（当前 56 语句 0 失败，需保持）。
- [ ] 单测：`test_pure`、`test_state_machine` 迁到 DuckDB 内存库并全绿。

### P2 调度层（1 天）

- [ ] `scripts/maintain.py`：
  - `--cleanup`：`duckdb_tables()` 替换 `sqlite_master`；`VACUUM` 改 `CHECKPOINT`（帮助文案同步）。
  - `--verify`：`integrity_check` 改 `CHECKPOINT` + `PRAGMA database_size` + 各表行数报告；文件大小统计改 `market.duckdb`。
  - `_pull_and_store` 分区 DELETE：`IN (SELECT unnest(?))`。
  - `_write_run_end` / `_done_count` / `_cal_query`：无需改 SQL，但统一用新的游标/行访问。
  - `db_lock`：写模式启动获取排他锁，避免与 convert 冲突。
- [ ] `scripts/_derived_tables.py`：
  - `require_tables` 用 `duckdb_tables()`。
  - `atomic_replace` 用 `register + CTAS`；保留 BEGIN/DROP/RENAME 事务。
  - `pd.read_sql_query` 改 `conn.execute(...).df()`（消除告警）。
- [ ] `scripts/refresh_pension_daily.py` / `refresh_national_team_daily.py`：`get_conn(args.db)`；注解换 DuckDB；两脚本跑通（或至少 dry 验证）。
- [ ] 验收：对 DuckDB 内存库/小样本库执行 `maintain.py --verify` / `--cleanup --dry` 等价路径。

### P3 Qlib 导出层（0.5-1 天）

- [ ] `scripts/convert_to_qlib.py`：`DB_PATH` → `data/market.duckdb`；只读模式 + 共享锁。
- [ ] `qlib_export/specs.py`：`_table_exists`、`PRAGMA table_info` 换成 engine helper；数值类型集合更新。
- [ ] `qlib_export/sync_log.py`：`dict(r)` 换 helper；`ALTER TABLE ADD COLUMN` 迁移逻辑改为可空列或建表即全列；异常类型改 `duckdb.Error`。
- [ ] `qlib_export/*.py`：类型注解替换；确认 `fetchall()` tuple 访问全部正确。
- [ ] 验收：`convert_to_qlib.py --dry-run` 在迁移后库上输出与基线一致（表数/字段数/待转换数）。

### P4 测试迁移（1-1.5 天）

- [ ] `tests/conftest.py`：内存库 fixture（`duckdb.connect(":memory:")` + `init_schema`），替换各文件的 `sqlite3.connect(":memory:")`。
- [ ] 逐文件迁移：`test_db_review`、`test_state_machine`、`test_maintain_review`、`test_daily_ok2`、`test_config_review`、`test_schema_gen_review`、`test_qlib_review`、`test_regression`。
- [ ] 断言更新：`sqlite3.OperationalError` → `duckdb.CatalogException/ConstraintException`；`sqlite_master` → `duckdb_tables()`；`REAL/INTEGER` 类型断言 → `DOUBLE/BIGINT`。
- [ ] `test_config_review` 的落盘种子库（tmp_path/market.db）改为 DuckDB 文件库。
- [ ] 新增回归测试：
  - PK 去重 last-wins；
  - 无约束表纯 INSERT；
  - dividend NULL ann_date 行往返；
  - `db_lock` 互斥（两个进程/线程）；
  - `execute_script` 幂等重放 schema.sql。
- [ ] 目标：261+ 测试全绿，`pytest tests/ -q` 无 SQLite 导入。

### P5 存量 60GB 迁移与验收（0.5 天 + 机时）

见 §7。完成后执行 §8 全部验收项。

### P6 文档与收尾（0.5 天）

- [ ] `AGENTS.md`：架构图 `market.db (SQLite WAL)` → `market.duckdb`；命令 `--vacuum` 说明改 CHECKPOINT；质检表说明；并发限制说明；目录说明。
- [ ] `README.md`：特性描述、连接示例（`import duckdb; duckdb.connect("data/market.duckdb")`）、并发限制。
- [ ] `user_config.template.yaml`：如新增 `duckdb_threads` / `duckdb_memory_limit` 配置项则补充。
- [ ] 清理 `grep -rn "sqlite" --include="*.py"` 为 0；`data/market.db*` 归档或保留为 `.bak`。

---

## 7. 存量 60GB 数据迁移方案

### 7.1 准备

```bash
# 1. 确认没有进程占用旧库
lsof data/market.db
# 2. SQLite WAL 落盘（用 sqlite3 CLI）
sqlite3 data/market.db "PRAGMA wal_checkpoint(TRUNCATE);"
# 3. 记录基线（行数 + pull_log/bin_sync_log 统计）
```

### 7.2 路径 A（推荐）：SQLite 扫描器 + 目标 schema 建表 + INSERT SELECT

已实测：`ATTACH 'data/market.db' AS sdb (TYPE sqlite, READ_ONLY)` 成功，48 张表可见，`sqlite` 扩展可自动安装。

新增一次性脚本 `scripts/migrate_sqlite_to_duckdb.py`：

1. 目标库执行新 `schema.sql`（含 dividend 新 DDL）。
2. `ATTACH` 旧库（READ_ONLY）。
3. 按 `duckdb_tables()` 遍历旧表；对每张目标表：
   - 从 `duckdb_columns()` 生成显式列清单 + `TRY_CAST`（按目标 DDL 类型），
   - `INSERT INTO target SELECT <casts> FROM sdb.<table>`，
   - 分批（如 50 万行）并支持 `--resume`（记录已完成表）。
4. 跳过 SQLite 视图（新 schema 会重建 dividend_grid 等 5 个视图）。
5. `pull_log` / `bin_sync_log` 必须迁移，保留增量状态。
6. 结束后 `CHECKPOINT`；对比每表行数与基线；抽样校验（如 `stk_factor_pro` 的 `SUM(vol)`、日期边界）。
7. 验收通过后旧库改名 `data/market.db.bak`（不删除），更新 `DDUP_MARKET_DB`/文档。

性能与资源：

- `SET threads=8; SET memory_limit='16GB'; SET preserve_insertion_order=false;`
- 预计运行数小时；DuckDB 列存压缩后目标文件预计 15-35GB，当前磁盘余量 108GB 足够。
- 中途失败可直接重跑（`INSERT` 前 `DELETE` 目标表或按表标记）。

### 7.3 路径 B（备选）：CTAS 全表 + 后补约束

`CREATE TABLE AS SELECT` 后无法补 PK/约束（DuckDB 不支持），需要重建表，且股息 NULL 主键问题仍需处理。仅在路径 A 受阻时使用。

### 7.4 路径 C（不推荐）

重新从 Tushare 全量拉取：受积分/频率限制，60GB 预计数天到数周，且丢失 `pull_log` 进度语义。仅当旧库损坏且无备份时考虑。

---

## 8. 验收标准

| # | 验收项 | 判定 |
|---|---|---|
| 1 | 代码无 SQLite | `grep -rn "import sqlite3\|sqlite_master\|sqlite3\.\|datetime('now'" --include="*.py"` 仅迁移脚本允许 |
| 2 | 单测 | `pytest tests/ -v` 全绿（≥261，含新增回归） |
| 3 | Schema 幂等 | DuckDB 内存库重复执行 `init_schema` 两次无报错 |
| 4 | 数据一致 | 迁移后每表行数与基线一致；`pull_log` ok=1/2/0 计数一致；`bin_sync_log` 行数一致 |
| 5 | 业务抽样 | dividend NULL ann_date 行数一致；5 个派生视图可查；`trade_cal` 边界一致 |
| 6 | 质检验证 | `python scripts/maintain.py --verify` 输出覆盖率与 SQLite 基线同档 |
| 7 | Qlib 转换 | `convert_to_qlib.py --dry-run` 与基线统计一致；`--daily` 跑通 |
| 8 | 并发行为 | maintain 持锁时 convert 明确等待/报错提示，不会静默损坏 |
| 9 | 性能 | `--daily` 全流程耗时不高于 SQLite 基线的 1.5 倍（首日观察） |
| 10 | 文档 | AGENTS/README 无 SQLite 过时描述 |

---

## 9. 风险与回滚

| 风险 | 等级 | 缓解 |
|---|---|---|
| 60GB 迁移中断/磁盘不足 | 中 | 旧库只读附加、不改动；迁移脚本支持按表重跑；先跑 `--table` 小表验证 |
| 并发语义变化导致误用 | 高 | 文件锁 + 文档 + 启动日志提示；convert 改只读 |
| dividend 无主键后重复行 | 中 | `partition_key=ts_code` 分区替换 + pandas 按事件键去重；迁移后校验行数 |
| DuckDB 未覆盖的隐藏 SQLite 方言 | 中 | 全量 grep + 测试兜底；`test_config_review` 增加全库脚本冒烟 |
| `ALTER TABLE` 迁移能力弱 | 低 | 本次为一次性格式切换；今后 schema 变更走"重建表 + INSERT SELECT"模式 |
| DuckDB 2.0 存储格式变更 | 低 | 固定 `<2`；2.0 稳定后再评估升级（官方提供升级说明） |
| 下游 ddup adapters 直连 market.db | 高 | 提前通知：需改用 duckdb 驱动/`get_conn`；派生视图契约不变；提供一周双格式并行期 |
| 60GB 上 PK/ART 索引占用与加载时间 | 中 | 迁移时先建表后插入；必要时对超大表评估是否保留 PK（`stk_factor_pro` 等） |

回滚：保留 `data/market.db` 原文件与对应代码 tag（迁移前 `git tag pre-duckdb`），代码回退分支即可恢复 SQLite 运行；DuckDB 文件独立，互不污染。

---

## 10. 待确认决策

1. 目标文件名：`data/market.duckdb`（推荐）还是沿用 `market.db`？
2. 并发策略：文件锁串行化（推荐）还是需要"在线并发读"的其他方案（DuckLake/快照导出）？
3. dividend 方案：无主键 + 分区替换（推荐，保住 NULL ann_date 行）是否接受？
4. 版本固定：`duckdb>=1.5,<2`（推荐）是否接受？
5. 下游项目（ddup adapters 等）的迁移窗口与负责人？

---

## 11. 总任务清单

- [x] P0 依赖/基线/决策确认
- [x] P1 `database/engine.py` + `utils.py` + `etl.py` + `generate_schema.py` + 重生成产物
- [x] P2 `maintain.py` + `_derived_tables.py` + `refresh_*.py`
- [x] P3 `convert_to_qlib.py` + `qlib_export/*`
- [x] P4 全部测试迁移 + 新增回归
- [x] P5 `migrate_sqlite_to_duckdb.py` + 60GB 数据迁移 + 验收
- [x] P6 AGENTS/README/配置文档更新 + 旧库归档

---

## 12. 执行记录（2026-09-18）

按推荐默认值落地：目标 `data/market.duckdb`；并发采用文件锁串行化；
dividend 改为 `no_pk + partition_key=ts_code + dedupe_cols`；版本固定 `duckdb>=1.5,<2`（实测 1.5.5）。

迁移结果：

| 项 | 结果 |
|---|---|
| 源库 | `data/market.db` 63.6GB（保持原样，未改动） |
| 目标库 | `data/market.duckdb` 18.8GB（约 3.4x 压缩） |
| 迁移表 | 46/46 行数校验全部一致 |
| 派生表 | `national_team_daily` / `pension_float_daily` 由刷新脚本在 DuckDB 重建 |
| pull_log | ok=1:83584 / ok=2:11121 / ok=0:0，与源库完全一致 |
| dividend | 183,033 行，其中 ann_date IS NULL 5,917 行完整保留 |
| stk_factor_pro | 10,003,413 行，日期边界 20180102 ~ 20260917 一致 |
| 质检 | `maintain.py --verify`：44 表 / 95,831,359 行 / 17.7GB，无新增缺口 |
| Qlib | `convert_to_qlib.py --dry-run` 正常；`--table gz_index` 真实转换通过 |
| 测试 | 293 passed（含 test_duckdb_review.py 22 项 DuckDB 专项回归） |

备注：

- 迁移进度文件保留在 `data/.market.migrate_progress.json`（断点续迁用），可随旧库一起归档。
- 旧库建议验收稳定一周后再改名 `market.db.bak` 或移出项目目录。
- DuckDB 2.0（2026-10）会切换默认存储格式，升级前需评估 `EXPORT/IMPORT` 重写。

---

## 13. 四路子代理 TDD 审查与修复记录（2026-09-18）

审查分四路：SQLite 残留扫描、DuckDB 性能特性、正确性与并发、对抗性边界探测。
共 308 tests 全绿（新增 15 项回归）。已修复：

| 等级 | 问题 | 修复 |
|---|---|---|
| P0 | `--cleanup` 会把派生表 `national_team_daily` / `pension_float_daily` 当孤儿表删除 | 加入 `INFRA_TABLES`，补回归测试 |
| P1 | 接口新增字段后，已有表不会补列且 `upsert_df` 静默丢列 | `init_schema` 解析 schema.sql 逐表补列（恢复 DEFAULT/NOT NULL）；`upsert_df` 对丢弃列告警 |
| P1 | `append_bin` 遇损坏 bin 用增量片段重建，静默丢历史且永不回补 | 改抛 `CorruptBinError`，`IncrementalSync` 捕获后整票全量重转 |
| P1 | 迁移 `--resume` 信任陈旧进度（目标被删也报"完成"）；进度文件与 target 解耦且非原子写 | 完成判定校验目标实际行数；进度文件跟随 target；`atomic_write_text`；失败退出码 1 |
| P1 | 宽表逐 instrument N+1 查询（`is_synced` 点查、每票两次 MIN/MAX 全表扫描、daily 逐条 MIN） | `load_synced_fields` 一次批量；`_convert_instrument` 直接从已取行返回日期边界；`_load_min_dates` 一次 GROUP BY。`--dry-run` 由分钟级降到 5s |
| P2 | `refresh_*_daily.py` 绕过 `db_lock`，与 maintain/convert 并发会触发 DuckDB 文件锁错误 | 包 `engine.db_lock(exclusive=True)` |
| P2 | `--dry-run` 实际会建库并取排他锁 | 完全不连库、不取锁 |
| P2 | `Result` 惰性：后续 execute 后消费旧 `Result` 会静默错行/错列 | Result 记录序号，跨 execute 消费显式 RuntimeError |
| P2 | `primary_key_cols`/`unique_constraints` 未限定当前库；`table_columns` 对带引号表名静默返回空 | 改用 `duckdb_columns()` + `current_database()` 过滤 |
| P2 | 旧版 `pull_log` 补列后 `retry_count` 为 NULL，daily 修复循环 TypeError | 补列后回填 DEFAULT 并恢复 NOT NULL |
| P2 | `Row` 缺键抛 `ValueError`（sqlite3.Row 为 IndexError）；无相等性 | 对齐 IndexError + `__eq__` |
| P2 | 只读打开缺失库/内存库 read_only 语义不清 | 明确友好错误 |
| P2 | `atomic_replace` DROP/RENAME 失败无回滚 | 加 try/except rollback |
| P2 | 迁移 `--force` 不清理 DuckDB 的 `.wal`；`chunksize` 死参数；pyproject/文档过时描述 | 清理后缀、删死参数、文档对齐 |

### 未实施的性能跟进项（已基准验证，风险中高，留待独立改动）

1. `qlib_export` 按 `(ts_code, trade_date)` 物理聚簇重写：实测宽表逐票查询 83ms → 7ms（~12x），
   需停机窗口重写 ~18GB 数据；或改为周期 `EXPORT/IMPORT ORDER BY` 重整。
2. `carry_expand` 改 DuckDB `JOIN cal` 区间展开（pension 3.28s → 0.54s，逐行等价已验证）。
3. `IndexConstituentSync.full_sync` 改 SQL `LAG/SUM OVER` 分组（7.2s → 1.14s）。
4. `TRY_CAST` 静默 NULL 的数据质量审计（`inf`/`nan`/BIGINT 四舍五入），建议迁移脚本增加可选列级校验。
