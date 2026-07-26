# tushare_db

Tushare_Pro A 股数据建库与维护工具，SQLite 数据库存储，支持 Qlib 格式转换。

- 接口分级：按积分、频率与权限自动分类，不可用接口自动排除
- 拉取调度：exchange 分片 + offset 分页，token 与 API 双重限速，限流冷却持久化到磁盘
- 断点续跑：`pull_log` 记录每次拉取状态，中断后自动续跑，失败与空数据严格区分
- 日常维护：盘后时间门禁控制更新窗口，质检报告覆盖缺口分析，失败自动修复
- Qlib 导出：SQLite 转为 Qlib 二进制，支持增量同步与字段级重建

> **重要声明：** 本软件是 Tushare 数据落地工具，不提供任何投资建议。所有数据来自 Tushare 开放平台，数据完整性取决于接口权限与积分等级。使用者须自行遵守 Tushare 平台使用协议。作者与贡献者不为因使用本软件而产生的任何数据偏差、交易损失或合规问题负责。

## 快速开始

```bash
# 1. 安装依赖
uv sync

# 2. 配置
cp user_config.template.yaml user_config.yaml
# 编辑 user_config.yaml：填入 tushare_token，设置 backfill_since 和 tushare_points

# 3. 全量建库
python scripts/maintain.py

# 4. 每日盘后更新（可加入 crontab）
python scripts/maintain.py --daily
```

---

## 命令

| 命令 | 用途 |
|---|---|
| `python scripts/maintain.py` | 全量建库（since=backfill_since，until 自动判定） |
| `python scripts/maintain.py --daily` | 每日增量更新（重分类+拉缺口+修复+质检） |
| `python scripts/maintain.py --verify` | 质检报告（含完整性扫描） |
| `python scripts/maintain.py --cleanup` | 清理孤儿表 |
| `python scripts/maintain.py --cleanup --hard` | 同上 + 清理 pickle 缓存 |
| `python scripts/maintain.py --cleanup --vacuum` | 同上 + VACUUM 回收磁盘 |
| `python scripts/maintain.py --refresh API DATE` | 重拉单表单日 |
| `python scripts/maintain.py --api <name>` | 只维护单个接口 |
| `python scripts/maintain.py --since 20150101` | 自定义起始日期 |
| `python scripts/maintain.py --dry-run` | 预览策略不拉取 |

### 每日自动维护

建库完成后，加入 crontab 即可无人值守运行。脚本内置收盘时间门禁（`pull_after`，默认 20:30 北京时间），盘中运行自动跳过当日。

```bash
# 每个交易日 21:00 执行每日更新
0 21 * * 1-5 cd /path/to/tushare_db && python scripts/maintain.py --daily >> logs/cron.log 2>&1
```

### Qlib 转换

```bash
python scripts/convert_to_qlib.py                  # 全量转换（中断自动续转）
python scripts/convert_to_qlib.py --daily           # 每日增量同步
python scripts/convert_to_qlib.py --reset           # 清除 bin 及同步状态，从头全量
python scripts/convert_to_qlib.py --dry-run         # 预览待转换统计
python scripts/convert_to_qlib.py --fields open,high  # 仅重建指定字段
python scripts/convert_to_qlib.py --table stk_factor_pro  # 仅转换指定表
```

---

## 能力一览

| 能力 | 怎么用 |
|---|---|
| 自动建库 | `python scripts/maintain.py`，自动生成 40+ 张表 |
| 接口分级 | 按积分/频率/权限自动分类（规则 1/2/3/6），开箱即用 |
| 智能分页 | exchange 分片 (SH/SZ/BJ) + offset 翻页 |
| 双重限速 | 全局 token 级 + per-API 级，内置 0.8 安全裕度 |
| 增量续跑 | `pull_log` 表记录状态 (ok=0/1/2/3)，中断自动续跑 |
| 天级限流冷却 | 触发 40203 后 24h 冷却，持久化到磁盘，跨进程生效 |
| 收盘时间门禁 | `pull_after` 配置（默认 20:30），自动判定 until 日期 |
| Qlib 转换 | SQLite 自动映射为 Qlib bin 格式，字段映射由 TABLE_SPECS 驱动 |
| 中断续转 | `bin_sync_log` 记录同步状态，中断后跳过已完成项 |
| 指数成分股 | 从 `index_weight` 生成各指数成分股存续期清单 |

---

## 接口积分规则

接口按 Tushare 积分和频率限制自动分类：

| 规则 | 含义 | 建表 | 入 REGISTRY |
|---|------|:---:|:---:|
| 1 | 积分满足，标准频率 | ✅ | ✅ |
| 2 | 积分满足，接口强制低频 (< 200/min) | ✅ | ✅ |
| 3 | 积分不足 | ❌ | ❌ |
| 6 | 专属付费 | ❌ | ❌ |

Tushare 积分需要在文件 `user_config.yaml` 内进行设置，根据积分等级划分，满足规则 1 2 的接口将会加入建库拉取目标列表

规则 3 不等于接口完全不可用，低积分用户通常仍保留少量日调用配额。对于单次请求即可覆盖全量的接口，可在额度内完成拉取，`api_index.json` 中手动标记 overdraft 后自动纳入建表拉取项。`api_index.json` 是 Tushare 接口的关键索引文件，无特殊情况不建议修改此文件内容。

建库不会拉取的内容：
* `ts_code` 为必选参数的接口不会拉取，因为无法批量拉全市场
* `exclude_apis` 列表内登记的接口即使满足积分要求也不会拉取，不需要的数据可以在此登记实现屏蔽节省数据库空间占用

---

## 下游消费

外部项目直接连接 `data/market.db`：

```python
from database.etl import REGISTRY
from database.utils import get_conn

conn = get_conn("data/market.db")
for entry in REGISTRY:
    print(entry["table"], entry.get("date_col"))
```

---

## 目录

```
tushare_db/
├── api_index.json              # 接口全集（170 API，含 _project.classification）
├── user_config.template.yaml   # 配置模板
├── pyproject.toml              # 项目依赖
├── database/
│   ├── client.py               # DataClient（HTTPS + 缓存 + 分页 + 限速）
│   ├── utils.py                # 连接、upsert、配置、时间工具
│   ├── etl.py                  # REGISTRY（自动生成）
│   ├── schema.py               # DDL 加载
│   └── schema.sql              # DDL（自动生成）
├── scripts/
│   ├── maintain.py             # 建库 / 日更 / 清理 / 质检
│   ├── classify_apis.py        # 接口分级
│   ├── generate_schema.py      # Schema + REGISTRY 生成
│   └── convert_to_qlib.py      # Qlib 转换 CLI 入口
├── qlib_export/                # Qlib 转换引擎子模块
│   ├── specs.py                # TABLE_SPECS + 字段映射
│   ├── sync_log.py             # bin_sync_log 状态机
│   ├── calendar.py             # 交易日历同步
│   ├── instruments.py          # 品种清单 + 指数成分股
│   ├── binio.py                # bin 原子读写
│   ├── features.py             # FeatureSync 全量转换
│   └── incremental.py          # IncrementalSync + FieldRebuilder
├── tests/                      # 61 tests, pytest
│   ├── test_pure.py            # 纯函数单测
│   ├── test_state_machine.py   # 状态机单测
│   ├── test_integration.py     # mock 集成测试
│   └── test_regression.py      # 回归护栏
└── data/                       # market.db（不入 git）
```

---

## 测试

```bash
pytest tests/ -v    # 61 tests，无需真实数据库
```
