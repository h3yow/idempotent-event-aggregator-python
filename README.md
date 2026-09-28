# 幂等事件聚合器（本地 SQLite）

在本地 SQLite 之上提供埋点/指标类事件的**幂等写入**与**聚合查询**，保证：

- 重复提交不重复计数；迟到事件可回补，已结算窗口不可改写；
- 并发读写下计数不重不漏，写入中途失败整体回滚；
- 事件结构可兼容演进，非法写入给出可区分的拒绝原因。

**不依赖任何外部服务或新凭据**：仅使用 Python 标准库 `sqlite3` + FastAPI；
数据库默认是本地文件 `events.db`，鉴权令牌为可选的本地环境变量。

## 快速开始

```bash
uv sync
uv run pytest -s                      # 运行全部测试（-s 查看输入与判定日志）
uv run python main.py                 # 启动 HTTP 服务（随机端口）
# 可选：为写接口启用令牌
EVENT_WRITE_TOKEN=tok-123 uv run python main.py
```

配置（均为环境变量，带本地默认值）：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `EVENT_DB_PATH` | `events.db` | SQLite 文件路径，`:memory:` 仅用于测试 |
| `EVENT_WINDOW_MS` | `60000` | 固定窗口大小（毫秒） |
| `EVENT_SETTLE_LAG_MS` | `5000` | 水位线滞后，窗口关闭后的迟到宽限 |
| `EVENT_WRITE_TOKEN` | 空 | 设置后写请求必须携带匹配的 `X-Write-Token` |

## HTTP 接口

| 方法/路径 | 说明 |
| --- | --- |
| `POST /events` | 提交单个事件 |
| `POST /settle` | 按当前时间推进水位线，结算已关闭窗口 |
| `GET /aggregates?event_type=&subject=&settled_only=true` | 查询聚合计数 |
| `GET /health` | 健康检查 |

事件体示例：

```json
{
  "event_id": "evt-1",
  "event_type": "purchase",
  "subject": "user-42",
  "ts_ms": 1730000000000,
  "payload": {"amount": 99.5, "coupon": "SAVE10"}
}
```

响应：`{"accepted": true, "event_id": "evt-1", "deduped": false, "bucket": ".../60000"}`。
重复提交返回 HTTP 200 且 `deduped=true`（重复不是错误），并返回**首次**写入的窗口。

## 幂等与窗口语义

- **幂等键**：`events.event_id` 为主键。重复提交最终由数据库主键冲突兜底，
  即使应用层预判与插入之间存在竞争，也绝不会计两次。
- **固定窗口（tumbling）**：事件按**事件时间** `ts_ms` 归入
  `floor(ts_ms / WINDOW_SIZE_MS)` 对应的窗口，而非到达时间，因此乱序/迟到
  事件仍落入正确窗口。
- **迟到回补**：窗口关闭后保留 `SETTLE_LAG_MS`（默认 5s）宽限。窗口起始时刻
  满足 `start + WINDOW_SIZE_MS <= now - SETTLE_LAG_MS`（即被水位线越过）之前，
  迟到事件都可以回补。
- **结算不可变**：窗口一旦登记进 `settled_windows`（显式结算或水位线越过），
  之后任何写入都以 `WINDOW_SETTLED` 拒绝，已结算的聚合结论不再改变、可复现。
- **同键不同内容**：相同 `event_id` 但字段被篡改时，按首次提交结果去重，
  不覆盖原记录，差异会写入日志。

## 并发一致性保证

- **单写者全序**：每次写入使用 `BEGIN IMMEDIATE` 立即获取保留锁，并发写请求
  被串行化为确定顺序；配合 `busy_timeout=30s`，竞争时等待而非报错，
  `INSERT ... ON CONFLICT DO UPDATE SET cnt=cnt+1` 在锁保护下做原子行加法，
  不重不漏（见 `tests/test_concurrency.py`，含两个独立连接写同一文件库）。
- **事务原子性**：`查重 → 插事件 → 更新计数` 在同一事务内提交；任一步失败
  立即 `ROLLBACK`，不会留下“事件已存但计数未加”的中间态，失败请求可安全重试
  （见 `tests/test_transaction.py`）。
- **快照读**：SQLite WAL 模式下聚合查询走一致性快照，与写入并发时读到的始终
  是某个已提交版本，不会读到半个事务。
- **顺序可解释**：同 `event_id` 的胜负由获取写锁的全序决定（先到者写入，
  其余 `deduped`）；跨窗口顺序由事件时间决定，与到达顺序无关。

## 结构演进与错误分类

事件类型在注册表 `app/validation.py` 中声明版本与字段：

- **新增字段必须是带默认值的可选字段**。旧生产者缺省该字段时按默认值归一化
  （如 `purchase.v2` 新增 `coupon` 默认 `""`，`click.v1` 的 `amount` 默认 `0`），
  旧数据与新数据混存时计数与求和口径保持稳定；未声明的额外字段原样保留。
- 归一化后的完整记录连同 `schema_version` 一起落库，历史数据重读结论可复现。

拒绝原因（响应体 `reason`，不静默丢弃任何记录）：

| reason | HTTP | 触发条件 |
| --- | --- | --- |
| `UNAUTHORIZED` | 401 | 配置了令牌但 `X-Write-Token` 缺失/错误 |
| `MISSING_FIELD` | 422 | 信封或业务必填字段缺失 / 为 null |
| `TYPE_MISMATCH` | 422 | 字段类型不符（如 amount 传字符串、ts_ms 传布尔） |
| `BAD_PAYLOAD` | 422 | 请求体不是对象、ts_ms 为负等 |
| `UNKNOWN_EVENT_TYPE` | 422 | 提交了未注册的事件类型 |
| `WINDOW_SETTLED` | 409 | 写入目标窗口已结算 |
| `DUPLICATE` | 200 | 重复 event_id（以 `deduped=true` 表达，非错误） |

## 本地验证

```bash
uv run pytest -s                      # 全部 28 个用例，-s 打印每条用例的输入与判定依据
uv run pytest tests/test_concurrency.py -s   # 只看并发一致性
uv run pytest tests/test_schema_evolution.py -s  # 只看结构演进与错误分类
```

测试矩阵覆盖：重复提交（含同键篡改）、乱序与迟到回补、水位线边界、
结算后拒绝、多线程/多连接并发、事务回滚、新旧结构混存、八类拒绝原因、
HTTP 状态码与鉴权。

## 代码结构

```
app/
  errors.py       错误枚举与异常分类
  models.py       请求/响应模型
  config.py       本地环境变量配置
  schema.py       SQLite 连接、PRAGMA 与建表
  validation.py   类型注册表、结构演进与字段校验
  windows.py      固定窗口、水位线与结算
  dedup.py        幂等查询辅助
  repository.py   事件插入与计数/聚合 SQL
  service.py      事务、鉴权与编排（并发语义核心）
  api.py          FastAPI 路由与错误码映射
tests/            按场景组织的 28 个用例
```
