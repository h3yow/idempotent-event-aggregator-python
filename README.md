# Idempotent Event Aggregator (本地版)

一套基于**本地 SQLite 数据库**的埋点/指标事件摄取与聚合服务，不依赖任何
外部服务，也不需要任何新凭据。它解决四类问题：

1. **写入幂等**：同一事件重复提交不会重复计数；迟到事件可在窗口开放时
   回补；已经结算的窗口不能被改写；越权/非法写入按明确原因拒绝。
2. **并发一致性**：并发读写时计数不会因竞争重复或漏计；批量写入要么全
   部成功要么整体回滚。
3. **结构兼容演进**：事件结构新增字段或字段缺省时，旧数据与新数据混存
   下的聚合结论稳定、可复现；缺失字段不报错、类型不匹配明确拒绝。
4. **可观测的失败分类**：每个拒绝都带稳定的机器可读 `error` 码和
   `details.reason`，测试日志打印输入与判定依据。

## 快速开始

```bash
uv sync
uv run uvicorn main:app --reload          # 启动，默认使用本地 events.db
uv run pytest -q                          # 运行全部测试（26 个用例）
```

默认令牌来自随项目提供的本地文件 `app/tokens.json`（仅用于本地验证，
不是真实凭据）：

| Token                   | 租户           | 权限           |
|-------------------------|----------------|----------------|
| `local-token-tenant-a`  | `tenant-a`     | 写事件/查聚合  |
| `local-token-tenant-b`  | `tenant-b`     | 写事件/查聚合  |
| `local-token-admin`     | `tenant-admin` | 结算（settle） |

可用环境变量覆盖：`EVENT_DB_PATH`（数据库文件路径）、
`EVENT_WINDOW_SECONDS`（窗口长度，默认 3600）、`EVENT_TOKENS_PATH`
（令牌 JSON 路径）。

## HTTP 接口

所有业务接口都需要 `Authorization: Bearer <token>` 头；租户身份**只**来自
令牌，请求体里的租户字段永远不被信任。

### `POST /events`

请求体可以是单个 JSON 对象或 JSON 数组（数组为**原子批量**）。

```bash
curl -s -X POST localhost:8000/events \
  -H "Authorization: Bearer local-token-tenant-a" \
  -H "Content-Type: application/json" \
  -d '{"event_id":"evt-1","event_type":"click","occurred_at":1727000000000}'
```

响应：

```json
{ "status": "accepted", "inserted": 1, "inserted_ids": ["evt-1"], "duplicates": [] }
```

幂等重放（`event_id` 相同且载荷完全一致）返回 200，但 `inserted=0`，
并在 `duplicates` 中给出 `{"event_id": "...", "reason": "already_stored"}`。

### `POST /admin/settle`（仅 admin）

```bash
curl -s -X POST localhost:8000/admin/settle \
  -H "Authorization: Bearer local-token-admin" \
  -H "Content-Type: application/json" \
  -d '{"watermark_ms": 1727003600000, "tenants": ["tenant-a"]}'
```

省略 `tenants` 时对所有非 admin 租户生效。重复结算同一水位是幂等的；
试图把水位往回退会被拒绝（`watermark_regression`）。

### `GET /aggregates`

查询参数（均可省略）：`event_type`、`start_ms`、`end_ms`（按窗口起点
过滤）。每个窗口返回 `count`、`sum_value`、`schema_versions`、`state`
（`settled` / `open`）与 `settled_at`。

## 幂等语义

* 幂等键是复合主键 **`(tenant, event_id)`**。同一租户内 `event_id`
  唯一；不同租户使用相同 `event_id` 互不影响（天然的租户隔离）。
* 幂等判定比较的是**规范化后的完整载荷**：
  * 同 id + 同载荷 ⇒ 幂等成功，不重复计数（即使窗口已结算也只回
    `duplicate_event/already_stored`，绝不触碰冻结快照）。
  * 同 id + 不同载荷 ⇒ 拒绝 `duplicate_event/event_id_conflict`。
  * 同一批量内出现重复 id ⇒ 拒绝 `duplicate_event/duplicate_in_batch`，
    整批回滚（无法确定调用方意图，保守失败）。
* 批量请求内包含“已存在的同载荷事件”属于正常重放：新事件照常写入，
  重放事件计入 `duplicates`，不触发回滚。

## 窗口与迟到事件语义

* 窗口是**固定大小、按 epoch 对齐**的：
  `window_start = occurred_at // WINDOW_SECONDS * WINDOW_SECONDS`。
  `occurred_at` 由客户端提供（unix 毫秒），乱序到达本身不影响归属。
* 结算同时维护两个前沿，概念分离、均可解释：
  * **写入水位线 `watermark_ms`**：单调递增的“不再接受回写”时间点。
    新事件若 `occurred_at < watermark_ms` ⇒ 拒绝
    `window_settled/window_settled`。水位线之后的迟到事件仍可回补
    其所属开放窗口。
  * **快照前沿 `snapshot_frontier`**：只按完整窗口边界推进
    （`watermark_ms` 向下取整到窗口边界）。只有在快照前沿之前、且
    完整关闭的窗口才会被物化到 `window_aggregates`；部分窗口不会被
    提前冻结。
* 已物化的窗口快照**不可变**：重复结算不会覆盖它，查询永远返回同一
  结果（`state="settled"`，带 `settled_at`），实现可复现。
* 边界约定：窗口 `[start, start+W)` 在 `watermark_ms >= start+W`
  时才会被快照；`occurred_at` 恰等于水位线的事件属于下一个开放窗口，
  正常接收。

查询时两个数据源互不重叠：`window_aggregates` 负责已结算窗口，
`events` 实时聚合快照前沿之后的开放窗口。

## 并发一致性保证

* SQLite 运行在 **WAL** 模式：读不阻塞写、写不阻塞读；读者总是看到
  某个已提交的一致状态，不会读到半截事务。
* 每个写事务以 **`BEGIN IMMEDIATE`** 开启，立即取得保留锁；进程内另有
  一把 `RLock` 把所有写者按到达顺序串行化。于是写入顺序就是加锁顺序，
  完全可解释。
* “查重 + 插入”在**同一把锁、同一个写事务**内完成，因此两个并发请求
  不可能同时通过查重再分别插入（check-then-act 竞态被消除）。
  `busy_timeout=5000` 处理极端的锁等待。
* 批量写是**全有或全无**：任何一个事件非法（结构错误、重复 id、冲突
  载荷、落入结算区）都会 `ROLLBACK`，该批次零行落库。
* 结算与写入并发时，结果只有两种且互斥：事件先于结算提交则进入快照/
  开放窗口且计数一次；事件晚于水位则得到 `window_settled`。测试
  `test_concurrent_settlement_and_write_is_serializable` 断言总数恒等于
  成功数，绝不出现重复或丢失。

> 进程内 `RLock` 保证的是**单进程多线程**部署下的串行化；SQLite
> `BEGIN IMMEDIATE` 仍会在多进程访问同一文件时由文件锁兜底。本项目
> 定位为本地单实例服务。

## 事件结构演进规则

* `schema_version` 缺省（或为 `null`）⇒ 视为 **v1**；显式支持 1、2。
  未知版本号 ⇒ `invalid_event/invalid_schema_version`。
* 演进是**纯增量**的：
  * v2 新增可选数值字段 `value`（参与 `sum_value` 聚合）。
  * v1 数据、或 v2 但缺省/显式 `null` 的 `value` 都正常计数，
    聚合时缺省值不参与求和（不会把缺失当 0 之外的错误，也不会静默
    丢弃整条记录）。
  * 未知的额外字段被容忍并原样保留（前向兼容）。
* 非法记录**绝不静默丢弃**，全部以 422 + 具体 reason 拒绝：

| reason                     | 触发条件                                   |
|----------------------------|--------------------------------------------|
| `not_json`                 | 请求体不是合法 JSON                        |
| `not_object`               | 事件不是 JSON 对象                         |
| `missing_field`            | 缺少 `event_id/event_type/occurred_at`     |
| `invalid_type`             | 字段类型错（布尔冒充数字也算）             |
| `invalid_schema_version`   | 版本号不是 int 或不在支持列表              |
| `invalid_timestamp`        | `occurred_at` 为负                         |

## 错误分类总览

响应体统一为 `{"error", "message", "details"}`，HTTP 状态与原因如下：

| HTTP | error 码            | 典型 details.reason                                   |
|------|---------------------|--------------------------------------------------------|
| 401  | `unauthorized`      | `missing_token` / `unknown_token`                      |
| 403  | `forbidden`         | `admin_required`（非 admin 调用 settle）               |
| 422  | `invalid_event`     | `not_json` / `not_object` / `missing_field` / `invalid_type` / `invalid_schema_version` / `invalid_timestamp` |
| 409  | `duplicate_event`   | `already_stored`（幂等重放，HTTP 仍 200）/ `event_id_conflict` / `duplicate_in_batch` |
| 409  | `window_settled`    | `window_settled`（迟到事件落入结算区）/ `watermark_regression` |

注意：**幂等重放不报错**——同 id 同载荷的重复提交返回 200，仅通过
`duplicates` 表达“这次没有新增计数”，调用方可以安全重试。

## 本地验证

```bash
# 全部单测（覆盖重复提交、乱序迟到、并发、结构演进四类场景）
uv run pytest -q

# 打印每条用例的输入与判定依据
uv run pytest --log-cli-level=INFO

# 手工冒烟
uv run uvicorn main:app --port 8000 &
TOK="local-token-tenant-a"; ADM="local-token-admin"
curl -s -X POST localhost:8000/events -H "Authorization: Bearer $TOK" \
  -H "Content-Type: application/json" \
  -d '{"event_id":"e1","event_type":"click","occurred_at":100}'
curl -s -X POST localhost:8000/events -H "Authorization: Bearer $TOK" \
  -H "Content-Type: application/json" \
  -d '{"event_id":"e1","event_type":"click","occurred_at":100}'   # 幂等重放
curl -s -X POST localhost:8000/admin/settle -H "Authorization: Bearer $ADM" \
  -H "Content-Type: application/json" -d '{"watermark_ms":1000}'
curl -s "localhost:8000/aggregates" -H "Authorization: Bearer $TOK"
```

## 代码结构

| 文件                  | 职责                                             |
|-----------------------|--------------------------------------------------|
| `app/errors.py`       | 稳定错误码与分类异常                             |
| `app/config.py`       | 窗口配置与本地静态令牌加载                       |
| `app/db.py`           | SQLite 连接（WAL/超时）、建表、进程写锁           |
| `app/events.py`       | 事件结构校验与 v1/v2 兼容规则                    |
| `app/store.py`        | 幂等写入、事务回滚、水位线推进与窗口快照         |
| `app/aggregation.py`  | 结算快照 + 开放窗口实时聚合                      |
| `app/api.py`          | FastAPI 路由、Bearer 鉴权、统一错误响应          |
| `tests/`              | 四类场景单测，日志打印输入与判定依据             |
