# 联邦 Kaplan–Meier 生存曲线合算平台

两院研究数据员在不交换病例明细的前提下合算 Kaplan–Meier（KM）生存曲线。
协调端只拿得到**快照标识、死亡日、风险人数和死亡数**；病例明细不出院端服务边界。

```
┌─────────────┐        快照标识 / 死亡日 / 风险数·死亡数         ┌─────────────┐
│ coordinator │ ◀──────────────────────────────────────────────▶ │  site-a     │
│  (FastAPI)  │ ◀──────────────────────────────────────────────▶ │  (FastAPI)  │
│  + Postgres │                                                  │  + Postgres │
└─────────────┘                                                  └─────────────┘
        ▲                                                        ┌─────────────┐
        └──────────────────────────────────────────────────────▶ │  site-b     │
              两轮快照齐备才发布；取消/失败即整轮停止并释放            │  + Postgres │
                                                                 └─────────────┘
```

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

共 6 个容器：协调端 + 两个独立院端 + 各自的 PostgreSQL。

| 服务 | 地址 | 说明 |
|---|---|---|
| coordinator | http://localhost:8000 （/docs） | 协调端 API |
| site-a | http://localhost:8001 （/docs） | 院端 A，160 例演示数据 |
| site-b | http://localhost:8002 （/docs） | 院端 B，140 例演示数据，**人为慢 5 秒/次**（慢院演示） |

院端病例表：`cases(case_no TEXT PK, days INT CHECK(days>0), event TEXT CHECK(event IN ('death','censored')))`，
每院至多 200 例（PostgreSQL 触发器强制，直连 SQL 也受限）。

## 一次计算

```bash
# 1. 发起一轮（异步）
RID=$(curl -s -X POST localhost:8000/rounds | python3 -c 'import sys,json;print(json.load(sys.stdin)["round_id"])')

# 2. 轮询状态（慢院不挡查询，始终毫秒级返回）
curl -s localhost:8000/rounds/$RID | python3 -m json.tool

# 3. 成功后 result.curve 即合算曲线
#    [{"t":..,"n_risk":..,"d":..,"survival":..}, ...]

# 随时可取消（立即返回，整轮停止，两院快照释放后才进入 cancelled 终态）
curl -s -X POST localhost:8000/rounds/$RID/cancel
```

一键演示：`scripts/demo_success.sh`（完整跑通一轮并打印曲线）、
`scripts/demo_slow_site.sh`（慢院下查询/取消依然即时，且终态后迟到的建快照请求被拒）。

## 计算口径

- 每轮开始，协调端在**两院各自新建一个本轮专用快照**（冻结时刻的日级直方图，非病例明细），
  整轮所有请求都固定使用这同一快照；下一轮一律重新建快照，绝不拼用新旧快照续算。
- 评估点 = **两院死亡日的并集**，按升序逐点合算。
- 每个评估点 t：风险人数 n_risk = 两院「随访天数 ≥ t」合计；死亡数 d = 两院「t 日死亡」合计。
  **同日死亡先于删失**：t 日删失者仍计入 t 日风险集（标准 KM 约定）。
- 生存率递推：`S(t) = S(t_prev) × (1 − d(t)/n_risk(t))`，`S(0)=1`。
- **两院齐备才发布**：只有两轮聚合都完成、曲线算完，`GET /rounds/{id}` 才会带 `result`。

## 轮次状态与资源释放保证

状态机：`running → (cancelling) → succeeded / failed / cancelled`

- **取消或任一院失败即停止整轮**：剩余步骤不再执行；协调端先在两院释放快照并
  `finalize` 本轮，**然后**才发布终态（自测逐条验证了这一先后顺序）。
- **终态后迟到的建快照请求不得占用**：院端记录已 finalize 的 round_id，迟到请求直接 409，不占槽位。
- **每院最多 2 个活动快照**：第 3 个请求 409；建快照按 round_id 幂等（重试返回同一快照，不重复占槽）；
  创建经进程内异步锁串行化，并发下槽位上限依然严格成立（10 并发实测仅 2 个成功）。
- **协调端退出后占用自动释放**：快照带 TTL（默认 300s，可配），院端后台清扫器到期即释放；
  优雅停机（`docker compose stop`）时协调端也会主动取消在途轮次并释放两院。
- **重启不续算**：协调端重启时把 `running/cancelling` 的轮次标记为
  `failed("coordinator restarted…")` 并尽力释放快照；新任务用全新快照正常完成。
- **正常更正不等计算结束**：快照是创建时刻的冻结副本，计算期间对 `cases` 的直连 SQL 更正立即生效、
  不被计算阻塞，也不影响进行中的本轮结果（下轮生效）。

## 直连 SQL 更正示例

```bash
docker compose exec site-a-db psql -U sitea -d site_a \
  -c "UPDATE cases SET days = 365, event = 'death' WHERE case_no = 'A-0007';"
```

更正即刻提交，无需等待任何计算；随后发起的新一轮即反映更正。

## 慢院演示

`docker-compose.yml` 中 `site-b` 设了 `RESPONSE_DELAY_SECONDS=5`（每个计算类请求慢 5 秒）：

```bash
./scripts/demo_slow_site.sh
```

可观察到：状态轮询仍是毫秒级；取消请求立即返回；整轮随后进入 `cancelled`；
两院 `GET /snapshots` 的 `active_count` 归零；对已终态轮次的迟到建快照请求返回 409。

## API 摘要

协调端（8000）：

| 方法/路径 | 说明 |
|---|---|
| `POST /rounds` | 发起一轮 → 202 `{round_id, status:"running"}` |
| `GET /rounds/{id}` | 查询状态；仅 `succeeded` 时含 `result` |
| `GET /rounds` | 轮次列表 |
| `POST /rounds/{id}/cancel` | 请求取消 → `cancelling`（已终态则 409） |
| `GET /health`、`/docs` | 健康检查、Swagger 文档 |

院端（8001/8002，仅聚合信息，无病例明细）：

| 方法/路径 | 说明 |
|---|---|
| `POST /snapshots` `{round_id}` | 建快照（幂等/槽位满 409/已终态 409） |
| `GET /snapshots/{id}/death-days` | 该快照的死亡日列表 |
| `POST /snapshots/{id}/aggregate` `{points:[..]}` | 各点的 n_risk 与 d |
| `DELETE /snapshots/{id}` | 释放快照（幂等 204） |
| `POST /rounds/{round_id}/finalize` | 标记终态：删除本轮快照、拒绝迟到建快照 |
| `GET /snapshots` | 活动槽位（仅标识与过期时间） |
| `GET /health`、`/docs` | 健康检查、Swagger 文档 |

## 主要配置（环境变量）

协调端：`DATABASE_URL`、`SITE_A_URL`、`SITE_B_URL`、`SITE_REQUEST_TIMEOUT`(30s)、
`SITE_CLEANUP_TIMEOUT`(5s)、`SHUTDOWN_TIMEOUT`(15s)。

院端：`SITE_ID`、`DATABASE_URL`、`MAX_SNAPSHOTS`(2)、`SNAPSHOT_TTL_SECONDS`(300)、
`RESPONSE_DELAY_SECONDS`(0)、`SEED`、`SEED_CASES`(≤200)、`SWEEP_INTERVAL_SECONDS`(2)。

> 注意：`SNAPSHOT_TTL_SECONDS` 需大于最慢一轮的耗时，否则院端会在计算途中回收快照导致整轮失败。

## 自测

```bash
pip install fastapi "uvicorn[standard]" httpx "sqlalchemy[asyncio]" aiosqlite pytest asyncpg pgserver

python -m pytest tests/test_km.py        # KM 口径单元测试（含同日死亡先于删失）
python tests/integration_local.py        # 三端真起服务（SQLite）：34 项端到端检查
python tests/integration_postgres.py     # 嵌入式 PostgreSQL：并发槽位、曲线对拍、200 例上限等
```

`integration_local.py` 覆盖：成功路径曲线与独立计算逐点对拍、运行中不发布结果、
慢院下查询/取消即时、取消与失败均先释放两院再报终态、槽位上限、迟到建快照拒绝、
幂等建快照、计算中直连 SQL 更正不被阻塞、协调端崩溃后 TTL 自动释放、重启恢复、
优雅停机释放。`integration_postgres.py` 另覆盖：10 并发抢 2 槽位、PostgreSQL 下曲线对拍、
200 例上限触发器。

## 已知限制

- 单进程部署模型：一个协调端、每院一个院端进程（与题目拓扑一致）；协调端库宕机则无法工作。
- 协调端崩溃后若院端同时不可达，快照释放依赖 TTL 兜底（默认 300s 内自动释放）。
- 结果不含置信区间（可在此框架上扩展 Greenwood 方差，需额外交换各点累计信息）。
- 未做鉴权/传输加密，假定运行在内网可信环境；院端 API 本身不输出任何病例级数据。
