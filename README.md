# 跨仓调拨决定服务（两库预备事务收尾）

一笔跨仓调拨的**扣减**与**入库**分别以 PostgreSQL **预备事务**（2PC,
`PREPARE TRANSACTION`）留在两个独立数据库里。数据库值班员拿到最终决定后，
用本服务：

1. 先在**控制库**把决定（`commit` / `rollback`）持久化；
2. 再对两侧分别执行 `COMMIT PREPARED` / `ROLLBACK PREPARED`；
3. 逐侧确认“它确实按决定结束了”，**两库都确认后才报告完成**。

关键点是第 3 步：进程可能在一侧处理完后崩溃/断线，预备事务也会从
`pg_prepared_xacts` 列表消失。仅凭“列表里没有”无法区分它是被提交还是
被撤销，所以样例在准备阶段就写入了随事务生效的痕迹（见下文）。

## 快速开始

```bash
docker compose up --build -d          # 三个库 + API
docker compose --profile seed run --rm seed   # 准备样例调拨 T-2024-0001
```

- 在 `/docs` 里操作：`GET /transfers` → `GET /transfers/{id}` →
  `POST /transfers/{id}/decision`
- 三个数据库都映射到宿主机：control `5432`、source `5433`、target `5434`，
  用户/密码均为 `app`，库名分别为 `control` / `source` / `target`
- `max_prepared_transactions=20` 已在 compose 中为每个库打开

准备更多样例（各自独立的调拨号）：

```bash
docker compose --profile seed run --rm seed python seed.py \
    --transfer T-2024-0002 --sku SKU-1002 --qty 4
# 预置“已经定了、还没收尾”的决定，便于演示：
docker compose --profile seed run --rm seed python seed.py \
    --transfer T-2024-0003 --decision commit
# 重建某个调拨号（先撤销遗留预备事务与痕迹）
docker compose --profile seed run --rm seed python seed.py \
    --transfer T-2024-0001 --force
```

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/transfers` | 列出登记过的调拨号、GID 与已登记的决定 |
| GET | `/transfers/{id}` | 实时查询两库，返回每侧状态与总体 `completed` |
| POST | `/transfers/{id}/decision` | 提交决定 `{"decision":"commit"\|"rollback"}`，先持久化再收尾 |
| GET | `/health` | 控制库可用性 |

响应里每一侧的状态：

| `state` | 含义 |
| --- | --- |
| `prepared` | 预备事务仍在 `pg_prepared_xacts` |
| `committed` | 预备事务已不在列表，且 journal 行存在 → 确认已提交 |
| `rolled_back` | 预备事务已不在列表，且 journal 行不存在 → 确认已撤销 |
| `unreachable` | 该库连不上（决定已持久化，稍后重试即可） |
| `timeout` | 语句超过 `statement_timeout`（通常是有别的会话持有行锁） |
| `unknown` | 登记不完整/证据不足，无法判定 |
| `error` | 其他数据库错误 |

`matches_decision`：`true` 已按决定结束；`false` 结局与决定矛盾；
`null` 尚未定论。总体 `status` 取 `undecided / pending / completed /
needs_attention`。

## 行为约定

- **决定先落盘**：`POST` 先在控制库提交决定，成功后才碰业务库。控制库
  不可用时返回 `503`，此时两边什么都没改。
- **重复提交同一决定**：`repeated=true`，继续处理未收尾的一侧；已完成的
  一侧只做证据核对，不会重复扣减/入库。响应仍为 `200`。
- **相反决定**：`409 decision_conflict`，已登记的决定不变，两侧预备事务
  原样保留。
- **不谎报完成**：只有两侧 `matches_decision=true` 才 `completed=true`。
  一侧不可达/未收尾时返回 `200` 且 `completed=false`、`pending_sides`
  列出未确认的一侧；某侧结局与决定矛盾时返回 `409`、`status=needs_attention`。
- **重启不丢进度**：服务不保存任何进程内状态，每次请求实时查询三库。
  即使一侧已被上一进程处理完、预备事务已不在列表里，也能凭 journal
  痕迹确认结局并继续处理另一侧。

## 为什么能“确认按决定结束”

`db/source/init.sql`、`db/target/init.sql` 里有两张表：

- `transfer_journal(gid, transfer_id, side, sku, delta, ...)`
  **在预备事务内部**写入。它只在预备事务**提交后**才可见；撤销不会留下行。
  因此它是“确实提交过”的持久证据。
- `transfer_registry(transfer_id, side, gid, status, prepared_at)`
  在预备事务**之外**维护（准备前写 `preparing`，`PREPARE` 成功并核实后
  改为 `prepared`），记录“这一侧确实准备完成了”。

判定顺序（`app/store.py:_observe_side_conn`）：
预备事务还在 → `prepared`；否则 journal 在 → `committed`；否则登记为
`prepared` → `rolled_back`；其余 → `unknown`。

控制库里 `transfers.decision` 是权威决定，`decision_log` 记录每次
accepted / repeated / conflict，`side_results` 保存最近一次收尾观察，
便于事后审计。

## 自测

前置：三个库已初始化、API 在运行。除 HTTP 外还需要直连三库的 DSN
（用于核对库存与预备事务列表）。

```bash
pip install -r tests/requirements.txt
export BASE_URL=http://127.0.0.1:8000
export CONTROL_URL=postgresql://app:app@127.0.0.1:5432/control
export SOURCE_URL=postgresql://app:app@127.0.0.1:5433/source
export TARGET_URL=postgresql://app:app@127.0.0.1:5434/target
# 可选：用于“进程重启”用例（重启后 API 需重新可用）
export RESTART_CMD="docker compose restart api && sleep 2"
python3 tests/selftest.py
```

覆盖：未决定状态、提交、撤销、未知调拨号、相反决定不动预备事务、
并发重复提交不重复生效、决定持久化后重启、单侧已处理（提交/撤销两种）
后重启仍能确认并完成收尾。每个用例使用独立 SKU；测试开始与结束会撤销
自己前缀残留的预备事务。

`RESTART_CMD` 未设置时跳过三个重启用例。

## 本地（无 Docker）试跑

本仓库不依赖 Compose 才能验证：只要三个库可连并已按 `db/*/init.sql`
初始化，用 `CONTROL_URL/SOURCE_URL/TARGET_URL` 指向它们即可：

```bash
python app/wait_for_db.py
python app/seed.py --transfer T-2024-0001
uvicorn main:app --app-dir app --port 8000     # 或在 app/ 下运行
python tests/selftest.py
```

## 已知限制

- **2PC 行锁**：预备事务在收尾前一直持有它改过的行锁。同一 SKU 上准备
  第二笔未收尾的调拨会等锁；`seed.py` 用 `lock_timeout` 快速失败并提示。
  真实场景应避免对同一库存行并发准备多笔调拨。
- 收尾按“先扣减侧、后入库侧”顺序执行。若扣减侧成功、入库侧长期不可达，
  会出现暂时的单边已提交状态——服务会一直报 `pending`，不会报完成；
  决定与已执行的一侧可通过控制库审计，人工恢复后重复提交即可续完。
- 控制库是单点：它不可用时只能看状态不能登记决定（`503`）。
  三库之间没有分布式事务，控制库决定与两侧收尾之间靠“先落盘 + 重试
  + 证据核对”保证最终一致，而不是原子性。
- 没有鉴权；示例只绑定本机/内网使用。
