# 市政道路线形加工服务

测绘队共享测点 → 路段线形（GeoJSON LineString）加工与入库服务，并支持**误更正坐标批次的整批撤回**。
FastAPI + SQLite（WAL），两个 API 进程共用一个数据库文件，Docker Compose 一键启动。

## 启动

```bash
docker compose up --build -d
```

* API A：<http://localhost:8001>  Swagger UI：<http://localhost:8001/docs>
* API B：<http://localhost:8002>  Swagger UI：<http://localhost:8002/docs>
* 两个进程共用卷 `gis-data:/data/gis.db`（命名卷，`docker compose down` 不会丢数据；要清空请 `docker compose down -v`）。

跑一遍样例（可选，会自动做在线演示，含撤回，见文末）：

```bash
docker compose --profile demo run --rm demo
```

重启验证（数据仍在）：

```bash
docker compose restart api-a api-b
curl -s http://localhost:8001/api/v1/state | head -c 400
```

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/batches` | 一个批次=一个事务：批量新增/更正测点与路段、批量删除；返回受影响路段的最终状态（含 `batch_id`、`withdrawable`） |
| POST | `/api/v1/batches/{batch_id}/withdraw` | **撤回**只更正了已有测点坐标的成功批次：恢复原坐标并重算全部当前引用路段 |
| GET | `/api/v1/batches/{batch_id}/rollback` | 查看该批次保存的原坐标、当前值及是否仍可撤回（排查用） |
| GET | `/api/v1/state` | **一个读快照**返回全部测点原值 + 全部已持久化线形（含 `generation`） |
| GET | `/api/v1/points` / `/api/v1/points/{id}` | 查询测点 |
| GET | `/api/v1/roads` / `/api/v1/roads/{id}` | 查询路段（只读已持久化的 `geometry`，不现场组线） |
| GET | `/health` | 健康检查（含 WAL 模式） |

批次请求体（所有字段可选，按 `points → roads → delete_roads → delete_points` 顺序生效）：

```json
{
  "batch_id": "fix-2026-11-14",   // 可选；服务端始终会返回一个批次 ID
  "points":       [{"id": "PT-001", "lon": 116.397428, "lat": 39.909230}],
  "roads":        [{"id": "RD-101", "point_ids": ["PT-001", "PT-002", "PT-003"]}],
  "delete_points": ["PT-004"],
  "delete_roads":  ["RD-102"]
}
```

* 路段 `point_ids`：≥ 2 个、有序、批内不可重复；允许引用**尚不存在**的测点（该路段进入 `incomplete`）。
* 已存在 id 视为**更正**（upsert）：测点改坐标、路段改引用顺序/成员。
* 同一 id 不能在同一批次里既 upsert 又 delete（返回 422）。
* 演示用查询参数：`pause_ms`（组线暂停，**事务外**）、`fail=true`（写完后注入异常以演示回滚）。
  `/docs` 的 “Try it out” 可直接选中这两个参数演示。

响应里的路段：

```json
{
  "id": "RD-101",
  "point_ids": ["PT-001", "PT-002", "PT-003"],
  "status": "ok",                 // ok | incomplete
  "geometry": {"type": "LineString", "coordinates": [[116.397428, 39.909230], ...]},
  "missing_point_ids": [],        // incomplete 时按路段顺序列出缺失测点 ID
  "revision": 3,                  // 每受影响一次 +1
  "updated_at": "2026-11-13T07:42:30.316Z"
}
```

## 撤回误更正批次

### 什么批次会被记录

只有**只更正已有测点坐标**的成功批次才保存批次 ID 与原坐标（响应里 `withdrawable: true`）。
以下批次不记录、因此不可撤回（`withdrawable: false`，撤回返回 404）：
新增测点、任何删除（点或路段）、任何路段新增/改引用、以及实际没有坐标变化的批次。
一批里只要有一个点原本不存在，整批就不可撤回——恢复坐标无法撤销"新增"。

### 撤回做了什么

`POST /api/v1/batches/{batch_id}/withdraw` 在**一个写事务**里：

1. 原子地恢复该批记录的全部原坐标；
2. 按恢复后的坐标重算**撤回时刻**所有引用这些测点的路段（包括该批次之后由其他批次
   新建或改引用进来的路段），并把线形与缺点状态持久化；
3. 标记撤回记录为已撤回。坐标、线形、撤回记录同事务提交；中途任何失败整体回滚，
   原状态保留（可用 `?fail=true` 演示）。

### 409 / 404 / 重复撤回

* 若记录中的**任一**测点此后被其他批次更正、删除或重建——**即使坐标又改回原值**——
  整批返回 **409**，`conflicts` 列出测点与原因，**不修改任何数据**（不会部分恢复）。
* 其他批次新增/删除路段、改变路段引用、改动**未记录**的测点，都**不妨碍**撤回。
* 重复撤回成功批次：返回首次结果（`already_withdrawn: true`、`points_restored: 0`），
  不再次写库，`revision` 不再增长。
* 无此批次记录 → **404**。

"被改过"靠逐测点的单调版本号判断，而不是比较坐标：把坐标改回原值、或删掉再重建，
版本号仍会前进，因此都能识别。`GET /api/v1/batches/{id}/rollback` 可看到每个点的原值、
当前值与是否仍可撤回。

响应：

```json
{
  "batch_id": "fix-2026-11-14",
  "withdrawn_at": "2026-11-14T16:13:06.001Z",
  "points_restored": 2,
  "already_withdrawn": false,
  "affected_road_ids": ["RD-101", "RD-102", "RD-106"],
  "roads": [ { "id": "RD-101", "geometry": {...}, "revision": 4, ... } ]
}
```

## 关键行为

1. **组线即持久化**：线形在提交事务内按 `point_ids` 顺序组装，写入 `roads.geometry`；
   查询只 SELECT 该列，绝不临时组线。
2. **缺点不输出残线**：任一引用测点不存在 → `geometry = null`、`status = incomplete`、
   `missing_point_ids` 列明缺失 ID（按路段顺序去重）。补点后**下一次批次**自动恢复线形，无需额外调用。
3. **影响面完整**：更正/删除测点、改变路段引用时，所有受影响路段（含引用该测点的全部路段）在
   **同一事务**里重算；未受影响的路段不重写（`revision` 不变）。撤回同理，按撤回时刻的引用关系重算。
4. **整批原子**：批次中途任何异常（含写完后注入）整体回滚，`generation` 不前进，库保持原状。
   撤回同样原子，失败保留原坐标、线形与撤回记录。
5. **重启保留**：SQLite 文件在命名卷里，`docker compose restart` / 重建容器后数据（含撤回记录）仍在。


## 并发模型（两个进程共库）

* 每个请求一条短连接；写事务一律 `BEGIN IMMEDIATE` + `busy_timeout=30s`，两个进程的写入互斥但排队等待，不报 busy。
* **组线在事务之外**：先在一个 WAL 读快照上把批次规划完（含组线），再进入极短的写事务。
  因此 `pause_ms=2000` 的慢批次不会挡住另一进程的入库（演示第 5 步实测 < 1s 完成）。
* **提交前复核快照代号**（`store_meta.generation`）：规划期间若有别的进程提交过，
  本次规划整体作废、从新快照重算（响应里 `commit_attempts` 可见），
  绝不把基于旧坐标算出的线形写回库；并发新增 / 改引用的路段也在重算中被重新纳入，
  不会漏更新。
* **撤回沿用同一机制**：撤回同样在事务外规划、提交前复核代号，并在写事务内再次复核
  观测版本号，因此两个进程同时撤回同一批次只有一个真正写库（另一个返回
  `already_withdrawn`），撤回与入库交错也不会漏重算路段或写回旧引用。
* **读一致性**：`/api/v1/state` 在一个延迟事务里完成全部读取，返回的测点原值与线形必定来自同一已提交状态。
* WAL 下读者不阻塞写者、写者不阻塞读者；`synchronous=FULL` 保证已提交数据断电安全。

## 本地运行与测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
GIS_DB_PATH=/tmp/gis.db uvicorn app.main:app --port 8001

pytest            # 功能 + 双进程并发（测试会真的起两个 API 进程共库运行）
```

（无 docker 环境时也可直接验证整套并发行为：`tests/test_concurrency.py` 用
`tests/server_runner.py` 起两个独立进程，连接同一个 SQLite 文件。）

`tests/test_concurrency.py` 起两个真实 API 进程共库运行：跨进程一致性、暂停组线不持写锁、
并发改线无漏更新/旧坐标、`/state` 在持续写入下的快照一致性，以及跨进程撤回
（含撤回后其他进程新建的路段也被重算、撤回与更正竞态、双进程同时撤回只写一次）；
`tests/test_retry.py` 用可控方式制造"规划后另一进程提交"，断言旧规划被丢弃并重算；
`tests/test_withdraw.py` 覆盖撤回记录、原坐标恢复、当前引用路段重算、409/404、
重复撤回幂等与失败回滚。

## 演示样例（要点）

`examples/run_demo.py`（compose profile `demo`）对两个进程逐步演示：

1. 批量入库测点 + 3 条路段，线形已持久化；
2. 删除被引用测点 → `geometry=null`、`missing_point_ids` 列明；
3. 补回该点 → 线形自动恢复；
4. 改由 API B 更正共享测点 → 两条引用它的路段同批生效；
5. **可控暂停**：A 提交一个 `pause_ms=2000` 的批次，B 在暂停期间完成测点更正 + 新建路段
   （用时 < 1s，未被写锁阻塞）；A 提交时发现代号已变，整批重算后 `commit_attempts=2`，
   重算结果把 B 新建的 RD-105 一并纳入（`affected_road_ids` 含 RD-103/104/105），
   两条路段与 PT-006 原值一致；
6. `fail=true` 批次写完后抛错 → 整体回滚，`/state` 与失败前逐字段一致；
7. **误更正撤回**：误更正一批测点坐标（响应带 `batch_id`、`withdrawable=true`）；期间
   养护系统新建引用其中一点的 RD-106 并更正另一点；API B 撤回 → 原坐标恢复，
   RD-106 等当前引用路段全部重算；重复撤回不再写库；其后再有批次更正同一点
   （即使改回原值）→ 整批 409，数据不变。

本地直接跑（不用 compose）：

```bash
uvicorn app.main:app --port 8001 &  uvicorn app.main:app --port 8002 &
python -m examples.run_demo --base-a http://localhost:8001 --base-b http://localhost:8002
```

## 说明

* 缺测点用 `point_id` 不加外键约束是刻意设计：悬空引用是"可表示状态"，才能稳定报告缺失 ID
  并在补点后自动恢复；路段删除时其引用行由 `ON DELETE CASCADE` 清理。
* 单机 SQLite 的写入吞吐上限是设计边界：本方案保证**正确性**，写入串行化 + 组线在事务外
  使得暂停/慢组线不会拖住其他入库。若未来需要多机横向扩展，应将 `store_meta.generation`
  复核替换为外部分布式锁/版本号。
