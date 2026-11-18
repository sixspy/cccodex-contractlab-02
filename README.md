# 入站契约

> 遵守 `ops/底座生成前必读_效率与技术栈.md`。FastAPI + Vue 3 + SQLite，交付须 docker compose 一键起；库用 SQLite，不挂外置 Postgres。  
> **禁止同构换皮**：本仓主链路、表结构、接口与页面均独立，不得与其它仓共用同一套 `/api/items` 壳。

## 骨架一句话

Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账

## 基本信息

| 项 | 值 |
|----|-----|
| 产品名（首页） | 入站契约 |
| 目录 | `05-contractlab` |
| 领域建议 | 数据采集、爬虫与 RPA |
| 接口 | http://127.0.0.1:10712 |
| 库 | `data/app.db` |
| 账号 | admin / 123456 |

## 缺字段失败闭（定案口径 · 2026-11-17）

缺字段只走**乙路径**，全仓一种写法，不与甲路径混用：

- 缺字段批次先落 `ingress_batches(result=缓冲)` 取批次号，同一事务再以该批次号写 `buffers` 缓冲行（`status=待补采`）；
- 缺字段**不得标通过**，**不得进拒收详情**；拒收详情只收非缺字段（类型不符等）；
- 禁止**无批次缓冲孤儿**，禁止缓冲行挂到已通过批次或标通过；
- 页序乙按 `batch_id` 先到先补，缓冲队列页可核；
- 同载荷双人抢补交：先到落库，后到只登记 `missing_field_contention`（缺字段争用），不重复落库；同值班重提只算重复提交；
- 以上任一偏离由库内触发器拦下并写 `missing_field_deviations`（`缺字段偏了`）；
- 口径落入 `missing_field_policy`（`branch=乙`，`chosen_at`），定案当天改口同样拦下并记「缺字段偏了」。

缓冲队列、拒收详情、入站列表、对账账册、策略卡片五处均返回同一组缺字段计数，`GET /api/reconcile` 的 `aligned` 为总校验。

库文件路径不变（`data/app.db`），历史缺字段行在启动时就地并入乙路径。

## 为何能出多题

字段增删改类型、宽严模式、影子契约、缓冲窗、生产者消费者双视角

## 避开

别做成质检签发换皮；别做成纯表单 CRUD

## 互斥切口候选

- 破坏性变更直接拒收 vs 双写缓冲
- 影子只读试跑 vs 双轨分开展示
- 契约回滚整版原子切换 vs 按生产者摘流

## 启动（一键）

`powershell
cd projects/05-contractlab
docker compose up -d --build
`

浏览器：http://127.0.0.1:10712/  
健康检查：GET http://127.0.0.1:10712/api/health

本机开发（可选）：

`powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --app-dir backend --host 127.0.0.1 --port 10602
python -m pytest -q
`
