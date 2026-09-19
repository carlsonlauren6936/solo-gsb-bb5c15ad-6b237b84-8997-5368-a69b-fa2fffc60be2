# 文件交接留痕服务（Handoff Chain）

基于 **FastAPI + SQLite** 的多人文件交接留痕服务。每次交接形成一条只可追加的
**SHA-256 哈希链**，可查询、可审计、可完整性校验；并发移交与随机数重放由服务端
原子拒绝，**链条不会分叉**。

## 核心规则

- **创建链条**：提交文件 `SHA-256` 摘要与首位持有人，服务端写入序号 `0` 的创世事件：
  ```json
  {"file_hash": "<文件摘要>", "holder": "<首位持有人>", "seq": 0}
  ```
  对其键名排序的**紧凑 JSON**（无空白）计算 SHA-256，即创世哈希（初始链尾）。
- **执行移交**：每次请求须携带
  - `tail_hash`：客户端认定的**当前链尾哈希**；
  - `receiver`：接收方；
  - `nonce`：一次性随机数（同一链内用过即作废，防重放）。

  交出方（`giver`）由服务端依据当前链尾持有人确定，客户端无法伪造。
  新事件哈希按以下紧凑 JSON（`sort_keys` 后）计算：
  ```json
  {"giver":"...","nonce":"...","prev_hash":"<前序哈希>","receiver":"...","seq":<序号>}
  ```
  服务端在**同一个 SQLite 立即事务**中完成「校验链尾 → 查重随机数 → 追加事件 →
  推进链尾」，全程串行。链尾不匹配（过期/竞争请求）或随机数重放一律返回
  **`409 Conflict`**，不写入任何数据，因而不可能产生分叉。
- **待接收移交（邀约确认）**：除直接移交外，持有人也可先发起邀约、由接收方确认：
  1. **发起邀约** `POST /offers`：携带当前 `tail_hash`、指定的 `receiver`、
     有效期 `expires_in_seconds`（秒，上限 7 天）与一次性确认码
     `confirmation_code`。服务端**只保存确认码的 SHA-256 摘要**（明文不落库），
     返回 `offer_id`；链尾不匹配返回 **409**。还可**可选**提交一次性撤销码
     `cancellation_code`：服务端仅保存「绑定 `offer_id` 做域分隔」的
     **SHA-256 撤销码摘要**；省略撤销码则创建**不可撤销**的邀约
     （与历史版本完全兼容）。
  2. **确认邀约** `POST /offers/{offer_id}/confirm`：接收方凭 `offer_id` 与
     确认码确认。服务端在**同一个 SQLite 立即事务**中依次校验「确认码摘要匹配 →
     邀约未过期 → 邀约未使用 → 邀约未撤销 → 链尾仍匹配」，全部通过后追加唯一
     移交事件（`nonce` 取 `offer:<offer_id>`，天然一次性）并将邀约置为
     `accepted`，整体原子提交。**过期（`offer_expired`）、重复确认
     （`offer_already_accepted`）、链尾已推进（`tail_mismatch`）、邀约已撤销
     （`offer_not_cancellable`）均返回 409 且不写任何事件**；
     确认码错误返回 **403**（`code_mismatch`）。
  3. **撤销邀约** `POST /offers/{offer_id}/cancel`（仅创建时提供了
     `cancellation_code` 的邀约可用）：发起方凭撤销码撤销。撤销、接收方确认与
     见证人表态在**同一个 SQLite 立即事务层面互斥**，竞争终态中至多一个成功。
     仅当邀约**仍 `pending` 且链尾未推进、未过期、无见证人拒绝**时可撤销，
     撤销只把邀约置为 `cancelled`，**绝不追加或修改任何哈希链事件**；撤销码
     错误返回 **403**（`cancel_code_mismatch`）。
  4. **不可撤销的情形**：邀约已 `accepted`、`rejected`、`expired`、
     `terminated`、`cancelled`，或创建时未提供撤销码（含全部旧邀约），
     调用撤销一律返回 **409**（`offer_not_cancellable`）且不改写链。
  5. **查询邀约** `GET /offers/{offer_id}`：返回邀约状态
     （`pending` / `accepted` / `expired` / `terminated` / `rejected` /
     `cancelled`，其中过期、终止、拒绝为派生状态）、`cancellable` 标记、
     `cancelled_at` 与确认结果，**不含确认码、撤销码及其摘要**。
- **见证确认（可选模式）**：发起邀约时可附带 2～5 名见证人及通过阈值
  `witness_threshold`（1～见证人数量），其余流程与普通邀约一致：
  1. 发起邀约时每个见证人提交 `name` 与一个**互相独立的一次性令牌** `token`。
     服务端只保存「绑定 offer_id 做域分隔」的 **SHA-256 令牌摘要**，明文仅在
     创建响应中返回一次，由发起方线下分发。
  2. **见证人表态** `POST /offers/{offer_id}/witness-decisions`：凭令牌表态
     `approved`（同意）或 `rejected`（拒绝）。令牌错误返回 **403**
     （`token_mismatch`）；令牌命中即作废，同一见证人重复表态返回 **409**
     （`witness_already_decided`）。
  3. 接收方确认时，服务端在**同一个 SQLite 立即事务**内于原有校验之后再校验
     「**无见证人拒绝** 且 **同意数达到阈值**」：有拒绝返回 **409**
     （`offer_rejected`），同意数不足返回 **409**
     （`witness_threshold_not_met`），均**不写任何事件**。
  4. **任一拒绝**即令邀约进入 `rejected` 终止态；**过期**、**链尾被其他移交
     推进**（状态派生为 `terminated`）、**被撤销码撤销**（`cancelled`）同样
     终止，终止后表态、确认与再次撤销均返回 409 且不写事件。撤销与表态/确认
     在 SQLite 立即事务中互斥，竞争中只有一个终态能成功。
  5. **见证状态查询** `GET /offers/{offer_id}/witnesses`：返回各见证人的名称、
     序号、表态结果与同意/拒绝/待表态计数及派生邀约状态，**绝不返回令牌及其
     摘要**（`GET /offers/{offer_id}` 的邀约详情同样只给统计与表态结果）。
- **完整性校验**：服务端从创世记录起逐条重算哈希、校验序号连续性与 `prev_hash`
  指针，并比对链尾缓存；校验结果给出 **首个断裂序号 `first_broken_seq`**。

## 一条命令启动（依赖安装 + 数据库初始化 + 启动）

需要已安装 Docker（含 Compose 插件）。在项目根目录执行：

```bash
docker compose up --build
```

该命令会：构建镜像（其中 `pip install -r requirements.txt` 完成依赖安装）→
容器启动时自动初始化 SQLite 数据库（自动建表，数据位于命名卷 `handoff-data`，
库文件 `/data/handoff.db`）→ 启动 API 服务。

### 访问地址与端口

| 用途 | 地址 |
| --- | --- |
| API 服务根地址 | <http://localhost:8000> |
| 健康检查 | <http://localhost:8000/health> |
| 交互式文档（Swagger UI） | <http://localhost:8000/docs> |
| OpenAPI 描述 | <http://localhost:8000/openapi.json> |

后台运行：`docker compose up --build -d`；停止：`docker compose down`
（数据保留在卷中；如需连数据一并清除：`docker compose down -v`）。

### 不用 Docker 的本地启动方式

```bash
pip install -r requirements.txt
DATA_DIR=./data python -m app.main        # 自动建库建表后启动
# 或显式初始化：
python -c "from app.database import init_db; init_db()"
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/chains` | 创建交接链（文件摘要 + 首位持有人） |
| `POST` | `/handovers` | 执行一次移交（链尾哈希 + 接收方 + 一次性随机数） |
| `POST` | `/offers` | 发起邀约（链尾 + 接收方 + 有效期 + 一次性确认码；可选撤销码；可选 2～5 名见证人与阈值） |
| `POST` | `/offers/{offer_id}/confirm` | 接收方凭确认码确认，原子追加移交事件（见证模式须无拒绝且同意数达阈值） |
| `POST` | `/offers/{offer_id}/cancel` | 凭一次性撤销码撤销邀约（仅 pending 且链尾未推进/未过期/无拒绝；错码 403，终态或无撤销码 409；不改链） |
| `POST` | `/offers/{offer_id}/witness-decisions` | 见证人凭独立一次性令牌表态同意 / 拒绝（每人至多一次） |
| `GET`  | `/offers/{offer_id}/witnesses` | 查询见证表态状态与计数（不含令牌摘要；无见证邀约 404） |
| `GET`  | `/offers/{offer_id}` | 查询单个邀约状态（pending / accepted / expired / rejected / terminated / cancelled） |
| `GET`  | `/chains` | 列出全部链条 |
| `GET`  | `/chains/{chain_id}` | 查询链条与全部事件 |
| `GET`  | `/chains/{chain_id}/verify` | 完整性校验，指出首个断裂序号 |
| `GET`  | `/health` | 健康检查 |

错误响应统一为：`{"error": "<code>", "detail": "<说明>"}`。状态码约定：
**409** —— `tail_mismatch`（链尾不匹配/已推进，邀约派生状态为 `terminated`）、
`nonce_replay`（随机数重放）、`offer_expired`（邀约已过期）、
`offer_already_accepted`（邀约重复确认 / 已接受后继续表态）、
`offer_rejected`（已有见证人拒绝，邀约终止）、
`witness_threshold_not_met`（同意数未达阈值）、
`witness_already_decided`（见证人已表态，令牌不可重复使用）、
`offer_not_witnessed`（对无见证邀约调用表态接口）、
`offer_not_cancellable`（邀约不可撤销：已 accepted / rejected / expired /
terminated / cancelled，或创建时未提供撤销码的旧邀约；撤销与确认/表态竞争
落败也返回此码）；
**403** —— `code_mismatch`（确认码错误）、`token_mismatch`（见证人令牌错误）、
`cancel_code_mismatch`（撤销码错误）；
**404** —— 链条、邀约不存在，或对无见证邀约查询 `witness_mode_not_found`。

## 使用示例

```bash
# 1) 计算文件摘要并创建链条
FILE_HASH=$(sha256sum some-file.bin | awk '{print $1}')
curl -s -X POST http://localhost:8000/chains \
  -H 'Content-Type: application/json' \
  -d "{\"file_hash\": \"$FILE_HASH\", \"first_holder\": \"alice\"}"

# 响应中含 chain_id 与 tail_hash（创世哈希），例如：
# {"chain_id":"ab12...","tail_hash":"9f0c...","current_holder":"alice", ...}

# 2) alice 移交给 bob（必须带上当前 tail_hash 和全新 nonce）
curl -s -X POST http://localhost:8000/handovers \
  -H 'Content-Type: application/json' \
  -d '{"chain_id":"ab12...","tail_hash":"9f0c...","receiver":"bob","nonce":"n-0001"}'
# 返回新的 event_hash（即新链尾），下一次移交必须携带它

# 3) 用旧链尾再交一次 → 409 tail_mismatch（拒绝，不分叉）
curl -i -X POST http://localhost:8000/handovers \
  -H 'Content-Type: application/json' \
  -d '{"chain_id":"ab12...","tail_hash":"9f0c...","receiver":"carol","nonce":"n-0002"}'

# 4) 随机数重放 → 409 nonce_replay
curl -i -X POST http://localhost:8000/handovers \
  -H 'Content-Type: application/json' \
  -d '{"chain_id":"ab12...","tail_hash":"<新链尾>","receiver":"carol","nonce":"n-0001"}'

# 5) 查询链条
curl -s http://localhost:8000/chains/ab12...

# 6) 完整性校验（正常：valid=true）
curl -s http://localhost:8000/chains/ab12.../verify
```

### 待接收移交（邀约确认）示例

```bash
# 1) alice 以当前链尾发起邀约：指定 bob、1 小时有效、确认码 "s3cret"
curl -s -X POST http://localhost:8000/offers \
  -H 'Content-Type: application/json' \
  -d '{"chain_id":"ab12...","tail_hash":"<当前链尾>","receiver":"bob",
       "expires_in_seconds":3600,"confirmation_code":"s3cret"}'
# 响应：{"offer_id":"7c4e...","status":"pending","expires_at":"...", ...}
# 服务端只保存确认码摘要；alice 通过线下渠道把 offer_id 与确认码告知 bob

# 2) bob 凭 offer_id + 确认码确认 → 原子追加移交事件，邀约置为 accepted
curl -s -X POST http://localhost:8000/offers/7c4e.../confirm \
  -H 'Content-Type: application/json' \
  -d '{"confirmation_code":"s3cret"}'
# 响应与移交结果一致：{"seq":2,"event_hash":"<新链尾>","giver":"alice","receiver":"bob", ...}

# 3) 再确认一次 → 409 offer_already_accepted；确认码错误 → 403 code_mismatch；
#    过期 → 409 offer_expired；期间链尾被其他移交推进 → 409 tail_mismatch；
#    以上均不写任何事件

# 4) 查询邀约状态（pending / accepted / expired / rejected / terminated / cancelled）
curl -s http://localhost:8000/offers/7c4e...
```

### 撤销邀约示例

```bash
# 1) alice 发起「可撤销」邀约：额外提供 cancellation_code（省略则不可撤销）
curl -s -X POST http://localhost:8000/offers \
  -H 'Content-Type: application/json' \
  -d '{"chain_id":"ab12...","tail_hash":"<当前链尾>","receiver":"bob",
       "expires_in_seconds":3600,"confirmation_code":"s3cret",
       "cancellation_code":"v0uch-99"}'
# 响应：{"offer_id":"5b8d...","status":"pending","cancellable":true, ...}
# 服务端只保存绑定 offer_id 做域分隔的撤销码 SHA-256 摘要，明文不落库

# 2) bob 确认之前，alice 凭撤销码撤销（不追加任何链上事件）
curl -s -X POST http://localhost:8000/offers/5b8d.../cancel \
  -H 'Content-Type: application/json' \
  -d '{"cancellation_code":"v0uch-99"}'
# {"offer_id":"5b8d...","status":"cancelled","cancellable":true,
#  "cancelled_at":"2026-...","tail_hash":"<邀约锁定的链尾，仍是当前链尾>"}

# 3) 撤销码错误 → 403 cancel_code_mismatch
curl -i -X POST http://localhost:8000/offers/5b8d.../cancel \
  -H 'Content-Type: application/json' \
  -d '{"cancellation_code":"wrong"}'

# 4) 已撤销后再撤销 / 接收方再确认 / 见证人再表态
#    → 409 offer_not_cancellable（不写事件，链尾不变）
curl -i -X POST http://localhost:8000/offers/5b8d.../confirm \
  -H 'Content-Type: application/json' \
  -d '{"confirmation_code":"s3cret"}'

# 5) 旧邀约（创建时省略 cancellation_code，cancellable=false）撤销
#    → 409 offer_not_cancellable；邀约过期、有见证人拒绝或链尾被推进后
#      撤销同样返回 409 offer_not_cancellable

# 6) 详情与见证查询返回 cancelled 状态，但不返回撤销码摘要
curl -s http://localhost:8000/offers/5b8d...
# {"status":"cancelled","cancellable":true,"cancelled_at":"...", ...}
```

### 见证确认示例

```bash
# 1) alice 发起见证邀约：3 名见证人（各持独立令牌），阈值 2
curl -s -X POST http://localhost:8000/offers \
  -H 'Content-Type: application/json' \
  -d '{"chain_id":"ab12...","tail_hash":"<当前链尾>","receiver":"bob",
       "expires_in_seconds":3600,"confirmation_code":"s3cret",
       "witness_threshold":2,
       "witnesses":[
         {"name":"carol","token":"tok-carol-001"},
         {"name":"dave","token":"tok-dave-002"},
         {"name":"erin","token":"tok-erin-003"}]}'
# 响应（令牌明文仅此一次返回，服务端只存摘要）：
# {"offer_id":"9d1a...","status":"pending","witness_mode":true,
#  "witness_threshold":2,
#  "witnesses":[{"witness_index":0,"name":"carol","token":"tok-carol-001"}, ...]}

# 2) 各见证人凭自己的令牌表态（同意 / 拒绝）
curl -s -X POST http://localhost:8000/offers/9d1a.../witness-decisions \
  -H 'Content-Type: application/json' \
  -d '{"token":"tok-carol-001","decision":"approved"}'
# {"witness_index":0,"name":"carol","decision":"approved",
#  "status":"pending","approved_count":1,"rejected_count":0,"pending_count":2, ...}

# 3) 同意数未达阈值时接收方确认 -> 409 witness_threshold_not_met，不写事件
#    任一见证人拒绝 -> 邀约即终止：确认返回 409 offer_rejected，不写事件
#    同一令牌再表态 -> 409 witness_already_decided；令牌错误 -> 403 token_mismatch

# 4) 第二名见证人同意，同意数达到阈值 2 且无拒绝 -> bob 确认，原子追加事件
curl -s -X POST http://localhost:8000/offers/9d1a.../witness-decisions \
  -H 'Content-Type: application/json' \
  -d '{"token":"tok-dave-002","decision":"approved"}'
curl -s -X POST http://localhost:8000/offers/9d1a.../confirm \
  -H 'Content-Type: application/json' \
  -d '{"confirmation_code":"s3cret"}'

# 5) 查询见证状态（响应不含令牌及其摘要）
curl -s http://localhost:8000/offers/9d1a.../witnesses
# {"offer_id":"9d1a...","witness_mode":true,"status":"accepted",
#  "witness_threshold":2,"witness_count":3,
#  "approved_count":2,"rejected_count":0,"pending_count":1,
#  "witnesses":[{"witness_index":0,"name":"carol","decision":"approved","decided_at":"..."},
#               {"witness_index":1,"name":"dave","decision":"approved","decided_at":"..."},
#               {"witness_index":2,"name":"erin","decision":null,"decided_at":null}]}
```

校验响应示例（正常）：

```json
{
  "chain_id": "ab12...",
  "valid": true,
  "length": 3,
  "expected_next_seq": 3,
  "first_broken_seq": null,
  "reason": null,
  "recomputed_tail_hash": "…",
  "stored_tail_hash": "…"
}
```

若中途某条事件被篡改，响应形如
`{"valid": false, "first_broken_seq": 2, "reason": "事件哈希不匹配：…"}`。

## 防分叉与并发保证

1. 每次追加都在 `BEGIN IMMEDIATE` 事务内完成，且进程内写操作由互斥锁串行化；
   「读链尾 → 比对 → 插入 → 推进」是一个原子步骤。邀约确认同样如此：
   「验码 → 未过期 → 未使用 → 未撤销 → 链尾匹配 → （见证模式）无拒绝且同意数
   达阈值 → 追加事件 → 置 accepted」全部在同一立即事务内，任一校验失败即整体
   回滚。**邀约撤销、接收方确认与见证人表态共用同一把写锁与同一立即事务语义，
   三者互斥**：撤销的「验撤销码 → 仍 pending → 未过期 → 链尾未推进 → 无拒绝 →
   置 cancelled（且不追加任何链上事件）」整体原子，竞争终态（accepted /
   cancelled / rejected）中最多一个成功，撤销成功后哈希链完全不变。
   见证人表态的「按令牌摘要定位见证人 → 查未表态 → 写入 decision」也在同一
   立即事务内串行完成，同一令牌并发表态最多一次成功。
2. 数据库层设有 `UNIQUE(chain_id, seq)` 与 `UNIQUE(chain_id, nonce)` 唯一约束
   作为兜底，任何竞争写入都无法产生重号事件或重放随机数；邀约确认使用
   派生随机数 `offer:<offer_id>`，同一邀约并发确认最多成功一次。撤销则以
   `UPDATE ... WHERE status='pending'` 的影响行数兜底，竞争落败即回滚返回 409。
   见证人表设有 `UNIQUE(offer_id, witness_index)` 与
   `UNIQUE(offer_id, token_digest)`，且表态以「行已存在 decision」判定一次性，
   重复使用令牌不可能产生第二条表态。
3. WAL 日志模式 + busy timeout，兼顾读写并发与单写入者语义。
4. 数据库初始化（`init_db`）全部使用 `CREATE ... IF NOT EXISTS`，可重复执行；
   对已有数据文件只做增量补表（含 `offers`、`offer_witnesses` 表）；对旧版
   `offers` 表按 SQLite 官方「建新表 → 搬数据 → 改名」流程一次性就地重建，
   增补 `witness_threshold`（旧无见证邀约为 NULL）与 `cancel_code_digest` /
   `cancelled_at`（旧邀约撤销码为 NULL，即不可撤销），存量邀约与事件不受影响。

## 项目结构

```
.
├── app/
│   ├── main.py        # FastAPI 路由与启动
│   ├── service.py     # 哈希链核心逻辑（创建 / 追加 / 邀约确认 / 撤销 / 见证 / 查询 / 校验）
│   ├── database.py    # SQLite 连接、建表与旧 offers 表重建迁移、串行写事务
│   ├── hashing.py     # 紧凑 JSON 规范与 SHA-256
│   ├── schemas.py     # Pydantic 请求/响应模型
│   └── config.py      # 路径与端口配置
├── Dockerfile
├── docker-compose.yml
├── docker-entrypoint.sh
├── requirements.txt
└── README.md
```

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `/data` | 数据目录 |
| `DB_PATH` | `$DATA_DIR/handoff.db` | SQLite 文件路径 |
| `PORT` | `8000` | 监听端口（容器内，Compose 映射同为 8000） |
| `HOST` | `0.0.0.0` | 监听地址 |
