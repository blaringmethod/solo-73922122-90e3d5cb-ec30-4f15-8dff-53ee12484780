# 发酵实验室仪器数据归档与离线交付服务

FastAPI + paho-mqtt + SQLite 实现的 QoS 1 温度/pH 归档服务，订阅本地 Mosquitto
固定主题，按业务键去重、留存矛盾读数，并在**落库提交之后**才回 PUBACK。

新增**离线打包交付 API**：按仪器 + 批号在后台生成含 `samples.csv` 与
`conflicts.csv` 的 ZIP，两张表取自开始读取时的**同一已提交快照**，后台分批读库、
流式写盘，单进程同时只允许一个打包任务，并提供状态、取消与下载接口。

## 一、需求与实现对照

| 要求 | 实现方式 |
| --- | --- |
| 订阅本地 Mosquitto 固定主题的 QoS 1 消息 | `app/app/mqtt_archiver.py`，主题由 `MQTT_TOPIC` 固定，订阅 QoS 1 |
| JSON：仪器 ID、批号、序号、UTC 时间、温度、pH | `app/app/models.py` 的 `Measurement`（pydantic 校验，缺字段/类型错直接拒绝并 ACK） |
| 同仪器+批号+序号只留一份样本 | `samples` 表对 `(instrument_id, batch_no, sample_seq)` 加 `UNIQUE` 约束 |
| 同键同值忽略重复 | `store_measurement` 返回 `duplicate`，不新增行、不冲突 |
| 同键异值留存冲突且不改原样本 | 写入 `conflicts` 表（逐字段、逐值留痕），原 `samples` 行永不更新 |
| 提交后才发 PUBACK | paho `manual_ack=True`，worker 线程在 SQLite 写完成后才 `client.ack(mid, 1)` |
| 不能用报文编号去重 | 去重只依赖业务键的唯一约束；mid 仅用于 ACK，从不入库、不参与判重 |
| 持久会话 | 固定 `client_id` + `clean_session=False`；Mosquitto 端开启 persistence |
| 落库前/提交后确认前退出，重启续收 | 未 ACK 的消息由 broker 重投，唯一约束保证重放幂等 |
| 写库等待不堵查询和 MQTT 心跳 | 三线程模型：paho 网络线程只入队，独立 writer 线程写 SQLite，FastAPI 用独立只读连接（WAL） |
| 重连后迟到处理不能确认复用编号的另一消息 | 每条连接单调递增 `epoch`；work item 记住入站 epoch，仅当 epoch 仍是当前连接才 ACK |
| 按仪器/批号查询样本与冲突 | `GET /samples`、`GET /conflicts`，支持 `instrument_id`、`batch_no` 过滤与分页 |
| 暂停、断线演示 | `POST /demo/pause`、`/demo/pause-post-commit`、`/demo/reconnect`、`GET /demo/stats` |
| 按仪器+批号后台打包含样本 CSV、冲突 CSV 的 ZIP | `app/app/packager.py` + `POST /packages`，后台线程流式生成 `samples.csv`、`conflicts.csv`、`manifest.json` |
| 两表取自开始读取时的同一已提交快照，后续读数不混入 | 打包线程开**一个** `query_only` 只读连接，`BEGIN` 后保持单个延迟事务覆盖两张表的全部批次读取 |
| 单 API 进程仅运行一份打包 | `PackageManager` 单一槽位 + 活动任务保护，并发请求返回 `409`，任务终态后才释放 |
| 分批读库、流式写盘，不将整批装入内存 | keyset 分页（`id > ? LIMIT N`）+ 自研流式 ZIP 写入器（data descriptor），内存上限约为一个批次 |
| 不持 SQLite 写锁，MQTT 入库和查询照常 | 打包连接 `PRAGMA query_only=ON` 且只发 `SELECT`；WAL 下读不阻塞写、写不阻塞读 |
| 状态、取消、下载接口 | `GET /packages`、`GET /packages/{id}`、`POST /packages/{id}/cancel`、`GET /packages/{id}/download` |
| 取消后等读写停止、连接/文件关闭、暂存清理完才报已取消 | 取消只置标志位，工作线程在检查点收尾，`settled` 事件放行后接口才返回 `cancelled`；重复取消等待同一次收尾 |
| 刚写完待发布的 ZIP 取消后也不得发布 | 验证通过后、原子 `os.replace` 发布前设有可演示窗口，该窗口内取消直接清理暂存，下载始终 404 |
| 请求断连不撤销任务或收尾 | 工作线程独立于 HTTP 请求运行，取消只能经显式取消接口触发 |
| 完整文件才开放下载，成功包内容固定 | 发布前 `ZipFile.testzip()` 全量 CRC 校验 + SHA-256，同卷原子移动；仅 `completed` 任务可下载 |
| 失败不提供下载，重启保留成功包，未完任务标失败并清暂存 | 每次状态迁移原子写 JSON 元数据；启动 `recover()`：保留 completed，未终态任务标 failed 并删暂存 |
| 读取中、写完待发布两个暂停样例 | `POST /demo/package-pause` 的 `read_batch_seconds` 与 `pre_publish_seconds` |
| 可实际启动的 Docker Compose | 持久化 broker + API + 仪器发布样例 + `/docs`，见下 |

## 二、目录结构

```
.
├── app/                      # FastAPI + 归档服务（构建为 ferment api 镜像）
│   ├── app/
│   │   ├── config.py         # 环境变量配置
│   │   ├── models.py         # 入站 payload、出站响应与打包状态模型
│   │   ├── db.py             # SQLite schema、去重/冲突/查询（WAL）
│   │   ├── mqtt_archiver.py  # paho 手动 ACK、epoch 防护、writer 线程
│   │   ├── packager.py       # 打包槽位、快照事务、流式 ZIP、取消/重启恢复
│   │   └── main.py           # FastAPI 路由与生命周期
│   ├── requirements.txt
│   └── Dockerfile
├── broker/
│   └── mosquitto.conf        # 开启 persistence 的本地 broker
├── publisher/
│   ├── publisher.py          # 仪器模拟器（含 A–E 引导场景）
│   ├── requirements.txt
│   └── Dockerfile
├── tests/
│   ├── test_archiver.py      # 12 个单元测试（去重/冲突/epoch/暂停/校验）
│   ├── test_packager.py      # 16 个打包单元测试（快照/流式/取消/重启/槽位）
│   ├── test_api_packages.py  # 9 个打包 HTTP 接口测试（202/409/404/取消/下载）
│   └── integration_mqtt.py   # 真实 broker 的 9 步端到端集成测试
├── scripts/demo.sh           # 一键演示脚本
└── docker-compose.yml
```

## 三、快速启动

```bash
docker compose up -d --build broker api
# 等待健康检查通过
curl -s http://localhost:8000/health
# 浏览器打开交互式文档
open http://localhost:8000/docs      # Linux: xdg-open http://localhost:8000/docs
```

发布仪器样例（publisher 在 `demo` profile 中，按需启动）：

```bash
# 引导场景：5 条新样本 -> 完全重复 -> 矛盾读数 -> 突发 -> 畸形 JSON
docker compose run --rm publisher python publisher.py --guided

# 单条发布
docker compose run --rm publisher python publisher.py \
    --seq 7 --temp 31.2 --ph 6.8
```

或直接跑完整演示：

```bash
./scripts/demo.sh
```

数据落在两个具名卷里，`docker compose down` 不丢数据；`docker compose down -v`
才会清空。

## 四、API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 服务与 MQTT 连接状态 |
| GET | `/samples?instrument_id=&batch_no=&limit=&offset=` | 查样本（两过滤项均可选） |
| GET | `/conflicts?instrument_id=&batch_no=&limit=&offset=` | 查矛盾读数 |
| POST | `/packages` | 启动后台打包任务（202；已有任务时 409） |
| GET | `/packages` | 列出全部打包任务（新的在前，含终态任务） |
| GET | `/packages/{job_id}` | 查单个任务状态、行数、字节数、校验和 |
| POST | `/packages/{job_id}/cancel` | 请求取消并**等待完整收尾**后返回 |
| GET | `/packages/{job_id}/download` | 下载已完成 ZIP（其他状态一律 404） |
| POST | `/demo/package-pause` | 设置读取批次暂停 / 发布前暂停（演示用） |
| GET | `/demo/package-pause` | 查看当前打包演示暂停值 |
| GET | `/demo/stats` | 连接 epoch、队列深度、各类计数器、当前暂停值 |
| POST | `/demo/pause` `{"seconds": 8}` | writer 在**写库前**暂停（心跳/查询不受影响） |
| POST | `/demo/pause-post-commit` `{"seconds": 5}` | writer 在**提交后、PUBACK 前**暂停 |
| POST | `/demo/reconnect` | 主动断开 TCP 并自动重连（持久会话保留） |

## 五、离线打包交付（重点）

### 1. 一次典型流程

```bash
# 1) 提交任务：后台线程开始工作，立即返回 job_id（HTTP 202）
curl -sS -X POST http://localhost:8000/packages \
  -H 'Content-Type: application/json' \
  -d '{"instrument_id": "FERM-01", "batch_no": "BATCH-2026-011"}'

# 2) 轮询状态：pending -> reading -> verifying -> publishing -> completed
curl -sS http://localhost:8000/packages/<job_id>

# 3) 仅 completed 可下载；响应头带 X-Job-Id 与 X-SHA256
curl -sS -OJ http://localhost:8000/packages/<job_id>/download
unzip -l FERM-01_BATCH-2026-011_xxxxxxxx.zip
#   samples.csv       样本快照（表头 + 数据行）
#   conflicts.csv     矛盾读数快照（表头 + 数据行）
#   manifest.json     仪器、批号、快照时刻、两表行数清单
```

### 2. 同一已提交快照如何保证

打包线程在读取任何数据前，打开**一个** `query_only` 只读连接并执行一次
`BEGIN`。该连接保持单个延迟事务，依次分页读取 `samples` 与 `conflicts`
两张表，全部读完才 `COMMIT` 并关连接。SQLite 事务保证这期间所有批次看到的
都是事务开始那一刻的已提交状态；MQTT writer 后续提交的新读数不会进入 ZIP。
只读连接只发 `SELECT`，配合 WAL：打包不阻塞入库，入库与查询也不阻塞打包。

### 3. 分批读库 + 流式写盘，内存与锁

- 查询用不可变主键做 keyset 分页：`... WHERE id > ? ORDER BY id LIMIT N`，
  默认每批 500 行（`PACKAGE_BATCH_SIZE` 可调），避免 OFFSET 全表扫描；
- 每批 CSV 序列化后立即写入自研流式 ZIP 写入器（local header + 逐块
  deflate + data descriptor + 收尾中央目录，含 ZIP64 扩展），随即释放，
  峰值内存约等于一个批次而非整批数据；
- ZIP 先落在 `packages/staging/<job_id>/package.zip.tmp`，经
  `ZipFile.testzip()` 全量 CRC 校验与 SHA-256 计算后，用同卷
  `os.replace` 原子移动到 `packages/published/`，外部永远看不到半成品。

### 4. 取消语义（最严格的部分）

- `cancel` 只设置取消标志；工作线程在「批次之间 / 发布前窗口 / 校验后」
  检查点响应，随后关闭数据库连接与 ZIP 文件、删除整个暂存目录，最后才把
  状态置为 `cancelled` 并唤醒等待者；
- 取消接口同步等待 `settled` 事件，返回 `cancelled` 时可以确信：读写已停、
  连接和文件已关、暂存已清、**不会再有发布动作**；
- 重复取消共享同一个 `settled` 事件，等待的是同一次收尾，不存在竞态清理；
- 即使 ZIP 已完整写盘并通过校验，只要还没执行原子发布（`pre_publish`
  窗口），取消照样压制该文件；
- HTTP 客户端断开连接与任务生命周期完全无关：任务继续跑、收尾继续做；
- 已 `completed` 的包内容固定不可变，对它调取消只返回现状、不删包；
- 旧任务的收尾只操作自己的 `staging/<job_id>/` 目录与自己的元数据，
  新任务使用另一个 job 目录，互不影响。

### 5. 重启恢复

任务每次状态迁移都原子写 `packages/jobs/<job_id>.json`。进程启动时
`recover()`：

- `completed` 且发布文件仍在：保留并继续提供下载；文件丢失则改判 `failed`；
- 其他非终态任务（含 `pending/reading/verifying/publishing`）：一律标记
  `failed`，记录原因并删除其暂存目录；
- 无元数据的孤儿暂存目录直接清除；恢复后打包槽位为空，可立即接新任务。

### 6. 两个暂停样例（`/docs` 可直接演示）

```bash
# 样例 A：读取中暂停——每读完一个 DB 批次停 2 秒，便于观察 reading 并中途取消
curl -sS -X POST http://localhost:8000/demo/package-pause \
  -H 'Content-Type: application/json' \
  -d '{"read_batch_seconds": 2.0, "pre_publish_seconds": 0.0}'

# 样例 B：写完待发布暂停——ZIP 已写完校验完，发布前停留 10 秒，
#         此时下载仍为 404，在窗口内取消可压制刚写完的 ZIP
curl -sS -X POST http://localhost:8000/demo/package-pause \
  -H 'Content-Type: application/json' \
  -d '{"read_batch_seconds": 0.0, "pre_publish_seconds": 10.0}'

# 清零恢复正常
curl -sS -X POST http://localhost:8000/demo/package-pause \
  -H 'Content-Type: application/json' \
  -d '{"read_batch_seconds": 0.0, "pre_publish_seconds": 0.0}'
```

完整脚本化演示（含下载验包、读取中取消、发布前取消）：`./scripts/demo.sh`
的第 9–12 步。

## 六、关键正确性设计

### 1. 三线程 + 有界队列，写库不堵心跳

- paho 网络线程：仅解析报文、把 work item 放入内存队列后立即返回，
  因此 keep-alive 的 PINGREQ/PINGRESP 永远不会被慢写库阻塞。
- 唯一 writer 线程：串行执行所有 SQLite 写并在提交后手动 ACK。
- FastAPI 事件循环：每次查询开一个 `query_only` 只读连接；SQLite 处于
  WAL 模式，读不阻塞写、写不阻塞读。
- `max_inflight_messages` 限制在途窗口，形成天然背压，避免无界堆积。

### 2. PUBACK 在提交之后，崩溃靠重放收敛

paho 以 `manual_ack=True` 创建，broker 只有在收到 PUBACK 后才移除该 QoS 1
消息。若进程在「提交成功 → PUBACK 发出」窗口内被杀，重启后 broker 重投同一
消息，而 `samples` / `conflicts` 的唯一约束让重放变成 `duplicate` /
`conflict_duplicate`，不会产生重复行。

### 3. 绝不用报文编号（mid）去重

mid 的作用域是单条 TCP 连接，重连后会被复用。去重只认业务键
`(instrument_id, batch_no, sample_seq)`。每条连接分配单调递增的 `epoch`，
work item 记录入站时的 epoch：

- 若 ACK 时发现 item 的 epoch 已不是当前连接（旧连接的迟到处理结果），
  **绝不**调用 `ack()`——即使新连接上同一 mid 已属于另一条消息；
- 该消息保持未确认，由 broker 按持久会话重投；
- 计数器 `stale_drops` 记录这种被丢弃的过期 ACK 决策。

### 4. 矛盾读数的留存规则

- 温度或 pH 任一不同，就为每个不同字段写一行 `conflicts`，记录原值、新值、
  新报文的 UTC 时间与完整 payload；
- 同一条矛盾消息重投（同键、同字段、同新值）不会产生第二行；
- 两个不同的矛盾值会各自留一行，形成完整矛盾历史；
- 原始 `samples` 行在任何路径下都不会被修改。

### 5. 打包任务的并发、取消与发布规则

- **单槽位**：`PackageManager` 用一个活动任务 ID 保护整个进程的打包能力，
  任务进入终态（completed/cancelled/failed）并完成收尾后才释放，并发提交
  统一收到 `409`；
- **快照优先**：工作线程先开只读延迟事务、记录 `snapshot_at`，再开始任何
  读取，两张表共享同一事务，事后提交的数据物理上不可见；
- **无写锁**：打包连接 `query_only` 且仅执行 `SELECT`，不触碰 writer 长连接，
  MQTT 入库线程与 API 查询线程全程并行；
- **取消可证**：取消不落库为终态，只置标志位；真正的关闭与清理由工作线程
  执行，`settled` 事件是「已收尾」的唯一权威，接口在事件放行后才返回；
- **发布原子**：暂存文件经 CRC + SHA-256 验证后同卷 `os.replace` 发布，
  下载只认 `published/` 目录中属于 `completed` 任务的文件，失败与取消路径
  没有任何可下载产物。

## 七、本地测试（无需 Docker）

```bash
python3 -m venv --without-pip .venv
curl -sS https://bootstrap.pypa.io/get-pip.py | .venv/bin/python
.venv/bin/pip install -r app/requirements.txt amqtt httpx

# 全部单元/接口测试（37 个：归档 12 + 打包 16 + 打包 HTTP 9）
PYTHONPATH=app .venv/bin/python -m unittest discover -s tests -v

# 仅打包相关测试
PYTHONPATH=app .venv/bin/python -m unittest tests.test_packager -v
PYTHONPATH=app .venv/bin/python -m unittest tests.test_api_packages -v

# 端到端集成测试（内置纯 Python MQTT broker，9 个断言步骤）
PYTHONPATH=app .venv/bin/python tests/integration_mqtt.py
```

打包测试覆盖：快照隔离（快照后新提交不混入）、ZIP 内两 CSV + manifest 内容、
小批次大表流式、单槽位 409、读取中取消、发布前窗口取消、重复取消同次收尾、
客户端断连不取消、重启后成功包保留/未完任务标失败清暂存、仅 completed 可下载。

> 说明：测试用的 amqtt broker 不为离线会话排队 QoS 1 消息，因此「离线期间
> 发布、上线后重投」这一步在测试里通过显式重放等价验证；生产使用的
> Mosquitto 已配置 `persistence true`，原生支持离线消息排队与重投。

## 八、消息格式

主题（固定）：`lab/fermenter/measurement`

```json
{
  "instrument_id": "FERM-01",
  "batch_no": "BATCH-2026-011",
  "sample_seq": 3,
  "ts_utc": "2026-11-13T08:30:00.000+00:00",
  "temperature_c": 30.5,
  "ph": 6.79
}
```

多余字段会被忽略（向前兼容），必填字段缺失或类型错误的报文被记录为
`invalid_payloads` 并确认掉，避免毒消息永久阻塞队列。
