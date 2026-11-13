# 发酵实验室仪器数据归档服务

FastAPI + paho-mqtt + SQLite 实现的 QoS 1 温度/pH 归档服务，订阅本地 Mosquitto
固定主题，按业务键去重、留存矛盾读数，并在**落库提交之后**才回 PUBACK。

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
| 可实际启动的 Docker Compose | 持久化 broker + API + 仪器发布样例 + `/docs`，见下 |

## 二、目录结构

```
.
├── app/                      # FastAPI + 归档服务（构建为 ferment api 镜像）
│   ├── app/
│   │   ├── config.py         # 环境变量配置
│   │   ├── models.py         # 入站 payload 与出站响应模型
│   │   ├── db.py             # SQLite schema、去重/冲突/查询（WAL）
│   │   ├── mqtt_archiver.py  # paho 手动 ACK、epoch 防护、writer 线程
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
| GET | `/demo/stats` | 连接 epoch、队列深度、各类计数器、当前暂停值 |
| POST | `/demo/pause` `{"seconds": 8}` | writer 在**写库前**暂停（心跳/查询不受影响） |
| POST | `/demo/pause-post-commit` `{"seconds": 5}` | writer 在**提交后、PUBACK 前**暂停 |
| POST | `/demo/reconnect` | 主动断开 TCP 并自动重连（持久会话保留） |

## 五、关键正确性设计

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

## 六、本地测试（无需 Docker）

```bash
python3 -m venv --without-pip .venv
curl -sS https://bootstrap.pypa.io/get-pip.py | .venv/bin/python
.venv/bin/pip install -r app/requirements.txt amqtt httpx

# 单元测试（12 个）
PYTHONPATH=app .venv/bin/python -m unittest discover -s tests -v

# 端到端集成测试（内置纯 Python MQTT broker，9 个断言步骤）
PYTHONPATH=app .venv/bin/python tests/integration_mqtt.py
```

集成测试覆盖：新样本入库、完全重复收敛、矛盾留存且原样本不变、写库暂停期间
连接存活、强制重连 epoch 递增、进程重启后重放幂等。

> 说明：测试用的 amqtt broker 不为离线会话排队 QoS 1 消息，因此「离线期间
> 发布、上线后重投」这一步在测试里通过显式重放等价验证；生产使用的
> Mosquitto 已配置 `persistence true`，原生支持离线消息排队与重投。

## 七、消息格式

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
