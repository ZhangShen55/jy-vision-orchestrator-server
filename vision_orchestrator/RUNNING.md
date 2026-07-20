# vision_orchestrator 运行文档

## 服务定位

`vision_orchestrator` 是课堂质量视觉分析调度服务，和 `tias` 推理服务独立运行。

它包含两个入口：

| 入口 | 命令 | 作用 |
| --- | --- | --- |
| API 控制面 | `python -m vision_orchestrator.app --config <config> serve` | 接收 TIAS 注册心跳，查询状态，修改 Worker 集群期望状态 |
| 受控 Kafka Worker | `python -m vision_orchestrator.app --config <config> worker` | 按 Redis `desired_state` 消费 `classroom_cv_task`，抽帧后调度 TIAS |
| 兼容 Kafka Worker | `python -m vision_orchestrator.app --config <config> consume` | 旧入口，启动后直接消费 Kafka，不受 Redis 控制 |

`vision_orchestrator` 不写 `lesson_ai_job`，视觉分析状态和结果写入 `lesson_ai_workflow`、`lesson_behavior_timeline`、`lesson_snapshot_event`、`lesson_student_behavior_stat`、`indicator_score_result`。

## 运行前准备

从仓库根目录执行：

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r vision_orchestrator/requirements.txt
```

准备配置文件：

```bash
cp vision_orchestrator/config.toml.example vision_orchestrator/config.toml
```

关键配置位于 `[Vision_Orchestrator]`：

| 配置 | 说明 |
| --- | --- |
| `KafkaBootstrapServers` | Kafka 地址，例如 `127.0.0.1:9092` |
| `KafkaTopic` | 课堂视觉任务 topic，固定使用 `classroom_cv_task` |
| `KafkaGroupId` | Kafka consumer group |
| `HttpHost` / `HttpPort` | vision_orchestrator HTTP 注册服务监听地址 |
| `RedisUrl` / `RedisKeyPrefix` | TIAS 注册表 Redis 地址和 key 前缀 |
| `WorkerControlEnabled` | 是否启用 Worker 控制接口 |
| `WorkerControlKey` / `WorkerControlHeaderName` | 控制接口鉴权 key 和 header 名 |
| `WorkerControlStateKey` | Redis 中保存 Worker 期望状态的 key |
| `WorkerRegistryKeyPrefix` | Redis 中保存 Worker 注册表的 key 前缀 |
| `WorkerDefaultDesiredState` | Redis 未设置状态时的默认状态，建议 `PAUSED` |
| `WorkerHeartbeatIntervalSeconds` / `WorkerHeartbeatTimeoutSeconds` | Worker 心跳间隔和过期时间 |
| `WorkerPollWhenPausedSeconds` | Worker 暂停状态下的 sleep 时间 |
| `TiasInferenceMode` | `remote` 表示通过 HTTP 调用 TIAS |
| `TiasBatchSize` | 每次发给 TIAS 的帧批大小 |
| `DBHost` / `DBPort` / `DBUser` / `DBPassword` / `DBName` | 业务库连接 |
| `SnapshotMountRoot` | 快照写入根目录 |
| `SnapshotRelativePrefix` | 快照入库相对路径前缀，默认 `cv` |
| `SnapshotScale` | 快照缩放比例，默认 `0.25` |
| `FrameIntervalSeconds` | 视频抽帧间隔 |
| `MaxFramesPerVideo` | `0` 表示全量；本地冒烟测试可临时设为 `1` |
| `LocalVideoBaseRoot` | 本地视频相对路径基准目录；为空时按 vision_orchestrator 进程当前工作目录解析 |

## Docker 部署 Redis

本地没有 Redis 时，可以直接用 Docker 拉起。

快速启动：

```bash
docker run -d \
  --name vision-orchestrator-redis \
  -p 6379:6379 \
  redis:7-alpine
```

带本地持久化目录启动：

```bash
mkdir -p /tmp/vision-orchestrator-redis-data
docker run -d \
  --name vision-orchestrator-redis \
  -p 6379:6379 \
  -v /tmp/vision-orchestrator-redis-data:/data \
  redis:7-alpine \
  redis-server --appendonly yes
```

检查 Redis：

```bash
docker exec vision-orchestrator-redis redis-cli ping
```

期望输出：

```text
PONG
```

停止或重建：

```bash
docker rm -f vision-orchestrator-redis
```

对应配置：

```toml
RedisUrl = "redis://127.0.0.1:6379/0"
RedisKeyPrefix = "vision_orchestrator:tias"
TiasHeartbeatTimeoutSeconds = 15
```

## 挂载快照目录

生产或联调环境可先把 NFS 挂载到仓库的 `mnt`。实际 NFS 地址由部署环境提供：

```bash
mkdir -p "$PWD/mnt"
mount -t nfs -o nolock,vers=3,tcp \
  <NFS_HOST>:/image \
  "$PWD/mnt"
```

检查挂载：

```bash
mount | grep '<NFS_HOST>:/image'
df -h "$PWD/mnt"
```

配置示例：

```toml
SnapshotMountRoot = "/absolute/path/to/jy-vision-orchestrator-server/mnt"
SnapshotRelativePrefix = "cv"
SnapshotScale = 0.25
```

Docker 容器内必须使用容器路径：

```toml
SnapshotMountRoot = "/mnt"
SnapshotRelativePrefix = "cv"
```

入库的 `image_url` 只保存相对路径，例如：

```text
cv/lesson-mul-test-6run-0001/student-5-0000.png
```

## 推荐部署形态

第一版推荐：

```text
vision_orchestrator-api x 1
vision_orchestrator-worker x N
Redis x 1
TIAS x N
```

说明：

- `vision_orchestrator-api` 是轻量控制面，默认单实例足够，单实例使用 `uvicorn --workers 1`。
- 如需 API 高可用，可启动 2 个 `vision_orchestrator-api` 并在上层接 Nginx/LB。
- `vision_orchestrator-worker` 独立扩容，数量受 Kafka partition 数、TIAS 容量和数据库写入能力共同限制。
- `/api/worker-control/resume|pause|drain` 只修改 Redis 期望状态，不启动或杀死本地进程。

## 启动 vision_orchestrator API 控制面

```bash
export CONFIG_PATH="$PWD/vision_orchestrator/config.toml"
python -m vision_orchestrator.app --config "$CONFIG_PATH" serve
```

默认监听：

```text
http://127.0.0.1:9000
```

接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/tias/instances/register` | TIAS 启动注册 |
| POST | `/api/tias/instances/heartbeat` | TIAS 心跳续约 |
| POST | `/api/tias/instances/unregister` | TIAS 注销 |
| GET | `/api/health` | vision_orchestrator API 健康检查 |
| GET | `/api/tias/instances` | 查询当前可用 TIAS 实例 |
| GET | `/api/workers` | 查询当前 Worker 实例 |
| GET | `/api/worker-control/state` | 查询 Worker 集群期望状态 |
| POST | `/api/worker-control/resume` | 设置期望状态为 `RUNNING` |
| POST | `/api/worker-control/pause` | 设置期望状态为 `PAUSED` |
| POST | `/api/worker-control/drain` | 设置期望状态为 `DRAINING` |

检查端口：

```bash
lsof -nP -iTCP:9000 -sTCP:LISTEN
```

健康检查：

```bash
curl http://127.0.0.1:9000/api/health
```

## 启动受控 Kafka Worker

确认 Redis、vision_orchestrator HTTP、至少一个 TIAS 实例都已启动并注册后，再启动 Worker：

```bash
export CONFIG_PATH="$PWD/vision_orchestrator/config.toml"
VISION_ORCHESTRATOR_WORKER_ID=worker-local-1 \
python -m vision_orchestrator.app --config "$CONFIG_PATH" worker
```

默认 `WorkerDefaultDesiredState = "PAUSED"` 时，Worker 只上报心跳，不拉 Kafka。启动消费：

```bash
curl -X POST http://127.0.0.1:9000/api/worker-control/resume \
  -H "X-VISION-ORCHESTRATOR-KEY: change-me" \
  -H "Content-Type: application/json" \
  -d '{"updated_by":"operator","reason":"manual resume"}'
```

暂停拉新消息：

```bash
curl -X POST http://127.0.0.1:9000/api/worker-control/pause \
  -H "X-VISION-ORCHESTRATOR-KEY: change-me" \
  -H "Content-Type: application/json" \
  -d '{"updated_by":"operator","reason":"manual pause"}'
```

优雅排空：

```bash
curl -X POST http://127.0.0.1:9000/api/worker-control/drain \
  -H "X-VISION-ORCHESTRATOR-KEY: change-me" \
  -H "Content-Type: application/json" \
  -d '{"updated_by":"operator","reason":"deploy drain"}'
```

查询 Worker：

```bash
curl http://127.0.0.1:9000/api/workers
```

Worker 处于 `RUNNING` 后，消费到消息会：

1. 标记 `lesson_ai_workflow` 为处理中。
2. 准备学生视频和教师视频；URL 会先下载，本地文件路径会直接读取。
3. 按 `FrameIntervalSeconds` 抽帧。
4. 按 `TiasBatchSize` 切小批次。
5. 从 Redis 注册表选择 TIAS 实例。
6. 调用 TIAS 学生和教师推理接口。
7. 聚合指标、保存快照、写入结果表。
8. 标记 `lesson_ai_workflow` 成功或失败。
9. 提交 Kafka offset。

## 兼容旧 consume 入口

旧入口仍保留：

```bash
python -m vision_orchestrator.app --config "$CONFIG_PATH" consume
```

该入口启动后直接消费 Kafka，不读取 Redis `desired_state`，只用于兼容和紧急回退。

## Docker 部署目录

部署资产集中在：

```text
vision_orchestrator/docker/
├── Dockerfile
├── docker-compose.yml
├── env.example
├── nginx.conf.example
└── README.md
```

本地示例：

```bash
cp vision_orchestrator/config.toml.example vision_orchestrator/config.toml
docker compose -f vision_orchestrator/docker/docker-compose.yml up --build
```

单独构建普通镜像：

```bash
docker build -f vision_orchestrator/docker/Dockerfile -t vision-orchestrator:6.0 .
```

构建 Cython 保护镜像：

```bash
docker build -f vision_orchestrator/docker/Dockerfile \
  --build-arg PROTECT_SOURCE=1 \
  -t vision-orchestrator:6.0-protected .
```

API 容器示例：

```bash
docker run -d \
  --name vision-orchestrator-api \
  -p 9000:9000 \
  -e CONFIG_PATH=/workspace/vision_orchestrator/config.toml \
  -v "$PWD/vision_orchestrator/config.toml:/workspace/vision_orchestrator/config.toml:ro" \
  -v "$PWD/mnt:/mnt" \
  vision-orchestrator:6.0 \
  python -m vision_orchestrator.app --config /workspace/vision_orchestrator/config.toml serve
```

Worker 容器示例：

```bash
docker run -d \
  --name vision-orchestrator-worker-1 \
  -e CONFIG_PATH=/workspace/vision_orchestrator/config.toml \
  -e VISION_ORCHESTRATOR_WORKER_ID=worker-1 \
  -v "$PWD/vision_orchestrator/config.toml:/workspace/vision_orchestrator/config.toml:ro" \
  -v "$PWD/mnt:/mnt" \
  vision-orchestrator:6.0 \
  python -m vision_orchestrator.app --config /workspace/vision_orchestrator/config.toml worker
```

如果 Kafka 消息中的 `teacher_video_path`、`student_video_path`、`slides_video_path` 使用本地文件路径，Worker 容器必须能访问这些路径。生产建议使用共享挂载目录，并在 `docker run` 中挂载到容器内一致路径：

```bash
docker run -d \
  --name vision-orchestrator-worker-1 \
  -e CONFIG_PATH=/workspace/vision_orchestrator/config.toml \
  -e VISION_ORCHESTRATOR_WORKER_ID=worker-1 \
  -v "$PWD/vision_orchestrator/config.toml:/workspace/vision_orchestrator/config.toml:ro" \
  -v "$PWD/mnt:/mnt" \
  -v "/data/course-videos:/data/course-videos:ro" \
  vision-orchestrator:6.0 \
  python -m vision_orchestrator.app --config /workspace/vision_orchestrator/config.toml worker
```

如果 Kafka 中传的是相对路径，可配置：

```toml
LocalVideoBaseRoot = "/data/course-videos"
```

如果部署 2 个 API 实例做高可用，可参考：

```text
vision_orchestrator/docker/nginx.conf.example
```

Nginx 只代理 API，不代理 Worker，也不会提升 Kafka 消费并发。控制接口请求打到任意 API 实例，都只读写 Redis 中的共享状态。

2 个 API 实例示例：

```bash
docker run -d --name vision-orchestrator-api-1 -p 9001:9000 \
  -v "$PWD/vision_orchestrator/config.toml:/workspace/vision_orchestrator/config.toml:ro" \
  -v "$PWD/mnt:/mnt" \
  vision-orchestrator:6.0 python -m vision_orchestrator.app --config /workspace/vision_orchestrator/config.toml serve

docker run -d --name vision-orchestrator-api-2 -p 9002:9000 \
  -v "$PWD/vision_orchestrator/config.toml:/workspace/vision_orchestrator/config.toml:ro" \
  -v "$PWD/mnt:/mnt" \
  vision-orchestrator:6.0 python -m vision_orchestrator.app --config /workspace/vision_orchestrator/config.toml serve
```

## 模拟 Kafka 消息

项目内提供脚本：

```bash
python scripts/produce_vision_orchestrator_kafka_message.py \
  --bootstrap-servers 127.0.0.1:9092 \
  --topic classroom_cv_task \
  --message tests/fixtures/lesson_message.json
```

如果需要固定任务 ID：

```bash
python scripts/produce_vision_orchestrator_kafka_message.py \
  --bootstrap-servers 127.0.0.1:9092 \
  --topic classroom_cv_task \
  --message tests/fixtures/lesson_message.json \
  --task-id lesson-local-test-0001 \
  --course-id cv-local-test-0001 \
  --student-count 38 \
  --no-unique-task-id
```

本地视频路径样例：

```bash
python scripts/produce_vision_orchestrator_kafka_message.py \
  --bootstrap-servers 127.0.0.1:9092 \
  --topic classroom_cv_task \
  --message tests/fixtures/vision_orchestrator_lesson_local_path.json
```

本地路径要求：

- `teacher_video_path` 和 `student_video_path` 必须存在、是普通文件、当前 vision_orchestrator Worker 进程可读。
- `slides_video_path` 仍不参与主流程抽帧，但如果传入本地路径，也必须存在且可读。
- URL 输入仍会下载到任务临时目录；本地文件输入直接读取源文件，任务结束后不会删除源文件。

## 本地视频文件服务

本地课程视频较大时，可以用 Docker Nginx 暴露本地目录，避免反复从远端拉取：

```bash
docker run -d \
  --name vision-orchestrator-course-nginx \
  -p 18080:80 \
  -v "/data/course-videos:/usr/share/nginx/html:ro" \
  nginx:1.27-alpine
```

检查视频：

```bash
curl -I "http://127.0.0.1:18080/%E6%95%99%E5%B8%882.mp4"
curl -I "http://127.0.0.1:18080/%E5%AD%A6%E7%94%9F1.mp4"
curl -I "http://127.0.0.1:18080/PPT.mp4"
```

停止文件服务：

```bash
docker rm -f vision-orchestrator-course-nginx
```

## 单条 JSON 调试

不经过 Kafka 时，可直接运行一条消息：

```bash
python -m vision_orchestrator.app --config "$CONFIG_PATH" run-json tests/fixtures/vision_orchestrator_lesson_message.json
```

## 运行状态检查

查看 TIAS 注册表：

```bash
python - <<'PY'
from vision_orchestrator.infrastructure.tias.registry import RedisTiasRegistry

registry = RedisTiasRegistry("redis://127.0.0.1:6379/0", "vision_orchestrator:tias", 15)
for instance in sorted(registry.list_instances(), key=lambda item: item.instance_id):
    print(
        instance.instance_id,
        instance.status,
        "running", instance.running_batches,
        "queued", instance.queued_batches,
        "success", instance.success_count,
        "failed", instance.failure_count,
    )
PY
```

查看数据库结果：

```sql
select task_id,status,progress,note,error_msg,started_at,completed_at
from lesson_ai_workflow
where task_id = '<task_id>';

select count(*) from lesson_behavior_timeline where task_id = '<task_id>';
select count(*) from lesson_snapshot_event where task_id = '<task_id>';
select count(*) from lesson_student_behavior_stat where task_id = '<task_id>';
select count(*) from indicator_score_result where task_id = '<task_id>';
```

`lesson_ai_job` 由上游生产者服务负责，本服务不创建、更新或删除该表的数据。

## 常见问题

### topic 不一致

Worker 只会消费配置中的 `KafkaTopic`。如果上游投递到 `classroom_asr_task`，而这里配置为 `classroom_cv_task`，Worker 会一直等待，不会处理那条消息。

### 消费过程中被踢出 consumer group

完整视频处理时间较长，`KafkaMaxPollIntervalMs` 必须大于单条任务最大处理时长。当前建议值：

```toml
KafkaMaxPollIntervalMs = 7200000
KafkaMaxPollRecords = 1
```

### 没有可用 TIAS 实例

检查顺序：

1. Redis 是否可访问。
2. vision_orchestrator HTTP 注册服务是否启动。
3. TIAS `VisionOrchestratorBaseUrl` 是否指向 vision_orchestrator HTTP 地址。
4. TIAS 心跳是否过期。
5. TIAS `capabilities` 是否包含学生和教师推理能力。

### 快照写入失败

检查顺序：

1. `SnapshotMountRoot` 是否存在。
2. 当前用户是否可写。
3. NFS 是否仍然挂载。
4. `SnapshotRelativePrefix` 是否为预期前缀。
