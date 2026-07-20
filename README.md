# jy-vision-orchestrator-server

课堂视觉任务编排服务。服务消费 Kafka 课堂任务，准备 URL 或本地路径视频，按小批次调度已注册的 TIAS 推理实例，并将课堂视觉指标、行为时间线和核心快照写入业务库。

## 服务边界

- `vision_orchestrator API`：接收 TIAS 注册、心跳和注销，查询 TIAS/Worker 注册表，维护 Worker 集群期望状态。
- `vision_orchestrator Worker`：消费 Kafka、抽帧、调度 TIAS、聚合结果、写数据库和快照目录。
- `TIAS`：独立部署的视觉模型推理服务，本仓库不包含 TIAS 模型和推理源码。
- `Redis`：共享 TIAS 注册表、Worker 注册表和 Worker 控制状态。

服务不写 `lesson_ai_job`，该表由上游任务生产者负责。服务写入 `lesson_ai_workflow`、`lesson_behavior_timeline`、`lesson_snapshot_event`、`lesson_student_behavior_stat` 和 `indicator_score_result`。

## 目录结构

```text
app/                      服务源码、配置示例和运行文档
  api/                     FastAPI 控制面
  application/             任务编排流程
  domain/                  指标、评分和快照策略
  infrastructure/          Kafka、Redis、MySQL、视频与 TIAS 客户端
  docker/                  Dockerfile、Compose 和 Nginx 示例
scripts/                   Kafka 模拟投递和 Cython 构建脚本
tests/                     单元测试与通用消息样例
```

## 本地启动

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r app/requirements.txt
cp app/config.toml.example app/config.toml
```

按环境修改 `app/config.toml` 后，分别启动 API 和 Worker：

```bash
python -m app.main --config app/config.toml serve
VISION_ORCHESTRATOR_WORKER_ID=worker-local-1 \
python -m app.main --config app/config.toml worker
```

默认 Worker 状态为 `PAUSED`。通过配置的控制请求头调用 `/api/worker-control/resume` 后开始消费。

## 测试

```bash
python -m pytest -q
```

完整配置说明和部署步骤见 [运行文档](app/RUNNING.md) 与 [Docker 部署文档](app/docker/README.md)。

## 迁移兼容

- 新配置段为 `[Vision_Orchestrator]`，仍可读取旧 `[AI_Quality]` 配置段。
- 新 Worker 环境变量为 `VISION_ORCHESTRATOR_WORKER_ID`，仍兼容旧 `AI_QUALITY_WORKER_ID`。
- Kafka consumer group 和现有 HTTP 接口路径保持不变，避免迁移时重复消费或影响 TIAS 注册。
