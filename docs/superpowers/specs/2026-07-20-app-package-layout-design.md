# 编排服务 app 目录重构设计

## 目标

将仓库内 Python 包目录从 `vision_orchestrator/` 调整为 `app/`，并采用常见 FastAPI 项目入口 `app/main.py`。Docker 容器项目根目录统一为 `/app`，不再使用 `/workspace`。

## 目录结构

```text
jy-vision-orchestrator-server/
├── app/
│   ├── main.py
│   ├── config.toml.example
│   ├── core/
│   │   ├── config.py
│   │   ├── config_loader.py
│   │   └── bootstrap.py
│   ├── api/
│   ├── application/
│   ├── domain/
│   ├── infrastructure/
│   └── docker/
├── scripts/
└── tests/
```

## Python 运行约定

- Python 包名由 `vision_orchestrator` 改为 `app`。
- 原 `vision_orchestrator/app.py` 改为 `app/main.py`。
- 所有内部导入统一使用 `from app...`。
- 本地入口统一为 `python -m app.main`。
- 不保留旧 Python 包导入兼容层，避免同时维护两套包路径。

## Docker 运行约定

```text
/app/                       容器项目根目录和 WORKDIR
/app/app/                   Python 包源码
/app/scripts/               构建辅助脚本
/app/config.toml            运行时只读挂载的配置文件
/mnt                        快照目录挂载点
```

Docker启动命令统一为：

```bash
python -m app.main --config /app/config.toml serve
python -m app.main --config /app/config.toml worker
```

Dockerfile中的 Cython 构建根目录改为 `/app`，编译包改为 `app`。Compose和 `docker run` 示例将配置文件挂载到 `/app/config.toml`。

## 保持不变的契约

- 配置段继续使用 `[Vision_Orchestrator]`，并兼容旧 `[AI_Quality]`。
- `VISION_ORCHESTRATOR_WORKER_ID`、Redis key前缀、Kafka消费组保持不变。
- FastAPI接口路径、Kafka消息格式、数据库表和TIAS调度协议保持不变。
- 镜像名、容器名和服务名继续使用 `vision-orchestrator`。

## 验证标准

1. 仓库不存在 `vision_orchestrator/` 目录。
2. Python源码和测试中不存在 `from vision_orchestrator` 或 `import vision_orchestrator`。
3. `python -m app.main --help` 正常运行。
4. 示例配置可以从 `app/config.toml.example` 加载。
5. Docker Compose配置可以正常解析，容器工作目录为 `/app`。
6. 全量自动化测试通过。
