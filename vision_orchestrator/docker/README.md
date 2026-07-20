# vision_orchestrator Docker 部署说明

`vision_orchestrator/docker/` 存放 vision_orchestrator API 和 Worker 的部署示例。

第一版推荐：

- `vision_orchestrator-api` 默认单实例，`uvicorn workers=1`
- `vision_orchestrator-worker` 按 Kafka partition、TIAS 容量和课程并发独立扩容
- Redis 保存 Worker 控制状态、Worker 注册表和 TIAS 注册表
- Nginx 只在部署 2 个 API 实例做高可用时使用

本地示例：

```bash
cp vision_orchestrator/config.toml.example vision_orchestrator/config.toml
mkdir -p mnt
docker compose -f vision_orchestrator/docker/docker-compose.yml up --build
```

容器内配置要求：

```toml
SnapshotMountRoot = "/mnt"
RedisUrl = "redis://redis:6379/0"
```

如果不用 compose，而是手动 `docker run`，需要先把 Redis、API 和 Worker 放到同一个 Docker 网络。此时 `config.toml` 中 Redis 地址要使用 Redis 容器名：

```toml
RedisUrl = "redis://vision-orchestrator-redis:6379/0"
```

启动 Redis 容器：

```bash
docker network create vision-orchestrator-net || true
docker run -d \
  --name vision-orchestrator-redis \
  --network vision-orchestrator-net \
  -p 6379:6379 \
  redis:7-alpine
```

如果 Redis 容器已经启动，但还没有加入网络，执行：

```bash
docker network connect vision-orchestrator-net vision-orchestrator-redis || true
```

如果宿主机使用 NFS，先挂载到项目 `mnt`，并替换实际 NFS 地址：

```bash
mkdir -p "$PWD/mnt"
mount -t nfs -o nolock,vers=3,tcp <NFS_HOST>:/image "$PWD/mnt"
```

构建 Cython 保护镜像：

```bash
docker build -f vision_orchestrator/docker/Dockerfile \
  --build-arg PROTECT_SOURCE=1 \
  -t vision-orchestrator:6.0-protected .
```

API 启动命令：

```bash
python -m vision_orchestrator.app --config /workspace/vision_orchestrator/config.toml serve
```

Worker 启动命令：

```bash
python -m vision_orchestrator.app --config /workspace/vision_orchestrator/config.toml worker
```

Docker run 示例：

```bash
docker run -d \
  --name vision-orchestrator-api \
  --network vision-orchestrator-net \
  -p 9000:9000 \
  -e CONFIG_PATH=/workspace/vision_orchestrator/config.toml \
  -v "$PWD/vision_orchestrator/config.toml:/workspace/vision_orchestrator/config.toml:ro" \
  -v "$PWD/mnt:/mnt" \
  vision-orchestrator:6.0 \
  python -m vision_orchestrator.app --config /workspace/vision_orchestrator/config.toml serve

docker run -d \
  --name vision-orchestrator-worker-1 \
  --network vision-orchestrator-net \
  -e CONFIG_PATH=/workspace/vision_orchestrator/config.toml \
  -e VISION_ORCHESTRATOR_WORKER_ID=worker-1 \
  -v "$PWD/vision_orchestrator/config.toml:/workspace/vision_orchestrator/config.toml:ro" \
  -v "$PWD/mnt:/mnt" \
  vision-orchestrator:6.0 \
  python -m vision_orchestrator.app --config /workspace/vision_orchestrator/config.toml worker
```

2 个 API 实例高可用时，参考 `nginx.conf.example`。Nginx 只代理 API 控制面，不代理 Worker，不提升 Kafka 消费并发。

控制接口示例：

```bash
curl -X POST http://127.0.0.1:9000/api/worker-control/resume \
  -H 'X-VISION-ORCHESTRATOR-KEY: change-me' \
  -H 'Content-Type: application/json' \
  -d '{"updated_by":"operator","reason":"manual resume"}'
```
