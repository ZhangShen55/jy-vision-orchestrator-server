FROM python:3.11-slim

ARG BUILD_HTTP_PROXY=
ARG BUILD_HTTPS_PROXY=
ARG APT_MIRROR=https://mirrors.tuna.tsinghua.edu.cn/debian
ENV HTTP_PROXY=${BUILD_HTTP_PROXY} \
    HTTPS_PROXY=${BUILD_HTTPS_PROXY} \
    http_proxy=${BUILD_HTTP_PROXY} \
    https_proxy=${BUILD_HTTPS_PROXY}

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CONFIG_PATH=/workspace/vision_orchestrator/config.toml

WORKDIR /workspace

RUN set -e; \
    if [ -n "$APT_MIRROR" ]; then \
        sed -i "s#http://deb.debian.org/debian#$APT_MIRROR#g" /etc/apt/sources.list.d/debian.sources; \
        sed -i "s#http://deb.debian.org/debian-security#$APT_MIRROR-security#g" /etc/apt/sources.list.d/debian.sources; \
    fi; \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        libgl1 \
        libglib2.0-0 && \
    rm -rf /var/lib/apt/lists/*

COPY ./vision_orchestrator/requirements.txt ./vision_orchestrator/requirements.txt
RUN pip install --no-cache-dir \
    --index-url https://pypi.tuna.tsinghua.edu.cn/simple \
    --trusted-host pypi.tuna.tsinghua.edu.cn \
    -r vision_orchestrator/requirements.txt

COPY ./vision_orchestrator ./vision_orchestrator
COPY ./scripts/build_cython_modules.py ./scripts/build_cython_modules.py

ARG PROTECT_SOURCE=0
RUN set -e; \
    if [ "$PROTECT_SOURCE" = "1" ]; then \
        python scripts/build_cython_modules.py \
          --root /workspace \
          --package vision_orchestrator \
          --keep-source vision_orchestrator/__init__.py \
          --keep-source vision_orchestrator/app.py \
          --keep-source vision_orchestrator/api/app.py \
          --keep-source vision_orchestrator/config_loader.py \
          --exclude-glob 'vision_orchestrator/http_app.py' \
          --remove-sources; \
        rm -rf build; \
        find /workspace -type d -name '__pycache__' -prune -exec rm -rf {} +; \
    fi

CMD ["python", "-m", "vision_orchestrator.app", "serve"]
