FROM python:3.11-slim-bookworm

# 基础环境与 Chromium 安装
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    MUSE2API_HOME=/app \
    MUSE2API_HOST=0.0.0.0 \
    MUSE2API_PORT=18610 \
    MUSE2API_CHROMIUM=/usr/bin/chromium \
    MUSE2API_CDP_PORT=19210

WORKDIR /app

# 安装 Chromium、中文字体与核心依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    fonts-wqy-zenhei \
    fonts-freefont-ttf \
    fonts-liberation \
    fonts-noto-core \
    fonts-dejavu-core \
    procps \
    curl \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制代码与必要目录
COPY admin.html app.py cdp.py config.py engine.py store.py proxyrelay.py version.json ./
COPY deploy/ ./deploy/
COPY extension/ ./extension/
COPY tools/ ./tools/

# 构建时写入当前提交（docker build --build-arg GIT_SHA=$(git rev-parse HEAD)）：
# 版本检测据此判断镜像里的代码是哪个提交；容器重建后不会沿用数据卷里旧的在线升级记录
ARG GIT_SHA=""
RUN printf '%s' "$GIT_SHA" > /app/.build_sha

# 创建持久化数据目录
RUN mkdir -p /app/data/media /app/data/downloads /root/muse2api-profiles

VOLUME ["/app/data"]

EXPOSE 18610

# 监听地址与端口取自 MUSE2API_HOST / MUSE2API_PORT（默认 0.0.0.0:18610）
CMD ["python", "app.py"]
