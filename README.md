# MUSE2API

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10%2B-blue?logo=python" alt="Python Version" />
  <img src="https://img.shields.io/badge/FastAPI-0.115%2B-009688?logo=fastapi" alt="FastAPI" />
  <img src="https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker" alt="Docker" />
  <img src="https://img.shields.io/badge/API-OpenAI%20Compatible-green" alt="OpenAI API Compatible" />
  <img src="https://img.shields.io/badge/License-MIT-orange" alt="License" />
</p>

把 [muse.ai](https://muse.ai/) 网页端的多模态能力封装成 **OpenAI 兼容 API**。通过无头浏览器和 CDP 维持登录会话，支持对话、文生图、图生图、文生视频和图生视频，并带多账号池、会话续期和配套导号工具。

当前版本见 [`version.json`](version.json)。

---

## 功能

- **对话**：`/v1/chat/completions` 与 `/v1/responses`，支持流式 SSE 和多轮上下文。`gpt-4o`、`gpt-5`、`claude-sonnet-4`、`deepseek-chat` 等常见模型名会映射到 Muse。
- **图像**：`/v1/images/generations` 与 `/v1/images/edits`。支持多种画幅、`url` / `b64_json`，以及最多 4 张有序参考图。只返回本次新生成的媒体。
- **视频**：`/v1/videos` 创建任务，`/v1/videos/{id}` 轮询。可请求 5/6/8/10 秒和横竖屏；成片时长以上游实际输出为准。支持首帧或多张参考图，不支持用视频文件做参考。
- **账号池**：按 LRU 分发。提示词发出前失败会换一个健康账号重试一次；额度耗尽或提示词已发出后失败则不换号重发，避免重复扣费。
- **会话保活**：后台续签会话 Cookie，并尝试唤醒云端 VM。保活走的是**部署服务器的出口**，不是导出 Cookie 的那台浏览器。
- **导号**：Chrome / Edge 扩展，或 Tampermonkey 脚本。可读取 `HttpOnly` Cookie。
- **管理台**：`/admin` 查看健康状态、账号、任务和媒体，并支持在线升级。升级会保留本地 `.env` 与 `data/`。

---

## 部署

管理台地址：`http://<服务器>:18610/admin`。在页面右上角填入密钥后保存。`/admin?key=…` 也能打开，但密钥会进访问日志，不建议使用。

### Docker Compose（推荐）

```bash
git clone https://github.com/biubiubiu125/muse2api.git
cd muse2api
cp .env.example .env
docker compose up -d
```

`.env` 可选。留空 `MUSE2API_KEY` 时，首次启动会生成密钥并写到 `./data/.env`。账号、任务和媒体都在 `./data`，升级时不要删这个目录。

不想在服务器上构建镜像时，用预构建包：

```bash
docker compose -f docker-compose.ghcr.yml up -d
```

升级镜像：`docker compose -f docker-compose.ghcr.yml pull && docker compose -f docker-compose.ghcr.yml up -d`。

服务是单进程、单浏览器，不要加 `uvicorn --workers`。

### 裸机（Ubuntu / Debian）

```bash
sudo apt-get update
sudo apt-get install -y chromium fonts-wqy-zenhei python3 python3-pip python3-venv
cd /opt/muse2api
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
sudo cp deploy/muse2api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now muse2api
```

### 反向代理

生图和视频可能要几分钟。Nginx / 宝塔 / 1Panel 需要拉长超时并关闭缓冲，否则会 502 或截断 SSE。完整示例见 `deploy/nginx.example.conf`。

```nginx
location / {
    proxy_pass http://127.0.0.1:18610;
    proxy_read_timeout 600s;
    proxy_send_timeout 600s;
    proxy_buffering off;
    client_max_body_size 64M;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

HTTPS 反代时请设置 `MUSE2API_PUBLIC_BASE=https://你的域名`，否则媒体直链可能仍是 `http://`。

---

## 导入账号

先在同一个浏览器里打开 [muse.ai](https://muse.ai/) 并登录到聊天界面。

### Chrome / Edge 扩展

1. 打开 `chrome://extensions`（Edge 为 `edge://extensions`），开启开发者模式。
2. 加载本仓库的 `extension/` 目录。
3. 点扩展图标，填入服务地址（如 `http://1.2.3.4:18610`）和密钥，再点「读取并导入」。

说明见 `extension/README.md`。

### Tampermonkey

安装 [Tampermonkey](https://www.tampermonkey.net/) 后，打开脚本安装页：

[安装油猴脚本](https://raw.githubusercontent.com/biubiubiu125/muse2api/main/tools/muse2api_cookie_importer.user.js)

再到 muse.ai，点击页面右下角的悬浮按钮，填入地址和密钥后推送。

---

## 调用

除 `/healthz`、`/readyz` 和 `/v1/media/*` 外，请求都要带：

```http
Authorization: Bearer <MUSE2API_KEY>
```

设置了 `MUSE2API_ADMIN_KEY` 时，管理接口只认管理密钥，业务密钥只能调 `/v1/*`。

### 对话

```bash
curl -X POST "http://localhost:18610/v1/chat/completions" \
  -H "Authorization: Bearer m2a_your_secret_key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "muse-spark",
    "messages": [{"role": "user", "content": "用三句话介绍这套接口"}],
    "stream": false
  }'
```

### 图像

同步接口适合内网短请求。前面有 CDN 或短超时客户端时，用下面的异步任务，不要靠把 HTTP 超时拉长后重复 POST。

```bash
curl -X POST "http://localhost:18610/v1/images/generations" \
  -H "Authorization: Bearer m2a_your_secret_key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "muse-image",
    "prompt": "一只机械柴犬，电影级光影",
    "size": "16:9",
    "response_format": "url"
  }'
```

多图参考用 `images`（最多 4 张，单张不超过 10 MiB，合计不超过 24 MiB）。不要和 `image`、`reference_image` 混用。`/v1/images/edits` 的 multipart 可以重复提交同名字段 `image`。

异步生图：

1. `POST /v1/images/tasks`，请求体与同步生图相同。建议带稳定的 `Idempotency-Key`。
2. 收到 `202` 和 `id` 后，每几秒 `GET /v1/images/tasks/{id}`。
3. `completed` 时读 `url` 或 `data[0]`；`failed` 时读 `error`。查询失败只重试查询，不要重新 POST。
4. 相同幂等键和相同输入返回原任务；同键不同输入返回 409；队列满返回 429。

`/v1/images/generations` 和 `/v1/images/edits` 也可以加 `async=true`，走同一套任务。默认同步行为不变。进程重启后，未完成的图片任务会标记失败，不会悄悄再生成一次。

参考图默认拒绝服务器本地路径和私网 / 本机 URL。公网 URL 会逐跳复查重定向。只有可信的单用户部署才应把对应开关设为 `1`。不合规请求在建任务前返回 400。

### 视频

```bash
curl -X POST "http://localhost:18610/v1/videos" \
  -H "Authorization: Bearer m2a_your_secret_key" \
  -H "Content-Type: application/json" \
  -d '{"prompt": "枫叶在风中落下", "duration": 5, "size": "16:9"}'
```

再查询：

```bash
curl "http://localhost:18610/v1/videos/task_xyz789" \
  -H "Authorization: Bearer m2a_your_secret_key"
```

完成后 `result.url` 指向 `/v1/media/` 下的文件。多图参考同样使用有序的 `images` 数组。

---

## 配置

复制 `.env.example` 为 `.env`。常用项如下。

| 变量 | 默认 | 说明 |
|---|---|---|
| `MUSE2API_KEY` | 自动生成 | 业务密钥，以 `m2a_` 开头。自动生成或轮换后的值在 `data/.env`，优先于本文件 |
| `MUSE2API_ADMIN_KEY` | 空 | 设置后，管理页和 `/admin/*` 只认它 |
| `MUSE2API_HOST` | `0.0.0.0` | 监听地址。只给本机用时可设 `127.0.0.1` |
| `MUSE2API_PORT` | `18610` | 服务端口 |
| `MUSE2API_PUBLIC_BASE` | 空 | 对外 URL 前缀，如 `https://api.example.com` |
| `MUSE2API_REPO` | `biubiubiu125/muse2api` | 版本检查和在线升级使用的仓库 |
| `MUSE2API_CHROMIUM` | `chromium` | 浏览器可执行文件 |
| `MUSE2API_CDP_PORT` | `19210` | 内部 CDP 端口 |
| `MUSE2API_CONCURRENCY` | `2` | 全局同时生成的任务数 |
| `MUSE2API_PER_ACCOUNT_CONCURRENCY` | `1` | 每个账号的并发槽。大于 1 前先自行确认上游不会互踢 |
| `MUSE2API_STEALTH` | `1` | 软隐身。排障时可设 `0` |
| `MUSE2API_IMAGE_TIMEOUT` | `240` | 生图等待上限（秒） |
| `MUSE2API_VIDEO_TIMEOUT` | `600` | 生视频等待上限（秒） |
| `MUSE2API_CHAT_TIMEOUT` | `300` | 对话等待上限（秒） |
| `MUSE2API_KEEPALIVE_DISABLED_ACCOUNTS` | `0` | `1` 时，已禁用账号仍保活，但不参与调度 |
| `MUSE2API_REF_IMAGE_LOCAL_FILES` | `0` | `1` 才允许读取服务器本地路径 |
| `MUSE2API_REF_IMAGE_PRIVATE_FETCH` | `0` | `1` 才允许下载私网 / 本机 URL |

---

## 排障

点管理页里账号的「测试」，看会话和额度两条结果。备注里的完整错误不要公开，也不要公开 Cookie、API Key 或带 key 的管理页链接。

- `401`：上游拒绝认证。先确认该账号能在官网登录，再重新导入 Cookie。
- `403`：访问被拒绝。可能是账号权限、服务器出口或地区限制，**不等于 Cookie 过期**。不要反复重导。
- `429`、`5xx`、超时或返回不是预期 JSON：这次保活未确认，不会把账号改成「可用」。
- 页面加载超时本身不会当成认证失效。只有明确认证失败才标记账号异常。
- 「可用」只表示最近一次已确认的结果。「剩余有效期」是 Cookie 时间，不是上游会话一定有效的证明。

一台机器能保活，不能说明另一台机器的网络、地区或代理也正常。

裸机更新：`git pull --ff-only` 后重启服务。Docker：`git pull --ff-only && docker compose up -d --build`。先保住本地修改、`.env` 和 `data/`。也可以在管理台使用在线升级。

---

## 安全

账号、任务和媒体都在本地 `data/`。不要把 `.env`、`data/accounts.json` 或 Cookie 提交到仓库。

本项目仅供技术交流和自动化测试。请遵守上游平台条款和所在地法律。

---

## 许可证

[MIT License](LICENSE)。
