# Muse2API 二开总体方案 · 代号 StealthFleet（隐身舰队）

> 副标题：**多账号软隐身矩阵 + 真并发直连 + 自动发布**
> 命名含义：Fleet = 多账号池，Stealth = 软隐身反指纹。名字只是标签，可改。
> 行级清单与出处见 `二开需求清单.md`；本文件为纲、可执行。
> 目标优先级：**防封号 > 调用稳定性 > 规模化**。

---

## 〇、决策基线（已锁定）

| # | 决策项 | 结论 |
|---|---|---|
| 1 | 防封号档位 | **软隐身**：单 Chromium 进程 + 多 BrowserContext，靠按账号注入指纹噪声/覆盖做差异化（不每账号独立进程/独立机器） |
| 2 | 代理 | 每账号可配 **SOCKS5 或 HTTP** 代理（含鉴权） |
| 3 | `/api/session` 续签 | 改为浏览器上下文内 **CDP fetch**（同源/同 TLS/同代理），弃用 curl_cffi |
| 4 | 多租户对外 | **不做**（取消原 P4-2、入站限速） |
| 5 | GHCR 构建 | **amd64 + arm64** 多架构 |
| 6 | `MUSE2API_REPO` 配置化 | **采纳**（P5 已完成） |

目标：让每个账号像"一个真实、归属地一致的美/加用户"，账号之间互不关联。
非目标：前端 i18n、媒体库管理等运维功能保持现状；不做多租户。

---

## 一、进度总览

| 阶段 | 主题 | 状态 | 量级 |
|---|---|---|---|
| **P5** | 发布与仓库：`MUSE2API_REPO` 配置化 + GHCR 多架构自动构建 | ✅ 已完成 | S |
| **P0** | 并发基线修复（锁顺序、超时空回复、下载串文件） | ✅ 代码完成（单测通过） | M |
| **P1** | 防封号核心（软隐身噪声 + 每账号代理 + 自动化标记掩蔽） | ✅ 代码完成（待真机验证） | L |
| **P2** | 会话/指纹一致（`/api/session` CDP fetch + UA 跟真实版本） | ✅ 代码完成（待真机验证） | M |
| **P3** | 同账号多并发（机制就位，默认 1，`MUSE2API_PER_ACCOUNT_CONCURRENCY` 控制） | ✅ 代码完成（放开前须实测） | M |
| **P4** | 调用增强（usage 估算、生图 n>1） | ✅ 代码完成（单测/导入通过） | S |
| **P6** | 账号供给：自动注册 + 养号（可选） | ⬜ 未做（可选） | L |

---

## 二、详细修改方案

> 模板：目标 / 改动点(文件:行) / 做法 / 风险 / 验证。

### P5 发布与仓库 ✅（已完成，记录在案）

- `config.py` 新增 `repo`（`MUSE2API_REPO`，默认 `biubiubiu125/muse2api`）。
- `app.py` `REPO/REPO_OWNER/REPO_URL` 由 `CFG.repo` 派生（版本检测、commits、codeload、push remote、git 身份）。
- `version.json`、`admin.html`、`README.md`（clone + 油猴 raw）、油猴脚本头部、`.env.example` 已改为 `biubiubiu125`。README 贡献者致谢已删除，不再保留原仓库 PR 链接。
- 新增 `.github/workflows/docker-publish.yml`：任意分支或 tag 的 `push` / 手动触发 → QEMU + buildx 构建 **amd64+arm64**，推 `ghcr.io/biubiubiu125/muse2api`（`latest` 只在默认分支打）。
- 新增 `docker-compose.ghcr.yml`（拉预构建镜像）。
- 自测：`py_compile` 全绿、`CFG.repo` 取值正确。
- **GitHub 侧手动项**：建仓并推送；首次构建后将 GHCR 包设 public；Actions 开启 `packages: write`；`/admin/repo/push` 用 biubiubiu125 的 token。

### P0 并发基线修复（清单 §3，后续并发改造的地基）

- **P0-1 锁顺序统一**：对话 `app.py:351`（先账号）与 `:361`（后 `GEN_LOCK`）；生图/视频 `:911`（先锁）`:943`（后账号）→ 统一为"先 `GEN_LOCK` 再 `ACCOUNT_POOL`"。验证：1 账号 + 并发 2 混合发对话/生图不互卡到超时。
- **P0-2 对话超时空回复**：`safe_chat_stream` 的 `ACCOUNT_POOL.acquire`（`app.py:351`）纳入 try，超时走 `q.put(("error",...))`。验证：占满时发对话收到明确错误而非空 200。
- **P0-3 下载串文件**：`_download_fallback`（`engine.py:1253-1276`）按上下文独立下载子目录（`Browser.setDownloadBehavior` 的 `downloadPath` 分目录）。验证：并发 2 各自下载不串。

### P1 防封号核心（软隐身，最高优先级）

**P1-A 每账号代理（SOCKS5/HTTP）+ 地域对齐**
- 改动点：`engine._open_page`（`engine.py:278` `createBrowserContext` 现无参）；账号模型（`store.py`/导入接口/admin UI）。
- 做法：
  1. 账号增加字段：`proxy`（`socks5://user:pass@host:port` 或 `http://...`）、`timezone`、`lang`（可由代理地区推导）。
  2. `createBrowserContext` 传 `proxyServer`（支持 `socks5://`、`http://` scheme）、`proxyBypassList`。
  3. HTTP 代理鉴权用 `Fetch.enable` + `Fetch.authRequired` 回填账密；**SOCKS5 带账密在 Chromium 侧支持有限 → 需实测**，必要时本地起一个无鉴权转发（socks 鉴权→本地监听）。
  4. 页面侧 `Emulation.setTimezoneOverride` + `Accept-Language`/`navigator.language` 与代理地区一致。
- 风险：Muse 仅美/加 → 必须美/加住宅 IP；时区/语言/IP 三者同源，否则比现状更可疑。
- 验证：绑美国代理后，页面内时区、`navigator.language`、出口 IP 归属三者一致。

**P1-B 软隐身指纹噪声（单进程内账号差异化）**
- 改动点：`engine.start()` 单进程保持；`--window-size=1440,2400`（`engine.py:187`）；`Dockerfile` 字体；每 context 注入脚本。
- 做法：
  1. `Page.addScriptToEvaluateOnNewDocument` 对每个 context 注入噪声：canvas / WebGL（`UNMASKED_VENDOR/RENDERER` 轻改 + 像素噪声）、audio、`navigator.hardwareConcurrency`/`deviceMemory`/`platform`、屏幕分辨率与 `devicePixelRatio`、字体可用性。
  2. **以账号 id 为种子**：同账号每次一致（自身前后一致），账号之间不同（避免"多账号同机"）。
  3. 分辨率改为常见真实值（如 1920×1080 一类），按账号稳定取值。
  4. Docker 补充常见桌面字体。
- 风险：噪声过度/前后不一致本身是破绽 → 必须稳定可复现；单进程 WebGL 底层仍相近，软隐身只降低关联度、非绝对隔离（这是选型取舍）。
- 验证：两账号跑指纹检测页，canvas/WebGL hash、分辨率、字体列表不同；同账号两次结果一致。

**P1-C 自动化标记掩蔽**
- 改动点：`engine.py:182` 启动参数 + 注入脚本。
- 做法：加 `--disable-blink-features=AutomationControlled`；注入令 `navigator.webdriver===false`；去 `HeadlessChrome`（并入 P2-1 的 UA 覆盖）。软隐身档暂不强制有头+Xvfb，留作后续可选。
- 验证：页面内 `navigator.webdriver===false`，UA 不含 `HeadlessChrome`。

### P2 会话/指纹一致

**P2-1 浏览器 UA + Client Hints**
- 做法：`Network.setUserAgentOverride`（或 `Emulation.*`）同时设 `userAgent` + `userAgentMetadata`，去 HeadlessChrome，版本**跟随真实 chromium（当前 156）**，不写死旧号。

**P2-2 `/api/session` 改 CDP fetch（替换 requests）**
- 改动点：`engine.renew_session_http`（`engine.py:314`，写死 `Chrome/135` 的 `requests` 实现）及其调用方（预续签、`_keepalive_loop`/`_probe_account_sync`）。
- 做法：在账号所属 context 的页面里执行 `fetch('https://muse.ai/api/session', {credentials:'include'})` 与 `/api/hatch/vm/wake`；JS 读响应 JSON（`status/vm_id/vm_state`），Set-Cookie 由浏览器写入 context cookie jar，再用 `Network.getCookies` 读回（与现有 `read_cookies()` 一致）。→ 续签与浏览器同源/同 TLS/同代理。
- **关键取舍**：现状续签不需要开浏览器页面（保活很轻）；改 CDP fetch 后每次续签需要一个该账号的 context。方案：续签在"已有热页面则复用，否则临时开一个短生命周期 context"；评估保活频率与开销，必要时保活仍走轻量路径、仅正常业务链路强制同源。实现时定稿。
- 验证：`/api/session` 的 TLS/JA3 与浏览器一致（抓包或指纹站确认不再是 Python requests）。

### P3 同账号多并发（清单 §1）

- 改动点：`AccountPoolManager`（`app.py:127`）`busy_accounts` 集合 → 按账号计数（每账号 N 槽）；`store.pick_account`（`store.py:147`）排除逻辑。
- **硬前提实测**：CDP fetch 同 context 续签后旧 cookie 是否仍有效（同源续签理论上冲突更小，仍需验证）；失效则按账号串行化续签。
- 风险：`_sync_cookies` 非原子互相覆盖；叠加 P1 代理/噪声后同账号并发风险下降。
- 验证：同账号两记录并发生图，对比 `elapsed`，观察是否 401/被登出。

### P4 调用增强（清单 §6/§8.2，已砍多租户）

- **P4-1 usage 估算**：`app.py` chat/responses 的 `usage` 恒为 0 → 按字符粗估 prompt/completion tokens。
- **P4-3 生图 n>1**：`ImageRequest.n`（`app.py:415`）当前忽略 → 循环生成返回多条。
- （P4-2 多租户、P4-4 入站限速：按决策取消；限速仅在未来放开同账号并发时再评估。）

### P6 账号供给（可选，清单 §8.3）

- 基于 `tools/mailtm_otp.py` 做自动注册 + 养号，必须与 P1 代理/噪声绑定。

---

## 三、执行顺序

1. **P5** ✅ 已完成。
2. **P0** 并发基线修复（地基）。
3. **P1** 软隐身 + 每账号代理（收益最大）。
4. **P2** CDP fetch + UA 跟 156（承接 P1 的代理/UA）。
5. **P3 / P4** 规模化与接口完善，按需。
6. **P6** 规模化养号，最后做。

---

## 四、部署 / 运维注意

- 服务依赖进程内全局锁 + 单浏览器实例：**uvicorn 必须单 worker**（勿加 `--workers>1`）。
- GHCR arm64 经 QEMU 模拟构建较慢；首次构建后将包设为 public 才能免登录 `docker pull`。
- 预构建镜像部署用 `docker-compose.ghcr.yml`；升级 `docker compose -f docker-compose.ghcr.yml pull && up -d`（与 app 内源码级自更新是两条路）。

## 五、仍需实测（动手前/中确认）

1. Chromium/CDP 对 **SOCKS5 带账密**代理的支持程度（P1-A）。
2. CDP fetch 同 context 续签后旧 cookie 是否失效（P3 前提）。
3. Muse 单账号可同时跑几个顶层会话、生图/视频是否按账号限流（P3）。

---

## 六、实现记录（P0–P4，2026-10-30）

**已改动文件**：`config.py`、`engine.py`、`store.py`、`app.py`、`requirements.txt`、`Dockerfile`。

- **P0-1 锁顺序**：`safe_chat_stream` 重写为"先 `GEN_LOCK` 再账号"，与 `_run_generation` 一致，消除相向死锁。
- **P0-2 超时空回复**：账号获取纳入 `try`，等号超时以错误入队、正常上抛，不再空 200。
- **P0-3 下载串文件**：每 BrowserContext 独立下载子目录（`current_download_dir`），关页时清理；`_download_fallback` 只在本任务目录找新文件。
- **P1-A 代理+地域**：账号新增 `proxy`/`timezone`/`lang`；`Target.createBrowserContext` 带 `proxyServer`（socks5/http，`host:port`）；`renew_session_http` 经账号代理（socks5 需 PySocks，已加依赖）；`Emulation.setTimezoneOverride` + `Accept-Language`。带账密代理：浏览器层仅支持 IP 鉴权（日志告警，待后续本地转发）。
- **P1-B 软隐身噪声**：`Page.addScriptToEvaluateOnNewDocument` 按账号 id 种子稳定注入 webdriver=false、hardwareConcurrency/deviceMemory、screen/dpr、WebGL vendor/renderer；分辨率改 1920×1080；Docker 补 liberation/noto/dejavu 字体。**未改画布/音频像素**（避免破坏页面渲染）。
- **P1-C 自动化掩蔽**：启动加 `--disable-blink-features=AutomationControlled`、`--lang=en-US`；UA 覆盖去 HeadlessChrome。
- **P2-1 UA/CH**：`Browser.getVersion` 探测真实版本 → `Network.setUserAgentOverride`（含 `userAgentMetadata`）跟随真实 Chrome。
- **P2-2 /api/session**：页面就绪后 `_renew_in_page` 用浏览器 `fetch` 同源命中（同 TLS/同代理）并尽力唤醒 VM；`renew_session_http`（requests）保留为保活轻量路径，且已改为经账号代理，UA 升到 156。
- **P3 同账号并发**：`AccountPoolManager.busy_accounts` 改为 `{id: 计数}`，上限 `CFG.per_account_concurrency`（默认 1=现状）。
- **P4-1 usage**：chat（流式/非流式）与 responses 按 ~4 字符/token 估算回报。
- **P4-3 n>1**：`/v1/images/generations` 非异步按 `n` 串行生成多张（上限 4），部分成功也返回。
- **软隐身总开关**：`MUSE2API_STEALTH=0` 可整体关闭（调试用）。

**已通过的离线验证**：`py_compile` 全绿；`import app` 成功；`tests/test_concurrency.py` 全过；`tests/test_session_health.py` 177 项；`tests/test_ref_image_safety.py` 6 项。

## 七、真机验证清单（本机无法验证，上线前务必跑）

1. 装真实 Chromium + 依赖，起服务，导入 1 个账号（先不配代理），确认对话/生图/生视频与改造前一致（软隐身默认开）。
2. 页面内核对：`navigator.webdriver===false`、UA 不含 `HeadlessChrome`、`navigator.hardwareConcurrency`/`screen` 等已被覆盖；两个账号指纹不同、同账号两次一致。
3. 配一个 **IP 鉴权**的美/加代理到某账号，确认出口 IP = 代理、时区/语言一致；`/api/session` 走代理。
4. **带账密代理**：确认当前浏览器层是否可用（预期需 IP 白名单；否则走后续本地转发方案）。
5. CDP fetch 续签：确认 `_renew_in_page` 命中 `/api/session` 且 VM 正常唤醒、生成不受影响。
6. 放开同账号并发前：设 `MUSE2API_PER_ACCOUNT_CONCURRENCY=2`，同账号并发两任务，观察是否 401/被登出（验证续签是否踢旧会话）。确认无误再在生产放开。
7. GHCR：推仓库触发 Actions，确认 amd64+arm64 均构建成功、包设 public、`docker pull` 可用。

---

## 八、代码审查问题修复记录（2026-10-30）

逐行真实链路审查发现的问题已全部修复（新增 `proxyrelay.py`；改动 `engine.py`/`app.py`/`store.py`/`.env.example`/`tests/test_session_health.py`）：

- **C1（高）/api/session 真闭环**：软隐身主链路不再用 Python `requests`。`_renew_and_persist` 在 `CFG.stealth` 下短路；`ensure_page` 跳过 requests 预续签，改由就绪后浏览器内同源 `_renew_in_page` 完成续签+唤醒。过期会话的快速失败搬到浏览器：`_renew_in_page` 命中 HTTP 401 直接抛 `MuseAuthError`，`ensure_page` 向上抛（同源、同 TLS、无 Python 请求，且不丢快速切号）。
- **C2（高）带账密代理**：新增 `proxyrelay.py` 本地无鉴权 CONNECT 转发器，向上游注入 http/socks5 账密；`_proxy_server_for` 对带账密代理让 Chromium 指向本地转发端口，无账密直连。浏览器层代理鉴权打通。
- **C3（中）语言一致**：指纹脚本新增 `navigator.language` 按账号覆盖，与 `navigator.languages`、Accept-Language 一致。
- **C4（中）脚本健壮**：`navigator.webdriver` 覆盖改为独立 try，单项失败不再中断其余指纹注入。
- **C5（低）UA 兜底**：`_detect_browser_ua` 探测失败时用 `chromium --version` CLI 兜底，再不行用常量；任何情况下 UA 都不含 HeadlessChrome。
- **C6（低）续签 UA**：`renew_session_http` 改为实例方法，UA 用探测到的真实版本（不再写死 156）。
- **C7（低）proxyBypassList**：移除语义相反的 `<-loopback>`。
- **C8（低）socks DNS**：requests 的 `socks5://` 归一为 `socks5h://`（远端 DNS）；转发器 socks 亦用 rdns。
- **S1（中）凭据脱敏**：`store.list_accounts` 对 `proxy` 账密脱敏（`***@host:port`）；功能路径经 `get_account` 读原值不受影响。
- **P3-2（低）并发指引**：`.env.example` 说明 `MUSE2API_CONCURRENCY` 不应超过「账号数 × 每账号并发」，并补充 `MUSE2API_PER_ACCOUNT_CONCURRENCY`、`MUSE2API_STEALTH`。
- **R1（低）唤醒非阻塞**：`_renew_in_page` 的 VM 唤醒改 fire-and-forget（不 await），不拖慢首字。
- **R2（低）n>1 一致**：`/v1/images/edits` 同步路径按 `n` 串行多图（上限 4），与 generations 一致。

**离线验证（Windows）**：`py_compile` 六文件全过；`import app, proxyrelay` 成功；`test_concurrency`、`test_session_health`（177，含新增"软隐身主链路不调用 requests"断言）、`test_ref_image_safety` 全绿。真浏览器端到端仍需按第七节清单在真机验证（本环境 WSL 为裸 Debian、网络限速，无法装 chromium 实测）。

---

## 九、第二轮代码审查问题修复记录（2026-10-30）

针对第二轮全真实链路复查发现的问题，已全部修复并验证：

- **H1（高）Docker 缺 proxyrelay.py**：`Dockerfile` 的 COPY、`app.py` 的 `TRACKED_REPO_PATHS` 均补入 `proxyrelay.py`，否则 C2 在容器里形同未做。
- **M1（中）软隐身主链路 VM 唤醒被架空**：`ensure_page` 改为导航后、`_wait_ws_ready` 之前先 `_wait_origin` 进入 muse.ai 同源，再 `_renew_in_page(wake_vm=True)` —— 此时 VM 多为非 RUNNING，唤醒真正生效；冷 VM 得以在 `login_wait` 内连上。401 在此快速失败切号。
- **M2（中）上下文创建失败回退共享默认上下文**：`_open_page` 改为 fail-closed——创建隔离上下文/页面失败即 `_close_page` 清理并抛错，绝不退回共享默认上下文（杜绝多账号 Cookie 串扰与代理泄漏）。
- **M3（中）管理页无代理入口**：`admin.html` 单条导入表单新增代理 / 时区 / 语言输入，`addAccount` 单条分支随 body 提交。
- **M4（中）地域三元组默认不一致**：新增 `MUSE2API_DEFAULT_TIMEZONE`（默认 `America/New_York`）；账号配了代理但未设时区时自动套用，避免美国 IP 配 UTC 时区。
- **L1**：`TRACKED_REPO_PATHS` 补 `proxyrelay.py`（见 H1）。
- **L2**：`_relays` 上限 32，超出淘汰最老，防止代理频繁变更时本地监听累积。
- **L3**：`patch_account` 忽略回传的脱敏代理值（含 `***`），避免把真实代理覆盖成脱敏串。
- **L5**：`proxyrelay` 在 CONNECT 握手后转发客户端可能粘包的后续字节。
- **L6**：`_effective_ua` 不在热路径起子进程（真实版本由 `start()` 时 `_detect_browser_ua` 一次性探测，含 `chromium --version` CLI 兜底），绝不漏 HeadlessChrome。
- **L4 / L7（说明）**：L4（WebGL 无扩展时 getParameter 返回值差异）属**有意且净收益**的伪装（真 Linux headless 的 WebGL 会暴露 SwiftShader/Mesa），保留不改；L7（socks5 转发路径）已用最小 SOCKS5 服务端真实验证。

**本轮真实验证**：
- `proxyrelay` **HTTP CONNECT + Basic 账密注入** 与 **SOCKS5 + user/pass 鉴权** 两条路径，均用真实 socket 跑通（账密校验、`200 Connection Established`、隧道回显正确）。
- `py_compile` 六文件、并发、`test_session_health`（177，修复了 `_effective_ua` 反复起子进程导致的挂起）、参考图回归全绿。
- 真浏览器 + 真代理 + muse.ai 的端到端仍需真机验证（WSL 为裸 Debian + 限速，装不了 chromium）。

---

## 十、第三轮代码审查问题修复记录（2026-10-30）

- **N1（中）下载目录隔离并发失效**：`_open_page` 的 `Browser.setDownloadBehavior` 补 `browserContextId=ctx_id`，使下载目录真正作用于该 incognito 上下文；修复 P0-3 在并发下的断链。
- **N2（中）`start()` 锁形同虚设**：整个启动体（spawn/连接）纳入 `_start_lock`，杜绝并发首次启动拉起两个 Chromium（同端口/同 profile 冲突）。
- **N3（低）默认时区一刀切**：新增 `_default_tz`——显式时区优先；有代理时按语言粗分（`*-ca`/`fr*` → `America/Toronto`，否则 `MUSE2API_DEFAULT_TIMEZONE`）。
- **N4（低）转发器误淘汰**：去掉"超量淘汰最老转发器"的逻辑（会误停在用隧道）；改为按上游去重复用、仅 `stop()` 时统一清理。
- **N5（低）坏代理等待**：`_wait_origin` 上限由 15s 降到 10s，坏/冷代理更快失败切号。
- **N6（低）管理页代理入口不全**：`admin.html` 账号列表行新增代理指示（🌐，悬停看脱敏值）与「代理」按钮（`setProxy` 支持设置/修改/留空清除）；配合单条导入表单，已可在 UI 全量管理每账号代理。

**离线验证**：`py_compile` 六文件 + `import app` 通过；并发、`test_session_health`（177）、参考图回归全绿。`start()` 整体加锁后的重新缩进经编译确认无误。真浏览器 + 真代理 + muse.ai 端到端仍需真机验证（WSL 裸环境无法安装 chromium）。

---

## 十一、第四轮代码审查问题修复记录（2026-10-30）

- **Q1（中）额度查询端点绕过账号代理/地域**：`/admin/accounts/{aid}/quota`（`query_quota`）与 `/admin/quota`（委托前者）在 `engine.start()` 前补 `engine.set_account_meta(aid, proxy, timezone, lang)`，使额度读取与生成走同一账号代理 IP/时区，消除同账号双 IP 关联特征。至此**所有开浏览器页的入口**（对话/生图/生视频/测试/重登/保活/预热/额度）都已在开页前注入账号元数据。
- **（低）删号清理**：`del_account` 调用新增的 `engine.drop_account_meta(aid)`，删除账号时清理其缓存的代理/时区/语言元数据。

**离线验证**：`py_compile` 六文件 + `import app` 通过；并发、`test_session_health`（177）、参考图回归全绿。
