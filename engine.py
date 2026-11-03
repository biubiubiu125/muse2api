"""Muse 生成引擎：用真实网页会话驱动 muse.ai 完成生图/生视频。

已验证流程：
  注入 cookie -> 打开 https://muse.ai/ -> 定位 textarea(placeholder=消息)
  -> Input.insertText 填入 -> 点「发送」
  -> 等待新的附件容器 [data-testid^=hatch-chat-attachment-presentation-]
  -> 从该容器内的 img/video 取 blob 字节 -> 落盘
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import http.client
import ipaddress
import mimetypes
import urllib.error
import urllib.parse
import urllib.request
import json
import logging
import os
import re
import shutil
import socket
import subprocess
import threading
import time
import uuid
import unicodedata

from cdp import CDP, http_json
try:
    from cdp import CDPConnectionError
except ImportError:  # 测试里用桩替换 cdp 模块时
    class CDPConnectionError(ConnectionError):
        pass

log = logging.getLogger("muse2api")

ATT_SEL = '[data-testid^="hatch-chat-attachment-presentation-"]'
MAX_REFERENCE_IMAGES = 4
MAX_REFERENCE_IMAGE_BYTES = 10 * 1024 * 1024
MAX_REFERENCE_TOTAL_BYTES = 24 * 1024 * 1024

# 决定账号生死的核心 cookie（缺失或过期 = 会话失效）
# 注：hatch_gw 为网关路由缓存，会话生死主要依赖 hatch_sess 与 hatch_vml，缺 gw 亦可正常续签与生成
ESSENTIAL_COOKIES = ("hatch_sess", "hatch_vml")

# UA 探测失败时的兜底（确保任何情况下都不泄漏 HeadlessChrome 标记）
_FALLBACK_CHROME_VERSION = "140.0.0.0"
_FALLBACK_UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/" + _FALLBACK_CHROME_VERSION + " Safari/537.36")


class MuseAuthError(RuntimeError):
    pass


class MuseGenerationError(RuntimeError):
    pass


class MuseModelRefusalError(MuseGenerationError):
    """Muse completed the turn with text instead of the requested media."""


class MuseQuotaError(MuseGenerationError):
    """站点提示账号额度耗尽。提示词可能已被受理并计费，不换账号重发。"""


class MuseSubmittedError(MuseGenerationError):
    """成品已经生成（已计费），只是取回或交付失败。换账号重发会重复扣额度，不重试。"""


class MusePageGoneError(MuseGenerationError):
    """浏览器页面的 CDP 连接已断开（标签页崩溃、浏览器被杀）：轮询不会再有结果，立即失败。"""


class RefImageSafetyError(MuseGenerationError):
    """A reference image source is forbidden by the configured input policy."""


class _RefImageHTTPHandler(urllib.request.HTTPHandler, urllib.request.HTTPSHandler):
    def __init__(self, address: str):
        super().__init__()
        self.address = address

    def _connection(self, host, connection_type, **kwargs):
        connection = connection_type(host, **kwargs)

        def create_connection(address, timeout, source_address):
            return socket.create_connection((self.address, address[1]), timeout, source_address)

        # Keep the original host for Host, TLS SNI and certificate validation;
        # only the socket destination is pinned to the checked address.
        connection._create_connection = create_connection
        return connection

    def http_open(self, req):
        return self.do_open(lambda host, **kw: self._connection(
            host, http.client.HTTPConnection, **kw), req)

    def https_open(self, req):
        return self.do_open(lambda host, **kw: self._connection(
            host, http.client.HTTPSConnection, **kw), req)


class _NoRefImageRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _open_public_ref_image(url: str):
    """Open one public HTTP(S) URL with DNS rebinding and redirect checks."""
    for _ in range(11):
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise RefImageSafetyError("参考图重定向仅允许有效的 http(s) URL")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        addresses = socket.getaddrinfo(parsed.hostname, port, type=socket.SOCK_STREAM)
        checked = []
        for address in addresses:
            ip = ipaddress.ip_address(address[4][0])
            if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
                ip = ip.ipv4_mapped
            if not ip.is_global:
                raise RefImageSafetyError(
                    "参考图地址被拒绝：仅允许公网地址；如确需私网抓取，请显式设置 "
                    "MUSE2API_REF_IMAGE_PRIVATE_FETCH=1")
            checked.append(str(ip))
        if not checked:
            raise urllib.error.URLError("参考图域名未解析到地址")
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), _NoRefImageRedirect(),
            _RefImageHTTPHandler(checked[0]))
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            return opener.open(req, timeout=20)
        except urllib.error.HTTPError as exc:
            location = exc.headers.get("Location")
            if exc.code not in (301, 302, 303, 307, 308) or not location:
                raise
            exc.close()
            url = urllib.parse.urljoin(url, location)
    raise urllib.error.URLError("参考图重定向次数过多")


# 站点「额度耗尽」提示：只认明确的耗尽说法。「接近 token limit」「达到上限」这类宽泛字样
# 在提示条里很常见（例如快用完的提醒），会误判并把账号暂停 6 小时
_QUOTA_RE = re.compile(
    r"额度(?:已)?(?:不足|用完|用尽|耗尽)|积分(?:已)?(?:不足|用完|用尽)|out of credits|"
    r"(?:reached|hit|exceeded) (?:your |the )?(?:usage|weekly|daily|token) limit|"
    r"usage limit reached|no (?:credits|tokens) (?:left|remaining)", re.I)

# 页面没就绪时判断会话是否已被登出：登录入口文案（整词匹配）出现、且页面上没有登出入口。
# 「退出登录」「Sign out」这类菜单只在已登录时才有，看到它们就不算登出（只是加载慢）。
_LOGGED_OUT_RE = re.compile(
    r"\b(?:log in|sign in|create an account|use another account)\b|(?<!退出)登录", re.I)
_LOGGED_IN_RE = re.compile(r"\b(?:log ?out|sign ?out)\b|退出登录|登出", re.I)


def _looks_logged_out(body: str) -> bool:
    return bool(_LOGGED_OUT_RE.search(body or "")) and not _LOGGED_IN_RE.search(body or "")

# 气泡内联图（不在附件容器里）短边小于这个像素数时不当作生成结果
MIN_RESULT_IMAGE_PX = 64

# 成品出现后用于取回字节 / 下载兜底的最短时限（请求剩余时间更多时用剩余时间）
FETCH_GRACE_SECONDS = 60

# 单张远程参考图的总下载时限：socket 超时只管单次读，慢速图源每秒吐 1 字节也能一直不超时
REF_DOWNLOAD_SECONDS = 30


def _response_socket(resp):
    """取 urllib / http.client 响应底层的 socket（取不到返回 None）。"""
    fp = getattr(resp, "fp", None)
    for obj in (getattr(getattr(fp, "raw", None), "_sock", None), getattr(fp, "_sock", None),
                getattr(resp, "_sock", None)):
        if obj is not None and hasattr(obj, "settimeout"):
            return obj
    return None


def _read_with_deadline(resp, limit: int, seconds: float) -> bytes:
    """分块读取，总耗时超过 seconds 就放弃；最多读 limit 字节。

    每次读之前把 socket 超时收紧到剩余时间，并用 read1（有数据就返回，不等读满一整块）：
    否则服务端每隔几秒吐一个字节，单次 read(64KiB) 就能无限期地阻塞下去。"""
    deadline = time.monotonic() + seconds
    sock = _response_socket(resp)
    reader = getattr(resp, "read1", None) or resp.read
    chunks, total = [], 0
    while total < limit:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise MuseGenerationError(f"下载远程参考图超过 {int(seconds)}s 总时限")
        if sock is not None:
            try:
                sock.settimeout(max(0.05, remaining))
            except OSError:
                pass
        try:
            chunk = reader(min(64 * 1024, limit - total))
        except (socket.timeout, TimeoutError):
            raise MuseGenerationError(f"下载远程参考图超过 {int(seconds)}s 总时限") from None
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
    return b"".join(chunks)


def _sniff_image_mime(data: bytes) -> str | None:
    """按文件头识别常见图片格式；认不出返回 None。"""
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    return None


def _stream_delta(emitted: str, prev: str, cur: str) -> str:
    """流式增量。emitted：已发给客户端的全部文本；prev：上一次读到的页面文本；cur：这次读到的。

    - 页面文本是已发内容的前缀扩展：发新增部分（常规逐字输出；文本短暂缩短后又长回来也不重复）；
    - 否则是页面重新渲染（Markdown 代码块插入语言标签 / Copy 按钮等）：已发的收不回来，
      渲染本身不算新内容，什么都不发；之后在渲染结果后面追加的文字仍按相对 prev 的前缀扩展发出；
      渲染与追加落在同一次轮询里时，按 prev 末尾的锚点在 cur 中对齐，只发锚点之后的部分。
    不按长度猜：重新渲染后的文本长度与原文无关，按长度补发会发出重复或错位的片段。"""
    if cur.startswith(emitted):
        return cur[len(emitted):]
    if prev and cur.startswith(prev):
        return cur[len(prev):]
    # 重新渲染与追加落在同一次轮询里：找 prev 末尾一段在 cur 里最后出现的位置，其后的就是新追加的。
    # 锚点至少 8 个字符，避免拿很短的公共片段误对齐。
    for n in range(min(len(prev), 64), 7, -1):
        anchor = prev[-n:]
        pos = cur.rfind(anchor)
        if pos >= 0:
            return cur[pos + n:]
    return ""


class MuseEngine:
    def __init__(self, cfg):
        self.cfg = cfg
        self.proc: subprocess.Popen | None = None
        self.browser: CDP | None = None
        self._local = threading.local()
        self._start_lock = threading.Lock()
        self._active_contexts: set[str] = set()
        self._contexts_lock = threading.Lock()
        self._last_http_renew: dict[str, float] = {}
        self._log = None
        # 账号级元数据（代理 / 时区 / 语言），由 app 在用号前写入，供开上下文时取用
        self._acct_meta: dict[str, dict] = {}
        self._acct_meta_lock = threading.Lock()
        # 真实 Chrome UA（去 HeadlessChrome）与版本，启动后由 Browser.getVersion 探测
        self._browser_ua: str | None = None
        self._ua_full: str = ""
        self._ua_major: str = ""
        # 带账密代理 → 本地无鉴权转发器缓存（每个上游一个）
        self._relays: dict = {}
        self._relay_lock = threading.Lock()
        # 页面内 /api/session 续签成功后的回调 on_renewed(account_id)：由 app 设置为 cookie 回写，
        # 在页面仍打开时把站点刚下发的新 cookie 存下来
        self.on_renewed = None
        os.makedirs(cfg.profile_dir, exist_ok=True)

    def _ensure_local(self):
        local = getattr(self, "_local", None)
        if local is None:
            local = threading.local()
            self._local = local
        return local

    @property
    def page(self) -> CDP | None:
        return getattr(self._ensure_local(), "page", None)

    @page.setter
    def page(self, val: CDP | None):
        self._ensure_local().page = val

    @property
    def current_acc_id(self) -> str | None:
        return getattr(self._ensure_local(), "current_acc_id", None)

    @current_acc_id.setter
    def current_acc_id(self, val: str | None):
        self._ensure_local().current_acc_id = val

    @property
    def loaded_sess(self) -> str | None:
        """本线程当前页面注入的 hatch_sess（ensure_page 时记下）。"""
        return getattr(self._ensure_local(), "loaded_sess", None)

    @loaded_sess.setter
    def loaded_sess(self, val: str | None):
        self._ensure_local().loaded_sess = val

    @property
    def last_chat_text(self) -> str | None:
        """本线程最近一次 chat_stream 正常结束时页面上的最终回复文本（非流式接口以它为准）。"""
        return getattr(self._ensure_local(), "last_chat_text", None)

    @last_chat_text.setter
    def last_chat_text(self, val: str | None):
        self._ensure_local().last_chat_text = val

    @property
    def last_renew(self) -> dict | None:
        """本线程最近一次页面内 /api/session 续签的结果（见 _renew_in_page）。"""
        return getattr(self._ensure_local(), "last_renew", None)

    @last_renew.setter
    def last_renew(self, val: dict | None):
        self._ensure_local().last_renew = val

    @property
    def current_context_id(self) -> str | None:
        return getattr(self._ensure_local(), "current_context_id", None)

    @current_context_id.setter
    def current_context_id(self, val: str | None):
        self._ensure_local().current_context_id = val

    @property
    def current_download_dir(self) -> str | None:
        return getattr(self._ensure_local(), "download_dir", None)

    @current_download_dir.setter
    def current_download_dir(self, val: str | None):
        self._ensure_local().download_dir = val

    # ---------------- 浏览器生命周期 ----------------
    def _debug_url(self):
        return f"http://127.0.0.1:{self.cfg.cdp_port}/json/version"

    def start(self):
        # 整个启动过程都在锁内，避免并发首次启动拉起两个 Chromium（同端口/同 profile 冲突）
        with self._start_lock:
            if self.browser is not None and getattr(self.browser, "closed", False):
                # 浏览器级 CDP websocket 已断开（进程可能还活着）：丢弃旧连接与它名下的隔离上下文，
                # 下面重新连接已有的调试端口，或重新拉起 Chromium。
                self.browser = None
                with self._contexts_lock:
                    self._active_contexts.clear()
            if self.browser is not None and (self.proc is None or self.proc.poll() is None):
                # 连接正常：自己拉起的进程还活着，或复用的是调试端口上已有的浏览器（proc 为空）
                return
            if self.browser is not None:
                # 自己拉起的进程已退出：旧连接作废，先关掉再重启，不留悬空的 websocket
                try:
                    self.browser.close()
                except Exception:  # noqa: BLE001
                    pass
                self.browser = None
                with self._contexts_lock:
                    self._active_contexts.clear()
            if self.proc and self.proc.poll() is None and not self.browser:
                try:
                    v = http_json(self._debug_url(), timeout=2)
                    if v and "webSocketDebuggerUrl" in v:
                        self.browser = CDP(v["webSocketDebuggerUrl"], timeout=180)
                        self._detect_browser_ua()
                        return
                except Exception:  # noqa: BLE001
                    pass
                # 进程还在但调试端口连不上：结束它再整体重启
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=10)
                except Exception:  # noqa: BLE001
                    self.proc.kill()
                self.proc = None
            env = dict(os.environ)
            env.setdefault("HOME", self.cfg.home_dir)
            env["PATH"] = (self.cfg.extra_path + os.pathsep + env.get("PATH", "")) if self.cfg.extra_path else env.get("PATH", "")
            args = [
                self.cfg.chromium,
                "--headless=new", "--no-sandbox", "--disable-gpu",
                "--disable-dev-shm-usage", "--disable-background-networking",
                "--no-first-run", "--no-default-browser-check",
                "--autoplay-policy=no-user-gesture-required",
                # 软隐身：屏蔽自动化标记；真实分辨率；默认英文界面
                "--disable-blink-features=AutomationControlled",
                # WebRTC 的 UDP 不经账号代理：只允许走代理的连接，免得 STUN 直连暴露服务器真实 IP
                "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
                "--lang=en-US",
                "--window-size=1920,1080",
                f"--remote-debugging-port={self.cfg.cdp_port}",
                "--remote-allow-origins=*",
                f"--user-data-dir={self.cfg.profile_dir}",
                "about:blank",
            ]
            # 尝试复用已有健康 CDP
            if not self.proc:
                try:
                    v = http_json(self._debug_url(), timeout=1)
                    if v and "webSocketDebuggerUrl" in v:
                        self.browser = CDP(v["webSocketDebuggerUrl"], timeout=180)
                        self._detect_browser_ua()
                        return
                except Exception:
                    pass

            # 清理残留锁
            for lock_name in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
                lp = os.path.join(self.cfg.profile_dir, lock_name)
                if os.path.exists(lp) or os.path.islink(lp):
                    try:
                        os.unlink(lp)
                    except Exception:
                        pass

            os.makedirs(self.cfg.data_dir, exist_ok=True)
            self._close_log()  # 浏览器反复重启时不累积文件句柄
            self._log = open(os.path.join(self.cfg.data_dir, "chromium.log"), "ab", buffering=0)
            cwd_dir = self.cfg.home_dir if (self.cfg.home_dir and os.path.isdir(self.cfg.home_dir)) else None
            self.proc = subprocess.Popen(args, stdout=self._log, stderr=subprocess.STDOUT,
                                         env=env, cwd=cwd_dir)
            last = None
            for _ in range(90):
                try:
                    v = http_json(self._debug_url(), timeout=2)
                    self.browser = CDP(v["webSocketDebuggerUrl"], timeout=180)
                    self._detect_browser_ua()
                    return
                except Exception as exc:  # noqa: BLE001
                    last = exc
                    time.sleep(1)
            raise MuseGenerationError(f"Chromium 启动失败: {last}")

    def _close_log(self):
        if self._log is not None:
            try:
                self._log.close()
            except Exception:  # noqa: BLE001
                pass
            self._log = None

    def stop(self):
        with self._start_lock:
            self._close_page()
            if self.browser:
                with self._contexts_lock:
                    for ctx_id in list(self._active_contexts):
                        try:
                            self.browser.send("Target.disposeBrowserContext", {"browserContextId": ctx_id})
                        except Exception:
                            pass
                    self._active_contexts.clear()
                try:
                    self.browser.close()
                except Exception:
                    pass
                self.browser = None
            if self.proc and self.proc.poll() is None:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=10)
                except Exception:  # noqa: BLE001
                    self.proc.kill()
            self.proc = None
            self._close_log()
            with self._relay_lock:
                for r in list(self._relays.values()):
                    try:
                        r.stop()
                    except Exception:  # noqa: BLE001
                        pass
                self._relays.clear()

    # ---------------- 账号元数据 / 软隐身 ----------------
    def set_account_meta(self, account_id: str | None, proxy: str | None = None,
                         timezone: str | None = None, lang: str | None = None):
        """用号前写入账号的代理 / 时区 / 语言，开上下文时取用。"""
        if not account_id:
            return
        with self._acct_meta_lock:
            self._acct_meta[account_id] = {
                "proxy": proxy or None,
                "timezone": timezone or None,
                "lang": lang or "en-US",
            }

    def drop_account_meta(self, account_id: str | None):
        """账号删除时清理其缓存的代理/时区/语言元数据。"""
        if not account_id:
            return
        with self._acct_meta_lock:
            self._acct_meta.pop(account_id, None)

    def _acct(self, account_id: str | None) -> dict:
        with self._acct_meta_lock:
            return dict(self._acct_meta.get(account_id) or {})

    def _default_tz(self, meta: dict) -> str | None:
        """账号时区：显式设置优先；否则在有代理时按语言粗分美/加默认时区，避免与美国 IP 的 UTC 冲突。"""
        tz = meta.get("timezone")
        if tz:
            return tz
        if not meta.get("proxy"):
            return None
        lang = (meta.get("lang") or "").lower()
        if lang.endswith("-ca") or lang.startswith("fr"):
            return "America/Toronto"
        return getattr(self.cfg, "default_timezone", "") or None

    def _chromium_version_via_cli(self) -> str:
        """用 `chromium --version` 兜底解析真实版本（CDP 探测失败时）。"""
        try:
            out = subprocess.run([self.cfg.chromium, "--version"],
                                 capture_output=True, text=True, timeout=10).stdout or ""
            m = re.search(r"(\d+\.\d+\.\d+\.\d+)", out)
            return m.group(1) if m else ""
        except Exception:  # noqa: BLE001
            return ""

    def _detect_browser_ua(self):
        """启动后探测真实 Chrome 版本并构造 UA（必定去掉 HeadlessChrome；探测失败也有兜底，绝不漏 Headless）。"""
        product = ""
        ua = ""
        ver = ""
        try:
            r = self.browser.send("Browser.getVersion")
            res = r.get("result", {}) if isinstance(r, dict) else {}
            product = res.get("product", "") or ""
            ua = res.get("userAgent", "") or ""
            ver = product.split("/", 1)[1] if "/" in product else ""
        except Exception as exc:  # noqa: BLE001
            log.warning("Browser.getVersion 探测失败: %s", exc)
        if not ver:
            ver = self._chromium_version_via_cli()
        if ua:
            self._browser_ua = ua.replace("HeadlessChrome", "Chrome")
        elif ver:
            self._browser_ua = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                                "(KHTML, like Gecko) Chrome/" + ver + " Safari/537.36")
        else:
            self._browser_ua = _FALLBACK_UA
            ver = _FALLBACK_CHROME_VERSION
        self._ua_full = ver
        self._ua_major = ver.split(".", 1)[0] if ver else ""
        log.info("浏览器版本 %s；UA 伪装为 Chrome/%s", product or ver, self._ua_major or "?")

    def _effective_ua(self) -> str:
        """当前应使用的真实 Chrome UA；绝不漏 HeadlessChrome。
        真实版本在 start() 时由 _detect_browser_ua（含 chromium --version CLI 兜底）一次性探测并写入
        _browser_ua，之后各处（含 keepalive）共享；未启动时用兜底 UA，且不在热路径反复起子进程。"""
        return self._browser_ua or _FALLBACK_UA

    @staticmethod
    def validate_proxy(proxy: str | None) -> str | None:
        """导入 / 修改账号时校验代理：返回错误说明，合法或为空时返回 None。"""
        if not proxy:
            return None
        raw = proxy if "://" in proxy else "http://" + proxy
        try:
            u = urllib.parse.urlsplit(raw)
            port = u.port  # 端口非法时抛 ValueError
        except ValueError:
            return "代理端口无效"
        if (u.scheme or "").lower() not in ("http", "https", "socks5", "socks5h", "socks4", "socks4a"):
            return "代理协议只支持 http / https / socks5 / socks4"
        if not u.hostname:
            return "代理缺少主机名"
        if port is not None and not 0 < port < 65536:
            return "代理端口无效"
        return None

    @staticmethod
    def _parse_proxy(proxy: str | None):
        """解析代理为 (proxyServer, bypassList, (user, pass)|None)；proxyServer 不含账密。
        支持 socks5:// 与 http(s)://，缺 scheme 时按 http 处理。"""
        if not proxy:
            return None, None, None
        try:
            raw = proxy if "://" in proxy else "http://" + proxy
            u = urllib.parse.urlsplit(raw)
            if not u.hostname:
                return None, None, None
            scheme = (u.scheme or "http").lower()
            # Chromium 不认 socks5h / socks4a；它的 socks5 本来就在代理端解析域名，等价于 socks5h
            scheme = {"socks5h": "socks5", "socks4a": "socks4"}.get(scheme, scheme)
            server = f"{scheme}://{u.hostname}:{u.port}" if u.port else f"{scheme}://{u.hostname}"
            creds = None
            if u.username:
                creds = (urllib.parse.unquote(u.username),
                         urllib.parse.unquote(u.password or ""))
            return server, "<-loopback>", creds
        except Exception:
            return None, None, None

    def _get_relay(self, proxy: str):
        """为带账密上游代理获取/启动本地无鉴权转发器（每个上游复用一个）。"""
        with self._relay_lock:
            r = self._relays.get(proxy)
            if r and r.port:
                return r
            try:
                from proxyrelay import ProxyRelay
                # 每个上游代理复用一个转发器（按 upstream 去重），进程退出时在 stop() 统一清理；
                # 不做超量淘汰，避免误停正在使用的隧道。
                r = ProxyRelay(proxy)
                r.start()
                self._relays[proxy] = r
                log.info("已为带账密代理启动本地转发器 127.0.0.1:%s", r.port)
                return r
            except Exception as exc:  # noqa: BLE001
                log.warning("启动本地代理转发器失败: %s", exc)
                return None

    def _proxy_server_for(self, proxy: str | None) -> str | None:
        """给 Chromium 的 proxyServer：无账密直接用；带账密走本地转发器（解决浏览器层不支持代理鉴权）。"""
        server, _bypass, creds = self._parse_proxy(proxy)
        if not server:
            return None
        if not creds:
            return server
        relay = self._get_relay(proxy)
        if relay and relay.port:
            return "http://127.0.0.1:" + str(relay.port)
        return None  # 转发器起不来：不能退回成不带账密的上游（鉴权失败），由调用方失败关闭

    def _ua_metadata(self) -> dict:
        major = self._ua_major or "156"
        full = self._ua_full or (major + ".0.0.0")
        return {
            "brands": [
                {"brand": "Chromium", "version": major},
                {"brand": "Google Chrome", "version": major},
                {"brand": "Not=A?Brand", "version": "99"},
            ],
            "fullVersionList": [
                {"brand": "Chromium", "version": full},
                {"brand": "Google Chrome", "version": full},
                {"brand": "Not=A?Brand", "version": "99.0.0.0"},
            ],
            "fullVersion": full, "platform": "Linux", "platformVersion": "6.1.0",
            "architecture": "x86", "bitness": "64", "model": "",
            "mobile": False, "wow64": False,
        }

    @staticmethod
    def _device_profile(seed: int) -> dict:
        """按账号 seed 选一套自洽的设备参数：窗口不大于屏幕，dpr 与屏幕档位搭配。"""
        screens = [(1920, 1080, 1), (1536, 864, 1.25), (1366, 768, 1), (1440, 900, 2),
                   (1600, 900, 1), (2560, 1440, 1.5)]
        sw, sh, dpr = screens[(seed >> 5) % len(screens)]
        return {"sw": sw, "sh": sh, "dpr": dpr,
                # 浏览器窗口：宽度占满屏幕，高度扣掉任务栏与浏览器工具栏
                "ww": sw, "wh": sh - 40 - 87}

    def _stealth_script(self, seed: int, lang: str = "en-US") -> str:
        """按账号 seed 生成稳定但彼此不同的指纹覆盖脚本（软隐身：不改画布/音频像素，避免破坏页面渲染）。

        屏幕、窗口、dpr 由 CDP Emulation 覆盖（主线程、iframe、matchMedia 一致，getter 仍是原生的）；
        这里只补 CDP 覆盖不到的：webdriver、语言、WebGL 厂商串。
        hardwareConcurrency / deviceMemory 不伪造：CDP 的覆盖到不了 Worker，脚本覆盖只改主线程，
        两边矛盾比保留真实值更容易被识别。"""
        base = lang.split("-")[0]
        langs = [lang, base] if base != lang else [lang]
        vendors = ["Google Inc. (Intel)", "Google Inc. (NVIDIA)", "Google Inc. (AMD)"]
        renderers = [
            "ANGLE (Intel, Mesa Intel(R) UHD Graphics (CML GT2), OpenGL 4.6)",
            "ANGLE (NVIDIA, NVIDIA GeForce RTX 3060, OpenGL 4.6)",
            "ANGLE (AMD, AMD Radeon Graphics, OpenGL 4.6)",
        ]
        gv = vendors[seed % 3]
        gr = renderers[seed % 3]
        tpl = r"""(function(){try{
  try{Object.defineProperty(navigator,'webdriver',{get:function(){return false;}});}catch(e){}
  try{Object.defineProperty(navigator,'language',{get:function(){return __LANG0__;}});}catch(e){}
  try{Object.defineProperty(navigator,'languages',{get:function(){return __LANGS__;}});}catch(e){}
  try{var patch=function(proto){if(!proto)return;var gp=proto.getParameter;
      proto.getParameter=function(p){if(p===37445)return __GV__;if(p===37446)return __GR__;return gp.apply(this,arguments);};};
      patch(window.WebGLRenderingContext&&WebGLRenderingContext.prototype);
      patch(window.WebGL2RenderingContext&&WebGL2RenderingContext.prototype);}catch(e){}
}catch(e){}})();"""
        return (tpl
                .replace("__LANG0__", json.dumps(langs[0]))
                .replace("__LANGS__", json.dumps(langs))
                .replace("__GV__", json.dumps(gv))
                .replace("__GR__", json.dumps(gr)))

    def _apply_stealth(self, page: CDP, account_id: str | None = None,
                       lang: str = "en-US", timezone: str | None = None,
                       target_id: str | None = None):
        """在导航前套用 UA 覆盖、时区、Accept-Language 与指纹脚本。"""
        if not getattr(self.cfg, "stealth", True):
            return
        lang = lang or "en-US"
        try:
            page.send("Network.setUserAgentOverride", {
                "userAgent": self._effective_ua(),
                "acceptLanguage": lang,
                "platform": "Linux x86_64",
                "userAgentMetadata": self._ua_metadata(),
            })
        except Exception as exc:  # noqa: BLE001
            log.warning("setUserAgentOverride 失败: %s", exc)
        if timezone:
            try:
                page.send("Emulation.setTimezoneOverride", {"timezoneId": timezone})
            except Exception as exc:  # noqa: BLE001
                log.warning("setTimezoneOverride 失败（%s）: %s", timezone, exc)
        seed = int(hashlib.sha256((account_id or "default").encode()).hexdigest()[:8], 16)
        prof = self._device_profile(seed)
        try:
            page.send("Emulation.setDeviceMetricsOverride", {
                "width": prof["ww"], "height": prof["wh"], "deviceScaleFactor": prof["dpr"],
                "mobile": False, "screenWidth": prof["sw"], "screenHeight": prof["sh"]})
        except Exception as exc:  # noqa: BLE001
            log.warning("setDeviceMetricsOverride 失败: %s", exc)
        try:
            # 窗口外框也与屏幕档位一致（outerWidth/Height 否则仍是启动时的真实窗口，大于屏幕）
            win = (self.browser.send("Browser.getWindowForTarget",
                                     {"targetId": target_id} if target_id else {})
                   if self.browser else None)
        except Exception:  # noqa: BLE001
            win = None
        wid = ((win or {}).get("result") or {}).get("windowId")
        if wid is not None:
            try:
                self.browser.send("Browser.setWindowBounds", {
                    "windowId": wid, "bounds": {"width": prof["sw"], "height": prof["sh"] - 40}})
            except Exception as exc:  # noqa: BLE001
                log.warning("setWindowBounds 失败: %s", exc)
        try:
            page.send("Page.addScriptToEvaluateOnNewDocument",
                      {"source": self._stealth_script(seed, lang)})
        except Exception as exc:  # noqa: BLE001
            log.warning("注入指纹脚本失败: %s", exc)

    _BLOCK_WEBRTC_JS = (
        "(function(){try{"
        "['RTCPeerConnection','webkitRTCPeerConnection','RTCDataChannel'].forEach(function(k){"
        "try{Object.defineProperty(window,k,{value:undefined,configurable:false,writable:false});}catch(e){}});"
        "}catch(e){}})();"
    )

    def _block_webrtc(self, page: CDP):
        """配了代理的上下文禁用 WebRTC：HTTP/SOCKS 代理不承载 UDP，STUN 会绕过代理直连，暴露服务器真实 IP。
        与软隐身开关无关，总是生效（muse.ai 的对话与生成不依赖 WebRTC）。"""
        try:
            page.send("Page.addScriptToEvaluateOnNewDocument", {"source": self._BLOCK_WEBRTC_JS})
        except Exception as exc:  # noqa: BLE001
            raise MuseGenerationError(f"无法为代理账号禁用 WebRTC，已停止以免泄漏真实 IP: {exc}") from exc

    def _renew_in_page(self, page: CDP, wake_vm: bool = True) -> dict:
        """在页面上下文内 fetch /api/session（同源/同 TLS/同代理）并尽力唤醒 VM。
        尽力而为：失败不抛，会话有效性仍由页面加载态与登录检测判定。
        结果记在 self.last_renew（按线程）：{"status": HTTP 状态或 None, "assigned": bool, "note": 诊断}，
        保活 / 账号测试据此判断是否真的续签成功，不能只凭页面出现输入框就报成功。"""
        self.last_renew = {"status": None, "assigned": False, "note": "未执行 /api/session"}
        try:
            raw = page.js(
                "(async function(){try{"
                "var r=await fetch('https://muse.ai/api/session',{credentials:'include',"
                "headers:{'Accept':'application/json'}});"
                "var t=await r.text();var j=null;try{j=JSON.parse(t);}catch(e){}"
                "return JSON.stringify({status:r.status,body:j});"
                "}catch(e){return JSON.stringify({error:String(e)});}})()",
                await_promise=True, timeout=20)
            data = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except Exception as exc:  # noqa: BLE001
            log.warning("in-page /api/session 失败: %s", exc)
            self.last_renew = {"status": None, "assigned": False,
                               "note": f"/api/session 请求失败 ({type(exc).__name__})"}
            return {}
        status = data.get("status") if isinstance(data, dict) else None
        body = data.get("body") if isinstance(data, dict) else None
        assigned = status == 200 and isinstance(body, dict) and body.get("status") == "assigned"
        if assigned:
            note = "会话有效"
        elif isinstance(data, dict) and data.get("error"):
            note = "/api/session 请求失败（浏览器网络错误）"
        elif status == 200:
            note = "/api/session HTTP 200 未返回 assigned 会话"
        else:
            note = f"/api/session HTTP {status}"
        self.last_renew = {"status": status, "assigned": assigned, "note": note}
        if isinstance(data, dict) and data.get("status") == 401:
            # 同源命中已确认会话失效：立即快速失败，交由上层切号（不必等页面加载超时）
            raise MuseAuthError("会话已失效 (/api/session HTTP 401)，请用浏览器扩展重新导入 cookie")
        if not isinstance(body, dict):
            return {}
        vm_id = body.get("vm_id")
        vm_state = body.get("vm_state")
        if wake_vm and vm_id and vm_state not in (None, "DISABLED", "RUNNING"):
            payload = json.dumps({"vm_id": vm_id, "retry_count": 0,
                                  "connect_attempt_id": uuid.uuid4().hex})
            try:
                # fire-and-forget：不等待唤醒完成，避免阻塞首字/生成
                page.js(
                    "(function(){try{fetch('https://muse.ai/api/hatch/vm/wake',"
                    "{method:'POST',credentials:'include',"
                    "headers:{'Content-Type':'application/json'},"
                    "body:JSON.stringify(" + payload + ")}).catch(function(){});}catch(e){}return 1;})()",
                    await_promise=False, timeout=5)
            except Exception:  # noqa: BLE001
                pass
        return body

    # ---------------- 页面 ----------------
    def _close_page(self):
        """关闭当前线程专属的页面并销毁隔离的 BrowserContext，彻底释放内存与 Session。"""
        p = self.page
        ctx_id = self.current_context_id
        if p:
            try:
                p.close()
            except Exception:
                pass
            self.page = None
        if ctx_id and self.browser:
            try:
                self.browser.send("Target.disposeBrowserContext", {"browserContextId": ctx_id})
            except Exception:
                pass
            with self._contexts_lock:
                self._active_contexts.discard(ctx_id)
            self.current_context_id = None
        # 清理该上下文的独立下载目录，避免并发任务互相串文件
        dl = self.current_download_dir
        if dl and dl != self.cfg.download_dir and os.path.isdir(dl):
            shutil.rmtree(dl, ignore_errors=True)
        self.current_download_dir = None
        self.current_acc_id = None

    def _open_page(self, account_id: str | None = None):
        """为当前工作线程创建独立页面。
        浏览器主连接存在时创建独立 Incognito BrowserContext（多账号 Cookies/会话隔离），
        并按账号套用独立代理、时区、语言与指纹噪声（软隐身）、独立下载目录。"""
        with self._acct_meta_lock:
            has_meta = account_id in self._acct_meta
        if account_id and not has_meta:
            # 账号的代理 / 时区 / 语言配置不在了（例如请求进行中账号被删除）：读到的空配置会被当成
            # 「没配代理」，带着该账号的 cookie 走服务器本机出口直连。失败关闭，不开页
            raise MuseGenerationError("账号配置已不存在（可能已被删除），已停止，未打开页面")
        meta = self._acct(account_id)
        lang = meta.get("lang") or "en-US"
        timezone = self._default_tz(meta)
        proxy_server = self._proxy_server_for(meta.get("proxy"))
        if meta.get("proxy") and not proxy_server:
            # 配了代理却用不上：失败关闭，绝不悄悄改用服务器本机出口（会暴露真实 IP、与账号地区不符）
            raise MuseGenerationError("账号代理无法使用（格式错误或本地转发器启动失败），已停止；请检查该账号的代理设置")
        # 先取局部引用：start() 重启浏览器时会短暂把 self.browser 置 None，
        # 避免判断时有值、使用时已被置 None
        browser = self.browser
        if not browser:
            # fail-closed：没有浏览器主连接就无法创建隔离上下文；绝不退回默认共享上下文开普通标签页
            # （不走账号代理会暴露本机 IP、cookie 写入持久 profile）。上层切号重试会重新调 start()
            raise MuseGenerationError("浏览器主连接不可用（可能正在重启），已停止，未打开页面")
        try:
            ctx_params: dict = {}
            if proxy_server:
                ctx_params["proxyServer"] = proxy_server
            res = browser.send("Target.createBrowserContext", ctx_params)
            ctx_id = res.get("result", {}).get("browserContextId")
            if not ctx_id:
                raise MuseGenerationError("创建隔离浏览器上下文失败：未返回 browserContextId")
            self.current_context_id = ctx_id
            with self._contexts_lock:
                self._active_contexts.add(ctx_id)
            t_res = browser.send("Target.createTarget", {
                "url": "about:blank",
                "browserContextId": ctx_id
            })
            target_id = t_res.get("result", {}).get("targetId")
            if not target_id:
                raise MuseGenerationError("创建隔离页面失败：未返回 targetId")
            ws_url = f"ws://127.0.0.1:{self.cfg.cdp_port}/devtools/page/{target_id}"
            page = CDP(ws_url, timeout=180)
            page.send("Network.enable")
            page.send("Page.enable")
            page.send("Runtime.enable")
            if proxy_server:
                self._block_webrtc(page)
            self._apply_stealth(page, account_id, lang, timezone, target_id=target_id)
            dl = os.path.join(self.cfg.download_dir, ctx_id)
            os.makedirs(dl, exist_ok=True)
            self.current_download_dir = dl
            page.send("Browser.setDownloadBehavior",
                      {"behavior": "allow", "downloadPath": dl,
                       "browserContextId": ctx_id})
            return page
        except Exception as exc:  # noqa: BLE001
            # fail-closed：绝不退回共享默认上下文（会导致多账号 Cookie 串扰 + 代理泄漏）
            self._close_page()
            raise MuseGenerationError(
                f"创建隔离浏览器上下文失败，已中止（不退回共享上下文）: {exc}") from exc

    def renew_session_http(self, cookies: dict, expires: dict | None = None,
                           wake_vm: bool = True, proxy: str | None = None) -> dict:
        """直接调用 muse.ai/api/session 续签 hatch_vml (+48h) / hatch_sess (+30d) / hatch_gw (+1y)，
        并按需调用 /api/hatch/vm/wake 唤醒云端工作区 VM。保活/软隐身关闭时的兜底路径。
        proxy 非空时经该账号代理发出（http/https/socks5h，socks5 需 PySocks），使出口 IP 与浏览器一致。"""
        import requests
        cur_cookies = dict(cookies or {})
        cur_exp = dict(expires or {})
        if proxy and proxy.startswith("socks5://"):
            proxy = "socks5h://" + proxy[len("socks5://"):]   # 远端 DNS，避免本地解析泄漏
        proxies = {"http": proxy, "https": proxy} if proxy else None
        headers = {
            "User-Agent": self._browser_ua or _FALLBACK_UA,
            "Origin": "https://muse.ai",
            "Referer": "https://muse.ai/thread/new",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Cookie": "; ".join(f"{k}={v}" for k, v in cur_cookies.items() if v),
        }
        headers["User-Agent"] = self._effective_ua()
        try:
            r = requests.get("https://muse.ai/api/session", headers=headers,
                             timeout=12, allow_redirects=False, proxies=proxies)
        except requests.RequestException as exc:
            # 不回显请求内容：异常可能包含带凭据的代理 URL。
            raise MuseGenerationError(
                f"/api/session 网络请求失败 ({type(exc).__name__})；请检查服务器网络/代理后重试") from None
        if r.status_code == 401:
            raise MuseAuthError("会话认证失败 (/api/session HTTP 401)，请在官网确认登录后重新导入 cookie")
        if r.status_code != 200:
            hint = ("访问被拒绝，请检查服务器出口/地区/访问限制；不能据此判定 Cookie 失效"
                    if r.status_code == 403 else "上游请求未成功，请稍后重试并检查服务器网络")
            raise MuseGenerationError(f"/api/session HTTP {r.status_code}：{hint}")
        try:
            sj = r.json()
        except ValueError:
            raise MuseGenerationError("/api/session HTTP 200 返回非 JSON；会话状态未确认") from None
        if not isinstance(sj, dict) or sj.get("status") != "assigned":
            raise MuseGenerationError("/api/session HTTP 200 未返回 assigned 会话；请在官网检查账号/工作区状态")
        for c in r.cookies:
            if c.value:
                cur_cookies[c.name] = c.value
            if c.expires:
                cur_exp[c.name] = int(float(c.expires))
        vm_id = sj.get("vm_id")
        vm_state = sj.get("vm_state")
        wake_ok = False
        wake_requested = False
        if wake_vm and vm_id and vm_state != "DISABLED":
            try:
                headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in cur_cookies.items() if v)
                rw = requests.post("https://muse.ai/api/hatch/vm/wake", headers=headers, json={
                    "vm_id": vm_id,
                    "retry_count": 0,
                    "connect_attempt_id": str(uuid.uuid4()),
                }, timeout=8, proxies=proxies)
                wake_requested = rw.status_code == 200
                if wake_requested:
                    # HTTP 200 acknowledges the request; only session state confirms the VM woke.
                    verify = requests.get("https://muse.ai/api/session", headers=headers,
                                          timeout=12, allow_redirects=False, proxies=proxies)
                    if verify.status_code == 200:
                        verified_session = verify.json()
                        if isinstance(verified_session, dict) and verified_session.get("status") == "assigned":
                            vm_state = verified_session.get("vm_state") or vm_state
                            wake_ok = vm_state == "RUNNING"
            except Exception:
                pass
        return {
            "ok": sj.get("status") == "assigned",
            "status": sj.get("status"),
            "vm_id": vm_id,
            "vm_state": vm_state,
            "wake_ok": wake_ok,
            "wake_requested": wake_requested,
            "cookies": cur_cookies,
            "cookies_exp": cur_exp,
        }

    def _apply_cookies(self, page: CDP, cookies: dict, expires: dict | None = None):
        """注入 cookie。彻底清空旧账号 cookie 保证隔离，且绝不传入过去时间的 expires 防止 Chromium 丢弃 hatch_vml。"""
        try:
            page.send("Network.clearBrowserCookies")
        except Exception:
            pass
        now = time.time()
        # 记下为缺失/过近的 expires 填入的占位值：read_cookies 读回时据此剔除，
        # 不能把这个占位当成 cookie 的真实有效期写回账号。
        fabricated = {}
        self._ensure_local().fabricated_expires = fabricated  # 与 page 一样按线程持有
        self._ensure_local().injected_values = {k: v for k, v in cookies.items() if v}
        for name, value in cookies.items():
            if not value:
                continue
            exp = (expires or {}).get(name)
            try:
                exp_val = float(exp) if exp and float(exp) > now + 3600 else None
            except (TypeError, ValueError):
                exp_val = None
            if exp_val is None:
                exp_val = int(now + 7 * 86400)
                fabricated[name] = exp_val
            # 每个 cookie 只写一份（.muse.ai 覆盖 muse.ai 及子域）：两份同名 cookie 时站点续签只会替换
            # 其中一份，请求头里新旧值并存，读回时取到哪个全凭顺序
            for dom in (".muse.ai",):
                params = {
                    "name": name,
                    "value": value,
                    "domain": dom,
                    "path": "/",
                    "secure": True,
                    "expires": exp_val,
                }
                try:
                    page.send("Network.setCookie", params)
                except Exception:  # noqa: BLE001
                    pass

    def read_cookies(self) -> dict[str, dict]:
        """从当前页面读回 cookie（**包含 httpOnly**，这是网页 JS 做不到的）。

        返回 {name: {"value":..., "expires": unix秒 或 -1}}。
        用途：muse.ai 在访问时会续期部分 cookie，生成完读回来写进账号池，
        账号就不容易过期。
        """
        if not self.page:
            return {}
        fabricated = getattr(self._ensure_local(), "fabricated_expires", None) or {}
        injected = getattr(self._ensure_local(), "injected_values", None) or {}
        try:
            msg = self.page.send("Network.getCookies",
                                 {"urls": [self.cfg.site_url]}, timeout=20)
        except Exception:  # noqa: BLE001
            return {}
        out: dict[str, dict] = {}
        for c in (msg.get("result", {}).get("cookies") or []):
            name = c.get("name")
            if not name:
                continue
            try:
                exp = int(float(c.get("expires", -1)))
            except (TypeError, ValueError):
                exp = -1
            if exp > 0 and abs(exp - fabricated.get(name, -10)) <= 1:
                exp = -1  # 注入时填的占位有效期（服务端没有重新下发），不是真实值
            prev = out.get(name)
            value = c.get("value", "")
            # 同名 cookie 有多份（站点续签按本域 muse.ai 下发，与注入的 .muse.ai 那份并存）：
            # 取更晚到期的；到期相同时取不等于注入值的那份（站点新下发的），不看返回顺序
            if (prev is None or exp > prev["expires"]
                    or (exp == prev["expires"] and prev["value"] == injected.get(name)
                        and value != injected.get(name))):
                out[name] = {"value": value, "expires": exp}
        return out

    def _wait_ws_ready(self, page: CDP, timeout: float = 15.0) -> bool:
        """等待 muse.ai 页面完成 React hydration 且不再处于 Connecting... 状态。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                st = page.js("""(function(){
                    if (document.readyState !== 'complete') return 'loading';
                    if (!document.querySelector('textarea')) return 'no-ta';
                    var h = document.querySelector('[data-hatch-shell-hydration-state]');
                    if (h && h.getAttribute('data-hatch-shell-hydration-state') !== 'hydrated') return 'hydrating';
                    var b = document.body ? (document.body.innerText || '') : '';
                    // 侧栏 / 会话列表里其它会话残留的状态字样不算本页卡住
                    [].slice.call(document.querySelectorAll('nav,aside,[role=navigation],[role=complementary]'))
                        .forEach(function(n){ var t = n.innerText || ''; if (t) b = b.split(t).join(''); });
                    if (b.indexOf('Connecting...') !== -1) return 'connecting';
                    // Empty composers may not render Send until text has been entered.
                    if (document.querySelector('textarea').disabled) return 'input-disabled';
                    return 'ready';
                })()""")
                if st == "ready":
                    return True
            except Exception:
                pass
            time.sleep(0.1)
        return False

    def _wait_origin(self, page: CDP, timeout: float = 15.0) -> bool:
        """等待页面进入 muse.ai 同源且文档可用（document 非 loading），以便在其中 fetch /api/session。
        用于在「完全就绪」之前尽早唤醒冷 VM。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if page.js("(function(){try{return (location.hostname||'').indexOf('muse.ai')!==-1"
                           " && document.readyState!=='loading';}catch(e){return false;}})()"):
                    return True
            except Exception:
                pass
            time.sleep(0.2)
        return False

    def reset_thread(self, for_chat: bool = False):
        """关闭残留弹窗并确保处于干净会话且 WebSocket 已就绪。
        对于纯文本对话（for_chat=True），若当前热页面无附件、无卡死且气泡数较少，直接复用现有热连接以实现 2s 级秒回。"""
        if not self.page:
            return
        try:
            needs_nav = self.page.js("""(function(forChat){
                var d = document.querySelector('[role="dialog"]');
                if (d) {
                    var b = d.querySelector('button[aria-label*="close" i], button');
                    if (b) b.click();
                }
                var scope = document.querySelector('main,[class*="chat-scroll"],[class*="hatch-chat-scroll"]') || document.body;
                var bubbleCount = scope ? scope.querySelectorAll('div[class*="hatch-chat-groupable-bubble"]').length : 0;
                var hasAtts = document.querySelectorAll('[data-testid^="hatch-chat-attachment-presentation-"]').length > 0;
                var hasStop = !!(document.querySelector('[data-testid="hatch-composer-stop-button"]')
                    || document.querySelector('button[aria-label*="Stop" i]')
                    || document.querySelector('button[aria-label*="停止"]'));
                var bodyTxt = document.body ? (document.body.innerText || '') : '';
                // 侧栏 / 会话列表里其它会话残留的状态字样不算本页卡住
                [].slice.call(document.querySelectorAll('nav,aside,[role=navigation],[role=complementary]'))
                    .forEach(function(n){ var t = n.innerText || ''; if (t) bodyTxt = bodyTxt.split(t).join(''); });
                var hasStuck =bodyTxt.indexOf('Still sending') !== -1 || bodyTxt.indexOf('Connecting...') !== -1;
                if (hasStop || hasStuck || hasAtts) return true;
                if (forChat) {
                    return bubbleCount >= 24;
                }
                return (window.location.pathname !== '/thread/new') || bubbleCount > 0;
            })(%s)""" % ("true" if for_chat else "false"))
            if needs_nav:
                self.page.send("Page.navigate", {"url": "https://muse.ai/thread/new"})
                t_end = time.time() + 10.0
                while time.time() < t_end:
                    time.sleep(0.08)
                    ready = self.page.js("""(function(){
                        return document.readyState === 'complete'
                            && !!document.querySelector('textarea')
                            && document.querySelectorAll('div[class*="hatch-chat-groupable-bubble"]').length === 0;
                    })()""")
                    if ready:
                        break
                self._wait_ws_ready(self.page, timeout=12.0)
        except Exception:
            pass

    def ensure_page(self, cookies: dict, expires: dict | None = None, account_id: str | None = None):
        self.last_renew = None
        self.loaded_sess = (cookies or {}).get("hatch_sess")  # 本线程页面加载的会话，cookie 回写时比对
        if self.page is not None and (account_id is None or getattr(self, "current_acc_id", None) == account_id):
            try:
                if self.page.js("!!document.querySelector('textarea')") and self._wait_ws_ready(self.page, timeout=8.0):
                    return self.page
            except Exception:
                pass
        self._close_page()
        # 仅当距离上次 HTTP 续签超过 10 分钟时才在主链路调用 /api/session，避免每次切号重复阻塞
        last_map = getattr(self, "_last_http_renew", None)
        if last_map is None:
            last_map = {}
            self._last_http_renew = last_map
        now_ts = time.time()
        meta = self._acct(account_id)
        # 软隐身开启时：主链路不再用 Python requests 命中 /api/session；
        # 改由页面就绪后浏览器内同源 fetch（_renew_in_page）续签+唤醒，TLS/代理与浏览器完全一致。
        # 会话失效由随后页面加载的登录态检测负责（不会漏判）。关闭软隐身时保留 requests 预续签兜底。
        if not getattr(self.cfg, "stealth", True) and (
                not account_id or (now_ts - last_map.get(account_id, 0) > 600)):
            try:
                renewed = self.renew_session_http(cookies, expires, wake_vm=True,
                                                  proxy=meta.get("proxy"))
                if renewed.get("cookies"):
                    cookies = renewed["cookies"]
                if renewed.get("cookies_exp"):
                    expires = renewed["cookies_exp"]
                if account_id:
                    last_map[account_id] = now_ts
            except MuseAuthError:
                raise
            except Exception as e:
                log.warning("预续签 /api/session 失败（继续尝试浏览器加载）: %s", e)

        page = self._open_page(account_id)
        self._apply_cookies(page, cookies, expires)
        page.send("Page.navigate", {"url": "https://muse.ai/thread/new"})
        self.page = page  # Cleanup must also own pages that never become ready.
        # 软隐身：导航后、等待完全就绪前，在 muse.ai 同源内命中 /api/session 并按需唤醒 VM。
        # 冷 VM 需要这一步才能在 login_wait 内连上（同 TLS/同代理、无 Python requests）；
        # 过期会话（401）在此快速失败、立即切号。
        if getattr(self.cfg, "stealth", True):
            try:
                if self._wait_origin(page, timeout=min(10.0, self.cfg.login_wait)):
                    self._renew_in_page(page, wake_vm=True)
                    # 续签成功就立即落盘新 cookie：之后页面若卡在 Connecting... 超时被关掉，
                    # 站点刚下发的新 cookie 也不会丢
                    if account_id and (self.last_renew or {}).get("assigned") and self.on_renewed:
                        try:
                            self.on_renewed(account_id)
                        except Exception as exc:  # noqa: BLE001
                            log.warning("续签后保存 cookie 失败: %s", exc)
            except MuseAuthError:
                self._close_page()
                raise
            except Exception:
                pass
        if self._wait_ws_ready(page, timeout=self.cfg.login_wait):
            self.current_acc_id = account_id
            return page
        try:
            # 去掉侧栏 / 会话列表：其它会话残留的 Connecting... 不算本页网关卡住
            body = (page.js(
                "(function(){if(!document.body)return '';var b=document.body.innerText||'';"
                "[].slice.call(document.querySelectorAll('nav,aside,[role=navigation],[role=complementary]'))"
                ".forEach(function(n){var t=n.innerText||'';if(t)b=b.split(t).join('');});"
                "return b.slice(0,1200);})()") or "").lower()
        except Exception:
            body = ""
        try:
            login_dom = bool(page.js(self._LOGIN_PAGE_JS))
        except Exception:
            login_dom = False
        self._close_page()
        if "connecting..." in body:
            raise MuseGenerationError("页面网关连接超时 (Connecting...)，正在自动切换账号")
        # Distinguish an expired session from a page that never became ready.
        if login_dom or _looks_logged_out(body):
            raise MuseAuthError("会话已被 muse.ai 登出（可能被其它登录挤掉或触发风控），"
                                "请用浏览器扩展重新导入 cookie")
        raise MuseGenerationError("muse.ai 页面加载超时（未出现聊天输入框），请检查服务器网络后重试；未确认会话失效")

    def close_current_page(self):
        """显式释放当前线程所持有的页面和隔离上下文。"""
        self._close_page()

    def refresh(self, cookies: dict, expires: dict | None = None, account_id: str | None = None):
        self._close_page()
        return self.ensure_page(cookies, expires, account_id=account_id)

    # ---------------- 额度查询（Settings 面板） ----------------
    # muse.ai 的额度在底部 Settings 菜单 → Settings 项 → 设置面板的
    # General → Usage 区块里，形如：
    #   Free plan
    #   Weekly limit resets on Sep 30
    #   1% used
    #   Additional tokens / Never expires / 0% used (2B tokens left)
    def _click_point(self, x: int, y: int):
        for t in ("mousePressed", "mouseReleased"):
            self.page.send("Input.dispatchMouseEvent",
                           {"type": t, "x": x, "y": y,
                            "button": "left", "clickCount": 1})

    _CLICK_JS = (
        "(function(){var sel=%s;"
        "var b=[...document.querySelectorAll(sel)]"
        ".filter(function(x){return x.offsetParent!==null;})[0];"
        "if(!b)return null;var r=b.getBoundingClientRect();"
        "return JSON.stringify({x:Math.round(r.x+r.width/2),"
        "y:Math.round(r.y+r.height/2)});})()")

    _QUOTA_SETTINGS_JS = r"""(() => {
        const visible = e => e.getClientRects().length > 0
            && getComputedStyle(e).visibility !== 'hidden';
        const labels = ['Settings', '設定', '设置'];
        const b = [...document.querySelectorAll('[data-testid="hatch-dock-more"]')].find(visible)
            || [...document.querySelectorAll('button,[role=button]')].find(e =>
                visible(e) && labels.includes((e.getAttribute('aria-label') || '').trim()));
        if (!b) return null;
        const r = b.getBoundingClientRect();
        return JSON.stringify({x:Math.round(r.x+r.width/2),y:Math.round(r.y+r.height/2)});
    })()"""

    _QUOTA_MENU_JS = r"""(() => {
        const visible = e => e.getClientRects().length > 0
            && getComputedStyle(e).visibility !== 'hidden';
        const labels = ['Settings', '設定', '设置'];
        const exact = e => labels.includes((e.textContent || '').trim());
        // Menu items include a shortcut suffix (for example 設定Ctrl+,).
        const b = [...document.querySelectorAll('[role=menuitem]')].find(e =>
            visible(e) && (exact(e) || [...e.querySelectorAll('span,div')].some(exact)))
            || [...document.querySelectorAll('[role=menu] button,[role=menu] span,[role=menu] div')]
                .find(e => visible(e) && exact(e));
        if (!b) return null;
        const r = b.getBoundingClientRect();
        return JSON.stringify({x:Math.round(r.x+r.width/2),y:Math.round(r.y+r.height/2)});
    })()"""

    _QUOTA_DIALOG_JS = r"""(() => {
        const d = [...document.querySelectorAll('[role=dialog],[aria-modal=true]')]
            .find(e => e.getClientRects().length > 0 && getComputedStyle(e).visibility !== 'hidden');
        return d ? (d.innerText || '') : '';
    })()"""

    def _quota_click(self, expr: str, error: str, timeout: float = 8.0):
        deadline = time.monotonic() + timeout
        while True:
            raw = self.page.js(expr)
            if raw:
                pt = json.loads(raw)
                self._click_point(pt['x'], pt['y'])
                return
            if time.monotonic() >= deadline:
                raise MuseGenerationError(error)
            time.sleep(0.15)

    def _dismiss_quota_ui(self):
        # A failed lookup can leave a menu open; clean up menus and dialogs.
        for _ in range(2):
            for typ in ('keyDown', 'keyUp'):
                self.page.send('Input.dispatchKeyEvent',
                               {'type': typ, 'key': 'Escape', 'code': 'Escape',
                                'windowsVirtualKeyCode': 27, 'nativeVirtualKeyCode': 27})
            time.sleep(0.1)  # Let the UI finish closing before the next interaction.

    def quota(self, cookies: dict, expires: dict | None = None,
              account_id: str | None = None) -> dict:
        """Read one account's quota in a fresh, disposable browser context."""
        # Muse's Settings menu can remain in a half-closed React state after a read.
        # A disposable context is slower, but prevents consecutive reads from
        # inheriting stale UI and keeps account cookies strictly isolated.
        self._close_page()
        try:
            self.ensure_page(cookies, expires, account_id=account_id)
            self._quota_click(self._QUOTA_SETTINGS_JS, '找不到设置入口（Settings／設定／设置）')
            self._quota_click(self._QUOTA_MENU_JS, '设置菜单未出现可识别的设置项')
            deadline = time.monotonic() + 12.0
            while True:
                parsed = self._parse_quota(self.page.js(self._QUOTA_DIALOG_JS) or '')
                if parsed['found']:
                    return parsed
                if time.monotonic() >= deadline:
                    raise MuseGenerationError('额度面板未读到有效用量；旧缓存未覆盖')
                time.sleep(0.2)
        finally:
            self._close_page()

    @staticmethod
    def _parse_quota(txt: str) -> dict:
        """Parse English, traditional Chinese and simplified Chinese usage text."""
        normalized = unicodedata.normalize('NFKC', txt or '')
        lines = [ln.strip() for ln in normalized.splitlines() if ln.strip()]
        # Avoid unrelated account/profile text before the Usage section.
        start = next((i + 1 for i, ln in enumerate(lines)
                      if ln.lower() in ('usage', '用量', '使用量')), 0)
        lines = lines[start:]
        end = next((i for i, ln in enumerate(lines)
                    if ln.lower() in ('language', '语言', '語言')), len(lines))
        lines = lines[:end]
        out: dict = {'raw': '\n'.join(lines[:40])}
        for ln in lines:
            if re.fullmatch(r'.{1,60}?(?:\s+plan|計劃|计划|方案)', ln, re.I):
                out['plan'] = ln
                break
        text = '\n'.join(lines)
        reset = re.search(r'Weekly limit resets? on ([^\n]+)', text, re.I)
        if not reset:
            reset = re.search(
                r'(?:每(?:週|周|星期)|週|周)(?:上限|限額|限额|額度|额度)'
                r'(?:會|会|將|将)?(?:於|于|在)\s*([^\n]+?)\s*(?:重設|重置)', text)
        if reset:
            out['weekly_reset'] = reset.group(1).strip()

        extra_at = next((i for i, ln in enumerate(lines) if re.match(
            r'^(?:(?:Additional|Extra)\s+tokens|(?:額外|额外)(?:的)?(?:符元|符記|代幣|代币|令牌|憑證|凭证|tokens))',
            ln, re.I)), len(lines))

        def percentage(line):
            m = re.search(r'(\d+(?:\.\d+)?)\s*%\s*(?:used\b|已(?:使)?用)', line, re.I)
            if not m:
                m = re.search(r'已(?:使)?用\s*(\d+(?:\.\d+)?)\s*%', line)
            if not m:
                return None
            value = float(m.group(1))
            if not 0 <= value <= 100:
                return None
            return int(value) if value.is_integer() else value

        for ln in lines[:extra_at]:
            value = percentage(ln)
            if value is not None:
                out['weekly_used_pct'] = value
                break
        for ln in lines[extra_at:]:
            value = percentage(ln)
            if value is not None:
                out['extra_used_pct'] = value
                left = re.search(r'\(([^)]+)\)', ln)
                if left:
                    out['extra_left'] = left.group(1).strip()
                break
        if re.search(r'Never expires|永不過期|永不过期', text, re.I):
            out['extra_expires'] = 'never'
        # A plan label alone is not a successful quota read. Preserve old cache.
        out['found'] = 'weekly_used_pct' in out
        return out

    # ---------------- 附件（生成结果） ----------------
    _ATT_JS = (
        "(function(){"
        "var list = []; var seen = new Set();"
        "function addEl(el, tid){"
        "  if(!el || seen.has(el)) return;"
        "  seen.add(el);"
        "  if(el.closest('form, [class*=chat-user-bubble], [class*=\"group/msg\"]')) return;"
        "  var v = el.querySelector('video') || (el.tagName === 'VIDEO' ? el : null);"
        "  var img = el.querySelector('img') || (el.tagName === 'IMG' ? el : null);"
        "  var isVid = (tid || '').includes('video') || !!v;"
        "  var primary = isVid ? (v || img) : (img || v);"
        "  var src = primary ? (primary.currentSrc || primary.src || '') : '';"
        "  if(src && !seen.has(src)){"
        "    seen.add(src);"
        "    list.push({"
        "      tid: tid || el.getAttribute('data-testid') || (isVid ? 'video' : 'image'),"
        "      hasVideo: !!v,"
        "      hasImg: !!img,"
        "      src: src,"
        "      vSrc: v ? (v.currentSrc || v.src || '') : '',"
        "      iSrc: img ? (img.currentSrc || img.src || '') : '',"
        "      w: primary ? (primary.videoWidth || primary.naturalWidth || 0) : 0,"
        "      h: primary ? (primary.videoHeight || primary.naturalHeight || 0) : 0"
        "    });"
        "  }"
        "}"
        "document.querySelectorAll('[data-testid^=\"hatch-chat-attachment-presentation-\"]').forEach(function(a){ addEl(a, a.getAttribute('data-testid')); });"
        "document.querySelectorAll('div[class*=\"hatch-agent-bubble-bg\"] img, div[class*=\"hatch-agent-bubble-bg\"] video').forEach(function(m){"
        "  var s = m.currentSrc || m.src || '';"
        "  if(s && !s.includes('avatar') && !s.includes('emoji')) addEl(m.parentElement || m, 'agent-media');"
        "});"
        "return JSON.stringify(list);"
        "})()"
    )

    # 登录页的结构特征：URL 在登录 / 授权路径上，或页面上有可见的密码 / 邮箱输入框
    _LOGIN_PAGE_JS = (
        "(function(){var p=(location.pathname||'').toLowerCase();"
        "if(/\\/(login|log-in|signin|sign-in|signup|sign-up|auth)(\\/|$)/.test(p))return true;"
        "return [].slice.call(document.querySelectorAll('input[type=password],input[type=email],"
        "input[autocomplete=email],input[autocomplete=username]'))"
        ".some(function(x){return x.offsetParent!==null;});})()"
    )

    def _check_page_alive(self, exc: BaseException | None = None):
        """页面连接已断开就抛 MusePageGoneError；轮询函数吞异常前先调它，免得空转到超时。"""
        page = self.page
        if isinstance(exc, CDPConnectionError) or page is None or getattr(page, "closed", False):
            raise MusePageGoneError("浏览器页面已断开（标签页崩溃或浏览器被关闭），已停止等待") from exc

    def attachments(self) -> list[dict]:
        try:
            raw = self.page.js(self._ATT_JS)
            return json.loads(raw) if raw else []
        except Exception as exc:  # noqa: BLE001
            self._check_page_alive(exc)
            return []

    # ---------------- 发送 ----------------
    # 点击 / 回车之后确认消息真的提交了：输入框被清空、用户气泡数增加或 Stop 按钮出现，任一即可
    _SEND_STATE_JS = (
        "(function(){var ta=document.querySelector('textarea');"
        "var n=[].slice.call(document.querySelectorAll('div[class*=\"hatch-chat-groupable-bubble\"]'))"
        ".filter(function(b){return /chat-user-bubble/.test(b.className||'');}).length;"
        "var stop=!!(document.querySelector('[data-testid=\"hatch-composer-stop-button\"]')"
        "||document.querySelector('button[aria-label*=\"Stop\" i]')"
        "||document.querySelector('button[aria-label*=\"停止\"]'));"
        "return JSON.stringify({v:ta?ta.value:'',users:n,stop:stop});})()"
    )
    SEND_CONFIRM_SECONDS = 6.0

    def _send_confirmed(self, users_before: int, typed: bool) -> bool:
        """typed：发送前输入框里确实有文字。没有的话「输入框为空」说明不了任何事，只认气泡数和 Stop。"""
        deadline = time.time() + self.SEND_CONFIRM_SECONDS
        while True:
            try:
                d = json.loads(self.page.js(self._SEND_STATE_JS) or "{}")
                if ((typed and not d.get("v")) or int(d.get("users") or 0) > users_before
                        or d.get("stop")):
                    return True
            except Exception as exc:  # noqa: BLE001
                self._check_page_alive(exc)
            if time.time() >= deadline:
                return False
            time.sleep(0.2)

    def _send(self, prompt: str):
        """返回 clicked / enter-sent（已确认提交），或 unconfirmed / no-button（没发出去，可安全切号重试）。"""
        users_before = self._user_count()
        # 1. 确保 textarea 滚动到视口中央并获得真实焦点
        try:
            self.page.js("""(function(){
                var ta = document.querySelector('textarea');
                if (ta) {
                    ta.scrollIntoView({block: 'center', inline: 'nearest'});
                    ta.focus();
                }
            })()""")
        except Exception:
            pass
        time.sleep(0.1)

        rect = self.page.js(
            "(function(){var t=document.querySelector('textarea');if(!t)return null;"
            "var r=t.getBoundingClientRect();"
            "return JSON.stringify({x:Math.round(r.left+r.width/2),"
            "y:Math.round(r.top+r.height/2)});})()")
        if not rect:
            raise MuseGenerationError("找不到聊天输入框")
        c = json.loads(rect)
        for t in ("mousePressed", "mouseReleased"):
            self.page.send("Input.dispatchMouseEvent",
                           {"type": t, "x": c["x"], "y": c["y"],
                            "button": "left", "clickCount": 1})
        time.sleep(0.1)

        # 触发 React 18 原型 setter 以及 input/change 事件以同步发送按钮状态
        # （对于 DeepSeek/Codex 等 100KB+ 超长上下文，直接走原型 setter 仅需 <1s，避免 Input.insertText 逐字注入卡死）
        _SETTER_JS = (
            "(function(t){var ta=document.querySelector('textarea');"
            "if(!ta) return 0;"
            "ta.focus();"
            "var s=Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype,'value').set;"
            "s.call(ta,t);"
            "ta.dispatchEvent(new Event('input',{bubbles:true}));"
            "ta.dispatchEvent(new Event('change',{bubbles:true}));"
            "return (ta.value||'').length;})(%s)")
        val_len = self.page.js(_SETTER_JS % json.dumps(prompt)) or 0
        if not val_len and len(prompt) < 500:
            self.page.send("Input.insertText", {"text": prompt})
            val_len = self.page.js(_SETTER_JS % json.dumps(prompt)) or 0
        typed = bool(val_len)

        try:
            clicked = self._click_send()
        except (MusePageGoneError, CDPConnectionError) as exc:
            # 点击脚本或回车按键已发出后页面断开：点击可能已生效，不能当成没发出去换号重发
            raise MuseSubmittedError(
                f"已触发发送，但浏览器页面随即断开（{type(exc).__name__}），可能已提交，未换号重发") from exc
        try:
            confirmed = clicked in ("clicked", "enter-sent") and self._send_confirmed(users_before, typed)
        except (MusePageGoneError, CDPConnectionError) as exc:
            # 已点击发送 / 按了回车，确认期间页面断开：提示词可能已送达计费，不能当成没发出去换号重发
            raise MuseSubmittedError(
                f"已点击发送，但确认提交时浏览器页面断开（{type(exc).__name__}），可能已提交，未换号重发") from exc
        if clicked in ("clicked", "enter-sent") and not confirmed:
            # 点了 / 按了却没提交（输入框没清空、没有新的用户气泡、也没出现 Stop）：
            # 立即失败，免得生图空等到总时限；没发出去，换号重试不会重复扣额度
            log.warning("提示词发送未确认（%s 之后输入框未清空、无新消息）", clicked)
            clicked = "unconfirmed"
        time.sleep(0.3)
        return clicked

    def _click_send(self) -> str:
        """点发送按钮，找不到或一直禁用就按回车兜底。返回 clicked / enter-sent / no-button。
        这里的每次页面调用都可能已经触发了发送：页面断开的异常原样抛出，由 _send 按可能已提交处理。"""
        clicked = "no-button"
        t_deadline = time.time() + 3.0
        while time.time() < t_deadline:
            res = self.page.js(
                "(function(){var b=[...document.querySelectorAll('button,[role=button]')]"
                ".filter(function(x){return x.offsetParent!==null;})"
                ".find(function(x){return /发送|send/i.test(x.getAttribute('aria-label')||'')"
                "||/发送|send/i.test(x.getAttribute('data-testid')||'')"
                "||/send/i.test(x.innerText||'');});"
                "if(!b)return 'no-button';"
                "if(b.disabled)return 'disabled';"
                "b.click();return 'clicked';})()")
            if res == "clicked":
                clicked = "clicked"
                break
            time.sleep(0.08)

        if clicked != "clicked":
            # 兜底：Ctrl+Enter 或普通 Enter
            for combo in ({"modifiers": 1}, {}):
                for t in ("keyDown", "char", "keyUp"):
                    params = {"type": t, "key": "Enter", "code": "Enter",
                              "windowsVirtualKeyCode": 13, "nativeVirtualKeyCode": 13,
                              "modifiers": combo.get("modifiers", 0)}
                    if t == "char":
                        params["text"] = "\r"
                        params["unmodifiedText"] = "\r"
                    self.page.send("Input.dispatchKeyEvent", params)
                time.sleep(1.0)
                try:
                    if self.page.js("!(document.querySelector('textarea')||{value:''}).value"):
                        clicked = "enter-sent"
                        break
                except Exception as exc:  # noqa: BLE001
                    self._check_page_alive(exc)  # 回车已发出后页面断开：交给 _send 按可能已提交处理
        return clicked

    # ---------------- 等待生成 ----------------
    def _last_attachment(self) -> dict | None:
        atts = self.attachments()
        return atts[-1] if atts else None

    def _scroll_bottom(self):
        """滚到聊天底部。muse.ai 的聊天滚动容器是内层 div（不是 document），
        虚拟列表按滚动位置渲染节点 —— 不滚到底，新消息根本不在 DOM 里。"""
        try:
            self.page.js(
                "(function(){"
                "var els=[...document.querySelectorAll('*')].filter(function(e){"
                "var s=getComputedStyle(e);"
                "return (s.overflowY==='auto'||s.overflowY==='scroll')"
                "&&e.scrollHeight>e.clientHeight+100;});"
                "els.sort(function(a,b){return b.scrollHeight-a.scrollHeight;});"
                "if(els[0])els[0].scrollTop=els[0].scrollHeight;"
                "var s=document.scrollingElement||document.body;"
                "s.scrollTop=s.scrollHeight;"
                "var el=document.querySelector('textarea');"
                "if(el)el.scrollIntoView({block:'end'});return 1;})()")
        except Exception:  # noqa: BLE001
            pass

    def _wait_attachment(self, baseline_src: str, timeout: int, expect: str,
                         on_progress=None, base_agent_cnt: int = 0,
                         base_att_cnt: int = 0, stop_event=None, baseline_sources=None) -> dict | None:
        baseline_sources = set(baseline_sources or ()) | {baseline_src}
        deadline = time.time() + timeout
        t_start = time.time()
        stable_src, stable_n = "", 0
        last_txt, txt_stable = "", 0
        refusal_last, refusal_stable = "", 0
        seen_stop = False
        previous_state = None
        last_activity = t_start
        progress_sent = 0
        while time.time() < deadline:
            if stop_event is not None and stop_event.is_set():
                raise MuseGenerationError("客户端已断开连接，终止生成任务")
            time.sleep(0.6)
            self._scroll_bottom()
            atts = self.attachments()
            # 严格按期望类型挑选附件，绝不接受跨模态兜底（如将视频请求降级为图片封面）
            matching_atts = []
            for a in atts:
                tid = (a.get("tid") or "").lower()
                has_video = bool(a.get("hasVideo"))
                src = a.get("src") or ""
                v_src = a.get("vSrc") or ""
                if expect == "video":
                    # tid 含 "video" 不足以命中：muse 会先把 data-testid 标成
                    # ...-presentation-video，此时 <video> 可能尚未挂上，_ATT_JS 的
                    # primary = v || img 会退回封面 img，src/w/h 全是静帧。
                    # 只认真正的视频源（vSrc / hasVideo / 视频扩展名）。
                    is_match = (
                        has_video
                        or bool(v_src)
                        or src.endswith((".mp4", ".webm", ".mov"))
                    )
                else:
                    # 图片任务不接受视频源（含 tid 写着 image 却挂着 mp4 的情况）
                    is_match = (not has_video and not v_src
                                and not src.endswith((".mp4", ".webm", ".mov")))
                    if is_match and tid == "agent-media":
                        # 不在附件容器里的气泡内联图（来源图标、表情、缩略图）：尺寸已知且太小就不是成品
                        w0, h0 = a.get("w", 0) or 0, a.get("h", 0) or 0
                        is_match = not (w0 and h0 and min(w0, h0) < MIN_RESULT_IMAGE_PX)
                if is_match:
                    matching_atts.append(a)

            # 附件容器（hatch-chat-attachment-presentation-*）优先；没有时才退到气泡内联媒体
            boxed = [a for a in matching_atts if (a.get("tid") or "").lower() != "agent-media"]
            att = (boxed or matching_atts)[-1] if matching_atts else None
            if att:
                src = att.get("src") or ""
                v_src = att.get("vSrc") or ""
                w = att.get("w", 0) or 0
                h = att.get("h", 0) or 0
                # 视频任务禁止用封面 src 冒充成品；无 vSrc 时仅接受显式视频后缀的 src
                if expect == "video":
                    check_src = v_src if v_src else (
                        src if src.endswith((".mp4", ".webm", ".mov")) else ""
                    )
                else:
                    check_src = src
                if check_src and check_src not in baseline_sources:
                    if w > 0 and h > 0:
                        return att
                    if check_src == stable_src:
                        stable_n += 1
                    else:
                        stable_src, stable_n = check_src, 0
                    if stable_n >= 1:
                        return att
            elapsed = time.time() - t_start
            cur_cnt = base_agent_cnt
            cur_txt = ""
            has_stop = False
            tail = ""
            try:
                st_raw = self.page.js("""(function(){
                    var bs=[].slice.call(document.querySelectorAll('div[class*="hatch-chat-groupable-bubble"]'))
                        .filter(function(b){return /hatch-agent-bubble-bg/.test(b.className||'');});
                    var lastTxt = bs.length ? (bs[bs.length-1].innerText||'').trim() : '';
                    var hasStop = !!(document.querySelector('[data-testid="hatch-composer-stop-button"]')
                        || document.querySelector('button[aria-label*="Stop" i]')
                        || document.querySelector('button[aria-label*="停止"]'));
                    var tail = document.body ? (document.body.innerText||'').slice(-1200) : '';
                    // 只留站点自身的 UI 文本（横幅 / 提示条）：去掉所有聊天气泡与输入框，
                    // 用户提示词或助手回复里出现「积分不足」等字样时不能误判额度耗尽
                    var sysTail = '';
                    if (document.body) {
                        // 只取可见节点：隐藏提示（display:none / hidden / aria-hidden）、脚本与样式不算
                        var hidden = [].slice.call(document.body.querySelectorAll('*')).filter(function(n){
                            if (/^(SCRIPT|STYLE|NOSCRIPT|TEMPLATE)$/.test(n.tagName)) return true;
                            if (!n.firstChild) return false;  // 没有子节点的元素不含文本，免算样式
                            if (n.hidden || n.getAttribute('aria-hidden') === 'true') return true;
                            var cs = getComputedStyle(n);
                            return cs.display === 'none' || cs.visibility === 'hidden';
                        });
                        hidden.forEach(function(n){ n.setAttribute('data-m2a-hidden', '1'); });
                        var clone = document.body.cloneNode(true);
                        hidden.forEach(function(n){ n.removeAttribute('data-m2a-hidden'); });
                        // 侧栏 / 会话列表里的旧会话标题（由以前的提示词生成）也不算站点提示
                        [].slice.call(clone.querySelectorAll('[data-m2a-hidden], [class*=hatch-chat-groupable-bubble], [class*=chat-user-bubble], [class*="group/msg"], form, textarea, nav, aside, [role=navigation], [role=complementary]'))
                            .forEach(function(n){ n.remove(); });
                        // 只取末尾 700 字（与原项目一致）：侧栏一般排在页面前部，就算上面的 nav/aside
                        // 没认出 muse.ai 的侧栏，末尾窗口也基本取不到它
                        sysTail = (clone.textContent||'').slice(-700);
                    }
                    return JSON.stringify({cnt: bs.length, txt: lastTxt, stop: hasStop, tail: tail,
                        sys_tail: sysTail,
                        path: location.pathname || '', href: location.href || ''});
                })()""")
                st = json.loads(st_raw) if st_raw else {}
            except Exception as exc:  # noqa: BLE001
                self._check_page_alive(exc)
                st = {}
            tail = st.get("tail") or ""
            sys_tail = st.get("sys_tail") or ""
            cur_cnt = int(st.get("cnt") or 0)
            cur_txt = st.get("txt") or ""
            has_stop = bool(st.get("stop"))
            path = st.get("path") or ""
            seen_stop = seen_stop or has_stop
            state = (cur_cnt, cur_txt, len(atts), has_stop, path)
            if previous_state is None or state != previous_state:
                last_activity = time.time()
                previous_state = state

            if on_progress:
                milestone = 25 if seen_stop else 20
                if cur_cnt > base_agent_cnt:
                    milestone = 40
                if milestone > progress_sent:
                    try:
                        on_progress(milestone)
                        progress_sent = milestone
                    except Exception:
                        pass

            if _QUOTA_RE.search(sys_tail):
                raise MuseQuotaError("账号额度不足")
            # A Stop button is also shown for messages never acknowledged by the gateway.
            if elapsed > 30.0 and cur_cnt == base_agent_cnt and re.search(
                    r"Still sending|Delivery not confirmed|正在发送|未确认送达", sys_tail, re.I):
                # 网关回执慢时消息可能其实已送达并开始计费：不换号重发，按「可能已提交」上报
                raise MuseSubmittedError(
                    "提示词未确认送达后端（Still sending / Delivery not confirmed），可能已提交，未换号重发")

            refusal_source = cur_txt if cur_cnt > base_agent_cnt else (
                tail if seen_stop and not has_stop and elapsed > 20.0 else "")
            refusal = self._media_refusal_text(refusal_source, expect)
            if refusal and not has_stop and len(atts) <= base_att_cnt:
                if refusal == refusal_last:
                    refusal_stable += 1
                else:
                    refusal_last, refusal_stable = refusal, 0
                if refusal_stable >= 4 and elapsed > 8.0:
                    raise MuseModelRefusalError(f"模型未生成媒体，仅返回文本: {refusal}")
            else:
                refusal_last, refusal_stable = "", 0

            if cur_cnt > base_agent_cnt and cur_txt and not has_stop and len(atts) <= base_att_cnt:
                # Media-status text can precede a real attachment; do not fail on it.
                is_media_report = bool(re.search(
                    r"\.(?:webp|png|jpe?g|mp4|webm)|imagine_media|deliverable|"
                    r"generated\s+.*image|verified\s+generated|artifact", cur_txt, re.I))
                if not is_media_report:
                    if cur_txt == last_txt:
                        txt_stable += 1
                    else:
                        last_txt, txt_stable = cur_txt, 0
                    if txt_stable >= 15 and elapsed > 8.0:
                        clean = self._clean_model_text(cur_txt)
                        raise MuseModelRefusalError(
                            f"模型未生成媒体，仅返回文本: {clean}")
                else:
                    txt_stable = 0
            else:
                txt_stable = 0

            # Muse accepted the click but never created a thread. Requiring a completed
            # Stop-button lifecycle, an idle page, no messages/attachments and /thread/new
            # avoids treating ordinary long video rendering as failure.
            if (expect == "video" and seen_stop and not has_stop
                    and path.rstrip("/").endswith("/thread/new")
                    and cur_cnt <= base_agent_cnt and len(atts) <= base_att_cnt
                    and elapsed >= 180.0 and time.time() - last_activity >= 90.0):
                # Stop 出现过 = 提示词已受理（可能已计费）：不换号重发
                raise MuseSubmittedError(
                    "Muse 未建立视频任务线程，页面在提交结束后持续无响应（已提前停止等待，可能已提交，未换号重发）")
        return None

    @staticmethod
    def _clean_model_text(text: str, limit: int = 240) -> str:
        clean = re.sub(r"\s+", " ", text or "").strip()
        return clean[:limit]

    @classmethod
    def _media_refusal_text(cls, text: str, expect: str) -> str:
        """Return only an explicit media refusal line, never an arbitrary page tail."""
        if not text:
            return ""
        patterns = [
            r"^(?:没生成成|视频没生成成功|视频未生成成功|视频生成失败)[^\n]{0,240}"
            r"(?:国家|地区|country|region)[^\n]{0,160}(?:不支持|不可用|无法|not available|unavailable|not supported)",
            r"^(?:could not|couldn't|cannot|can't|unable to)[^\n]{0,120}video[^\n]{0,160}"
            r"(?:country|region|not available|unavailable|not supported)",
        ] if expect == "video" else [
            r"^(?:图片没生成成功|图片未生成成功|图片生成失败|无法生成图片)",
            r"^(?:could not|couldn't|cannot|can't|unable to)[^\n]{0,120}image",
        ]
        for line in reversed((text or "").splitlines()):
            clean = cls._clean_model_text(line)
            if clean and any(re.search(pattern, clean, re.I) for pattern in patterns):
                return clean
        return ""

    # ---------------- 取字节 ----------------
    _EXTRACT_JS = r"""
    (async function(src, expect){
      try{
        var u = src;
        if(!u) return JSON.stringify({ok:false,err:'no-media-src'});
        var r = await fetch(u);
        if(!r.ok) return JSON.stringify({ok:false,err:'media-http-'+r.status});
        var b = await r.blob();
        var ab = await b.arrayBuffer();
        var bytes = new Uint8Array(ab);
        var s = '';
        for(var i=0; i<bytes.length; i+=65536){
          s += String.fromCharCode.apply(null, bytes.subarray(i, i+65536));
        }
        return JSON.stringify({ok:true, mime:b.type||'', size:b.size, url:u, b64:btoa(s)});
      }catch(e){
        return JSON.stringify({ok:false, err:String(e)});
      }
    })(%s, %s)
    """

    # ---------------- 文本 / 代码对话 ----------------
    _AGENT_TEXT_JS = (
        "(function(){"
        "var bs=[].slice.call(document.querySelectorAll("
        "'div[class*=\"hatch-chat-groupable-bubble\"]'));"
        "for(var i=bs.length-1;i>=0;i--){"
        "var cs=bs[i].className||'';"
        "if(/hatch-agent-bubble-bg/.test(cs))return bs[i].innerText||'';}"
        "return '';})()"
    )
    _USER_COUNT_JS = (
        "(function(){"
        "var bs=[].slice.call(document.querySelectorAll("
        "'div[class*=\"hatch-chat-groupable-bubble\"]'));"
        "var n=0;for(var i=0;i<bs.length;i++){"
        "if(/chat-user-bubble/.test(bs[i].className||''))n++;}"
        "return String(n);})()"
    )
    _AGENT_COUNT_JS = (
        "(function(){"
        "var bs=[].slice.call(document.querySelectorAll("
        "'div[class*=\"hatch-chat-groupable-bubble\"]'));"
        "var n=0;for(var i=0;i<bs.length;i++){"
        "if(/hatch-agent-bubble-bg/.test(bs[i].className||''))n++;}"
        "return String(n);})()"
    )
    _POLL_CHAT_JS = (
        "(function(){"
        "var els=[...document.querySelectorAll('*')].filter(function(e){"
        "var s=getComputedStyle(e);"
        "return (s.overflowY==='auto'||s.overflowY==='scroll')&&e.scrollHeight>e.clientHeight+100;});"
        "els.sort(function(a,b){return b.scrollHeight-a.scrollHeight;});"
        "if(els[0])els[0].scrollTop=els[0].scrollHeight;"
        "var scope=document.querySelector('main,[class*=\"chat-scroll\"],[class*=\"hatch-chat-scroll\"]')||document.body;"
        "var bs=[].slice.call(scope.querySelectorAll('div[class*=\"hatch-chat-groupable-bubble\"]'))"
        ".filter(function(b){return /hatch-agent-bubble-bg/.test(b.className||'');});"
        "var nonEmpty=bs.filter(function(b){return ((b.innerText||'').trim().length)>0;});"
        "var txt=nonEmpty.length?(nonEmpty[nonEmpty.length-1].innerText||'').trim():'';"
        "var all=(typeof window.__m2aBase==='number')?nonEmpty.slice(window.__m2aBase)"
        ".map(function(b){return (b.innerText||'').trim();}).join('\\n\\n'):null;"
        "var stop=!!(document.querySelector('[data-testid=\"hatch-composer-stop-button\"]')"
        "||document.querySelector('button[aria-label*=\"Stop\" i]')"
        "||document.querySelector('button[aria-label*=\"停止\"]'));"
        "return JSON.stringify({cnt:nonEmpty.length,total:bs.length,txt:txt,all:all,stop:stop});})()"
    )

    def _agent_text(self) -> str:
        """最后一个助手气泡的文本（取不到就返回空串）。"""
        try:
            return (self.page.js(self._AGENT_TEXT_JS) or "").strip()
        except Exception:  # noqa: BLE001
            return ""

    def _agent_count(self) -> int:
        try:
            return int(self.page.js(self._AGENT_COUNT_JS) or 0)
        except Exception:  # noqa: BLE001
            return 0

    def _user_count(self) -> int:
        try:
            return int(self.page.js(self._USER_COUNT_JS) or 0)
        except Exception:  # noqa: BLE001
            return 0

    _SITE_UI_TEXT_JS = (
        "(function(){if(!document.body)return '';"
        # 只留可见节点：隐藏提示（display:none / hidden / aria-hidden）不算站点在提示用户
        "var hid=[].slice.call(document.body.querySelectorAll('*')).filter(function(n){"
        "if(!n.firstChild)return false;"
        "if(n.hidden||n.getAttribute('aria-hidden')==='true')return true;"
        "var cs=getComputedStyle(n);return cs.display==='none'||cs.visibility==='hidden';});"
        "hid.forEach(function(n){n.setAttribute('data-m2a-hidden','1');});"
        "var c=document.body.cloneNode(true);"
        "hid.forEach(function(n){n.removeAttribute('data-m2a-hidden');});"
        "[].slice.call(c.querySelectorAll('[data-m2a-hidden],[class*=hatch-chat-groupable-bubble],[class*=chat-user-bubble],"
        # 侧栏 / 会话列表里的旧会话标题（由以前的提示词生成）也不算站点提示
        "[class*=\"group/msg\"],form,textarea,script,style,noscript,template,"
        "nav,aside,[role=navigation],[role=complementary]'))"
        ".forEach(function(n){n.remove();});"
        # 只取末尾 700 字（与原项目一致）：侧栏一般排在页面前部，nav/aside 没认出侧栏时也基本取不到它
        "return (c.textContent||'').slice(-700);})()"
    )

    def _site_ui_text(self) -> str:
        """页面上站点自身的 UI 文本（横幅、提示条），不含聊天气泡、输入框与脚本。"""
        try:
            return self.page.js(self._SITE_UI_TEXT_JS) or ""
        except Exception as exc:  # noqa: BLE001
            self._check_page_alive(exc)
            return ""

    def _poll_chat(self) -> tuple[int, str, bool]:
        """(助手气泡数, 文本, Stop 是否可见)。文本为本次发送后所有新助手气泡的拼接
        （见 chat_stream 设置的 window.__m2aBase）；没有设置时退化为最后一个气泡。"""
        try:
            raw = self.page.js(self._POLL_CHAT_JS)
            if raw:
                d = json.loads(raw)
                txt = d.get("all") if d.get("all") is not None else d.get("txt")
                return int(d.get("cnt") or 0), (txt or "").strip(), bool(d.get("stop"))
        except Exception as exc:  # noqa: BLE001
            self._check_page_alive(exc)
        return 0, "", False

    def chat_stream(self, cookies: dict, prompt: str, expires: dict | None = None,
                    timeout: int | None = None, account_id: str | None = None,
                    stop_event=None):
        """发一条消息，流式 yield 增量文本。"""
        timeout = int(timeout or getattr(self.cfg, "chat_timeout", 300))
        self.last_chat_text = None
        self.ensure_page(cookies, expires, account_id=account_id)
        self.reset_thread(for_chat=True)
        base_agent, base_text, _ = self._poll_chat()
        try:  # 之后的轮询只拼接本次发送后新出现的助手气泡（多气泡回复不丢后面的气泡）
            self.page.js(f"window.__m2aBase={int(base_agent)};1")
        except Exception:  # noqa: BLE001
            pass
        if self._send(prompt) not in ("clicked", "enter-sent"):
            # 与生图一致：提示词没发出去就立即失败（可切号重试），不空等 40 秒首字超时
            raise MuseGenerationError("提示词发送未确认（找不到发送按钮或输入框未清空）")

        t_sent = time.time()
        deadline = t_sent + timeout
        first_token_deadline = min(deadline, t_sent + 40.0)
        emitted, last, stable = "", None, 0   # emitted：已发给客户端的全部文本；last：上一次的页面文本
        got_first = False
        seen_stop = False  # Stop 出现过 = 助手已受理（已计费）
        next_quota_check = t_sent + 1.0

        # 1. 等待助手生成并开始吐字（高频 60ms 采样，捕获到首批增量文字瞬间 yield 出去）
        while time.time() < first_token_deadline:
            if stop_event is not None and stop_event.is_set():
                return
            time.sleep(0.06)
            try:
                cnt, cur, has_stop = self._poll_chat()
                if has_stop:
                    # Stop 可见说明助手已受理、正在思考：首字可能远超 40 秒，等到总时限
                    first_token_deadline = deadline
                    seen_stop = True
                if time.time() >= next_quota_check:
                    # 与生图一致：站点提示额度耗尽就立即失败（上层暂停该账号调度），不空等首字超时
                    next_quota_check = time.time() + 1.5
                    if _QUOTA_RE.search(self._site_ui_text()):
                        raise MuseQuotaError("账号额度不足")
                if not cur or (cnt <= base_agent and cur == base_text):
                    if time.time() - t_sent > 14.0 and not has_stop:
                        # 只看站点自身 UI（去掉聊天气泡与输入框）：用户贴的日志里有 "Connecting..." 不算
                        tail = self._site_ui_text()
                        if "Still sending" in tail or "Connecting..." in tail:
                            # 提示词已确认发出：网关回执慢时消息可能已送达并计费，与生图一致不换号重发
                            raise MuseSubmittedError("云端 VM 连接超时 (Still sending)，可能已提交，未换号重发")
                    continue
            except (MusePageGoneError, CDPConnectionError) as exc:
                # 提示词已确认发出（_send 已确认）后页面断开：没看到 Stop 也可能已送达计费，不换号重发
                raise MuseSubmittedError(
                    f"提示词已发出，但等待回复时浏览器页面断开（{type(exc).__name__}），可能已提交，未换号重发") from exc

            delta = _stream_delta(emitted, "", cur)
            if delta:
                emitted += delta
                yield delta
                last = cur
                got_first = True
                break

        if not got_first:
            if seen_stop:
                # 助手受理过（Stop 出现过）却一直没出字：提示词已计费，不换号重发
                raise MuseSubmittedError("等待助手首字响应超时（助手已受理但未输出）")
            # 提示词已确认发出（_send 已确认）：没看到 Stop 也可能已送达计费，不换号重发
            raise MuseSubmittedError("等待助手首字响应超时（提示词已发出，可能已提交，未换号重发）")

        # 2. 持续捕获增量文本
        while time.time() < deadline:
            if stop_event is not None and stop_event.is_set():
                return
            time.sleep(0.06)
            cnt, cur, has_stop = self._poll_chat()
            if not cur:
                continue

            if cur != last:
                delta = _stream_delta(emitted, last or "", cur)
                if delta:
                    emitted += delta
                    yield delta
                last, stable = cur, 0
            else:
                stable += 1
                # Stop 按钮消失说明前端生成彻底结束，连续 3 次（约 0.18s）无新文本即正常退出
                if not has_stop and stable >= 3:
                    # 页面最终文本：非流式接口以它为准。增量拼接在气泡出现非前缀变化（占位文字被替换、
                    # 首字后重新渲染）时会与它不一致，已发出的流式内容收不回来，只记日志
                    self.last_chat_text = cur
                    if emitted != cur:
                        log.warning("流式增量与页面最终回复不一致（气泡内容出现过非前缀变化），"
                                    "非流式结果以最终文本为准")
                    return
                # Stop 按钮仍在说明助手还在生成（思考、执行工具、网络抖动），文本停顿再久也不算结束：
                # 继续等到总时限，超时按错误上报，不能把截断的回复当成功返回

        raise MuseSubmittedError("等待助手回复超时（助手仍在生成，回复未完成）")

    def chat(self, cookies: dict, prompt: str, expires: dict | None = None,
             timeout: int | None = None, account_id: str | None = None) -> str:
        """发一条消息，返回完整回复文本（非流式）。"""
        out = ""
        for chunk in self.chat_stream(cookies, prompt, expires, timeout, account_id=account_id):
            out += chunk
        return self.last_chat_text or out

    def extract_bytes(self, src: str, expect: str = "image", retries: int = 4,
                      deadline: float | None = None):
        """deadline（time.monotonic）：取回总时限；到期不再重试，每次 CDP 调用的超时也不超过剩余时间。"""
        last = "未知"
        for _ in range(retries):
            budget = 600.0 if deadline is None else deadline - time.monotonic()
            if budget <= 1:
                break
            raw = self.page.js(self._EXTRACT_JS % (json.dumps(src), json.dumps(expect)),
                               await_promise=True, timeout=min(600.0, budget))
            try:
                info = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:  # noqa: BLE001
                info = {"ok": False, "err": f"解析失败 {str(raw)[:150]}"}
            if info.get("ok"):
                return base64.b64decode(info["b64"]), info.get("mime", ""), info.get("url", "")
            last = info.get("err", "未知")
            time.sleep(2)
        raise MuseGenerationError(f"未能取回生成结果: {last}")

    # ---------------- 下载兜底 ----------------
    def _download_fallback(self, src: str, timeout: int = 180) -> str | None:
        dd = self.current_download_dir or self.cfg.download_dir
        before = set(os.listdir(dd))
        # ponytail: fail closed when the selected result has no local download;
        # never click an unrelated/global button that can return the upload.
        clicked = self.page.js("""(function(src){
            var media=[...document.querySelectorAll('img,video')]
                .find(m=>(m.currentSrc||m.src||'')===src);
            var node=media && media.closest('[data-testid^="hatch-chat-attachment-presentation-"]');
            if(!node) return 'none';
            node=node.closest('[class*="group/widget-presentation"]')||node;
            var b=[...node.querySelectorAll('button,[role=button]')]
                .find(x=>/下载|保存|download/i.test(x.getAttribute('aria-label')||''));
            if(!b) return 'none';
            b.click();return 'ok';
        })(%s)""" % json.dumps(src))
        if clicked == "none":
            return None
        deadline = time.time() + timeout
        while time.time() < deadline:
            time.sleep(1.5)
            new = [f for f in (set(os.listdir(dd)) - before)
                   if not f.endswith(".crdownload")]
            if new:
                p = os.path.join(dd, max(
                    new, key=lambda f: os.path.getmtime(os.path.join(dd, f))))
                if os.path.getsize(p) > 0:
                    return p
        return None

    def _normalize_image(self, img: str) -> tuple[str, str]:
        """Normalize one image and enforce source, MIME and size policy."""
        if not img:
            return "", "image/png"
        img = str(img).strip()

        def checked(data: bytes, mime: str) -> tuple[str, str]:
            mime = (mime or "image/png").split(";", 1)[0].strip().lower()
            if not mime.startswith("image/"):
                raise MuseGenerationError(f"参考文件不是图片（{mime or 'unknown'}）")
            if len(data) > MAX_REFERENCE_IMAGE_BYTES:
                raise MuseGenerationError("单张参考图超过 10 MiB 限制")
            return base64.b64encode(data).decode("ascii"), mime

        if img.startswith("data:"):
            head, sep, payload = img.partition(",")
            if not sep:
                raise MuseGenerationError("参考图 Data URL 格式无效")
            mime = head[5:].split(";", 1)[0] or "image/png"
            try:
                data = (base64.b64decode(payload.strip(), validate=True)
                        if ";base64" in head.lower()
                        else urllib.parse.unquote_to_bytes(payload))
            except (binascii.Error, ValueError) as exc:
                raise MuseGenerationError("参考图 Data URL 编码无效") from exc
            return checked(data, mime)

        if img.startswith(("http://", "https://")):
            try:
                req = urllib.request.Request(img, headers={"User-Agent": "Mozilla/5.0"})
                response = (urllib.request.urlopen(req, timeout=20)
                            if self.cfg.ref_image_private_fetch
                            else _open_public_ref_image(img))
                with response as resp:
                    mime = resp.headers.get_content_type() or "image/png"
                    data = _read_with_deadline(resp, MAX_REFERENCE_IMAGE_BYTES + 1, REF_DOWNLOAD_SECONDS)
                if not mime.lower().startswith("image/"):
                    # 对象存储（S3 / OSS）常以 application/octet-stream 返回图片：以文件头为准
                    mime = _sniff_image_mime(data) or mime
                return checked(data, mime)
            except (RefImageSafetyError, MuseGenerationError):
                raise
            except Exception as exc:
                raise MuseGenerationError(
                    f"下载远程参考图失败: {type(exc).__name__}") from exc

        # Raw Base64 contains no server-side path and remains safe in restricted mode.
        # 裸 Base64 没有声明类型：按文件头识别，认不出是图片就拒绝（任意字母串都是合法 Base64）
        try:
            raw = base64.b64decode(img, validate=True)
        except (binascii.Error, ValueError):
            raw = None
        if raw is not None:
            sniffed = _sniff_image_mime(raw)
            if sniffed:
                return checked(raw, sniffed)
            if not os.path.isfile(img):
                raise MuseGenerationError("参考图 Base64 解码后不是可识别的图片（PNG/JPEG/WebP/GIF）")

        if not self.cfg.ref_image_local_files:
            raise RefImageSafetyError(
                "参考图本地路径被拒绝；如确需读取服务器文件，请显式设置 "
                "MUSE2API_REF_IMAGE_LOCAL_FILES=1")
        if os.path.isfile(img):
            try:
                if os.path.getsize(img) > MAX_REFERENCE_IMAGE_BYTES:
                    raise MuseGenerationError("单张参考图超过 10 MiB 限制")
                with open(img, "rb") as f:
                    return checked(f.read(MAX_REFERENCE_IMAGE_BYTES + 1),
                                   mimetypes.guess_type(img)[0] or "image/png")
            except MuseGenerationError:
                raise
            except Exception as exc:
                raise MuseGenerationError(
                    f"读取本地参考图失败: {type(exc).__name__}") from exc
        raise MuseGenerationError("参考图必须是 URL、Data URL、本地图片或有效 Base64")

    def _clear_attachments(self):
        """清除聊天输入框里遗留的附件缩略图（兼容中/英文 UI）。"""
        try:
            self.page.js(
                "(function(){"
                # 只在输入框所在的 composer 表单里找、只认「移除附件」：页面别处（会话列表、侧栏）
                # 的「删除 / Remove」按钮绝不能点，可能误删账号里的历史会话
                "var ta=document.querySelector('form textarea')||document.querySelector('textarea');"
                "var form=ta?ta.closest('form'):null;"
                "if(!form)return 0;"
                "var btns=Array.from(form.querySelectorAll('button[aria-label]')).filter(function(b){"
                "var al=b.getAttribute('aria-label')||'';"
                "return /remove\\s*(attachment|image|file)|移除附件|删除附件|移除图片|删除图片/i.test(al);"
                "});"
                "btns.forEach(function(b){b.click();});"
                "Array.from(form.querySelectorAll('input[type=\"file\"]')).forEach(function(inp){inp.value='';});"
                "return btns.length;"
                "})()")
            time.sleep(0.3)
        except Exception:
            pass

    def _attach_images(self, image_data: list[str]):
        """逐张附加参考图，并确认全部进入 composer。"""
        if len(image_data) > MAX_REFERENCE_IMAGES:
            raise MuseGenerationError(f"参考图最多 {MAX_REFERENCE_IMAGES} 张")
        images = []
        total_bytes = 0
        for index, value in enumerate(image_data):
            if not value:
                continue
            b64, mime = self._normalize_image(value)
            if not b64:
                raise MuseGenerationError(f"第 {index + 1} 张参考图读取失败，已停止生成")
            try:
                total_bytes += len(base64.b64decode(b64, validate=True))
            except (binascii.Error, ValueError) as exc:
                raise MuseGenerationError(f"第 {index + 1} 张参考图编码无效") from exc
            if total_bytes > MAX_REFERENCE_TOTAL_BYTES:
                raise MuseGenerationError("参考图总大小超过 24 MiB 限制")
            images.append({"b64": b64, "mime": mime, "index": index + 1})
        if not images:
            return

        self._clear_attachments()

        _INJECT_JS = """
        (async function(images) {
            try {
                // 现网 composer 里可能有多个 file input；优先取位于 composer 内的那个
                var inputs = Array.from(document.querySelectorAll('input[type="file"]'));
                if (!inputs.length) return JSON.stringify({ok: false, err: 'no-file-input'});
                var ov = document.querySelector('[data-testid="hatch-composer-placeholder-overlay"]');
                var composer = ov ? ov.closest('form') : null;
                var input = null;
                if (composer) {
                    input = inputs.find(function(x){ return composer.contains(x); }) || null;
                }
                if (!input) {
                    // 退而求其次：取不在对话气泡里的那个 input
                    input = inputs.find(function(x){
                        return !x.closest('[class*=chat-user-bubble], [class*="group/msg"]');
                    }) || inputs[0];
                }
                // 有些版本 accept 为空，补上 image/* 让页面接受本次 File
                if (!input.getAttribute('accept')) input.setAttribute('accept', 'image/*');
                for (var image of images) {
                    var byteChars = atob(image.b64);
                    var byteNumbers = new Array(byteChars.length);
                    for (var i = 0; i < byteChars.length; i++) {
                        byteNumbers[i] = byteChars.charCodeAt(i);
                    }
                    var mime = image.mime || 'image/png';
                    var byteArray = new Uint8Array(byteNumbers);
                    var blob = new Blob([byteArray], {type: mime});
                    var ext = mime.split('/')[1] || 'png';
                    if (ext === 'jpeg') ext = 'jpg';
                    var dt = new DataTransfer();
                    dt.items.add(new File(
                        [blob], 'reference_image_' + image.index + '.' + ext, {type: mime}));
                    input.files = dt.files;
                    input.dispatchEvent(new Event('change', {bubbles: true}));
                    input.dispatchEvent(new Event('input', {bubbles: true}));
                    await new Promise(function(resolve){ setTimeout(resolve, 350); });
                }
                return JSON.stringify({ok: true, count: images.length});
            } catch(e) {
                return JSON.stringify({ok: false, err: String(e)});
            }
        })(%s)
        """
        try:
            raw_res = self.page.js(
                _INJECT_JS % json.dumps(images), await_promise=True,
                timeout=max(15, len(images) * 3))
            res_obj = json.loads(raw_res) if isinstance(raw_res, str) else raw_res
            if not res_obj.get("ok") or res_obj.get("count") != len(images):
                raise MuseGenerationError("附加参考图失败，已停止生成")
        except Exception as e:
            raise MuseGenerationError("附加参考图失败，已停止生成") from e

        # 等待输入框附件确认；历史图片不能证明本次上传成功。
        # 现网 UI 已切中文：确认按钮 aria-label = "附加文件"/"移除附件"；
        # 同时兼容英文（Remove attachment / Attach file），并以 composer 内缩略图兜底。
        deadline = time.time() + 15.0
        attached_count = 0
        while time.time() < deadline:
            attached_count = self.page.js(
                """(function(){
                var ov = document.querySelector('[data-testid="hatch-composer-placeholder-overlay"]');
                var composer = ov ? (ov.closest('form') || ov.parentElement.parentElement.parentElement) : null;
                if (!composer) return 0;
                var btns = Array.from(composer.querySelectorAll('button[aria-label]'));
                var removeCount = btns.filter(function(b){
                    return /remove\\s*attachment|移除附件|删除附件/i.test(b.getAttribute('aria-label') || '');
                }).length;
                if (removeCount) return removeCount;
                return composer.querySelectorAll('img, video').length;
                })()"""
            )
            if int(attached_count or 0) >= len(images):
                break
            time.sleep(0.3)
        else:
            raise MuseGenerationError(
                f"参考图上传未确认（{int(attached_count or 0)}/{len(images)}），已停止生成")
        time.sleep(0.5)

    def _attach_image(self, image_data: str):
        """Backward-compatible single-reference wrapper."""
        self._attach_images([image_data])
    # ---------------- 主流程 ----------------
    def generate(self, cookies: dict, prompt: str, expect: str = "image",
                 timeout: int = 240, expires: dict | None = None, account_id: str | None = None,
                 on_progress=None, reference_image: str | None = None,
                 reference_images: list[str] | None = None,
                 stop_event=None) -> dict:
        deadline = time.monotonic() + timeout
        self.ensure_page(cookies, expires, account_id=account_id)
        self.reset_thread(for_chat=False)
        self._scroll_bottom()
        refs = list(reference_images or ([] if not reference_image else [reference_image]))
        if refs:
            self._attach_images(refs)
        else:
            self._clear_attachments()
        atts_before = self.attachments()
        baseline_sources = {a.get(k) for a in atts_before for k in ("src", "vSrc", "iSrc") if a.get(k)}
        base = atts_before[-1] if atts_before else {}
        baseline_src = base.get("src") or ""
        base_agent_cnt = self._agent_count()
        if time.monotonic() >= deadline:
            raise MuseGenerationError("任务准备超时，已停止发送")
        if self._send(prompt) not in ("clicked", "enter-sent"):
            raise MuseGenerationError("提示词发送未确认，已停止生成")
        try:
            att = self._wait_attachment(
                baseline_src, max(0, deadline - time.monotonic()), expect, on_progress=on_progress,
                base_agent_cnt=base_agent_cnt, base_att_cnt=len(atts_before),
                stop_event=stop_event, baseline_sources=baseline_sources
            )
        except (MusePageGoneError, CDPConnectionError) as exc:
            # 提示词已确认提交（可能已计费）后页面断开：换号重发会重复扣额度，按已提交处理
            raise MuseSubmittedError(f"提示词已提交，但等待结果时浏览器页面断开（{type(exc).__name__}）") from exc
        if not att:
            self._debug_dump("no-attachment")
            raise MuseGenerationError("等待生成超时，未出现新的生成结果")

        try:
            os.makedirs(self.cfg.media_dir, exist_ok=True)
        except OSError as exc:
            raise MuseSubmittedError(f"已生成但无法创建媒体目录（{type(exc).__name__}: {exc}）") from exc
        data = mime = url = None
        selected_src = (att.get("vSrc") if expect == "video" else None) or att.get("src") or ""
        # 成品出现后的取回时限：剩余时间，至少留 FETCH_GRACE_SECONDS（成品已生成，值得取回），
        # 但不再像以前那样可以拖上几十分钟
        fetch_deadline = max(deadline, time.monotonic() + FETCH_GRACE_SECONDS)
        try:
            data, mime, url = self.extract_bytes(selected_src, expect=expect, deadline=fetch_deadline)
        except Exception:  # noqa: BLE001
            self._debug_dump("extract-fail")

        if data:
            ext = self._pick_ext(mime, url, expect)
            kind = "video" if ext in (".mp4", ".webm", ".mov") else "image"
            if expect == "video" and kind != "video":  # 先判类型再落盘，不在媒体库留下孤儿文件
                raise MuseSubmittedError(f"期望生成视频，但提取成品类型为图片 ({ext})，拒绝以图片冒充视频交付")
            if expect == "image" and kind != "image":
                raise MuseSubmittedError(f"期望生成图片，但提取成品类型为视频 ({ext})，拒绝交付")
            name = f"{uuid.uuid4().hex}{ext}"
            dst = os.path.join(self.cfg.media_dir, name)
            try:
                with open(dst, "wb") as f:
                    f.write(data)
            except OSError as exc:
                # 成品已生成（已计费）：本地落盘失败（如磁盘满）不能让上层换号重新生成
                raise MuseSubmittedError(f"已生成但保存文件失败（{type(exc).__name__}: {exc}）") from exc
            return {"path": dst, "filename": name, "size": len(data), "ext": ext, "mime": mime,
                    "kind": kind,
                    "via": "blob", "attachment": att.get("tid"),
                    "w": att.get("w"), "h": att.get("h")}

        dl_budget = int(fetch_deadline - time.monotonic())
        try:
            path = self._download_fallback(selected_src, timeout=dl_budget) if dl_budget > 1 else None
        except Exception as exc:  # noqa: BLE001
            # 成品已经生成：下载兜底的任何异常（含页面断开的 CDPConnectionError）都算「已计费、取回失败」，
            # 不能让上层当成可重试的错误换账号重新生成
            raise MuseSubmittedError(f"已生成但未能取回文件（{type(exc).__name__}）") from exc
        if not path:
            raise MuseSubmittedError("已生成但未能取回文件")
        ext = os.path.splitext(path)[1].lower() or ".bin"
        kind = "video" if ext in (".mp4", ".webm", ".mov") else "image"
        if expect == "video" and kind != "video":  # 不搬进媒体库；下载目录可能是共享目录，顺手删掉
            try:
                os.remove(path)
            except OSError:
                pass
            raise MuseSubmittedError(f"期望生成视频，但下载成品类型为图片 ({ext})，拒绝以图片冒充视频交付")
        name = f"{uuid.uuid4().hex}{ext}"
        dst = os.path.join(self.cfg.media_dir, name)
        try:
            shutil.move(path, dst)
            size = os.path.getsize(dst)
        except OSError as exc:
            raise MuseSubmittedError(f"已生成但保存文件失败（{type(exc).__name__}: {exc}）") from exc
        return {"path": dst, "filename": name, "size": size, "ext": ext, "mime": "",
                "kind": kind,
                "via": "download", "attachment": att.get("tid"),
                "w": att.get("w"), "h": att.get("h")}

    @staticmethod
    def _pick_ext(mime: str, url: str, expect: str) -> str:
        m = (mime or "").lower()
        for key, ext in (("mp4", ".mp4"), ("webm", ".webm"), ("png", ".png"),
                         ("jpeg", ".jpg"), ("jpg", ".jpg"), ("webp", ".webp"),
                         ("gif", ".gif")):
            if key in m:
                return ext
        for e in (".mp4", ".webm", ".png", ".jpg", ".webp"):
            if e in (url or "").lower():
                return e
        return ".mp4" if expect == "video" else ".png"

    def _debug_dump(self, tag: str):
        try:
            info = self.page.js(
                "JSON.stringify({atts:[...document.querySelectorAll('" + ATT_SEL + "')]"
                ".map(function(a){var m=a.querySelector('img,video');return {"
                "tid:a.getAttribute('data-testid'),"
                "src:m?(m.currentSrc||m.src||'').slice(0,60):''};}),"
                "buttons:[...document.querySelectorAll('button,[role=button]')]"
                ".filter(b=>b.offsetParent!==null)"
                ".map(b=>b.getAttribute('aria-label')||b.innerText.trim().slice(0,20))"
                ".filter(Boolean).slice(-40),"
                "tail:document.body.innerText.slice(-500)})")
            with open(os.path.join(self.cfg.data_dir, f"debug-{tag}.json"), "w",
                      encoding="utf-8") as f:
                f.write(str(info))
        except Exception:  # noqa: BLE001
            pass
