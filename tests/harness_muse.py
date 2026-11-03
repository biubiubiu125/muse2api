"""离线仿真 muse.ai 测试脚手架。

不访问真实 muse.ai、不消耗额度。后续测试用 ``import harness_muse`` 复用
（测试按 ``python3 tests/xxx.py`` 运行时，tests/ 目录自动在 sys.path 上）。

- FakeMuse：本地自签 HTTPS 仿真站，复刻 muse2api 依赖的页面 DOM 与接口契约：
  textarea、aria-label=Send 发送按钮、data-testid=hatch-composer-stop-button 停止按钮、
  hatch-chat-groupable-bubble / hatch-agent-bubble-bg 气泡、
  hatch-chat-attachment-presentation-{image,video} 附件容器、
  hatch-dock-more → 菜单项 Settings → role=dialog 内的 Usage 文本；
  GET /api/session（Set-Cookie 续签、可切换为 401 失效账号）、POST /api/hatch/vm/wake、
  /media/img.png、/media/vid.mp4。
- make_chromium_wrapper：生成 Chromium 包装脚本，用 --host-resolver-rules 把 muse.ai
  映射到 FakeMuse 端口，并加 --ignore-certificate-errors，被测代码按真实域名访问仿真站。
- load_app：在 $HOME 下的独立临时目录里重新加载一份 app.py（重新读取环境变量），
  Chromium profile、下载目录、data 目录都不落进仓库。
- 另外再导出 harness_asgi 的进程内 ASGI 夹具：LiveServer / serve_app（后台线程里的
  uvicorn）、StreamClient（原始 socket 流式客户端，可中途 abort）、
  snapshot / wait_released / worker_stacks / PoolProbe（槽位、账号占用、浏览器上下文、
  worker 线程栈观测）、fake_engine / SlowChat（不开真实浏览器的轻量假引擎）。
- 以及 harness_net 的网络与指纹夹具：HttpConnectProxy / Socks5Proxy / StunServer /
  TcpEcho（本地代理、STUN 与回显靶子）、primary_ipv4 / stun_binding_request /
  probe_webrtc（WebRTC ICE 收集）、FINGERPRINT_JS / probe_fingerprint / probe_page（指纹探针）、
  cdp_call_cost / close_page_target / kill_browser / forget_browser（CDP 调用墙钟与 CPU
  耗时探针；杀浏览器后的安全收尾）。

典型用法::

    import harness_muse as hm

    with hm.FakeMuse() as muse:
        wrapper = hm.make_chromium_wrapper(muse.port, muse.workdir)
        app = hm.load_app({"MUSE2API_CHROMIUM": wrapper}, "demo")
        try:
            ...  # TestClient(app.app)、app.engine、app.store
        finally:
            hm.close_app(app)  # 先停浏览器、删 app 临时目录，再退出 with 停仿真站

缺少 openssl 或 Chromium 时抛 unittest.SkipTest；脚本入口用 run_main(...) 把它
转换为打印 ``SKIP: 原因`` 并以 0 退出。
"""
from __future__ import annotations

import base64
import http.server
import importlib.util
import json
import os
import re
import shlex
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# 仿真站返回的媒体字节；生图/视频用例据此比对落盘结果。
PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==")
MP4 = b"\x00\x00\x00\x18ftypmp42" + b"0" * 2000

DEFAULT_KEY = "harness-key"
WEEKLY_USED_PCT = 7
SESSION_COOKIE = "hatch_vml=RENEWED-{n}; Path=/; Secure; HttpOnly; SameSite=Lax; Max-Age=172800"

_WRAPPER_MARK = "# harness_muse chromium wrapper; real chromium: "
_PURGE_MODULES = ("config", "store", "engine", "cdp", "proxyrelay")
_ENV_ORIGINAL: dict[str, str | None] = {}


# ------------------------- 通用工具 -------------------------
def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def home_tempdir(prefix: str) -> str:
    """在 $HOME 下建临时目录（snap 版 Chromium 只能读写 $HOME 下的非隐藏路径）。"""
    return tempfile.mkdtemp(prefix=prefix, dir=str(Path.home()))


def rmtree(path) -> None:
    """删除目录；浏览器子进程退出稍慢时可能边删边写，重试几次。"""
    if not path:
        return
    for _ in range(5):
        shutil.rmtree(path, ignore_errors=True)
        if not os.path.exists(path):
            return
        time.sleep(0.4)


def parse_cookie_header(header: str) -> list[tuple[str, str]]:
    """按出现顺序拆 Cookie 请求头，保留同名重复项（用于断言每个 cookie 只发一份）。"""
    out = []
    for part in (header or "").split(";"):
        part = part.strip()
        if part:
            name, _, value = part.partition("=")
            out.append((name.strip(), value.strip()))
    return out


def run_main(*tests) -> None:
    """脚本入口：依次运行测试函数；遇到 unittest.SkipTest 打印 SKIP 并以 0 退出。"""
    try:
        for test in tests:
            test()
    except unittest.SkipTest as exc:
        print(f"SKIP: {exc}")
        sys.exit(0)


# ------------------------- 仿真页面 -------------------------
# DOM 契约与 engine.py 的选择器一一对应；__FAKE_MUSE_CFG__ 在每次出页时替换为当前开关。
_UI = r"""<!doctype html><html><head><meta charset="utf-8"><title>Muse</title></head><body>
<div data-hatch-shell-hydration-state="hydrated"><main id="scroll"></main>
<button data-testid="hatch-dock-more" aria-label="Settings" id="more">More</button>
<form><div data-testid="hatch-composer-placeholder-overlay"></div><textarea id="ta"></textarea>
<input type="file" accept="image/*"><button type="button" aria-label="Send" id="send">Send</button></form></div>
<script>
const CFG = __FAKE_MUSE_CFG__;
const ta = document.getElementById('ta'), main = document.getElementById('scroll');
document.getElementById('more').addEventListener('click', () => {
  const m = document.createElement('div'); m.setAttribute('role', 'menu');
  m.innerHTML = '<div role="menuitem"><span>Settings</span><span>Ctrl+,</span></div>';
  document.body.appendChild(m);
  m.querySelector('[role=menuitem]').addEventListener('click', () => {
    const d = document.createElement('div'); d.setAttribute('role', 'dialog');
    d.innerText = CFG.usage;
    document.body.appendChild(d);
  });
});
document.getElementById('send').addEventListener('click', () => {
  const text = ta.value; if (!text) return; ta.value = '';
  const u = document.createElement('div'); u.className = 'hatch-chat-groupable-bubble chat-user-bubble';
  u.innerText = text; main.appendChild(u);
  const stop = document.createElement('button');
  stop.setAttribute('data-testid', 'hatch-composer-stop-button'); stop.setAttribute('aria-label', 'Stop');
  document.querySelector('form').appendChild(stop);
  const a = document.createElement('div'); a.className = 'hatch-chat-groupable-bubble hatch-agent-bubble-bg';
  main.appendChild(a);
  const wantsVideo = /视频|video/i.test(text), wantsImage = !wantsVideo && /文生图|生图|image/i.test(text);
  const slow = CFG.slow || (!!CFG.slowMarker && text.indexOf(CFG.slowMarker) >= 0);
  let reply = 'ECHO[' + text.slice(-12) + '] cookie_visible_to_js=' + (document.cookie.indexOf('hatch_sess=') >= 0);
  if (slow) reply += ' ' + 'S'.repeat(CFG.slowChars);
  const step = slow ? 1 : 4;
  let i = 0;
  const tick = setInterval(() => {
    i += step; a.innerText = reply.slice(0, i);
    if (i >= reply.length) {
      clearInterval(tick);
      if (wantsVideo) {
        const w = document.createElement('div'); w.setAttribute('data-testid', 'hatch-chat-attachment-presentation-video');
        w.innerHTML = '<video src="/media/vid.mp4"></video>'; a.appendChild(w);
      } else if (wantsImage) {
        const w = document.createElement('div'); w.setAttribute('data-testid', 'hatch-chat-attachment-presentation-image');
        w.innerHTML = '<img src="/media/img.png">'; a.appendChild(w);
      }
      setTimeout(() => stop.remove(), 400);
    }
  }, slow ? CFG.slowIntervalMs : 60);
});
</script></body></html>"""


class _TLSServer(http.server.ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64

    def __init__(self, addr, handler, ssl_ctx: ssl.SSLContext, fake: "FakeMuse"):
        self.ssl_ctx = ssl_ctx
        self.fake = fake
        self._conns: set = set()
        self._conns_lock = threading.Lock()
        super().__init__(addr, handler)

    def finish_request(self, request, client_address):
        # TLS 握手放在每个连接自己的线程里，握不上手的连接不会卡住 accept 循环。
        request.settimeout(10)
        try:
            tls = self.ssl_ctx.wrap_socket(request, server_side=True)
        except OSError:
            return
        with self._conns_lock:
            self._conns.add(tls)
        try:
            self.RequestHandlerClass(tls, client_address, self)
        finally:
            with self._conns_lock:
                self._conns.discard(tls)
            try:
                tls.close()
            except OSError:
                pass

    def handle_error(self, request, client_address):
        if isinstance(sys.exc_info()[1], OSError):
            return  # 浏览器断开/复位连接属于正常现象，不刷屏
        super().handle_error(request, client_address)

    def close_connections(self):
        with self._conns_lock:
            conns = list(self._conns)
        for conn in conns:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "FakeMuse/1.0"
    timeout = 30  # 空闲 keep-alive 连接 30s 后关闭

    def do_GET(self):
        self.server.fake._serve(self, "GET")

    def do_POST(self):
        self.server.fake._serve(self, "POST")

    def log_message(self, format, *args):  # noqa: A002
        pass


class FakeMuse:
    """本地仿真 muse.ai（自签 HTTPS，绑定 127.0.0.1 的随机端口）。

    以下开关是实例属性，运行中可直接修改：接口类开关每次请求读取，
    页面类开关（slow_*、weekly_used_pct）在每次加载页面时读取。

    - slow_stream：True 时所有回复逐字慢速输出（末尾追加 slow_chars 个 'S'，
      每 slow_interval_ms 毫秒 1 个字符）。
    - slow_marker：提示词含该子串时该条回复慢速输出（与参考实现一致，默认 "slow"；None 关闭）。
    - dead_marker：请求 Cookie 含该子串时 /api/session 返回 401（默认 "hatch_sess=dead"；None 关闭）。
    - set_cookie_on_session：/api/session 返回 200 时是否下发 Set-Cookie 续签。
    - session_cookie：Set-Cookie 模板（字符串或字符串列表），{n} 为第 n 次成功续签。
    - vm_state：/api/session 返回的 vm_state（默认 SUSPENDED）。
    - wake_sets_state：非 None 时，POST /api/hatch/vm/wake 把 vm_state 改为该值。
    - weekly_used_pct：Settings → Usage 面板里的周额度已用百分比（默认 7）。

    观测：``log`` 按到达顺序记录每个请求（method、path、query、cookie、user_agent、
    accept_language、sec_ch_ua、sec_fetch_site、headers、time、status，POST 另有 body）；
    ``session_count`` 为成功续签次数，``wake_count`` 为唤醒请求次数。
    """

    def __init__(self, *, slow_stream: bool = False, slow_marker: str | None = "slow",
                 slow_chars: int = 300, slow_interval_ms: int = 50,
                 dead_marker: str | None = "hatch_sess=dead",
                 set_cookie_on_session: bool = True, session_cookie=SESSION_COOKIE,
                 vm_state: str = "SUSPENDED", wake_sets_state: str | None = None,
                 weekly_used_pct: int = WEEKLY_USED_PCT):
        self.slow_stream = slow_stream
        self.slow_marker = slow_marker
        self.slow_chars = slow_chars
        self.slow_interval_ms = slow_interval_ms
        self.dead_marker = dead_marker
        self.set_cookie_on_session = set_cookie_on_session
        self.session_cookie = session_cookie
        self.vm_state = vm_state
        self.wake_sets_state = wake_sets_state
        self.weekly_used_pct = weekly_used_pct
        self.port: int | None = None
        self.workdir: str | None = None
        self.log: list[dict] = []
        self.session_count = 0
        self.wake_count = 0
        self._lock = threading.Lock()
        self._server: _TLSServer | None = None
        self._thread: threading.Thread | None = None

    # ---------- 生命周期 ----------
    def start(self) -> "FakeMuse":
        if self._server:
            return self
        openssl = shutil.which("openssl")
        if not openssl:
            raise unittest.SkipTest("openssl not found; FakeMuse needs it to create a self-signed certificate")
        self.workdir = home_tempdir("m2a-fakemuse-")
        try:
            cert = os.path.join(self.workdir, "cert.pem")
            key = os.path.join(self.workdir, "key.pem")
            r = subprocess.run(
                [openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key,
                 "-out", cert, "-days", "2", "-subj", "/CN=muse.ai",
                 "-addext", "subjectAltName=DNS:muse.ai,DNS:www.muse.ai"],
                capture_output=True, text=True, timeout=60)
            if r.returncode != 0:
                raise RuntimeError("openssl failed to create the certificate: " + (r.stderr or r.stdout)[-400:])
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx.load_cert_chain(cert, key)
            self._server = _TLSServer(("127.0.0.1", 0), _Handler, ctx, self)
        except BaseException:
            rmtree(self.workdir)
            self.workdir = None
            raise
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever, kwargs={"poll_interval": 0.2},
                                        name=f"FakeMuse-{self.port}", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        srv, self._server = self._server, None
        if srv:
            srv.shutdown()
            srv.server_close()
            srv.close_connections()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        if self.workdir:
            rmtree(self.workdir)
            self.workdir = None

    def __enter__(self) -> "FakeMuse":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    # ---------- 观测 ----------
    def requests_to(self, path: str, method: str | None = None) -> list[dict]:
        with self._lock:
            return [e for e in self.log if e["path"] == path and (method is None or e["method"] == method)]

    def clear_log(self) -> None:
        with self._lock:
            self.log.clear()

    def expected_reply(self, prompt: str, cookie_visible: bool = True, slow: bool | None = None) -> str:
        """仿真页面对 prompt（页面输入框收到的原文）给出的完整回复。"""
        if slow is None:
            slow = bool(self.slow_stream) or bool(self.slow_marker and self.slow_marker in prompt)
        reply = f"ECHO[{prompt[-12:]}] cookie_visible_to_js={'true' if cookie_visible else 'false'}"
        return reply + (" " + "S" * int(self.slow_chars) if slow else "")

    def usage_text(self) -> str:
        return ("General\nUsage\nFree plan\nWeekly limit resets on Nov 3\n"
                f"{self.weekly_used_pct}% used\nAdditional tokens\nNever expires\n"
                "0% used (2B tokens left)\nLanguage")

    def page_html(self) -> bytes:
        cfg = {"slow": bool(self.slow_stream), "slowMarker": self.slow_marker or "",
               "slowChars": int(self.slow_chars), "slowIntervalMs": int(self.slow_interval_ms),
               "usage": self.usage_text()}
        return _UI.replace("__FAKE_MUSE_CFG__", json.dumps(cfg).replace("</", "<\\/")).encode("utf-8")

    # ---------- 请求处理 ----------
    def _serve(self, h: _Handler, method: str) -> None:
        received = time.time()
        path, _, query = h.path.partition("?")
        body = b""
        if method == "POST":
            n = int(h.headers.get("Content-Length") or 0)
            body = h.rfile.read(n) if n > 0 else b""
        cookie = h.headers.get("Cookie", "")
        status, ctype, payload, extra = self._route(method, path, cookie)
        entry = {"method": method, "path": path, "query": query, "cookie": cookie,
                 "user_agent": h.headers.get("User-Agent", ""),
                 "accept_language": h.headers.get("Accept-Language", ""),
                 "sec_ch_ua": h.headers.get("sec-ch-ua", ""),
                 "sec_fetch_site": h.headers.get("Sec-Fetch-Site", ""),
                 "headers": dict(h.headers.items()), "time": received, "status": status}
        if method == "POST":
            entry["body"] = body.decode("utf-8", "replace")
        with self._lock:
            self.log.append(entry)
        h.send_response(status)
        h.send_header("Content-Type", ctype)
        h.send_header("Content-Length", str(len(payload)))
        h.send_header("Cache-Control", "no-store")
        for k, v in extra:
            h.send_header(k, v)
        h.end_headers()
        h.wfile.write(payload)

    def _route(self, method: str, path: str, cookie: str):
        js = "application/json"
        if method == "GET" and path == "/api/session":
            if self.dead_marker and self.dead_marker in cookie:
                return 401, js, b'{"error":"unauthorized"}', []
            with self._lock:
                self.session_count += 1
                n = self.session_count
                vm_state = self.vm_state
            body = json.dumps({"status": "assigned", "vm_id": "vm-1", "vm_state": vm_state}).encode()
            extra = []
            if self.set_cookie_on_session:
                templates = [self.session_cookie] if isinstance(self.session_cookie, str) else list(self.session_cookie)
                extra = [("Set-Cookie", t.format(n=n)) for t in templates]
            return 200, js, body, extra
        if method == "POST" and path == "/api/hatch/vm/wake":
            with self._lock:
                self.wake_count += 1
                if self.wake_sets_state:
                    self.vm_state = self.wake_sets_state
            return 200, js, b"{}", []
        if method == "GET":
            if path == "/media/img.png":
                return 200, "image/png", PNG, []
            if path == "/media/vid.mp4":
                return 200, "video/mp4", MP4, []
            if path == "/" or path.startswith("/thread/"):
                return 200, "text/html; charset=utf-8", self.page_html(), []
            return 404, "text/plain", b"nf", []
        return 200, js, b"{}", []


# ------------------------- Chromium 包装脚本 -------------------------
def _executable(cand: str) -> str | None:
    if os.path.isfile(cand) and os.access(cand, os.X_OK):
        return cand
    return shutil.which(cand)


def _unwrap(path: str) -> str:
    """path 若是本模块生成的包装脚本，返回它包装的真实 Chromium，避免包装套包装。"""
    try:
        if os.path.getsize(path) < 4096:
            with open(path, encoding="utf-8", errors="replace") as f:
                for line in f:
                    if line.startswith(_WRAPPER_MARK):
                        return line[len(_WRAPPER_MARK):].strip()
    except OSError:
        pass
    return path


def find_chromium(real_chromium: str | None = None) -> str | None:
    """真实 Chromium 路径：参数 > MUSE2API_CHROMIUM > PATH 上的 chromium / chromium-browser / google-chrome。"""
    if real_chromium:
        found = _executable(real_chromium)
        return _unwrap(found) if found else None
    configured = os.environ.get("MUSE2API_CHROMIUM", "").strip()
    if configured:
        found = _executable(configured)
        if found:
            return _unwrap(found)
    for name in ("chromium", "chromium-browser", "google-chrome"):
        found = shutil.which(name)
        if found:
            return found
    return None


def make_chromium_wrapper(port: int, workdir, real_chromium: str | None = None) -> str:
    """在 workdir 下生成 shell 包装脚本并返回其路径（供 MUSE2API_CHROMIUM 使用）。

    脚本 exec 真实 Chromium（engine.stop() 的 terminate 直接作用于浏览器进程），
    并追加 --host-resolver-rules（muse.ai / www.muse.ai → 127.0.0.1:port）与
    --ignore-certificate-errors。
    """
    if os.name != "posix":
        raise unittest.SkipTest("the Chromium wrapper needs a POSIX shell; run under WSL/Linux")
    real = find_chromium(real_chromium)
    if not real:
        raise unittest.SkipTest("chromium not found; pass real_chromium, set MUSE2API_CHROMIUM, "
                                "or put chromium / chromium-browser / google-chrome on PATH")
    port = int(port)
    rules = f"MAP muse.ai 127.0.0.1:{port},MAP www.muse.ai 127.0.0.1:{port}"
    os.makedirs(workdir, exist_ok=True)
    path = os.path.join(str(workdir), f"chromium-muse-{port}")
    script = ("#!/bin/sh\n"
              f"{_WRAPPER_MARK}{real}\n"
              f"exec {shlex.quote(real)} {shlex.quote('--host-resolver-rules=' + rules)} "
              "--ignore-certificate-errors \"$@\"\n")
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(script)
    os.chmod(path, 0o755)
    return path


# ------------------------- 加载被测 app -------------------------
def restore_env() -> None:
    """把 load_app 改过的环境变量恢复为第一次修改前的值。"""
    for key, value in list(_ENV_ORIGINAL.items()):
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    _ENV_ORIGINAL.clear()


def _set_env(key: str, value) -> None:
    if key not in _ENV_ORIGINAL:
        _ENV_ORIGINAL[key] = os.environ.get(key)
    if value is None:
        os.environ.pop(key, None)
    else:
        os.environ[key] = str(value)


def load_app(env: dict | None = None, name: str = "app"):
    """在 $HOME 下的独立临时目录里加载一份全新的 app.py 模块并返回。

    默认设置 MUSE2API_HOME（data / 下载 / 媒体目录的根）、MUSE2API_PROFILE_ROOT、
    MUSE2API_KEY=DEFAULT_KEY、随机空闲的 MUSE2API_CDP_PORT（避免连上别处的 Chromium）
    与 MUSE2API_LOGIN_WAIT=25；env 中的键覆盖默认值，值为 None 表示删除该变量。
    上一次 load_app 改过的环境变量会先恢复，再应用本次设置；随后从 sys.modules 移除
    config、store、engine、cdp、proxyrelay，并以唯一模块名执行 app.py，
    因此同一进程内多次加载都读到新的环境变量。

    返回的模块带 HARNESS_WORKDIR（临时根目录）；用完调用 close_app(app)。
    """
    tag = re.sub(r"\W+", "_", name or "app").strip("_") or "app"
    workdir = home_tempdir(f"m2a-{tag}-")
    settings = {
        "MUSE2API_HOME": os.path.join(workdir, "home"),
        "MUSE2API_PROFILE_ROOT": os.path.join(workdir, "profiles"),
        "MUSE2API_KEY": DEFAULT_KEY,
        "MUSE2API_CDP_PORT": str(free_port()),
        "MUSE2API_LOGIN_WAIT": "25",
    }
    settings.update(env or {})
    restore_env()
    for key, value in settings.items():
        _set_env(key, value)
    for mod in _PURGE_MODULES:
        sys.modules.pop(mod, None)
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    modname = f"harness_app_{tag}_{uuid.uuid4().hex[:8]}"
    spec = importlib.util.spec_from_file_location(modname, REPO_ROOT / "app.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[modname] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(modname, None)
        rmtree(workdir)
        raise
    module.HARNESS_WORKDIR = workdir
    return module


def auth_headers(app) -> dict:
    key = getattr(app.CFG, "api_key", "")
    return {"Authorization": f"Bearer {key}"} if key else {}


def close_app(app) -> None:
    """停掉 app 的浏览器（含本地代理转发器），删除 load_app 建的临时目录并恢复环境变量。"""
    if app is None:
        return
    try:
        app.engine.stop()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: engine.stop() failed: {exc}", file=sys.stderr)
    sys.modules.pop(getattr(app, "__name__", ""), None)
    rmtree(getattr(app, "HARNESS_WORKDIR", None))
    restore_env()

# ------------------------- 其它夹具（再导出） -------------------------
# 放在文件末尾：harness_asgi 只在函数里反向导入本模块，这样两边谁先被 import 都不会循环。
_TESTS_DIR = str(Path(__file__).resolve().parent)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from harness_asgi import (  # noqa: E402,F401
    LiveServer,
    PoolProbe,
    SlowChat,
    StreamClient,
    browser_contexts,
    fake_engine,
    serve_app,
    snapshot,
    wait_released,
    worker_stacks,
)
from harness_net import (  # noqa: E402,F401
    FINGERPRINT_JS,
    HttpConnectProxy,
    Socks5Proxy,
    StunServer,
    TcpEcho,
    cdp_call_cost,
    close_page_target,
    forget_browser,
    kill_browser,
    primary_ipv4,
    probe_fingerprint,
    probe_page,
    probe_webrtc,
    stun_binding_request,
)
