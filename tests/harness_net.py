"""网络与指纹夹具：本地代理、STUN、指纹探针与 CDP 调用耗时探针。

与 harness_muse / harness_asgi 搭配使用（harness_muse 已把本模块的公开名字再导出，
测试写 ``import harness_muse as hm`` 就能用 ``hm.HttpConnectProxy`` 等）。
全部服务绑在本机、不出网、不触达真实 muse.ai。

- HttpConnectProxy：本地 HTTP CONNECT 代理，可要求 Basic 认证（不匹配回 407）。
- Socks5Proxy：最小 SOCKS5 服务，支持无认证与 RFC1929 的 user/pass，atyp=3 时
  域名交给代理解析（远端 DNS），可选兼容 SOCKS4/4a。
- StunServer：UDP STUN，记录每个 Binding Request 的来源并回 XOR-MAPPED-ADDRESS；
  默认绑在 primary_ipv4()（非回环网卡），用于判断 WebRTC 是否泄漏真实网卡 IP。
  stun_binding_request 自己发一次请求做自检；probe_webrtc 在页面里用
  RTCPeerConnection 对它收集 ICE 候选（供 H3-EXP 复用）。
- TcpEcho：回显 TCP 服务，给上面两种代理做"隧道真的通了"的靶子。
- FINGERPRINT_JS / probe_fingerprint / probe_page：指纹探针，一次取回主线程、
  Worker、iframe 的对比结果（供 H4-PROBE / H4-WORKER / H4-SEED 复用）。
- cdp_call_cost / close_page_target / kill_browser / forget_browser：测一次 CDP 调用的
  墙钟与 CPU 耗时（供 M7-EXP 判断是否忙等）；杀掉浏览器后先 forget_browser 再 close_app，
  否则未修复的 CDP 会在 engine.stop() 里对死连接忙等到 180s 超时。

典型用法::

    import harness_muse as hm

    with hm.FakeMuse() as muse, \
         hm.HttpConnectProxy(upstream=("127.0.0.1", muse.port)) as px:
        wrapper = hm.make_chromium_wrapper(muse.port, muse.workdir)
        app = hm.load_app({"MUSE2API_CHROMIUM": wrapper}, "proxy")
        try:
            app.store.add_account({"hatch_sess": "a"}, "A", proxy=px.proxy_url())
            ...  # 跑一次对话
            assert px.connects("muse.ai", 443)
        finally:
            hm.close_app(app)

非 POSIX 平台上 kill_browser 抛 unittest.SkipTest（harness_muse.run_main 会转成
``SKIP: 原因``）；其余部件只用标准库，任何平台都能起。
"""
from __future__ import annotations

import base64
import ipaddress
import json
import os
import signal
import socket
import socketserver
import select
import struct
import threading
import time
import unittest
import urllib.parse

__all__ = [
    "HttpConnectProxy", "Socks5Proxy", "StunServer", "TcpEcho",
    "primary_ipv4", "stun_binding_request", "probe_webrtc",
    "FINGERPRINT_JS", "probe_fingerprint", "probe_page",
    "cdp_call_cost", "close_page_target", "kill_browser", "forget_browser",
]

# STUN（RFC 5389）
STUN_MAGIC = 0x2112A442
_STUN_BINDING_REQUEST = 0x0001
_STUN_BINDING_RESPONSE = 0x0101
_STUN_XOR_MAPPED_ADDRESS = 0x0020

_PIPE_IDLE_SECONDS = 180.0   # 隧道空闲这么久就收线，避免 keep-alive 连接把线程留到进程退出


# ------------------------- 通用工具 -------------------------
def primary_ipv4() -> str | None:
    """本机主 IPv4（非回环）；取不到返回 None。

    用 UDP socket connect 一个文档用地址只为让内核选路由，不会真的发包，
    因此在不能出网的环境里也拿得到 eth0 的地址。
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("203.0.113.1", 9))  # TEST-NET-3，RFC 5737
            ip = s.getsockname()[0]
    except OSError:
        return None
    return ip if ip and not ip.startswith("127.") else None


def _recv_exact(sock: socket.socket, n: int, timeout: float = 10.0) -> bytes:
    """读满 n 字节；对端提前关闭或超时则抛 OSError（调用方按握手失败处理）。"""
    sock.settimeout(timeout)
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise OSError(f"对端在读满 {n} 字节前关闭了连接（已读 {len(buf)}）")
        buf += chunk
    return buf


def _pipe(a: socket.socket, b: socket.socket, idle: float = _PIPE_IDLE_SECONDS) -> None:
    """双向转发，直到一端关闭或空闲超过 idle 秒。"""
    last = time.monotonic()
    try:
        while True:
            ready, _, _ = select.select([a, b], [], [], 0.5)
            if not ready:
                if time.monotonic() - last > idle:
                    return
                continue
            last = time.monotonic()
            for src in ready:
                data = src.recv(65536)
                if not data:
                    return
                (b if src is a else a).sendall(data)
    except OSError:
        return


_REFUSED_EXTERNAL = "refused: upstream=None 时只放行回环目标（夹具不出网）"


def _loopback_target(host: str) -> bool:
    """upstream=None 时允许直连的目标：回环 IP 字面量或 localhost（不为判断而做 DNS 查询）。

    浏览器配了代理后连后台流量（如 content-autofill.googleapis.com）和 muse.ai 本身都会交给
    代理解析；不加这道闸，忘了传 upstream 的用例就会经夹具连到真实网络与真实 muse.ai。
    """
    name = (host or "").strip().strip("[]").rstrip(".").lower()
    if name in ("localhost", "ip6-localhost", "ip6-loopback"):
        return True
    try:
        return ipaddress.ip_address(name).is_loopback
    except ValueError:
        return False


def _parse_upstream(upstream) -> tuple[str, int] | None:
    """把 ``("host", port)`` / ``"host:port"`` 归一为元组；None 表示按请求的目标连。"""
    if upstream is None:
        return None
    if isinstance(upstream, (tuple, list)):
        return str(upstream[0]).strip("[]"), int(upstream[1])
    host, _, port = str(upstream).rpartition(":")
    return host.strip("[]") or "127.0.0.1", int(port)


class _TCPFixture:
    """绑 127.0.0.1 随机端口的 ThreadingTCPServer 骨架：start/stop/上下文管理器 + log。"""

    handler_class: type | None = None

    def __init__(self, *, host: str = "127.0.0.1", port: int = 0):
        self.host = host
        self.req_port = int(port)
        self.port: int | None = None
        self.log: list[dict] = []
        self._lock = threading.Lock()
        self._server: socketserver.ThreadingTCPServer | None = None
        self._thread: threading.Thread | None = None
        self._socks: set[socket.socket] = set()

    # ---------- 生命周期 ----------
    def start(self):
        if self._server:
            return self
        server = socketserver.ThreadingTCPServer((self.host, self.req_port), self.handler_class,
                                                 bind_and_activate=False)
        server.daemon_threads = True
        server.allow_reuse_address = True
        server.request_queue_size = 64  # 浏览器会并发开多条隧道，默认 backlog=5 偏小
        server.fixture = self
        try:
            server.server_bind()
            server.server_activate()
        except BaseException:
            server.server_close()
            raise
        self._server = server
        self.port = server.server_address[1]
        self._thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.2},
                                        name=f"{type(self).__name__}-{self.port}", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        server, self._server = self._server, None
        if server:
            server.shutdown()
            server.server_close()
        self._close_socks()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    # ---------- 观测 ----------
    def _record(self, entry: dict) -> dict:
        entry.setdefault("time", time.time())
        with self._lock:
            self.log.append(entry)
        return entry

    def clear_log(self) -> None:
        with self._lock:
            self.log.clear()

    def lines(self) -> list[str]:
        with self._lock:
            return [e["line"] for e in self.log]

    def connects(self, host: str | None = None, port: int | None = None) -> list[dict]:
        """已记录的隧道请求；给了 host / port 就按它们过滤。"""
        with self._lock:
            return [e for e in self.log
                    if (host is None or e.get("host") == host)
                    and (port is None or e.get("port") == port)]

    # ---------- 连接登记（stop() 时强制断开，避免线程留到进程退出） ----------
    def _track(self, *socks: socket.socket) -> None:
        with self._lock:
            self._socks.update(socks)

    def _untrack(self, *socks: socket.socket) -> None:
        with self._lock:
            for s in socks:
                self._socks.discard(s)

    def _close_socks(self) -> None:
        with self._lock:
            socks, self._socks = list(self._socks), set()
        for s in socks:
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                s.close()
            except OSError:
                pass


# ------------------------- 回显靶子 -------------------------
class _EchoHandler(socketserver.BaseRequestHandler):
    def handle(self):
        fx = self.server.fixture
        fx._record({"line": "ECHO", "peer": list(self.client_address)})
        self.request.settimeout(30)
        fx._track(self.request)
        try:
            while True:
                data = self.request.recv(65536)
                if not data:
                    return
                self.request.sendall(fx.prefix + data)
        except OSError:
            return
        finally:
            fx._untrack(self.request)


class TcpEcho(_TCPFixture):
    """回显 TCP 服务：把收到的字节加上 prefix 原样发回。

    用来验证"代理隧道真的通了"：不需要 TLS、不需要浏览器，socket 直连即可断言。
    """

    handler_class = _EchoHandler

    def __init__(self, prefix: bytes = b"ECHO:", **kwargs):
        super().__init__(**kwargs)
        self.prefix = prefix


# ------------------------- HTTP CONNECT 代理 -------------------------
class _ConnectHandler(socketserver.BaseRequestHandler):
    def handle(self):
        fx: HttpConnectProxy = self.server.fixture
        sock = self.request
        try:
            head = self._read_head(sock)
        except OSError:
            return
        if head is None:
            return
        raw_head, leftover = head
        text = raw_head.decode("latin-1")
        rows = text.split("\r\n")
        line = rows[0]
        auth = ""
        for row in rows[1:]:
            if row.lower().startswith("proxy-authorization:"):
                auth = row.split(":", 1)[1].strip()
        method, _, rest = line.partition(" ")
        target = rest.rpartition(" ")[0].strip() or rest.strip()
        host, port = _split_hostport(target)
        authorized = fx._check_auth(auth)
        entry = fx._record({"line": line, "method": method.upper(), "host": host, "port": port,
                            "auth": auth, "authorized": authorized, "status": None})

        if method.upper() != "CONNECT":
            # 本夹具只做 CONNECT 隧道（被测浏览器访问 https://muse.ai 走的就是它）。
            entry["status"] = 501
            _send(sock, b"HTTP/1.1 501 Not Implemented\r\nContent-Length: 0\r\n"
                        b"Proxy-Connection: close\r\n\r\n")
            return
        if not authorized:
            entry["status"] = 407
            _send(sock, b"HTTP/1.1 407 Proxy Authentication Required\r\n"
                        b'Proxy-Authenticate: Basic realm="' + fx.realm.encode("latin-1") + b'"\r\n'
                        b"Content-Length: 0\r\nProxy-Connection: close\r\n\r\n")
            return
        if fx.upstream is None and not _loopback_target(host):
            entry["status"] = 403
            entry["error"] = _REFUSED_EXTERNAL
            _send(sock, b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nProxy-Connection: close\r\n\r\n")
            return

        dest = fx.upstream or (host, port)
        try:
            up = socket.create_connection(dest, timeout=10)
        except OSError as exc:
            entry["status"] = 502
            entry["error"] = str(exc)
            _send(sock, b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
            return
        entry["status"] = 200
        entry["upstream"] = list(dest)
        fx._track(sock, up)
        try:
            _send(sock, b"HTTP/1.1 200 Connection Established\r\n\r\n")
            if leftover:
                up.sendall(leftover)
            sock.settimeout(None)
            up.settimeout(None)
            _pipe(sock, up)
        finally:
            fx._untrack(sock, up)
            try:
                up.close()
            except OSError:
                pass

    @staticmethod
    def _read_head(sock: socket.socket):
        sock.settimeout(15)
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = sock.recv(4096)
            if not chunk:
                return None
            data += chunk
            if len(data) > 65536:
                return None
        head, _, leftover = data.partition(b"\r\n\r\n")
        return head, leftover


def _send(sock: socket.socket, payload: bytes) -> None:
    try:
        sock.sendall(payload)
    except OSError:
        pass


def _split_hostport(target: str) -> tuple[str, int]:
    """拆 ``host:port``（兼容 ``[v6]:port`` 与缺端口），缺端口按 443。"""
    target = (target or "").strip()
    if target.startswith("["):
        host, _, tail = target.partition("]")
        host = host[1:]
        port = tail.lstrip(":")
    else:
        host, _, port = target.rpartition(":")
        if not host:
            host, port = port, ""
    try:
        return host, int(port)
    except ValueError:
        return host, 443


class HttpConnectProxy(_TCPFixture):
    """本地 HTTP CONNECT 代理。

    - require_auth=("user","p@ss")：校验 ``Proxy-Authorization: Basic …``，
      不匹配回 ``407`` 并带 ``Proxy-Authenticate``；None 表示不要求认证。
    - upstream：``("host", port)`` 或 ``"host:port"`` 时所有隧道都固定转发到那里
      （被测浏览器用 ``--host-resolver-rules`` 映射 muse.ai 时必须这么用，因为
      配了代理后域名由代理解析，而仿真站只在 127.0.0.1 上；Chromium 的后台请求如
      content-autofill.googleapis.com 也会走账号代理，一并落到仿真站）；None 表示按
      CONNECT 请求里的目标 host:port 连，但只放行回环目标（127.0.0.0/8、::1、localhost），
      其它一律回 ``403`` 并在日志记 ``error``，保证夹具不出网。

    观测：``log`` 每条含 ``line``（请求行）、``method``、``host``、``port``、
    ``auth``（原始 Proxy-Authorization）、``authorized``、``status``；
    ``connects(host, port)`` 过滤，``lines()`` 取请求行，``clear_log()`` 清空。
    """

    handler_class = _ConnectHandler

    def __init__(self, require_auth: tuple[str, str] | None = None, upstream=None,
                 *, realm: str = "muse2api-harness", **kwargs):
        super().__init__(**kwargs)
        self.require_auth = tuple(require_auth) if require_auth else None
        self.upstream = _parse_upstream(upstream)
        self.realm = realm

    # ---------- 认证 ----------
    @property
    def expected_auth(self) -> str | None:
        if not self.require_auth:
            return None
        raw = f"{self.require_auth[0]}:{self.require_auth[1]}".encode()
        return "Basic " + base64.b64encode(raw).decode()

    def _check_auth(self, auth: str) -> bool:
        expected = self.expected_auth
        return True if expected is None else auth == expected

    def proxy_url(self, with_auth: bool = True) -> str:
        """可直接写进账号 proxy 字段的地址；要求认证时带上百分号转义的账密。"""
        if self.require_auth and with_auth:
            user = urllib.parse.quote(self.require_auth[0], safe="")
            pwd = urllib.parse.quote(self.require_auth[1], safe="")
            return f"http://{user}:{pwd}@{self.host}:{self.port}"
        return f"http://{self.host}:{self.port}"


# ------------------------- SOCKS5 服务 -------------------------
class _Socks5Handler(socketserver.BaseRequestHandler):
    def handle(self):
        fx: Socks5Proxy = self.server.fixture
        sock = self.request
        try:
            ver = _recv_exact(sock, 1)[0]
            if ver == 4:
                if not fx.allow_socks4:
                    fx._record({"line": "SOCKS4 rejected", "host": None, "port": None,
                                "atyp": None, "username": None, "remote_dns": False,
                                "version": 4, "status": "rejected"})
                    return
                self._socks4(fx, sock)
                return
            if ver != 5:
                fx._record({"line": f"bad version {ver}", "host": None, "port": None,
                            "atyp": None, "username": None, "remote_dns": False,
                            "version": ver, "status": "rejected"})
                return
            self._socks5(fx, sock)
        except OSError:
            return

    # ---------- SOCKS5 ----------
    def _socks5(self, fx: "Socks5Proxy", sock: socket.socket) -> None:
        nmethods = _recv_exact(sock, 1)[0]
        methods = set(_recv_exact(sock, nmethods)) if nmethods else set()
        want = 0x02 if fx.require_auth else 0x00
        if want not in methods:
            _send(sock, b"\x05\xff")
            fx._record({"line": "no acceptable auth method", "host": None, "port": None,
                        "atyp": None, "username": None, "remote_dns": False,
                        "version": 5, "status": "no-method", "offered": sorted(methods)})
            return
        _send(sock, bytes((0x05, want)))

        username = None
        if want == 0x02:
            if _recv_exact(sock, 1)[0] != 0x01:
                return
            ulen = _recv_exact(sock, 1)[0]
            username = _recv_exact(sock, ulen).decode("utf-8", "replace") if ulen else ""
            plen = _recv_exact(sock, 1)[0]
            password = _recv_exact(sock, plen).decode("utf-8", "replace") if plen else ""
            if (username, password) != tuple(fx.require_auth):
                _send(sock, b"\x01\x01")
                fx._record({"line": f"auth failed for {username!r}", "host": None, "port": None,
                            "atyp": None, "username": username, "remote_dns": False,
                            "version": 5, "status": "auth-failed"})
                return
            _send(sock, b"\x01\x00")

        _ver, cmd, _rsv, atyp = _recv_exact(sock, 4)
        if atyp == 0x01:
            host = socket.inet_ntoa(_recv_exact(sock, 4))
        elif atyp == 0x03:
            host = _recv_exact(sock, _recv_exact(sock, 1)[0]).decode("utf-8", "replace")
        elif atyp == 0x04:
            host = socket.inet_ntop(socket.AF_INET6, _recv_exact(sock, 16))
        else:
            _send(sock, b"\x05\x08\x00\x01" + b"\x00" * 6)
            return
        port = struct.unpack("!H", _recv_exact(sock, 2))[0]
        entry = fx._record({"line": f"SOCKS5 CONNECT {host}:{port}", "host": host, "port": port,
                            "atyp": atyp, "username": username, "remote_dns": atyp == 0x03,
                            "version": 5, "status": None})
        if cmd != 0x01:
            entry["status"] = "cmd-not-supported"
            _send(sock, b"\x05\x07\x00\x01" + b"\x00" * 6)
            return
        self._tunnel(fx, sock, entry, host, port,
                     ok=b"\x05\x00\x00\x01" + b"\x00" * 6,
                     fail=b"\x05\x05\x00\x01" + b"\x00" * 6,
                     deny=b"\x05\x02\x00\x01" + b"\x00" * 6)

    # ---------- SOCKS4 / 4a（可选兼容） ----------
    def _socks4(self, fx: "Socks5Proxy", sock: socket.socket) -> None:
        cmd = _recv_exact(sock, 1)[0]
        port = struct.unpack("!H", _recv_exact(sock, 2))[0]
        raw_ip = _recv_exact(sock, 4)
        userid = b""
        while True:
            ch = _recv_exact(sock, 1)
            if ch == b"\x00":
                break
            userid += ch
        if raw_ip[:3] == b"\x00\x00\x00" and raw_ip[3] != 0:  # SOCKS4a：IP 为 0.0.0.x，随后是域名
            host = b""
            while True:
                ch = _recv_exact(sock, 1)
                if ch == b"\x00":
                    break
                host += ch
            host, atyp = host.decode("utf-8", "replace"), 0x03
        else:
            host, atyp = socket.inet_ntoa(raw_ip), 0x01
        entry = fx._record({"line": f"SOCKS4 CONNECT {host}:{port}", "host": host, "port": port,
                            "atyp": atyp, "username": userid.decode("utf-8", "replace") or None,
                            "remote_dns": atyp == 0x03, "version": 4, "status": None})
        if cmd != 0x01:
            entry["status"] = "cmd-not-supported"
            _send(sock, b"\x00\x5b" + b"\x00" * 6)
            return
        self._tunnel(fx, sock, entry, host, port,
                     ok=b"\x00\x5a" + b"\x00" * 6, fail=b"\x00\x5b" + b"\x00" * 6)

    # ---------- 共用的建隧道 ----------
    def _tunnel(self, fx: "Socks5Proxy", sock: socket.socket, entry: dict,
                host: str, port: int, *, ok: bytes, fail: bytes, deny: bytes | None = None) -> None:
        if fx.upstream is None and not _loopback_target(host):
            entry["status"] = "refused"
            entry["error"] = _REFUSED_EXTERNAL
            _send(sock, deny or fail)
            return
        dest = fx.upstream or (host, port)
        try:
            up = socket.create_connection(dest, timeout=10)
        except OSError as exc:
            entry["status"] = "connect-failed"
            entry["error"] = str(exc)
            _send(sock, fail)
            return
        entry["status"] = "ok"
        entry["upstream"] = list(dest)
        fx._track(sock, up)
        try:
            _send(sock, ok)
            sock.settimeout(None)
            up.settimeout(None)
            _pipe(sock, up)
        finally:
            fx._untrack(sock, up)
            try:
                up.close()
            except OSError:
                pass


class Socks5Proxy(_TCPFixture):
    """最小 SOCKS5 服务（只支持 CONNECT）。

    - require_auth=("user","pass")：要求 RFC1929 的 user/pass 子协商，账密不符回
      ``0x01 0x01``；None 表示只接受"无认证"方法。
    - allow_socks4：也接受 SOCKS4/4a 握手（默认 False，第一个字节是 4 时直接断开）。
    - upstream：固定转发目标（同 HttpConnectProxy）；None 表示按请求里的目标连，
      同样只放行回环目标，其它回 ``0x02``（规则不允许；SOCKS4 回 ``0x5b``），日志
      ``status="refused"``。

    远端解析：atyp=3（域名）时域名原样交给本服务，**不在客户端本地解析**，
    对应日志里的 ``remote_dns=True``。

    观测：``log`` 每条含 ``line``、``host``、``port``、``atyp``、``username``、
    ``remote_dns``、``version``、``status``。
    """

    handler_class = _Socks5Handler

    def __init__(self, require_auth: tuple[str, str] | None = None,
                 allow_socks4: bool = False, upstream=None, **kwargs):
        super().__init__(**kwargs)
        self.require_auth = tuple(require_auth) if require_auth else None
        self.allow_socks4 = bool(allow_socks4)
        self.upstream = _parse_upstream(upstream)

    def proxy_url(self, scheme: str = "socks5", with_auth: bool = True) -> str:
        """可直接写进账号 proxy 字段的地址（scheme 可用 socks5 / socks5h）。"""
        if self.require_auth and with_auth:
            user = urllib.parse.quote(self.require_auth[0], safe="")
            pwd = urllib.parse.quote(self.require_auth[1], safe="")
            return f"{scheme}://{user}:{pwd}@{self.host}:{self.port}"
        return f"{scheme}://{self.host}:{self.port}"


# ------------------------- STUN 服务 -------------------------
class StunServer:
    """本地 UDP STUN（只回 Binding Response + XOR-MAPPED-ADDRESS）。

    默认绑在 ``primary_ipv4()``（非回环网卡）：WebRTC 若绕过代理直接走本机网卡，
    这里就能收到来自真实网卡 IP 的 Binding Request——这正是 H3 要检测的泄漏。
    primary_ipv4() 取不到时退回 127.0.0.1（此时用例只能验证服务本身可用）。

    观测：``log`` 每条含 ``ip``、``port``、``tid``（事务 ID 十六进制）、``time``；
    ``sources()`` 返回去重后的 ``[(ip, port), …]``，``clear_log()`` 清空。
    """

    def __init__(self, *, host: str | None = None, port: int = 0):
        self.host = host or primary_ipv4() or "127.0.0.1"
        self.req_port = int(port)
        self.port: int | None = None
        self.log: list[dict] = []
        self._lock = threading.Lock()
        self._sock: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    # ---------- 生命周期 ----------
    def start(self) -> "StunServer":
        if self._sock:
            return self
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.bind((self.host, self.req_port))
        except OSError:
            sock.close()
            raise
        sock.settimeout(0.3)
        self._sock = sock
        self.port = sock.getsockname()[1]
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name=f"StunServer-{self.port}",
                                        daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        sock, self._sock = self._sock, None
        if sock:
            try:
                sock.close()
            except OSError:
                pass

    def __enter__(self) -> "StunServer":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    @property
    def address(self) -> str:
        """``host:port``，可直接拼成 WebRTC 的 ``stun:host:port``。"""
        return f"{self.host}:{self.port}"

    @property
    def is_loopback(self) -> bool:
        return self.host.startswith("127.")

    # ---------- 观测 ----------
    def sources(self) -> list[tuple[str, int]]:
        with self._lock:
            seen, out = set(), []
            for e in self.log:
                key = (e["ip"], e["port"])
                if key not in seen:
                    seen.add(key)
                    out.append(key)
            return out

    def source_ips(self) -> list[str]:
        return sorted({ip for ip, _ in self.sources()})

    def clear_log(self) -> None:
        with self._lock:
            self.log.clear()

    # ---------- 收发 ----------
    def _loop(self) -> None:
        sock = self._sock
        while not self._stop.is_set():
            try:
                data, addr = sock.recvfrom(2048)
            except socket.timeout:
                continue
            except ConnectionResetError:
                continue  # Windows 上前一次回包遇到 ICMP 不可达会在这里冒出来，不能让服务因此退出
            except OSError:
                return
            if len(data) < 20:
                continue
            mtype, _mlen, cookie = struct.unpack("!HHI", data[:8])
            tid = data[8:20]
            if mtype != _STUN_BINDING_REQUEST or cookie != STUN_MAGIC:
                continue
            with self._lock:
                self.log.append({"ip": addr[0], "port": addr[1], "tid": tid.hex(),
                                 "time": time.time()})
            try:
                sock.sendto(_binding_response(tid, addr), addr)
            except OSError:
                continue


def _binding_response(tid: bytes, addr: tuple[str, int]) -> bytes:
    """Binding Response + XOR-MAPPED-ADDRESS（IPv4）。"""
    xport = addr[1] ^ (STUN_MAGIC >> 16)
    xip = struct.unpack("!I", socket.inet_aton(addr[0]))[0] ^ STUN_MAGIC
    attr = struct.pack("!HHBBHI", _STUN_XOR_MAPPED_ADDRESS, 8, 0, 0x01, xport, xip)
    return struct.pack("!HHI", _STUN_BINDING_RESPONSE, len(attr), STUN_MAGIC) + tid + attr


def stun_binding_request(address: str, *, timeout: float = 3.0,
                         source: tuple[str, int] | None = None) -> dict:
    """自己发一个 Binding Request 并解析 XOR-MAPPED-ADDRESS（用于冒烟自检）。

    返回 ``{"sent_from": (ip, port), "mapped": (ip, port) | None, "matched_tid": bool}``；
    ``sent_from`` 是内核为这次发送选定的真实源地址（服务正常时应与 ``mapped`` 相同），
    超时或对端不可达时 ``mapped`` 为 None。
    """
    host, _, port = address.rpartition(":")
    tid = os.urandom(12)
    req = struct.pack("!HHI", _STUN_BINDING_REQUEST, 0, STUN_MAGIC) + tid
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        if source:
            s.bind(source)
        s.settimeout(timeout)
        # 先 connect 再发：未绑定的 UDP socket 在 sendto 之后 getsockname() 只给 0.0.0.0，
        # connect 才会让内核按路由选出真实源 IP。
        s.connect((host, int(port)))
        sent_from = tuple(s.getsockname())
        try:
            s.send(req)
            data = s.recv(2048)
        except OSError:  # 超时，或对端端口不可达（connect 过的 UDP 会收到 ECONNREFUSED）
            return {"sent_from": sent_from, "mapped": None, "matched_tid": False}
    if len(data) < 20:
        return {"sent_from": sent_from, "mapped": None, "matched_tid": False}
    mtype, mlen, cookie = struct.unpack("!HHI", data[:8])
    if mtype != _STUN_BINDING_RESPONSE or cookie != STUN_MAGIC:
        return {"sent_from": sent_from, "mapped": None, "matched_tid": False}
    mapped, body = None, data[20:20 + mlen]
    while len(body) >= 4:
        atype, alen = struct.unpack("!HH", body[:4])
        value, body = body[4:4 + alen], body[4 + alen:]
        if atype == _STUN_XOR_MAPPED_ADDRESS and len(value) >= 8:
            xport, xip = struct.unpack("!HI", value[2:8])
            mapped = (socket.inet_ntoa(struct.pack("!I", xip ^ STUN_MAGIC)),
                      xport ^ (STUN_MAGIC >> 16))
    return {"sent_from": sent_from, "mapped": mapped, "matched_tid": data[8:20] == tid}


# ------------------------- WebRTC 探针 -------------------------
# 只收集 ICE 候选，不建立任何连接：数据通道让 createOffer 产生 m 行，从而触发收集。
_WEBRTC_TPL = r"""(async function () {
  var out = {candidates: [], state: null, error: null}, pc = null;
  try {
    pc = new RTCPeerConnection({iceServers: __ICE_SERVERS__});
    pc.createDataChannel('probe');
    var finished = new Promise(function (res) {
      var timer = setTimeout(function () { res('timeout'); }, __TIMEOUT_MS__);
      pc.onicecandidate = function (e) {
        if (!e.candidate) { clearTimeout(timer); res('complete'); return; }
        if (e.candidate.candidate) out.candidates.push(e.candidate.candidate);
      };
    });
    await pc.setLocalDescription(await pc.createOffer());
    out.state = await finished;
  } catch (e) { out.error = String(e); }
  try { if (pc) pc.close(); } catch (e) {}
  return JSON.stringify(out);
})()"""


def _parse_candidate(line: str) -> dict | None:
    """拆 ``candidate:<foundation> <component> <protocol> <priority> <address> <port> typ <type> …``。"""
    parts = (line or "").strip().split()
    if len(parts) < 8 or not parts[0].startswith("candidate:") or parts[6] != "typ":
        return None
    out = {"foundation": parts[0].split(":", 1)[1], "component": parts[1],
           "protocol": parts[2].lower(), "priority": parts[3], "address": parts[4],
           "port": int(parts[5]) if parts[5].isdigit() else parts[5], "type": parts[7]}
    rest = parts[8:]
    for i in range(0, len(rest) - 1, 2):
        out.setdefault(rest[i], rest[i + 1])
    return out


def probe_webrtc(page, stun_address: str | None, *, timeout: float = 8.0) -> dict:
    """在页面里用 RTCPeerConnection 对 ``stun:<stun_address>`` 收集 ICE 候选。

    与 StunServer 搭配判断 H3：本地 STUN 收到了来自哪些源 IP 的 Binding Request
    （``StunServer.source_ips()``），页面拿到的 srflx 候选又暴露了哪些地址。

    返回 ``{"state": "complete"|"timeout"|None, "error": 异常文本或 None,
    "candidates": [原始候选行], "parsed": [拆好的候选 dict], "srflx_addresses": [...],
    "host_addresses": [...]}``；stun_address 为 None 时不配 iceServers（只看 host 候选）。
    """
    servers = [{"urls": f"stun:{stun_address}"}] if stun_address else []
    expr = (_WEBRTC_TPL.replace("__ICE_SERVERS__", json.dumps(servers))
            .replace("__TIMEOUT_MS__", str(max(100, int(float(timeout) * 1000)))))
    data = _json_result(page.js(expr, await_promise=True, timeout=float(timeout) + 15), "WebRTC 探针")
    parsed = [c for c in (_parse_candidate(x) for x in data.get("candidates") or []) if c]
    data["parsed"] = parsed
    data["srflx_addresses"] = sorted({c["address"] for c in parsed if c["type"] == "srflx"})
    data["host_addresses"] = sorted({c["address"] for c in parsed if c["type"] == "host"})
    return data


def _json_result(raw, what: str) -> dict:
    """解析探针脚本返回的 JSON 字符串；脚本本身抛错时 CDP.js 返回的是 dict，这里转成异常。"""
    if isinstance(raw, str):
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    detail = raw
    if isinstance(raw, dict):
        exc = raw.get("exceptionDetails") or {}
        detail = ((exc.get("exception") or {}).get("description") or exc.get("text")
                  or (raw.get("result") or {}).get("description") or raw)
    raise RuntimeError(f"{what}执行失败或返回了意外结果: {detail!r}"[:2000])


# ------------------------- 指纹探针 -------------------------
# Worker 里能看到的那部分指纹（Emulation 覆盖是否作用于 Worker 是 H4 的待验证点）。
_WORKER_SRC = r"""
postMessage({
  ua: navigator.userAgent,
  webdriver: (navigator.webdriver === undefined ? null : navigator.webdriver),
  hardware_concurrency: navigator.hardwareConcurrency,
  device_memory: (navigator.deviceMemory === undefined ? null : navigator.deviceMemory),
  language: navigator.language,
  languages: navigator.languages ? Array.prototype.slice.call(navigator.languages) : null,
  platform: navigator.platform,
  timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
  intl_locale: Intl.DateTimeFormat().resolvedOptions().locale,
  brands: (navigator.userAgentData
    ? navigator.userAgentData.brands.map(function (b) { return b.brand + '/' + b.version; })
    : null)
});
"""

_PROBE_TPL = r"""(async function () {
  var out = {}, src = function (fn) {
    try { return fn ? Function.prototype.toString.call(fn) : null; } catch (e) { return 'ERR ' + e; }
  }, getter = function (obj, key) {
    // 取读属性时真正生效的描述符：沿原型链从对象自身往上找第一个。实例上的覆盖
    // （软隐身的 defineProperty 正落在这里）会遮住原型上的原生 getter，必须先看实例。
    try {
      for (var o = obj; o; o = Object.getPrototypeOf(o)) {
        var d = Object.getOwnPropertyDescriptor(o, key);
        if (d) return src(d.get) || (typeof d.value === 'function' ? src(d.value) : 'value:' + typeof d.value);
      }
      return null;
    } catch (e) { return 'ERR ' + e; }
  };
  out.ua = navigator.userAgent;
  out.webdriver = navigator.webdriver;
  out.own_navigator_props = Object.getOwnPropertyNames(navigator);
  out.own_screen_props = Object.getOwnPropertyNames(screen);
  out.hardware_concurrency = navigator.hardwareConcurrency;
  out.device_memory = (navigator.deviceMemory === undefined ? null : navigator.deviceMemory);
  out.language = navigator.language;
  out.languages = Array.prototype.slice.call(navigator.languages || []);
  out.platform = navigator.platform;
  out.screen = {width: screen.width, height: screen.height,
                avail_width: screen.availWidth, avail_height: screen.availHeight,
                color_depth: screen.colorDepth};
  out.window = {outer_width: outerWidth, outer_height: outerHeight,
                inner_width: innerWidth, inner_height: innerHeight};
  out.dpr = devicePixelRatio;
  out.dpr_media_matches = [1, 1.25, 1.5, 2, 3].filter(function (x) {
    return matchMedia('(resolution: ' + x + 'dppx)').matches;
  });
  var opts = Intl.DateTimeFormat().resolvedOptions();
  out.timezone = opts.timeZone;
  out.intl_locale = opts.locale;

  var gl = null;
  try { gl = document.createElement('canvas').getContext('webgl'); } catch (e) { gl = null; }
  out.webgl = !!gl;
  // 37445/37446 是 WEBGL_debug_renderer_info 的枚举，不先启用扩展真实浏览器只会回 null
  try { if (gl) gl.getExtension('WEBGL_debug_renderer_info'); } catch (e) {}
  out.webgl_unmasked = gl ? [gl.getParameter(37445), gl.getParameter(37446)] : null;

  out.getter_sources = {
    'navigator.webdriver': getter(navigator, 'webdriver'),
    'navigator.hardwareConcurrency': getter(navigator, 'hardwareConcurrency'),
    'navigator.deviceMemory': getter(navigator, 'deviceMemory'),
    'navigator.language': getter(navigator, 'language'),
    'navigator.languages': getter(navigator, 'languages'),
    'navigator.userAgent': getter(navigator, 'userAgent'),
    'screen.width': getter(screen, 'width'),
    'screen.height': getter(screen, 'height'),
    'window.devicePixelRatio': getter(window, 'devicePixelRatio'),
    'WebGLRenderingContext.prototype.getParameter':
      (window.WebGLRenderingContext ? src(WebGLRenderingContext.prototype.getParameter) : null),
    'WebGL2RenderingContext.prototype.getParameter':
      (window.WebGL2RenderingContext ? src(WebGL2RenderingContext.prototype.getParameter) : null)
  };
  // 软隐身的 defineProperty 覆盖会落在实例上：记下哪些键已变成 navigator/screen 自有属性
  out.own_overrides = {
    navigator: ['webdriver', 'hardwareConcurrency', 'deviceMemory', 'language', 'languages', 'userAgent']
      .filter(function (k) { return Object.prototype.hasOwnProperty.call(navigator, k); }),
    screen: ['width', 'height', 'availWidth', 'availHeight']
      .filter(function (k) { return Object.prototype.hasOwnProperty.call(screen, k); })
  };

  if (navigator.userAgentData) {
    out.ua_data = {
      brands: navigator.userAgentData.brands.map(function (b) { return b.brand + '/' + b.version; }),
      mobile: navigator.userAgentData.mobile, platform: navigator.userAgentData.platform
    };
    try {
      var he = await navigator.userAgentData.getHighEntropyValues(
        ['platformVersion', 'fullVersionList', 'architecture', 'bitness']);
      out.ua_data.platform_version = he.platformVersion;
      out.ua_data.architecture = he.architecture;
      out.ua_data.bitness = he.bitness;
      out.ua_data.full_version_list = (he.fullVersionList || []).map(function (b) {
        return b.brand + '/' + b.version;
      });
    } catch (e) { out.ua_data.high_entropy_error = String(e); }
  } else {
    out.ua_data = null;
  }

  try {
    var w = new Worker(URL.createObjectURL(new Blob([__WORKER_SRC__], {type: 'text/javascript'})));
    out.worker = await new Promise(function (res) {
      var done = setTimeout(function () { res({error: 'worker timeout'}); }, 8000);
      w.onmessage = function (e) { clearTimeout(done); res(e.data); };
      w.onerror = function (e) { clearTimeout(done); res({error: String(e.message || e)}); };
    });
    w.terminate();
  } catch (e) { out.worker = {error: String(e)}; }

  try {
    var f = document.createElement('iframe');
    f.src = 'about:blank';
    document.body.appendChild(f);
    var wn = f.contentWindow, nav = wn.navigator, o2 = wn.Intl.DateTimeFormat().resolvedOptions();
    out.iframe = {
      ua: nav.userAgent, webdriver: nav.webdriver,
      hardware_concurrency: nav.hardwareConcurrency,
      device_memory: (nav.deviceMemory === undefined ? null : nav.deviceMemory),
      language: nav.language,
      languages: Array.prototype.slice.call(nav.languages || []),
      platform: nav.platform,
      screen_width: wn.screen.width, screen_height: wn.screen.height,
      dpr: wn.devicePixelRatio, timezone: o2.timeZone, intl_locale: o2.locale,
      own_navigator_props: Object.getOwnPropertyNames(nav),
      brands: (nav.userAgentData
        ? nav.userAgentData.brands.map(function (b) { return b.brand + '/' + b.version; })
        : null)
    };
    f.remove();
  } catch (e) { out.iframe = {error: String(e)}; }

  return JSON.stringify(out);
})()"""

FINGERPRINT_JS = _PROBE_TPL.replace("__WORKER_SRC__", json.dumps(_WORKER_SRC))

# worker / iframe 结果里与主线程同义的键（用于算不一致项）；
# screen_width / screen_height 在主线程结果里对应 screen.width / screen.height。
_WORKER_COMPARE = ("ua", "hardware_concurrency", "device_memory", "language", "languages",
                   "platform", "timezone", "intl_locale")
_IFRAME_COMPARE = _WORKER_COMPARE + ("webdriver", "dpr", "screen_width", "screen_height")


def probe_fingerprint(page, timeout: float = 40.0) -> dict:
    """在给定 CDP page 上执行指纹探针，返回主线程 / Worker / iframe 的对比结果。

    关键字段：``ua``、``webdriver``、``own_navigator_props``、``own_screen_props``、
    ``own_overrides``、``getter_sources``、``non_native_getters``、
    ``hardware_concurrency``、``device_memory``、``language``/``languages``、
    ``platform``、``screen``、``window``、``dpr``、``dpr_media_matches``、
    ``timezone``、``intl_locale``、``webgl_unmasked``、
    ``ua_data``（brands / platform_version / full_version_list）、``worker``、``iframe``。

    另外由 Python 侧补三个派生字段，直接服务 H4 的断言：

    - ``non_native_getters``：``getter_sources`` 里 toString 不含 ``[native code]`` 的键
      （按读属性时实际生效的描述符取，实例上的覆盖优先于原型上的原生 getter）；
    - ``worker_mismatch`` / ``iframe_mismatch``：Worker / iframe 与主线程取值不同的键
      （含 brands；iframe 另比 dpr 与 screen 宽高），``{键: [主线程值, 另一侧值]}``；
    - ``screen_smaller_than_window``：screen 尺寸是否小于窗口（inner / outer 取大者；
      真实有头浏览器不会出现）。

    页面必须已有 document.body（用 probe_page 打开即可）。探针脚本本身抛错时抛 RuntimeError，
    不会返回一份缺字段的结果冒充"无覆盖"。
    """
    data = _json_result(page.js(FINGERPRINT_JS, await_promise=True, timeout=timeout), "指纹探针")

    sources = data.get("getter_sources") or {}
    data["non_native_getters"] = sorted(k for k, v in sources.items()
                                        if isinstance(v, str) and "[native code]" not in v)
    screen, window = data.get("screen") or {}, data.get("window") or {}
    main = dict(data, screen_width=screen.get("width"), screen_height=screen.get("height"))
    main_brands = (data.get("ua_data") or {}).get("brands")
    for side, keys in (("worker", _WORKER_COMPARE), ("iframe", _IFRAME_COMPARE)):
        other = data.get(side)
        mismatch: dict = {}
        if isinstance(other, dict) and "error" not in other:
            for key in keys:
                if key in other and other[key] != main.get(key):
                    mismatch[key] = [main.get(key), other[key]]
            if other.get("brands") is not None and other.get("brands") != main_brands:
                mismatch["brands"] = [main_brands, other.get("brands")]
        data[f"{side}_mismatch"] = mismatch
    win_w = max(window.get("inner_width") or 0, window.get("outer_width") or 0)
    win_h = max(window.get("inner_height") or 0, window.get("outer_height") or 0)
    data["screen_smaller_than_window"] = bool(
        screen and window
        and ((screen.get("width") or 0) < win_w or (screen.get("height") or 0) < win_h))
    return data


def probe_page(engine, account_id: str = "probe", url: str = "about:blank",
               *, timeout: float = 30.0):
    """为 account_id 开一个隔离页面、导航到 url、等到 document.body 就绪后返回该 page。

    与被测代码同一条路径（``engine._open_page`` → 套用代理 / 时区 / 软隐身），
    所以探针看到的就是真实账号页面的指纹。调用方用完自己调 ``engine._close_page()``。

    导航失败（代理不通、证书、DNS 等，``Page.navigate`` 返回 errorText）时抛 RuntimeError，
    不会把 Chromium 的错误页当成目标页返回。
    """
    if account_id and not engine._acct(account_id):
        # 被测代码对没有账号配置的 account_id 失败关闭；探针账号没写过配置时按「无代理」写一份
        engine.set_account_meta(account_id)
    page = engine._open_page(account_id)
    engine.page = page
    try:
        res = page.send("Page.navigate", {"url": url}, timeout=timeout) or {}
        err = (res.get("result") or {}).get("errorText")
        if err:
            raise RuntimeError(f"导航到 {url} 失败: {err}")
        deadline = time.monotonic() + float(timeout)
        while time.monotonic() < deadline:
            try:
                if page.js("document.readyState !== 'loading' && !!document.body", timeout=5):
                    return page
            except Exception:  # noqa: BLE001 - 导航中途 evaluate 可能短暂失败
                pass
            time.sleep(0.2)
        raise TimeoutError(f"{timeout}s 内 {url} 的 document.body 仍未就绪")
    except BaseException:
        engine._close_page()
        raise


# ------------------------- CDP 调用耗时探针 -------------------------
def cdp_call_cost(fn) -> dict:
    """调一次 fn() 并返回 ``{"wall": 墙钟秒, "cpu": CPU 秒, "error": 异常描述或 None}``。

    ``cpu`` 取 ``time.process_time()`` 的增量（整个进程的 CPU 时间，含其它线程），
    所以要在进程基本空闲时测。CPU 接近墙钟即说明调用在忙等（M7 的判据）。
    fn 里给 ``CDP.send`` 显式传小 timeout（如 2s）：未修复的 send 对断开的连接会一直
    忙等到超时，浏览器主连接的默认超时是 180s。
    """
    wall0, cpu0 = time.monotonic(), time.process_time()
    error = None
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 - 这里就是要把异常当数据记下来
        error = f"{type(exc).__name__}: {exc}"
    return {"wall": round(time.monotonic() - wall0, 3),
            "cpu": round(time.process_time() - cpu0, 3),
            "error": error}


def close_page_target(engine) -> str | None:
    """用 ``Target.closeTarget`` 关掉当前线程页面所在上下文里的页面 target。

    页面的 CDP 连接随之失效，但 websocket 本身不会立刻报错——正好用来测
    "目标已没了还对它 send" 的行为。返回被关掉的 targetId；找不到返回 None。
    """
    browser = getattr(engine, "browser", None)
    if browser is None:
        raise RuntimeError("engine.browser 为空：先 engine.start()")
    ctx_id = getattr(engine, "current_context_id", None)
    if not ctx_id:
        raise RuntimeError("当前线程没有隔离上下文：先用 probe_page / _open_page 开页")
    infos = ((browser.send("Target.getTargets", timeout=10).get("result") or {})
             .get("targetInfos") or [])
    target_id = next((t["targetId"] for t in infos
                      if t.get("type") == "page" and t.get("browserContextId") == ctx_id), None)
    if not target_id:
        return None
    browser.send("Target.closeTarget", {"targetId": target_id}, timeout=10)
    return target_id


def kill_browser(engine, *, wait: float = 5.0) -> int | None:
    """SIGKILL 掉 engine 拉起的 Chromium 进程，返回被杀的 pid（没有可杀的返回 None）。

    用于测"浏览器整个没了之后 CDP.send 的行为"；复用外部 Chromium 时 ``engine.proc``
    为 None，此时返回 None（调用方据此 skip）。

    测完务必先 ``forget_browser(engine)`` 再 ``close_app``：engine 仍握着已断开的
    CDP 连接，``engine.stop()`` 会对每个隔离上下文发 ``Target.disposeBrowserContext``，
    未修复的 CDP.send 会在死连接上忙等到 180s 超时。
    """
    proc = getattr(engine, "proc", None)
    if proc is None or proc.poll() is not None:
        return None
    if not hasattr(signal, "SIGKILL"):
        raise unittest.SkipTest("kill_browser needs SIGKILL (POSIX); run under WSL/Linux")
    os.kill(proc.pid, signal.SIGKILL)
    try:
        proc.wait(timeout=wait)
    except Exception:  # noqa: BLE001
        pass
    return proc.pid


def forget_browser(engine) -> None:
    """让 engine 忘掉已断开（或进程已被杀）的浏览器，之后 ``engine.stop()`` / close_app 不再卡住。

    只做本地清理、不发任何 CDP 命令：直接关掉浏览器主连接与当前线程页面连接的 socket
    （不经 ``CDP.close()``，它要拿 ``_lock``，别的线程正卡在 send 里时会跟着卡），
    再把 ``engine.browser`` / ``engine.page`` / ``engine.current_context_id`` 置空、
    清空活动上下文登记。其它线程各自持有的页面不在此列（它们的 socket 已随浏览器断开）。
    """
    for conn in (getattr(engine, "page", None), getattr(engine, "browser", None)):
        ws = getattr(conn, "ws", None)
        if ws is not None:
            try:
                ws.shutdown()
            except Exception:  # noqa: BLE001 - socket 早已断开，关不掉也无所谓
                pass
    engine.page = None
    engine.current_context_id = None
    engine.browser = None
    active = getattr(engine, "_active_contexts", None)
    if active is not None:
        lock = getattr(engine, "_contexts_lock", None)
        if lock is None:
            active.clear()
        else:
            with lock:
                active.clear()
