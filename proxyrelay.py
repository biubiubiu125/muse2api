"""带账密代理的本地转发器。

Chromium 的 BrowserContext 代理不支持账号密码鉴权（只支持 IP 白名单代理）。
本模块为"带账密的上游代理"在本地起一个**无鉴权的 HTTP CONNECT 代理**，
Chromium 指向 http://127.0.0.1:<随机端口>，由本转发器把流量按账密转发到上游
（上游可为 http(s):// 或 socks5://，socks5 需 PySocks）。

仅实现 CONNECT 隧道（muse.ai 全为 HTTPS，足够），其它方法直接拒绝。
"""
from __future__ import annotations

import base64
import logging
import select
import socket
import ssl
import threading
import urllib.parse

log = logging.getLogger("muse2api")


class ProxyRelay:
    # 转发器无鉴权、本机任何进程都连得上：只放行 HTTPS（Chromium 只对 https/wss 发 CONNECT），
    # 不让人借账号的付费代理去连任意端口
    ALLOWED_PORTS = (443,)

    def __init__(self, upstream: str, allowed_ports: tuple[int, ...] | None = ALLOWED_PORTS):
        """allowed_ports=None 不限制目标端口（仅测试用）。"""
        self.upstream = upstream
        self.allowed_ports = allowed_ports
        self.port: int | None = None
        self._srv: socket.socket | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> int:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(128)
        self._srv = srv
        self.port = srv.getsockname()[1]
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()
        return self.port

    def _serve(self):
        while True:
            try:
                cli, _ = self._srv.accept()
            except OSError:
                return
            threading.Thread(target=self._handle, args=(cli,), daemon=True).start()

    def _handle(self, cli: socket.socket):
        up = None
        try:
            cli.settimeout(60)
            data = b""
            while b"\r\n\r\n" not in data:
                chunk = cli.recv(4096)
                if not chunk:
                    return
                data += chunk
                if len(data) > 65536:
                    return
            line = data.split(b"\r\n", 1)[0].decode("latin1", "replace")
            parts = line.split()
            if len(parts) < 2 or parts[0].upper() != "CONNECT":
                cli.sendall(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
                return
            host, _, port = parts[1].rpartition(":") if ":" in parts[1] else (parts[1], "", "443")
            try:
                port_n = int(port or "443")
            except ValueError:
                port_n = -1
            if not host or (self.allowed_ports is not None and port_n not in self.allowed_ports):
                log.warning("代理转发拒绝 CONNECT 目标 %s（只放行端口 %s）", parts[1][:100], self.allowed_ports)
                cli.sendall(b"HTTP/1.1 403 Forbidden\r\n\r\n")
                return
            up = self._dial_upstream(host, port_n)
            if up is None:
                cli.sendall(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                return
            cli.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            # 转发客户端在 CONNECT 头之后可能已粘包发来的字节（标准客户端一般不会，但稳妥起见）
            leftover = data[data.index(b"\r\n\r\n") + 4:]
            if leftover:
                try:
                    up.sendall(leftover)
                except Exception:  # noqa: BLE001
                    return
            self._pipe(cli, up)
        except Exception:  # noqa: BLE001
            pass
        finally:
            for s in (cli, up):
                try:
                    if s:
                        s.close()
                except Exception:  # noqa: BLE001
                    pass

    def _dial_upstream(self, host: str, port: int):
        u = urllib.parse.urlsplit(self.upstream if "://" in self.upstream else "http://" + self.upstream)
        scheme = (u.scheme or "http").lower()
        user = urllib.parse.unquote(u.username) if u.username else None
        pwd = urllib.parse.unquote(u.password) if u.password else None
        try:
            if scheme.startswith("socks"):
                import socks  # PySocks
                s = socks.socksocket()
                stype = socks.SOCKS4 if scheme in ("socks4", "socks4a") else socks.SOCKS5
                rdns = scheme not in ("socks4",)  # 远端 DNS，避免本地解析泄漏
                s.set_proxy(stype, u.hostname, u.port, rdns=rdns, username=user, password=pwd)
                s.settimeout(60)
                s.connect((host, port))
                return s
            if scheme == "https":
                # https:// 代理：到代理本身先走 TLS（带 SNI 并校验证书），再在 TLS 里发 CONNECT
                raw = socket.create_connection((u.hostname, u.port or 443), timeout=60)
                try:
                    s = ssl.create_default_context().wrap_socket(raw, server_hostname=u.hostname)
                except Exception:
                    raw.close()
                    raise
            else:
                s = socket.create_connection((u.hostname, u.port or 80), timeout=60)
            req = f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n"
            if user is not None:
                cred = base64.b64encode(f"{user}:{pwd or ''}".encode()).decode()
                req += f"Proxy-Authorization: Basic {cred}\r\n"
            req += "\r\n"
            s.sendall(req.encode())
            resp = b""
            while b"\r\n\r\n" not in resp:
                ch = s.recv(4096)
                if not ch:
                    s.close()
                    return None
                resp += ch
                if len(resp) > 65536:
                    s.close()
                    return None
            status = resp.split(b"\r\n", 1)[0]
            if b" 200" not in status:
                s.close()
                return None
            return s
        except Exception as exc:  # noqa: BLE001
            log.warning("代理转发连接上游失败: %s", exc)
            return None

    @staticmethod
    def _pipe(a: socket.socket, b: socket.socket):
        a.settimeout(None)
        b.settimeout(None)
        socks_pair = [a, b]
        while True:
            # TLS 套接字（https:// 上游）可能已把数据解密进内部缓冲，select 看不到：先处理掉
            r = [s for s in socks_pair if isinstance(s, ssl.SSLSocket) and s.pending()]
            if not r:
                try:
                    r, _, _ = select.select(socks_pair, [], [], 300)
                except Exception:  # noqa: BLE001
                    return
                if not r:
                    return
            for s in r:
                try:
                    data = s.recv(65536)
                except Exception:  # noqa: BLE001
                    return
                if not data:
                    return
                dst = b if s is a else a
                try:
                    dst.sendall(data)
                except Exception:  # noqa: BLE001
                    return

    def stop(self):
        try:
            if self._srv:
                self._srv.close()
        except Exception:  # noqa: BLE001
            pass
