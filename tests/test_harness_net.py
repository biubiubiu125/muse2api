"""网络与指纹夹具冒烟测试：python3 tests/test_harness_net.py

先跑不需要浏览器的部分（HTTP CONNECT 代理、SOCKS5、STUN、耗时探针，只用标准库和仓库内的
proxyrelay），再跑需要 Chromium 的部分（经 app 引擎开页走代理、指纹探针、WebRTC 对本地 STUN、
关目标 / 杀浏览器后的 CDP 调用耗时）。浏览器部分出问题时，前面各项的结论已经打印出来。

全部服务绑在本机；Chromium 经包装脚本只访问本地自签仿真站，不触达真实 muse.ai、不消耗额度。
缺少 openssl 或 Chromium 时浏览器部分打印 SKIP 并以 0 退出。

断言只针对夹具本身：能起停、协议行为正确、能分辨"有 / 无"覆盖或泄漏（用不依赖被测代码的
正 / 负对照）。H3（WebRTC 绕过代理）、H4（软隐身指纹）、M7（CDP 对断开连接忙等）的现状只以
OBSERVED 打印，不做断言，因此相应修复落地前后本脚本都应通过；缺陷本身由各修复任务的 -EXP 用例断言。
"""
import base64
import contextlib
import importlib.util
import os
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import types
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness_muse as hm  # noqa: E402
import harness_net as hn  # noqa: E402

PAGE_URL = "https://muse.ai/thread/new"
HAVE_PYSOCKS = importlib.util.find_spec("socks") is not None


# ------------------------- 原始 socket 小工具 -------------------------
def _recv_n(sock, n, timeout=5.0):
    """读最多 n 字节；对端关闭、复位或超时就返回已读到的部分。"""
    sock.settimeout(timeout)
    buf = b""
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except OSError:
            break
        if not chunk:
            break
        buf += chunk
    return buf


def _peer_closed(sock, timeout=3.0):
    """对端是否在 timeout 内关掉了连接（收到 FIN 或 RST）。"""
    sock.settimeout(timeout)
    try:
        return sock.recv(1) == b""
    except socket.timeout:
        return False
    except OSError:
        return True


def _read_head(sock, timeout=5.0):
    """读一个 HTTP 响应头，返回 (状态码, 头部文本)；对端没回完整响应头就断开时状态码为 None。"""
    sock.settimeout(timeout)
    data = b""
    while b"\r\n\r\n" not in data:
        try:
            chunk = sock.recv(4096)
        except OSError:
            chunk = b""
        if not chunk:
            return None, data.decode("latin-1")
        data += chunk
    head = data.split(b"\r\n\r\n", 1)[0].decode("latin-1")
    return int(head.split(" ", 2)[1]), head


def _http_connect(port, target, auth=None):
    """向 127.0.0.1:port 发 CONNECT target，返回 (socket, 状态码, 响应头)。"""
    sock = socket.create_connection(("127.0.0.1", port), timeout=5)
    req = f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n"
    if auth:
        req += f"Proxy-Authorization: {auth}\r\n"
    sock.sendall((req + "\r\n").encode("latin-1"))
    status, head = _read_head(sock)
    return sock, status, head


def _echo_roundtrip(sock, payload=b"ping"):
    """经已建立的隧道发 payload，确认收到 TcpEcho 加了前缀的回显。"""
    sock.sendall(payload)
    want = b"ECHO:" + payload
    return _recv_n(sock, len(want)) == want


def _socks5(port, host, dport, *, methods=(0x00,), creds=None, atyp=3):
    """原始 SOCKS5 CONNECT。返回 (socket, 结果)：结果为 "no-method" / "auth-failed" / 应答码（0 为成功）。"""
    sock = socket.create_connection(("127.0.0.1", port), timeout=5)
    sock.sendall(bytes([5, len(methods), *methods]))
    chosen = _recv_n(sock, 2)
    if chosen[1:2] == b"\xff":
        return sock, "no-method"
    if chosen[1:2] == b"\x02":
        user, pwd = (x.encode() for x in creds)
        sock.sendall(bytes([1, len(user)]) + user + bytes([len(pwd)]) + pwd)
        if _recv_n(sock, 2) != b"\x01\x00":
            return sock, "auth-failed"
    addr = bytes([3, len(host)]) + host.encode() if atyp == 3 else b"\x01" + socket.inet_aton(host)
    sock.sendall(b"\x05\x01\x00" + addr + struct.pack("!H", dport))
    reply = _recv_n(sock, 10)
    return sock, (reply[1] if len(reply) > 1 else "closed")


def _socks4a(port, host, dport, userid=b"m2a"):
    """原始 SOCKS4a CONNECT（域名交给代理解析），返回 (socket, 8 字节应答)。"""
    sock = socket.create_connection(("127.0.0.1", port), timeout=5)
    sock.sendall(b"\x04\x01" + struct.pack("!H", dport) + b"\x00\x00\x00\x01"
                 + userid + b"\x00" + host.encode() + b"\x00")
    return sock, _recv_n(sock, 8)


def _proxyrelay():
    """仓库内的 proxyrelay 模块（带账密代理的本地转发器，engine 对带账密代理就走它）。"""
    if str(hm.REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(hm.REPO_ROOT))
    import proxyrelay  # noqa: PLC0415
    return proxyrelay


def _creds_of(url):
    u = urllib.parse.urlsplit(url)
    return urllib.parse.unquote(u.username or ""), urllib.parse.unquote(u.password or "")


# ------------------------- 不需要浏览器 -------------------------
def test_exports_and_primary_ipv4():
    """harness_muse 再导出 harness_net 的全部公开名字；primary_ipv4 给出非回环 IPv4 或 None。"""
    missing = [n for n in hn.__all__ if getattr(hm, n, None) is not getattr(hn, n)]
    assert not missing, f"harness_muse 没有再导出: {missing}"
    ip = hm.primary_ipv4()
    if ip is not None:
        socket.inet_aton(ip)
        assert not ip.startswith("127."), ip
    print(f"PASS: harness_muse 再导出 harness_net 的全部公开名字；primary_ipv4()={ip}")


def test_http_connect_proxy():
    """HTTP CONNECT 代理：Basic 认证、隧道、501/502、固定上游、stop() 断隧道、与 proxyrelay 对接。"""
    creds = ("m2a user", "p@ss:w0rd/")
    with hm.TcpEcho() as echo, hm.HttpConnectProxy(require_auth=creds) as px:
        target = f"127.0.0.1:{echo.port}"
        sock, status, head = _http_connect(px.port, target)
        sock.close()
        assert status == 407 and 'Proxy-Authenticate: Basic realm="muse2api-harness"' in head, (status, head)
        wrong = "Basic " + base64.b64encode(b"m2a user:nope").decode()
        sock, status, _ = _http_connect(px.port, target, wrong)
        sock.close()
        assert status == 407, status
        sock, status, _ = _http_connect(px.port, target, px.expected_auth)
        with sock:
            assert status == 200 and _echo_roundtrip(sock), status
        entries = px.connects("127.0.0.1", echo.port)
        assert [(e["method"], e["status"], e["authorized"]) for e in entries] == [
            ("CONNECT", 407, False), ("CONNECT", 407, False), ("CONNECT", 200, True)], entries
        assert entries[-1]["auth"] == px.expected_auth and entries[-1]["upstream"] == ["127.0.0.1", echo.port]

        sock = socket.create_connection(("127.0.0.1", px.port), timeout=5)
        sock.sendall(b"GET http://muse.ai/ HTTP/1.1\r\nHost: muse.ai\r\n\r\n")
        assert _read_head(sock)[0] == 501
        sock.close()
        sock, status, _ = _http_connect(px.port, f"127.0.0.1:{hm.free_port()}", px.expected_auth)
        sock.close()
        assert status == 502, status
        for external in ("muse.ai:443", "203.0.113.5:443"):  # 没配 upstream 时不出网
            sock, status, _ = _http_connect(px.port, external, px.expected_auth)
            sock.close()
            assert status == 403, (external, status)
        refused = px.connects("muse.ai", 443)
        assert len(refused) == 1 and refused[0]["status"] == 403 and refused[0]["error"], refused

        url = px.proxy_url()
        assert _creds_of(url) == creds and urllib.parse.urlsplit(url).port == px.port, url
        assert px.proxy_url(with_auth=False) == f"http://127.0.0.1:{px.port}"

        # engine 对带账密代理起本地无鉴权转发器（proxyrelay），由它带 Basic 认证连本夹具
        # 默认只放行 443：连回显靶子的随机端口返回 403，且不连上游
        strict = _proxyrelay().ProxyRelay(url)
        strict.start()
        try:
            px.clear_log()
            sock, status, _ = _http_connect(strict.port, target)
            sock.close()
            assert status == 403 and not px.log, (status, px.log)
        finally:
            strict.stop()
        relay = _proxyrelay().ProxyRelay(url, allowed_ports=None)
        relay.start()
        try:
            px.clear_log()
            sock, status, _ = _http_connect(relay.port, target)
            with sock:
                assert status == 200 and _echo_roundtrip(sock, b"via-relay"), status
            last = px.log[-1]
            assert (last["status"], last["authorized"], last["auth"]) == (200, True, px.expected_auth), last
        finally:
            relay.stop()
    print("PASS: HTTP CONNECT 代理 —— 缺认证/错认证 407（带 Proxy-Authenticate）、正确认证 200 且隧道回显正常、"
          "非 CONNECT 501、目标不可达 502、未配 upstream 时非回环目标 403（不出网）、proxy_url 账密转义可还原、"
          "proxyrelay 带 Basic 认证经它建隧道")

    with hm.TcpEcho() as echo:
        px = hm.HttpConnectProxy(upstream=f"127.0.0.1:{echo.port}").start()
        try:
            sock, status, _ = _http_connect(px.port, "muse.ai:443")
            assert status == 200 and _echo_roundtrip(sock), status
            entry = px.connects("muse.ai", 443)[0]
            assert entry["line"] == "CONNECT muse.ai:443 HTTP/1.1" and entry["upstream"] == ["127.0.0.1", echo.port], entry
        finally:
            px.stop()
        try:
            assert _peer_closed(sock), "stop() 之后隧道仍未断开"
        finally:
            sock.close()
    print("PASS: HTTP CONNECT 代理固定上游 —— CONNECT muse.ai:443 转到回显靶子，stop() 切断仍在用的隧道")


def test_socks5_proxy():
    """SOCKS5：无认证与 user/pass、atyp=3 远端解析、无可用方法 / 认证失败、SOCKS4 拒绝与 4a 兼容、
    与 proxyrelay（经 PySocks）对接。"""
    with hm.TcpEcho() as echo:
        with hm.Socks5Proxy() as s5:
            sock, rep = _socks5(s5.port, "localhost", echo.port)
            with sock:
                assert rep == 0 and _echo_roundtrip(sock), rep
            sock, rep = _socks5(s5.port, "127.0.0.1", echo.port, atyp=1)
            with sock:
                assert rep == 0 and _echo_roundtrip(sock), rep
            got = [(e["host"], e["atyp"], e["remote_dns"], e["status"]) for e in s5.log]
            assert got == [("localhost", 3, True, "ok"), ("127.0.0.1", 1, False, "ok")], s5.log
            sock = socket.create_connection(("127.0.0.1", s5.port), timeout=5)
            sock.sendall(b"\x04\x01" + struct.pack("!H", echo.port) + socket.inet_aton("127.0.0.1") + b"\x00")
            try:
                assert _peer_closed(sock), "默认应拒绝 SOCKS4 握手并断开"
            finally:
                sock.close()
            assert s5.log[-1]["status"] == "rejected", s5.log[-1]
            sock, rep = _socks5(s5.port, "muse.ai", 443)  # 没配 upstream 时不出网：规则不允许
            sock.close()
            assert rep == 2 and s5.log[-1]["status"] == "refused", (rep, s5.log[-1])
            assert s5.proxy_url() == f"socks5://127.0.0.1:{s5.port}"

        creds = ("m2a", "s0cks/pw:@")
        with hm.Socks5Proxy(require_auth=creds, allow_socks4=True) as s5:
            sock, rep = _socks5(s5.port, "localhost", echo.port)
            sock.close()
            assert rep == "no-method", rep
            sock, rep = _socks5(s5.port, "localhost", echo.port, methods=(0, 2), creds=("m2a", "wrong"))
            sock.close()
            assert rep == "auth-failed", rep
            sock, rep = _socks5(s5.port, "localhost", echo.port, methods=(0, 2), creds=creds)
            with sock:
                assert rep == 0 and _echo_roundtrip(sock), rep
            sock, rep = _socks4a(s5.port, "localhost", echo.port)
            with sock:
                assert rep[:2] == b"\x00\x5a" and _echo_roundtrip(sock), rep
            got = [(e["version"], e["status"], e["username"]) for e in s5.log]
            assert got == [(5, "no-method", None), (5, "auth-failed", "m2a"), (5, "ok", "m2a"),
                           (4, "ok", "m2a")], s5.log
            assert (s5.log[-1]["atyp"], s5.log[-1]["remote_dns"]) == (3, True), s5.log[-1]
            url = s5.proxy_url("socks5h")
            assert urllib.parse.urlsplit(url).scheme == "socks5h" and _creds_of(url) == creds, url

            if not HAVE_PYSOCKS:
                print("INFO: 未安装 PySocks，跳过 proxyrelay → SOCKS5 对接检查")
            else:
                relay = _proxyrelay().ProxyRelay(s5.proxy_url(), allowed_ports=None)
                relay.start()
                try:
                    s5.clear_log()
                    sock, status, _ = _http_connect(relay.port, f"localhost:{echo.port}")
                    with sock:
                        assert status == 200 and _echo_roundtrip(sock, b"via-relay"), status
                    last = s5.log[-1]
                    assert (last["username"], last["atyp"], last["status"]) == ("m2a", 3, "ok"), last
                finally:
                    relay.stop()
    print("PASS: SOCKS5 —— 无认证与 RFC1929 认证、atyp=3 记为远端 DNS、无可用方法回 0xFF、认证失败回 01 01、"
          "SOCKS4 默认拒绝 / 4a 可选兼容、未配 upstream 时非回环目标回 0x02、proxy_url(socks5h) 账密转义可还原"
          + ("、proxyrelay 经 PySocks 带账密远端解析建隧道" if HAVE_PYSOCKS else ""))


def test_stun_server():
    """STUN：回 XOR-MAPPED-ADDRESS、按真实源地址记录、忽略非 STUN 报文、停止后不再应答。"""
    primary = hm.primary_ipv4()
    with hm.StunServer() as stun:
        assert stun.host == (primary or "127.0.0.1") and stun.is_loopback == (primary is None), stun.host
        res = hm.stun_binding_request(stun.address)
        assert res["matched_tid"] and res["mapped"] == res["sent_from"], res
        assert res["sent_from"][0] == stun.host, res  # 发往本机网卡地址时内核就用它当源地址
        assert stun.sources() == [res["sent_from"]] and stun.source_ips() == [stun.host], stun.log

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as junk:
            junk.settimeout(0.5)
            junk.connect((stun.host, stun.port))
            junk.send(b"definitely not a STUN binding request")
            try:
                answered = bool(junk.recv(2048))
            except OSError:
                answered = False
        assert not answered and len(stun.log) == 1, stun.log
        stun.clear_log()
        assert stun.sources() == []
        address = stun.address
    after = hm.stun_binding_request(address, timeout=0.5)
    assert after["mapped"] is None, after
    print(f"PASS: STUN（{address}）—— Binding Response 的 XOR-MAPPED-ADDRESS 等于真实源地址 {res['sent_from']}，"
          "日志按源 IP 去重，非 STUN 报文不记录不应答，stop() 后不再应答")


def test_probe_result_parsing_offline():
    """探针脚本抛错时 CDP.js 返回 dict：必须转成异常，不能当成一份"无覆盖"的结果；ICE 候选行能拆开。"""
    failed_eval = {"result": {"type": "object", "subtype": "error"},
                   "exceptionDetails": {"text": "Uncaught",
                                        "exception": {"description": "ReferenceError: x is not defined"}}}
    try:
        hn._json_result(failed_eval, "指纹探针")
    except RuntimeError as exc:
        assert "ReferenceError: x is not defined" in str(exc), exc
    else:
        raise AssertionError("探针脚本抛错时 _json_result 应抛 RuntimeError")
    cand = hn._parse_candidate("candidate:842163049 1 udp 1677729535 172.27.160.27 54321 typ srflx "
                               "raddr 0.0.0.0 rport 0 generation 0 network-cost 999")
    assert (cand["type"], cand["address"], cand["port"], cand["protocol"], cand["raddr"]) == (
        "srflx", "172.27.160.27", 54321, "udp", "0.0.0.0"), cand
    assert hn._parse_candidate("not a candidate") is None
    print("PASS: 探针结果解析 —— 脚本抛错转成 RuntimeError（带异常描述），ICE 候选行按字段拆开")


def test_cdp_cost_helpers_offline():
    """cdp_call_cost 能分辨"阻塞等待"与"忙等"并把异常记成数据；kill/close/forget 的边界行为。"""
    def spin():
        end = time.monotonic() + 0.3
        while time.monotonic() < end:
            pass

    def boom():
        raise TimeoutError("CDP Runtime.evaluate 超时")

    idle = hm.cdp_call_cost(lambda: time.sleep(0.3))
    busy = hm.cdp_call_cost(spin)
    failed = hm.cdp_call_cost(boom)
    print(f"OBSERVED: sleep 0.3s -> {idle}; 忙等 0.3s -> {busy}; 抛异常 -> {failed}")
    assert idle["error"] is None and 0.25 <= idle["wall"] < 2 and idle["cpu"] < 0.5 * idle["wall"], idle
    # 忙等的 CPU 占比随机器负载波动（全量并跑时可低到 ~50%）：只要求明显高于 sleep，不卡绝对比例
    assert busy["error"] is None and busy["wall"] >= 0.25 and busy["cpu"] >= 0.3 * busy["wall"] \
        and busy["cpu"] > idle["cpu"] + 0.05, (busy, idle)
    assert failed["error"] == "TimeoutError: CDP Runtime.evaluate 超时" and failed["wall"] < 0.5, failed

    assert hm.kill_browser(types.SimpleNamespace(proc=None)) is None
    try:
        hm.close_page_target(types.SimpleNamespace(browser=None))
    except RuntimeError:
        pass
    else:
        raise AssertionError("engine.browser 为空时 close_page_target 应抛 RuntimeError")
    if os.name == "posix":
        proc = subprocess.Popen(["sleep", "30"])
        pid = hm.kill_browser(types.SimpleNamespace(proc=proc))
        assert pid == proc.pid and proc.poll() == -signal.SIGKILL, (pid, proc.poll())

    shut = []

    class _WS:
        def __init__(self, name):
            self.name = name

        def shutdown(self):
            shut.append(self.name)

    fake = types.SimpleNamespace(page=types.SimpleNamespace(ws=_WS("page")),
                                 browser=types.SimpleNamespace(ws=_WS("browser")),
                                 current_context_id="ctx-1", _active_contexts={"ctx-1", "ctx-2"},
                                 _contexts_lock=threading.Lock())
    hm.forget_browser(fake)
    assert sorted(shut) == ["browser", "page"], shut
    assert (fake.page, fake.browser, fake.current_context_id, fake._active_contexts) == (None, None, None, set())
    print("PASS: cdp_call_cost 区分阻塞（CPU≈0）与忙等（CPU≈墙钟）并把异常记为数据；"
          "kill_browser 对无进程返回 None、对活进程 SIGKILL；forget_browser 只做本地清理")


# ------------------------- 需要 Chromium -------------------------
@contextlib.contextmanager
def _browser_app(name, env=None):
    """FakeMuse + Chromium 包装脚本 + load_app，并预先拉起浏览器；退出时先关 app 再停仿真站。"""
    with hm.FakeMuse() as muse:
        wrapper = hm.make_chromium_wrapper(muse.port, muse.workdir)
        app = hm.load_app({"MUSE2API_CHROMIUM": wrapper, "MUSE2API_STEALTH": "1", **(env or {})}, name)
        try:
            app.engine.start()
            yield muse, app
        finally:
            hm.close_app(app)


def test_browser_proxies():
    """经 app 引擎开页（生产同一条 _open_page 路径）用四种代理写法访问 https://muse.ai。"""
    with _browser_app("net-proxy") as (muse, app):
        up = ("127.0.0.1", muse.port)
        with hm.HttpConnectProxy(upstream=up) as http_plain, \
                hm.HttpConnectProxy(require_auth=("m2a user", "p@ss:w0rd"), upstream=up) as http_auth, \
                hm.Socks5Proxy(upstream=up) as socks_plain, \
                hm.Socks5Proxy(require_auth=("m2a", "s0cks/pw"), upstream=up) as socks_auth:
            cases = [("http", http_plain), ("http+auth", http_auth), ("socks5", socks_plain)]
            if HAVE_PYSOCKS:
                cases.append(("socks5+auth", socks_auth))
            else:
                print("INFO: 未安装 PySocks，跳过 socks5+auth（engine 经 proxyrelay+PySocks 连带账密 SOCKS5）")
            for label, px in cases:
                aid = "proxy-" + label
                app.engine.set_account_meta(aid, proxy=px.proxy_url())
                muse.clear_log()
                page = hm.probe_page(app.engine, aid, PAGE_URL)
                try:
                    href = page.js("location.href")
                    composer = page.js("!!document.querySelector('textarea') && "
                                       "!!document.querySelector('[aria-label=Send]')")
                finally:
                    app.engine._close_page()
                tunnels = [e for e in px.connects("muse.ai", 443) if e["status"] in (200, "ok")]
                print(f"OBSERVED: [{label}] {px.proxy_url(with_auth=False)} 日志 "
                      + "; ".join(f"{e['line']} -> {e['status']}" for e in px.log[:4]))
                assert href == PAGE_URL and composer, (label, href, composer)
                assert tunnels, (label, px.log)
                assert muse.requests_to("/thread/new"), (label, muse.log)
                if isinstance(px, hm.Socks5Proxy):
                    assert all(e["atyp"] == 3 and e["remote_dns"] for e in tunnels), (label, tunnels)
                    if px.require_auth:
                        assert all(e["username"] == px.require_auth[0] for e in tunnels), (label, tunnels)
                elif px.require_auth:
                    assert all(e["authorized"] and e["auth"] == px.expected_auth for e in tunnels), (label, tunnels)
                print(f"PASS: 浏览器经 {label} 代理打开 {href}（仿真站 DOM 就绪，代理记录 {len(tunnels)} 条到 "
                      "muse.ai:443 的已建隧道" + ("，均为远端 DNS" if isinstance(px, hm.Socks5Proxy) else "") + "）")


_CONTROL_JS = ("Object.defineProperty(navigator, 'hardwareConcurrency', {get: function () { return 61; }});"
               "Object.defineProperty(screen, 'width', {get: function () { return 777; }});true")


def _fp_summary(fp):
    ua_data = fp.get("ua_data") or {}
    return (f"own_overrides={fp['own_overrides']} non_native_getters={fp['non_native_getters']} "
            f"worker_mismatch={fp['worker_mismatch']} iframe_mismatch={fp['iframe_mismatch']} "
            f"screen={fp['screen']['width']}x{fp['screen']['height']} "
            f"window(inner)={fp['window']['inner_width']}x{fp['window']['inner_height']} "
            f"screen_smaller_than_window={fp['screen_smaller_than_window']} dpr={fp['dpr']} "
            f"dpr_media_matches={fp['dpr_media_matches']} hardware_concurrency={fp['hardware_concurrency']} "
            f"device_memory={fp['device_memory']} languages={fp['languages']} intl_locale={fp['intl_locale']} "
            f"timezone={fp['timezone']} webdriver={fp['webdriver']} webgl_unmasked={fp['webgl_unmasked']} "
            f"brands={ua_data.get('brands')} platform_version={ua_data.get('platform_version')} ua={fp['ua']}")


def test_browser_fingerprint_probe():
    """指纹探针：软隐身关闭为负对照（无覆盖）、页面内手动覆盖为正对照（必须被识别）、软隐身开启只打印现状。"""
    with _browser_app("net-fp") as (_muse, app):
        engine = app.engine
        app.CFG.stealth = False  # _apply_stealth 每开一页读一次该开关，运行中切换即可
        page = hm.probe_page(engine, "fp-off", PAGE_URL)
        try:
            base = hm.probe_fingerprint(page)
            page.js(_CONTROL_JS)
            ctrl = hm.probe_fingerprint(page)
        finally:
            engine._close_page()
        app.CFG.stealth = True
        page = hm.probe_page(engine, "fp-on", PAGE_URL)
        try:
            soft = hm.probe_fingerprint(page)
        finally:
            engine._close_page()

    print("OBSERVED: 软隐身关闭（基线）" + _fp_summary(base))
    print("OBSERVED: 正对照（页面内覆盖 hardwareConcurrency=61、screen.width=777）" + _fp_summary(ctrl))
    print("OBSERVED: 软隐身开启（H4 现状）" + _fp_summary(soft))

    for name, fp in (("基线", base), ("正对照", ctrl), ("软隐身", soft)):
        assert "error" not in fp["worker"] and "error" not in fp["iframe"], (name, fp["worker"], fp["iframe"])
    assert base["own_navigator_props"] == [] and base["own_screen_props"] == [], base["own_navigator_props"]
    assert base["own_overrides"] == {"navigator": [], "screen": []}, base["own_overrides"]
    assert base["non_native_getters"] == [], base["getter_sources"]
    assert base["worker_mismatch"] == {} and base["iframe_mismatch"] == {}, (base["worker_mismatch"],
                                                                            base["iframe_mismatch"])
    assert (base["ua_data"] or {}).get("brands"), base["ua_data"]
    print("PASS: 指纹探针负对照 —— 无覆盖时 navigator/screen 无自有属性、getter 全为原生、Worker 与 iframe 和主线程一致")

    real_cores, real_width = base["hardware_concurrency"], base["screen"]["width"]
    assert ctrl["own_overrides"] == {"navigator": ["hardwareConcurrency"], "screen": ["width"]}, ctrl["own_overrides"]
    assert {"navigator.hardwareConcurrency", "screen.width"} <= set(ctrl["non_native_getters"]), ctrl["getter_sources"]
    assert (ctrl["hardware_concurrency"], ctrl["screen"]["width"]) == (61, 777), ctrl
    assert ctrl["worker_mismatch"].get("hardware_concurrency") == [61, real_cores], ctrl["worker_mismatch"]
    assert ctrl["iframe_mismatch"].get("hardware_concurrency") == [61, real_cores], ctrl["iframe_mismatch"]
    assert ctrl["iframe_mismatch"].get("screen_width") == [777, real_width], ctrl["iframe_mismatch"]
    assert ctrl["screen_smaller_than_window"] is True, (ctrl["screen"], ctrl["window"])
    print("PASS: 指纹探针正对照 —— 实例上的覆盖被识别为自有属性与非原生 getter，Worker / iframe 不一致与"
          "screen 小于窗口都被标出")

    # 软隐身开启：伪装必须自洽（H4 已修复）——屏幕不小于窗口、Worker / iframe 与主线程一致、
    # dpr 与 matchMedia 一致；screen / hardwareConcurrency 不再用脚本覆盖（getter 保持原生）
    assert soft["screen_smaller_than_window"] is False, (soft["screen"], soft["window"])
    assert soft["worker_mismatch"] == {} and soft["iframe_mismatch"] == {}, (soft["worker_mismatch"],
                                                                            soft["iframe_mismatch"])
    assert soft["dpr"] in soft["dpr_media_matches"], (soft["dpr"], soft["dpr_media_matches"])
    assert soft["own_overrides"]["screen"] == [], soft["own_overrides"]
    assert not {"navigator.hardwareConcurrency", "screen.width"} & set(soft["non_native_getters"]), \
        soft["non_native_getters"]
    print("PASS: 软隐身指纹自洽 —— 屏幕不小于窗口、Worker / iframe 与主线程一致、dpr 与 matchMedia 一致")


def test_browser_webrtc_stun():
    """WebRTC 对本地 STUN 收集 ICE：未配代理与配 HTTP 代理各一次，只打印 H3 现状并核对夹具自洽。"""
    with hm.StunServer() as stun:
        if stun.is_loopback:
            print("INFO: 取不到非回环网卡 IPv4（Chromium 不用回环网卡做 WebRTC），跳过浏览器 WebRTC 检查")
            return
        observed = []
        with _browser_app("net-webrtc") as (muse, app), \
                hm.HttpConnectProxy(upstream=("127.0.0.1", muse.port)) as px:
            for label, proxy in (("未配代理", None), ("HTTP 代理", px.proxy_url())):
                aid = "rtc-" + ("direct" if proxy is None else "proxy")
                app.engine.set_account_meta(aid, proxy=proxy)
                page = hm.probe_page(app.engine, aid, PAGE_URL)
                stun.clear_log()
                try:
                    rtc = hm.probe_webrtc(page, stun.address, timeout=6)
                finally:
                    app.engine._close_page()
                time.sleep(0.3)  # 让最后几个重传包也落进日志
                observed.append((label, rtc, stun.source_ips()))
            tunnelled = [e for e in px.connects("muse.ai", 443) if e["status"] == 200]

    for label, rtc, sources in observed:
        print(f"OBSERVED: WebRTC[{label}] state={rtc['state']} error={rtc['error']} 本地 STUN({stun.address}) "
              f"收到来源 {sources}；srflx={rtc['srflx_addresses']} host={rtc['host_addresses']}")
    leak = observed[-1][2]
    print("OBSERVED: H3 现状 —— " + (f"账号配了 HTTP 代理时本地 STUN 仍收到来自 {leak} 的 UDP Binding Request"
                                     "（WebRTC 绕过代理直连）" if leak else
                                     "账号配了 HTTP 代理时本地 STUN 未收到 UDP（WebRTC 未绕过代理）"))
    direct, proxied = observed[0][1], observed[-1][1]
    # 未配代理：探针本身要能正常跑完（夹具自洽）
    assert direct["error"] is None and direct["state"] in ("complete", "timeout"), direct
    assert set(direct["srflx_addresses"]) <= set(observed[0][2]), (direct, observed[0][2])
    # 配了代理：WebRTC 被禁用（构造失败）或至少没有任何 UDP 直连 STUN
    assert not leak, f"配了代理的账号 WebRTC 仍直连 STUN，暴露 {leak}"
    assert not proxied["srflx_addresses"], proxied
    assert tunnelled, "配 HTTP 代理的页面没有经代理加载"
    print("PASS: 未配代理时 WebRTC 探针正常跑完 ICE 收集并与 STUN 日志对上；配了代理的账号 WebRTC 被禁用、"
          "没有 UDP 直连（H3 已修复）")


def test_browser_cdp_call_cost():
    """关掉页面目标、杀掉浏览器进程后各测一次 CDP 调用耗时（只打印 M7 现状），再验证 forget_browser 收尾。"""
    def verdict(cost):
        if cost["wall"] >= 1.0 and cost["cpu"] >= 0.5 * cost["wall"]:
            return "忙等（CPU≈墙钟）"
        return "快速返回" if cost["wall"] < 1.0 else "阻塞等待（CPU 低）"

    with _browser_app("net-cdp") as (_muse, app):
        engine = app.engine
        page = hm.probe_page(engine, "cdp-close")
        healthy = hm.cdp_call_cost(lambda: page.send("Runtime.evaluate", {"expression": "1+1"}, timeout=2))
        target = hm.close_page_target(engine)
        time.sleep(0.3)  # 等 Chromium 把该目标的 websocket 关掉
        closed = hm.cdp_call_cost(lambda: page.send("Runtime.evaluate", {"expression": "1+1"}, timeout=2))
        cleanup = hm.cdp_call_cost(engine._close_page)
        print(f"OBSERVED: 正常连接 {healthy}；关闭目标 {target} 后 send(timeout=2) -> {closed} [{verdict(closed)}]；"
              f"随后 _close_page -> {cleanup}")

        hm.probe_page(engine, "cdp-kill")
        pid = hm.kill_browser(engine)
        assert pid and engine.proc.poll() is not None, (pid, engine.proc)
        dead = hm.cdp_call_cost(lambda: engine.browser.send("Browser.getVersion", timeout=2))
        print(f"OBSERVED: SIGKILL 浏览器进程 {pid} 后 browser.send(timeout=2) -> {dead} [{verdict(dead)}]")
        hm.forget_browser(engine)
        forgotten = (engine.browser, engine.page, engine.current_context_id, set(engine._active_contexts))
        stop = hm.cdp_call_cost(engine.stop)
        print(f"OBSERVED: forget_browser 后 engine.stop() -> {stop}")

    print("OBSERVED: M7 现状 —— 目标关闭后 " + verdict(closed) + "，浏览器被杀后 " + verdict(dead))
    assert healthy["error"] is None and healthy["wall"] < 1.0, healthy
    assert target, "close_page_target 没找到当前上下文里的页面目标"
    assert closed["error"] and closed["wall"] < 3.5, closed
    assert cleanup["error"] is None and cleanup["wall"] < 5, cleanup
    assert dead["error"] and dead["wall"] < 3.5, dead
    assert forgotten == (None, None, None, set()), forgotten
    assert stop["error"] is None and stop["wall"] < 10, stop
    print("PASS: CDP 耗时探针 —— 健康连接即时返回；关目标 / 杀浏览器后 send 都以异常结束且受 timeout 约束；"
          "forget_browser 后 engine.stop() 不再卡在死连接上")


if __name__ == "__main__":
    started = time.monotonic()
    hm.run_main(
        # 不需要浏览器
        test_exports_and_primary_ipv4, test_http_connect_proxy, test_socks5_proxy,
        test_stun_server, test_probe_result_parsing_offline, test_cdp_cost_helpers_offline,
        # 需要 Chromium（缺失时打印 SKIP 并以 0 退出）
        test_browser_proxies, test_browser_fingerprint_probe, test_browser_webrtc_stun,
        test_browser_cdp_call_cost,
    )
    print(f"PASS: harness_net smoke ({time.monotonic() - started:.1f}s)")
