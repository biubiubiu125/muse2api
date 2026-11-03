"""进程内 ASGI 夹具、断连流式客户端与并发资源观测工具。

与 harness_muse 搭配使用（harness_muse 已把本模块的公开名字再导出，
后续测试写 ``import harness_muse as hm`` 就能用 ``hm.LiveServer`` 等）。

- LiveServer / serve_app：在后台线程里用进程内 uvicorn 跑被测 app
  （``lifespan="off"``，跳过启动期浏览器预热与保活守护线程），
  绑定 127.0.0.1 的随机空闲端口，``start()`` 等到 ``/healthz`` 可访问。
- StreamClient：原始 socket 流式客户端，可在读到部分分片后 ``abort()``
  直接关掉连接，模拟"客户端中途断开"，并给出首个内容分片的到达耗时。
- snapshot / wait_released / worker_stacks / PoolProbe：观测 GEN_LOCK 槽位、
  ACCOUNT_POOL 占用、浏览器上下文数量，以及 worker 线程是否卡在写队列。
- fake_engine / SlowChat：不启动真实浏览器就能跑纯调度用例的轻量假引擎。

典型用法（断连释放用例）::

    import harness_muse as hm

    app = hm.load_app({"MUSE2API_CONCURRENCY": "1"}, "c1")
    try:
        hm.fake_engine(app, chat_stream=hm.SlowChat())
        aid = app.store.add_account({"hatch_sess": "a"}, "A")["id"]
        with hm.serve_app(app) as srv:
            with hm.StreamClient(srv) as cli:
                cli.post_stream("/v1/chat/completions",
                                {"stream": True,
                                 "messages": [{"role": "user", "content": "hi"}]})
                cli.read_until(b'"content"', timeout=20)
                cli.abort()
            print(hm.snapshot(app), hm.wait_released(app, timeout=10))
    finally:
        hm.close_app(app)

缺少 uvicorn 时抛 unittest.SkipTest（harness_muse.run_main 会转成 ``SKIP: 原因``）。
"""
from __future__ import annotations

import http.client
import json
import os
import socket
import sys
import threading
import time
import traceback
import unittest
from pathlib import Path

__all__ = [
    "LiveServer", "serve_app", "StreamClient", "PoolProbe", "SlowChat",
    "fake_engine", "snapshot", "wait_released", "worker_stacks", "browser_contexts",
]


def _hm():
    """延迟导入 harness_muse：避免与它尾部的再导出形成循环导入。"""
    tests_dir = str(Path(__file__).resolve().parent)
    if tests_dir not in sys.path:
        sys.path.insert(0, tests_dir)
    import harness_muse  # noqa: PLC0415
    return harness_muse


# ------------------------- 进程内 ASGI 服务 -------------------------
class LiveServer:
    """在后台线程里跑一份进程内 uvicorn。

    - target：load_app 返回的模块（取其 ``.app``）或任意 ASGI 应用。
    - lifespan 固定为 off：不触发 app 的 startup 事件，因此不会预热浏览器、
      也不会拉起保活守护线程，测试只面对被观测的那一条链路。
    - start() 轮询 ``/healthz`` 直到 200；stop() 置 ``should_exit`` 并 join 线程
      （线程为 daemon，个别卡死的请求不会拖住进程退出）。
    """

    def __init__(self, target, *, host: str = "127.0.0.1", port: int | None = None,
                 log_level: str = "warning", health_path: str = "/healthz",
                 start_timeout: float = 20.0):
        self.asgi = getattr(target, "app", target)
        self.host = host
        self.port = int(port) if port else _hm().free_port()
        self.log_level = log_level
        self.health_path = health_path
        self.start_timeout = float(start_timeout)
        self._server = None
        self._thread: threading.Thread | None = None

    # ---------- 生命周期 ----------
    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def url(self, path: str) -> str:
        return self.base_url + (path if path.startswith("/") else "/" + path)

    def start(self) -> "LiveServer":
        if self._server is not None:
            return self
        try:
            import uvicorn  # noqa: PLC0415
        except ImportError as exc:  # pragma: no cover - 环境缺依赖
            raise unittest.SkipTest(f"uvicorn not importable; the in-process ASGI fixture needs it ({exc})")
        self._server = uvicorn.Server(uvicorn.Config(
            self.asgi, host=self.host, port=self.port, log_level=self.log_level,
            lifespan="off", access_log=False))
        self._thread = threading.Thread(target=self._server.run,
                                        name=f"LiveServer-{self.port}", daemon=True)
        self._thread.start()
        deadline = time.monotonic() + self.start_timeout
        while time.monotonic() < deadline:
            if not self._thread.is_alive():
                self._server = None
                raise RuntimeError("uvicorn 线程已退出，进程内服务没起来")
            try:
                if self.request("GET", self.health_path, timeout=1.5)[0] == 200:
                    return self
            except OSError:
                pass
            time.sleep(0.1)
        self.stop()
        raise RuntimeError(f"{self.start_timeout}s 内 {self.health_path} 仍不可访问")

    def stop(self) -> None:
        server, self._server = self._server, None
        thread, self._thread = self._thread, None
        if server is None:
            return
        server.should_exit = True
        if thread:
            thread.join(timeout=5)
            if thread.is_alive():
                # 还有请求没结束（例如被测代码卡在写队列）：强制退出，别拖住测试
                server.force_exit = True
                thread.join(timeout=5)
            if thread.is_alive():
                print(f"WARN: LiveServer({self.port}) 线程仍在运行（daemon，进程退出时回收）",
                      file=sys.stderr)

    def __enter__(self) -> "LiveServer":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    # ---------- 普通（非流式）请求 ----------
    def request(self, method: str, path: str, *, body=None, headers: dict | None = None,
                timeout: float = 10.0):
        """发一个普通请求，返回 (状态码, 响应体字节)。body 为 dict 时按 JSON 发送。"""
        hdrs = dict(headers or {})
        if isinstance(body, (dict, list)):
            body = json.dumps(body, ensure_ascii=False).encode("utf-8")
            hdrs.setdefault("Content-Type", "application/json")
        conn = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
        try:
            conn.request(method, path, body=body, headers=hdrs)
            resp = conn.getresponse()
            return resp.status, resp.read()
        finally:
            conn.close()

    def is_listening(self, timeout: float = 1.0) -> bool:
        """端口是否还在监听（stop() 之后应为 False）。"""
        try:
            with socket.create_connection((self.host, self.port), timeout=timeout):
                return True
        except OSError:
            return False


def serve_app(target, **kwargs) -> LiveServer:
    """起一个 LiveServer 并等到 /healthz 可用；可直接用作上下文管理器。"""
    return LiveServer(target, **kwargs).start()


# ------------------------- 原始 socket 流式客户端 -------------------------
class StreamClient:
    """用原始 socket 手写 HTTP 请求的流式客户端。

    之所以不用 requests / TestClient：要能在读到部分分片后"直接关掉连接"，
    不给对端任何收尾（除 TCP 自身的 FIN/RST），从而复现真实客户端中途断开。

    观测属性（单位秒，均相对发出请求的时刻）：
    - first_byte_seconds：收到第一个字节（响应头）的耗时；
    - first_chunk_seconds：第一次 read_until 命中标记（首个内容分片到达）的耗时；
    - last_match_seconds：最近一次 read_until 命中的耗时；
    - aborted_seconds：调用 abort() 的时刻。
    """

    def __init__(self, target, *, api_key: str | None = None, connect_timeout: float = 10.0):
        base = target.base_url if isinstance(target, LiveServer) else str(target)
        host, _, port = base.split("//", 1)[-1].partition(":")
        self.host = host or "127.0.0.1"
        self.port = int(port or 80)
        self.api_key = _hm().DEFAULT_KEY if api_key is None else api_key
        self.connect_timeout = float(connect_timeout)
        self.buf = b""
        self.sock: socket.socket | None = None
        self.t_request: float | None = None
        self.first_byte_seconds: float | None = None
        self.first_chunk_seconds: float | None = None
        self.last_match_seconds: float | None = None
        self.aborted_seconds: float | None = None
        self.eof = False

    # ---------- 发请求 ----------
    def post_stream(self, path: str, json_body: dict, headers: dict | None = None) -> "StreamClient":
        body = json.dumps(json_body, ensure_ascii=False).encode("utf-8")
        hdrs = {"Host": f"{self.host}:{self.port}", "Content-Type": "application/json",
                "Accept": "text/event-stream", "Content-Length": str(len(body))}
        if self.api_key:
            hdrs["Authorization"] = f"Bearer {self.api_key}"
        hdrs.update(headers or {})
        head = f"POST {path} HTTP/1.1\r\n" + "".join(f"{k}: {v}\r\n" for k, v in hdrs.items()) + "\r\n"
        self.close()
        self.buf = b""
        self.eof = False
        self.first_byte_seconds = self.first_chunk_seconds = None
        self.last_match_seconds = self.aborted_seconds = None
        self.sock = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
        self.t_request = time.monotonic()
        self.sock.sendall(head.encode("latin-1") + body)
        return self

    # ---------- 读响应 ----------
    def elapsed(self) -> float:
        return 0.0 if self.t_request is None else time.monotonic() - self.t_request

    def read_some(self, n: int = 4096, timeout: float = 10.0) -> bytes:
        """读最多 n 字节；超时返回 b""（不抛），对端关闭时置 eof。"""
        if self.sock is None or self.eof:
            return b""
        self.sock.settimeout(max(0.05, float(timeout)))
        try:
            data = self.sock.recv(n)
        except socket.timeout:
            return b""
        except OSError:
            self.eof = True
            return b""
        if not data:
            self.eof = True
            return b""
        if self.first_byte_seconds is None:
            self.first_byte_seconds = self.elapsed()
        self.buf += data
        return data

    def read_until(self, marker: bytes, timeout: float = 30.0) -> bytes:
        """读到 buf 中出现 marker 为止，返回累计字节；超时抛 TimeoutError。"""
        deadline = time.monotonic() + float(timeout)
        while marker not in self.buf:
            if self.eof:
                raise TimeoutError(f"连接已关闭仍未读到 {marker!r}；已收到 {len(self.buf)} 字节")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"{timeout}s 内未读到 {marker!r}；已收到 {len(self.buf)} 字节："
                                   f"{self.buf[-200:]!r}")
            self.read_some(timeout=min(1.0, remaining))
        self.last_match_seconds = self.elapsed()
        if self.first_chunk_seconds is None:
            self.first_chunk_seconds = self.last_match_seconds
        return self.buf

    def status(self) -> int | None:
        """已读到的响应状态码（还没读到状态行时为 None）。"""
        line, sep, _ = self.buf.partition(b"\r\n")
        if not sep or not line.startswith(b"HTTP/"):
            return None
        parts = line.split(None, 2)
        return int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None

    def sse_data(self) -> list[str]:
        """已读到的 SSE ``data:`` 行（最后一行可能还不完整，调用方自行判断）。"""
        text = self.buf.split(b"\r\n\r\n", 1)[-1].decode("utf-8", "replace")
        return [ln[6:] for ln in text.splitlines() if ln.startswith("data: ")]

    # ---------- 断开 ----------
    def abort(self) -> float:
        """直接关掉 socket 模拟客户端中途断开；返回断开时刻（相对请求发出）。"""
        self.aborted_seconds = self.elapsed()
        self.close()
        return self.aborted_seconds

    def close(self) -> None:
        sock, self.sock = self.sock, None
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    def __enter__(self) -> "StreamClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


# ------------------------- 资源释放观测 -------------------------
def browser_contexts(app, timeout: float = 5.0) -> int | None:
    """浏览器隔离上下文数量；浏览器未启动或连接不可用时返回 None（不抛异常）。"""
    browser = getattr(getattr(app, "engine", None), "browser", None)
    if browser is None:
        return None
    try:
        res = browser.send("Target.getBrowserContexts", timeout=timeout) or {}
    except Exception:  # noqa: BLE001 - 连接已断/浏览器已退出都算"观测不到"
        return None
    return len((res.get("result") or {}).get("browserContextIds") or [])


def snapshot(app) -> dict:
    """一次资源占用快照：GEN_LOCK 槽位、账号占用计数、浏览器上下文数量。"""
    gen_lock = getattr(app, "GEN_LOCK", None)
    pool = getattr(app, "ACCOUNT_POOL", None)
    return {"gen_lock_active": int(getattr(gen_lock, "_active_count", 0) or 0),
            "busy_accounts": dict(getattr(pool, "busy_accounts", None) or {}),
            "contexts": browser_contexts(app)}


def wait_released(app, timeout: float = 10.0, interval: float = 0.2) -> float | None:
    """轮询到"GEN_LOCK 槽位归零且无账号占用"，返回耗时秒数；超时返回 None。"""
    t0 = time.monotonic()
    deadline = t0 + float(timeout)
    while True:
        snap = snapshot(app)
        if snap["gen_lock_active"] == 0 and not snap["busy_accounts"]:
            return round(time.monotonic() - t0, 2)
        if time.monotonic() >= deadline:
            return None
        time.sleep(interval)


def worker_stacks(app=None, name: str = "worker", depth: int = 4) -> list[dict]:
    """所有包含名为 name 的帧的线程栈摘要。

    返回 ``[{"thread": 线程名, "frames": ["app.py:383 worker", ...]}, …]``，
    frames 取末 depth 帧（最后一帧就是线程当前阻塞的位置，例如 ``queue.py:… put``）。
    传入 app 时只保留栈里出现过该 app 模块文件的线程，避免误报同名函数。
    """
    want_file = os.path.abspath(getattr(app, "__file__", "") or "") if app is not None else ""
    names = {t.ident: t.name for t in threading.enumerate()}
    out = []
    for tid, frame in list(sys._current_frames().items()):
        stack = traceback.extract_stack(frame)
        if not any(f.name == name for f in stack):
            continue
        if want_file and not any(os.path.abspath(f.filename or "") == want_file for f in stack):
            continue
        out.append({"thread": names.get(tid, f"tid-{tid}"),
                    "frames": [f"{Path(f.filename).name}:{f.lineno} {f.name}" for f in stack[-depth:]]})
    return out


class PoolProbe:
    """绑定一份 app 的观测器（方法与同名模块级函数等价）。"""

    def __init__(self, app):
        self.app = app

    def snapshot(self) -> dict:
        return snapshot(self.app)

    def wait_released(self, timeout: float = 10.0, interval: float = 0.2) -> float | None:
        return wait_released(self.app, timeout, interval)

    def worker_stacks(self, name: str = "worker", depth: int = 4) -> list[dict]:
        return worker_stacks(self.app, name, depth)

    def describe(self) -> str:
        """一行摘要，便于直接打印到测试输出。"""
        snap = self.snapshot()
        stacks = self.worker_stacks()
        tail = "; ".join(st["frames"][-1] for st in stacks) or "无 worker 线程"
        return (f"gen_lock_active={snap['gen_lock_active']} busy_accounts={snap['busy_accounts']} "
                f"contexts={snap['contexts']} worker@{tail}")


# ------------------------- 轻量假引擎 -------------------------
def fake_engine(app, *, chat_stream=None, generate=None, read_cookies=None, **overrides) -> dict:
    """把 app.engine 的浏览器相关方法换成轻量假实现，不启动真实 Chromium。

    固定替换 ``start`` / ``close_current_page`` / ``reset_thread`` / ``read_cookies``；
    ``chat_stream`` / ``generate`` 传了就注入（签名与 engine 同名方法一致）；
    其余关键字参数按名字直接赋到 engine 上（例如 quota=…、refresh=…）。

    返回调用计数 dict（运行中会被就地更新）::

        {"start": 0, "close_current_page": 0, "reset_thread": 0, "read_cookies": 0}
    """
    engine = app.engine
    calls = {"start": 0, "close_current_page": 0, "reset_thread": 0, "read_cookies": 0}

    def _counter(key, result=None):
        def fn(*args, **kwargs):
            calls[key] += 1
            return result() if callable(result) else result
        return fn

    engine.start = _counter("start")
    engine.close_current_page = _counter("close_current_page")
    engine.reset_thread = _counter("reset_thread")
    if callable(read_cookies):
        engine.read_cookies = read_cookies
    else:
        engine.read_cookies = _counter("read_cookies", lambda: dict(read_cookies or {}))
    if chat_stream is not None:
        engine.chat_stream = chat_stream
    if generate is not None:
        engine.generate = generate
    for key, value in overrides.items():
        setattr(engine, key, value)
    return calls


class SlowChat:
    """可注入 engine.chat_stream 的慢速假实现：每 interval 秒吐一个 chunk。

    chunk 默认 1 个字符：app 的 _pace_text 对 <=3 字的文本零延迟放行，
    所以客户端看到的分片节奏就是这里的 interval。

    观测属性：calls（每次调用的 account_id / prompt）、yielded（已吐出分片数）、
    stop_seen（是否看到 stop_event 置位后主动收尾）、finished（生成器是否已结束）。
    """

    def __init__(self, chunk: str = "S", interval: float = 0.01, count: int = 5000):
        self.chunk = chunk
        self.interval = float(interval)
        self.count = int(count)
        self.calls: list[dict] = []
        self.yielded = 0
        self.stop_seen = False
        self.finished = False

    def __call__(self, cookies: dict, prompt: str, expires: dict | None = None,
                 timeout: int | None = None, account_id: str | None = None,
                 stop_event=None):
        self.calls.append({"account_id": account_id, "prompt": prompt,
                           "session": (cookies or {}).get("hatch_sess")})
        try:
            for _ in range(self.count):
                if stop_event is not None and stop_event.is_set():
                    self.stop_seen = True
                    return
                yield self.chunk
                self.yielded += 1
                time.sleep(self.interval)
        finally:
            self.finished = True
