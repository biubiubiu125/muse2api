"""极简 Chrome DevTools Protocol 客户端（仅依赖 websocket-client / requests）。

send 的三种失败：CDP 返回错误时抛 CDPError；连接正常、到 timeout 仍没等到响应时抛 TimeoutError；
连接已断开（页面目标被关闭或崩溃、浏览器进程退出）或本端已 close() 时立即抛 CDPConnectionError。
"""
from __future__ import annotations

import json
import socket
import time

import threading

import requests
import websocket


class CDPError(RuntimeError):
    pass


class CDPConnectionError(CDPError, ConnectionError):
    """CDP 连接已断开，或本端已 close()。

    继承 CDPError，按 ``except CDPError`` / ``except Exception`` 处理的调用方不受影响；同时是
    ConnectionError，调用方也可以按连接错误单独处理。抛出时实例已标记为关闭（``closed``），
    之后对它的 send / pump 都直接抛本异常，不再收发。
    """


# 每轮 recv 的 socket 超时下限（秒）。
_MIN_RECV_WAIT = 0.05
# recv 超时只说明本轮没有消息到来，连接本身没问题。websocket-client 会把 socket 超时转成
# WebSocketTimeoutException；socket.timeout（3.10+ 即 TimeoutError）一并列出，以防直接冒出来。
_RECV_TIMEOUTS = (websocket.WebSocketTimeoutException, socket.timeout, TimeoutError)


class CDP:
    def __init__(self, ws_url: str, timeout: float = 90.0, max_size: int = 256 << 20):
        self.ws = websocket.create_connection(ws_url, timeout=timeout, max_size=max_size)
        self.timeout = timeout
        self.closed = False  # 连接已断开或已 close()；为真时 send / pump 直接抛 CDPConnectionError
        self._id = 0
        self._lock = threading.Lock()

    def _broken(self, what: str, cause) -> CDPConnectionError:
        """把实例标记为已关闭并立即释放 socket（不做关闭握手），返回要抛出的 CDPConnectionError。"""
        self.closed = True
        try:
            self.ws.shutdown()
        except Exception:  # noqa: BLE001 - socket 早已断开，关不掉也无所谓
            pass
        if isinstance(cause, BaseException):
            cause = f"{type(cause).__name__}: {cause}"
        return CDPConnectionError(f"CDP {what}: 连接已断开（{cause}）")

    def _recv(self, what: str, wait: float):
        """读一帧并解析成 dict，最多等 wait 秒（不超过 self.timeout，不少于 0.05s）。

        本轮没有消息、或该帧不是 JSON 对象时返回 None；连接断开时标记实例已关闭并抛 CDPConnectionError。
        """
        try:
            self.ws.settimeout(max(_MIN_RECV_WAIT, min(wait, self.timeout or wait)))
            raw = self.ws.recv()
        except _RECV_TIMEOUTS:
            return None
        except Exception as exc:  # noqa: BLE001 - 连接已断开，或帧读到一半出错、帧流已不可再用
            raise self._broken(what, exc) from exc
        if not getattr(self.ws, "connected", True):
            # 收到对端的关闭帧：websocket-client 回完关闭帧后 recv 返回空串
            raise self._broken(what, "对端关闭了 websocket")
        try:
            msg = json.loads(raw)
        except ValueError:  # json.JSONDecodeError：跳过该帧
            return None
        return msg if isinstance(msg, dict) else None

    def send(self, method: str, params: dict | None = None, timeout: float | None = None):
        if self.closed:  # 已关闭就不必排队等锁
            raise CDPConnectionError(f"CDP {method}: 连接已关闭")
        with self._lock:
            if self.closed:
                raise CDPConnectionError(f"CDP {method}: 连接已关闭")
            self._id += 1
            mid = self._id
            data = json.dumps({"id": mid, "method": method, "params": params or {}})
            try:
                # 发送沿用连接级超时（与改动前相同）：上一轮 recv 可能把 socket 超时收到了 0.05s，
                # 大参数（如参考图的 base64）在发送缓冲区满时会被误判为超时。
                self.ws.settimeout(self.timeout)
                self.ws.send(data)
            except Exception as exc:  # noqa: BLE001 - 发送失败（含写到一半超时）后帧流已不可再用
                raise self._broken(method, exc) from exc
            deadline = time.monotonic() + (timeout or self.timeout)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"CDP {method} 超时")
                msg = self._recv(method, remaining)
                if msg is not None and msg.get("id") == mid:
                    if "error" in msg:
                        raise CDPError(f"{method}: {msg['error']}")
                    return msg

    def js(self, expr: str, await_promise: bool = False, timeout: float | None = None):
        r = self.send("Runtime.evaluate",
                      {"expression": expr, "returnByValue": True,
                       "awaitPromise": await_promise}, timeout)
        res = r.get("result", {}).get("result", {})
        if "value" in res:
            return res["value"]
        return r.get("result")

    def pump(self, seconds: float, on_event=None, sock_timeout: float = 2.0):
        """在 seconds 秒内持续读取事件，交给回调处理；连接断开时抛 CDPConnectionError。

        每轮 recv 最多等 min(剩余时间, sock_timeout)；socket 超时只在本次调用里起作用，send 每轮自己设。
        """
        end = time.monotonic() + seconds
        while True:
            if self.closed:
                raise CDPConnectionError("CDP pump: 连接已关闭")
            remaining = end - time.monotonic()
            if remaining <= 0:
                return
            ev = self._recv("pump", min(remaining, sock_timeout or remaining))
            if ev is not None and on_event and "method" in ev:
                on_event(ev)

    def close(self):
        self.closed = True  # 先置位：之后的 send 不再排队等锁，直接抛 CDPConnectionError
        with self._lock:
            try:
                self.ws.close()
            except Exception:  # noqa: BLE001
                pass


def http_json(url: str, timeout: float = 5.0):
    return requests.get(url, timeout=timeout).json()
