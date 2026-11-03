"""muse2api — 把 Muse(muse.ai) 网页免费账号的对话/生图/生视频额度反代成 API。

OpenAI 兼容:
  POST /v1/chat/completions          （文本 / 代码对话，支持 stream，可接 Codex）
  POST /v1/images/generations
  POST /v1/videos  +  GET /v1/videos/{task_id}
  GET  /v1/models
  GET  /v1/media/{name}

管理（前端账号池管理页面在 GET /）:
  GET    /admin/status
  GET    /admin/accounts
  POST   /admin/accounts            （单条 / 批量文本 / 批量数组）
  PATCH  /admin/accounts/{id}       （改标签、启用/禁用）
  DELETE /admin/accounts/{id}
  POST   /admin/accounts/{id}/test  （真实打开 muse.ai 验证会话是否有效）
  POST   /admin/accounts/{id}/relogin
  GET    /admin/tasks               （任务记录）
  DELETE /admin/tasks/{id}  |  POST /admin/tasks/clear
  GET    /admin/media  |  POST /admin/media/delete  |  DELETE /admin/media/{name}
  GET    /admin/extension           （浏览器扩展 zip，用来取 cookie）
  GET    /admin/cookie-helper       （命令行取 cookie 脚本，进阶）
"""
from __future__ import annotations

from typing import Any

import asyncio
import base64
import binascii
import concurrent.futures
import contextlib
import contextvars
import io
import hashlib
import json
import logging
import os
import re
import threading
import time
import uuid
import zipfile

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.responses import (FileResponse, HTMLResponse, JSONResponse,
                               Response, StreamingResponse)
from pydantic import BaseModel, Field, ValidationError

from config import CFG, dotenv_paths
from engine import (ESSENTIAL_COOKIES, MAX_REFERENCE_IMAGE_BYTES,
                    MAX_REFERENCE_IMAGES, MAX_REFERENCE_TOTAL_BYTES,
                    MuseAuthError, MuseEngine, MuseGenerationError,
                    MuseModelRefusalError, MuseQuotaError, MuseSubmittedError,
                    RefImageSafetyError,
                    _sniff_image_mime)
from store import Store, _mask_proxy, account_expiry, min_expiry

import sys
log = logging.getLogger("muse2api")
log.setLevel(logging.INFO)
if not log.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    log.addHandler(_h)

CFG.ensure_dirs()
app = FastAPI(title="muse2api", version="1.6.4")

# Cookie 助手脚本从 muse.ai 页面发起导入请求，需要放行该来源；
# 浏览器扩展从 chrome-extension:// 发起，也一并放行。
#
# 这里直接放行所有来源：本服务用 Bearer Key 鉴权、不依赖 Cookie，
# 放行来源不会带来越权风险；反之如果把来源限死，浏览器端的智能体
# （Open WebUI / LobeChat / 各种 Web 客户端）会被 CORS 拦住用不了。
# 这是刻意设计，没有可配置的来源白名单。
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
# CORSMiddleware 在下面 _BodyLimitMiddleware 之后注册（最外层），超限 413 也带 CORS 头

# 本次请求的来源地址（scheme://host[:port]），供 media_url 在未配置 public_base 时拼绝对 URL。
# 反向代理场景按 X-Forwarded-Proto / X-Forwarded-Host 还原客户端看到的地址。
_REQUEST_BASE: contextvars.ContextVar[str] = contextvars.ContextVar("muse2api_request_base", default="")


class _RequestBaseMiddleware:
    def __init__(self, asgi_app):
        self.asgi_app = asgi_app

    async def __call__(self, scope, receive, send):
        asgi_app = self.asgi_app
        if scope.get("type") != "http":
            return await asgi_app(scope, receive, send)
        headers = {k.decode("latin1").lower(): v.decode("latin1") for k, v in scope.get("headers") or []}
        proto = (headers.get("x-forwarded-proto") or scope.get("scheme") or "http").split(",")[0].strip()
        host = (headers.get("x-forwarded-host") or headers.get("host") or "").split(",")[0].strip()
        if proto not in ("http", "https") or not host or any(c in host for c in "/\\ @?#"):
            host = ""
        token = _REQUEST_BASE.set(f"{proto}://{host}" if host else "")
        try:
            return await asgi_app(scope, receive, send)
        finally:
            _REQUEST_BASE.reset(token)


app.add_middleware(_RequestBaseMiddleware)

# 请求体上限：参考图最多 24 MiB 原始字节，Base64 / multipart 编码后约 1.4 倍，再留余量
MAX_REQUEST_BODY = int(os.environ.get("MUSE2API_MAX_BODY_BYTES", str(40 * 1024 * 1024)))


class _BodyLimitMiddleware:
    """入口处限制请求体大小：先看 Content-Length，再按实际收到的字节数兜底（chunked 上传）。
    超限返回 413，不让 multipart 落盘或 JSON 读进内存后才截断检查。"""
    def __init__(self, asgi_app):
        self.asgi_app = asgi_app

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http":
            return await self.asgi_app(scope, receive, send)
        for k, v in scope.get("headers") or []:
            if k.lower() == b"content-length":
                try:
                    too_big = int(v) > MAX_REQUEST_BODY
                except ValueError:
                    too_big = False
                if too_big:
                    return await self._reject(send)
        received = 0
        rejected = False

        async def limited_receive():
            nonlocal received, rejected
            message = await receive()
            if message.get("type") == "http.request":
                received += len(message.get("body") or b"")
                if received > MAX_REQUEST_BODY:
                    rejected = True
                    raise StarletteHTTPException(413, "请求体过大")
            return message
        try:
            return await self.asgi_app(scope, limited_receive, send)
        except StarletteHTTPException:
            if rejected:
                return await self._reject(send)
            raise

    @staticmethod
    async def _reject(send):
        body = json.dumps({"error": {"message": f"请求体超过 {MAX_REQUEST_BODY // (1024 * 1024)} MiB 上限",
                                     "type": "invalid_request_error", "param": None, "code": 413}},
                          ensure_ascii=False).encode()
        await send({"type": "http.response.start", "status": 413,
                    "headers": [(b"content-type", b"application/json"),
                                (b"content-length", str(len(body)).encode())]})
        await send({"type": "http.response.body", "body": body})


app.add_middleware(_BodyLimitMiddleware)
# 最后注册 = 最外层：_BodyLimitMiddleware 直接发出的 413 也经过它，浏览器端客户端能看到真实错误
app.add_middleware(CORSMiddleware,
                   allow_origins=["*"],
                   allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
                   allow_headers=["*"],
                   expose_headers=["*"],
                   max_age=600)


store = Store(CFG)
engine = MuseEngine(CFG)


# ------------------------- 流式断连控制（C1） -------------------------
# 下游连续这么多秒没有取数、且 worker 还有数据要写（队列已满）时，判定下游停止读取并中止生成。
# 这只是兜底：客户端断开由 _watch_disconnect 在数秒内察觉；慢速读取只要还在取数就不会触发。
STREAM_STALL_SECONDS = 60


class _ClientGone(Exception):
    """客户端已断开或停止读取。worker 只做清理（关页、释放账号与 GEN_LOCK 槽位），不写账号状态。"""


class InternalGenerationError(MuseGenerationError):
    """对话 worker 里未包装的底层异常（CDP 断连、超时等），包成生成失败；错误码仍按 server_error 报。"""


class NoAccountError(MuseAuthError):
    """账号池里没有可用账号（或指定账号不存在 / 已禁用）。与某个账号的会话失效无关，接口统一返回 400。"""


class _NoTimeLeft(MuseGenerationError):
    """排队耗掉了大部分时限，剩余时间不够完成一次生成，没有发送提示词。与账号无关：不写账号状态、不切号。"""


class QuotaPausedError(MuseGenerationError):
    """所有可调度的账号都在额度暂停期：不打开页面、不发提示词，直接告诉调用方何时恢复。
    接口按 429 + Retry-After 返回（见 _quota_paused_http），客户端不会当成网关故障立即重试。"""

    def __init__(self, message: str, until: int | None = None):
        super().__init__(message)
        self.until = until

    def retry_after(self) -> int:
        return max(1, int((self.until or 0) - time.time())) if self.until else 3600


def _quota_paused_http(exc: "QuotaPausedError") -> HTTPException:
    return HTTPException(429, str(exc), headers={"Retry-After": str(exc.retry_after())})


class NoAlternativeAccount(MuseGenerationError):
    """切号重试时，排除刚失败的账号后没有其它启用、有 cookie 且未判为失效（ok is not False）的账号（L6 / 2.53）。

    AccountPoolManager.acquire 立即抛出、不进入等待；对话 worker 捕获后不重试，返回第一次的错误。"""


class _StreamCtl:
    """一次对话请求的流式控制块，由端点、断连监视任务、worker 与消费侧共用。

    - stop_event：置位即叫停 worker（客户端断开、下游停读或生成器被关闭）；
    - done：worker 结束（无论成败）时置位，消费侧据此判断结束；
    - last_pull：下游最近一次取数的时刻（time.monotonic()），由 touch() 更新。
    """

    def __init__(self):
        self.stop_event = threading.Event()
        self.done = threading.Event()
        self.last_pull = time.monotonic()
        self.started = False   # safe_chat_stream 已开始运行（worker 线程已拉起）
        self.final_text = None  # 对话正常结束时页面上的最终回复文本（非流式结果以它为准）
        self.release = lambda: None   # 释放排队名额（PENDING.once()），可重复调用

    def touch(self) -> None:
        self.last_pull = time.monotonic()


# 长耗时请求（对话、同步生图）排队等浏览器时各占一个 anyio 线程池名额（默认共 40 个）；
# 不设上限的话，排队请求会占满线程池，连 /healthz 和管理接口都无法响应。超出上限返回 429。
MAX_PENDING = max(1, int(os.environ.get("MUSE2API_MAX_PENDING", "30")))


class _PendingGate:
    def __init__(self):
        self._lock = threading.Lock()
        self.count = 0

    def enter(self) -> None:
        with self._lock:
            if self.count >= MAX_PENDING:
                raise HTTPException(429, "排队请求过多，请稍后重试",
                                    headers={"Retry-After": "10"})
            self.count += 1

    def leave(self) -> None:
        with self._lock:
            self.count = max(0, self.count - 1)

    def once(self):
        """返回只生效一次的 leave：流式请求在正常结束与断连两条路径上都会调用它。"""
        done = threading.Event()

        def _leave():
            if not done.is_set():
                done.set()
                self.leave()
        return _leave

    @contextlib.asynccontextmanager
    async def hold(self):
        self.enter()
        try:
            yield
        finally:
            self.leave()


PENDING = _PendingGate()

# 长耗时等待（对话、同步生图、参考图下载）专用线程池：大小与 PENDING 上限一致。
# 默认线程池只有 min(32, CPU+4) 个线程（2 核机器只有 6 个），被排队请求占满后，
# 管理接口、账号测试、保活都要跟着排队；分开之后它们始终有线程可用。
_LONG_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PENDING, thread_name_prefix="m2a-long")


async def _in_long_pool(fn, *args, **kwargs):
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()  # 带上请求上下文（如 media_url 用的请求来源地址）
    return await loop.run_in_executor(_LONG_POOL, lambda: ctx.run(fn, *args, **kwargs))


class ConcurrencyLock:
    """多账号并发调度锁。
    由 CFG.max_concurrency 限制最大并发槽位；
    同时提供与标准 Lock 兼容的 acquire / release / locked 接口，
    确保向下兼容历史单锁断言与管理端操作。"""
    def __init__(self, limit: int = 2):
        self.limit = max(1, limit)
        self._sem = threading.Semaphore(self.limit)
        self._count_lock = threading.Lock()
        self._active_count = 0

    def acquire(self, blocking: bool = True, timeout: float | None = -1) -> bool:
        if timeout is None:
            timeout = -1
        if timeout < 0:
            ok = self._sem.acquire(blocking=blocking)
        else:
            ok = self._sem.acquire(blocking=blocking, timeout=timeout)
        if ok:
            with self._count_lock:
                self._active_count += 1
        return ok

    def release(self):
        with self._count_lock:
            if self._active_count > 0:
                self._active_count -= 1
        self._sem.release()

    def locked(self) -> bool:
        with self._count_lock:
            return self._active_count > 0

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class AccountPoolManager:
    """账号并发冲突管理器：每账号最多 CFG.per_account_concurrency 个并发（默认 1=串行），多账号并行。
    busy_accounts 为 {账号ID: 占用计数}（计数归零即删键，兼容成员判断与清点）。"""
    def __init__(self, store_instance):
        self.store = store_instance
        self.busy_accounts: dict[str, int] = {}
        self.cond = threading.Condition()

    def _limit(self) -> int:
        try:
            return max(1, int(getattr(CFG, "per_account_concurrency", 1)))
        except Exception:
            return 1

    def _switch_targets(self, exclude_ids: set) -> set:
        """排除 exclude_ids 后可供切号的账号 ID：启用、有 cookie 且未判为失效（ok is not False），忙碌的也算。

        直接读 store.accounts 里各账号的字段（用 .get），不依赖 Store 的其它方法。"""
        now_ts = time.time()
        return {a.get("id") for a in list(self.store.accounts)
                if a.get("enabled", True) and a.get("cookies") and a.get("ok") is not False
                and (a.get("quota_blocked_until") or 0) <= now_ts
                and a.get("id") not in exclude_ids}

    def _quota_paused_until(self) -> int | None:
        """启用、带 cookie 且未判为失效（ok is not False）的账号全部处于额度暂停期时，返回最早恢复的时间戳；
        否则（含没有这样的账号）返回 None。判为失效的账号不参与：不能因为池里还有一个失效账号，
        就把请求兜底发给额度暂停的账号。"""
        now_ts = time.time()
        until = [int(a.get("quota_blocked_until") or 0) for a in list(self.store.accounts)
                 if a.get("enabled", True) and a.get("cookies") and a.get("ok") is not False]
        if not until or any(u <= now_ts for u in until):
            return None
        return min(until)

    def acquire(self, account_id: str | None = None, preferred_id: str | None = None,
                rotate: bool = True, force_rotate: bool = False,
                timeout: float = 60.0, record_use: bool = True,
                stop_event: threading.Event | None = None,
                exclude_ids: set[str] | list[str] | None = None,
                allow_disabled: bool = False, allow_quota_paused: bool = False) -> dict:
        """租一个账号；等待期间每 0.5s 检查一次 stop_event，置位即抛 _ClientGone。
        allow_disabled 只对显式 account_id 生效：管理端探测已禁用的账号时也照样排队占用。

        exclude_ids（切号重试用，例如刚失败的账号）：只在排除它们之后"启用、有 cookie 且未判为失效
        （ok is not False）"的账号里选；要排除的、已判为失效的账号与已达并发上限的忙碌账号一并经
        pick_account 的 exclude_ids 跳过。这样的账号一个都没有时立即抛 NoAlternativeAccount，不进入等待；
        还有（哪怕正忙）就照常等待。显式指定 account_id 时不看 exclude_ids。

        没有指定 account_id 且启用的账号全部处于额度暂停期时，立即抛 QuotaPausedError（不选号、不发提示词）；
        管理端探测和预热传 allow_quota_paused=True，照常兜底选号。"""
        limit = self._limit()
        deadline = time.monotonic() + timeout
        excluded = None if exclude_ids is None else set(exclude_ids)
        with self.cond:
            while True:
                if stop_event is not None and stop_event.is_set():
                    raise _ClientGone()
                if not account_id and not allow_quota_paused:
                    paused_until = self._quota_paused_until()
                    if paused_until:
                        raise QuotaPausedError(
                            "所有账号额度均已耗尽，暂停调度中，预计 "
                            + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(paused_until))
                            + " 恢复；补充额度后可在管理页重新导入 cookie 立即解除", until=paused_until)
                targets = None
                if excluded is not None and not account_id:
                    targets = self._switch_targets(excluded)
                    if not targets:
                        raise NoAlternativeAccount("没有其它可用账号可切换")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise MuseGenerationError("等待空闲账号超时，当前所有账号均处于繁忙中")

                if account_id:
                    acc = self.store.get_account(account_id)
                    if not acc or not (allow_disabled or acc.get("enabled", True)):
                        raise NoAccountError(f"指定账号 {account_id} 不存在或已禁用")
                    if self.busy_accounts.get(account_id, 0) < limit:
                        self.busy_accounts[account_id] = self.busy_accounts.get(account_id, 0) + 1
                        return acc
                else:
                    blocked = {aid for aid, cnt in self.busy_accounts.items() if cnt >= limit}
                    if targets is not None:
                        # 切号重试只在候选账号里选：要排除的、已判为失效的账号与忙碌账号一并跳过
                        blocked |= {a.get("id") for a in list(self.store.accounts) if a.get("id") not in targets}
                    pref = preferred_id if preferred_id not in blocked else None
                    acc = self.store.pick_account(
                        preferred_id=pref,
                        rotate=rotate,
                        force_rotate=force_rotate,
                        exclude_ids=blocked,
                        record_use=record_use,
                    )
                    if acc:
                        self.busy_accounts[acc["id"]] = self.busy_accounts.get(acc["id"], 0) + 1
                        return acc

                live = [a for a in self.store.accounts if a.get("enabled", True) and a.get("cookies")]
                if not live:
                    raise NoAccountError("没有可用账号，请先在管理页面导入 cookie")

                self.cond.wait(timeout=min(remaining, 0.5))

    def wait_idle(self, account_id: str, timeout: float) -> bool:
        """不占用、只等指定账号低于并发上限（或超时）。返回等待结束时它是否空闲。"""
        limit = self._limit()
        deadline = time.monotonic() + max(0.0, timeout)
        with self.cond:
            while self.busy_accounts.get(account_id, 0) >= limit:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self.cond.wait(timeout=min(remaining, 0.5))
            return True

    def try_acquire_id(self, account_id: str, allow_disabled: bool = False) -> dict | None:
        """立即尝试占用指定账号：空闲就占用并返回账号，忙则返回 None（不等待）。"""
        with self.cond:
            acc = self.store.get_account(account_id)
            if not acc or not (allow_disabled or acc.get("enabled", True)):
                raise NoAccountError(f"指定账号 {account_id} 不存在或已禁用")
            if self.busy_accounts.get(account_id, 0) >= self._limit():
                return None
            self.busy_accounts[account_id] = self.busy_accounts.get(account_id, 0) + 1
            return acc

    def release(self, account_id: str | None):
        if not account_id:
            return
        with self.cond:
            cnt = self.busy_accounts.get(account_id, 0)
            if cnt <= 1:
                self.busy_accounts.pop(account_id, None)
            else:
                self.busy_accounts[account_id] = cnt - 1
            self.cond.notify_all()


GEN_LOCK = ConcurrencyLock(limit=CFG.max_concurrency)
ACCOUNT_POOL = AccountPoolManager(store)


def _acquire_slot(timeout: float, stop_event: threading.Event | None = None) -> bool:
    """分片等待一个 GEN_LOCK 槽位：每片最多 0.5s，片间检查 stop_event。

    拿到返回 True；超时返回 False（至少尝试一次）；stop_event 置位时抛 _ClientGone，不占槽位。"""
    deadline = time.monotonic() + max(0.0, float(timeout))
    while True:
        if stop_event is not None and stop_event.is_set():
            raise _ClientGone()
        if GEN_LOCK.acquire(timeout=max(0.0, min(0.5, deadline - time.monotonic()))):
            if stop_event is not None and stop_event.is_set():
                GEN_LOCK.release()
                raise _ClientGone()
            return True
        if time.monotonic() >= deadline:
            return False


def _has_live_account() -> bool:
    """只读判断：是否存在启用且带 cookie 的账号（与 pick_account 的候选范围一致）。

    对话入口用它做"没有可用账号"的 400 检查：不选号，不累加 use_count / last_used，不动轮转计数。
    账号由 worker 拿到 GEN_LOCK 槽位后经 ACCOUNT_POOL.acquire 租用。"""
    return any(a.get("enabled", True) and a.get("cookies") for a in list(store.accounts))


# 首次尝试失败后切号重试时，最多等其它账号空出来这么久（期间占着浏览器槽位）
SWITCH_WAIT = 15
# 发提示词前至少要剩这么多秒（timeout 较小时取 timeout 的一半）
MIN_GENERATION_SECONDS = 30
ADMIN_PROBE_WAIT = 120  # 管理端探测（测试 / 额度 / 重登）等浏览器槽位与账号的上限（秒）


@contextlib.contextmanager
def _leased_account(account_id: str | None = None, timeout: float = ADMIN_PROBE_WAIT,
                    record_use: bool = True):
    """管理端探测用：遵守每账号并发上限；退出时关闭本线程的页面与隔离上下文，再释放账号与槽位。

    - 指定 account_id：与生成链路同一顺序（先槽位后账号），但不占着槽位空等忙碌的账号：
      先在不持任何资源的情况下等这个账号空闲，再取槽位，占着槽位时只「立即尝试」租账号；
      租不到（被别人抢先）就放回槽位重来。既不会持账号等槽位（与生成任务相向卡住），
      也不会在目标账号跑长任务时占着全局槽位拖慢其它账号。
    - account_id 为空：先槽位再按轮转租一个空闲账号。"""
    acc = None
    slot = False
    try:
        if account_id:
            deadline = time.monotonic() + timeout
            while acc is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise MuseGenerationError("等待空闲账号超时，当前所有账号均处于繁忙中")
                ACCOUNT_POOL.wait_idle(account_id, timeout=remaining)
                slot = GEN_LOCK.acquire(timeout=max(0.0, deadline - time.monotonic()))
                if not slot:
                    raise MuseGenerationError("等待浏览器队列超时，请稍后重试")
                acc = ACCOUNT_POOL.try_acquire_id(account_id, allow_disabled=True)
                if acc is None:  # 刚空出来又被生成任务抢走：放回槽位重来
                    GEN_LOCK.release()
                    slot = False
        else:
            slot = GEN_LOCK.acquire(timeout=timeout)
            if not slot:
                raise MuseGenerationError("等待浏览器队列超时，请稍后重试")
            acc = ACCOUNT_POOL.acquire(rotate=True, timeout=timeout, record_use=record_use,
                                       allow_quota_paused=True)
        yield acc
    finally:
        try:
            if slot:
                engine.close_current_page()
        finally:
            if acc:
                ACCOUNT_POOL.release(acc["id"])
            if slot:
                GEN_LOCK.release()


IMAGE_TASK_LOCK = threading.Lock()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


# ------------------------- OpenAI 风格的错误响应 -------------------------
# 各类智能体基本都按 OpenAI 的 {"error": {"message": ...}} 取错误信息；
# FastAPI 默认返回的是 {"detail": ...}，客户端会读不到原因、只显示"未知错误"。
# 所以 /v1/* 统一转成 OpenAI 格式，管理接口保持原样（前端依赖 detail）。
def _err_type(status: int) -> str:
    if status in (404, 410):
        return "not_found_error"
    if status == 429:
        return "rate_limit_error"
    if status >= 500:
        return "server_error"
    return "invalid_request_error"


class UpstreamAuthHTTPException(HTTPException):
    """muse.ai 会话失效（上游认证失败）：用 502 + 独立错误码，不能返回 401，
    否则下游 OpenAI 客户端与管理页都会误判成本服务的 API Key 错误。"""
    def __init__(self, detail: str):
        super().__init__(502, detail)


def _upstream_auth_error(exc: BaseException) -> HTTPException:
    if isinstance(exc, NoAccountError):
        return HTTPException(400, str(exc))
    return UpstreamAuthHTTPException(str(exc))


# 注册在 Starlette 的 HTTPException 上（FastAPI 的是它的子类）：路由 404 / 405、multipart 超限 400
# 这些框架层错误在 /v1/* 下也按 OpenAI 格式返回
@app.exception_handler(StarletteHTTPException)
async def _http_exc(request: Request, exc: StarletteHTTPException):
    if request.url.path.startswith("/v1/"):
        upstream_auth = isinstance(exc, UpstreamAuthHTTPException)
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": {"message": str(exc.detail),
                               "type": "upstream_auth_error" if upstream_auth
                               else _err_type(exc.status_code),
                               "param": None,
                               "code": "upstream_auth_error" if upstream_auth
                               else exc.status_code}},
            headers=getattr(exc, "headers", None))
    return JSONResponse(status_code=exc.status_code,
                        content={"detail": exc.detail},
                        headers=getattr(exc, "headers", None))


@app.exception_handler(RequestValidationError)
async def _validation_exc(request: Request, exc: RequestValidationError):
    if request.url.path.startswith("/v1/"):
        return JSONResponse(status_code=422, content={"error": {
            "message": "请求参数校验失败：" + str(exc.errors())[:400],
            "type": "invalid_request_error", "param": None, "code": 422}})
    # errors() 里可能带原始请求体 bytes（如 JSON 用错了 Content-Type），直接序列化会抛错变成 500
    return JSONResponse(status_code=422, content={"detail": jsonable_encoder(exc.errors())})

# created 是 OpenAI Model 对象的必填字段：严格反序列化的客户端（如 Rust 的 async-openai）缺了它会直接报错
_MODELS_CREATED = 1735689600
MODELS = [
    {"id": "muse-spark", "object": "model", "created": _MODELS_CREATED, "owned_by": "muse",
     "description": "Muse Spark —— 文本 / 代码对话（网页免费额度，支持流式）"},
    {"id": "muse-image", "object": "model", "created": _MODELS_CREATED, "owned_by": "muse",
     "description": "Muse Image —— 文生图 / 图像编辑（网页免费额度）"},
    {"id": "muse-video", "object": "model", "created": _MODELS_CREATED, "owned_by": "muse",
     "description": "Muse Video —— 文生视频 / 图生视频（网页免费额度）"},
]

# 下游（Codex / Cline / 各种客户端）习惯按 OpenAI、Anthropic 的名字传模型，
# 这里统一映射到 muse 的真实能力上。
#
# 说明：muse.ai 网页是自动路由的 agent，对外**没有可枚举的模型清单**，
# 能稳定调用的就是三条真实能力 —— Muse Spark（语言/代码）、
# Muse Image（生图）、Muse Video（生视频）。别名只是让下游不用改配置。
MODEL_ALIASES = {
    # ---- 文本 / 代码 → muse-spark ----
    "muse-text": "muse-spark", "muse-chat": "muse-spark", "muse-llm": "muse-spark",
    "koda": "muse-spark",
    "gpt-3.5-turbo": "muse-spark", "gpt-4": "muse-spark", "gpt-4-turbo": "muse-spark",
    "gpt-4o": "muse-spark", "gpt-4o-mini": "muse-spark", "gpt-4.1": "muse-spark",
    "gpt-4.1-mini": "muse-spark", "gpt-5": "muse-spark", "gpt-5-codex": "muse-spark",
    "o1": "muse-spark", "o1-mini": "muse-spark", "o3": "muse-spark",
    "o3-mini": "muse-spark", "o4-mini": "muse-spark",
    "codex": "muse-spark", "codex-mini-latest": "muse-spark",
    "claude-3-5-sonnet": "muse-spark", "claude-3-5-sonnet-latest": "muse-spark",
    "claude-3-7-sonnet": "muse-spark", "claude-sonnet-4": "muse-spark",
    "claude-opus-4": "muse-spark", "claude-3-opus": "muse-spark",
    "claude-3-haiku": "muse-spark",
    "deepseek-chat": "muse-spark", "deepseek-coder": "muse-spark",
    "deepseek-reasoner": "muse-spark", "qwen-coder": "muse-spark",
    "gemini-2.5-pro": "muse-spark", "gemini-2.5-flash": "muse-spark",
    # ---- 生图 → muse-image ----
    "muse-img": "muse-image", "dall-e": "muse-image", "dall-e-3": "muse-image",
    "gpt-image-1": "muse-image", "flux": "muse-image", "midjourney": "muse-image",
    # ---- 生视频 → muse-video ----
    "muse-vid": "muse-video", "muse-videos": "muse-video",
    "sora": "muse-video", "sora-2": "muse-video", "veo": "muse-video",
    "veo-3": "muse-video", "kling": "muse-video", "runway": "muse-video",
}


def resolve_model(name: str | None, default: str = "muse-image") -> str:
    n = (name or "").strip().lower()
    return MODEL_ALIASES.get(n, n or default)


# ------------------------- 鉴权 -------------------------
def auth(request: Request, authorization: str | None = Header(default=None)):
    """/v1/* 用 MUSE2API_KEY。配置了 MUSE2API_ADMIN_KEY 时，/admin/* 只认管理 Key：
    分发给下游客户端的 API Key 不能轮换 Key、删账号、升级或推送代码。"""
    return _check_key(authorization, admin=request.url.path.startswith("/admin"))


def _check_key(authorization: str | None, admin: bool) -> bool:
    admin_key = (getattr(CFG, "admin_key", "") or "").strip()
    expected = admin_key if (admin and admin_key) else CFG.api_key
    if not expected:
        return True
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "缺少 Authorization: Bearer <key>")
    parts = authorization.split(None, 1)
    token = parts[1].strip() if len(parts) > 1 else ""
    if not token or token != expected or token in _PLACEHOLDER_KEYS:
        raise HTTPException(401, "管理 Key 无效" if expected is admin_key and admin_key else "API key 无效")
    return True


# ------------------------- 请求模型 -------------------------

def _renew_and_persist(acc_id: str, wake_vm: bool = True, force: bool = False) -> dict | None:
    """调用 /api/session 续签账号 cookie 并写回 store，返回最新账号 dict。
    近期（10分钟内）已续签且状态正常的账号直接复用，避免每次请求阻塞 2~3 秒 HTTP 往返。"""
    acc = store.get_account(acc_id)
    if not acc or not acc.get("cookies"):
        return acc
    if getattr(CFG, "stealth", True):
        # 软隐身：主链路 /api/session 续签改由浏览器内同源 fetch 完成
        # （engine.ensure_page → _renew_in_page，TLS/代理与浏览器一致）；
        # cookie 刷新由生成后的 _sync_cookies 落盘，这里不再用 Python requests 命中 /api/session。
        return acc
    now = time.time()
    last_sync = max(
        int(acc.get("synced_at") or 0),
        int(acc.get("last_keepalive") or 0),
        int(engine._last_http_renew.get(acc_id, 0)),
    )
    if not force and acc.get("ok") is True and (now - last_sync) < 600:
        engine._last_http_renew[acc_id] = last_sync
        return acc
    try:
        res = engine.renew_session_http(acc["cookies"], acc.get("cookies_exp"),
                                        wake_vm=wake_vm, proxy=acc.get("proxy"))
        engine._last_http_renew[acc_id] = now
        if res.get("cookies"):
            store.update_account(acc_id, cookies=res["cookies"],
                                 cookies_exp=res.get("cookies_exp"),
                                 ok=True if res.get("ok") else acc.get("ok"),
                                 synced_at=int(now))
            store.touch_keepalive(acc_id, True, f"会话正常 (VM: {res.get('vm_state') or 'RUNNING'})")
    except MuseAuthError as exc:
        store.mark(acc_id, False, str(exc))
        raise
    except Exception as exc:
        log.warning("HTTP 预续签账号 %s 异常: %s", acc_id, exc)
    return store.get_account(acc_id)


# 后台 asyncio 任务（断连监视）的强引用：asyncio 只弱引用任务，不保存的话可能在执行中被回收。
_BG_TASKS: set = set()


def _start_bg_task(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)
    return task


def _stream_watch_cap(timeout: int) -> float:
    """断连监视的兜底上限：覆盖 worker 最坏情况的寿命——首次尝试（等槽位 + 等账号 + 生成，各至多
    timeout）加切号重试（等槽位 + 短等账号 + 生成）——再留余量。worker 结束（ctl.done）时监视提前退出，
    这个上限只防 worker 从未启动时监视任务常驻。"""
    return 5 * float(timeout) + SWITCH_WAIT + 120


async def _watch_disconnect(request: Request, ctl: _StreamCtl, cap: float) -> None:
    """流式断连监视：每 1s 查一次 request.is_disconnected()，断开即置位 ctl.stop_event。

    客户端断开后，StreamingResponse 不再迭代 sync_stream，它停在 yield 处既不结束也不被关闭
    （gc 之后也不会），生成器的 finally 指望不上，只能从外面察觉。worker 已结束（done）或已被
    叫停就退出；最长运行 cap 秒。"""
    deadline = time.monotonic() + max(1.0, float(cap))
    try:
        while time.monotonic() < deadline:
            if ctl.stop_event.is_set() or ctl.done.is_set():
                return
            try:
                gone = await request.is_disconnected()
            except Exception as exc:  # noqa: BLE001 - 查询失败不影响主流程，写队列看门狗兜底
                log.debug("流式断连检测失败: %s", exc)
                gone = False
            if gone:
                log.info("流式客户端已断开连接：停止生成，释放浏览器槽位与账号")
                ctl.stop_event.set()
                return
            await asyncio.sleep(1.0)
    finally:
        if not ctl.started:
            ctl.release()  # 流还没开始就结束（如开流前断开）：worker 不会再来释放排队名额


def _pull_tracked(gen, ctl: _StreamCtl):
    """包住流式端点的 sync_stream：每次被 ASGI 取数（从 yield 恢复）都 ctl.touch()，
    写队列看门狗据此判断下游是否还在取数；结束或被关闭时置位 stop_event
    （只是正常结束时的附加保障，断连检测靠 _watch_disconnect）。"""
    try:
        for item in gen:
            yield item
            ctl.touch()
    finally:
        ctl.stop_event.set()
        gen.close()
        if not ctl.started:
            ctl.release()


def safe_chat_stream(prompt: str, timeout: int, account_id: str | None = None,
                     preferred_id: str | None = None, ctl: _StreamCtl | None = None):
    """在独立线程中执行 chat_stream 并持有 GEN_LOCK，通过 Queue 往外吐。
    若首字前遇到单账号 VM 卡死或会话异常，自动切换下一个健康账号重试一次：重试经 exclude_ids 排除
    刚失败的账号；没有其它可切换的账号（只剩它自己或只剩 ok=False 的账号），或第一次尝试没租到账号
    就出错时，不重试，直接返回第一次的错误。

    账号由 worker 拿到 GEN_LOCK 槽位后经 ACCOUNT_POOL 租用，cookie 与有效期取自租到的账号：
    - 默认与生图链路一致，按 preferred_id 偏好与 rotate 轮转选号，已达并发上限的忙碌账号自动跳过；
      所有账号都忙时在 timeout 内排队等任一账号释放；
    - 只有调用方显式传 account_id 时才按指定账号等待。

    账号状态只由 worker 写，且只写本次尝试实际租到的账号（端点不按入口账号写）：成功记
    mark(True, "")；认证失效记 mark(False, 原因)；生成失败交给 _record_generation_failure；
    acquire 在租到账号之前抛出的错误不写任何账号。

    资源释放不依赖本生成器被关闭（客户端断开后它可能永远停在 yield 处）：
    - 流式端点传入 ctl，客户端断开时由 _watch_disconnect 置位 ctl.stop_event；
    - worker 写队列带超时，每 0.5s 检查一次 stop_event；队列已满且下游超过
      STREAM_STALL_SECONDS 没有取数（以 ctl.last_pull 为准）时，判定下游停止读取；
    - 等 GEN_LOCK 槽位与等账号都分片进行，并检查 stop_event。
    叫停后 worker 只做清理（关页、释放账号与槽位），不写账号状态。非流式调用不传 ctl，内部新建。"""
    import queue
    ctl = ctl or _StreamCtl()
    ctl.started = True
    stop_event = ctl.stop_event
    q = queue.Queue(maxsize=100)

    def _put(item) -> None:
        while True:
            if stop_event.is_set():
                raise _ClientGone()
            try:
                q.put(item, timeout=0.5)
                return
            except queue.Full:
                idle = time.monotonic() - ctl.last_pull
                if idle > STREAM_STALL_SECONDS:
                    log.warning("流式下游已 %.0fs 未取数且队列已满，判定客户端停止读取，终止生成", idle)
                    stop_event.set()
                    raise _ClientGone() from None

    def worker():
        cur_id = account_id
        last_exc = None
        failed_id = None  # 上一次尝试实际租到、首字前出错的账号；切号重试经 exclude_ids 避开它
        try:
            for attempt in range(2):
                if stop_event.is_set():
                    return
                if attempt > 0 and not failed_id:
                    break  # 第一次尝试没租到账号就出错（如等号超时）：没有要切走的账号，不重试
                # 统一加锁顺序：先占 GEN_LOCK 槽位，再占账号（与生图/视频一致，避免相向死锁）。
                # 切号重试只短等槽位：等不到就返回第一次的真实错误，不再把总耗时拖到 2×timeout
                if not _acquire_slot(timeout if attempt == 0 else min(timeout, SWITCH_WAIT), stop_event):
                    if last_exc is None:
                        last_exc = MuseGenerationError("等待浏览器队列超时，请稍后重试")
                    break
                leased_id = None
                yielded = False
                try:
                    if stop_event.is_set():
                        return
                    # 账号获取纳入 try：等号超时会以错误上抛，不再被吞成空的正常结束
                    if attempt > 0:
                        # 切号重试：排除刚失败的账号；没有其它可切换的账号时立即抛 NoAlternativeAccount。
                        # 只短暂等其它账号空出来（占着槽位），等不到就返回第一次的真实错误
                        try:
                            acc = ACCOUNT_POOL.acquire(rotate=True, force_rotate=True, exclude_ids={failed_id},
                                                       timeout=min(timeout, SWITCH_WAIT), stop_event=stop_event)
                        except _ClientGone:
                            raise
                        except (MuseGenerationError, MuseAuthError):
                            break
                    elif account_id:
                        # 调用方显式指定账号时才按该账号等待（目前没有调用方，保留这个能力）
                        acc = ACCOUNT_POOL.acquire(account_id=account_id, timeout=timeout,
                                                   stop_event=stop_event)
                    else:
                        # 与生图链路一致：已达并发上限的账号进 blocked、经 exclude_ids 跳过，
                        # 不会占着槽位空等某个忙碌账号；全部账号都忙时才排队等任一账号释放
                        acc = ACCOUNT_POOL.acquire(preferred_id=preferred_id, rotate=True,
                                                   timeout=timeout, stop_event=stop_event)
                    leased_id = acc["id"]
                    cur_id = acc["id"]
                    engine.loaded_sess = None  # 本次尝试的会话由 ensure_page 记下，切号重试不沿用上一次的
                    cur_cookies = acc["cookies"]
                    cur_exp = acc.get("cookies_exp")
                    engine.set_account_meta(cur_id, acc.get("proxy"),
                                            acc.get("timezone"), acc.get("lang"))
                    if attempt > 0:
                        log.info("【对话自动切号】切换到账号 %s (%s) 重试...", acc.get("label"), cur_id)
                    refreshed = _renew_and_persist(cur_id, wake_vm=True, force=(attempt > 0))
                    if refreshed:
                        cur_cookies = refreshed["cookies"]
                        cur_exp = refreshed.get("cookies_exp")
                    engine.start()
                    if stop_event.is_set():
                        return
                    for chunk in engine.chat_stream(
                        cur_cookies, prompt, cur_exp, timeout,
                        account_id=cur_id, stop_event=stop_event
                    ):
                        yielded = True
                        _put(("data", chunk))
                        if stop_event.is_set():
                            return
                    if stop_event.is_set():
                        return  # 引擎看到 stop_event 提前收尾：不算成功，也不写账号状态
                    ctl.final_text = getattr(engine, "last_chat_text", None)
                    if not _session_replaced(cur_id):  # 旧会话成功说明不了新换上的会话
                        store.mark(cur_id, True, "")
                    _sync_cookies(cur_id)
                    return
                except _ClientGone:
                    return
                except NoAlternativeAccount:
                    # 没有其它账号可切换：不以切号名义重试同一账号，last_exc 保持第一次的错误。
                    # 这次没租到账号，不写任何账号状态
                    break
                # 出错时只写本次尝试实际租到的账号（leased_id）。acquire 在租到账号之前抛出的错误
                # （如"没有可用账号"、等号超时）与上一次尝试的账号无关，不能覆盖它真实的失败原因。
                except MuseAuthError as exc:
                    last_exc = exc
                    if leased_id:
                        _mark_auth_failed(leased_id, exc)
                    if yielded:
                        break
                except Exception as exc:
                    if stop_event.is_set():
                        return  # 叫停后引擎抛出的错误（如"客户端已断开连接"）不是账号故障，也不重试
                    if not isinstance(exc, MuseGenerationError):
                        # 未包装的底层异常（CDPConnectionError、TimeoutError…）：统一成生成失败，
                        # 账号留下记录，端点也能按 502 + OpenAI 错误格式返回
                        exc = InternalGenerationError(f"{exc}")
                    last_exc = exc
                    if leased_id:
                        _record_generation_failure(leased_id, exc)
                    # 不 reset_thread：这个页面马上会被 finally 关掉，导航回 /thread/new 只会白占 10 多秒
                    engine.close_current_page()
                    if yielded or isinstance(exc, (MuseSubmittedError, MuseQuotaError, QuotaPausedError)):
                        break  # 已出字，或助手已受理（已计费）：不换号重发
                finally:
                    engine.close_current_page()
                    if leased_id:
                        ACCOUNT_POOL.release(leased_id)
                    GEN_LOCK.release()
                failed_id = leased_id  # 首字前出错、准备重试：记下这次实际租到的账号（没租到为 None）
            if last_exc is not None:
                _put(("error", last_exc))
        except _ClientGone:
            pass  # 等槽位或回传错误时下游已离开：没占着资源，也不写账号状态
        finally:
            ctl.done.set()
            ctl.release()
            try:
                q.put_nowait(("done", None))
            except queue.Full:
                pass  # 队列满时消费侧靠 ctl.done 判断结束

    threading.Thread(target=worker, daemon=True).start()

    try:
        while True:
            ctl.touch()
            try:
                kind, val = q.get(timeout=0.5)
            except queue.Empty:
                if stop_event.is_set():
                    raise _ClientGone() from None
                if ctl.done.is_set() and q.empty():
                    break
                continue
            if kind == "data":
                yield val
            elif kind == "error":
                raise val
            else:
                break
    finally:
        stop_event.set()


class ImageRequest(BaseModel):
    prompt: str
    model: str = "muse-image"
    n: int = 1
    size: str | None = None
    aspect_ratio: str | None = None
    response_format: str = "url"      # url | b64_json
    timeout: int | None = Field(default=None, ge=1, le=600)
    extra: str | None = None
    image: Any = None
    images: list | None = None
    image_url: Any = None             # 与 edits / 视频接口一致的旧版单图字段
    reference_image: str | None = None
    async_: bool = Field(False, alias="async")


class VideoRequest(BaseModel):
    prompt: str
    model: str = "muse-video"
    duration: int | None = Field(default=None, ge=1, le=30)   # 秒；写进提示词，不能为 0 或负数
    size: str | None = None
    aspect_ratio: str | None = None
    resolution: str | None = None
    timeout: int | None = Field(default=None, ge=1, le=3600)
    extra: str | None = None
    image: Any = None
    images: list | None = None
    image_url: Any = None
    reference_image: str | None = None


class ChatMessage(BaseModel):
    role: str
    content: str | list | None = None
    name: str | None = None
    tool_call_id: str | None = None
    tool_calls: list | None = None


class ChatRequest(BaseModel):
    """OpenAI Chat Completions 请求。

    对第三方客户端要尽量宽松：不认识字段一律忽略（Pydantic 默认行为），
    只挑我们真正用得上的读 —— 否则各种智能体各传各的参数就会 422。
    """
    model: str = "muse-spark"
    messages: list[ChatMessage] = []
    stream: bool = False
    temperature: float | None = None
    max_tokens: int | None = None
    timeout: int | None = Field(default=None, ge=1, le=3600)
    prompt: str | None = None       # 兼容把 prompt 直接放顶层的客户端
    # 下面这些声明出来只是为了「能读到」，muse.ai 端不做对应处理
    tools: list | None = None
    tool_choice: object | None = None
    response_format: object | None = None
    stream_options: object | None = None


class ResponsesRequest(BaseModel):
    """OpenAI Responses API（新版 Codex 默认走这个）。"""
    model: str = "muse-spark"
    input: str | list | None = None
    instructions: str | None = None
    stream: bool = False
    max_output_tokens: int | None = None
    timeout: int | None = Field(default=None, ge=1, le=3600)
    tools: list | None = None
    store: bool | None = None


class AccountRequest(BaseModel):
    label: str = ""
    cookies: dict[str, str] = Field(default_factory=dict)
    cookie_header: str | None = None
    batch: str | None = None          # 多行文本，每行一个账号
    expires: dict[str, int] = Field(default_factory=dict)   # cookie 名 -> 过期时间戳
    # 软隐身：每账号独立代理（socks5:// 或 http(s)://，可带账密）、时区、语言
    proxy: str | None = None
    timezone: str | None = None
    lang: str | None = None


class AccountPatch(BaseModel):
    label: str | None = None
    enabled: bool | None = None
    proxy: str | None = None
    timezone: str | None = None
    lang: str | None = None


# ------------------------- 提示词构造 -------------------------
def _image_value(value: Any) -> str | None:
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, dict):
        candidate = value.get("image_url") or value.get("url") or value.get("b64_json")
        if isinstance(candidate, dict):
            candidate = candidate.get("url")
        return candidate.strip() if isinstance(candidate, str) and candidate.strip() else None
    return None


def _reference_list(images: list | None, legacy: list[Any], field: str) -> list[str]:
    """Validate ordered image references and make legacy precedence explicit."""
    # 部分客户端把多张图塞进 image / image_url 数组：按 images 处理；
    # 认不出的非空值一律报 400，不能悄悄丢掉参考图、退化成文生图
    listed = [v for v in legacy if isinstance(v, list)]
    if listed:
        if images or len(listed) > 1 or any(_image_value(v) for v in legacy if not isinstance(v, list)):
            raise HTTPException(400, f"{field} 不能与旧版单图字段同时使用")
        images = listed[0]
        legacy = []
    for value in legacy:
        if value not in (None, "", {}) and not _image_value(value):
            raise HTTPException(400, "image / image_url / reference_image 不是有效图片引用")
    primary = next((ref for value in legacy if (ref := _image_value(value))), None)
    if primary and images:
        raise HTTPException(400, f"{field} 不能与旧版单图字段同时使用")
    if primary:
        return [primary]
    if images is None:
        return []
    if len(images) > MAX_REFERENCE_IMAGES:
        raise HTTPException(400, f"{field} 最多 {MAX_REFERENCE_IMAGES} 张参考图")
    refs = []
    for index, value in enumerate(images):
        ref = _image_value(value)
        if not ref:
            raise HTTPException(400, f"{field}[{index}] 不是有效图片引用")
        refs.append(ref)
    return refs


def _image_references(req: ImageRequest) -> list[str]:
    return _reference_list(req.images, [req.reference_image, req.image_url, req.image], "images")


def _video_references(req: VideoRequest) -> list[str]:
    return _reference_list(
        req.images, [req.reference_image, req.image_url, req.image], "images")

def build_image_prompt(r: ImageRequest) -> str:
    has_ref = bool(_image_references(r))
    if has_ref:
        parts = [f"基于我本次上传附带的参考图片进行生图/编辑：{r.prompt.strip()}"]
    else:
        parts = [f"全新文生图创作（当前未提供任何参考图，请勿查找历史相册或向用户索要原图，直接根据文字描述从零绘制生成一张全新图片）：{r.prompt.strip()}"]
    ar = (r.aspect_ratio or "").strip().lower()
    sz = (r.size or "").strip().lower()

    if any(k in ar or k in sz for k in ("9:16", "9/16", "portrait", "竖屏", "720x1280", "1080x1920")):
        parts.append("【画面构图与比例要求】：严格 9:16 竖屏满屏画幅（9:16 vertical portrait aspect ratio，高大于宽的手机全屏竖版画面），绝对不要生成横屏，保持垂直构图")
    elif any(k in ar or k in sz for k in ("16:9", "16/9", "landscape", "横屏", "1280x720", "1920x1080")):
        parts.append("【画面构图与比例要求】：16:9 宽屏横屏画幅（16:9 widescreen landscape aspect ratio）")
    elif any(k in ar or k in sz for k in ("1:1", "square", "正方形", "1024x1024")):
        parts.append("【画面构图与比例要求】：1:1 正方形画幅（1:1 square aspect ratio）")
    elif any(k in ar or k in sz for k in ("4:3", "4/3")):
        parts.append("【画面构图与比例要求】：4:3 比例画幅")
    elif any(k in ar or k in sz for k in ("3:4", "3/4")):
        parts.append("【画面构图与比例要求】：3:4 竖向画幅")
    elif r.aspect_ratio:
        parts.append(f"【画面构图与比例要求】：{r.aspect_ratio} 画面比例")
    elif r.size:
        parts.append(f"尺寸/比例：{r.size}")

    if has_ref:
        parts.append("【纯净画面要求】：彻底清除并去除参考图中的所有文字、水印、签名、角标及Logo标记（clean image without any watermark, text, or logo），输出绝对纯净无字画面")

    if r.extra:
        parts.append(r.extra)
    return "，".join(parts)


def _content_text(content) -> str:
    """把 OpenAI 的 content 归一成纯文本（兼容多模态 list 形式）。"""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                t = item.get("type")
                if t in (None, "text", "input_text", "output_text"):
                    parts.append(str(item.get("text") or ""))
                elif t == "image_url":
                    parts.append("[图片]")
            elif isinstance(item, str):
                parts.append(item)
        return "\n".join(p for p in parts if p)
    return str(content)


def build_chat_prompt(messages: list[ChatMessage]) -> str:
    """把 messages 拼成发给 muse.ai 的一段提示词。

    muse.ai 网页本身是个带上下文的会话，但本 API 是无状态的（每次可能落到
    不同账号/页面），所以把历史拼进 prompt 最可控 —— 这正好匹配 Codex 这类
    「每轮都带全量历史」的客户端。
    """
    system, turns = [], []
    for m in messages:
        role = (m.role or "").strip().lower()
        text = _content_text(m.content).strip()
        if role == "tool":
            # 工具执行结果 → 当成"用户提供的信息"发过去
            if text:
                turns.append(("user", "【工具执行结果】\n" + text))
            continue
        if not text:
            # 部分客户端的 assistant 消息只带 tool_calls、没有正文
            if role == "assistant" and m.tool_calls:
                turns.append(("assistant", "【请求调用工具】" + json.dumps(
                    m.tool_calls, ensure_ascii=False)[:600]))
            continue
        if role in ("system", "developer"):
            system.append(text)
        else:
            turns.append((role, text))

    # 单轮且无系统指令 → 直接发原文，最贴近自然对话
    if len(turns) == 1 and not system and turns[0][0] == "user":
        return turns[0][1]

    parts = []
    if system:
        sys_text = "\n\n".join(system)
        sys_text = sys_text.replace("danger-full-access", "standard-workspace-access")
        parts.append(f"背景与任务设定：\n{sys_text}")
    for role, text in turns:
        label = "助手" if role == "assistant" else "用户"
        parts.append(f"{label}：\n{text}")
    return "\n\n".join(parts)


# ------------------------- 工具调用（function calling）适配 -------------------------
# muse.ai 的网页模型**不会**返回结构化的 tool_calls，所以这里做一层协议适配：
#   1) 请求带 tools 时，把工具定义翻译成提示词里的【工具调用协议】；
#   2) 模型按协议输出 ```json {"tool": "...", "arguments": {...}} ```；
#   3) 我们把这段解析回 OpenAI 的 tool_calls 交给下游 agent。
#
# 注意：这是"尽力适配"而非保证 —— 目标模型是通用对话模型，没有针对
# function calling 做微调，遵守协议的程度需要实测观察。
_TOOL_PROTOCOL_HEAD = """你可以根据需要调用以下工具来协助用户完成任务。
若需调用工具，请直接输出如下格式的 JSON 代码块（不要包含其他多余解释）：
```json
{"name": "<工具名>", "arguments": {<参数>}}
```
如果需要调用多个工具，请输出包含多个对象的 JSON 数组。
如果无需调用工具，请直接用自然语言回答。

可用工具列表：
"""


def _describe_params(params) -> str:
    """把 JSON Schema 的参数描述成易读的多行文本。"""
    if not isinstance(params, dict):
        return "      （无参数）"
    props = params.get("properties") or {}
    required = set(params.get("required") or [])
    if not props:
        return "      （无参数）"
    lines = []
    for name, spec in props.items():
        spec = spec if isinstance(spec, dict) else {}
        lines.append("      - %s (%s, %s)%s" % (
            name, spec.get("type") or "any",
            "必填" if name in required else "可选",
            (" " + spec["description"]) if spec.get("description") else ""))
    return "\n".join(lines)


def build_tools_prompt(tools: list | None) -> str:
    """把 tools 定义翻译成提示词片段（没有工具时返回空串）。

    同时兼容 Chat Completions 的 `{"type":"function","function":{...}}`
    和 Responses API 的 `{"type":"function","name":...,"parameters":...}`。
    """
    if not tools:
        return ""
    items = []
    for t in tools:
        if not isinstance(t, dict):
            continue
        fn = t.get("function") if isinstance(t.get("function"), dict) else t
        if not fn.get("name"):
            continue
        desc = fn.get("description") or ""
        items.append("%d. %s%s\n   参数：\n%s" % (
            len(items) + 1, fn["name"], (" — " + desc) if desc else "",
            _describe_params(fn.get("parameters"))))
    if not items:
        return ""
    return _TOOL_PROTOCOL_HEAD + "\n".join(items) + "\n"


_FENCE_RE = re.compile(r"```(?:json|JSON)?\s*([\s\S]*?)```")


def _as_tool_calls(obj) -> list[dict] | None:
    """把解析出的 JSON 转成 OpenAI tool_calls；不像工具调用就返回 None。"""
    raw = obj if isinstance(obj, list) else [obj]
    if not raw:
        return None
    out = []
    for item in raw:
        if not isinstance(item, dict):
            return None
        name = item.get("tool") or item.get("name") or item.get("function")
        if not isinstance(name, str) or not name:
            return None
        args = item.get("arguments")
        if args is None:
            args = item.get("parameters") or item.get("args") or {}
        if not isinstance(args, str):
            args = json.dumps(args, ensure_ascii=False)
        out.append({"id": "call_" + uuid.uuid4().hex[:20], "type": "function",
                    "function": {"name": name, "arguments": args}})
    return out or None


def parse_tool_calls(text: str) -> tuple[list[dict] | None, str]:
    """从模型输出里抽出工具调用。

    返回 (tool_calls, 剩余文本)；抽不到就返回 (None, 原文)。
    支持多种形态：
    1) ```json ... ``` 代码块；
    2) 裸 JSON（或带有前导 json/JSON 关键字）；
    3) 文本中内嵌的完整 JSON 对象或数组。
    """
    if not text:
        return None, text
    for m in reversed(list(_FENCE_RE.finditer(text))):
        try:
            calls = _as_tool_calls(json.loads(m.group(1).strip()))
        except Exception:
            continue
        if calls:
            return calls, (text[:m.start()] + text[m.end():]).strip()

    stripped = text.strip()
    clean_stripped = re.sub(r"^(?:```)?(?:json|JSON)?\s*", "", stripped).rstrip("`").strip()
    if clean_stripped.startswith(("{", "[")):
        try:
            calls = _as_tool_calls(json.loads(clean_stripped))
            if calls:
                return calls, ""
        except Exception:
            pass

    m_json = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", text)
    if m_json:
        try:
            calls = _as_tool_calls(json.loads(m_json.group(1).strip()))
            if calls:
                rest = (text[:m_json.start()] + text[m_json.end():]).strip()
                if re.sub(r"^(?:```)?(?:json|JSON)?\s*", "", rest).strip("`").strip() == "":
                    rest = ""
                return calls, rest
        except Exception:
            pass

    return None, text


def build_video_prompt(r: VideoRequest) -> str:
    user_prompt = r.prompt.strip()
    ar = (r.aspect_ratio or "").strip().lower()
    sz = (r.size or "").strip().lower()
    dur = r.duration or 6
    refs = _video_references(r)
    has_ref = bool(refs)
    reference_instruction = (
        f"基于我本次上传的 {len(refs)} 张参考图片生成视频（以第一张作为主要起始画面，"
        "其余图片作为主体、场景与风格参考，严禁使用历史图片或其他图像）"
        if len(refs) > 1 else
        "基于我本次上传的参考图片附件作为第一帧参考图（严禁使用历史图片或任何其他图像，"
        "必须严格以我当前刚刚上传并附带在此处的这张图片为起始帧）"
    )

    is_vertical = any(k in ar or k in sz for k in ("9:16", "9/16", "portrait", "竖屏", "720x1280", "1080x1920"))

    parts = []
    if is_vertical:
        if has_ref:
            parts.append(f"{reference_instruction}：生成一个严格9:16竖屏手机满屏的动态图生视频（9:16 vertical portrait video，720x1280，高大于宽的手机全屏竖版画面，严禁生成横屏或黑边，时长严格为 {dur} 秒）：{user_prompt}")
        else:
            parts.append(f"全新文生视频创作（纯文本全新生成，严禁参考任何历史图片或上下文）：生成一个严格9:16竖屏手机满屏视频（9:16 vertical portrait video，720x1280，高大于宽的手机全屏竖版画面，严禁生成横屏或带有左右黑边，保持垂直构图，时长严格为 {dur} 秒）：{user_prompt}")
    elif any(k in ar or k in sz for k in ("16:9", "16/9", "landscape", "横屏", "1280x720", "1920x1080")):
        if has_ref:
            parts.append(f"{reference_instruction}：生成一个16:9宽屏横屏图生视频（16:9 widescreen landscape video，时长严格为 {dur} 秒）：{user_prompt}")
        else:
            parts.append(f"全新文生视频创作（纯文本全新生成，严禁参考任何历史图片或上下文）：生成一个16:9横屏宽屏视频（16:9 widescreen landscape video，时长严格为 {dur} 秒）：{user_prompt}")
    else:
        if has_ref:
            parts.append(f"{reference_instruction}：生成动态图生视频（时长严格为 {dur} 秒）：{user_prompt}")
        else:
            parts.append(f"全新文生视频创作（纯文本全新生成，严禁参考任何历史图片或上下文）：生成一个视频（时长严格为 {dur} 秒）：{user_prompt}")

    if r.resolution:
        parts.append(f"画质规格：{r.resolution}")
    if r.extra:
        parts.append(r.extra)
    return "，".join(parts)


def media_url(name: str) -> str:
    """媒体地址。

    返回**绝对 URL** —— OpenAI 兼容客户端（以及各类智能体平台）拿到 data[].url 后
    一般会直接渲染或下载，相对路径会被解析到客户端自己的域名上，导致 404。
    优先用 public_base；没配时用本次请求的来源地址（见 _request_base_mw）；
    都没有（后台线程里写任务记录）时退回相对路径，响应时再由 absolute_url 补全。
    """
    return absolute_url(f"/v1/media/{name}")


def absolute_url(url: str | None) -> str | None:
    """把站内相对路径（/v1/...）补成绝对 URL；已是绝对 URL 或没有可用基址时原样返回。"""
    if not url or not url.startswith("/"):
        return url
    base = _public_base() or _REQUEST_BASE.get()
    return f"{base}{url}" if base else url


# ------------------------- cookie 解析 -------------------------
def parse_cookie_text(text: str) -> dict[str, str]:
    """把 `a=1; b=2` / JSON / Set-Cookie 行解析成 dict。"""
    text = (text or "").strip()
    if not text:
        return {}
    if text.startswith("{"):
        import json
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                return {str(k): str(v) for k, v in obj.items()}
        except Exception:  # noqa: BLE001
            pass
    out: dict[str, str] = {}
    for part in re.split(r"[;\n]+", text):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = k.strip()
        if k:
            out[k] = v.strip()
    return out


def parse_batch(text: str) -> list[tuple[str, dict]]:
    """批量导入：每行 `标签 | cookie串`，标签可省略。

    每行都只有一个 `name=value`（没有 `;`、没有标签）时，是「每行一个 cookie」的单账号格式
    （DevTools 复制出来就是这样）：整段当成一个账号解析，不拆成一堆各缺核心项的残缺账号。"""
    lines = [ln.strip() for ln in (text or "").splitlines()
             if ln.strip() and not ln.strip().startswith("#")]
    if len(lines) > 1 and all(";" not in ln and "|" not in ln and "=" in ln for ln in lines):
        cookies = parse_cookie_text("\n".join(lines))
        return [("", cookies)] if cookies else []
    out: list[tuple[str, dict]] = []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        label = ""
        body = line
        if "|" in line:
            head, tail = line.split("|", 1)
            if "=" not in head:          # `|` 出现在 cookie 里时不算分隔符
                label, body = head.strip(), tail.strip()
        cookies = parse_cookie_text(body)
        if cookies:
            out.append((label, cookies))
    return out


# ------------------------- 核心生成 -------------------------
def _sync_cookies(acc_id: str) -> dict:
    """生成完从浏览器读回 cookie 并写回账号池。

    实测结论：**核心 cookie 的 expires 不会被使用行为续期**（hatch_vml 固定
    约 2 天就到期），这里同步回来的主要是那些每次访问都会重新下发的非核心
    cookie（_fbp / wd / dpr 等）以及可能新增的条目，避免账号信息比实际更旧。

    注意：这个函数**绝不能把生成结果搞失败** —— 生成已经成功了，
    同步只是锦上添花，出任何问题都只记一条日志。
    """
    try:
        live = engine.read_cookies()
        if not live:
            return {"synced": 0}
        acc = store.get_account(acc_id)
        if not acc:
            return {"synced": 0}
        cur = dict(acc.get("cookies") or {})
        new_vals = {k: v["value"] for k, v in live.items() if v.get("value")}
        stored_sess = cur.get("hatch_sess")
        loaded_sess = getattr(engine, "loaded_sess", None)
        if stored_sess and loaded_sess and stored_sess != loaded_sess:
            # 这次生成期间管理员已换上新会话（更新 Cookie）：浏览器里是旧会话，不能写回覆盖
            log.info("账号 %s 的会话在生成期间已被替换，跳过 cookie 回写", acc_id)
            return {"synced": 0, "skipped": "会话已在生成期间被替换"}
        # 有效期要「合并」而不是「覆盖」：会话 cookie 读回的 expires 是 -1，
        # 若整体覆盖会把原有有效期记录清空（实测踩过这个坑）。
        exps = dict(acc.get("cookies_exp") or {})
        exps.update({k: v["expires"] for k, v in live.items()
                     if _pos(v.get("expires"))})
        changed = sum(1 for k, v in new_vals.items() if cur.get(k) != v)
        store.update_account(acc_id, cookies={**cur, **new_vals},
                             cookies_exp=exps, synced_at=int(time.time()))
        if new_vals.get("hatch_sess"):
            # 站点续签换了 hatch_sess：本页面之后的回写以新值为准，不能再被当成「会话已被替换」跳过
            engine.loaded_sess = new_vals["hatch_sess"]
        updated = store.get_account(acc_id) or {}
        return {"synced": len(new_vals), "changed": changed,
                "expires_at": updated.get("expires_at")}
    except Exception as exc:  # noqa: BLE001
        log.warning("cookie 同步失败（不影响本次生成）: %s", exc)
        return {"synced": 0, "error": str(exc)[:200]}


engine.on_renewed = _sync_cookies


def _pos(v) -> bool:
    try:
        return int(v) > 0
    except (TypeError, ValueError):
        return False


def _session_replaced(acc_id: str, loaded_sess: str | None = None) -> bool:
    """本线程页面加载的 hatch_sess（或调用方传入的、本次实际使用的 hatch_sess）与账号当前存的不一致：
    生成、保活或探测期间管理员已换上新会话（更新 Cookie）。
    这时旧会话的结果（登出、额度、有效）说明不了新会话，不能写到账号上。"""
    acc = store.get_account(acc_id) or {}
    stored_sess = (acc.get("cookies") or {}).get("hatch_sess")
    if loaded_sess is None:
        loaded_sess = getattr(engine, "loaded_sess", None)
    if stored_sess and loaded_sess and stored_sess != loaded_sess:
        log.info("账号 %s 的会话在生成期间已被替换，旧会话的失败不写入账号状态", acc_id)
        return True
    return False


def _mark_auth_failed(acc_id: str, exc: BaseException) -> None:
    """认证失效写 ok=False；会话已在生成期间被替换时不写（见 _session_replaced）。"""
    if acc_id and not _session_replaced(acc_id):
        store.mark(acc_id, False, str(exc))


def _record_generation_failure(acc_id: str, exc: BaseException) -> None:
    """生成失败时按失败原因写账号状态（M2 / 2.20）。

    只用于租到账号之后由引擎抛出的 MuseGenerationError，且只写实际执行生成的账号；
    ACCOUNT_POOL.acquire 在租到账号之前抛出的错误（如等号超时）与任何账号无关，不要传进来。
    - MuseModelRefusalError：模型拒绝生成，与账号无关，维持现状 mark(ok=True, "任务异常: …")；
    - 其它 MuseGenerationError（超时、页面加载失败、未送达等偶发失败）：只更新备注与 checked_at，
      ok 保持原值，也不暂停调度（3.35）。
    """
    if not acc_id:
        return
    note = f"任务异常: {str(exc)[:60]}"
    if isinstance(exc, MuseQuotaError) and _session_replaced(acc_id):
        return  # 旧会话的额度提示说明不了新换上的会话：不暂停调度
    if isinstance(exc, MuseQuotaError):
        # 额度耗尽：暂停调度一段时间，后续请求不再分给它（本次不换号重发，见 _run_generation_locked）
        store.block_quota(acc_id, note=f"额度耗尽，暂停调度 6 小时: {str(exc)[:60]}")
    elif isinstance(exc, MuseModelRefusalError):
        store.mark(acc_id, True, note)
    else:
        store.note(acc_id, note)


def _run_generation(prompt: str, kind: str, timeout: int,
                    account_id: str | None = None, on_progress=None,
                    reference_image: str | None = None,
                    reference_images: list[str] | None = None,
                    on_acquired=None,
                    queue_timeout: float | None = None,
                    stop_event: threading.Event | None = None) -> tuple[dict, str | None]:
    """queue_timeout 为 None（同步接口）：排队与生成共用 timeout 这一个总时限。
    给了 queue_timeout（异步任务）：排队（等槽位、等账号）最多 queue_timeout 秒，
    租到账号之后生成再单独拥有完整的 timeout，排队时间不挤占生成时间。"""
    # Browser ownership covers account selection, retry and cleanup, not just generate().
    wait = max(1, timeout if queue_timeout is None else queue_timeout)
    deadline = time.monotonic() + wait
    # 有 stop_event（同步接口）时分片等槽位并检查断连：客户端走了就不再占着排队位置空等
    got = _acquire_slot(wait, stop_event) if stop_event is not None else GEN_LOCK.acquire(timeout=wait)
    if not got:
        raise MuseGenerationError("等待浏览器队列超时，请稍后重试")
    try:
        if on_acquired:
            try:
                on_acquired()
            except Exception:
                pass
        if stop_event is not None and stop_event.is_set():
            raise _ClientGone()
        return _run_generation_locked(prompt, kind, timeout, account_id,
                                      on_progress, reference_image, reference_images,
                                      deadline=deadline,
                                      restart_deadline=queue_timeout is not None,
                                      stop_event=stop_event)
    finally:
        GEN_LOCK.release()


def _run_generation_locked(prompt: str, kind: str, timeout: int,
                    account_id: str | None = None, on_progress=None,
                    reference_image: str | None = None,
                    reference_images: list[str] | None = None,
                    deadline=None, restart_deadline: bool = False,
                    stop_event: threading.Event | None = None) -> tuple[dict, str | None]:
    remaining_wait = int(deadline - time.monotonic()) if deadline is not None else timeout
    # 页面与隔离上下文按线程持有、每次用完即关，没有可复用的热页；默认按 LRU 轮转选号。
    preferred_id = None
    if kind == "video" and not account_id:
        # Prefer the last account that completed a video; session health alone is insufficient.
        recent = sorted(list(store.tasks.values()), key=lambda task: task.get("created_at", 0), reverse=True)
        for task in recent:
            if task.get("kind") != "video" or task.get("status") != "completed" or not task.get("account"):
                continue
            candidate = store.get_account(task["account"])
            if (candidate and candidate.get("enabled", True) and candidate.get("ok") is not False
                    and (candidate.get("quota_blocked_until") or 0) <= time.time()):
                preferred_id = candidate["id"]
                break
    acc = ACCOUNT_POOL.acquire(account_id=account_id,
                               preferred_id=preferred_id,
                               timeout=max(1, remaining_wait), stop_event=stop_event)
    if restart_deadline:
        deadline = time.monotonic() + max(1, timeout)
    cur_acc = acc
    last_exc = None
    min_send = min(MIN_GENERATION_SECONDS, max(1, timeout // 2))
    try:
        for attempt in range(2):
            if deadline is not None and time.monotonic() >= deadline:
                # 时限到了：返回上一次的真实错误（类型决定 HTTP 状态与错误码），没有才报时限
                raise last_exc or MuseGenerationError("任务总等待时限已到，停止重试")
            if attempt > 0:
                # 拒答、额度耗尽、成品已生成只是取回失败：都已计费或换号无益，不换账号重发
                if isinstance(last_exc, (MuseModelRefusalError, MuseQuotaError, MuseSubmittedError)):
                    break
                old_id = cur_acc["id"] if cur_acc else None
                if old_id:
                    ACCOUNT_POOL.release(old_id)
                cur_acc = None
                rem = int(deadline - time.monotonic()) if deadline is not None else timeout
                if rem < min_send:
                    break  # 剩下的时间不够再生成一次：不白切一个账号，直接返回第一次的错误
                # 与对话链路一致：排除刚失败的账号，只短暂等其它账号空出来；
                # 没有可切换的或等不到，都返回第一次的真实错误（不被「等号超时」覆盖）
                try:
                    alt = ACCOUNT_POOL.acquire(rotate=True, force_rotate=True,
                                               exclude_ids={old_id} if old_id else None,
                                               timeout=min(rem, SWITCH_WAIT), stop_event=stop_event)
                except (MuseGenerationError, MuseAuthError):
                    break
                if not alt or (old_id and alt["id"] == old_id):
                    if alt:
                        cur_acc = alt
                    break
                cur_acc = alt
                log.info("【生图/视频自动切号】切换到备用账号 %s (%s) 重试...", cur_acc.get("label"), cur_acc["id"])
            try:
                engine.loaded_sess = None  # 线程池线程会复用：不让上一个请求留下的会话值影响本次比对
                refreshed = _renew_and_persist(cur_acc["id"], wake_vm=True, force=(attempt > 0))
                if refreshed:
                    cur_acc = refreshed
                engine.set_account_meta(cur_acc["id"], cur_acc.get("proxy"),
                                        cur_acc.get("timezone"), cur_acc.get("lang"))
                engine.start()
                remaining = int(deadline - time.monotonic()) if deadline is not None else timeout
                if remaining < min_send:
                    # 排队耗掉了大部分时限：剩下的时间不够完成一次生成，不发提示词
                    # （发了几乎必然超时，muse.ai 那边却可能已开始生成并扣额度）
                    raise _NoTimeLeft(
                        f"排队等待后剩余时间不足 {MIN_GENERATION_SECONDS}s，未发送提示词，请稍后重试")
                refs = list(reference_images or [])
                engine_kwargs = {}
                if len(refs) == 1 and not reference_image:
                    reference_image = refs[0]
                elif len(refs) > 1:
                    engine_kwargs["reference_images"] = refs
                if stop_event is not None:
                    if stop_event.is_set():
                        raise _ClientGone()  # 客户端已断开：不再发提示词
                    engine_kwargs["stop_event"] = stop_event
                res = engine.generate(cur_acc["cookies"], prompt, expect=kind,
                                      timeout=remaining, expires=cur_acc.get("cookies_exp"),
                                      account_id=cur_acc["id"], on_progress=on_progress,
                                      reference_image=reference_image, **engine_kwargs)
                if not _session_replaced(cur_acc["id"]):  # 旧会话成功说明不了新换上的会话
                    store.mark(cur_acc["id"], True, "")
                _sync_cookies(cur_acc["id"])
                return res, cur_acc["id"]
            except _ClientGone:
                raise  # 客户端已断开：不写账号状态、不重试，finally 里释放资源
            except _NoTimeLeft:
                # 时间不够不是账号故障：不写账号状态、不切号；有上一次的真实错误就返回它
                if last_exc is not None:
                    raise last_exc from None
                raise
            except MuseAuthError as exc:
                last_exc = exc
                _mark_auth_failed(cur_acc["id"], exc)
                engine.close_current_page()
            except MuseGenerationError as exc:
                if stop_event is not None and stop_event.is_set():
                    raise _ClientGone() from None  # 叫停后引擎抛出的「已停止」不是账号故障
                last_exc = exc
                _record_generation_failure(cur_acc["id"], exc)
                # 不 reset_thread：重试会换账号重开页面（finally 也会关页），导航回 /thread/new
                # 只会白占槽位与账号 10 多秒，还吃掉重试的剩余时限
                engine.close_current_page()
            except Exception as exc:  # noqa: BLE001
                # 能走到这里的底层异常（如 CDPConnectionError）都发生在提示词发出之前：
                # 发送及之后的页面断开已由引擎包成 MuseSubmittedError。可以换号重试，但账号要留下记录
                engine.close_current_page()
                last_exc = MuseGenerationError(f"生成失败: {exc}")
                _record_generation_failure(cur_acc["id"], last_exc)
        raise last_exc
    finally:
        if cur_acc:
            ACCOUNT_POOL.release(cur_acc.get("id"))
        engine.close_current_page()


# ------------------------- 基础接口 -------------------------
def _browser_running() -> bool:
    """自己拉起的 Chromium 还活着，或复用了调试端口上已有浏览器且连接未断开。"""
    if engine.proc is not None:
        return engine.proc.poll() is None
    return engine.browser is not None and not getattr(engine.browser, "closed", False)


@app.get("/healthz")
def healthz():
    return {"status": "ok", "time": int(time.time())}


@app.get("/readyz")
def readyz():
    accs = [a for a in store.list_accounts() if a.get("enabled", True)]
    return {"status": "ready" if accs else "no_account",
            "accounts": len(accs),
            "browser_running": _browser_running()}


@app.get("/v1/models")
def models(_=Depends(auth)):
    return {"object": "list", "data": MODELS}


# ------------------------- 生图 -------------------------
def _image_item(req: ImageRequest, res: dict) -> dict:
    item = {"revised_prompt": req.prompt, "url": media_url(res["filename"]),
            "size": req.size or "auto", "kind": res["kind"], "bytes": res["size"]}
    if req.response_format == "b64_json":
        fpath = res.get("path") or os.path.join(CFG.media_dir, res["filename"])
        with open(fpath, "rb") as f:
            item["b64_json"] = base64.b64encode(f.read()).decode()
        item.pop("url", None)
    return item


def _prepare_reference_images(images: list[str]) -> list[str]:
    """Validate and snapshot all sources before a task is accepted."""
    prepared = []
    total_bytes = 0
    for index, image in enumerate(images):
        try:
            b64, mime = engine._normalize_image(image)
            data = base64.b64decode(b64, validate=True)
        except (RefImageSafetyError, MuseGenerationError, binascii.Error, ValueError) as exc:
            raise HTTPException(400, f"images[{index}]: {exc}") from exc
        total_bytes += len(data)
        if total_bytes > MAX_REFERENCE_TOTAL_BYTES:
            raise HTTPException(400, "参考图总大小超过 24 MiB 限制")
        prepared.append(f"data:{mime};base64,{b64}")
    return prepared


async def _prepare_references_async(images: list[str]) -> list[str]:
    """在线程里下载 / 校验参考图，并计入排队名额：慢速图源不能无限占用共享线程池。"""
    if not images:
        return []
    async with PENDING.hold():
        return await _in_long_pool(_prepare_reference_images, images)


def _queue_image(req: ImageRequest, prompt: str, reference_images: list[str],
                 idempotency_key: str | None = None):
    """Opt-in polling avoids reverse-proxy timeouts; no generation is repeated by polling."""
    if idempotency_key and len(idempotency_key) > 256:
        raise HTTPException(400, "Idempotency-Key too long")
    key_hash = hashlib.sha256(idempotency_key.encode()).hexdigest() if idempotency_key else None
    request_hash = hashlib.sha256(json.dumps(
        {"request": req.model_dump(by_alias=True), "references": reference_images},
        sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    with IMAGE_TASK_LOCK:
        tasks = list(store.tasks.values())
        if key_hash:
            for existing in tasks:
                if existing.get("image_request_key") == key_hash:
                    if existing.get("status") == "failed":
                        # 失败的任务不占着这个键：同键重提（如服务重启中断后）新建任务
                        store.update_task(existing["id"], image_request_key=None)
                        continue
                    if existing.get("image_request_hash") != request_hash:
                        raise HTTPException(409, "Idempotency-Key reused with different request")
                    return JSONResponse(status_code=202, content={
                        "id": existing["id"], "task_id": existing["id"],
                        "object": "image.task", "status": existing["status"],
                        "progress": existing.get("progress", 0),
                        "created_at": existing["created_at"]})
        # ponytail: one Chromium worker; cap admission instead of adding a broker.
        if sum(t.get("kind") == "image" and t.get("status") in ("queued", "processing")
               for t in tasks) >= 8:
            raise HTTPException(429, "Image queue is full; retry later")
        task = store.create_task("image", req.prompt)
        tid = task["id"]
        store.update_task(tid, api_prompt=prompt, size=req.size,
                          response_format=req.response_format, progress=0,
                          image_request_key=key_hash, image_request_hash=request_hash)

    def worker():
        timing = {"t0": time.time()}
        def _on_acquired():
            timing["t0"] = time.time()
            store.update_task(tid, status="processing", progress=5)
        count = max(1, min(int(req.n or 1), 4))   # 与同步接口一致：n>1 串行生成多张（上限 4）
        results: list[dict] = []
        acc_id = None
        try:
            for _i in range(count):
                res, acc_id = _run_generation(
                    prompt, "image", req.timeout or CFG.image_timeout,
                    reference_images=reference_images,
                    on_progress=lambda p, i=_i: store.update_task(
                        tid, progress=int((i * 100 + p) / count)),
                    on_acquired=_on_acquired if _i == 0 else None,
                    queue_timeout=req.timeout or CFG.image_timeout)
                # Store only media metadata, not large base64 payloads or reference credentials.
                results.append({**{k: res[k] for k in ("filename", "size", "kind")},
                                "bytes": res["size"],  # 与视频任务同名字段，管理页任务列表按它显示大小
                                "url": media_url(res["filename"])})
        except Exception as exc:  # noqa: BLE001
            if not results:
                store.update_task(tid, status="failed", error=str(exc),
                                  elapsed=round(time.time() - timing["t0"], 1))
                return
            log.warning("图片任务 %s 第 %d/%d 张失败，返回已完成的 %d 张: %s",
                        tid, len(results) + 1, count, len(results), exc)
        store.update_task(tid, status="completed", progress=100, account=acc_id,
                          elapsed=round(time.time() - timing["t0"], 1),
                          url=results[0]["url"], result=results[0], results=results)

    threading.Thread(target=worker, daemon=True).start()
    return JSONResponse(status_code=202, content={
        "id": tid, "task_id": tid, "object": "image.task", "status": "queued",
        "progress": 0, "created_at": task["created_at"]})


@app.post("/v1/images/tasks")
async def create_image_task(req: ImageRequest,
                            idempotency_key: str | None = Header(default=None), _=Depends(auth)):
    # 与 generations 一致：参考图下载计入排队名额、在长任务线程池里跑，不占用共享线程池
    refs = await _prepare_references_async(_image_references(req))
    return _queue_image(req, build_image_prompt(req), refs, idempotency_key)


@app.get("/v1/images/tasks/{task_id}")
def get_image_task(task_id: str, _=Depends(auth)):
    task = store.get_task(task_id)
    if not task or task.get("kind") != "image":
        raise HTTPException(404, "image task 不存在")
    out = dict(task)
    if out.get("status") == "completed":
        req = ImageRequest(prompt=out["prompt"], size=out.get("size"),
                           response_format=out.get("response_format") or "url")
        try:
            out.update({"created": int(time.time()),
                        "data": [_image_item(req, r) for r in (out.get("results") or [out["result"]])]})
        except FileNotFoundError as exc:
            raise HTTPException(410, "任务结果的媒体文件已被删除") from exc
        if out.get("url"):
            out["url"] = absolute_url(out["url"])
        if isinstance(out.get("result"), dict) and out["result"].get("url"):
            out["result"] = {**out["result"], "url": absolute_url(out["result"]["url"])}
        if isinstance(out.get("results"), list):
            out["results"] = [{**r, "url": absolute_url(r.get("url"))} for r in out["results"]]
    return out


async def _run_sync_images(request: Request, req: ImageRequest, prompt: str, timeout: int,
                           refs: list[str]) -> dict:
    """同步生图：n>1 串行生成多张（上限 4）。客户端断开就叫停，不在发提示词或等结果时空占浏览器与账号。"""
    count = max(1, min(int(req.n or 1), 4))
    items: list[dict] = []
    ctl = _StreamCtl()
    watcher = _start_bg_task(_watch_disconnect(request, ctl, _stream_watch_cap(timeout) * count))
    try:
        async with PENDING.hold():
            for _i in range(count):
                res, _acc = await _in_long_pool(
                    _run_generation, prompt, "image", timeout, reference_images=refs,
                    stop_event=ctl.stop_event)
                items.append(_image_item(req, res))
    except _ClientGone:
        raise HTTPException(499, "客户端已断开连接") from None
    except MuseAuthError as exc:
        if items:
            return {"created": int(time.time()), "data": items}
        raise _upstream_auth_error(exc) from exc
    except QuotaPausedError as exc:
        if items:
            return {"created": int(time.time()), "data": items}
        raise _quota_paused_http(exc) from exc
    except MuseGenerationError as exc:
        if items:
            return {"created": int(time.time()), "data": items}
        raise HTTPException(502, str(exc)) from exc
    finally:
        ctl.done.set()
        watcher.cancel()
    return {"created": int(time.time()), "data": items}


@app.post("/v1/images/generations")
async def images_generations(req: ImageRequest, request: Request,
                             idempotency_key: str | None = Header(default=None), _=Depends(auth)):
    ref_images = await _prepare_references_async(_image_references(req))

    prompt = build_image_prompt(req)
    timeout = req.timeout or CFG.image_timeout
    if req.async_:
        return _queue_image(req, prompt, ref_images, idempotency_key)
    return await _run_sync_images(request, req, prompt, timeout, ref_images)


@app.post("/v1/images/edits")
async def images_edits(request: Request,
                       idempotency_key: str | None = Header(default=None), _=Depends(auth)):
    """OpenAI 兼容的图生图/图像编辑接口，兼容 multipart/form-data 与 application/json。"""
    content_type = request.headers.get("content-type", "").lower()
    prompt = ""
    model = "muse-image"
    size = None
    aspect_ratio = None
    response_format = "url"
    timeout = None
    ref_image_data: list[str] = []
    async_mode = False
    n_val = 1

    if "multipart/form-data" in content_type:
        form = await request.form(
            max_files=MAX_REFERENCE_IMAGES, max_fields=20)
        async_mode = form.get("async", False)
        prompt = form.get("prompt") or ""
        model = form.get("model") or "muse-image"
        size = form.get("size")
        aspect_ratio = form.get("aspect_ratio")
        response_format = form.get("response_format") or "url"
        try:
            n_val = max(1, min(int(form.get("n") or 1), 4))
        except (TypeError, ValueError):
            n_val = 1
        timeout_val = form.get("timeout")
        if timeout_val:
            try:
                timeout = int(timeout_val)
            except ValueError:
                pass
        # openai-python 传多张图时字段名是 image[]；单张时是 image
        img_fields = form.getlist("image") + form.getlist("image[]")
        if len(img_fields) > MAX_REFERENCE_IMAGES:
            raise HTTPException(400, f"image 最多 {MAX_REFERENCE_IMAGES} 张参考图")
        total_upload_bytes = 0
        for index, img_field in enumerate(img_fields):
            if img_field and hasattr(img_field, "read"):
                ref_mime = getattr(img_field, "content_type", "image/png") or "image/png"
                content = await img_field.read(MAX_REFERENCE_IMAGE_BYTES + 1)
                if not ref_mime.lower().startswith("image/"):
                    # curl 等客户端认不出扩展名时按 application/octet-stream 上传：以文件头为准
                    ref_mime = _sniff_image_mime(content) or ""
                    if not ref_mime:
                        raise HTTPException(400, f"image[{index}] 不是图片文件")
                if len(content) > MAX_REFERENCE_IMAGE_BYTES:
                    raise HTTPException(400, f"image[{index}] 超过 10 MiB 限制")
                total_upload_bytes += len(content)
                if total_upload_bytes > MAX_REFERENCE_TOTAL_BYTES:
                    raise HTTPException(400, "参考图总大小超过 24 MiB 限制")
                ref_image_data.append(
                    f"data:{ref_mime};base64,{base64.b64encode(content).decode('ascii')}")
            elif isinstance(img_field, str) and img_field.strip():
                ref_image_data.append(img_field.strip())
            else:
                raise HTTPException(400, f"image[{index}] 不是有效图片引用")
    else:
        try:
            body = await request.json()
        except ValueError as exc:
            raise HTTPException(400, "请求体不是合法 JSON") from exc
        if not isinstance(body, dict):
            raise HTTPException(400, "请求体必须是 JSON 对象")
        async_mode = body.get("async", False)
        prompt = body.get("prompt") or ""
        model = body.get("model") or "muse-image"
        size = body.get("size")
        aspect_ratio = body.get("aspect_ratio")
        response_format = body.get("response_format") or "url"
        try:
            n_val = max(1, min(int(body.get("n") or 1), 4))
        except (TypeError, ValueError):
            n_val = 1
        timeout = body.get("timeout")
        raw_images = body.get("images")
        if raw_images is not None and not isinstance(raw_images, list):
            raise HTTPException(400, "images 必须是数组")
        legacy = [body.get("reference_image"), body.get("image_url"), body.get("image")]
        ref_image_data = _reference_list(raw_images, legacy, "images")

    if not ref_image_data:
        raise HTTPException(400, "图像编辑需要至少一张参考图（image / image[] / images）")
    if not prompt:
        prompt = "参考此图片并进行生图创作"

    try:
        req_obj = ImageRequest(
            prompt=prompt,
            model=model,
            size=size,
            aspect_ratio=aspect_ratio,
            response_format=response_format,
            timeout=timeout,
            images=ref_image_data,
            n=n_val,
            **{"async": async_mode}
        )
    except ValidationError as exc:
        raise RequestValidationError(exc.errors()) from exc
    timeout = req_obj.timeout
    ref_image_data = await _prepare_references_async(ref_image_data)
    full_prompt = build_image_prompt(req_obj)
    gen_timeout = timeout or CFG.image_timeout
    if req_obj.async_:
        return _queue_image(req_obj, full_prompt, ref_image_data, idempotency_key)
    return await _run_sync_images(request, req_obj, full_prompt, gen_timeout, ref_image_data)


# ------------------------- 生视频（异步任务） -------------------------
@app.post("/v1/videos")
@app.post("/v1/videos/generations")
async def create_video(req: VideoRequest, _=Depends(auth)):
    ref_images = await _prepare_references_async(_video_references(req))

    prompt = build_video_prompt(req)
    timeout = req.timeout or CFG.video_timeout
    with IMAGE_TASK_LOCK:  # 与生图队列同一把锁：判断与建任务原子完成
        # 每个视频任务一个线程、最长阻塞 timeout 秒等浏览器：限制同时排队/处理的数量
        if sum(t.get("kind") == "video" and t.get("status") in ("queued", "processing")
               for t in list(store.tasks.values())) >= 8:
            raise HTTPException(429, "Video queue is full; retry later")
        task = store.create_task("video", req.prompt)
    store.update_task(task["id"], api_prompt=prompt)

    store.update_task(task["id"], progress=10)

    def worker():
        timing = {"t0": time.time()}
        def _on_acquired():
            timing["t0"] = time.time()
            store.update_task(task["id"], status="processing", progress=15)
        try:
            def prog_cb(p):
                store.update_task(task["id"], progress=p)
            res, acc_id = _run_generation(prompt, "video", timeout, on_progress=prog_cb,
                                          reference_images=ref_images, on_acquired=_on_acquired,
                                          queue_timeout=timeout)
            vurl = media_url(res["filename"])
            store.update_task(task["id"], status="completed", progress=100, account=acc_id,
                              elapsed=round(time.time() - timing["t0"], 1),
                              url=vurl,
                              video={"url": vurl},
                              result={"url": vurl,
                                      "filename": res["filename"],
                                      "bytes": res["size"], "kind": res["kind"]})
        except Exception as exc:  # noqa: BLE001
            store.update_task(task["id"], status="failed",
                              elapsed=round(time.time() - timing["t0"], 1), error=str(exc))

    threading.Thread(target=worker, daemon=True).start()
    return {"id": task["id"], "task_id": task["id"], "object": "video.task", "status": "queued",
            "progress": 10, "created_at": task["created_at"]}


@app.get("/v1/videos/{task_id}")
@app.get("/v1/videos/generations/{task_id}")
def get_video(task_id: str, _=Depends(auth)):
    t = store.get_task(task_id)
    if not t or t.get("kind") != "video":
        raise HTTPException(404, "video task 不存在")
    out = dict(t)
    status = out.get("status")
    if status in ("succeeded", "success", "done"):
        out["status"] = "completed"
    if out.get("status") == "completed":
        out["progress"] = 100
    elif out.get("status") == "processing":
        # Progress reports observed milestones only; Muse exposes no percentage.
        out["progress"] = max(out.get("progress", 0) or 0, 15)
    vurl = out.get("url")
    if not vurl and isinstance(out.get("result"), dict):
        vurl = out["result"].get("url")
    if vurl:
        vurl = absolute_url(vurl)
        out["url"] = vurl
        out["video"] = {**(out.get("video") or {}), "url": vurl}
        if isinstance(out.get("result"), dict):
            out["result"] = {**out["result"], "url": vurl}
    return out


# ------------------------- 对话（OpenAI 兼容） -------------------------
def _sse(obj: dict) -> str:
    return "data: " + json.dumps(obj, ensure_ascii=False) + "\n\n"


def _chat_chunk(cid: str, created: int, model: str, delta: dict,
                finish: str | None = None) -> dict:
    return {"id": cid, "object": "chat.completion.chunk", "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}


def _want_usage(stream_options) -> bool:
    """下游（LangChain、部分 SDK / 智能体框架）会带
    `stream_options.include_usage=true`，要求在 [DONE] 之前补一个
    `choices: []` + `usage` 的分片。不发的话少数框架会一直等 usage 而卡住。"""
    if isinstance(stream_options, dict):
        return bool(stream_options.get("include_usage"))
    return False


def _estimate_tokens(text) -> int:
    """粗略 token 估算（约 4 字符/token）；muse.ai 不回报 token，用于兼容按量统计的下游。"""
    if not text:
        return 0
    return max(1, len(str(text)) // 4)


def _usage(prompt_tokens: int, completion_tokens: int) -> dict:
    return {"prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens}


PACE_MAX_SECONDS = 1.5


def _pace_text(text: str, chunk_size: int = 2, delay: float = 0.012):
    """把大块文本平滑切分为仿原生 LLM 打字机的微流式 Token。
    若传入文本本身微小（<= 3 字），直接秒级放行，零延迟；
    若传入文本是大块（如 DOM 批量刷新），按每 chunk_size 字间隔 delay 平滑输出。
    """
    if not text:
        return
    if len(text) <= 3 or delay <= 0:
        yield text
        return
    # 一块文本的平滑输出总共最多拖 PACE_MAX_SECONDS 秒：大块文本加大每片字数，
    # 否则一次刷出几千字时下游要多等几十秒，长代码回复容易触发客户端超时
    max_pieces = max(1, int(PACE_MAX_SECONDS / delay))
    chunk_size = max(chunk_size, -(-len(text) // max_pieces))
    for i in range(0, len(text), chunk_size):
        yield text[i:i + chunk_size]
        if i + chunk_size < len(text):
            time.sleep(delay)


_SSE_HEADERS = {
    "Cache-Control": "no-cache, no-transform",
    "X-Accel-Buffering": "no",
    "Connection": "keep-alive",
}


@app.post("/v1/chat/completions")
async def chat_completions(req: ChatRequest, request: Request, _=Depends(auth)):
    """OpenAI 兼容的对话接口 —— 各类智能体客户端都能接。

    说明：muse.ai 网页是**自动路由**的 agent，界面上没有模型选择器，所有请求
    都会落到同一个网页助手（自称 Koda，底层是 Muse 系列语言模型）。所以
    `model` 字段只用于兼容下游，不影响路由结果。

    请求带 `tools` 时，会注入【工具调用协议】并把模型输出的 JSON 解析回
    `tool_calls`（muse.ai 没有原生 function calling，这层属于协议适配）。

    对话与生图/生视频共用同一个浏览器实例，靠 `GEN_LOCK` 串行。
    """
    prompt = build_chat_prompt(req.messages) or (req.prompt or "").strip()
    if not prompt:
        raise HTTPException(400, "messages 为空")

    # 工具调用协议默认不注入 —— 实测 muse.ai 的助手会明确拒绝输出"伪工具调用"，
    # 注入反而污染正常回答；详见 config.py 的 tool_protocol 注释。
    tool_note = build_tools_prompt(req.tools) if CFG.tool_protocol else ""
    if tool_note:
        # 放在末尾：开头是 agent 自己的 system 提示，夹在中间容易被忽略；
        # 紧贴用户消息之前，模型对末尾指令的遵守度明显更高。
        prompt = prompt + "\n\n" + tool_note

    model = resolve_model(req.model, default="muse-spark")
    timeout = int(req.timeout or CFG.chat_timeout)
    # 入口只做只读的"有无可用账号"检查，不预选账号：选号由 worker 拿到槽位后经 ACCOUNT_POOL 完成，
    # 忙碌账号自动跳过
    if not _has_live_account():
        raise HTTPException(400, "没有可用账号，请先在管理页面导入 cookie")
    preferred_id = None  # 每次请求都新开隔离上下文，没有热页可复用：按 LRU 轮转选号

    cid = "chatcmpl-" + uuid.uuid4().hex[:24]
    created = int(time.time())

    def tool_call_deltas(calls: list[dict]) -> list[list[dict]]:
        """按 OpenAI 习惯分两片发：先 id/name，再 arguments 全文。"""
        head = [{"index": i, "id": c["id"], "type": "function",
                 "function": {"name": c["function"]["name"], "arguments": ""}}
                for i, c in enumerate(calls)]
        body = [{"index": i, "function": {"arguments": c["function"]["arguments"]}}
                for i, c in enumerate(calls)]
        return [head, body]

    if req.stream:
        ctl = _StreamCtl()
        PENDING.enter()
        ctl.release = PENDING.once()

        def sync_stream():
            comp_chars = 0
            try:
                # 握手建立瞬间立即发送 role: assistant 首包，让下游客户端秒级捕获光标
                yield _sse(_chat_chunk(cid, created, model, {"role": "assistant"}))

                stream_gen = safe_chat_stream(prompt, timeout, preferred_id=preferred_id, ctl=ctl)
                if not tool_note:
                    for chunk in stream_gen:
                        comp_chars += len(chunk)
                        for piece in _pace_text(chunk):
                            yield _sse(_chat_chunk(cid, created, model, {"content": piece}))
                    yield _sse(_chat_chunk(cid, created, model, {}, finish="stop"))
                else:
                    buf, mode = "", None
                    for chunk in stream_gen:
                        if mode == "text":
                            for piece in _pace_text(chunk):
                                yield _sse(_chat_chunk(cid, created, model, {"content": piece}))
                            continue
                        buf += chunk
                        if mode is None:
                            head = buf.lstrip()
                            if len(head) >= 2:
                                if head[0] in "{[" or head.startswith("```"):
                                    mode = "maybe_tool"
                                else:
                                    mode = "text"
                                    for piece in _pace_text(buf):
                                        yield _sse(_chat_chunk(cid, created, model, {"content": piece}))
                                    buf = ""
                    if mode == "text":
                        yield _sse(_chat_chunk(cid, created, model, {}, finish="stop"))
                    else:
                        calls, rest = parse_tool_calls(buf)
                        if calls:
                            if rest:
                                for piece in _pace_text(rest):
                                    yield _sse(_chat_chunk(cid, created, model, {"content": piece}))
                            for piece in tool_call_deltas(calls):
                                yield _sse(_chat_chunk(cid, created, model, {"tool_calls": piece}))
                            yield _sse(_chat_chunk(cid, created, model, {}, finish="tool_calls"))
                        else:
                            if buf:
                                for piece in _pace_text(buf):
                                    yield _sse(_chat_chunk(cid, created, model, {"content": piece}))
                            yield _sse(_chat_chunk(cid, created, model, {}, finish="stop"))
                if _want_usage(req.stream_options):
                    yield _sse({"id": cid, "object": "chat.completion.chunk",
                                "created": created, "model": model,
                                "choices": [],
                                "usage": _usage(_estimate_tokens(prompt),
                                                max(1, comp_chars // 4) if comp_chars else 0)})
                yield "data: [DONE]\n\n"
            # 账号状态由 worker 按实际执行的账号写入，这里只把错误转成 SSE 错误事件
            except _ClientGone:
                return  # 客户端已断开或停止读取：不再输出，也不写账号状态
            except NoAccountError as exc:
                yield _sse({"error": {"message": str(exc), "type": "invalid_request_error", "code": 400}})
            except MuseAuthError as exc:
                yield _sse({"error": {"message": str(exc), "type": "upstream_auth_error",
                                      "code": "upstream_auth_error"}})
            except QuotaPausedError as exc:
                yield _sse({"error": {"message": str(exc), "type": "rate_limit_error", "code": 429,
                                      "retry_after": exc.retry_after()}})
            except MuseGenerationError as exc:
                yield _sse({"error": {"message": str(exc), "type": "server_error", "code": 502}})
            except Exception as exc:
                yield _sse({"error": {"message": f"内部错误: {exc}", "type": "server_error", "code": 500}})
        _start_bg_task(_watch_disconnect(request, ctl, _stream_watch_cap(timeout)))
        return StreamingResponse(_pull_tracked(sync_stream(), ctl), media_type="text/event-stream",
                                 headers=_SSE_HEADERS)

    text = await _run_chat_nonstream(request, prompt, timeout, preferred_id)

    usage = _usage(_estimate_tokens(prompt), _estimate_tokens(text))
    if tool_note:
        calls, rest = parse_tool_calls(text)
        if calls:
            return {"id": cid, "object": "chat.completion", "created": created,
                    "model": model,
                    "choices": [{"index": 0, "finish_reason": "tool_calls",
                                 "message": {"role": "assistant",
                                             "content": rest or None,
                                             "tool_calls": calls}}],
                    "usage": usage}
    return {"id": cid, "object": "chat.completion", "created": created,
            "model": model,
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant", "content": text}}],
            "usage": usage}


def _responses_messages(req: ResponsesRequest) -> list[ChatMessage]:
    """把 Responses API 的 instructions / input 归一成 messages。"""
    msgs: list[ChatMessage] = []
    if req.instructions:
        msgs.append(ChatMessage(role="system", content=req.instructions))
    inp = req.input
    if isinstance(inp, str):
        msgs.append(ChatMessage(role="user", content=inp))
        return msgs
    if isinstance(inp, list):
        for item in inp:
            if isinstance(item, str):
                msgs.append(ChatMessage(role="user", content=item))
                continue
            if not isinstance(item, dict):
                continue
            itype = item.get("type")
            if item.get("role"):                      # message 形态
                content = item.get("content")
                if isinstance(content, dict):         # 单个 content part 直接给成对象
                    content = [content]
                if not isinstance(item["role"], str) or not (
                        content is None or isinstance(content, (str, list))):
                    raise HTTPException(422, "input[].content 必须是字符串或 content part 数组")
                msgs.append(ChatMessage(role=item["role"], content=content))
            elif itype == "input_text":
                msgs.append(ChatMessage(role="user", content=item.get("text")))
            elif itype == "function_call_output":
                msgs.append(ChatMessage(role="user", content="【工具执行结果】\n"
                                        + str(item.get("output") or "")))
            elif itype == "function_call":
                msgs.append(ChatMessage(role="assistant", content="【请求调用工具】"
                                        + str(item.get("name") or "")))
    return msgs


async def _run_chat_nonstream(request: Request, prompt: str, timeout: int, preferred_id) -> str:
    """非流式对话：同样监视客户端断连，断开就叫停 worker，释放浏览器槽位与账号，不在后台空跑到超时。"""
    ctl = _StreamCtl()

    def run() -> str:
        joined = "".join(safe_chat_stream(prompt, timeout, preferred_id=preferred_id, ctl=ctl))
        # 增量拼接在气泡出现非前缀变化（占位文字被替换、重新渲染）时会出错：有页面最终文本就以它为准
        return ctl.final_text or joined

    watcher = _start_bg_task(_watch_disconnect(request, ctl, _stream_watch_cap(timeout)))
    try:
        async with PENDING.hold():
            return await _in_long_pool(run)
    except _ClientGone:
        raise HTTPException(499, "客户端已断开连接") from None
    except MuseAuthError as exc:
        raise _upstream_auth_error(exc) from exc
    except QuotaPausedError as exc:
        raise _quota_paused_http(exc) from exc
    except MuseGenerationError as exc:
        raise HTTPException(502, str(exc))
    finally:
        ctl.stop_event.set()
        watcher.cancel()


def _sse_event(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def _response_error(exc: BaseException) -> dict:
    """responses 流式失败时 response.failed 里的 response.error（L6 / 2.54），与 Responses API 的
    response.error 形状一致：{"code": …, "message": …}。"""
    if isinstance(exc, NoAccountError):
        code = "no_available_account"
    elif isinstance(exc, MuseAuthError):
        code = "upstream_auth_error"
    elif isinstance(exc, QuotaPausedError):
        code = "rate_limit_exceeded"
    elif isinstance(exc, InternalGenerationError):
        code = "server_error"
    elif isinstance(exc, MuseGenerationError):
        code = "upstream_error"
    else:
        code = "server_error"
    return {"code": code, "message": str(exc)}


@app.post("/v1/responses")
async def responses_api(req: ResponsesRequest, request: Request, _=Depends(auth)):
    """OpenAI Responses API —— 新版 Codex 默认走这个端点。

    只实现 Codex 实际用到的子集：文本进 → 文本出（含流式事件）。
    muse.ai 不会返回结构化的 tool_calls，关于工具调用的边界见 README。
    """
    prompt = build_chat_prompt(_responses_messages(req))
    if not prompt:
        raise HTTPException(400, "input 为空")

    model = resolve_model(req.model, default="muse-spark")
    timeout = int(req.timeout or CFG.chat_timeout)
    # 与 chat_completions 相同：入口只读检查有无可用账号，选号交给 worker（忙碌账号自动跳过）
    if not _has_live_account():
        raise HTTPException(400, "没有可用账号，请先在管理页面导入 cookie")
    preferred_id = None  # 每次请求都新开隔离上下文，没有热页可复用：按 LRU 轮转选号

    rid = "resp_" + uuid.uuid4().hex[:24]
    mid = "msg_" + uuid.uuid4().hex[:24]
    created = int(time.time())

    def envelope(status: str, text: str = "") -> dict:
        done = status == "completed"
        _it, _ot = _estimate_tokens(prompt), _estimate_tokens(text)
        return {"id": rid, "object": "response", "created_at": created,
                "status": status, "model": model,
                "output": [{"type": "message", "id": mid, "role": "assistant",
                            "status": "completed" if done else "in_progress",
                            "content": ([{"type": "output_text", "text": text,
                                          "annotations": []}] if text else [])}],
                "usage": {"input_tokens": _it, "output_tokens": _ot,
                          "total_tokens": _it + _ot}}

    if req.stream:
        ctl = _StreamCtl()
        PENDING.enter()
        ctl.release = PENDING.once()

        def sync_stream():
            try:
                yield _sse_event("response.created", {
                    "type": "response.created",
                    "response": envelope("in_progress")})
                yield _sse_event("response.output_item.added", {
                    "type": "response.output_item.added", "output_index": 0,
                    "item": {"id": mid, "type": "message", "role": "assistant",
                             "status": "in_progress", "content": []}})
                yield _sse_event("response.content_part.added", {
                    "type": "response.content_part.added", "item_id": mid,
                    "output_index": 0, "content_index": 0,
                    "part": {"type": "output_text", "text": "",
                             "annotations": []}})
                full = ""
                for chunk in safe_chat_stream(prompt, timeout, preferred_id=preferred_id, ctl=ctl):
                    full += chunk
                    yield _sse_event("response.output_text.delta", {
                        "type": "response.output_text.delta", "item_id": mid,
                        "output_index": 0, "content_index": 0, "delta": chunk})
                yield _sse_event("response.output_text.done", {
                    "type": "response.output_text.done", "item_id": mid,
                    "output_index": 0, "content_index": 0, "text": full})
                yield _sse_event("response.output_item.done", {
                    "type": "response.output_item.done", "output_index": 0,
                    "item": {"id": mid, "type": "message", "role": "assistant",
                             "status": "completed",
                             "content": [{"type": "output_text", "text": full,
                                          "annotations": []}]}})
                yield _sse_event("response.completed", {
                    "type": "response.completed",
                    "response": envelope("completed", full)})
            except _ClientGone:
                return  # 客户端已断开或停止读取：不再输出
            except Exception as exc:  # noqa: BLE001
                log.warning("responses 流式失败: %s", exc)
                yield _sse_event("response.failed", {
                    "type": "response.failed",
                    "response": {**envelope("failed"), "error": _response_error(exc)}})
        _start_bg_task(_watch_disconnect(request, ctl, _stream_watch_cap(timeout)))
        return StreamingResponse(_pull_tracked(sync_stream(), ctl), media_type="text/event-stream",
                                 headers=_SSE_HEADERS)

    text = await _run_chat_nonstream(request, prompt, timeout, preferred_id)

    return envelope("completed", text)


def _safe_media_name(name: str) -> bool:
    """媒体文件名只能是 media 目录下的一个普通文件名：拒绝路径分隔符、`..`、
    Windows 盘符（`D:x`）与 NTFS 流（`x:stream`），并确认解析后仍落在 media 目录内。"""
    if not name or any(c in name for c in '/\\:\x00') or ".." in name or os.path.basename(name) != name:
        return False
    root = os.path.realpath(CFG.media_dir)
    return os.path.dirname(os.path.realpath(os.path.join(root, name))) == root


@app.get("/v1/media/{name}")
def get_media(name: str):
    if not _safe_media_name(name):
        raise HTTPException(400, "非法文件名")
    p = os.path.join(CFG.media_dir, name)
    if not os.path.isfile(p):
        raise HTTPException(404, "文件不存在")
    return FileResponse(p)


# ------------------------- 管理：总览 -------------------------
@app.get("/admin/status")
def admin_status(_=Depends(auth)):
    base = _public_base()
    return {
        "accounts": store.list_accounts(),
        "stats": store.stats(),
        "tasks": store.list_tasks(20),
        "media_count": len(os.listdir(CFG.media_dir))
        if os.path.isdir(CFG.media_dir) else 0,
        "browser_running": _browser_running(),
        "essential_cookies": list(ESSENTIAL_COOKIES),
        "base_url": f"{base}/v1" if base else "",
        "config": {"site": CFG.site_url, "cdp_port": CFG.cdp_port,
                   "image_timeout": CFG.image_timeout,
                   "video_timeout": CFG.video_timeout,
                   "host": CFG.host, "port": CFG.port,
                   "public_base": base},
    }


# ------------------------- 管理：账号池 -------------------------
@app.get("/admin/accounts")
def list_accounts(_=Depends(auth)):
    return {"accounts": store.list_accounts(), "stats": store.stats()}


def _check_proxy(proxy: str | None):
    err = MuseEngine.validate_proxy(proxy)
    if err:
        raise HTTPException(400, f"{err}：{proxy}")


@app.post("/admin/accounts")
def add_account(req: AccountRequest, _=Depends(auth)):
    _check_proxy(req.proxy)
    added: list[dict] = []
    seen: list[dict] = []          # 本次导入的所有 cookie，用来检查核心项是否齐全

    if req.batch:
        for label, cookies in parse_batch(req.batch):
            # 同一请求里给的代理 / 时区 / 语言 / 有效期同样作用于批量里的每个账号，不悄悄丢掉
            acc, updated = store.add_account(cookies, label, cookies_exp=req.expires,
                                             proxy=req.proxy, timezone=req.timezone, lang=req.lang,
                                             with_status=True)
            seen.append(cookies)
            added.append({"id": acc["id"], "label": acc["label"],
                          "cookie_count": len(cookies),
                          "expires_at": acc.get("expires_at"),
                          "updated": updated})

    cookies = dict(req.cookies)
    if req.cookie_header:
        cookies.update(parse_cookie_text(req.cookie_header))
    if cookies:
        acc, updated = store.add_account(cookies, req.label, cookies_exp=req.expires,
                                         proxy=req.proxy, timezone=req.timezone, lang=req.lang,
                                         with_status=True)
        seen.append(cookies)
        added.append({"id": acc["id"], "label": acc["label"],
                      "cookie_count": len(cookies),
                      "expires_at": acc.get("expires_at"),
                      "updated": updated})

    if not added:
        raise HTTPException(400, "未解析到任何 cookie，请检查格式")

    # 核心 cookie 按账号逐个检查：批量里每一条都要自己凑齐，不能靠别的行补
    per_missing = [[n for n in ESSENTIAL_COOKIES if not c.get(n)] for c in seen]
    for item, miss in zip(added, per_missing):
        item["essential_missing"] = miss
    missing = sorted({n for miss in per_missing for n in miss}, key=list(ESSENTIAL_COOKIES).index)
    bad = [item["label"] for item, miss in zip(added, per_missing) if miss]
    warning = ""
    if bad:
        warning = (f"缺少核心 cookie：{', '.join(missing)}，该账号可能无法生成" if len(added) == 1
                   else f"{len(bad)} 个账号缺少核心 cookie（{', '.join(missing)}），可能无法生成：{', '.join(bad)}")
    return {"added": added, "count": len(added),
            "essential_missing": missing,
            "warning": warning}


@app.patch("/admin/accounts/{aid}")
def patch_account(aid: str, req: AccountPatch, _=Depends(auth)):
    # 忽略回传的脱敏代理值（list_accounts 返回的是 ***@host:port），避免覆盖真实代理
    proxy_val = None if (isinstance(req.proxy, str) and "***" in req.proxy) else req.proxy
    _check_proxy(proxy_val)
    acc = store.update_account(aid, label=req.label, enabled=req.enabled,
                               proxy=proxy_val, timezone=req.timezone, lang=req.lang)
    if not acc:
        raise HTTPException(404, "账号不存在")
    out = {k: v for k, v in acc.items() if k != "cookies"} | {
        "cookie_count": len(acc.get("cookies", {}))}
    if out.get("proxy"):
        out["proxy"] = _mask_proxy(out["proxy"])  # 与列表接口一致，不回显代理账密
    return out


@app.delete("/admin/accounts/{aid}")
def del_account(aid: str, _=Depends(auth)):
    ok = store.delete_account(aid)
    if not ok:
        raise HTTPException(404, "账号不存在")
    engine.drop_account_meta(aid)
    return {"deleted": True, "id": aid}


@app.post("/admin/accounts/{aid}/test")
async def test_account(aid: str, _=Depends(auth)):
    """真实打开 muse.ai 验证该账号 cookie 是否仍可登录。"""
    acc = store.get_account(aid)
    if not acc:
        raise HTTPException(404, "账号不存在")
    if not acc.get("cookies"):
        raise HTTPException(400, "该账号没有 cookie")

    def _probe():
        try:
            with _leased_account(aid) as acc:
                engine.loaded_sess = None  # 由 refresh → ensure_page 记下本次测试实际用的会话
                engine.set_account_meta(aid, acc.get("proxy"),
                                        acc.get("timezone"), acc.get("lang"))
                engine.start()
                engine.refresh(acc["cookies"], acc.get("cookies_exp"), account_id=aid)
                unconfirmed = _renew_unconfirmed()
                if unconfirmed:
                    raise MuseGenerationError(unconfirmed)
                synced = _sync_cookies(aid)
                quota = None
                quota_error = None
                try:  # Session validity and quota readability are independent results.
                    quota = engine.quota(acc["cookies"],
                                         acc.get("cookies_exp"), account_id=aid)
                    if quota.get("found"):
                        quota["checked_at"] = int(time.time())
                        store.update_account(aid, quota=quota)
                    else:
                        quota_error = "额度面板未读到有效数据；旧缓存未覆盖"
                        quota = None
                except MuseAuthError:
                    raise  # 读额度时发现已登出：会话失效，按认证失败处理（外层标记 ok=False）
                except Exception as exc:  # noqa: BLE001
                    quota_error = f"{type(exc).__name__}；额度缓存未更新"
                if _session_replaced(aid):
                    return {"ok": False, "message": "测试期间账号 cookie 已被更新，本次结果不代表新会话，请重新测试"}
                store.mark(aid, True, "会话有效" + ("；额度读取失败" if quota_error else ""))
                return {"ok": True, "message": "会话有效，可正常生成",
                        "synced": synced, "quota": quota,
                        "quota_error": quota_error}
        except NoAccountError as exc:
            return {"ok": False, "message": str(exc)[:200]}
        except MuseAuthError as exc:
            if _session_replaced(aid):
                return {"ok": False, "message": "测试期间账号 cookie 已被更新，本次结果不代表新会话，请重新测试"}
            store.mark(aid, False, str(exc)[:200])
            return {"ok": False, "message": str(exc)[:200]}
        except Exception as exc:  # noqa: BLE001
            store.touch_keepalive(aid, None, f"测试未确认（保留账号状态）: {str(exc)[:200]}")
            return {"ok": False, "message": str(exc)[:200]}

    res = await asyncio.to_thread(_probe)
    if not res["ok"]:
        return JSONResponse(res, status_code=200)
    return res


@app.post("/admin/accounts/{aid}/relogin")
async def relogin_account(aid: str, _=Depends(auth)):
    return await test_account(aid, _)


@app.post("/admin/accounts/{aid}/cookies")
def update_cookies(aid: str, payload: dict = Body(...), _=Depends(auth)):
    """更新某个账号的 cookie（用于会话过期后补新 cookie）。"""
    raw_cookies = payload.get("cookies") or {}
    raw_exp = payload.get("expires") or {}
    header = payload.get("cookie_header")
    if (not isinstance(raw_cookies, dict) or not isinstance(raw_exp, dict)
            or (header is not None and not isinstance(header, str))):
        raise HTTPException(400, "cookies 与 expires 必须是对象，cookie_header 必须是字符串")
    cookies = {str(k): str(v) for k, v in raw_cookies.items() if v is not None}
    if header:
        cookies.update(parse_cookie_text(header))
    if not cookies:
        raise HTTPException(400, "未解析到 cookie")
    missing = [n for n in ESSENTIAL_COOKIES if not cookies.get(n)]
    if missing:
        # 下面是整份替换：缺核心项（常见于从控制台 document.cookie 复制，httpOnly 的会话 cookie 不在里面）
        # 会把账号原有的有效会话覆盖掉、且无法找回。拒绝写入，原 cookie 保持不变
        raise HTTPException(400, f"缺少核心 cookie：{', '.join(missing)}，未做任何修改；"
                                 "请用浏览器扩展读取（含 httpOnly）后再更新")
    exp = {k: int(v) for k, v in raw_exp.items()
           if _pos(v)}
    sess = cookies.get("hatch_sess")
    owner = next((a for a in list(store.accounts) if a["id"] != aid and sess
                  and (a.get("cookies") or {}).get("hatch_sess") == sess), None)
    if owner:
        # 两条记录共用一个会话会绕过每账号并发上限，导致同一会话被并发使用
        raise HTTPException(409, f"该会话已属于账号「{owner.get('label') or owner['id']}」，请直接更新那个账号")
    cur = store.get_account(aid) or {}
    if sess and (cur.get("cookies") or {}).get("hatch_sess") == sess:
        # 粘贴的还是同一个会话：寿命不因此延长（与 Store.add_account 的同会话分支一致）。
        # 有效期合并而不是覆盖，估算锚点沿用原值，没有就退回导入时间
        exp = {**(cur.get("cookies_exp") or {}), **exp}
        anchor = cur.get("expiry_anchor") or cur.get("created_at") or int(time.time())
    else:
        # 补入的是「全新会话」的 cookie，有效期估算锚点必须重置到当下，
        # 否则会沿用旧会话的锚点，把剩余天数算少。旧会话的有效期一并作废
        anchor = int(time.time())
    # ok 重置为「未检测」使账号重新参与调度，cookies_exp 总是写入以重算 expires_at。
    acc = store.update_account(aid, clear=("ok", "quota_blocked_until"), cookies=cookies,
                               note="已更新 cookie",
                               expiry_anchor=anchor,
                               cookies_exp=exp)
    if not acc:
        raise HTTPException(404, "账号不存在")
    return {"ok": True, "id": aid, "cookie_count": len(cookies),
            "expires_at": acc.get("expires_at")}


@app.post("/admin/relogin")
def relogin(_=Depends(auth)):
    if not _has_live_account():
        raise HTTPException(400, "没有可用账号")
    try:
        with _leased_account() as acc:
            engine.set_account_meta(acc["id"], acc.get("proxy"),
                                    acc.get("timezone"), acc.get("lang"))
            engine.start()
            engine.refresh(acc["cookies"], acc.get("cookies_exp"), account_id=acc["id"])
            return {"ok": True, "account": acc["id"]}
    except NoAccountError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)


# ------------------------- 管理：额度 -------------------------
@app.post("/admin/accounts/{aid}/quota")
async def query_quota(aid: str, _=Depends(auth)):
    """打开 muse.ai 的 Settings 面板读该账号的额度（实时），并缓存到账号记录。

    返回例：{"plan":"Free plan","weekly_reset":"Sep 30",
             "weekly_used_pct":1,"extra_left":"2B tokens left",
             "extra_used_pct":0,"extra_expires":"never"}
    """
    acc = store.get_account(aid)
    if not acc:
        raise HTTPException(404, "账号不存在")
    if not acc.get("cookies"):
        raise HTTPException(400, "该账号没有 cookie")
    return await _quota_probe(aid)


async def _quota_probe(aid: str | None):
    """aid 为空时租一个空闲账号（跳过忙碌账号，不累加 use_count / last_used）。"""
    used = {"id": aid}

    def _probe():
        with _leased_account(aid, record_use=False) as acc:
            used["id"] = acc["id"]
            used["sess"] = (acc.get("cookies") or {}).get("hatch_sess")
            engine.set_account_meta(acc["id"], acc.get("proxy"),
                                    acc.get("timezone"), acc.get("lang"))
            engine.start()
            q = engine.quota(acc["cookies"], acc.get("cookies_exp"), account_id=acc["id"])
            if not q.get("found"):
                raise MuseGenerationError("额度面板未读到有效数据；旧缓存未覆盖")
            q["checked_at"] = int(time.time())
            store.update_account(acc["id"], quota=q)
            return q

    try:
        return await asyncio.to_thread(_probe)
    except NoAccountError as exc:
        raise HTTPException(400, str(exc)) from exc
    except MuseAuthError as exc:
        # 查询期间 cookie 被更新：旧会话的登出说明不了新会话，不写 ok=False
        if used["id"] and not _session_replaced(used["id"], used.get("sess") or ""):
            store.mark(used["id"], False, str(exc)[:200])
        raise _upstream_auth_error(exc) from exc
    except MuseGenerationError as exc:
        raise HTTPException(502, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(502, f"额度查询失败: {exc}") from exc


@app.post("/admin/quota")
async def query_any_quota(_=Depends(auth)):
    """用当前最久未用的可用账号查一次额度（同池账号共享同一 muse.ai 计划的
    通常只有一人使用时够用；多账号时建议按账号查）。"""
    if not _has_live_account():
        raise HTTPException(400, "没有可用账号")
    return await _quota_probe(None)


# ------------------------- 管理：接入信息 / API Key -------------------------
def _public_base() -> str:
    return (CFG.public_base or "").rstrip("/")


@app.get("/admin/apikey")
def get_apikey(_=Depends(auth)):
    base = _public_base()
    base_url = f"{base}/v1" if base else ""
    return {"api_key": CFG.api_key, "base_url": base_url,
            # True：/admin/* 用独立的 MUSE2API_ADMIN_KEY，上面的 api_key 只能调用 /v1/*
            "admin_key_separate": bool((getattr(CFG, "admin_key", "") or "").strip()),
            "models_url": f"{base}/v1/models" if base else "",
            "media_url": f"{base}/v1/media/{{name}}" if base else "/v1/media/{name}"}


@app.post("/admin/apikey/rotate")
def rotate_apikey(_=Depends(auth)):
    """生成新的 API Key，写入 .env 并立即生效（不用重启）。"""
    import secrets
    new_key = "m2a_" + secrets.token_hex(24)
    old = CFG.api_key
    CFG.api_key = new_key
    _persist_env("MUSE2API_KEY", new_key)
    return {"ok": True, "api_key": new_key, "previous": old,
            "message": "已生成新 Key 并立即生效；旧 Key 已失效，请更新下游项目"}


def _persist_env(key: str, value: str):
    """把运行时生成的配置写进 data/.env（随 data 卷持久化；保留其它行，原子替换）。

    data/.env 的优先级高于项目根 .env（见 config.dotenv_paths），写入后重启仍取到新值。"""
    path = dotenv_paths(CFG.base_dir)[-1]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, encoding="utf-8") as f:
            lines = f.read().splitlines()
    except OSError:
        lines = []
    found = False
    for i, ln in enumerate(lines):
        if ln.strip().startswith(key + "="):
            lines[i] = f"{key}={value}"
            found = True
    if not found:
        lines.append(f"{key}={value}")
    tmp = path + ".tmp"
    # .env 里有管理 Key、GITHUB_TOKEN：只给属主读写，多用户主机上其他本地用户读不到
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.chmod(tmp, 0o600)  # 文件已存在时 O_CREAT 的权限不生效
    except OSError:
        pass
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).strip() + "\n")
    os.replace(tmp, path)


# ------------------------- 管理：Cookie 获取 -------------------------
@app.get("/admin/cookie-helper")
def cookie_helper(download: int = 0):
    """返回本机取 cookie 的助手脚本（进阶方式，需要装 Python）。"""
    p = os.path.join(BASE_DIR, "tools", "get_muse_cookie.py")
    if not os.path.isfile(p):
        raise HTTPException(404, "助手脚本缺失")
    headers = {}
    if download:
        headers["Content-Disposition"] = 'attachment; filename="get_muse_cookie.py"'
    return FileResponse(p, media_type="text/x-python", headers=headers)


@app.get("/admin/extension")
def extension_zip():
    """把浏览器扩展打包成 zip 返回（推荐方式，零命令行）。

    用户下载后解压 → chrome://extensions 开发者模式加载 → 点一下就导入 cookie。
    """
    src = os.path.join(BASE_DIR, "extension")
    if not os.path.isdir(src):
        raise HTTPException(404, "扩展目录缺失")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, dirs, names in os.walk(src):
            dirs.sort()
            for name in sorted(names):
                p = os.path.join(root, name)
                relative_path = os.path.relpath(p, src).replace(os.sep, "/")
                z.write(p, f"muse2api-extension/{relative_path}")
    buf.seek(0)
    return Response(
        content=buf.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition":
                 'attachment; filename="muse2api-extension.zip"',
                 "Cache-Control": "no-store"})


@app.get("/admin/extension/files")
def extension_files():
    """列出扩展目录内容（前端展示用）。"""
    src = os.path.join(BASE_DIR, "extension")
    if not os.path.isdir(src):
        raise HTTPException(404, "扩展目录缺失")
    files = []
    for root, dirs, names in os.walk(src):
        dirs.sort()
        for name in sorted(names):
            p = os.path.join(root, name)
            files.append(os.path.relpath(p, src).replace(os.sep, "/"))
    return {"files": sorted(files)}


# ------------------------- 管理：任务 / 媒体 -------------------------
@app.get("/admin/tasks")
def admin_tasks(limit: int = 50, _=Depends(auth)):
    return {"tasks": store.list_tasks(limit)}


@app.delete("/admin/tasks/{tid}")
def del_task(tid: str, _=Depends(auth)):
    t = store.get_task(tid)
    if t and t.get("status") in ("queued", "processing"):
        # worker 还在跑：删掉记录只会让结果无人认领、轮询方收到 404
        raise HTTPException(409, "任务尚未结束，不能删除；请等它完成或失败后再删")
    if not store.delete_task(tid):
        raise HTTPException(404, "任务不存在")
    return {"deleted": True}


@app.post("/admin/tasks/clear")
def clear_tasks(payload: dict = Body(default={}), _=Depends(auth)):
    try:
        keep = int(payload.get("keep") or 0)
    except (TypeError, ValueError):
        raise HTTPException(400, "keep 必须是非负整数") from None
    if keep < 0:
        raise HTTPException(400, "keep 必须是非负整数")
    return {"removed": store.clear_tasks(keep)}


@app.get("/admin/media")
def admin_media(_=Depends(auth)):
    d = CFG.media_dir
    items = []
    if os.path.isdir(d):
        for name in os.listdir(d):
            p = os.path.join(d, name)
            if not os.path.isfile(p):
                continue
            ext = os.path.splitext(name)[1].lower()
            items.append({"name": name, "url": media_url(name), "bytes": os.path.getsize(p),
                          "mtime": int(os.path.getmtime(p)),
                          "kind": "video" if ext in (".mp4", ".webm", ".mov") else "image"})
    items.sort(key=lambda x: x["mtime"], reverse=True)
    return {"media": items, "count": len(items)}


@app.post("/admin/media/delete")
def delete_media(payload: dict = Body(default={}), _=Depends(auth)):
    names = payload.get("names", [])
    if isinstance(names, str):
        names = [names]
    if not isinstance(names, list):
        raise HTTPException(400, "names 必须是文件名数组")
    d = CFG.media_dir
    removed = 0
    errors = []
    for name in names:
        if not isinstance(name, str) or not _safe_media_name(name):
            errors.append(f"非法文件名: {name}")
            continue
        p = os.path.join(d, name)
        if os.path.isfile(p):
            try:
                os.remove(p)
                removed += 1
            except OSError as e:
                errors.append(f"{name}: {str(e)}")
    return {"removed": removed, "errors": errors}


@app.delete("/admin/media/{name}")
def delete_single_media(name: str, _=Depends(auth)):
    if not _safe_media_name(name):
        raise HTTPException(400, "非法文件名")
    p = os.path.join(CFG.media_dir, name)
    if not os.path.isfile(p):
        raise HTTPException(404, "文件不存在")
    try:
        os.remove(p)
        return {"status": "ok", "deleted": name}
    except OSError as e:
        raise HTTPException(500, f"删除失败: {e}")


# ------------------------- 前端页面 -------------------------
def _admin_html() -> str:
    p = os.path.join(BASE_DIR, "admin.html")
    try:
        with open(p, encoding="utf-8") as f:
            return f.read()
    except OSError:
        return ("<!doctype html><meta charset=utf-8><body style='background:#0b0f19;"
                "color:#e6e8ee;font-family:system-ui;padding:40px'>"
                "<h2>muse2api</h2><p>管理页面文件 admin.html 缺失。</p>"
                "<p>接口可用：<code>/v1/images/generations</code>、"
                "<code>/v1/videos</code></p></body>")


@app.get("/", response_class=HTMLResponse)
def index():
    return _admin_html()


@app.get("/admin", response_class=HTMLResponse)
def admin_page():
    return _admin_html()



# ------------------------- 账号自动保活与静默续期 -------------------------
KEEPALIVE_LOCK = asyncio.Lock()
KEEPALIVE_STATE = {
    "running": False,
    "last_run": None,
    "last_result": None,
    "next_run": None,
}


# 单账号保活与全池保活共用：同一时刻只有一个保活续签在跑
KEEPALIVE_SYNC_LOCK = threading.Lock()


def _probe_account_sync(aid: str, check_quota: bool = False) -> dict:
    """通过 muse.ai/api/session 触发 Meta 网关签发新 hatch_vml (+48h) / hatch_sess (+30d) 并唤醒云端 VM。

    软隐身开启（默认）时与生成主链路一致：在浏览器页面内同源续签并唤醒 VM，不用 Python requests；
    浏览器槽位全忙时跳过本次。关闭软隐身时走 HTTP 续签兜底。
    先租账号：账号正在生成时跳过本次，不与生成同时续签同一会话（可能互踢）。"""
    acc = store.get_account(aid)
    if not acc or not acc.get("cookies"):
        return {"ok": False, "id": aid, "label": (acc or {}).get("label", aid), "error": "账号无有效 cookie"}
    try:
        ACCOUNT_POOL.acquire(account_id=aid, timeout=0.1, record_use=False, allow_disabled=True)
    except (MuseGenerationError, MuseAuthError):
        return {"ok": False, "id": aid, "label": acc.get("label", aid), "skipped": True,
                "error": "账号正在使用中，本次保活跳过"}
    try:
        with KEEPALIVE_SYNC_LOCK:
            if getattr(CFG, "stealth", True):
                return _probe_in_browser(aid)
            return _probe_leased_account(aid, check_quota)
    finally:
        ACCOUNT_POOL.release(aid)


def _renew_unconfirmed() -> str | None:
    """页面内续签没有确认会话有效时返回诊断；软隐身关闭（未走页面内续签）或已确认时返回 None。"""
    if not getattr(CFG, "stealth", True):
        return None
    r = getattr(engine, "last_renew", None)
    if not r:
        return "页面内 /api/session 续签未执行"
    return None if r.get("assigned") else r.get("note") or "页面内 /api/session 续签未确认"


def _probe_in_browser(aid: str) -> dict:
    """软隐身保活：账号已由调用方租住；拿一个空闲浏览器槽位，加载页面完成同源续签与 VM 唤醒，回写 cookie。"""
    acc = store.get_account(aid) or {}
    label = acc.get("label", aid)
    if not GEN_LOCK.acquire(blocking=False):
        return {"ok": False, "id": aid, "label": label, "skipped": True,
                "error": "浏览器槽位繁忙，本次保活跳过"}
    try:
        engine.loaded_sess = None  # 由 refresh → ensure_page 记下本次保活实际用的会话
        engine.set_account_meta(aid, acc.get("proxy"), acc.get("timezone"), acc.get("lang"))
        engine.start()
        engine.refresh(acc["cookies"], acc.get("cookies_exp"), account_id=aid)
        unconfirmed = _renew_unconfirmed()
        if unconfirmed:
            # 403 / 5xx / 非 JSON / 未 assigned：本次保活未确认，保留原状态，不假报成功
            store.touch_keepalive(aid, None, f"保活未确认（保留账号状态）: {unconfirmed}")
            return {"ok": False, "id": aid, "label": label, "error": unconfirmed}
        _sync_cookies(aid)
        if _session_replaced(aid):
            return {"ok": False, "id": aid, "label": label, "skipped": True,
                    "error": "保活期间账号 cookie 已被更新，本次结果不写入"}
        store.touch_keepalive(aid, True, "会话有效 · 自动保活（浏览器内续签）")
        updated = store.get_account(aid) or {}
        return {"ok": True, "id": aid, "label": label, "via": "browser",
                "expires_at": updated.get("expires_at"), "quota": updated.get("quota")}
    except MuseAuthError as exc:
        if _session_replaced(aid):
            return {"ok": False, "id": aid, "label": label, "skipped": True,
                    "error": "保活期间账号 cookie 已被更新，旧会话的认证失败不写入"}
        store.touch_keepalive(aid, False, f"保活认证失败: {str(exc)[:200]}")
        return {"ok": False, "id": aid, "label": label, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        store.touch_keepalive(aid, None, f"保活未确认（保留账号状态）: {str(exc)[:200]}")
        return {"ok": False, "id": aid, "label": label, "error": str(exc)}
    finally:
        try:
            engine.close_current_page()
        finally:
            GEN_LOCK.release()


def _probe_leased_account(aid: str, check_quota: bool) -> dict:
    acc = store.get_account(aid)
    if not acc or not acc.get("cookies"):
        return {"ok": False, "id": aid, "label": (acc or {}).get("label", aid), "error": "账号无有效 cookie"}
    try:
        before_cookies = dict(acc["cookies"])
        before_exp = dict(acc.get("cookies_exp") or {})
        res = engine.renew_session_http(before_cookies, before_exp,
                                        wake_vm=True, proxy=acc.get("proxy"))
        # 只把续签实际改动的条目合并进账号的最新值，不用续签前的快照整份覆盖
        changed = {k: v for k, v in res["cookies"].items() if before_cookies.get(k) != v}
        changed_exp = {k: v for k, v in res["cookies_exp"].items() if before_exp.get(k) != v}
        if _session_replaced(aid, before_cookies.get("hatch_sess") or ""):
            # 续签期间管理员换上了新会话：旧会话的续签结果不合并、不写状态
            return {"ok": False, "id": aid, "label": acc.get("label", aid), "skipped": True,
                    "error": "保活期间账号 cookie 已被更新，本次结果不写入"}
        latest = store.get_account(aid) or acc
        store.update_account(
            aid,
            cookies={**(latest.get("cookies") or {}), **changed},
            cookies_exp={**(latest.get("cookies_exp") or {}), **changed_exp},
            ok=True if res.get("ok") else False,
            synced_at=int(time.time()),
        )
        vm_state = res.get("vm_state") or "RUNNING"
        store.touch_keepalive(aid, True, f"会话有效 · 自动保活 (VM: {vm_state})")
        quota = acc.get("quota")
        if check_quota and GEN_LOCK.acquire(blocking=False):
            try:
                engine.set_account_meta(aid, acc.get("proxy"),
                                        acc.get("timezone"), acc.get("lang"))
                engine.start()
                quota = engine.quota(res["cookies"], res["cookies_exp"],
                                     account_id=aid)
                if not quota.get("found"):
                    raise MuseGenerationError("额度面板未读到有效数据；旧缓存未覆盖")
                quota["checked_at"] = int(time.time())
                store.update_account(aid, quota=quota)
            except Exception as qe:  # noqa: BLE001
                log.warning("读取账号 %s 额度失败: %s", aid, qe)
            finally:
                GEN_LOCK.release()
        updated = store.get_account(aid) or {}
        return {
            "ok": True,
            "id": aid,
            "label": acc.get("label", aid),
            "vm_id": res.get("vm_id"),
            "vm_state": vm_state,
            "wake_ok": res.get("wake_ok"),
            "expires_at": updated.get("expires_at"),
            "quota": quota,
        }
    except MuseAuthError as exc:
        if _session_replaced(aid, (acc.get("cookies") or {}).get("hatch_sess") or ""):
            return {"ok": False, "id": aid, "label": acc.get("label", aid), "skipped": True,
                    "error": "保活期间账号 cookie 已被更新，旧会话的认证失败不写入"}
        store.touch_keepalive(aid, False, f"保活认证失败: {str(exc)[:200]}")
        return {"ok": False, "id": aid, "label": acc.get("label", aid), "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        store.touch_keepalive(aid, None, f"保活未确认（保留账号状态）: {str(exc)[:200]}")
        return {"ok": False, "id": aid, "label": acc.get("label", aid), "error": str(exc)}


async def run_keepalive_all(force: bool = False) -> dict:
    """执行账号保活：force=True 时强制刷新所有启用账号，否则刷新临期/未检账号并保持 VM 热备。"""
    async with KEEPALIVE_LOCK:
        now = int(time.time())
        KEEPALIVE_STATE["running"] = True
        KEEPALIVE_STATE["last_run"] = now
        results = []
        skipped = []

        try:
            include_disabled = getattr(CFG, "keepalive_disabled_accounts", False)
            accounts = [a for a in store.list_accounts() if include_disabled or a.get("enabled", True)]
            for a in accounts:
                aid = a["id"]
                label = a.get("label", aid)
                exp_at = a.get("expires_at")
                last_ka = a.get("last_keepalive") or 0

                needs_run = (
                    force
                    or not last_ka
                    or ((now - last_ka) > 6 * 3600 and
                        (not exp_at or exp_at - now < 36 * 3600 or
                         a.get("ok") is not True))
                )
                # 从没成功过 / 已判为失效的账号：失败后按 checked_at 退避，不每 15 分钟拉一次浏览器
                # ok=True 但从没保活成功过（出口访问 /api/session 一直未确认）同样退避，
                # 否则 not last_ka 每轮成立，每 15 分钟拉一次浏览器
                retry_after = 6 * 3600 if a.get("ok") is False else 3600
                if (needs_run and not force and (a.get("ok") is not True or not last_ka)
                        and now - (a.get("checked_at") or 0) < retry_after):
                    skipped.append({"id": aid, "label": label, "reason": "近期检测未成功，稍后再试"})
                    continue
                if not needs_run:
                    skipped.append({"id": aid, "label": label, "reason": "会话充足且近期已保活"})
                    continue

                log.info("【自动保活】正在为账号 %s (%s) 执行静默续期与 VM 唤醒...", label, aid)
                res = await asyncio.to_thread(_probe_account_sync, aid, False)
                results.append(res)
                log.info("【自动保活】账号 %s 执行结果: ok=%s expires_at=%s", label, res.get("ok"), res.get("expires_at"))
                await asyncio.sleep(0.5)

            # 探测过的账号再按结果分：成功、因忙碌跳过、未成功（失败或未确认）
            busy = [r for r in results if r.get("skipped")]
            ok_n = sum(1 for r in results if r.get("ok"))
            summary = {
                "checked_at": now,
                "refreshed_count": len(results),   # 实际探测的账号数（含失败），兼容旧字段
                "refreshed": results,
                "ok_count": ok_n,
                "failed_count": len(results) - ok_n - len(busy),
                "skipped_count": len(skipped) + len(busy),
                "skipped": skipped + [{"id": r.get("id"), "label": r.get("label"), "reason": r.get("error")}
                                      for r in busy],
            }
            KEEPALIVE_STATE["last_result"] = summary
            KEEPALIVE_STATE["next_run"] = now + 900
            return summary
        finally:
            KEEPALIVE_STATE["running"] = False


def _warmup_browser_sync():
    """后台预热：拉起 Chromium，并用首个可用账号完整加载一次 muse.ai（续签会话、唤醒云端 VM）。

    预热用的页面在结束时关闭（页面按线程持有，请求线程无法复用），所以首条请求仍要新开页面；
    省下的是浏览器启动与冷 VM 唤醒的时间。"""
    if not GEN_LOCK.acquire(blocking=False):
        return
    acc = None
    try:
        acc = ACCOUNT_POOL.acquire(rotate=False, timeout=0.1, record_use=False,
                                   allow_quota_paused=True)
        engine.set_account_meta(acc["id"], acc.get("proxy"),
                                acc.get("timezone"), acc.get("lang"))
        log.info("【浏览器预热】正在用账号 %s (%s) 预热浏览器与云端 VM...", acc.get("label"), acc["id"])
        refreshed = _renew_and_persist(acc["id"], wake_vm=True, force=False) or acc
        engine.start()
        engine.ensure_page(refreshed["cookies"], refreshed.get("cookies_exp"), account_id=acc["id"])
        log.info("【浏览器预热】账号 %s (%s) 页面加载成功，浏览器与 VM 已就绪", acc.get("label"), acc["id"])
    except Exception as exc:  # noqa: BLE001
        log.warning("【浏览器预热】预热异常: %s", exc)
    finally:
        # Warmup runs in a worker thread; do not retain its account reservation or page.
        engine.close_current_page()
        if acc:
            ACCOUNT_POOL.release(acc["id"])
        GEN_LOCK.release()


async def _keepalive_loop():
    """后台常驻守护任务：每 15 分钟轮询一次账号健康状态并保持 VM 热备。"""
    log.info("【自动保活守护进程】已启动，检测周期: 15 分钟")
    await asyncio.sleep(1)
    try:
        await asyncio.to_thread(_warmup_browser_sync)
    except Exception as e:  # noqa: BLE001
        log.warning("【浏览器预热】异常: %s", e)
    while True:
        try:
            await run_keepalive_all(force=False)
        except Exception as e:  # noqa: BLE001
            log.error("【自动保活守护进程】轮询异常: %s", e)
        await asyncio.sleep(900)


@app.post("/admin/accounts/keepalive")
async def trigger_keepalive_all(force: bool = True, _=Depends(auth)):
    """管理员手动触发一次全账号保活续期。"""
    if KEEPALIVE_STATE["running"]:
        return {"status": "busy", "message": "保活任务正在执行中，请稍候"}
    return await run_keepalive_all(force=force)


@app.post("/admin/accounts/{aid}/keepalive")
async def trigger_keepalive_single(aid: str, _=Depends(auth)):
    """手动针对单个账号执行保活续期。"""
    return await asyncio.to_thread(_probe_account_sync, aid, False)


@app.get("/admin/keepalive/status")
def get_keepalive_status(_=Depends(auth)):
    """获取保活守护协程状态。"""
    return KEEPALIVE_STATE


# ------------------------- 仓库实时更新检测、通知与一键在线升级 -------------------------
# 仓库由 CFG.repo（环境变量 MUSE2API_REPO）决定，默认指向本二开仓库；
# 换 owner/name 只改配置即可，下面所有 GitHub URL 均由它派生。
REPO = CFG.repo
REPO_OWNER = REPO.split("/", 1)[0]
REPO_URL = f"https://github.com/{REPO}"
TRACKED_REPO_PATHS = [
    "app.py", "engine.py", "store.py", "cdp.py", "config.py", "proxyrelay.py",
    "admin.html", "README.md", "version.json", "requirements.txt",
    "Dockerfile", "docker-compose.yml", ".env.example", ".gitignore",
    "LICENSE", "extension", "deploy", "tools",
]
_UPDATE_CACHE: dict[str, Any] = {"ts": 0.0, "data": None}


def _read_env_key(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if val:
        return val
    found = ""
    for path in dotenv_paths(CFG.base_dir):  # 后面的 data/.env 优先
        try:
            with open(path, encoding="utf-8") as f:
                for ln in f.read().splitlines():
                    s = ln.strip()
                    if s.startswith(key + "="):
                        found = s.split("=", 1)[1].strip().strip("'\"") or found
        except OSError:
            pass
    return found


def _read_local_version() -> dict:
    p = os.path.join(BASE_DIR, "version.json")
    try:
        with open(p, encoding="utf-8") as f:
            obj = json.load(f)
            if isinstance(obj, dict):
                return obj
    except Exception:
        pass
    return {"version": "1.5.0", "highlights": []}


def _image_build_sha() -> str:
    """Docker 镜像构建时写入的提交 SHA（Dockerfile 的 GIT_SHA 构建参数）；非镜像部署为空串。"""
    try:
        with open(os.path.join(BASE_DIR, ".build_sha"), encoding="utf-8") as f:
            return f.read().strip()
    except OSError:
        return ""


def _installed_sha_file() -> str:
    return os.path.join(CFG.data_dir, ".installed_sha")


def _git(args: list[str], timeout: int = 30, token: str = ""):
    """token 非空时只为这一条命令经环境变量注入 GitHub 鉴权头，不写进 .git/config 或命令行参数。"""
    import subprocess
    env = dict(os.environ)
    env["GIT_TERMINAL_PROMPT"] = "0"
    if token:
        basic = base64.b64encode(f"x-access-token:{token}".encode()).decode()
        env["GIT_CONFIG_COUNT"] = "1"
        env["GIT_CONFIG_KEY_0"] = "http.https://github.com/.extraheader"
        env["GIT_CONFIG_VALUE_0"] = f"AUTHORIZATION: basic {basic}"
    return subprocess.run(
        ["git", "-c", f"safe.directory={BASE_DIR}", *args],
        cwd=BASE_DIR,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


def _ensure_git_repo(token: str = ""):
    """确保 BASE_DIR 已初始化为绑定配置仓库（CFG.repo / MUSE2API_REPO）的 Git 仓库。"""
    git_dir = os.path.join(BASE_DIR, ".git")
    # remote URL 不含凭据：token 只在 fetch/push 时经 _git(token=...) 临时注入（旧版写进 URL 的 token 在这里被覆盖掉）
    remote_url = f"{REPO_URL}.git"
    if not os.path.isdir(git_dir):
        _git(["init", "-b", "main"])
        _git(["remote", "add", "origin", remote_url])
        _git(["fetch", "origin", "main"], timeout=45, token=token)
        _git(["reset", "--mixed", "origin/main"])
        # 只给新建的仓库设提交身份；已有仓库保留使用者自己的 user.name / user.email
        _git(["config", "user.name", REPO_OWNER])
        _git(["config", "user.email", f"{REPO_OWNER}@users.noreply.github.com"])
        return
    cur = _git(["remote", "get-url", "origin"])
    cur_url = (cur.stdout or "").strip() if cur.returncode == 0 else ""
    if not cur_url:
        _git(["remote", "add", "origin", remote_url])
        return
    norm = re.sub(r"^https://[^/@]+@github\.com/", "https://github.com/", cur_url)
    if norm != cur_url:
        # 旧版把 token 写进了 remote URL：去掉凭据，其余保持原样
        _git(["remote", "set-url", "origin", norm])
    if _github_repo_of(norm) != REPO.lower():
        raise HTTPException(409, f"本地仓库的 origin（{_github_repo_of(norm) or norm}）不是配置的仓库 {REPO}，"
                                 "已取消；请检查 MUSE2API_REPO 或手动处理 git remote")


def _github_repo_of(url: str) -> str:
    """从 https / ssh 形式的 GitHub 远端地址取出 owner/name（小写）；不是 GitHub 地址返回空串。"""
    m = re.match(r"^(?:https://github\.com/|git@github\.com:|ssh://git@github\.com/)([^/]+/[^/]+?)(?:\.git)?/?$",
                 url.strip(), re.I)
    return m.group(1).lower() if m else ""


def _check_update_sync(force: bool = False, use_token: bool = True) -> dict:
    """检测 GitHub 仓库 (CFG.repo) 是否有新版本或新提交。
    默认缓存 90 秒，防止频繁刷新触发 GitHub API 速率限制。
    use_token=False（匿名调用方）时不带 GITHUB_TOKEN 请求 GitHub API。"""
    now = time.time()
    if not force and _UPDATE_CACHE["data"] and (now - _UPDATE_CACHE["ts"]) < 90:
        return _UPDATE_CACHE["data"]

    import requests
    token = _read_env_key("GITHUB_TOKEN") if use_token else ""
    local_ver_obj = _read_local_version()
    local_version = str(local_ver_obj.get("version") or "1.5.0")

    has_git = os.path.isdir(os.path.join(BASE_DIR, ".git"))
    local_sha, local_msg, local_ts = "", "", 0
    if has_git:
        try:
            r_sha = _git(["rev-parse", "--short", "HEAD"])
            if r_sha.returncode == 0:
                local_sha = r_sha.stdout.strip()[:7]
            r_log = _git(["log", "-1", "--format=%s||%ct"])
            if r_log.returncode == 0 and "||" in r_log.stdout:
                parts = r_log.stdout.strip().split("||", 1)
                local_msg = parts[0]
                local_ts = int(parts[1])
        except Exception:
            pass

    image_sha = _image_build_sha()
    if not local_sha and os.path.isfile(_installed_sha_file()):
        # 在线升级写在数据卷里的 SHA 只对写入它的那个镜像有效：容器重建（换回镜像里的代码）后作废
        try:
            with open(_installed_sha_file(), encoding="utf-8") as f:
                rec_sha, _, rec_image = f.read().strip().partition(" ")
            if not image_sha or rec_image == image_sha:
                local_sha = rec_sha[:7]
        except OSError:
            pass
    if not local_sha and image_sha:
        local_sha = image_sha[:7]  # 镜像构建时写入的真实提交

    remote_version = local_version
    highlights = list(local_ver_obj.get("highlights") or [])
    highlights_en = list(local_ver_obj.get("highlights_en") or [])
    try:
        rv = requests.get(
            f"https://raw.githubusercontent.com/{REPO}/main/version.json?t={int(now)}",
            timeout=6,
        )
        if rv.status_code == 200:
            rvj = rv.json()
            if isinstance(rvj, dict):
                remote_version = str(rvj.get("version") or remote_version)
                if rvj.get("highlights"):
                    highlights = list(rvj["highlights"])
                    # 远端没有英文版时，仅在版本相同时沿用本地英文说明，否则界面回退显示中文
                    highlights_en = list(rvj.get("highlights_en")
                                         or (highlights_en if remote_version == local_version else []))
    except Exception:
        pass

    remote_sha, remote_msg, remote_time = "", "", ""
    recent_commits = []
    try:
        headers = {"Accept": "application/vnd.github.v3+json", "User-Agent": "muse2api-updater"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        resp = requests.get(
            f"https://api.github.com/repos/{REPO}/commits?sha=main&per_page=5",
            headers=headers,
            timeout=6,
        )
        if resp.status_code == 200 and isinstance(resp.json(), list):
            commits = resp.json()
            for idx, c in enumerate(commits):
                sha7 = (c.get("sha") or "")[:7]
                c_msg = ((c.get("commit") or {}).get("message") or "").splitlines()[0]
                c_date = (((c.get("commit") or {}).get("committer") or {}).get("date") or "")
                c_url = c.get("html_url") or f"{REPO_URL}/commit/{sha7}"
                if idx == 0:
                    remote_sha = sha7
                    remote_msg = c_msg
                    remote_time = c_date
                recent_commits.append({
                    "sha": sha7,
                    "message": c_msg,
                    "date": c_date,
                    "url": c_url,
                })
    except Exception:
        pass

    # 若用户通过 Docker/ZIP 部署（无 .git）且首次运行版本一致，记录初始基准 SHA
    if not local_sha and remote_sha and local_version == remote_version:
        # 不知道本地是哪个提交（非 Git、旧镜像没写入构建 SHA）：版本号相同时按远端算，只显示不落盘，
        # 免得把猜测当成事实一直保留下去
        local_sha = remote_sha

    def _parse_v(v: str) -> tuple[int, ...]:
        try:
            return tuple(int(x) for x in re.findall(r"\d+", v))
        except Exception:
            return (0,)

    has_update = False
    if remote_sha and local_sha:
        has_update = (remote_sha != local_sha)
    elif remote_version and local_version:
        has_update = _parse_v(remote_version) > _parse_v(local_version)

    data = {
        "repo": REPO,
        "repo_url": REPO_URL,
        "has_git": has_git,
        "local_version": local_version,
        "remote_version": remote_version,
        "local_sha": local_sha,
        "local_msg": local_msg,
        "local_ts": local_ts,
        "remote_sha": remote_sha,
        "remote_msg": remote_msg,
        "remote_time": remote_time,
        "has_update": has_update,
        "up_to_date": not has_update and bool(remote_sha or remote_version),
        "highlights": highlights,
        "highlights_en": highlights_en,
        "recent_commits": recent_commits,
        "checked_at": int(now),
    }
    _UPDATE_CACHE["ts"] = now
    _UPDATE_CACHE["data"] = data
    return data


def _install_requirements() -> None:
    """升级后、重启前按新的 requirements.txt 补装依赖；失败就报错，不进入「重启即 ImportError」的循环。"""
    import subprocess
    req = os.path.join(BASE_DIR, "requirements.txt")
    if not os.path.isfile(req):
        return
    res = subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-r", req],
                         cwd=BASE_DIR, capture_output=True, text=True, timeout=600)
    if res.returncode != 0:
        raise HTTPException(500, "代码已更新，但安装依赖失败，未重启服务："
                                 + (res.stderr or res.stdout or "").strip()[-300:])


def _upgrade_from_github_sync() -> dict:
    """从 GitHub 拉取最新代码覆盖核心文件（兼容 Git 与无 Git 的 Docker/ZIP 环境），绝不触碰 .env 与 data/。"""
    import requests
    import tarfile

    token = _read_env_key("GITHUB_TOKEN")
    upgraded_via = ""
    had_git = os.path.isdir(os.path.join(BASE_DIR, ".git"))
    if had_git:
        # 已有 Git 仓库：不静默丢弃本地改动或本地提交；任何一步失败都直接报错，不回退到 tarball 覆盖
        _ensure_git_repo(token)
        f_res = _git(["fetch", "origin", "main"], timeout=45, token=token)
        if f_res.returncode != 0:
            raise HTTPException(502, f"Git fetch 失败: {(f_res.stderr or f_res.stdout or '').strip()[:300]}")
        dirty = _git(["status", "--porcelain", "--untracked-files=no"])
        if dirty.returncode != 0 or dirty.stdout.strip():
            raise HTTPException(409, "本地有未提交的改动，已取消升级；请先提交或还原后再升级")
        ahead = _git(["rev-list", "--count", "origin/main..HEAD"])
        if ahead.returncode != 0 or ahead.stdout.strip() not in ("", "0"):
            raise HTTPException(409, "本地有未推送到上游的提交，已取消升级以免丢失；请先处理后再升级")
        m_res = _git(["merge", "--ff-only", "origin/main"])
        if m_res.returncode != 0:
            raise HTTPException(500, f"Git 快进合并失败: {(m_res.stderr or m_res.stdout or '').strip()[:300]}")
        upgraded_via = "git"
    else:
        try:
            _ensure_git_repo(token)
            f_res = _git(["fetch", "origin", "main"], timeout=45, token=token)
            if f_res.returncode == 0:
                c_res = _git(["checkout", "-f", "origin/main", "--", "."])
                r_res = _git(["reset", "--mixed", "origin/main"])
                if c_res.returncode == 0 and r_res.returncode == 0:
                    upgraded_via = "git"
        except Exception as e:
            log.warning("Git 拉取更新失败，将使用 Tarball 方式更新: %s", e)
        if not upgraded_via:
            # 本次新建的 .git 没建成（fetch / reset 失败，HEAD 未建立）：删掉，回到无 Git 状态，
            # 否则下次升级会走「已有 Git」分支，被 unborn HEAD 误判成有本地提交而一直 409
            import shutil
            shutil.rmtree(os.path.join(BASE_DIR, ".git"), ignore_errors=True)

    if not upgraded_via:
        resp = requests.get(
            f"https://codeload.github.com/{REPO}/tar.gz/refs/heads/main",
            timeout=60,
        )
        if resp.status_code != 200:
            raise HTTPException(502, f"下载 GitHub 更新包失败 (HTTP {resp.status_code})")
        protected_files = {".env", "data/accounts.json", "data/tasks.json"}
        with tarfile.open(fileobj=io.BytesIO(resp.content), mode="r:gz") as tar:
            for member in tar.getmembers():
                parts = member.name.split("/", 1)
                if len(parts) < 2 or not parts[1]:
                    continue
                rel = parts[1].replace("\\", "/")
                if ".." in rel or rel in protected_files:
                    continue
                target_path = os.path.join(BASE_DIR, rel)
                if member.isdir():
                    os.makedirs(target_path, exist_ok=True)
                elif member.isfile():
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)
                    fobj = tar.extractfile(member)
                    if fobj is not None:
                        with open(target_path, "wb") as out_f:
                            out_f.write(fobj.read())
        upgraded_via = "tarball"

    _install_requirements()
    _UPDATE_CACHE["ts"] = 0.0
    status = _check_update_sync(force=True)
    if status.get("remote_sha"):
        try:
            with open(_installed_sha_file(), "w", encoding="utf-8") as f:
                # 连同当前镜像的构建 SHA 一起记：容器重建回到镜像代码后，这条记录不再生效
                f.write(f"{status['remote_sha']} {_image_build_sha()}".strip())
            status["local_sha"] = status["remote_sha"]
            status["has_update"] = False
            status["up_to_date"] = True
        except OSError:
            pass
    return {
        "ok": True,
        "via": upgraded_via,
        "message": f"已成功更新至最新版本 {status.get('remote_version')} ({status.get('remote_sha')})",
        "status": status,
    }


@app.get("/admin/update/check")
@app.get("/admin/repo/status")
async def admin_check_update(force: bool = False,
                             authorization: str | None = Header(default=None)):
    """供所有已部署节点实时检测 GitHub 官方仓库是否有新版本更新。

    无需鉴权即可读缓存结果；force（绕过缓存）与使用 GITHUB_TOKEN 只对已鉴权的管理员生效，
    匿名请求不能借服务器的 Token 反复打 GitHub API。"""
    try:
        authed = _check_key(authorization, admin=True)
    except HTTPException:
        authed = False
    return await asyncio.to_thread(_check_update_sync, force and authed, authed)


@app.post("/admin/update/upgrade")
@app.post("/admin/repo/pull")
async def admin_upgrade_now(payload: dict = Body(default={}), _=Depends(auth)):
    """一键从 GitHub 官方仓库拉取最新更新并自动平滑重启服务。

    重启会断开进行中的生成：同步请求失败，异步任务在重启后被标记为中断（提示词可能已计费）。
    所以有进行中的生成或未结束的任务时，先拉代码之前就返回 409，调用方确认后传 force=true 再强制执行。"""
    restart = payload.get("restart", True) if isinstance(payload, dict) else True
    force = bool(payload.get("force")) if isinstance(payload, dict) else False
    if restart and not force:
        active = sum(1 for t in list(store.tasks.values()) if t.get("status") in ("queued", "processing"))
        if GEN_LOCK.locked() or active:
            raise HTTPException(409, f"当前有进行中的生成（未结束的任务 {active} 个），重启会中断它们，"
                                     "已扣的额度不会退回；请等任务结束后再升级，或确认后强制升级")
    res = await asyncio.to_thread(_upgrade_from_github_sync)
    if restart:
        def _delayed_restart():
            time.sleep(0.5)
            try:
                engine.stop()
            except Exception:
                pass
            os._exit(0)
        threading.Thread(target=_delayed_restart, daemon=True).start()
    return res


@app.post("/admin/repo/push")
async def admin_repo_push(payload: dict = Body(default={}), _=Depends(auth)):
    """维护者专用：将当前节点核心代码推送到 GitHub 仓库（自动过滤 .env 与 data 目录）。"""
    msg = (payload.get("message") or "").strip() or f"chore: sync update ({time.strftime('%Y-%m-%d %H:%M:%S')})"
    new_token = (payload.get("github_token") or "").strip()
    if new_token:
        _persist_env("GITHUB_TOKEN", new_token)
        os.environ["GITHUB_TOKEN"] = new_token
    token = new_token or _read_env_key("GITHUB_TOKEN")

    def _do_push():
        _ensure_git_repo(token)
        existing_paths = [p for p in TRACKED_REPO_PATHS if os.path.exists(os.path.join(BASE_DIR, p))]
        _git(["add", "--", *existing_paths])
        st = _git(["status", "--porcelain", "--", *existing_paths])
        committed = False
        if st.stdout.strip():
            c_res = _git(["commit", "-m", msg])
            if c_res.returncode != 0:
                raise HTTPException(500, f"Git commit 失败: {c_res.stderr or c_res.stdout}")
            committed = True
        p_res = _git(["push", "origin", "HEAD:main"], timeout=60, token=token)
        if p_res.returncode != 0:
            err = (p_res.stderr or p_res.stdout or "").strip()
            raise HTTPException(500, f"Git push 失败: {err[:300]}")
        _UPDATE_CACHE["ts"] = 0.0
        status = _check_update_sync(force=True)
        return {
            "ok": True,
            "committed": committed,
            "message": "已成功提交并推送到 GitHub 仓库",
            "status": status,
        }

    return await asyncio.to_thread(_do_push)


_PLACEHOLDER_KEYS = {"m2a_your_secret_admin_key_here", "changeme", "your_api_key"}


@app.on_event("startup")
async def _startup():
    for task in list(store.tasks.values()):
        if task.get("kind") in ("image", "video") and task.get("status") in ("queued", "processing"):
            store.update_task(task["id"], status="failed", error="服务重启中断了任务，请重新提交")
    if not CFG.api_key or CFG.api_key in _PLACEHOLDER_KEYS:
        # 示例配置里公开的占位 Key 等同于没有设置：它能管理、升级和推送，不能拿来当正式 Key
        import secrets
        placeholder = bool(CFG.api_key)
        new_key = "m2a_" + secrets.token_hex(24)
        CFG.api_key = new_key
        _persist_env("MUSE2API_KEY", new_key)
        log.info("🔑 %s，已自动生成初始密钥: %s",
                 "MUSE2API_KEY 是示例占位值" if placeholder else "未检测到 MUSE2API_KEY", new_key)
    if (CFG.admin_key or "").strip() in _PLACEHOLDER_KEYS:
        # 占位值本来就会被 auth 拒绝：留着它管理员永远 401 还没有提示。忽略它，退回共用 MUSE2API_KEY
        log.error("MUSE2API_ADMIN_KEY 是示例占位值，已忽略（/admin 改用 MUSE2API_KEY）；请改成你自己的管理 Key")
        CFG.admin_key = ""
    asyncio.create_task(_keepalive_loop())


@app.on_event("shutdown")
def _shutdown():
    engine.stop()


if __name__ == "__main__":
    # 统一入口：监听地址与端口取自 MUSE2API_HOST / MUSE2API_PORT（含 .env）。
    # 服务依赖进程内全局锁与单浏览器实例，只能单 worker。
    import uvicorn
    uvicorn.run(app, host=CFG.host, port=CFG.port)
