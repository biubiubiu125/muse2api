"""账号池与任务存储（JSON 落盘，原子写）。"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import threading
import time
import urllib.parse
import uuid

_LOCK = threading.Lock()

# 决定账号生死的核心 cookie（与 engine.ESSENTIAL_COOKIES 保持一致）
# 注：hatch_gw 为网关路由缓存（约1年），服务端续签与 VM 唤醒主要依赖 hatch_sess 和 hatch_vml，缺 gw 亦可正常分配路由
ESSENTIAL_COOKIES = ("hatch_sess", "hatch_vml")


def _mask_proxy(p):
    """隐藏代理账密，仅用于对外展示（功能路径用 get_account 读原值，不受影响）。"""
    if not p:
        return p
    try:
        u = urllib.parse.urlsplit(p if "://" in p else "http://" + p)
        if u.username:
            host = u.hostname or ""
            port = (":" + str(u.port)) if u.port else ""
            return (u.scheme or "http") + "://***@" + host + port
    except Exception:
        return "***"
    return p


def _is_pos(v) -> bool:
    try:
        return int(v) > 0
    except (TypeError, ValueError):
        return False


# hatch_vml 固定约 2 天，且不因使用而延长 —— 它就是决定账号寿命的那条 cookie。
# 实测对照：生成前后 synced_at 变了、也捞到新 cookie，但 4 条核心 cookie 的
# expires 一动不动。所以拿不到 hatch_vml 的 expires 时，按「导入时间 + 2 天」估算，
# 而不是退回到 hatch_native_auth_device 的 30 天（那个不决定生死，会误导用户）。
VML_TTL = 2 * 86400

# tasks.json 里最多保留的已结束任务数
MAX_TASKS = int(os.environ.get("MUSE2API_MAX_TASKS", "500"))


def min_expiry(cookies_exp: dict | None) -> int | None:
    """核心 cookie 里最早过期的那个（通用兜底，不区分哪条决定寿命）。"""
    if not cookies_exp:
        return None
    vals = []
    for name in ESSENTIAL_COOKIES:
        v = cookies_exp.get(name)
        try:
            v = int(v)
        except (TypeError, ValueError):
            continue
        if v > 0:
            vals.append(v)
    if not vals:
        for v in cookies_exp.values():
            try:
                v = int(v)
            except (TypeError, ValueError):
                continue
            if v > 0:
                vals.append(v)
    return min(vals) if vals else None


def account_expiry(cookies_exp: dict | None,
                   base_ts: int | None = None) -> int | None:
    """账号实际失效时间。

    优先级：
      1) hatch_vml 的真实 expires（最准）；
      2) 拿不到就用 base_ts(导入/同步时刻) + VML_TTL 估算 —— 因为寿命就是 2 天；
      3) 都没有再退回核心 cookie 最早过期。
    """
    ce = cookies_exp or {}
    vml = ce.get("hatch_vml")
    if _is_pos(vml):
        return int(vml)
    if base_ts:
        return int(base_ts) + VML_TTL
    return min_expiry(ce)


class StoreCorruptError(RuntimeError):
    """账号或任务文件无法解析。拒绝启动，绝不能当成空数据，否则下一次写入会永久覆盖原文件。"""


def _read(path: str, default):
    if not os.path.isfile(path):
        return default
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError) as exc:
        backup = f"{path}.corrupt-{int(time.time())}"
        try:
            shutil.copy2(path, backup)
        except OSError:
            backup = "（备份失败）"
        raise StoreCorruptError(
            f"无法解析 {path}（{type(exc).__name__}: {exc}）。原文件未改动，副本：{backup}。"
            "请修复或从备份恢复后再启动") from exc
    if not isinstance(data, type(default)):
        raise StoreCorruptError(f"{path} 的内容类型不对（应为 {type(default).__name__}），请检查后再启动")
    return data


def _write(path: str, obj):
    d = os.path.dirname(path)
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=1)
            f.flush()
            os.fsync(f.fileno())  # 断电或崩溃时不留下半截文件
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


class Store:
    def __init__(self, cfg):
        self.cfg = cfg
        self.accounts: list[dict] = _read(cfg.accounts_file, [])
        for a in self.accounts:
            ce = a.get("cookies_exp") or {}
            if _is_pos(ce.get("hatch_vml")):
                a["expires_at"] = int(ce["hatch_vml"])
        self.tasks: dict[str, dict] = _read(cfg.tasks_file, {})

    # ---------- 账号 ----------
    def add_account(self, cookies: dict, label: str = "",
                    cookies_exp: dict | None = None,
                    proxy: str | None = None, timezone: str | None = None,
                    lang: str | None = None, with_status: bool = False):
        """新增账号；hatch_sess 与已有账号相同时更新那条记录（不新增）。
        with_status=True 时返回 (账号, 是否为更新已有记录)。"""
        with _LOCK:
            exp = {k: int(v) for k, v in (cookies_exp or {}).items()
                   if _is_pos(v)}
            now = int(time.time())
            sess = (cookies or {}).get("hatch_sess")
            for a in self.accounts:
                if sess and (a.get("cookies") or {}).get("hatch_sess") == sess:
                    # 同一会话重复导入：更新已有记录，不新增，否则每账号并发上限会被绕过
                    a["cookies"] = cookies
                    a["cookies_exp"] = exp
                    # 同一会话：寿命不因重新导入而延长。拿不到 hatch_vml 真实 expires 时沿用原锚点
                    # （没有就退回导入时间 created_at），不能钉到「这次重新导入」的时刻
                    a["expiry_anchor"] = (None if _is_pos(exp.get("hatch_vml"))
                                          else a.get("expiry_anchor") or a.get("created_at") or now)
                    a["expires_at"] = account_expiry(exp, a["expiry_anchor"])
                    a["ok"] = None
                    a["quota_blocked_until"] = None  # 管理员重新导入：解除额度暂停，下次使用时重新判断
                    a["note"] = "重复导入：已更新该会话的 cookie"
                    if label:
                        a["label"] = label
                    for k, v in (("proxy", proxy), ("timezone", timezone), ("lang", lang)):
                        if v:
                            a[k] = v
                    _write(self.cfg.accounts_file, self.accounts)
                    return (a, True) if with_status else a
            aid = uuid.uuid4().hex[:12]
            acc = {"id": aid, "label": label or aid, "cookies": cookies,
                   "cookies_exp": exp, "expires_at": account_expiry(exp, now),
                   # 拿不到 hatch_vml 的真实 expires 时按「导入时间 + 2 天」估算：锚点钉在导入时刻
                   "expiry_anchor": None if _is_pos(exp.get("hatch_vml")) else now,
                   "enabled": True, "created_at": now,
                   "last_used": None, "last_keepalive": None, "use_count": 0,
                   "ok": None, "note": "", "synced_at": None,
                   "proxy": proxy or None, "timezone": timezone or None,
                   "lang": lang or None}
            self.accounts.append(acc)
            _write(self.cfg.accounts_file, self.accounts)
            return (acc, False) if with_status else acc

    def list_accounts(self) -> list[dict]:
        # 持锁取快照：worker 线程同时 update_account 新增字段时，遍历原 dict 会抛
        # 「dictionary changed size during iteration」
        with _LOCK:
            snapshot = [dict(a) for a in self.accounts]
        out = []
        for a in snapshot:
            item = {k: v for k, v in a.items() if k != "cookies"}
            if item.get("proxy"):
                item["proxy"] = _mask_proxy(item["proxy"])
            ce = a.get("cookies_exp") or {}
            if _is_pos(ce.get("hatch_vml")):
                item["expires_at"] = int(ce["hatch_vml"])
            item["cookie_count"] = len(a.get("cookies", {}))
            item["essential_ok"] = all(
                a.get("cookies", {}).get(n) for n in ESSENTIAL_COOKIES)
            # 有效期是 hatch_vml 的实测 expires，还是按「2 天寿命」推算的
            item["expires_estimated"] = not _is_pos(ce.get("hatch_vml"))
            out.append(item)
        return out

    def delete_account(self, aid: str) -> bool:
        with _LOCK:
            n = len(self.accounts)
            self.accounts = [a for a in self.accounts if a["id"] != aid]
            _write(self.cfg.accounts_file, self.accounts)
            return len(self.accounts) != n

    def pick_account(self, preferred_id: str | None = None, rotate: bool = True,
                     force_rotate: bool = False, exclude_id: str | None = None,
                     exclude_ids: set[str] | list[str] | None = None,
                     record_use: bool = True) -> dict | None:
        """选择可用账号：优先复用当前已预热的健康账号（减少切号重连耗时），每满 15 次或遇错自动轮转。
        exclude_ids: 需要排除的账号 ID 集合（例如正在被其他并发线程占用的账号）。"""
        with _LOCK:
            live = [a for a in self.accounts if a.get("enabled", True) and a.get("cookies")]
            if not live:
                return None
            now_ts = time.time()

            def usable(a):  # 未判为失效、也不在额度暂停期
                return a.get("ok") is not False and (a.get("quota_blocked_until") or 0) <= now_ts
            pool_has_usable = any(usable(a) for a in live)
            blocked = set(exclude_ids or [])
            if exclude_id:
                blocked.add(exclude_id)
            if blocked:
                available = [a for a in live if a["id"] not in blocked]
                if available:
                    live = available
                elif exclude_ids is not None:
                    # 并发模式下要求避开所有繁忙账号，但当前无剩余空闲账号
                    return None
                elif len(live) > 1:
                    live = [a for a in live if a["id"] != exclude_id] or live
            if preferred_id and not force_rotate:
                for a in live:
                    if a["id"] == preferred_id and usable(a):
                        sticky_n = getattr(self, "_sticky_count", 0) if getattr(self, "_sticky_id", None) == preferred_id else 0
                        if not rotate or sticky_n < 15:
                            self._sticky_id = preferred_id
                            self._sticky_count = sticky_n + 1
                            if record_use:
                                a["last_used"] = time.time()
                                a["use_count"] = a.get("use_count", 0) + 1
                                _write(self.cfg.accounts_file, self.accounts)
                            return a
            healthy = [a for a in live if a.get("ok") is not False]
            unblocked = [a for a in healthy if (a.get("quota_blocked_until") or 0) <= now_ts]
            if not unblocked and pool_has_usable and exclude_ids is not None:
                # 可用账号都只是暂时忙（被 exclude_ids 排除）：返回 None 让调用方排队等它，
                # 不退回到失效 / 额度暂停的账号上白跑一次
                return None
            # 整个池里都没有可用账号时才兜底：先选额度暂停的，再选判为失效的
            # （全部账号都在额度暂停期时，生成链路在 AccountPoolManager.acquire 入口就直接报错，不走到这里）
            candidates = unblocked or healthy or live
            candidates.sort(key=lambda a: (a.get("last_used") or 0.0, a.get("use_count") or 0))
            acc = candidates[0]
            self._sticky_id = acc["id"]
            self._sticky_count = 1
            if record_use:
                acc["last_used"] = time.time()
                acc["use_count"] = acc.get("use_count", 0) + 1
                _write(self.cfg.accounts_file, self.accounts)
            return acc

    def touch_keepalive(self, aid: str, ok: bool | None = True, note: str = ""):
        """ok=None 仅记录未确认的检测；保留上次账号状态、成功保活时间和有效期。"""
        with _LOCK:
            now_ts = int(time.time())
            for a in self.accounts:
                if a["id"] == aid:
                    if ok is not None:
                        a["ok"] = ok
                    if ok is True:
                        a["last_keepalive"] = now_ts
                        # ponytail: 只信实际 Cookie 到期时间或原导入锚点，不凭检测成功虚推 48h。
                        a["expires_at"] = account_expiry(
                            a.get("cookies_exp"), a.get("expiry_anchor") or a.get("created_at"))
                    if note:
                        a["note"] = note[:300]
                    a["checked_at"] = now_ts
            _write(self.cfg.accounts_file, self.accounts)

    def mark(self, aid: str, ok: bool, note: str = ""):
        with _LOCK:
            for a in self.accounts:
                if a["id"] == aid:
                    a["ok"] = ok
                    a["note"] = note[:300]
                    a["checked_at"] = int(time.time())
            _write(self.cfg.accounts_file, self.accounts)

    def block_quota(self, aid: str, seconds: int = 6 * 3600, note: str = ""):
        """额度耗尽：暂停调度 seconds 秒（不改 ok，到期自动恢复；保活 / 测试成功不会提前解除）。"""
        with _LOCK:
            for a in self.accounts:
                if a["id"] == aid:
                    a["quota_blocked_until"] = int(time.time()) + int(seconds)
                    if note:
                        a["note"] = note[:300]
                    a["checked_at"] = int(time.time())
            _write(self.cfg.accounts_file, self.accounts)

    def note(self, aid: str, note: str = ""):
        """只更新备注与 checked_at，ok 保持原值：偶发的生成失败留下记录，但不改健康状态、不影响调度。"""
        with _LOCK:
            for a in self.accounts:
                if a["id"] == aid:
                    a["note"] = (note or "")[:300]
                    a["checked_at"] = int(time.time())
            _write(self.cfg.accounts_file, self.accounts)

    def get_account(self, aid: str) -> dict | None:
        for a in self.accounts:
            if a["id"] == aid:
                return a
        return None

    def update_account(self, aid: str, clear: tuple[str, ...] = (), **kw) -> dict | None:
        """更新标签 / 启用状态 / cookies / 有效期等；值为 None 的字段跳过。

        clear 里列出的字段显式写成 None（例如补 cookie 后把 ok 重置为「未检测」）。
        传了 cookies_exp 会自动重算 expires_at，不用调用方自己算。
        估算锚点 expiry_anchor 的维护规则（避免有效期被反复虚推）：
          - 读到了 hatch_vml 的真实 expires  -> 清空锚点，直接用真值；
          - 读不到且锚点为空                -> 把锚点钉在当下（只钉一次）；
          - 补入全新会话 cookie（重登）      -> 调用方显式传 expiry_anchor=now 重置。
        """
        with _LOCK:
            for a in self.accounts:
                if a["id"] == aid:
                    for k in clear:
                        a[k] = None
                    for k, v in kw.items():
                        if v is not None:
                            a[k] = v
                    if "cookies_exp" in kw and kw["cookies_exp"] is not None:
                        ce = a["cookies_exp"]
                        if _is_pos(ce.get("hatch_vml")):
                            a["expiry_anchor"] = None
                        elif not a.get("expiry_anchor"):
                            # 旧数据没有锚点：以导入时间为准，不能挪到「这次同步」的时刻把寿命虚推
                            a["expiry_anchor"] = a.get("created_at") or int(time.time())
                        a["expires_at"] = account_expiry(
                            ce, a.get("expiry_anchor") or a.get("created_at"))
                    _write(self.cfg.accounts_file, self.accounts)
                    return a
        return None

    def stats(self) -> dict:
        now = int(time.time())
        total = len(self.accounts)
        enabled = sum(1 for a in self.accounts if a.get("enabled", True))
        bad = sum(1 for a in self.accounts
                  if a.get("enabled", True) and a.get("ok") is False)
        healthy = sum(1 for a in self.accounts
                      if a.get("enabled", True) and a.get("ok") is True)
        # 与管理页的提醒一致：12 小时内到期且尚未过期（hatch_vml 寿命只有 2 天，3 天阈值会把所有账号都算进来）
        expiring = sum(1 for a in self.accounts
                       if a.get("enabled", True)
                       and a.get("expires_at")
                       and now < a["expires_at"] < now + 12 * 3600)
        expired = sum(1 for a in self.accounts
                      if a.get("enabled", True)
                      and a.get("expires_at") and a["expires_at"] <= now)
        return {"total": total, "enabled": enabled, "disabled": total - enabled,
                "healthy": healthy, "error": bad,
                "expiring": expiring, "expired": expired}

    # ---------- 任务 ----------
    def create_task(self, kind: str, prompt: str) -> dict:
        with _LOCK:
            tid = "task_" + uuid.uuid4().hex[:20]
            t = {"id": tid, "object": "task", "kind": kind, "prompt": prompt,
                 "status": "queued", "created_at": int(time.time()),
                 "updated_at": int(time.time()), "result": None, "error": None}
            self.tasks[tid] = t
            self._prune_tasks_locked()
            _write(self.cfg.tasks_file, self.tasks)
            return t

    def _prune_tasks_locked(self):
        """只保留最近 MAX_TASKS 条已结束的任务（未结束的一律保留）：文件不无限变大，
        每次 update_task 整份重写的代价也有上限。调用方须持有 _LOCK。"""
        done = [t for t in self.tasks.values() if t.get("status") not in ("queued", "processing")]
        if len(done) <= MAX_TASKS:
            return
        done.sort(key=lambda t: t.get("created_at") or 0)
        for t in done[:len(done) - MAX_TASKS]:
            self.tasks.pop(t["id"], None)

    def update_task(self, tid: str, **kw):
        with _LOCK:
            t = self.tasks.get(tid)
            if not t:
                return None
            t.update(kw)
            t["updated_at"] = int(time.time())
            _write(self.cfg.tasks_file, self.tasks)
            return t

    def get_task(self, tid: str) -> dict | None:
        return self.tasks.get(tid)

    def list_tasks(self, limit: int = 50) -> list[dict]:
        with _LOCK:  # 返回副本：调用方序列化时 worker 仍可能在改原任务
            items = sorted((dict(t) for t in self.tasks.values()),
                           key=lambda t: t.get("created_at") or 0, reverse=True)
        return items[:limit]

    def delete_task(self, tid: str) -> bool:
        with _LOCK:
            if tid in self.tasks:
                del self.tasks[tid]
                _write(self.cfg.tasks_file, self.tasks)
                return True
            return False

    def clear_tasks(self, keep: int = 0) -> int:
        with _LOCK:
            items = sorted(self.tasks.values(),
                           key=lambda t: t.get("created_at") or 0, reverse=True)
            keep_ids = {t["id"] for t in items[:keep]}
            # 未结束的任务一律保留：worker 还会回写状态，客户端还在轮询
            keep_ids |= {t["id"] for t in items if t.get("status") in ("queued", "processing")}
            removed = len(self.tasks) - len(keep_ids)
            self.tasks = {k: v for k, v in self.tasks.items() if k in keep_ids}
            _write(self.cfg.tasks_file, self.tasks)
            return max(0, removed)
