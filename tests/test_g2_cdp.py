"""G2 CDP 连接与浏览器复用用例：python3 tests/test_g2_cdp.py

经 harness_muse 拉起真实 Chromium（FakeMuse 仿真站 + Chromium 包装脚本 + load_app 加载的真实 engine），
页面只开 about:blank，不触达真实 muse.ai、不消耗额度。缺少 openssl 或 Chromium 时打印 SKIP 并以 0 退出。
每个用例先把全部子项量完、逐项打印 PASS / FAIL，最后有失败再整体断言失败。

- PRES-CDP（3.40）：连接正常时
  (a) 普通命令照常返回结果；
  (b) CDP 返回错误（调用不存在的方法）时抛 CDPError，不是 CDPConnectionError，连接照常可用；
  (c) 一直没有消息到来时 send 按调用方的 timeout 抛 TimeoutError，阻塞等待、CPU 很低（不是忙等）：
      c1 engine 开的页面连接（与生产相同，CDP(ws_url, timeout=180)）上 send(永不 resolve 的 Promise, timeout=1)。
         修复前单次 recv 会阻塞到连接创建时的 180s socket 超时；为让它在有限时间内暴露，用例在 15s 时
         由看门狗 abort 该连接的 socket（shutdown(SHUT_RDWR) 唤醒阻塞中的 recv），修复前这里量到约 15s；
      c2 另开一条到同一页面的连接，创建时 timeout=8，同样 send(timeout=1)：修复前量到约 8s，即等的是
         连接的 socket 超时而不是调用方的 timeout；
      c3 pump 之后在 c2 的连接上 send(timeout=0.5)：修复前 pump 把 socket 超时永久改成 sock_timeout（2s），
         这里量到约 2s；
  (d) pump(seconds, on_event) 照常读到事件（c2 的连接开 Page 域后让页面 300ms 后 reload，收到 Page.* 事件）；
  (e) 超时之后同一连接仍可正常使用。
- M7-EXP（1.28 → 2.28）：
  (a) 页面目标被 Target.closeTarget 关掉后 page.send("Runtime.evaluate", timeout=5)；
  (b) 浏览器进程被 SIGKILL 后 engine.browser.send("Browser.getVersion", timeout=5)；
  两种情况都用 cdp_call_cost 量：墙钟与 CPU 都 < 0.2s，抛 CDPConnectionError（同时是 CDPError 与
  ConnectionError）；紧接着第二次调用同一实例也立即抛。
  (c) 锁不被占住：杀进程前另开两条与 engine.browser 同类的浏览器级连接，杀进程后它们同样是死连接、且之前
  没被调用过（(b) 调过之后的 engine.browser 已被标记关闭，在它上面只能测到"已关闭直接抛"）。在死连接上
  一个线程 send(timeout=5) 的同时另一个线程 send(timeout=5) 或 close()，两边都在 0.5s 内返回或抛错。
  杀进程之后先 forget_browser 再 close_app（修复前 engine.stop() 会在死连接上忙等到 180s 超时）。

以下 L3 用例不用仿真站（页面只到 about:blank），直接用真实 Chromium，并读 /proc/self/fd 与
/proc/<pid>/cmdline，只在 Linux / WSL 下运行（否则 SKIP）。用例自己起的外部 Chromium、engine 拉起后
留下的 Chromium 都在 finally 里 SIGKILL 并等它们退出，临时 profile 目录一并删掉。

- L3-EXP（1.44 → 2.44）：先不经 engine、在 load_app 分到的随机 CDP 端口上自己起一个外部 Chromium
  （headless、--no-sandbox、--remote-allow-origins=*、$HOME 下的独立 profile），再连续调用 engine.start()
  6 次：只有 1 个浏览器连接对象（每次的 engine.browser 都留着引用，id 不会被复用，旧连接也不会被 GC
  悄悄关掉）、/proc/self/fd 数量不随调用增长（只允许首个连接带来的增量）、proc 为 None、
  _external_browser 为真、is_running() 为真、/readyz 与 /admin/status 的 browser_running 为 true；
  engine.stop() 只关自己的连接、不结束外部 Chromium，并清掉 _external_browser。
- L3-DEAD：复用的外部 Chromium 被杀后 is_running() 变为假、/readyz 显示未运行；再次 start() 不复用死连接，
  而是自己拉起 Chromium（proc 不为 None 且在运行、是该端口上唯一的 Chromium 主进程、_external_browser
  为假、is_running() 为真），旧连接已关闭。
- L3-RECONNECT：自己拉起的 Chromium 仍在运行、主连接断了时，start() 重新连接它的调试地址，不再拉起第二个
  Chromium（pid 不变、该 profile / 端口上只有一个 Chromium 主进程），新连接可用、is_running() 为真。
  (a) engine.browser.close() 之后 start()；(b) 关掉之后再把 engine.browser 置空（引用丢失，例如上一次
  start() 拉起进程后没连上）之后 start()——修复前这种情况会清掉 Singleton 锁、在同一端口和 profile 上
  再 Popen 一个 Chromium。
- PRES-START（3.53）：没有已在运行的 Chromium 时，start() 照常拉起并建立 CDP 连接；再次 start() 复用
  同一连接与进程；有账号时 /readyz 为 status=ready、browser_running=true，/admin/status 也显示运行中。
"""
import contextlib
import json
import os
import signal
import subprocess
import sys
import threading
import time
import unittest
import urllib.request
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness_muse as hm  # noqa: E402

NEVER_RESOLVES = {"expression": "new Promise(()=>{})", "awaitPromise": True}
FAST = 0.2          # M7-EXP：断开后的调用墙钟与 CPU 都要小于它（秒）；PRES-CDP(c) 的 CPU 上限
LOCK_FREE = 0.5     # M7-EXP(c)：并发的两个调用都要在这么多秒内结束
CAP = 15.0          # PRES-CDP(c)：看门狗 abort socket 的时间（修复前单次 recv 会阻塞到 180s）
SIDE_TIMEOUT = 8.0  # PRES-CDP(c2)：另开连接创建时的 timeout（即它的 socket 超时）


# ------------------------- 小工具 -------------------------
@contextlib.contextmanager
def _browser_app(name):
    """FakeMuse + Chromium 包装脚本 + load_app，并预先拉起浏览器；退出时先关 app 再停仿真站。"""
    with hm.FakeMuse() as muse:
        wrapper = hm.make_chromium_wrapper(muse.port, muse.workdir)
        app = hm.load_app({"MUSE2API_CHROMIUM": wrapper}, name)
        try:
            app.engine.start()
            yield app
        finally:
            hm.close_app(app)


def _cdp_names(conn) -> dict:
    """conn 所在 cdp 模块的命名空间。load_app 每次都重新导入 cdp，异常类必须取被测连接同一份。"""
    return type(conn).send.__globals__


def _is(exc, cls) -> bool:
    return cls is not None and isinstance(exc, cls)


def _cost(fn) -> dict:
    """hm.cdp_call_cost(fn) 的结果（wall / cpu / error），外加抛出的异常对象 exc（没抛为 None）。"""
    box = {}

    def call():
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 - 记下来再交给 cdp_call_cost
            box["exc"] = exc
            raise

    cost = hm.cdp_call_cost(call)
    cost["exc"] = box.get("exc")
    cost.setdefault("capped", False)
    return cost


def _capped_cost(conn, fn, cap: float = CAP) -> dict:
    """同 _cost；cap 秒后 fn 还没返回就 abort 掉 conn 的 socket，唤醒阻塞中的 recv，capped 记看门狗是否出手。"""
    fired = threading.Event()

    def abort():
        fired.set()
        try:
            conn.ws.abort()
        except Exception:  # noqa: BLE001
            pass

    timer = threading.Timer(cap, abort)
    timer.daemon = True
    timer.start()
    try:
        cost = _cost(fn)
    finally:
        timer.cancel()
    cost["capped"] = fired.is_set()
    return cost


def _show(cost) -> str:
    text = f"wall={cost['wall']:.3f}s cpu={cost['cpu']:.3f}s -> {cost['error'] or '正常返回'}"
    return text + (f"（{CAP:g}s 时由看门狗 abort socket 收尾）" if cost.get("capped") else "")


def _current_target_id(engine) -> str:
    """当前线程页面（probe_page 开的）所在隔离上下文里的页面 targetId。"""
    infos = ((engine.browser.send("Target.getTargets", timeout=10).get("result") or {})
             .get("targetInfos") or [])
    ctx = engine.current_context_id
    return next(t["targetId"] for t in infos
                if t.get("type") == "page" and t.get("browserContextId") == ctx)


def _race(conn, second) -> dict:
    """线程 A 调 conn.send("Browser.getVersion", timeout=5)，50ms 后线程 B 调 second(conn)（修复前 A 此时正持锁空转）。

    返回 {"A": 记录, "B": 记录}，记录含 wall（从该线程开始算，没结束为 None）、error、exc。"""
    out = {}

    def run(tag, fn):
        t0 = time.monotonic()
        rec = {"error": None, "exc": None}
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            rec.update(error=f"{type(exc).__name__}: {exc}", exc=exc)
        rec["wall"] = round(time.monotonic() - t0, 3)
        out[tag] = rec

    a = threading.Thread(target=run, args=("A", lambda: conn.send("Browser.getVersion", timeout=5)), daemon=True)
    b = threading.Thread(target=run, args=("B", lambda: second(conn)), daemon=True)
    a.start()
    time.sleep(0.05)
    b.start()
    a.join(15)
    b.join(15)
    pending = {"wall": None, "error": "15s 内没结束", "exc": None}
    return {tag: dict(out.get(tag) or pending) for tag in ("A", "B")}


def _race_show(rec) -> str:
    wall = "-" if rec["wall"] is None else f"{rec['wall']:.3f}s"
    return f"{wall} -> {rec['error'] or '正常返回'}"


class _Verdicts:
    """逐项记录子用例结论：先全部打印，最后有失败再整体断言失败，修复前一次就能看到每个子项的结果。"""

    def __init__(self, label: str):
        self.label = label
        self.failed: list[str] = []

    def check(self, name: str, ok: bool, detail: str) -> None:
        print(f"{'PASS' if ok else 'FAIL'}: {self.label}{name} —— {detail}")
        if not ok:
            self.failed.append(name)

    def done(self) -> None:
        assert not self.failed, f"{self.label.strip()} 未通过的子项: {'; '.join(self.failed)}"


def _check_conn_error(v: _Verdicts, name: str, cost: dict, ns: dict) -> None:
    """断开后的调用：墙钟与 CPU 都 < FAST，抛 CDPConnectionError（同时是 CDPError 与 ConnectionError）。"""
    conn_error = ns.get("CDPConnectionError")
    exc = cost["exc"]
    ok = (_is(exc, conn_error) and isinstance(exc, ns["CDPError"]) and isinstance(exc, ConnectionError)
          and cost["wall"] < FAST and cost["cpu"] < FAST)
    detail = _show(cost) + ("；cdp 模块里没有 CDPConnectionError" if conn_error is None else "")
    v.check(name, ok, detail)


# ------------------------- PRES-CDP -------------------------
def test_pres_cdp_healthy_connection():
    """PRES-CDP（3.40）：连接正常时普通命令、CDP 错误、超时与 pump 的语义不变，超时之后连接仍可用。"""
    v = _Verdicts("PRES-CDP")
    with _browser_app("g2-pres-cdp") as app:
        engine = app.engine
        page = hm.probe_page(engine, "pres-cdp")  # engine._open_page 开的连接：CDP(ws_url, timeout=180)
        ns = _cdp_names(page)
        side = None
        try:
            r = page.send("Runtime.evaluate", {"expression": "1+1", "returnByValue": True}, timeout=5)
            a_value = ((r.get("result") or {}).get("result") or {}).get("value")
            a_js = page.js("6*7", timeout=5)

            b = _cost(lambda: page.send("Muse2api.noSuchMethod", timeout=5))
            b_after = page.js("2+2", timeout=5)

            c1 = _capped_cost(page, lambda: page.send("Runtime.evaluate", NEVER_RESOLVES, timeout=1))
            e_page = None if c1["capped"] else page.js("5+6", timeout=5)

            side = ns["CDP"](f"ws://127.0.0.1:{engine.cfg.cdp_port}/devtools/page/{_current_target_id(engine)}",
                             timeout=SIDE_TIMEOUT)
            c2 = _capped_cost(side, lambda: side.send("Runtime.evaluate", NEVER_RESOLVES, timeout=1))
            e_side = side.js("3+4", timeout=5)

            side.send("Page.enable", timeout=5)
            side.js("setTimeout(function () { location.reload(); }, 300); 1", timeout=5)
            events = []
            d = _cost(lambda: side.pump(3, events.append))
            c3 = _capped_cost(side, lambda: side.send("Runtime.evaluate", NEVER_RESOLVES, timeout=0.5))
            e_after = side.js("8+9", timeout=5)
        finally:
            if side is not None:
                side.close()
            engine._close_page()

    page_events = sorted({str(e.get("method")) for e in events if str(e.get("method", "")).startswith("Page.")})
    e_page_text = "跳过（连接已被看门狗 abort）" if c1["capped"] else repr(e_page)
    print(f"OBSERVED: PRES-CDP(a) send(Runtime.evaluate 1+1) -> {a_value!r}；js('6*7') -> {a_js!r}")
    print(f"OBSERVED: PRES-CDP(b) send(不存在的方法) {_show(b)}；之后 js('2+2') -> {b_after!r}")
    print(f"OBSERVED: PRES-CDP(c1) engine 页面连接（创建时 timeout=180）上 send(永不 resolve, timeout=1) {_show(c1)}")
    print(f"OBSERVED: PRES-CDP(c2) 同一页面另开连接（创建时 timeout={SIDE_TIMEOUT:g}）上 "
          f"send(永不 resolve, timeout=1) {_show(c2)}")
    print(f"OBSERVED: PRES-CDP(d) pump(3) {_show(d)}；收到 {len(events)} 个事件，其中 Page.*: {page_events}")
    print(f"OBSERVED: PRES-CDP(c3) pump 之后 send(永不 resolve, timeout=0.5) {_show(c3)}")
    print(f"OBSERVED: PRES-CDP(e) 超时之后：另开连接 js('3+4') -> {e_side!r}、js('8+9') -> {e_after!r}；"
          f"engine 页面连接 js('5+6') -> {e_page_text}")

    conn_error = ns.get("CDPConnectionError")
    v.check("(a) 普通命令照常返回结果", a_value == 2 and a_js == 42, f"value={a_value!r} js={a_js!r}")
    v.check("(b) CDP 返回错误时抛 CDPError、不是 CDPConnectionError，连接照常可用",
            isinstance(b["exc"], ns["CDPError"]) and not _is(b["exc"], conn_error)
            and b["wall"] < 1.0 and b_after == 4, f"{_show(b)}；之后 js('2+2') -> {b_after!r}")
    for name, cost, wait in (("(c1) engine 页面连接（timeout=180）超时抛 TimeoutError、按调用方 timeout=1", c1, 1.0),
                             (f"(c2) 另开连接（timeout={SIDE_TIMEOUT:g}）超时抛 TimeoutError、按调用方 timeout=1", c2, 1.0),
                             ("(c3) pump 之后 send 仍按调用方 timeout=0.5", c3, 0.5)):
        ok = (isinstance(cost["exc"], TimeoutError) and not cost["capped"]
              and 0.9 * wait <= cost["wall"] < wait + (1.5 if wait >= 1 else 0.75) and cost["cpu"] < FAST)
        v.check(name, ok, _show(cost))
    v.check("(d) pump 照常读到事件", d["exc"] is None and bool(page_events),
            f"{_show(d)}；Page.* 事件 {page_events}")
    v.check("(e) 超时之后同一连接仍可正常使用",
            e_side == 7 and e_after == 17 and (c1["capped"] or e_page == 11),
            f"另开连接 3+4={e_side!r}、8+9={e_after!r}；engine 页面连接 5+6={e_page_text}")
    v.done()


# ------------------------- M7-EXP -------------------------
def test_m7_exp_closed_target():
    """M7-EXP(a)：页面目标被关闭后 page.send 立即抛 CDPConnectionError，第二次调用同一实例也立即抛。"""
    v = _Verdicts("M7-EXP(a) ")
    with _browser_app("g2-m7-close") as app:
        engine = app.engine
        page = hm.probe_page(engine, "m7-close")
        ns = _cdp_names(page)
        healthy = page.js("1+1", timeout=5)
        target = hm.close_page_target(engine)
        time.sleep(0.3)  # 等 Chromium 把该目标的 websocket 关掉
        call = lambda: page.send("Runtime.evaluate", {"expression": "1+1"}, timeout=5)  # noqa: E731
        first = _cost(call)
        second = _cost(call)
        cleanup = _cost(engine._close_page)
    print(f"OBSERVED: M7-EXP(a) 关闭前 js('1+1') -> {healthy!r}；Target.closeTarget({target}) 后 "
          f"page.send(timeout=5) 第一次 {_show(first)}；第二次 {_show(second)}；"
          f"随后 _close_page {_show(cleanup)}；page.closed={getattr(page, 'closed', '（无此属性）')}")
    assert healthy == 2 and target, (healthy, target)
    _check_conn_error(v, "第一次调用立即抛 CDPConnectionError", first, ns)
    _check_conn_error(v, "第二次调用同一实例也立即抛", second, ns)
    v.done()


def test_m7_exp_killed_browser():
    """M7-EXP(b)(c)：浏览器进程被杀后 browser.send 立即抛 CDPConnectionError；死连接上并发的 send / close 不被锁住。"""
    v = _Verdicts("M7-EXP")
    with _browser_app("g2-m7-kill") as app:
        engine = app.engine
        ns = _cdp_names(engine.browser)
        version_url = f"http://127.0.0.1:{engine.cfg.cdp_port}/json/version"
        ws_url = ns["http_json"](version_url, timeout=5)["webSocketDebuggerUrl"]
        spare = [ns["CDP"](ws_url, timeout=180) for _ in range(2)]  # 与 engine.browser 同类的浏览器级连接
        try:
            for conn in spare:
                conn.send("Browser.getVersion", timeout=5)
            pid = hm.kill_browser(engine)
            dead = engine.proc is not None and engine.proc.poll() is not None
            call = lambda: engine.browser.send("Browser.getVersion", timeout=5)  # noqa: E731
            first = _cost(call)
            second = _cost(call)
            closed_flag = getattr(engine.browser, "closed", "（无此属性）")
            race_send = _race(spare[0], lambda c: c.send("Browser.getVersion", timeout=5))
            race_close = _race(spare[1], lambda c: c.close())
        finally:
            for conn in spare:  # 只做本地清理，不经 close()（修复前它要等别的线程放锁）
                try:
                    conn.ws.shutdown()
                except Exception:  # noqa: BLE001
                    pass
            hm.forget_browser(engine)
    print(f"OBSERVED: M7-EXP(b) SIGKILL 浏览器进程 {pid}（已退出={dead}）后 browser.send(timeout=5) "
          f"第一次 {_show(first)}；第二次 {_show(second)}；browser.closed={closed_flag}")
    print(f"OBSERVED: M7-EXP(c) 死连接上 A=send ‖ B=send：A {_race_show(race_send['A'])}；"
          f"B {_race_show(race_send['B'])}")
    print(f"OBSERVED: M7-EXP(c) 死连接上 A=send ‖ B=close()：A {_race_show(race_close['A'])}；"
          f"B {_race_show(race_close['B'])}")
    assert pid and dead, (pid, engine.proc)
    _check_conn_error(v, "(b) 浏览器被杀后第一次调用立即抛 CDPConnectionError", first, ns)
    _check_conn_error(v, "(b) 第二次调用同一实例也立即抛", second, ns)
    conn_error = ns.get("CDPConnectionError")
    for name, race, b_raises in (("(c) A 持锁 send 时 B 调 send", race_send, True),
                                 ("(c) A 持锁 send 时 B 调 close()", race_close, False)):
        a, b = race["A"], race["B"]
        ok = (a["wall"] is not None and a["wall"] < LOCK_FREE and _is(a["exc"], conn_error)
              and b["wall"] is not None and b["wall"] < LOCK_FREE
              and (_is(b["exc"], conn_error) if b_raises else b["exc"] is None))
        v.check(name + f"，两边都在 {LOCK_FREE:g}s 内结束", ok, f"A {_race_show(a)}；B {_race_show(b)}")
    v.done()


if __name__ == "__main__":
    hm.run_main(test_pres_cdp_healthy_connection, test_m7_exp_closed_target, test_m7_exp_killed_browser)
    print("PASS: test_g2_cdp")
