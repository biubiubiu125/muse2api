"""Offline regressions for account state, error mapping, timeouts and admin leases."""
import asyncio
import base64
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
# config 在首次导入时就按 MUSE2API_HOME 定下数据目录：必须先指到临时目录，绝不碰仓库里的 data/
_HOME = tempfile.mkdtemp(prefix="m2a-review-")
os.environ["MUSE2API_HOME"] = _HOME
os.environ["MUSE2API_KEY"] = "test-secret"


async def call(app, method, path, body=b"", headers=None):
    hdr = {"authorization": "Bearer test-secret", "content-type": "application/json", **(headers or {})}
    hdr["content-length"] = str(len(body))
    path, _, query = path.partition("?")
    scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
             "method": method, "scheme": "http", "path": path, "raw_path": path.encode(),
             "query_string": query.encode(), "root_path": "",
             "headers": [(k.encode(), v.encode()) for k, v in hdr.items()],
             "server": ("test", 80), "client": ("test", 1)}
    messages, sent = [], False

    async def receive():
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        await asyncio.Event().wait()

    async def send(message):
        messages.append(message)

    await app(scope, receive, send)
    status = next(m["status"] for m in messages if m["type"] == "http.response.start")
    raw = b"".join(m.get("body", b"") for m in messages)
    return status, (json.loads(raw) if raw else {})


def load_app():
    spec = importlib.util.spec_from_file_location("app_review_candidate", ROOT / "app.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # pydantic 按模块名解析请求模型的注解
    spec.loader.exec_module(module)
    assert module.CFG.data_dir.startswith(_HOME), module.CFG.data_dir
    return module


def check_dotenv_empty_env(tmp):
    """docker-compose 注入的空 MUSE2API_KEY 不能挡住 data/.env 里保存的 Key。"""
    import config
    os.makedirs(os.path.join(tmp, "data"), exist_ok=True)
    with open(os.path.join(tmp, ".env"), "w", encoding="utf-8") as f:
        f.write("MUSE2API_KEY=m2a_root\n")
    with open(os.path.join(tmp, "data", ".env"), "w", encoding="utf-8") as f:
        f.write("MUSE2API_KEY=m2a_persisted\n")
    saved = {k: os.environ.get(k) for k in ("MUSE2API_HOME", "MUSE2API_KEY")}  # 只改环境变量，不重建 CFG
    try:
        os.environ["MUSE2API_HOME"] = tmp
        os.environ["MUSE2API_KEY"] = ""
        config._load_dotenv()
        assert os.environ["MUSE2API_KEY"] == "m2a_persisted", os.environ["MUSE2API_KEY"]
        os.environ["MUSE2API_KEY"] = "m2a_old_from_envfile"   # systemd EnvironmentFile / compose 注入的旧 Key
        config._load_dotenv()
        assert os.environ["MUSE2API_KEY"] == "m2a_persisted", "rotated key in data/.env must win"
        os.remove(os.path.join(tmp, "data", ".env"))
        os.environ["MUSE2API_KEY"] = "m2a_explicit"
        config._load_dotenv()
        assert os.environ["MUSE2API_KEY"] == "m2a_explicit", "root .env must not override env"
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    print("PASS: empty env var falls back to .env; data/.env (rotated key) beats env; root .env does not")


def check_corrupt_store_refuses_to_start(tmp):
    """账号文件损坏时拒绝启动并留下副本，绝不按空数据处理（下一次写入会覆盖原文件）。"""
    from config import Config
    import store as store_module
    cfg = Config(base_dir=tmp)
    cfg.ensure_dirs()
    with open(cfg.accounts_file, "w", encoding="utf-8") as f:
        f.write('[{"id": "a", "cookies": {"hatch_sess": "x"}}, {"id": "b"')
    original = open(cfg.accounts_file, encoding="utf-8").read()
    try:
        store_module.Store(cfg)
        raise AssertionError("corrupt accounts.json must refuse to load")
    except store_module.StoreCorruptError:
        pass
    assert open(cfg.accounts_file, encoding="utf-8").read() == original
    assert any(n.startswith("accounts.json.corrupt-") for n in os.listdir(cfg.data_dir))
    print("PASS: corrupt accounts.json refuses to load, original kept and a copy saved")


def check_open_page_without_browser_fails_closed(app):
    """浏览器主连接为空（start() 重连时会短暂置 None）时 _open_page 必须失败关闭，
    不能退回 /json/new 在共享默认上下文里开普通标签页：那个标签页不走账号代理，会暴露本机 IP。"""
    import requests
    eng = app.MuseEngine(app.CFG)
    eng.set_account_meta("acc-proxy", "http://user:pw@203.0.113.9:3128", None, None)
    eng._proxy_server_for = lambda proxy: "http://127.0.0.1:1" if proxy else None
    eng.browser = None
    fallback_calls = []
    orig_put = requests.put
    requests.put = lambda *a, **k: fallback_calls.append(a) or (_ for _ in ()).throw(AssertionError("fallback used"))
    try:
        for aid in ("acc-proxy", None):
            try:
                eng._open_page(aid)
                raise AssertionError("_open_page should fail without a browser connection")
            except app.MuseGenerationError as exc:
                assert "浏览器主连接不可用" in str(exc), exc
    finally:
        requests.put = orig_put
    assert not fallback_calls, fallback_calls
    assert eng.page is None and eng.current_context_id is None
    print("PASS: _open_page fails closed instead of opening an unproxied shared tab when browser is None")


async def main():
    with tempfile.TemporaryDirectory(prefix="m2a-dotenv-") as tmp:
        check_dotenv_empty_env(tmp)
    with tempfile.TemporaryDirectory(prefix="m2a-corrupt-") as tmp:
        check_corrupt_store_refuses_to_start(tmp)
    try:
        app = load_app()
        check_open_page_without_browser_fails_closed(app)
        store = app.store
        acc = store.add_account({"hatch_sess": "s", "hatch_vml": "v", "hatch_gw": "g"}, label="A")
        aid = acc["id"]

        # 补 cookie：ok 重置为未检测，expires_at 按新锚点重算
        store.update_account(aid, ok=False, expires_at=int(time.time()) - 100)
        status, body = await call(app.app, "POST", f"/admin/accounts/{aid}/cookies",
                                  json.dumps({"cookie_header": "hatch_sess=n; hatch_vml=n; hatch_gw=n"}).encode())
        assert status == 200, (status, body)
        fresh = store.get_account(aid)
        assert fresh["ok"] is None, fresh["ok"]
        assert fresh["expires_at"] > time.time() + 86400, fresh["expires_at"]
        print("PASS: cookie update resets ok to unchecked and recomputes expires_at")

        # 更新 Cookie 缺核心项（如从 document.cookie 复制、没有 httpOnly 的会话 cookie）：拒绝，原会话不被覆盖
        before = json.loads(json.dumps(store.get_account(aid)))
        for header in ("_fbp=f2; wd=1920x1080", "hatch_vml=only-vml", "hatch_sess=only-sess"):
            status, body = await call(app.app, "POST", f"/admin/accounts/{aid}/cookies",
                                      json.dumps({"cookie_header": header}).encode())
            assert status == 400 and "核心" in str(body.get("detail")), (header, status, body)
            after = store.get_account(aid)
            assert after["cookies"] == before["cookies"] and after["ok"] == before["ok"] \
                and after.get("expires_at") == before.get("expires_at"), (header, after)
        print("PASS: cookie update missing essential cookies is rejected and keeps the old session")

        # 管理端请求体校验失败（JSON 配了表单 Content-Type）返回 422，不能因错误详情里的 bytes 变成 500
        for path in (f"/admin/accounts/{aid}/cookies", "/admin/tasks/clear", "/admin/media/delete"):
            status, body = await call(app.app, "POST", path, b'{"keep": 1}',
                                      headers={"content-type": "application/x-www-form-urlencoded"})
            assert status == 422 and isinstance(body.get("detail"), list), (path, status, body)
        print("PASS: admin body validation errors are 422 even when the error carries raw bytes")

        # 偶发生成失败不把已失效账号改回健康
        store.mark(aid, False, "dead")
        engine = app.engine
        orig = {n: getattr(engine, n) for n in ("generate", "start", "set_account_meta",
                                                 "close_current_page", "reset_thread")}
        engine.start = lambda: None
        engine.set_account_meta = lambda *a, **k: None
        engine.close_current_page = lambda: None
        engine.reset_thread = lambda *a, **k: None

        def page_timeout(*a, **k):
            raise app.MuseGenerationError("muse.ai 页面加载超时（未出现聊天输入框）；未确认会话失效")
        engine.generate = page_timeout
        try:
            app._run_generation("p", "image", 5, account_id=aid)
            raise AssertionError("generation should fail")
        except app.MuseGenerationError:
            pass
        assert store.get_account(aid)["ok"] is False, store.get_account(aid)
        assert not app.ACCOUNT_POOL.busy_accounts and not app.GEN_LOCK.locked()
        print("PASS: transient generation failure keeps ok=False")

        # 异步任务：排队用 queue_timeout，生成重新拿到完整 timeout
        store.mark(aid, True, "")
        seen = {}

        def capture(*a, **k):
            seen["timeout"] = k["timeout"]
            return {"filename": "x.webp", "size": 1, "kind": "image"}
        engine.generate = capture
        slots = app.GEN_LOCK.limit
        for _ in range(slots):
            app.GEN_LOCK.acquire()
        releaser = asyncio.get_running_loop().run_in_executor(
            None, lambda: (time.sleep(2.2), [app.GEN_LOCK.release() for _ in range(slots)]))
        await asyncio.get_running_loop().run_in_executor(
            None, lambda: app._run_generation("p", "video", 5, account_id=aid, queue_timeout=10))
        await releaser
        assert seen["timeout"] >= 4, seen
        print(f"PASS: queued async task still gets full generation timeout ({seen['timeout']}s of 5s)")

        # 上游会话失效：502 + upstream_auth_error，而不是 401；没有账号：400
        def auth_fail(*a, **k):
            raise app.MuseAuthError("会话已失效 (/api/session HTTP 401)")
        engine.generate = auth_fail
        status, body = await call(app.app, "POST", "/v1/images/generations",
                                  json.dumps({"prompt": "cat", "timeout": 5}).encode())
        assert status == 502 and body["error"]["code"] == "upstream_auth_error", (status, body)
        store.update_account(aid, enabled=False)
        status, body = await call(app.app, "POST", "/v1/images/generations",
                                  json.dumps({"prompt": "cat", "timeout": 5}).encode())
        assert status == 400, (status, body)
        store.update_account(aid, enabled=True)
        print("PASS: upstream auth failure is 502 upstream_auth_error; no account is 400")

        # /v1/images/edits JSON 分支入参校验
        for raw in (b"[1,2]", b"not json", json.dumps({"prompt": "x", "timeout": "abc"}).encode(),
                    json.dumps({"prompt": "x", "timeout": 9999}).encode()):
            status, body = await call(app.app, "POST", "/v1/images/edits", raw)
            assert status in (400, 422), (raw, status, body)
        print("PASS: images/edits JSON branch rejects bad bodies with 4xx")

        # 视频 / 对话 timeout 边界
        for path, payload in (("/v1/videos", {"prompt": "x", "timeout": -5}),
                              ("/v1/chat/completions", {"messages": [{"role": "user", "content": "hi"}],
                                                        "timeout": -5})):
            status, body = await call(app.app, "POST", path, json.dumps(payload).encode())
            assert status == 422, (path, status, body)
        print("PASS: negative timeout is rejected with 422")

        # b64_json 任务的媒体被删后返回 410
        task = store.create_task("image", "p")
        store.update_task(task["id"], status="completed", response_format="b64_json",
                          result={"filename": "gone.webp", "size": 1, "kind": "image"})
        status, body = await call(app.app, "GET", f"/v1/images/tasks/{task['id']}")
        assert status == 410, (status, body)
        print("PASS: polling a b64_json task whose media was deleted returns 410")

        # 管理端探测经账号租约：同账号正在生成时排队，结束时关页并释放
        closed = []
        engine.close_current_page = lambda: closed.append(True)
        with app._leased_account(aid, timeout=1) as leased:
            assert leased["id"] == aid and app.ACCOUNT_POOL.busy_accounts.get(aid) == 1
            assert app.GEN_LOCK.locked()
            try:
                with app._leased_account(aid, timeout=1):
                    raise AssertionError("second lease on same account must wait")
            except app.MuseGenerationError:
                pass
        assert closed and not app.ACCOUNT_POOL.busy_accounts and not app.GEN_LOCK.locked()
        print("PASS: admin probes lease the account, respect per-account limit and close the page")

        # 匿名更新检测不能强制刷新、不带 GITHUB_TOKEN
        calls = []
        app._check_update_sync = lambda force=False, use_token=True: calls.append((force, use_token)) or {}
        await call(app.app, "GET", "/admin/update/check?force=true", headers={"authorization": ""})
        await call(app.app, "GET", "/admin/update/check?force=true")
        assert calls == [(False, False), (True, True)], calls
        print("PASS: anonymous update check cannot force refresh or use the token")

        # 同一会话重复导入更新原记录，不新增
        n_before = len(store.accounts)
        status, body = await call(app.app, "POST", "/admin/accounts",
                                  json.dumps({"cookie_header": "hatch_sess=n; hatch_vml=n2; hatch_gw=n"}).encode())
        assert status == 200 and body["added"][0]["id"] == aid and body["added"][0]["updated"], body
        assert len(store.accounts) == n_before
        print("PASS: re-importing the same hatch_sess updates the existing account")

        # 清空任务保留未结束的任务
        running = store.create_task("video", "running")
        store.update_task(running["id"], status="processing")
        store.clear_tasks(0)
        assert store.get_task(running["id"]), "processing task must survive clear"
        print("PASS: clearing tasks keeps queued/processing tasks")

        # 未配置 public_base 时媒体地址按请求 Host 补成绝对 URL
        store.update_task(running["id"], status="completed", url="/v1/media/v.mp4",
                          result={"url": "/v1/media/v.mp4"})
        status, body = await call(app.app, "GET", f"/v1/videos/{running['id']}",
                                  headers={"host": "api.example.test:8443", "x-forwarded-proto": "https"})
        assert body["url"] == "https://api.example.test:8443/v1/media/v.mp4", body
        assert body["video"]["url"] == body["url"] == body["result"]["url"], body
        print("PASS: media URLs are absolute, built from the request host")

        # 流式增量：文本不再是前缀扩展时不整段重发
        import engine as engine_module
        def replay(snapshots):
            emitted, last = "", ""
            for cur in snapshots:
                d = engine_module._stream_delta(emitted, last, cur)
                emitted += d
                last = cur
            return emitted
        # 逐字输出
        assert replay(["H", "He", "Hel", "Hello"]) == "Hello"
        # 代码块渲染：页面文本被整段重写（插入语言标签 / Copy），之后又追加了文字
        raw = "Here:\n```python\nprint(1)\n```\nDone."
        rendered = "Here:\npython\nCopy\nprint(1)\nDone."
        out = replay([raw[:i] for i in range(1, len(raw) + 1)] + [rendered, rendered + " More."])
        assert out == raw + " More.", repr(out)
        # 文本短暂缩短后又恢复：不重复
        assert replay(["hello world", "hello", "hello world!"]) == "hello world!"
        # 重新渲染与追加落在同一次轮询里：追加的句子不丢
        before = "Here:\n```py\nprint(1)\n```\nso far so good"
        after = "Here:\npy\nCopy\nprint(1)\nso far so good IMPORTANT-SENTENCE."
        out = replay([before, after])
        assert out.endswith(" IMPORTANT-SENTENCE."), repr(out)
        print("PASS: non-prefix stream updates do not resend already-sent text")

        # 注入时为缺失 expires 填的占位值，读回时不当成真实有效期
        class FakePage:
            def __init__(self):
                self.set = {}

            def send(self, method, params=None, timeout=None):
                if method == "Network.setCookie":
                    self.set[params["name"]] = params["expires"]
                if method == "Network.getCookies":
                    return {"result": {"cookies": [
                        {"name": "hatch_vml", "value": "v", "expires": self.set["hatch_vml"] + 0.4},
                        {"name": "hatch_sess", "value": "s", "expires": 1900000000}]}}
                return {}
        page = FakePage()
        engine._apply_cookies(page, {"hatch_vml": "v", "hatch_sess": "s"}, {"hatch_sess": 1900000000})
        prev_page = engine.page
        engine.page = page
        try:
            live = engine.read_cookies()
        finally:
            engine.page = prev_page
        assert live["hatch_vml"]["expires"] == -1 and live["hatch_sess"]["expires"] == 1900000000, live
        print("PASS: placeholder expiry injected for cookies without expires is not read back as real")

        # socks5h 交给 Chromium 前归一成 socks5
        assert engine.__class__._parse_proxy("socks5h://1.2.3.4:1080")[0] == "socks5://1.2.3.4:1080"
        print("PASS: socks5h proxy is normalized for Chromium")

        # 排队上限：满了返回 429
        app.PENDING.count = app.MAX_PENDING
        try:
            status, body = await call(app.app, "POST", "/v1/chat/completions", json.dumps(
                {"messages": [{"role": "user", "content": "hi"}], "stream": True}).encode())
            assert status == 429, (status, body)
        finally:
            app.PENDING.count = 0
        print("PASS: requests beyond MAX_PENDING get 429 instead of exhausting the thread pool")

        # 视频轮询接口不返回图片任务
        img_task = store.create_task("image", "img")
        status, _body = await call(app.app, "GET", f"/v1/videos/{img_task['id']}")
        assert status == 404, status
        print("PASS: /v1/videos/{id} rejects non-video tasks")

        # 没有 hatch_vml 有效期的账号：首次同步不把锚点挪到同步时刻
        old = store.add_account({"hatch_sess": "anchor-s"}, label="anchor")
        assert old["expiry_anchor"] == old["created_at"], old
        # 模拟旧数据：没有锚点、36 小时前导入
        store.update_account(old["id"], clear=("expiry_anchor",), created_at=int(time.time()) - 36 * 3600)
        store.update_account(old["id"], cookies_exp={"hatch_sess": 1900000000})
        left_h = (store.get_account(old["id"])["expires_at"] - time.time()) / 3600
        assert 11 < left_h < 13, left_h
        print(f"PASS: expiry estimate stays anchored at import time ({left_h:.1f}h left after 36h)")

        # 把别的账号的会话塞给另一个账号：拒绝
        status, body = await call(app.app, "POST", f"/admin/accounts/{old['id']}/cookies",
                                  json.dumps({"cookie_header": "hatch_sess=n; hatch_vml=x"}).encode())
        assert status == 409, (status, body)
        print("PASS: cookie update cannot reuse a session owned by another account")

        # 对话 worker 的底层异常：非流式返回 OpenAI 格式 502，账号留下记录
        class CDPBoom(Exception):
            pass

        def boom_stream(*a, **k):
            raise CDPBoom("连接已关闭")
            yield ""  # noqa: unreachable - keeps this a generator
        orig_chat = engine.chat_stream
        engine.chat_stream = boom_stream
        try:
            for live_acc in store.accounts:
                store.update_account(live_acc["id"], enabled=live_acc["id"] == aid)
            store.mark(aid, True, "")
            status, body = await call(app.app, "POST", "/v1/chat/completions", json.dumps(
                {"messages": [{"role": "user", "content": "hi"}], "timeout": 5}).encode())
            assert status == 502 and "error" in body, (status, body)
            assert "连接已关闭" in store.get_account(aid)["note"], store.get_account(aid)
        finally:
            engine.chat_stream = orig_chat
        print("PASS: unexpected engine exceptions become 502 OpenAI errors and are noted on the account")

        # image 传数组按 images 处理；认不出的单图值返回 400
        png = "data:image/png;base64," + base64.b64encode(b"\x89PNG\r\n\x1a\nx").decode()
        refs = app._image_references(app.ImageRequest(prompt="x", image=[png, png]))
        assert refs == [png, png], refs
        for bad in ({"prompt": "x", "image": 123}, {"prompt": "x", "images": ["none"]}):
            status, body = await call(app.app, "POST", "/v1/images/generations", json.dumps(bad).encode())
            assert status == 400, (bad, status, body)
        print("PASS: image arrays are used as references; unrecognized references are rejected")

        # /v1/responses 的 content 为对象时按 content part 处理；非法类型 422
        for content, want in (({"type": "input_text", "text": "hi"}, None), (123, 422)):
            status, body = await call(app.app, "POST", "/v1/responses", json.dumps(
                {"input": [{"role": "user", "content": content}], "timeout": 5}).encode())
            assert status != 500 and (want is None or status == want), (content, status, body)
        print("PASS: /v1/responses bad content types are 4xx, not 500")

        # 框架层 404 / 405 在 /v1 下也是 OpenAI 错误格式
        for method, path in (("GET", "/v1/nope"), ("GET", "/v1/chat/completions")):
            status, body = await call(app.app, method, path)
            assert status in (404, 405) and "error" in body, (path, status, body)
        print("PASS: framework 404/405 under /v1 use the OpenAI error shape")

        # 未结束的任务不能单条删除
        live = store.create_task("video", "live")
        store.update_task(live["id"], status="processing")
        status, _body = await call(app.app, "DELETE", f"/admin/tasks/{live['id']}")
        assert status == 409 and store.get_task(live["id"]), status
        store.update_task(live["id"], status="failed")
        print("PASS: deleting a running task is refused")

        # 保活跳过正在使用的账号
        app.ACCOUNT_POOL.busy_accounts[aid] = 1
        try:
            calls_before = []
            orig_renew = engine.renew_session_http
            engine.renew_session_http = lambda *a, **k: calls_before.append(1) or {}
            res = app._probe_account_sync(aid)
            assert res.get("skipped") and not calls_before, res
        finally:
            engine.renew_session_http = orig_renew
            app.ACCOUNT_POOL.busy_accounts.pop(aid, None)
        print("PASS: keepalive skips an account that is busy generating")

        # 管理端探测指定账号时，账号忙不占用全局浏览器槽位
        app.ACCOUNT_POOL.busy_accounts[aid] = 1
        seen_locked = []

        def probe():
            try:
                with app._leased_account(aid, timeout=1):
                    pass
            except app.MuseGenerationError:
                pass
        t = asyncio.get_running_loop().run_in_executor(None, probe)
        await asyncio.sleep(0.3)
        seen_locked.append(app.GEN_LOCK.locked())
        await t
        app.ACCOUNT_POOL.busy_accounts.pop(aid, None)
        assert seen_locked == [False], seen_locked
        print("PASS: admin probe waits for a busy account without holding a browser slot")

        # 软隐身保活：页面内 /api/session 未返回 assigned 时不报成功、不改状态
        app.CFG.stealth = True
        orig_refresh = engine.refresh
        for status_code in (403, 500):
            def fake_refresh(*a, _sc=status_code, **k):
                engine.last_renew = {"status": _sc, "assigned": False, "note": f"/api/session HTTP {_sc}"}
            engine.refresh = fake_refresh
            store.mark(aid, False, "dead")
            res = app._probe_account_sync(aid)
            acc_now = store.get_account(aid)
            assert res["ok"] is False and acc_now["ok"] is False, (status_code, res, acc_now)
            assert f"HTTP {status_code}" in acc_now["note"], acc_now
        engine.refresh = orig_refresh
        print("PASS: in-browser keepalive does not report success on 403/5xx renewals")

        # 额度耗尽：暂停调度，后续选号跳过它
        b_acc = store.add_account({"hatch_sess": "quota-b", "hatch_vml": "v"}, label="B")
        for x in store.accounts:
            store.update_account(x["id"], enabled=x["id"] in (aid, b_acc["id"]))
        store.mark(aid, True, "")
        app._record_generation_failure(aid, app.MuseQuotaError("账号额度不足"))
        picks = [store.pick_account(record_use=True)["id"] for _ in range(4)]
        assert set(picks) == {b_acc["id"]}, picks
        store.update_account(aid, quota_blocked_until=0)
        print("PASS: quota-exhausted account is paused from scheduling")

        # 生成期间换上了新会话：旧页面的 cookie 不回写
        store.update_account(aid, cookies={"hatch_sess": "NEW-SESS", "hatch_vml": "v-new"})
        engine.loaded_sess = "OLD-SESS"
        orig_read = engine.read_cookies
        engine.read_cookies = lambda: {"hatch_sess": {"value": "OLD-SESS", "expires": -1}}
        try:
            res = app._sync_cookies(aid)
        finally:
            engine.read_cookies = orig_read
            engine.loaded_sess = None
        assert store.get_account(aid)["cookies"]["hatch_sess"] == "NEW-SESS", res
        print("PASS: cookies from a replaced session are not written back")

        # 同步接口：排队耗尽时限后不发提示词；时限到时返回真实错误
        sent = []
        engine.generate = lambda *a, **k: sent.append(k["timeout"]) or {"filename": "x", "size": 1, "kind": "image"}
        slots = app.GEN_LOCK.limit  # 占满全部浏览器槽位，8 秒后释放
        for _ in range(slots):
            app.GEN_LOCK.acquire()
        releaser = asyncio.get_running_loop().run_in_executor(
            None, lambda: (time.sleep(8), [app.GEN_LOCK.release() for _ in range(slots)]))
        try:
            await asyncio.get_running_loop().run_in_executor(
                None, lambda: app._run_generation("p", "image", 10, account_id=aid))
            raise AssertionError("must not send with ~2s left")
        except app.MuseGenerationError as exc:
            assert "未发送提示词" in str(exc), exc
        await releaser
        assert not sent, sent
        print("PASS: sync generation with too little time left after queueing does not send the prompt")

        # multipart 声明为 octet-stream、内容是 PNG：按文件头接受
        boundary = "b"
        png_bytes = b"\x89PNG\r\n\x1a\nbody"
        body = (f'--{boundary}\r\nContent-Disposition: form-data; name="prompt"\r\n\r\nedit\r\n'
                f'--{boundary}\r\nContent-Disposition: form-data; name="async"\r\n\r\ntrue\r\n'
                f'--{boundary}\r\nContent-Disposition: form-data; name="image"; filename="ref.webp"\r\n'
                f'Content-Type: application/octet-stream\r\n\r\n').encode() + png_bytes + f"\r\n--{boundary}--\r\n".encode()
        status, resp = await call(app.app, "POST", "/v1/images/edits", body,
                                  headers={"content-type": f"multipart/form-data; boundary={boundary}"})
        assert status == 202, (status, resp)
        for _ in range(100):  # 等后台任务结束，免得它在引擎桩还原后真去拉起浏览器
            if store.get_task(resp["task_id"])["status"] in ("completed", "failed"):
                break
            await asyncio.sleep(0.1)
        print("PASS: octet-stream uploads with image bytes are accepted by file header")

        # 选号：额度暂停的账号即便是视频偏好账号也不选；健康账号只是忙时排队等，不退回坏账号
        vid = store.create_task("video", "v")
        store.update_task(vid["id"], status="completed", account=aid, created_at=int(time.time()) + 10)
        store.block_quota(aid)
        used = []
        engine.generate = lambda *a, **k: used.append(k["account_id"]) or {"filename": "x", "size": 1, "kind": "video"}
        app._run_generation("p", "video", 30)
        assert used == [b_acc["id"]], used
        store.update_account(aid, quota_blocked_until=0)
        print("PASS: the preferred video account is skipped while quota-paused")

        store.mark(b_acc["id"], False, "dead")
        used.clear()
        app.ACCOUNT_POOL.busy_accounts[aid] = 1

        def release_later():
            time.sleep(1.5)
            app.ACCOUNT_POOL.release(aid)
        rel = asyncio.get_running_loop().run_in_executor(None, release_later)
        await asyncio.get_running_loop().run_in_executor(None, lambda: app._run_generation("p", "image", 30))
        await rel
        assert used == [aid], used
        store.mark(b_acc["id"], True, "")
        print("PASS: a busy healthy account is waited for instead of falling back to a dead one")

        # 同一 Idempotency-Key：原任务失败后可以重提
        req_body = json.dumps({"prompt": "idem", "async": True}).encode()
        status, first = await call(app.app, "POST", "/v1/images/tasks", req_body, headers={"idempotency-key": "k-r6"})
        for _ in range(100):
            if store.get_task(first["task_id"])["status"] in ("completed", "failed"):
                break
            await asyncio.sleep(0.05)
        store.update_task(first["task_id"], status="failed", error="服务重启中断了任务，请重新提交")
        status, second = await call(app.app, "POST", "/v1/images/tasks", req_body, headers={"idempotency-key": "k-r6"})
        assert status == 202 and second["task_id"] != first["task_id"], (first, second)
        for _ in range(100):
            if store.get_task(second["task_id"])["status"] in ("completed", "failed"):
                break
            await asyncio.sleep(0.05)
        print("PASS: a failed task does not pin its Idempotency-Key")

        # 全池保活的计数：失败的不算成功
        def failing_probe(aid_, check_quota=False):
            return {"ok": False, "id": aid_, "label": aid_, "error": "页面加载超时"}
        orig_probe = app._probe_account_sync
        app._probe_account_sync = failing_probe
        try:
            summary = await app.run_keepalive_all(force=True)
        finally:
            app._probe_account_sync = orig_probe
        assert summary["ok_count"] == 0 and summary["failed_count"] == summary["refreshed_count"] > 0, summary
        print("PASS: keepalive summary separates successes from failures")

        # 切号重试拿不到账号：返回第一次的真实错误；成品已生成的失败不换号重发
        calls_gen = []

        def auth_dead(*a, **k):
            calls_gen.append(k["account_id"])
            raise app.MuseAuthError("会话已失效 (/api/session HTTP 401)")
        engine.generate = auth_dead
        app.ACCOUNT_POOL.busy_accounts[b_acc["id"]] = 1
        saved_wait = app.SWITCH_WAIT
        app.SWITCH_WAIT = 1
        try:
            try:
                app._run_generation("p", "image", 30, account_id=aid)
                raise AssertionError("should fail")
            except app.MuseAuthError as exc:
                assert "401" in str(exc), exc
        finally:
            app.SWITCH_WAIT = saved_wait
            app.ACCOUNT_POOL.busy_accounts.pop(b_acc["id"], None)
        store.mark(aid, True, "")
        print("PASS: retry that cannot lease another account returns the original auth error")

        calls_gen.clear()

        def fetched_failed(*a, **k):
            calls_gen.append(k["account_id"])
            raise app.MuseSubmittedError("已生成但未能取回文件")
        engine.generate = fetched_failed
        try:
            app._run_generation("p", "image", 30)
        except app.MuseSubmittedError:
            pass
        assert len(calls_gen) == 1, calls_gen
        print("PASS: post-generation fetch failures are not resubmitted on another account")

        # Windows 盘符 / NTFS 流形式的媒体名被拒绝
        for bad in ("D:secret.txt", "x.png:stream", "..", "a/../b"):
            assert not app._safe_media_name(bad), bad
        assert app._safe_media_name("abc.png")
        print("PASS: media names with drive letters / streams are rejected")

        # 流式输出平滑：大块文本的总延迟有上限
        t0 = time.monotonic()
        pieces = list(app._pace_text("x" * 6000))
        assert "".join(pieces) == "x" * 6000 and time.monotonic() - t0 < app.PACE_MAX_SECONDS + 1, len(pieces)
        print("PASS: pacing a 6000-char chunk is bounded")

        # 请求体超限：413
        saved_limit = app.MAX_REQUEST_BODY
        app.MAX_REQUEST_BODY = 1024
        try:
            status, body = await call(app.app, "POST", "/v1/chat/completions", b"x" * 2048)
        finally:
            app.MAX_REQUEST_BODY = saved_limit
        assert status == 413, (status, body)
        print("PASS: oversized request bodies are rejected with 413")

        # 跨域客户端收到的 413 也带 CORS 头（CORS 中间件在最外层）
        app.MAX_REQUEST_BODY = 1024
        starts = []
        payload = b"x" * 2048
        scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
                 "method": "POST", "scheme": "http", "path": "/v1/chat/completions",
                 "raw_path": b"/v1/chat/completions", "query_string": b"", "root_path": "",
                 "headers": [(b"origin", b"https://webui.example"), (b"content-type", b"application/json"),
                             (b"authorization", b"Bearer test-secret"),
                             (b"content-length", str(len(payload)).encode())],
                 "server": ("test", 80), "client": ("test", 1)}
        delivered = []

        async def receive_once():
            if not delivered:
                delivered.append(1)
                return {"type": "http.request", "body": payload, "more_body": False}
            await asyncio.Event().wait()

        async def send_capture(message):
            if message["type"] == "http.response.start":
                starts.append(message)
        try:
            await app.app(scope, receive_once, send_capture)
        finally:
            app.MAX_REQUEST_BODY = saved_limit
        hdrs = {k.decode().lower(): v.decode() for k, v in starts[0]["headers"]}
        assert starts[0]["status"] == 413 and hdrs.get("access-control-allow-origin"), starts
        print("PASS: the 413 body-limit response carries CORS headers")

        # 独立管理 Key：设置后 /v1 的 Key 不能调用 /admin
        app.CFG.admin_key = "m2a_admin_only"
        try:
            status, _b = await call(app.app, "GET", "/admin/apikey")
            assert status == 401, status
            status, _b = await call(app.app, "GET", "/admin/apikey",
                                    headers={"authorization": "Bearer m2a_admin_only"})
            assert status == 200, status
            status, _b = await call(app.app, "GET", "/v1/models")
            assert status == 200, status
        finally:
            app.CFG.admin_key = ""
        print("PASS: MUSE2API_ADMIN_KEY separates admin access from the /v1 key")

        # /v1/models：每个模型对象都带 OpenAI 规范要求的 id / object / created / owned_by
        status, body = await call(app.app, "GET", "/v1/models")
        assert status == 200 and body.get("object") == "list" and body.get("data"), (status, body)
        for item in body["data"]:
            assert item.get("object") == "model" and isinstance(item.get("created"), int) \
                and item["created"] > 0 and item.get("owned_by") and item.get("id"), item
        print("PASS: /v1/models items include the required created field")

        # duration 边界
        status, _b = await call(app.app, "POST", "/v1/videos", json.dumps({"prompt": "x", "duration": -5}).encode())
        assert status == 422, status
        print("PASS: invalid video duration is rejected")

        # tasks.json 自动裁剪已结束任务
        import store as store_mod
        saved_max = store_mod.MAX_TASKS
        store_mod.MAX_TASKS = 3
        try:
            for i in range(6):
                t_ = store.create_task("image", f"prune-{i}")
                store.update_task(t_["id"], status="completed")
            store.create_task("image", "trigger")
            done = [t_ for t_ in store.tasks.values() if t_.get("status") == "completed"]
            assert len(done) <= 3, len(done)
        finally:
            store_mod.MAX_TASKS = saved_max
        print("PASS: finished tasks are pruned to MAX_TASKS")

        # 管理端探测：槽位全被占时，不占着账号等槽位（否则与「先槽位后账号」的生成任务相向卡住）
        slots = app.GEN_LOCK.limit
        for _ in range(slots):
            app.GEN_LOCK.acquire()
        held = []

        def probe_waits_slot():
            try:
                with app._leased_account(aid, timeout=1.5):
                    pass
            except app.MuseGenerationError:
                pass
        fut = asyncio.get_running_loop().run_in_executor(None, probe_waits_slot)
        await asyncio.sleep(0.5)
        held.append(app.ACCOUNT_POOL.busy_accounts.get(aid, 0))
        await fut
        for _ in range(slots):
            app.GEN_LOCK.release()
        assert held == [0], held
        print("PASS: admin probe never holds the account while waiting for a browser slot")

        # 慢速参考图源：总时限内放弃
        class SlowResp:
            def read(self, n):
                time.sleep(0.05)
                return b"x"
        saved_dl = engine_module.REF_DOWNLOAD_SECONDS
        try:
            t0 = time.monotonic()
            try:
                engine_module._read_with_deadline(SlowResp(), 10 ** 6, 0.3)
                raise AssertionError("slow source must hit the deadline")
            except app.MuseGenerationError:
                pass
            assert time.monotonic() - t0 < 1.5
        finally:
            engine_module.REF_DOWNLOAD_SECONDS = saved_dl
        # 真实 socket：服务端每 0.5s 吐 1 字节（远小于 socket 超时），总时限仍要生效
        import socket as socket_mod
        import threading as threading_mod
        import urllib.request as urlreq
        srv = socket_mod.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        stop_trickle = threading_mod.Event()

        def trickle():
            conn, _ = srv.accept()
            conn.recv(4096)
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: 100\r\n\r\n")
            while not stop_trickle.is_set():
                try:
                    conn.sendall(b"x")
                except OSError:
                    break
                time.sleep(0.5)
            conn.close()
        th = threading_mod.Thread(target=trickle, daemon=True)
        th.start()
        try:
            with urlreq.urlopen(f"http://127.0.0.1:{srv.getsockname()[1]}/x.png", timeout=20) as resp:
                t0 = time.monotonic()
                try:
                    engine_module._read_with_deadline(resp, 10 ** 6, 2)
                    raise AssertionError("trickling source must hit the deadline")
                except app.MuseGenerationError:
                    pass
                took = time.monotonic() - t0
            assert took < 4, took
        finally:
            stop_trickle.set()
            srv.close()
        print(f"PASS: a trickling real socket is cut off at the deadline ({took:.1f}s for a 2s limit)")
        print("PASS: slow reference image downloads stop at the total deadline")

        # 同步生图：叫停后不再发提示词
        stop = app.threading.Event()
        stop.set()
        sent.clear()
        engine.generate = lambda *a, **k: sent.append(1) or {"filename": "x", "size": 1, "kind": "image"}
        try:
            app._run_generation("p", "image", 30, account_id=aid, stop_event=stop)
            raise AssertionError("stopped request must not generate")
        except app._ClientGone:
            pass
        assert not sent and not app.ACCOUNT_POOL.busy_accounts and not app.GEN_LOCK.locked()
        print("PASS: a disconnected sync image request does not send the prompt and frees resources")

        # start()：复用的已有浏览器连接正常时直接返回，不每次新建 websocket
        class LiveConn:
            closed = False
        saved_proc, saved_browser = engine.proc, engine.browser
        saved_start = app.MuseEngine.start
        try:
            engine.proc, engine.browser = None, LiveConn()
            conn = engine.browser
            app.MuseEngine.start(engine)
            assert engine.browser is conn
            assert app._browser_running()
        finally:
            engine.proc, engine.browser = saved_proc, saved_browser
        print("PASS: start() keeps a healthy reused browser connection")

        # 取回成品受时限约束：到期不再重试
        class SlowPage:
            calls = 0

            def js(self, *a, timeout=None, **k):
                SlowPage.calls += 1
                assert timeout is not None and timeout <= 3, timeout
                time.sleep(0.2)
                return json.dumps({"ok": False, "err": "slow"})
        saved_page = engine.page
        engine.page = SlowPage()
        try:
            t0 = time.monotonic()
            try:
                engine.extract_bytes("blob:x", deadline=time.monotonic() + 3)
            except app.MuseGenerationError:
                pass
            assert time.monotonic() - t0 < 6, time.monotonic() - t0
        finally:
            engine.page = saved_page
        print("PASS: result fetch respects the deadline instead of retrying for minutes")

        # 管理页多行粘贴：每行一个 cookie 的格式当成一个账号；残缺账号逐个报警
        parsed = app.parse_batch("hatch_sess=S1\nhatch_vml=V1\nhatch_gw=G1")
        assert len(parsed) == 1 and parsed[0][1] == {"hatch_sess": "S1", "hatch_vml": "V1", "hatch_gw": "G1"}, parsed
        status, body = await call(app.app, "POST", "/admin/accounts", json.dumps(
            {"batch": "a | hatch_sess=X1; hatch_vml=Y1\nb | hatch_sess=X2"}).encode())
        assert status == 200 and body["count"] == 2 and body["warning"], body
        assert body["added"][1]["essential_missing"] and not body["added"][0]["essential_missing"], body
        print("PASS: one-cookie-per-line paste is one account; incomplete batch accounts are flagged")

        # 同一会话重新导入（没有 hatch_vml 有效期）：寿命不往后推
        ra = store.add_account({"hatch_sess": "reimport-s"}, label="re")
        store.update_account(ra["id"], expiry_anchor=int(time.time()) - 86400)
        before_exp = store.update_account(ra["id"], cookies_exp={})["expires_at"]
        store.add_account({"hatch_sess": "reimport-s"}, label="re")
        assert store.get_account(ra["id"])["expires_at"] == before_exp, (before_exp, store.get_account(ra["id"]))
        print("PASS: re-importing the same session does not extend its estimated lifetime")

        # 管理页「更新 Cookie」粘贴同一会话：寿命同样不往后推；换成新会话才重置锚点
        status, _b = await call(app.app, "POST", f"/admin/accounts/{ra['id']}/cookies",
                                json.dumps({"cookie_header": "hatch_sess=reimport-s; hatch_vml=v2"}).encode())
        assert status == 200, (status, _b)
        assert store.get_account(ra["id"])["expires_at"] == before_exp, (before_exp, store.get_account(ra["id"]))
        status, _b = await call(app.app, "POST", f"/admin/accounts/{ra['id']}/cookies",
                                json.dumps({"cookie_header": "hatch_sess=reimport-new; hatch_vml=v3"}).encode())
        assert status == 200 and store.get_account(ra["id"])["expires_at"] > before_exp + 20 * 3600, \
            store.get_account(ra["id"])
        print("PASS: cookie update with the same session keeps its lifetime; a new session resets it")

        # 管理接口请求体类型错误：返回 400，不是 500
        for path, payload in ((f"/admin/accounts/{ra['id']}/cookies", {"cookies": {"hatch_sess": "zz"}, "expires": [1, 2]}),
                              (f"/admin/accounts/{ra['id']}/cookies", {"cookies": ["a"]}),
                              ("/admin/tasks/clear", {"keep": "abc"}),
                              ("/admin/tasks/clear", {"keep": -1})):
            status, _b = await call(app.app, "POST", path, json.dumps(payload).encode())
            assert status == 400, (path, payload, status, _b)
        print("PASS: malformed admin payloads are rejected with 400")

        # 有进行中的生成时，一键升级先拒绝（409），不拉代码、不重启
        pulled = []
        saved_up = app._upgrade_from_github_sync
        app._upgrade_from_github_sync = lambda: pulled.append(1) or {"ok": True}
        app.GEN_LOCK.acquire()
        try:
            status, _b = await call(app.app, "POST", "/admin/update/upgrade", json.dumps({"restart": True}).encode())
            assert status == 409 and not pulled, (status, _b, pulled)
            status, _b = await call(app.app, "POST", "/admin/update/upgrade", json.dumps({"restart": False}).encode())
            assert status == 200 and pulled == [1], (status, _b, pulled)
        finally:
            app.GEN_LOCK.release()
            app._upgrade_from_github_sync = saved_up
        print("PASS: upgrade with restart is refused while generation is running")

        # 账号配置不在了（请求进行中账号被删除）：不开页，绝不按「无代理」直连
        real_open = engine_module.MuseEngine._open_page
        engine.set_account_meta("r20-gone", proxy="http://u:p@127.0.0.1:9")
        engine.drop_account_meta("r20-gone")
        try:
            real_open(engine, "r20-gone")
            raise AssertionError("must refuse to open a page")
        except engine_module.MuseGenerationError as exc:
            assert "配置已不存在" in str(exc), exc
        print("PASS: a page is not opened when the account's meta is gone")

        # data/.env 含管理 Key：权限 600
        if os.name == "posix":
            app._persist_env("M2A_R20_PROBE", "x")
            env_path = app.dotenv_paths(app.CFG.base_dir)[-1]
            assert env_path.startswith(_HOME), env_path
            mode = os.stat(env_path).st_mode & 0o777
            assert mode == 0o600, oct(mode)
            print("PASS: data/.env is written with mode 600")

        # 生图结果：小的气泡内联图不算成品，附件容器优先；图片请求不接受视频源
        seq = iter([
            [{"src": "favicon.png", "tid": "agent-media", "w": 16, "h": 16, "hasVideo": False}],
            [{"src": "favicon.png", "tid": "agent-media", "w": 16, "h": 16, "hasVideo": False},
             {"src": "clip.mp4", "tid": "hatch-chat-attachment-presentation-image", "w": 512, "h": 512,
              "hasVideo": False}],
            [{"src": "favicon.png", "tid": "agent-media", "w": 16, "h": 16, "hasVideo": False},
             {"src": "real.png", "tid": "hatch-chat-attachment-presentation-image", "w": 512, "h": 512,
              "hasVideo": False}],
        ])
        last_list = []

        def att_seq():
            nonlocal last_list
            last_list = next(seq, last_list)
            return last_list
        saved_att, saved_scroll, saved_pg = engine.attachments, engine._scroll_bottom, engine.page
        engine.attachments, engine._scroll_bottom = att_seq, lambda: None

        class QuietPage:
            def js(self, *a, **k):
                return json.dumps({"cnt": 0, "txt": "", "stop": True, "tail": "", "sys_tail": "", "path": "/thread/x"})
        engine.page = QuietPage()
        try:
            picked = engine._wait_attachment("", 6, "image")
        finally:
            engine.attachments, engine._scroll_bottom, engine.page = saved_att, saved_scroll, saved_pg
        assert picked and picked["src"] == "real.png", picked
        print("PASS: tiny inline bubble images and video sources are not delivered as image results")

        # 额度判断：隐藏提示 / 宽泛的「token limit」字样不算耗尽
        assert not engine_module._QUOTA_RE.search("You are close to your token limit")
        assert not engine_module._QUOTA_RE.search("单次达到上限会自动截断")
        assert engine_module._QUOTA_RE.search("You have reached your weekly limit")
        assert engine_module._QUOTA_RE.search("账号额度已用完")
        print("PASS: quota detection only matches explicit exhaustion messages")

        # 补 Cookie / 重新导入解除额度暂停
        store.block_quota(aid)
        status, _b = await call(app.app, "POST", f"/admin/accounts/{aid}/cookies",
                                json.dumps({"cookie_header": "hatch_sess=n; hatch_vml=n3; hatch_gw=n"}).encode())
        assert status == 200 and not store.get_account(aid).get("quota_blocked_until"), store.get_account(aid)
        print("PASS: updating cookies lifts the quota pause")

        # image_url 不再被生图接口静默丢弃
        refs = app._image_references(app.ImageRequest(prompt="x", image_url=png))
        assert refs == [png], refs
        print("PASS: image_url is honoured by the image generation endpoints")

        # 代理：格式非法时导入被拒；运行时用不上就失败关闭
        status, body = await call(app.app, "POST", "/admin/accounts", json.dumps(
            {"cookie_header": "hatch_sess=px", "proxy": "http://u:p@1.2.3.4:80800"}).encode())
        assert status == 400, (status, body)
        assert engine.validate_proxy("socks5://u:p@host:1080") is None
        print("PASS: malformed proxies are rejected at import")

        # 读回 cookie：同名多份时取更晚到期的
        class TwoCopies:
            def send(self, method, params=None, timeout=None):
                return {"result": {"cookies": [
                    {"name": "hatch_vml", "value": "RENEWED", "expires": 1900000500, "domain": "muse.ai"},
                    {"name": "hatch_vml", "value": "OLD", "expires": 1900000100, "domain": ".muse.ai"}]}}
        prev_page = engine.page
        engine.page = TwoCopies()
        try:
            assert engine.read_cookies()["hatch_vml"]["value"] == "RENEWED"
        finally:
            engine.page = prev_page
        print("PASS: duplicate cookies resolve to the later-expiring copy")

        # 列表接口返回快照：并发 update 新增字段不影响序列化
        snap = store.list_tasks(5)
        store.update_task(snap[0]["id"], brand_new_field=1)
        assert "brand_new_field" not in snap[0]
        print("PASS: list_tasks returns snapshots, not live dicts")

        # PATCH 不回显代理账密；批量导入带上代理
        status, body = await call(app.app, "PATCH", f"/admin/accounts/{aid}", json.dumps(
            {"proxy": "http://user:secret@1.2.3.4:8080"}).encode())
        assert status == 200 and "secret" not in json.dumps(body), body
        status, body = await call(app.app, "POST", "/admin/accounts", json.dumps(
            {"batch": "x | hatch_sess=BX; hatch_vml=VX\ny | hatch_sess=BY; hatch_vml=VY",
             "proxy": "socks5://9.9.9.9:1080"}).encode())
        assert status == 200 and all(store.get_account(i["id"])["proxy"] == "socks5://9.9.9.9:1080"
                                     for i in body["added"]), body
        store.update_account(aid, clear=("proxy",))
        print("PASS: PATCH masks proxy credentials; batch import applies the proxy")

        # 对话链路也识别额度耗尽
        class QuotaPage:
            def js(self, script, *a, **k):
                if "__m2aBase=" in script:
                    return 1
                if "data-m2a-hidden" in script and "slice(-700)" in script:
                    return "You have reached your weekly limit."
                return json.dumps({"cnt": 0, "total": 0, "txt": "", "all": "", "stop": True})
        saved = {n: getattr(engine, n) for n in ("ensure_page", "reset_thread", "_send", "page")}
        engine.ensure_page = lambda *a, **k: None
        engine.reset_thread = lambda *a, **k: None
        engine._send = lambda *a, **k: "clicked"
        engine.page = QuotaPage()
        try:
            try:
                list(engine.chat_stream({}, "hi", None, 10))
                raise AssertionError("quota message must fail the chat")
            except app.MuseQuotaError:
                pass
        finally:
            for n, v in saved.items():
                setattr(engine, n, v)
        print("PASS: chat stream detects quota exhaustion")

        # 页面断开：轮询立即失败，不空转到超时
        class DeadPage:
            closed = True

            def js(self, *a, **k):
                raise engine_module.CDPConnectionError("连接已关闭")
        prev_page = engine.page
        engine.page = DeadPage()
        try:
            t0 = time.monotonic()
            try:
                engine._wait_attachment("", 30, "image")
                raise AssertionError("dead page must fail fast")
            except engine_module.MusePageGoneError:
                pass
            assert time.monotonic() - t0 < 3, time.monotonic() - t0
        finally:
            engine.page = prev_page
        print("PASS: polling a disconnected page fails fast with MusePageGoneError")

        # 取回阶段下载兜底抛底层异常：按已计费处理（不换号重发）
        def raise_cdp(*a, **k):
            raise engine_module.CDPConnectionError("连接已关闭")
        stubs = {
            "ensure_page": lambda *a, **k: None, "reset_thread": lambda *a, **k: None,
            "_clear_attachments": lambda *a, **k: None, "attachments": lambda: [],
            "_send": lambda *a, **k: "clicked", "_agent_count": lambda: 0, "_user_count": lambda: 0,
            "_wait_attachment": lambda *a, **k: {"src": "blob:r", "tid": "image", "w": 512, "h": 512},
            "extract_bytes": raise_cdp, "_download_fallback": raise_cdp, "_debug_dump": lambda *a, **k: None,
        }
        stubs["_scroll_bottom"] = lambda *a, **k: None
        saved = {n: getattr(engine, n) for n in stubs}
        for n, f in stubs.items():
            setattr(engine, n, f)
        try:
            try:
                # engine.generate 在本测试里被换成过桩：调用类上的真实实现
                engine_module.MuseEngine.generate(engine, {}, "p", expect="image", timeout=30)
                raise AssertionError("must fail")
            except engine_module.MuseSubmittedError:
                pass
        finally:
            for n, f in saved.items():
                setattr(engine, n, f)
        print("PASS: download fallback failures after a result appeared are MuseSubmittedError")

        # 读回 cookie：到期相同时取站点新下发（不等于注入值）的那份
        class TiedCopies:
            def send(self, method, params=None, timeout=None):
                if method == "Network.getCookies":
                    return {"result": {"cookies": [
                        {"name": "hatch_vml", "value": "INJECTED", "expires": 1900000100},
                        {"name": "hatch_vml", "value": "SITE-NEW", "expires": 1900000100}]}}
                return {}
        tp = TiedCopies()
        engine._apply_cookies(tp, {"hatch_vml": "INJECTED"}, {"hatch_vml": 1900000100})
        prev_page = engine.page
        engine.page = tp
        try:
            assert engine.read_cookies()["hatch_vml"]["value"] == "SITE-NEW"
        finally:
            engine.page = prev_page
        print("PASS: tied duplicate cookies resolve to the site-issued value")

        # 示例占位 Key 不能用于鉴权
        assert "m2a_your_secret_admin_key_here" in app._PLACEHOLDER_KEYS
        saved_key = app.CFG.api_key
        app.CFG.api_key = "m2a_your_secret_admin_key_here"
        try:
            app._check_key("Bearer m2a_your_secret_admin_key_here", admin=False)
            raise AssertionError("placeholder key must not authenticate")
        except app.HTTPException as exc:
            assert exc.status_code == 401
        finally:
            app.CFG.api_key = saved_key
        print("PASS: the published example key is rejected")

        # 提示词已提交后页面断开：按已提交处理（不换号重发）
        def page_gone(*a, **k):
            raise engine_module.MusePageGoneError("gone")
        stubs = {
            "ensure_page": lambda *a, **k: None, "reset_thread": lambda *a, **k: None,
            "_clear_attachments": lambda *a, **k: None, "attachments": lambda: [],
            "_send": lambda *a, **k: "clicked", "_agent_count": lambda: 0,
            "_scroll_bottom": lambda *a, **k: None, "_wait_attachment": page_gone,
        }
        saved = {n: getattr(engine, n) for n in stubs}
        for n, f in stubs.items():
            setattr(engine, n, f)
        try:
            try:
                engine_module.MuseEngine.generate(engine, {}, "p", expect="image", timeout=30)
                raise AssertionError("must fail")
            except engine_module.MuseSubmittedError:
                pass
        finally:
            for n, f in saved.items():
                setattr(engine, n, f)
        print("PASS: page loss after the prompt was submitted is MuseSubmittedError")

        class StopThenGone:
            polls = 0

            def js(self, script, *a, **k):
                if script.startswith("window.__m2aBase="):
                    return 1
                if "data-m2a-hidden" in script:
                    return ""
                StopThenGone.polls += 1
                if StopThenGone.polls > 3:
                    raise engine_module.CDPConnectionError("连接已关闭")
                return json.dumps({"cnt": 0, "total": 0, "txt": "", "all": "", "stop": True})
        saved = {n: getattr(engine, n) for n in ("ensure_page", "reset_thread", "_send", "page")}
        engine.ensure_page = lambda *a, **k: None
        engine.reset_thread = lambda *a, **k: None
        engine._send = lambda *a, **k: "clicked"
        engine.page = StopThenGone()
        try:
            try:
                list(engine_module.MuseEngine.chat_stream(engine, {}, "hi", None, 10))
                raise AssertionError("must fail")
            except engine_module.MuseSubmittedError:
                pass
        finally:
            for n, v in saved.items():
                setattr(engine, n, v)
        print("PASS: chat page loss after the assistant accepted the prompt is MuseSubmittedError")

        # 点了发送却没提交：立即判为未确认
        class SendPage:
            state = {"v": "hello", "users": 0, "stop": False}

            def send(self, *a, **k):
                return {}

            def js(self, script, *a, **k):
                if "getBoundingClientRect" in script:
                    return json.dumps({"x": 1, "y": 1})
                if "s.call(ta,t)" in script:
                    return 5
                if "b.click()" in script:
                    return "clicked"
                if "users:n" in script:
                    return json.dumps(SendPage.state)
                if "chat-user-bubble" in script:
                    return "0"
                return None
        saved_page, saved_wait = engine.page, engine_module.MuseEngine.SEND_CONFIRM_SECONDS
        engine.page = SendPage()
        engine_module.MuseEngine.SEND_CONFIRM_SECONDS = 0.5
        try:
            assert engine_module.MuseEngine._send(engine, "hello") == "unconfirmed"
            SendPage.state = {"v": "", "users": 1, "stop": True}
            assert engine_module.MuseEngine._send(engine, "hello") == "clicked"
        finally:
            engine.page = saved_page
            engine_module.MuseEngine.SEND_CONFIRM_SECONDS = saved_wait
        print("PASS: a click that did not submit the prompt is reported as unconfirmed")

        # 点击发送之后、确认阶段页面断开：可能已提交，不换号重发
        class GoneAfterClick(SendPage):
            def js(self, script, *a, **k):
                if "users:n" in script:
                    raise engine_module.CDPConnectionError("连接已关闭")
                return SendPage.js(self, script, *a, **k)
        engine.page = GoneAfterClick()
        try:
            try:
                engine_module.MuseEngine._send(engine, "hello")
                raise AssertionError("must fail")
            except engine_module.MuseSubmittedError:
                pass
        finally:
            engine.page = saved_page
        print("PASS: page loss while confirming a clicked send is MuseSubmittedError")

        # 没有发送按钮、走回车兜底：按键发出后页面断开，同样按可能已提交处理
        class GoneAfterEnter(SendPage):
            keys = 0

            def send(self, method, *a, **k):
                if method == "Input.dispatchKeyEvent":
                    GoneAfterEnter.keys += 1
                    if GoneAfterEnter.keys > 3:
                        raise engine_module.CDPConnectionError("连接已关闭")
                return {}

            def js(self, script, *a, **k):
                if "b.click()" in script:
                    return "no-button"
                if "querySelector('textarea')||{value:''}" in script:
                    raise engine_module.CDPConnectionError("连接已关闭")
                return SendPage.js(self, script, *a, **k)
        engine.page = GoneAfterEnter()
        try:
            try:
                engine_module.MuseEngine._send(engine, "hello")
                raise AssertionError("must fail")
            except engine_module.MuseSubmittedError:
                pass
        finally:
            engine.page = saved_page
        print("PASS: page loss after the Enter fallback was sent is MuseSubmittedError")

        # 剩余时间不足：不写账号状态
        store.update_account(aid, enabled=True)
        store.mark(aid, True, "clean")
        try:
            app._run_generation_locked("p", "image", 60, account_id=aid, deadline=time.monotonic() + 5)
            raise AssertionError("must not send")
        except app._NoTimeLeft:
            pass
        assert store.get_account(aid)["note"] == "clean", store.get_account(aid)
        print("PASS: too little time left is not recorded as an account failure")

        # 全部账号额度暂停：生成链路直接报错，管理端探测照常选号
        live = [a["id"] for a in store.accounts if a.get("enabled", True) and a.get("cookies")]
        for x in live:
            store.block_quota(x)
        try:
            try:
                app.ACCOUNT_POOL.acquire(timeout=1)
                raise AssertionError("must not lease a quota-paused account")
            except app.QuotaPausedError:
                pass
            got = app.ACCOUNT_POOL.acquire(timeout=1, record_use=False, allow_quota_paused=True)
            app.ACCOUNT_POOL.release(got["id"])
        finally:
            for x in live:
                store.update_account(x, quota_blocked_until=0)
        assert app.ACCOUNT_POOL._quota_paused_until() is None
        print("PASS: generation fails fast when every account is quota-paused")

        # 登出判定：已登录页面上的「退出登录」「Sign in with Google to share」不算登出
        assert engine_module._looks_logged_out("sign in")
        assert engine_module._looks_logged_out("请登录后继续")
        assert not engine_module._looks_logged_out("设置 退出登录")
        assert not engine_module._looks_logged_out("sign in with google to share · sign out")
        assert not engine_module._looks_logged_out("signing images")
        print("PASS: logout detection ignores menus that only exist when signed in")

        # 生成期间管理员换上了新会话：旧会话的登出 / 额度失败不写到账号上
        for x in store.accounts:
            store.update_account(x["id"], enabled=x["id"] == aid, quota_blocked_until=0)
        store.update_account(aid, clear=("ok",), cookies={"hatch_sess": "R14-NEW", "hatch_vml": "v"})
        for exc_type in (app.MuseAuthError, app.MuseQuotaError):
            def stale_session(*a, _e=exc_type, **k):
                engine.loaded_sess = "R14-OLD"  # 本页面加载的是被替换前的会话
                raise _e("旧会话失败")
            engine.generate = stale_session
            try:
                app._run_generation("p", "image", 30)
                raise AssertionError("must fail")
            except exc_type:
                pass
            now_acc = store.get_account(aid)
            assert now_acc["ok"] is None and not now_acc.get("quota_blocked_until"), (exc_type, now_acc)
        def stale_success(*a, **k):
            engine.loaded_sess = "R14-OLD"
            return {"filename": "x", "size": 1, "kind": "image"}
        engine.generate = stale_success
        store.update_account(aid, clear=("ok",), note="已更新 cookie")
        app._run_generation("p", "image", 30)
        now_acc = store.get_account(aid)
        assert now_acc["ok"] is None and now_acc["note"] == "已更新 cookie", now_acc
        engine.loaded_sess = None
        print("PASS: failures of a session replaced mid-generation are not written to the account")
        print("PASS: success of a replaced session does not mark the new cookies healthy")

        # 一个账号失效、另一个额度暂停：直接报额度暂停，不把请求兜底发给暂停中的账号
        dead = store.add_account({"hatch_sess": "r14-dead", "hatch_vml": "v"}, label="dead")
        store.mark(dead["id"], False, "dead")
        store.block_quota(aid)
        try:
            try:
                app.ACCOUNT_POOL.acquire(timeout=1)
                raise AssertionError("must not lease the quota-paused account")
            except app.QuotaPausedError:
                pass
        finally:
            store.update_account(aid, quota_blocked_until=0)
            store.update_account(dead["id"], enabled=False)
        print("PASS: a dead account does not make a quota-paused account schedulable")

        # 账号测试：读额度时发现已登出，按认证失败处理
        saved = {n: getattr(engine, n) for n in ("refresh", "quota", "read_cookies")}

        def ok_refresh(*a, **k):
            engine.last_renew = {"status": 200, "assigned": True, "note": ""}
        engine.refresh = ok_refresh
        engine.read_cookies = lambda: {}

        def logged_out_quota(*a, **k):
            raise app.MuseAuthError("会话已被 muse.ai 登出")
        engine.quota = logged_out_quota
        try:
            status, body = await call(app.app, "POST", f"/admin/accounts/{aid}/test")
        finally:
            for n, v in saved.items():
                setattr(engine, n, v)
        assert status == 200 and body["ok"] is False, (status, body)
        assert store.get_account(aid)["ok"] is False, store.get_account(aid)
        store.mark(aid, True, "")
        print("PASS: account test reports a logout found while reading quota as an auth failure")

        # 非流式对话以页面最终文本为准（流式增量拼错时不跟着错）
        def placeholder_stream(*a, **k):
            yield "Thinking..."
            yield " world"
            engine.last_chat_text = "Hello world"
        orig_chat = engine.chat_stream
        engine.chat_stream = placeholder_stream
        try:
            status, body = await call(app.app, "POST", "/v1/chat/completions", json.dumps(
                {"messages": [{"role": "user", "content": "hi"}], "timeout": 5}).encode())
        finally:
            engine.chat_stream = orig_chat
        assert status == 200, (status, body)
        assert body["choices"][0]["message"]["content"] == "Hello world", body
        print("PASS: non-stream chat returns the final page text")

        # 成品已取回、本地落盘失败（如磁盘满）：按已提交处理，不换号重新生成
        blocker = os.path.join(_HOME, "not-a-dir")
        with open(blocker, "w") as f:
            f.write("x")
        stubs = {
            "ensure_page": lambda *a, **k: None, "reset_thread": lambda *a, **k: None,
            "_clear_attachments": lambda *a, **k: None, "attachments": lambda: [],
            "_send": lambda *a, **k: "clicked", "_agent_count": lambda: 0,
            "_scroll_bottom": lambda *a, **k: None,
            "_wait_attachment": lambda *a, **k: {"src": "blob:x", "tid": "t"},
            "extract_bytes": lambda *a, **k: (b"\x89PNG\r\n\x1a\nx", "image/png", "blob:x"),
        }
        saved = {n: getattr(engine, n) for n in stubs}
        for n, f in stubs.items():
            setattr(engine, n, f)
        from unittest.mock import patch
        try:
            with patch.object(type(engine.cfg), "media_dir", property(lambda s: os.path.join(blocker, "media"))):
                try:
                    engine_module.MuseEngine.generate(engine, {}, "p", expect="image", timeout=30)
                    raise AssertionError("must fail")
                except engine_module.MuseSubmittedError:
                    pass
        finally:
            for n, f in saved.items():
                setattr(engine, n, f)
        print("PASS: a local save failure after the result was fetched is MuseSubmittedError")

        # 对话：提示词已确认发出后首字超时，不换号重发（没出现 Stop 也一样）
        class SilentPage:
            def js(self, script, *a, **k):
                if script.startswith("window.__m2aBase="):
                    return 1
                if "data-m2a-hidden" in script:
                    return ""
                return json.dumps({"cnt": 0, "total": 0, "txt": "", "all": "", "stop": False})
        saved = {n: getattr(engine, n) for n in ("ensure_page", "reset_thread", "_send", "page")}
        engine.ensure_page = lambda *a, **k: None
        engine.reset_thread = lambda *a, **k: None
        engine._send = lambda *a, **k: "clicked"
        engine.page = SilentPage()
        clock = [0.0]
        from unittest.mock import patch as _patch
        fake_time = type("T", (), {"time": staticmethod(lambda: clock[0]),
                                    "sleep": staticmethod(lambda n: clock.__setitem__(0, clock[0] + n)),
                                    "monotonic": staticmethod(lambda: clock[0])})
        try:
            with _patch.object(engine_module, "time", fake_time):
                try:
                    list(engine_module.MuseEngine.chat_stream(engine, {}, "hi", None, 60))
                    raise AssertionError("must fail")
                except engine_module.MuseSubmittedError as exc:
                    assert "首字" in str(exc), exc
        finally:
            for n, v in saved.items():
                setattr(engine, n, v)
        print("PASS: first-token timeout after a confirmed send is MuseSubmittedError")

        # 保活期间管理员换上了新 cookie：旧会话的认证失败不写 ok=False
        store.update_account(aid, clear=("ok",), cookies={"hatch_sess": "R15-NEW", "hatch_vml": "v"})
        saved = {n: getattr(engine, n) for n in ("refresh",)}

        def stale_refresh(*a, **k):
            engine.loaded_sess = "R15-OLD"
            raise app.MuseAuthError("会话已被 muse.ai 登出")
        engine.refresh = stale_refresh
        saved_stealth = app.CFG.stealth
        app.CFG.stealth = True
        try:
            res = app._probe_account_sync(aid)
            assert res["ok"] is False and res.get("skipped"), res
            assert store.get_account(aid)["ok"] is None, store.get_account(aid)
            status, body = await call(app.app, "POST", f"/admin/accounts/{aid}/test")
            assert status == 200 and body["ok"] is False, (status, body)
            assert store.get_account(aid)["ok"] is None, store.get_account(aid)
        finally:
            app.CFG.stealth = saved_stealth
            for n, v in saved.items():
                setattr(engine, n, v)
            engine.loaded_sess = None
        store.mark(aid, True, "")
        print("PASS: keepalive/test results of a session replaced mid-probe are not written")

        # 对话：提示词已确认发出、Stop 还没出现时页面断开，也不换号重发
        class GoneBeforeStop:
            polls = 0

            def js(self, script, *a, **k):
                if script.startswith("window.__m2aBase="):
                    return 1
                if "data-m2a-hidden" in script:
                    return ""
                GoneBeforeStop.polls += 1
                if GoneBeforeStop.polls > 2:
                    raise engine_module.CDPConnectionError("连接已关闭")
                return json.dumps({"cnt": 0, "total": 0, "txt": "", "all": "", "stop": False})
        saved = {n: getattr(engine, n) for n in ("ensure_page", "reset_thread", "_send", "page")}
        engine.ensure_page = lambda *a, **k: None
        engine.reset_thread = lambda *a, **k: None
        engine._send = lambda *a, **k: "clicked"
        engine.page = GoneBeforeStop()
        try:
            try:
                list(engine_module.MuseEngine.chat_stream(engine, {}, "hi", None, 10))
                raise AssertionError("must fail")
            except engine_module.MuseSubmittedError:
                pass
        finally:
            for n, v in saved.items():
                setattr(engine, n, v)
        print("PASS: chat page loss after a confirmed send (before Stop) is MuseSubmittedError")

        # 全部账号额度暂停：同步接口返回 429 + Retry-After，不是 502
        live = [a["id"] for a in store.accounts if a.get("enabled", True) and a.get("cookies")]
        for x in live:
            store.block_quota(x)
        try:
            status, body = await call(app.app, "POST", "/v1/chat/completions", json.dumps(
                {"messages": [{"role": "user", "content": "hi"}], "timeout": 5}).encode())
            assert status == 429 and body["error"]["type"] == "rate_limit_error", (status, body)
            exc = app.QuotaPausedError("x", until=int(time.time()) + 600)
            assert 590 <= exc.retry_after() <= 600, exc.retry_after()
            assert app._quota_paused_http(exc).headers["Retry-After"] == str(exc.retry_after())
        finally:
            for x in live:
                store.update_account(x, quota_blocked_until=0)
        print("PASS: all-accounts quota pause is reported as 429 with Retry-After")

        # stats.expiring：只算 12 小时内到期、尚未过期的账号
        now_i = int(time.time())
        saved_exp = {a["id"]: a.get("expires_at") for a in store.accounts}
        try:
            for a in store.accounts:
                a["expires_at"] = now_i + 2 * 86400   # hatch_vml 正常寿命：不算即将过期
            st = store.stats()
            assert st["expiring"] == 0 and st["expired"] == 0, st
            store.accounts[0]["expires_at"] = now_i + 3600
            store.accounts[-1]["expires_at"] = now_i - 10
            st = store.stats()
            enabled_ids = {a["id"] for a in store.accounts if a.get("enabled", True)}
            want_expiring = 1 if store.accounts[0]["id"] in enabled_ids else 0
            want_expired = 1 if store.accounts[-1]["id"] in enabled_ids else 0
            assert (st["expiring"], st["expired"]) == (want_expiring, want_expired), st
        finally:
            for a in store.accounts:
                a["expires_at"] = saved_exp.get(a["id"])
        print("PASS: stats.expiring counts only accounts expiring within 12 hours")

        for n, fn in orig.items():
            setattr(engine, n, fn)
    finally:
        import shutil
        shutil.rmtree(_HOME, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
