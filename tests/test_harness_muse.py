"""仿真 muse.ai 脚手架冒烟测试：python3 tests/test_harness_muse.py

离线运行：Chromium 经包装脚本只访问本地自签仿真站，不触达真实 muse.ai、不消耗额度。
缺少 openssl 或 Chromium 时打印 SKIP 并以 0 退出。
"""
import http.client
import json
import os
from pathlib import Path
import ssl
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness_muse as hm  # noqa: E402


def _https(port, method, path, headers=None, body=None):
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    conn = http.client.HTTPSConnection("127.0.0.1", port, timeout=10, context=ctx)
    try:
        conn.request(method, path, body=body, headers=headers or {})
        resp = conn.getresponse()
        return resp.status, resp.getheader("Set-Cookie"), resp.read()
    finally:
        conn.close()


def test_fake_muse_contract():
    """不开浏览器，直接核对仿真站的接口契约、开关与请求日志。"""
    with hm.FakeMuse() as muse:
        workdir = muse.workdir
        probe = {"Cookie": "hatch_sess=good", "User-Agent": "probe-ua",
                 "Accept-Language": "fr-CA,fr;q=0.9", "sec-ch-ua": '"Chromium";v="156"'}
        status, set_cookie, body = _https(muse.port, "GET", "/api/session", probe)
        assert status == 200, status
        assert json.loads(body) == {"status": "assigned", "vm_id": "vm-1", "vm_state": "SUSPENDED"}, body
        assert set_cookie and set_cookie.startswith("hatch_vml=RENEWED-1;") and "HttpOnly" in set_cookie, set_cookie
        entry = muse.log[0]
        assert (entry["method"], entry["path"], entry["cookie"], entry["user_agent"],
                entry["accept_language"], entry["sec_ch_ua"]) == (
            "GET", "/api/session", "hatch_sess=good", "probe-ua", "fr-CA,fr;q=0.9", '"Chromium";v="156"'), entry
        assert entry["time"] > 0 and entry["status"] == 200

        status, set_cookie, _ = _https(muse.port, "GET", "/api/session", {"Cookie": "a=1; hatch_sess=dead"})
        assert status == 401 and set_cookie is None, (status, set_cookie)

        muse.set_cookie_on_session = False
        muse.wake_sets_state = "RUNNING"
        status, _, _ = _https(muse.port, "POST", "/api/hatch/vm/wake",
                              {"Content-Type": "application/json"}, b'{"vm_id":"vm-1"}')
        assert status == 200 and muse.wake_count == 1
        assert muse.requests_to("/api/hatch/vm/wake", "POST")[0]["body"] == '{"vm_id":"vm-1"}'
        status, set_cookie, body = _https(muse.port, "GET", "/api/session", {"Cookie": "hatch_sess=good"})
        assert status == 200 and set_cookie is None and json.loads(body)["vm_state"] == "RUNNING"
        assert muse.session_count == 2

        assert _https(muse.port, "GET", "/media/img.png")[2] == hm.PNG
        assert _https(muse.port, "GET", "/media/vid.mp4")[2] == hm.MP4
        muse.slow_stream = True
        muse.weekly_used_pct = 42
        page = _https(muse.port, "GET", "/thread/new")[2].decode("utf-8")
        for needle in ('"slow": true', "42% used", 'aria-label="Send"', "hatch-composer-stop-button",
                       "hatch-agent-bubble-bg", "hatch-dock-more", "hatch-chat-attachment-presentation-"):
            assert needle in page, needle
        assert muse.expected_reply("hello") == "ECHO[hello] cookie_visible_to_js=true " + "S" * 300
    assert workdir and not os.path.exists(workdir), workdir
    print("PASS: fake muse contract (session renew / 401 / Set-Cookie toggle / vm_state / wake / media / page / request log)")


def test_load_app_isolation():
    """同一进程两次 load_app：各自读到本次环境变量、目录独立，关闭后恢复环境。"""
    keys = ("MUSE2API_HOME", "MUSE2API_CONCURRENCY", "MUSE2API_STEALTH")
    before = {k: os.environ.get(k) for k in keys}
    outer_stealth = (before["MUSE2API_STEALTH"] or "1").strip() != "0"
    first = hm.load_app({"MUSE2API_CONCURRENCY": "3", "MUSE2API_STEALTH": "0"}, "env-one")
    second = None
    try:
        # 第二次加载前恢复第一次的覆盖：MUSE2API_STEALTH 回到外部环境的取值
        second = hm.load_app({"MUSE2API_CONCURRENCY": "1"}, "env-two")
        assert (first.CFG.max_concurrency, first.CFG.stealth) == (3, False), first.CFG
        assert (second.CFG.max_concurrency, second.CFG.stealth) == (1, outer_stealth), second.CFG
        assert first.__name__ != second.__name__ and first.MuseEngine is not second.MuseEngine
        assert first.CFG.data_dir != second.CFG.data_dir
        for loaded in (first, second):
            p = Path(loaded.CFG.data_dir).resolve()
            assert p.is_relative_to(Path.home().resolve()) and not p.is_relative_to(hm.REPO_ROOT), p
    finally:
        hm.close_app(second)
        hm.close_app(first)
    assert not os.path.exists(first.HARNESS_WORKDIR) and not os.path.exists(second.HARNESS_WORKDIR)
    assert {k: os.environ.get(k) for k in keys} == before
    print("PASS: load_app isolation (fresh env per load, separate dirs under $HOME, env restored)")


def test_browser_smoke():
    """FakeMuse + Chromium 包装脚本 + load_app：用真实 app 代码与真实 Chromium 跑通主链路。"""
    from fastapi.testclient import TestClient

    with hm.FakeMuse() as muse:
        muse_dir = muse.workdir
        wrapper = hm.make_chromium_wrapper(muse.port, muse.workdir)
        app = hm.load_app({"MUSE2API_CHROMIUM": wrapper, "MUSE2API_CONCURRENCY": "2",
                           "MUSE2API_CHAT_TIMEOUT": "60", "MUSE2API_IMAGE_TIMEOUT": "90",
                           "MUSE2API_VIDEO_TIMEOUT": "120"}, "smoke")
        app_dir = app.HARNESS_WORKDIR
        try:
            home = Path.home().resolve()
            for d in (app.CFG.data_dir, app.CFG.download_dir, app.CFG.profile_dir):
                p = Path(d).resolve()
                assert p.is_relative_to(home) and not p.is_relative_to(hm.REPO_ROOT), d
            assert app.CFG.chromium == wrapper, app.CFG.chromium
            # MUSE2API_CHROMIUM 现在指向包装脚本：再找真实 Chromium 时不得把包装脚本再包一层
            assert hm.find_chromium() == hm.find_chromium(wrapper) != wrapper

            client = TestClient(app.app, raise_server_exceptions=False)
            headers = hm.auth_headers(app)
            now = int(time.time())
            aid = app.store.add_account({"hatch_sess": "good-A", "hatch_vml": "V0"}, "A",
                                        cookies_exp={"hatch_vml": now + 172800})["id"]

            prompt = "hello harness one"
            r = client.post("/v1/chat/completions", headers=headers,
                            json={"messages": [{"role": "user", "content": prompt}]})
            assert r.status_code == 200, r.text[:300]
            content = r.json()["choices"][0]["message"]["content"]
            assert content == muse.expected_reply(prompt), content
            print("PASS: non-stream chat ->", content)

            prompt = "hello harness two"
            r = client.post("/v1/chat/completions", headers=headers,
                            json={"stream": True, "messages": [{"role": "user", "content": prompt}]})
            assert r.status_code == 200, r.text[:300]
            data = [line[6:] for line in r.text.splitlines() if line.startswith("data: ")]
            assert data and data[-1] == "[DONE]", r.text[-300:]
            text = "".join((json.loads(x)["choices"][0].get("delta") or {}).get("content") or ""
                           for x in data[:-1])
            assert text == muse.expected_reply(prompt), text
            print("PASS: stream chat -> [DONE], assembled", text)

            r = client.post("/v1/images/generations", headers=headers,
                            json={"prompt": "a red dot", "size": "1:1"})
            assert r.status_code == 200, r.text[:300]
            item = r.json()["data"][0]
            media = client.get(item["url"])
            assert media.status_code == 200 and media.content == hm.PNG, (media.status_code, len(media.content))
            print(f"PASS: image -> {item['url']} ({len(media.content)} bytes, identical to fake site)")

            r = client.post("/v1/videos", headers=headers,
                            json={"prompt": "a moving dot", "duration": 5, "size": "16:9"})
            assert r.status_code == 200, r.text[:300]
            task_id, task = r.json()["id"], {}
            deadline = time.time() + 150
            while time.time() < deadline:
                task = client.get(f"/v1/videos/{task_id}", headers=headers).json()
                if task.get("status") in ("completed", "failed"):
                    break
                time.sleep(1)
            assert task.get("status") == "completed", task
            video = client.get(task["url"])
            assert video.status_code == 200 and video.content == hm.MP4, (video.status_code, len(video.content))
            print(f"PASS: video task -> completed, {task['url']} ({len(video.content)} bytes)")

            r = client.post(f"/admin/accounts/{aid}/test", headers=headers)
            body = r.json()
            assert r.status_code == 200 and body.get("ok") is True, body
            assert (body.get("quota") or {}).get("weekly_used_pct") == hm.WEEKLY_USED_PCT, body
            print(f"PASS: admin test -> ok, quota weekly_used_pct={body['quota']['weekly_used_pct']}")

            pages = muse.requests_to("/thread/new")
            renewals = [e for e in muse.requests_to("/api/session") if e["status"] == 200]
            assert pages and renewals and muse.wake_count >= 1, (len(pages), len(renewals), muse.wake_count)
            assert all("hatch_sess=good-A" in e["cookie"] for e in pages)
            assert renewals[0]["user_agent"] and renewals[0]["sec_ch_ua"], renewals[0]
            print(f"PASS: fake muse served {len(pages)} page loads, {len(renewals)} /api/session renewals, "
                  f"{muse.wake_count} wake calls via the Chromium wrapper")
        finally:
            hm.close_app(app)
    leftovers = [d for d in (app_dir, muse_dir) if os.path.exists(d)]
    assert not leftovers, leftovers
    print("PASS: browser and fake site stopped, temp dirs removed")


def test_browser_prompt_words_and_stalls():
    """真实 Chromium + FakeMuse：
    - 提示词里含「积分不足」：生图照常完成，不被误判为额度耗尽；
    - 助手停顿超过 5s 且 Stop 一直在：不能把截断回复当成功，按超时报错。"""
    from fastapi.testclient import TestClient

    with hm.FakeMuse(slow_marker="stall", slow_chars=1, slow_interval_ms=7000) as muse:
        wrapper = hm.make_chromium_wrapper(muse.port, muse.workdir)
        app = hm.load_app({"MUSE2API_CHROMIUM": wrapper, "MUSE2API_CONCURRENCY": "1",
                           "MUSE2API_CHAT_TIMEOUT": "60", "MUSE2API_IMAGE_TIMEOUT": "90"}, "words")
        try:
            client = TestClient(app.app, raise_server_exceptions=False)
            headers = hm.auth_headers(app)
            now = int(time.time())
            aid = app.store.add_account({"hatch_sess": "good-A", "hatch_vml": "V0"}, "A",
                                        cookies_exp={"hatch_vml": now + 172800})["id"]

            # FakeMuse 的助手回复会回显提示词末尾 12 个字：额度字样同时出现在用户气泡和助手气泡里
            r = client.post("/v1/images/generations", headers=headers,
                            json={"prompt": "image poster titled 积分不足 token limit"})
            assert r.status_code == 200, r.text[:300]
            print("PASS: quota words in the prompt and the assistant reply do not trip quota detection")

            t0 = time.time()
            r = client.post("/v1/chat/completions", headers=headers,
                            json={"messages": [{"role": "user", "content": "stall please"}], "timeout": 12})
            took = time.time() - t0
            assert r.status_code == 502, (r.status_code, r.text[:300])
            assert took >= 11, took
            assert app.store.get_account(aid)["note"], app.store.get_account(aid)
            print(f"PASS: stalled reply with Stop visible -> 502 after {took:.1f}s, not a truncated 200")
        finally:
            hm.close_app(app)


if __name__ == "__main__":
    hm.run_main(test_fake_muse_contract, test_load_app_isolation, test_browser_smoke,
                test_browser_prompt_words_and_stalls)
    print("PASS: harness_muse smoke")
