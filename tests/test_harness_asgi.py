"""进程内 ASGI 夹具冒烟测试：python3 tests/test_harness_asgi.py

全程不启动真实浏览器、不触达真实 muse.ai：engine 的浏览器方法由 fake_engine
换成轻量假实现，chat_stream 由 SlowChat 慢速吐分片，整个脚本几秒内跑完。

第二个用例用断连客户端观测 C1（流式断连不释放资源）。任务 2.1 已修复 C1，这里断言
客户端断开后数秒内 GEN_LOCK 槽位与账号占用归零、页面被关闭、worker 线程不卡在写队列；
修复前的基线（断开 8s 后仍占着槽位与账号、worker 停在 queue.put）见
tests/test_g1_dispatch.py 的 C1-EXP。

缺少 uvicorn 时打印 SKIP 并以 0 退出。
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness_muse as hm  # noqa: E402

CHAT_BODY = {"stream": True, "model": "muse-spark",
             "messages": [{"role": "user", "content": "please answer slowly"}]}


def test_live_server_start_stop():
    """LiveServer 能起停：/healthz 返回 200，stop() 后端口不再监听。"""
    app = hm.load_app({"MUSE2API_CONCURRENCY": "1"}, "asgi-health")
    try:
        server = hm.serve_app(app)
        try:
            assert server.port > 0 and server.base_url.endswith(str(server.port)), server.base_url
            status, body = server.request("GET", "/healthz")
            assert status == 200, (status, body[:200])
            assert json.loads(body)["status"] == "ok", body
            # lifespan=off：没有触发 startup，浏览器没被预热
            assert app.engine.browser is None and app.engine.proc is None
            # 未鉴权的 /v1 请求照常被 app 自己的鉴权拦下（证明跑的是真实 app）
            assert server.request("POST", "/v1/chat/completions", body=CHAT_BODY)[0] == 401
        finally:
            server.stop()
        assert not server.is_listening(), f"stop() 后 {server.port} 仍在监听"
        print(f"PASS: LiveServer start/stop (port={server.port}, /healthz 200, 停止后端口已释放)")
    finally:
        hm.close_app(app)


def test_stream_disconnect_releases():
    """StreamClient 读到首个分片后 abort()，再用 PoolProbe 观测资源在数秒内释放。"""
    app = hm.load_app({"MUSE2API_CONCURRENCY": "1", "MUSE2API_CHAT_TIMEOUT": "60",
                       "MUSE2API_STEALTH": "1"}, "asgi-c1")
    try:
        chat = hm.SlowChat(chunk="S", interval=0.01, count=5000)
        calls = hm.fake_engine(app, chat_stream=chat)
        aid = app.store.add_account({"hatch_sess": "good-A", "hatch_vml": "V0"}, "A",
                                    cookies_exp={"hatch_vml": int(time.time()) + 172800})["id"]
        probe = hm.PoolProbe(app)
        with hm.serve_app(app) as server:
            with hm.StreamClient(server) as cli:
                cli.post_stream("/v1/chat/completions", CHAT_BODY)
                cli.read_until(b'"content": "S"', timeout=20)
                assert cli.status() == 200, (cli.status(), cli.buf[:200])
                assert cli.sse_data(), cli.buf[-200:]
                print(f"PASS: StreamClient 首个内容分片到达耗时 {cli.first_chunk_seconds:.2f}s "
                      f"(响应头 {cli.first_byte_seconds:.2f}s, 已读 {len(cli.buf)} 字节)")

                before = probe.snapshot()
                assert before["gen_lock_active"] == 1, before
                assert before["busy_accounts"] == {aid: 1}, before
                assert calls["start"] == 1 and chat.calls[0]["account_id"] == aid, (calls, chat.calls)
                assert before["contexts"] is None, before  # 没开真实浏览器：观测不到上下文而非抛异常
                running = probe.worker_stacks(depth=6)
                assert running, "没抓到 app.py 里名为 worker 的线程栈"
                print(f"PASS: 断开前 {probe.describe()}")
                print("PASS: worker_stacks() 抓到 " + str(len(running)) + " 个 worker 线程栈")

                at = cli.abort()
            print(f"INFO: 客户端已在 {at:.2f}s 处直接关闭 socket（模拟中途断开）")

            released = hm.wait_released(app, timeout=8)
            after = probe.snapshot()
            deadline = time.monotonic() + 3  # 释放之后 worker 线程随即退出；等它退干净再看栈
            stacks = probe.worker_stacks(depth=6)
            while stacks and time.monotonic() < deadline:
                time.sleep(0.1)
                stacks = probe.worker_stacks(depth=6)
            print("OBSERVED: 断开后 " + probe.describe())
            for st in stacks:
                print(f"OBSERVED: worker 线程 {st['thread']} 栈 -> " + " <- ".join(reversed(st["frames"])))
            print(f"OBSERVED: SlowChat 已吐 {chat.yielded} 个分片, stop_seen={chat.stop_seen}, "
                  f"finished={chat.finished}, close_current_page 调用 {calls['close_current_page']} 次")

            # —— C1 已修复（任务 2.1）：数秒内释放，worker 不卡在写队列 ——
            assert released is not None and released <= 5, \
                f"断开 {8 if released is None else released}s 后槽位/账号仍未释放：{probe.describe()}"
            assert after["gen_lock_active"] == 0, after
            assert after["busy_accounts"] == {}, after
            assert calls["close_current_page"] >= 1, calls
            put_frames = [f for st in stacks for f in st["frames"] if f.endswith(" put")]
            assert not put_frames, stacks
            print(f"PASS: 客户端断开后 {released:.2f}s 内释放 —— "
                  f"gen_lock_active={after['gen_lock_active']}、busy_accounts={after['busy_accounts']}、"
                  f"contexts={after['contexts']}，close_current_page 调用 {calls['close_current_page']} 次，"
                  f"worker 未卡在写队列（{'已退出' if not stacks else f'{len(stacks)} 个仍在运行'}）")
    finally:
        hm.close_app(app)


if __name__ == "__main__":
    hm.run_main(test_live_server_start_stop, test_stream_disconnect_releases)
    print("PASS: harness_asgi smoke")
