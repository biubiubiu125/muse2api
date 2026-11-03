"""G1 对话调度与流式断连用例：python3 tests/test_g1_dispatch.py

全部离线。C1-EXP 假引擎版、C1-SLOW、C1-WD、PRES-STREAM 用 fake_engine 换掉浏览器方法，不启动
Chromium；C1-EXP 真实链路版用 FakeMuse 仿真站 + Chromium 包装脚本 + 真实 engine，只访问本地
自签仿真站，不触达真实 muse.ai、不消耗额度。

- C1-EXP（任务 2.1）：MUSE2API_CONCURRENCY=1，对 /v1/chat/completions 与 /v1/responses 各发一次
  流式请求，读到首个内容分片后直接关 socket。断言数秒内 GEN_LOCK 槽位归零、账号不再 busy、
  worker 不停在 queue.put、页面已关闭、假生成器已收尾、账号状态没被改写；紧接着的下一个请求
  能拿到槽位正常完成。真实链路版另外检查 Target.getBrowserContexts 没有残留上下文。
- C1-SLOW（3.11）：客户端每读一次停 1–3s，仍拿到完整内容与 [DONE]。看门狗阈值临时调到 5s，
  并让首个分片是一个要节流输出约 7s 的大块：这段时间外层一直在取数、内层不取队列，
  不能被误判为下游停读。
- C1-WD：看门狗单测。直接调用 safe_chat_stream 取一个分片后不再取（连接没断、下游停读），
  阈值调到 2s，断言 worker 在阈值之后停止，释放槽位与账号，不写账号状态。
- PRES-STREAM（3.1、3.61、3.12）：完整读取时 chat 与 responses 的流式事件序列、非流式结果与现状
  一致；生成出错时照常释放槽位与账号。
- H1-EXP（任务 2.2）：MUSE2API_CONCURRENCY=2，后台 _run_generation 指定占住账号 A（假 generate
  占约 6s），A 设为 LRU 首选、B 空闲，对 /v1/chat/completions 与 /v1/responses 各发一次非流式对话。
  断言对话由 B 完成、耗时 < 1.5s、完成时 A 仍被生成任务占着，且对话进行中没有线程停在
  AccountPoolManager.acquire 的 cond.wait 里等账号（即没有"持有 GEN_LOCK 槽位等 A"）。
- PRES-ROTATE（3.13、3.14、3.7；期望值取自修复前代码的实测）：
  (a) 全部账号空闲：无偏好时按 LRU 轮流 A→B→A→B；入口偏好 A 时连续复用 15 次后轮转到 B 一次；
      每次对话只给实际执行的账号 use_count +1、更新 last_used。
  (b) MUSE2API_CONCURRENCY=3，A、B 都被生成任务占着：对话排队，账号释放后 1s 内完成；
      请求 timeout=2s 时返回 502"等待空闲账号超时，当前所有账号均处于繁忙中"。
  (c) A 在首字前返回 401：快速切到 B 完成（流式 chat 与非流式 responses 各一次），A 记为 ok=False
      并保留 401 原因。
- H2-EXP（任务 2.3，1.3 → 2.3）：A 是 LRU 首选、首字前返回 401，worker 切到 B 完成。chat 与 responses 的
  流式、非流式各一次，结束后 A 仍为 ok=False 且备注是 401 原因，B 为 ok=True、备注清空。
- H2-ERR（任务 2.3，1.4 → 2.4）：账号状态只写本次尝试实际租到的账号。
  (a) 流式吐出分片后认证失效：不切号，该账号 ok=False 并记原因，另一个账号不动，SSE 以错误事件结束；
  (b) 流式吐出分片后生成失败：该账号只记"任务异常: …"备注与 checked_at，ok 保持原值（True、None 各一次）；
  (c) 首字前 A 生成失败、切到 B 完成：A 只记备注、ok 不变，B 记为 ok=True；
  (d) A 首字前 401 后，第二次尝试没租到账号（账号全被禁用 → 没有其它可切换的账号，不重试、返回第一次的
      401 错误；或只剩被占着的 B → 等号超时）：错误不写到 A 上，A 的备注仍是 401 原因。
- PRES-H2（3.15）：流式对话由首选账号直接成功完成，该账号记 ok=True、备注清空，另一个账号不动。
  3.16（实际执行账号认证失效时记异常与原因）由 H2-EXP 与 H2-ERR(a) 覆盖。
- L6-RETRY（任务 2.4，1.53 → 2.53）：首字前失败后没有其它可切换的健康账号就不重试，直接返回第一次的错误。
  (a) 只有一个账号 A：MuseGenerationError 与 MuseAuthError 各在 chat / responses 的流式、非流式下跑一次；
  (b) 另一个账号 B 存在但 ok=False；(a)(b) 断言 chat_stream 只调用 1 次、返回第一次的错误、总耗时 < 1.8s
  （假 chat_stream 抛错前停 1s）；(c) 另一个健康账号 B 正被占着、A 是唯一空闲的账号：第二次尝试不再选回 A，
  排队等 B 释放后由 B 完成。
- L6-FAILED（任务 2.4，1.54 → 2.54、3.61）：/v1/responses 流式吐出分片后分别以 MuseAuthError、
  MuseGenerationError、RuntimeError 失败：response.failed 的 response.error 为 {code, message}，code 依次为
  upstream_auth_error、upstream_error、server_error；事件序列与 failed 的 response 其它字段不变。
- PRES-SWITCH（3.60、3.7、3.12）：(a) A、B 都健康时 A 首字前失败（两类错误各一次）照常切到 B 完成；
  (b) B 正被占着时（A 抛错前把自己禁用），第二次尝试排队等 B 释放后由 B 完成。

缺少 uvicorn、openssl 或 Chromium 时打印 SKIP 并以 0 退出；需要 Chromium 的真实链路用例排在最后。
"""
import json
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness_muse as hm  # noqa: E402

CHAT = "/v1/chat/completions"
RESPONSES = "/v1/responses"
RELEASE_WITHIN = 5.0  # 断连后槽位与账号必须在这么多秒内释放


# ------------------------- 小工具 -------------------------
class ScriptedChat:
    """按脚本吐分片的假 engine.chat_stream：依次吐出 chunks，相邻分片间隔 interval 秒；
    raise_exc 非空时吐完后抛出它。观测属性与 hm.SlowChat 相同。"""

    def __init__(self, chunks, interval: float = 0.0, raise_exc: Exception | None = None):
        self.chunks = list(chunks)
        self.interval = float(interval)
        self.raise_exc = raise_exc
        self.calls: list[dict] = []
        self.yielded = 0
        self.stop_seen = False
        self.finished = False

    def __call__(self, cookies, prompt, expires=None, timeout=None, account_id=None, stop_event=None):
        self.calls.append({"account_id": account_id, "prompt": prompt})
        try:
            for chunk in self.chunks:
                if stop_event is not None and stop_event.is_set():
                    self.stop_seen = True
                    return
                yield chunk
                self.yielded += 1
                if self.interval:
                    time.sleep(self.interval)
            if self.raise_exc is not None:
                raise self.raise_exc
        finally:
            self.finished = True


def _add_account(app, sess: str = "good-A", label: str = "A") -> str:
    return app.store.add_account({"hatch_sess": sess, "hatch_vml": "V0"}, label,
                                 cookies_exp={"hatch_vml": int(time.time()) + 172800})["id"]


def _acc_state(app, aid: str) -> dict:
    """store.mark 会写的三个字段：断连中止不能动它们。"""
    acc = app.store.get_account(aid) or {}
    return {k: acc.get(k) for k in ("ok", "note", "checked_at")}


def _body(path: str, prompt: str, *, stream: bool, timeout: int | None = None, **extra) -> dict:
    if path == CHAT:
        body = {"model": "muse-spark", "messages": [{"role": "user", "content": prompt}]}
    else:
        body = {"model": "muse-spark", "input": prompt}
    body["stream"] = stream
    if timeout:
        body["timeout"] = timeout
    body.update(extra)
    return body


def _reply_text(path: str, obj: dict) -> str:
    if path == CHAT:
        return obj["choices"][0]["message"]["content"]
    return obj["output"][0]["content"][0]["text"]


def _settled_stacks(app, timeout: float = 3.0) -> list[dict]:
    """等 app.py 里的 worker 线程退出（最多 timeout 秒），返回仍在运行的 worker 栈。"""
    deadline = time.monotonic() + timeout
    while True:
        stacks = hm.worker_stacks(app, depth=6)
        if not stacks or time.monotonic() >= deadline:
            return stacks
        time.sleep(0.1)


def _put_frames(stacks: list[dict]) -> list[str]:
    return [f for st in stacks for f in st["frames"] if f.endswith(" put")]


def _show_stacks(stacks: list[dict]) -> None:
    if not stacks:
        print("OBSERVED:   worker 线程已退出")
    for st in stacks:
        print(f"OBSERVED:   worker 线程 {st['thread']} 栈 -> " + " <- ".join(reversed(st["frames"])))


def _released_text(released) -> str:
    return f"{released:.2f}s 内释放" if released is not None else f"{RELEASE_WITHIN:g}s 内未释放"


def _sse_events(body: bytes) -> list[tuple[str | None, str]]:
    """SSE 响应体 → [(event 名或 None, data 文本)]。"""
    events, name = [], None
    for line in body.decode("utf-8").splitlines():
        if line.startswith("event: "):
            name = line[len("event: "):]
        elif line.startswith("data: "):
            events.append((name, line[len("data: "):]))
            name = None
    return events


def _check_disconnect_released(app, path, aid, state0, released, stacks, calls=None, chat=None):
    """C1-EXP 断连后的共同断言。"""
    after = hm.snapshot(app)
    assert released is not None, (f"{path}: 断开 {RELEASE_WITHIN:g}s 后槽位/账号仍未释放："
                                  f"{hm.PoolProbe(app).describe()}")
    assert after["gen_lock_active"] == 0 and after["busy_accounts"] == {}, (path, after)
    assert not _put_frames(stacks), (path, stacks)
    if calls is not None:
        assert calls["close_current_page"] >= 1, (path, calls)
    if chat is not None:
        assert chat.stop_seen or chat.finished, (path, vars(chat))
    assert _acc_state(app, aid) == state0, ("断连中止不能写账号状态", path, state0, _acc_state(app, aid))
    return after


# ------------------------- C1-EXP（假引擎） -------------------------
def test_c1_exp_disconnect_releases():
    """两个流式端点读到首个分片后断开：数秒内释放槽位与账号，下一个请求拿到槽位正常完成。"""
    app = hm.load_app({"MUSE2API_CONCURRENCY": "1", "MUSE2API_CHAT_TIMEOUT": "60",
                       "MUSE2API_STEALTH": "1"}, "g1-c1-exp")
    try:
        aid = _add_account(app)
        headers = hm.auth_headers(app)
        with hm.serve_app(app) as server:
            for path, marker in ((CHAT, b'"content": "S"'), (RESPONSES, b'"delta": "S"')):
                chat = hm.SlowChat(chunk="S", interval=0.01, count=5000)
                calls = hm.fake_engine(app, chat_stream=chat)
                state0 = _acc_state(app, aid)
                with hm.StreamClient(server) as cli:
                    cli.post_stream(path, _body(path, "please answer slowly", stream=True))
                    cli.read_until(marker, timeout=20)
                    assert cli.status() == 200, (path, cli.buf[:200])
                    before = hm.snapshot(app)
                    assert before["gen_lock_active"] == 1 and before["busy_accounts"] == {aid: 1}, (path, before)
                    at = cli.abort()
                released = hm.wait_released(app, timeout=RELEASE_WITHIN)
                stacks = _settled_stacks(app)
                snap = hm.snapshot(app)
                print(f"OBSERVED: C1-EXP {path} 首个分片 {cli.first_chunk_seconds:.2f}s，{at:.2f}s 断开；"
                      f"{_released_text(released)}；gen_lock_active={snap['gen_lock_active']} "
                      f"busy_accounts={snap['busy_accounts']}；SlowChat 吐 {chat.yielded} 片 "
                      f"stop_seen={chat.stop_seen} finished={chat.finished}；"
                      f"close_current_page {calls['close_current_page']} 次")
                _show_stacks(stacks)
                _check_disconnect_released(app, path, aid, state0, released, stacks, calls, chat)

                # 紧接着再发一个请求：MUSE2API_CONCURRENCY=1 时只有槽位已空出才能完成
                app.engine.chat_stream = hm.SlowChat(chunk="N", interval=0.001, count=20)
                t0 = time.monotonic()
                status, raw = server.request("POST", path, body=_body(path, "next request", stream=False,
                                                                      timeout=10),
                                             headers=headers, timeout=30)
                took = time.monotonic() - t0
                assert status == 200, (path, status, raw[:300])
                assert _reply_text(path, json.loads(raw)) == "N" * 20, raw[:300]
                assert took < RELEASE_WITHIN, (path, took)
                print(f"PASS: C1-EXP {path} 断开后 {released:.2f}s 释放 GEN_LOCK 与账号，worker 未卡在写队列，"
                      f"页面已关闭，账号状态未改写；下一个请求 {took:.2f}s 正常完成")
    finally:
        hm.close_app(app)


# ------------------------- C1-SLOW -------------------------
def test_c1_slow_reader_gets_everything():
    """客户端慢速但持续读取：拿到完整内容与 [DONE]，不被当成断连或停读截断。"""
    app = hm.load_app({"MUSE2API_CONCURRENCY": "1", "MUSE2API_CHAT_TIMEOUT": "60",
                       "MUSE2API_STEALTH": "1"}, "g1-c1-slow")
    try:
        # 阈值调小，让"外层在取数、内层不取队列"一旦被误判就会在本用例里暴露：
        # 首个分片 1200 字经 _pace_text 节流输出约 7s，期间后续 200 个小分片把队列写满。
        app.STREAM_STALL_SECONDS = 5
        head, tail = "H" * 1200, ["t"] * 200
        chat = ScriptedChat([head] + tail, interval=0.01)
        calls = hm.fake_engine(app, chat_stream=chat)
        aid = _add_account(app)
        pauses = (1.0, 2.0, 3.0, 1.5, 2.5)
        with hm.serve_app(app) as server:
            with hm.StreamClient(server) as cli:
                cli.post_stream(CHAT, _body(CHAT, "slow reader", stream=True))
                deadline = time.monotonic() + 120
                reads = 0
                while b"data: [DONE]" not in cli.buf:
                    assert not cli.eof, f"连接被提前关闭：{cli.buf[-300:]!r}"
                    assert time.monotonic() < deadline, f"120s 内未读到 [DONE]：{cli.buf[-300:]!r}"
                    burst_end = time.monotonic() + 0.2  # 醒来读 0.2s，再停 1–3s
                    while time.monotonic() < burst_end and cli.read_some(65536, timeout=0.05):
                        pass
                    if b"data: [DONE]" in cli.buf:
                        break
                    time.sleep(pauses[reads % len(pauses)])
                    reads += 1
                total = cli.elapsed()
                assert cli.status() == 200, cli.buf[:200]
            data = cli.sse_data()
            assert data[-1] == "[DONE]", data[-3:]
            objs = [json.loads(x) for x in data[:-1]]
            assert not any("error" in o for o in objs), [o for o in objs if "error" in o]
            text = "".join((o["choices"][0].get("delta") or {}).get("content") or "" for o in objs if o["choices"])
            assert text == head + "".join(tail), (len(text), text[:40], text[-40:])
            assert objs[-1]["choices"][0]["finish_reason"] == "stop", objs[-1]
            assert not chat.stop_seen, vars(chat)
            released = hm.wait_released(app, timeout=RELEASE_WITHIN)
            assert released is not None, hm.PoolProbe(app).describe()
            assert calls["close_current_page"] >= 1, calls
        print(f"PASS: C1-SLOW 客户端停顿 {reads} 次（每次 1–3s），{total:.1f}s 收齐 {len(text)} 字与 [DONE]；"
              f"看门狗阈值 5s 下节流输出约 7s 的大块未被误判为停读")
    finally:
        hm.close_app(app)


# ------------------------- C1-WD -------------------------
def test_c1_wd_stall_watchdog():
    """下游停止取数但连接没断：worker 在 STREAM_STALL_SECONDS 之后停止并释放槽位与账号。"""
    app = hm.load_app({"MUSE2API_CONCURRENCY": "1", "MUSE2API_CHAT_TIMEOUT": "60",
                       "MUSE2API_STEALTH": "1"}, "g1-c1-wd")
    gen = None
    try:
        default = getattr(app, "STREAM_STALL_SECONDS", None)
        stall = 2
        app.STREAM_STALL_SECONDS = stall
        chat = hm.SlowChat(chunk="W", interval=0.005, count=20000)
        calls = hm.fake_engine(app, chat_stream=chat)
        aid = _add_account(app)
        state0 = _acc_state(app, aid)
        gen = app.safe_chat_stream("watchdog probe", 60, account_id=aid)
        assert next(gen) == "W"  # 取一个分片后不再取
        before = hm.snapshot(app)
        assert before["gen_lock_active"] == 1 and before["busy_accounts"] == {aid: 1}, before
        released = hm.wait_released(app, timeout=stall + 6)
        stacks = _settled_stacks(app)
        snap = hm.snapshot(app)
        print(f"OBSERVED: C1-WD 阈值 {stall}s，停止取数后 "
              f"{f'{released:.2f}s 释放' if released is not None else f'{stall + 6}s 内未释放'}；"
              f"gen_lock_active={snap['gen_lock_active']} busy_accounts={snap['busy_accounts']}；"
              f"SlowChat 吐 {chat.yielded} 片 stop_seen={chat.stop_seen} finished={chat.finished}；"
              f"close_current_page {calls['close_current_page']} 次")
        _show_stacks(stacks)
        assert released is not None, f"下游停读 {stall + 6}s 后仍未释放：{hm.PoolProbe(app).describe()}"
        assert released >= stall * 0.75, f"{released}s 就停了，早于阈值 {stall}s"
        assert snap["gen_lock_active"] == 0 and snap["busy_accounts"] == {}, snap
        assert not _put_frames(stacks), stacks
        assert calls["close_current_page"] >= 1, calls
        assert chat.stop_seen or chat.finished, vars(chat)
        assert _acc_state(app, aid) == state0, ("停读中止不能写账号状态", state0, _acc_state(app, aid))
        assert default == 60, f"STREAM_STALL_SECONDS 默认应为 60（模块常量），实际 {default!r}"
        print(f"PASS: C1-WD 下游停读 {released:.2f}s 后（阈值 {stall}s）worker 停止，槽位与账号已释放，"
              f"不卡写队列、不写账号状态；默认阈值 STREAM_STALL_SECONDS={default}")
    finally:
        if gen is not None:
            gen.close()
        hm.close_app(app)


# ------------------------- PRES-STREAM -------------------------
PRES_CHUNKS = ["Hello", ", ", "world", "!"]
PRES_TEXT = "".join(PRES_CHUNKS)
# _pace_text 把 >3 字的分片按 2 字切开，<=3 字的原样放行
PRES_CHAT_PIECES = ["He", "ll", "o", ", ", "wo", "rl", "d", "!"]
PRES_RESPONSES_EVENTS = (["response.created", "response.output_item.added", "response.content_part.added"]
                         + ["response.output_text.delta"] * len(PRES_CHUNKS)
                         + ["response.output_text.done", "response.output_item.done", "response.completed"])


def test_pres_stream_sequences():
    """完整读取时流式事件序列与非流式结果不变；生成出错时照常释放资源。"""
    app = hm.load_app({"MUSE2API_CONCURRENCY": "1", "MUSE2API_CHAT_TIMEOUT": "60",
                       "MUSE2API_STEALTH": "1"}, "g1-pres")
    try:
        aid = _add_account(app)
        headers = hm.auth_headers(app)
        with hm.serve_app(app) as server:
            # chat 流式：role 首包 → content 分片 → finish_reason=stop → usage → [DONE]
            calls = hm.fake_engine(app, chat_stream=ScriptedChat(PRES_CHUNKS))
            status, raw = server.request("POST", CHAT, headers=headers, timeout=30,
                                         body=_body(CHAT, "pres chat", stream=True,
                                                    stream_options={"include_usage": True}))
            assert status == 200, raw[:300]
            data = [d for _, d in _sse_events(raw)]
            assert data[-1] == "[DONE]", data[-3:]
            objs = [json.loads(x) for x in data[:-1]]
            assert len(objs) == 1 + len(PRES_CHAT_PIECES) + 2, objs
            assert len({o["id"] for o in objs}) == 1 and objs[0]["id"].startswith("chatcmpl-"), objs[0]
            assert all(o["object"] == "chat.completion.chunk" and o["model"] == "muse-spark" for o in objs)
            assert objs[0]["choices"] == [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]
            pieces = [o["choices"] for o in objs[1:-2]]
            assert pieces == [[{"index": 0, "delta": {"content": p}, "finish_reason": None}]
                              for p in PRES_CHAT_PIECES], pieces
            assert objs[-2]["choices"] == [{"index": 0, "delta": {}, "finish_reason": "stop"}], objs[-2]
            usage = objs[-1]["usage"]
            assert objs[-1]["choices"] == [] and usage["completion_tokens"] == len(PRES_TEXT) // 4, objs[-1]
            assert usage["total_tokens"] == usage["prompt_tokens"] + usage["completion_tokens"], usage
            assert hm.wait_released(app, timeout=RELEASE_WITHIN) is not None
            print(f"PASS: PRES-STREAM chat 流式 role → {len(PRES_CHAT_PIECES)} 个 content 分片 → "
                  f"finish_reason=stop → usage → [DONE]")

            # responses 流式：created → output_item.added → content_part.added → delta… → done… → completed
            hm.fake_engine(app, chat_stream=ScriptedChat(PRES_CHUNKS))
            status, raw = server.request("POST", RESPONSES, headers=headers, timeout=30,
                                         body=_body(RESPONSES, "pres responses", stream=True))
            assert status == 200, raw[:300]
            events = [(name, json.loads(d)) for name, d in _sse_events(raw)]
            assert [name for name, _ in events] == PRES_RESPONSES_EVENTS, [name for name, _ in events]
            assert all(obj["type"] == name for name, obj in events), events
            by = {}
            for name, obj in events:
                by.setdefault(name, []).append(obj)
            assert by["response.created"][0]["response"]["status"] == "in_progress"
            assert [d["delta"] for d in by["response.output_text.delta"]] == PRES_CHUNKS
            assert by["response.output_text.done"][0]["text"] == PRES_TEXT
            item = by["response.output_item.done"][0]["item"]
            assert item["status"] == "completed" and item["content"][0]["text"] == PRES_TEXT, item
            final = by["response.completed"][0]["response"]
            assert final["status"] == "completed" and final["output"][0]["content"][0]["text"] == PRES_TEXT, final
            assert final["id"] == by["response.created"][0]["response"]["id"], final["id"]
            assert hm.wait_released(app, timeout=RELEASE_WITHIN) is not None
            print("PASS: PRES-STREAM responses 流式 created → output_item.added → content_part.added → "
                  f"{len(PRES_CHUNKS)} 个 delta → output_text.done → output_item.done → completed")

            # 非流式：chat.completion 与 response 结构不变
            for path in (CHAT, RESPONSES):
                hm.fake_engine(app, chat_stream=ScriptedChat(PRES_CHUNKS))
                status, raw = server.request("POST", path, headers=headers, timeout=30,
                                             body=_body(path, "pres plain", stream=False))
                assert status == 200, (path, raw[:300])
                obj = json.loads(raw)
                if path == CHAT:
                    assert obj["object"] == "chat.completion" and obj["id"].startswith("chatcmpl-"), obj
                    assert obj["choices"] == [{"index": 0, "finish_reason": "stop",
                                               "message": {"role": "assistant", "content": PRES_TEXT}}], obj
                    assert set(obj["usage"]) == {"prompt_tokens", "completion_tokens", "total_tokens"}, obj
                else:
                    assert obj["object"] == "response" and obj["status"] == "completed", obj
                    assert obj["output"][0]["content"] == [{"type": "output_text", "text": PRES_TEXT,
                                                            "annotations": []}], obj
                assert hm.wait_released(app, timeout=RELEASE_WITHIN) is not None
            print("PASS: PRES-STREAM 非流式 chat.completion 与 response 结果不变")

            # 3.12：生成出错时照常释放槽位与账号，并以错误事件结束
            failing = ScriptedChat([], raise_exc=app.MuseGenerationError("pres boom"))
            calls = hm.fake_engine(app, chat_stream=failing)
            status, raw = server.request("POST", CHAT, headers=headers, timeout=30,
                                         body=_body(CHAT, "pres error", stream=True))
            assert status == 200, raw[:300]
            data = [d for _, d in _sse_events(raw)]
            assert json.loads(data[0])["choices"][0]["delta"] == {"role": "assistant"}, data[0]
            last = json.loads(data[-1])
            assert "pres boom" in last["error"]["message"], data[-1]
            assert hm.wait_released(app, timeout=RELEASE_WITHIN) is not None, hm.PoolProbe(app).describe()
            assert calls["close_current_page"] >= 1 and failing.calls, (calls, failing.calls)
            print(f"PASS: PRES-STREAM 出错路径以错误事件结束并释放槽位与账号"
                  f"（chat_stream 调用 {len(failing.calls)} 次，close_current_page {calls['close_current_page']} 次）")
    finally:
        hm.close_app(app)


# ------------------------- H1 / PRES-ROTATE 小工具 -------------------------
class HoldGenerate:
    """假 engine.generate：进入后一直占着（等 release 事件，最多 hold 秒），再返回一个视频结果。

    参数与 _run_generation_locked 调用 engine.generate 时一致。active 为正在生成的账号集合，
    ended[账号] 为该账号生成结束的时刻（time.monotonic()）。"""

    def __init__(self, hold: float):
        self.hold = float(hold)
        self.release = threading.Event()
        self.cond = threading.Condition()
        self.active: set = set()
        self.ended: dict = {}

    def __call__(self, cookies, prompt, expect="image", timeout=None, expires=None,
                 account_id=None, on_progress=None, reference_image=None, **kwargs):
        with self.cond:
            self.active.add(account_id)
            self.cond.notify_all()
        try:
            self.release.wait(self.hold)
            return {"path": "", "filename": "hold.mp4", "size": 1, "ext": ".mp4",
                    "mime": "video/mp4", "kind": expect}
        finally:
            with self.cond:
                self.active.discard(account_id)
                self.ended[account_id] = time.monotonic()

    def wait_active(self, aid: str, timeout: float = 5.0) -> bool:
        with self.cond:
            return self.cond.wait_for(lambda: aid in self.active, timeout)


AUTH_401 = "会话已失效 (/api/session HTTP 401)"  # AuthFailFor 抛出的认证失效原因


class FailFor:
    """假 engine.chat_stream：bad 账号在首字前抛 exc_type(message)，其它账号吐 reply。

    before_raise 非空时先调用它再抛错（例如把账号禁用，让下一次尝试的 acquire 在租号前自己出错）。"""

    def __init__(self, bad: str, reply: str, exc_type, message: str, before_raise=None):
        self.bad, self.reply = bad, reply
        self.exc_type, self.message = exc_type, message
        self.before_raise = before_raise
        self.calls: list[dict] = []

    def __call__(self, cookies, prompt, expires=None, timeout=None, account_id=None, stop_event=None):
        self.calls.append({"account_id": account_id})
        if account_id == self.bad:
            if self.before_raise is not None:
                self.before_raise()
            raise self.exc_type(self.message)
        yield self.reply


class AuthFailFor(FailFor):
    """假 engine.chat_stream：bad 账号在首字前抛 MuseAuthError（模拟 /api/session 401），其它账号吐 reply。"""

    def __init__(self, app, bad: str, reply: str, before_raise=None):
        super().__init__(bad, reply, app.MuseAuthError, AUTH_401, before_raise)


def _occupy(app, aid: str, gen: HoldGenerate) -> threading.Thread:
    """后台线程跑 app._run_generation(…, account_id=aid)：占一个 GEN_LOCK 槽位并租住账号 aid，直到 gen 放行。

    指定账号走 ACCOUNT_POOL 的指定账号分支，不经 pick_account，aid 的 last_used / use_count 不变。"""
    box: dict = {}

    def hold():
        try:
            box["result"] = app._run_generation(f"hold video on {aid}", "video", 60, account_id=aid)
        except BaseException as exc:  # noqa: BLE001 - 记下来由用例断言
            box["error"] = exc

    runner = threading.Thread(target=hold, name=f"hold-{aid}", daemon=True)
    runner.box = box
    runner.start()
    assert gen.wait_active(aid), f"生成任务 5s 内没占上账号 {aid}：{box}"
    return runner


def _release_all(holds) -> None:
    for gen, _ in holds:
        gen.release.set()
    for _, runner in holds:
        runner.join(10)


class _Request(threading.Thread):
    """后台发一个普通请求，记下状态码、响应体、耗时，以及请求刚返回时的资源快照（done_snap）。"""

    def __init__(self, app, server, path: str, body: dict, headers: dict, timeout: float = 60):
        super().__init__(name="g1-request", daemon=True)
        self.app, self.server, self.path, self.body, self.headers = app, server, path, body, headers
        self.req_timeout = float(timeout)
        self.status = self.raw = self.error = None
        self.done_at = self.took = self.done_snap = None
        self.t0 = time.monotonic()
        self.start()

    def run(self):
        try:
            self.status, self.raw = self.server.request("POST", self.path, body=self.body,
                                                        headers=self.headers, timeout=self.req_timeout)
        except Exception as exc:  # noqa: BLE001 - 记下来由用例断言
            self.error = exc
        self.done_at = time.monotonic()
        self.took = self.done_at - self.t0
        self.done_snap = hm.snapshot(self.app)


def _account_waiters(app) -> list:
    """此刻停在 AccountPoolManager.acquire 的 cond.wait 里等账号的线程：[(线程名, 指定的 account_id)]。

    按代码对象识别帧（ConcurrencyLock.acquire 等同名函数不算）；account_id 为 None 表示等任一账号。"""
    acquire = app.AccountPoolManager.acquire.__code__
    cond_wait = threading.Condition.wait.__code__
    names = {t.ident: t.name for t in threading.enumerate()}
    out = []
    for tid, frame in sys._current_frames().items():
        waiting = False
        while frame is not None:
            if frame.f_code is cond_wait:
                waiting = True
            elif frame.f_code is acquire:
                if waiting:
                    try:
                        want = frame.f_locals.get("account_id")
                    except Exception:  # noqa: BLE001
                        want = "?"
                    out.append((names.get(tid, f"tid-{tid}"), want))
                break
            frame = frame.f_back
    return out


def _sample_waits(app, req: _Request, interval: float = 0.05) -> list:
    """请求进行期间每 interval 秒采样：有线程停在 AccountPoolManager.acquire 等账号时，
    记下它等的账号、GEN_LOCK 槽位占用与账号占用。"""
    samples = []
    while req.is_alive():
        waiters = _account_waiters(app)
        if waiters:
            snap = hm.snapshot(app)
            samples.append({"t": time.monotonic(), "waiters": waiters,
                            "gen_lock_active": snap["gen_lock_active"], "busy": snap["busy_accounts"]})
        req.join(interval)
    return samples


def _named(value, names: dict):
    """把账号 id 换成 A / B 标签，便于打印与比较。"""
    if isinstance(value, dict):
        return {names.get(k, k): v for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [names.get(v, v) for v in value]
    return names.get(value, value)


def _waits_text(samples: list, names: dict) -> str:
    if not samples:
        return "对话进行中没有线程停在 AccountPoolManager.acquire 等账号"
    first, last = samples[0], samples[-1]
    wanted = sorted({"任一" if w is None else str(_named(w, names)) for s in samples for _, w in s["waiters"]})
    return (f"{len(samples)} 次采样看到对话 worker 停在 AccountPoolManager.acquire 等账号 "
            f"account_id={'/'.join(wanted)}（持续约 {last['t'] - first['t']:.1f}s），当时 "
            f"gen_lock_active={first['gen_lock_active']} busy_accounts={_named(first['busy'], names)}")


def _reply(path: str, stream: bool, raw: bytes) -> str:
    """取回复文本；流式时同时确认正常收尾、没有错误事件。"""
    if not stream:
        return _reply_text(path, json.loads(raw))
    events = _sse_events(raw)
    if path == CHAT:
        data = [d for _, d in events]
        assert data and data[-1] == "[DONE]", data[-3:]
        objs = [json.loads(x) for x in data[:-1]]
        assert not any("error" in o for o in objs), objs
        return "".join((o["choices"][0].get("delta") or {}).get("content") or ""
                       for o in objs if o.get("choices"))
    kinds = [name for name, _ in events]
    assert kinds and kinds[-1] == "response.completed", kinds
    return "".join(json.loads(d)["delta"] for name, d in events if name == "response.output_text.delta")


def _make_lru(app, first: str, second: str) -> None:
    """让 first 成为 LRU 首选（最久未用），second 较新。"""
    now = time.time()
    app.store.get_account(first)["last_used"] = now - 100
    app.store.get_account(second)["last_used"] = now - 50


H1_ENV = {"MUSE2API_CONCURRENCY": "2", "MUSE2API_PER_ACCOUNT_CONCURRENCY": "1",
          "MUSE2API_CHAT_TIMEOUT": "60", "MUSE2API_STEALTH": "1"}


# ------------------------- H1-EXP -------------------------
H1_HOLD = 6.0  # 生成任务占住 A 的时长（秒）：未修复代码上对话要等这么久
H1_FAST = 1.5  # A 忙、B 空闲时，对话必须在这么多秒内完成


def test_h1_exp_skips_busy_account():
    """A 被生成任务占着、B 空闲：两个对话端点都立即由 B 完成，不在持有 GEN_LOCK 槽位时等 A。"""
    app = hm.load_app(H1_ENV, "g1-h1-exp")
    holds = []
    try:
        a = _add_account(app, "good-A", "A")
        b = _add_account(app, "good-B", "B")
        names = {a: "A", b: "B"}
        headers = hm.auth_headers(app)
        results = []
        with hm.serve_app(app) as server:
            for path in (CHAT, RESPONSES):
                gen = HoldGenerate(H1_HOLD)
                chat = ScriptedChat(["h1 ", "reply"])
                hm.fake_engine(app, chat_stream=chat, generate=gen)
                runner = _occupy(app, a, gen)
                holds.append((gen, runner))
                t_hold = time.monotonic()
                _make_lru(app, a, b)  # 审查阶段的做法：A 最久未用，是 LRU 首选
                before = hm.snapshot(app)
                assert before["gen_lock_active"] == 1 and before["busy_accounts"] == {a: 1}, before
                req = _Request(app, server, path, _body(path, "h1 probe", stream=False, timeout=30), headers)
                left = H1_HOLD - (req.t0 - t_hold)
                samples = _sample_waits(app, req)
                a_held = ((req.done_snap or {}).get("busy_accounts") or {}).get(a) == 1
                gen.release.set()
                runner.join(10)
                # 对话返回时 A 仍在生成任务手里：生成在对话返回之后才结束
                a_held = a_held and gen.ended.get(a, float("inf")) > req.done_at
                served = _named([c["account_id"] for c in chat.calls], names)
                text = _reply_text(path, json.loads(req.raw)) if req.status == 200 else None
                print(f"OBSERVED: H1-EXP {path} 生成任务占着 A（还剩约 {left:.1f}s）、B 空闲；"
                      f"对话 status={req.status}，由 {served} 完成，耗时 {req.took:.2f}s，完成时 A "
                      f"{'仍被生成任务占着' if a_held else '已不在生成任务手里'}；{_waits_text(samples, names)}")
                results.append({"path": path, "status": req.status, "error": req.error, "raw": req.raw,
                                "text": text, "served": served, "took": req.took, "left": left,
                                "a_held": a_held, "samples": samples, "gen_box": dict(runner.box)})
        for r in results:
            path = r["path"]
            assert r["status"] == 200, (path, r["status"], r["error"], (r["raw"] or b"")[:300])
            assert r["text"] == "h1 reply", (path, r["text"])
            assert r["served"] == ["B"], (f"{path}: A 忙、B 空闲时对话应由 B 完成，实际由 {r['served']} 完成"
                                          f"（耗时 {r['took']:.2f}s）")
            assert r["took"] < H1_FAST, f"{path}: 对话耗时 {r['took']:.2f}s，应 < {H1_FAST}s"
            assert r["a_held"], f"{path}: 对话完成时 A 应仍被生成任务占着（对话不应等 A 释放）"
            assert not r["samples"], f"{path}: 不应在持有 GEN_LOCK 槽位时等账号：{_waits_text(r['samples'], names)}"
            assert "error" not in r["gen_box"], (path, r["gen_box"])
            print(f"PASS: H1-EXP {path} A 被生成任务占着（还剩约 {r['left']:.1f}s）时，对话 {r['took']:.2f}s "
                  f"由 B 完成，完成时 A 仍被占着；对话进行中没有线程持有 GEN_LOCK 槽位等账号")
    finally:
        _release_all(holds)
        hm.close_app(app)


# ------------------------- PRES-ROTATE -------------------------
# 期望值取自修复前代码的实测（见任务 2.2 的回报）。
ROTATE_LRU = ["A", "B", "A", "B"]                    # 无偏好：按 LRU 轮流
BUSY_QUEUE = 1.0      # 所有账号都忙时，先观察对话排队这么久（秒）
BUSY_TIMEOUT = 2      # 超时用例里对话请求的 timeout（秒）
BUSY_TIMEOUT_MSG = "等待空闲账号超时，当前所有账号均处于繁忙中"


def _use_state(app, ids) -> dict:
    out = {}
    for aid in ids:
        acc = app.store.get_account(aid) or {}
        out[aid] = (acc.get("use_count") or 0, acc.get("last_used") or 0.0)
    return out


def _rotate_once(app, server, headers, chat, names, path=CHAT, stream=False) -> str:
    """发一次对话，返回实际执行账号的标签；并断言只有它 use_count +1、last_used 前移。"""
    before = _use_state(app, names)
    n = len(chat.calls)
    status, raw = server.request("POST", path, headers=headers, timeout=30,
                                 body=_body(path, "rotate", stream=stream))
    assert status == 200, (path, stream, raw[:300])
    assert _reply(path, stream, raw) == "rot", (path, stream, raw[:300])
    assert len(chat.calls) == n + 1, chat.calls[n:]
    aid = chat.calls[-1]["account_id"]
    after = _use_state(app, names)
    for x in names:
        if x == aid:
            assert after[x][0] == before[x][0] + 1 and after[x][1] > before[x][1], (path, names[x], before[x], after[x])
        else:
            assert after[x] == before[x], (path, names[x], before[x], after[x])
    return names[aid]


def _prefer(app, box: dict):
    """让 engine.current_acc_id 在任何线程都读到 box["id"]，返回原 property（用完还原）。

    对话入口不再读 current_acc_id 当偏好（页面每次用完即关，没有热页可复用）：即使引擎里留着
    某个账号 ID，选号也照常按 LRU 轮转（3.13）。"""
    cls = type(app.engine)
    orig = cls.__dict__["current_acc_id"]
    cls.current_acc_id = property(lambda self: box.get("id"), orig.fset)
    return orig


def test_pres_rotate_idle():
    """(a) 全部账号空闲：偏好与轮转语义、use_count / last_used 记账与修复前一致（3.13）。"""
    app = hm.load_app(H1_ENV, "g1-pres-rotate-idle")
    orig = None
    try:
        a = _add_account(app, "good-A", "A")
        b = _add_account(app, "good-B", "B")
        names = {a: "A", b: "B"}
        headers = hm.auth_headers(app)
        chat = ScriptedChat(["rot"])
        hm.fake_engine(app, chat_stream=chat)
        with hm.serve_app(app) as server:
            lru = [_rotate_once(app, server, headers, chat, names, path, stream)
                   for path, stream in ((CHAT, False), (RESPONSES, False), (CHAT, True), (RESPONSES, True))]
            orig = _prefer(app, {"id": a})
            remembered = [_rotate_once(app, server, headers, chat, names) for _ in ROTATE_LRU]
        use = _named({k: v[0] for k, v in _use_state(app, names).items()}, names)
        print(f"OBSERVED: PRES-ROTATE(a) 4 次对话（chat/responses × 非流式/流式）依次由 "
              f"{''.join(lru)} 执行；引擎记着 A 时再 4 次依次由 {''.join(remembered)} 执行；"
              f"每次只有执行账号 use_count +1、last_used 前移；最终 use_count={use}")
        assert lru == ROTATE_LRU, lru
        assert remembered == ROTATE_LRU, remembered
        print(f"PASS: PRES-ROTATE(a) 全部空闲：按 LRU 轮流 {'→'.join(ROTATE_LRU)}，引擎记着的账号不影响选号；"
              f"每次对话只给执行账号记一次 use_count / last_used")
    finally:
        if orig is not None:
            type(app.engine).current_acc_id = orig
        hm.close_app(app)


def test_pres_rotate_all_busy():
    """(b) 所有账号都忙：对话在超时内排队、任一账号释放后立即完成；超时返回修复前的错误信息（3.14）。

    MUSE2API_CONCURRENCY=3：两个生成任务各占一个槽位与账号后还剩一个槽位，对话等的是账号。"""
    app = hm.load_app({**H1_ENV, "MUSE2API_CONCURRENCY": "3"}, "g1-pres-rotate-busy")
    holds = []
    try:
        a = _add_account(app, "good-A", "A")
        b = _add_account(app, "good-B", "B")
        names = {a: "A", b: "B"}
        headers = hm.auth_headers(app)
        with hm.serve_app(app) as server:
            # 排队：A、B 都被占着，对话等账号；同时放行两个生成任务后应立即完成
            gen = HoldGenerate(30)
            chat = ScriptedChat(["queued ", "reply"])
            hm.fake_engine(app, chat_stream=chat, generate=gen)
            holds += [(gen, _occupy(app, aid, gen)) for aid in (a, b)]
            req = _Request(app, server, CHAT, _body(CHAT, "queued probe", stream=False, timeout=20), headers)
            req.join(BUSY_QUEUE)
            pending = req.is_alive()
            waiters = _account_waiters(app)
            queued = hm.snapshot(app)
            t_release = time.monotonic()
            gen.release.set()
            req.join(15)
            for _, runner in holds:
                runner.join(10)
            after_release = None if req.done_at is None else req.done_at - t_release
            served = _named([c["account_id"] for c in chat.calls], names)
            want = sorted({"任一" if w is None else str(_named(w, names)) for _, w in waiters})
            print(f"OBSERVED: PRES-ROTATE(b) A、B 都被生成任务占着：对话 {BUSY_QUEUE:g}s 后仍在排队={pending}"
                  f"（停在 AccountPoolManager.acquire 等账号 account_id={'/'.join(want) or '-'}，"
                  f"gen_lock_active={queued['gen_lock_active']} busy_accounts={_named(queued['busy_accounts'], names)}）；"
                  f"放行生成任务后 {after_release if after_release is None else round(after_release, 2)}s "
                  f"返回 status={req.status}，由 {served} 执行")
            assert pending, f"所有账号都忙时对话应排队等待，实际 {req.took}s 就返回了 status={req.status}"
            assert req.status == 200, (req.status, req.error, (req.raw or b"")[:300])
            assert _reply_text(CHAT, json.loads(req.raw)) == "queued reply", req.raw[:300]
            assert after_release < 1.0, f"账号释放 {after_release:.2f}s 后对话才完成"

            # 超时：A、B 一直被占着，请求 timeout=BUSY_TIMEOUT
            gen2 = HoldGenerate(30)
            chat2 = ScriptedChat(["never"])
            hm.fake_engine(app, chat_stream=chat2, generate=gen2)
            holds += [(gen2, _occupy(app, aid, gen2)) for aid in (a, b)]
            req2 = _Request(app, server, RESPONSES,
                            _body(RESPONSES, "timeout probe", stream=False, timeout=BUSY_TIMEOUT), headers)
            req2.join(BUSY_TIMEOUT * 3 + 10)
            gen2.release.set()
            assert not req2.is_alive(), f"timeout={BUSY_TIMEOUT}s 的请求 {BUSY_TIMEOUT * 3 + 10}s 后仍未返回"
            err = (json.loads(req2.raw).get("error") or {}) if req2.raw else {}
            print(f"OBSERVED: PRES-ROTATE(b) 同样全忙、请求 timeout={BUSY_TIMEOUT}s：{req2.took:.2f}s 后返回 "
                  f"status={req2.status} error={err}；chat_stream 被调用 {len(chat2.calls)} 次")
            assert req2.status == 502, (req2.status, req2.error, (req2.raw or b"")[:300])
            assert err.get("message") == BUSY_TIMEOUT_MSG, err
            assert BUSY_TIMEOUT <= req2.took < BUSY_TIMEOUT * 2 + 3, req2.took
            assert not chat2.calls, chat2.calls
            print(f"PASS: PRES-ROTATE(b) 全部账号都忙：对话排队，账号释放后 {after_release:.2f}s 完成；"
                  f"timeout={BUSY_TIMEOUT}s 时 {req2.took:.1f}s 后返回 502「{BUSY_TIMEOUT_MSG}」")
    finally:
        _release_all(holds)
        hm.close_app(app)


def test_pres_rotate_auth_switch():
    """(c) 首字前入口账号 401：照常快速切到另一个健康账号完成请求（3.7）；A 记为异常并保留原因（3.16）。"""
    app = hm.load_app(H1_ENV, "g1-pres-rotate-401")
    try:
        a = _add_account(app, "good-A", "A")
        b = _add_account(app, "good-B", "B")
        names = {a: "A", b: "B"}
        headers = hm.auth_headers(app)
        chat = AuthFailFor(app, a, "served-by-B")
        hm.fake_engine(app, chat_stream=chat)
        with hm.serve_app(app) as server:
            for path, stream in ((CHAT, True), (RESPONSES, False)):
                for aid in (a, b):
                    app.store.mark(aid, True, "")
                _make_lru(app, a, b)  # A 健康且是 LRU 首选：首次尝试落在 A
                n = len(chat.calls)
                t0 = time.monotonic()
                status, raw = server.request("POST", path, headers=headers, timeout=30,
                                             body=_body(path, "auth switch", stream=stream))
                took = time.monotonic() - t0
                served = _named([c["account_id"] for c in chat.calls[n:]], names)
                acc_a, acc_b = app.store.get_account(a), app.store.get_account(b)
                print(f"OBSERVED: PRES-ROTATE(c) {path} stream={stream} 首字前 A 返回 401：{took:.2f}s 返回 "
                      f"status={status}，chat_stream 依次用 {served}；A ok={acc_a['ok']} note={acc_a['note']!r}，"
                      f"B ok={acc_b['ok']}")
                assert status == 200, raw[:300]
                assert _reply(path, stream, raw) == "served-by-B", raw[:300]
                assert served == ["A", "B"], served
                assert took < 3.0, took
                assert acc_b["ok"] is True, acc_b
                # 流式与非流式一样：切号成功后 A 不被改回 ok=True，401 原因保留（H2，任务 2.3）
                assert acc_a["ok"] is False and "401" in acc_a["note"], (path, stream, acc_a)
                print(f"PASS: PRES-ROTATE(c) {path} stream={stream} 首字前 A 401，{took:.2f}s 内切到 B 完成，"
                      f"A 保持 ok=False 与 401 原因")
    finally:
        hm.close_app(app)


# ------------------------- H2 小工具 -------------------------
H2_COMBOS = ((CHAT, True), (CHAT, False), (RESPONSES, True), (RESPONSES, False))
H2_STALE = "待检测"                 # 另一个账号的旧备注：被误写、被成功清空都看得出来
H2_GEN_ERR = "助手回复超时（模拟）"  # 假 chat_stream 抛出的偶发 MuseGenerationError
H2_DENIED = "没有可用账号"           # ACCOUNT_POOL.acquire 找不到启用账号时抛出的 MuseAuthError
H2_BUSY_TIMEOUT = 2                 # H2-ERR(d2) 的请求 timeout：第二次尝试等被占着的 B 这么久后超时


def _set_state(app, aid: str, ok, note: str = "") -> None:
    """直接设定账号的 ok 与备注（含 ok=None 的未检测状态），并清空 checked_at。"""
    app.store.get_account(aid).update(ok=ok, note=note, checked_at=None)


def _state_text(state: dict) -> str:
    return f"ok={state['ok']} note={state['note']!r}"


def _chat_stream_outcome(raw: bytes) -> dict:
    """chat 流式响应 → 已输出的文本、最后一个事件里的 error（没有则 None）、是否以 [DONE] 收尾。"""
    data = [d for _, d in _sse_events(raw)]
    objs = [json.loads(x) for x in data if x != "[DONE]"]
    text = "".join((o["choices"][0].get("delta") or {}).get("content") or ""
                   for o in objs if o.get("choices"))
    return {"text": text, "error": objs[-1].get("error") if objs else None,
            "done": bool(data) and data[-1] == "[DONE]"}


def _safe_reply(path: str, stream: bool, status: int, raw: bytes):
    """_reply 的观测版：不完整时返回说明文字而不是抛错，便于先打印再断言。"""
    if status != 200:
        return None
    try:
        return _reply(path, stream, raw)
    except AssertionError as exc:
        return f"<回复不完整: {exc}>"


# ------------------------- H2-EXP -------------------------
def test_h2_exp_switched_entry_keeps_error():
    """H2-EXP（1.3 → 2.3）：首选账号 A 首字前 401、worker 切到 B 完成后，A 仍为异常并保留 401 原因，
    B 记为 ok=True、备注清空。chat 与 responses 的流式、非流式各跑一次。"""
    app = hm.load_app(H1_ENV, "g1-h2-exp")
    try:
        a = _add_account(app, "good-A", "A")
        b = _add_account(app, "good-B", "B")
        names = {a: "A", b: "B"}
        headers = hm.auth_headers(app)
        chat = AuthFailFor(app, a, "served-by-B")
        hm.fake_engine(app, chat_stream=chat)
        results = []
        with hm.serve_app(app) as server:
            for path, stream in H2_COMBOS:
                _set_state(app, a, True, "")        # A 健康且是 LRU 首选：首次尝试落在 A
                _set_state(app, b, None, H2_STALE)  # B 的 ok=True 与空备注只能由这次成功写入
                _make_lru(app, a, b)
                n = len(chat.calls)
                status, raw = server.request("POST", path, headers=headers, timeout=30,
                                             body=_body(path, "h2 probe", stream=stream))
                served = _named([c["account_id"] for c in chat.calls[n:]], names)
                text = _safe_reply(path, stream, status, raw)
                acc_a, acc_b = _acc_state(app, a), _acc_state(app, b)
                print(f"OBSERVED: H2-EXP {path} stream={stream} 首字前 A 返回 401：status={status}，"
                      f"chat_stream 依次用 {served}，回复 {text!r}；结束后 A {_state_text(acc_a)}，"
                      f"B {_state_text(acc_b)}")
                results.append({"where": f"{path} stream={stream}", "status": status, "raw": raw,
                                "text": text, "served": served, "a": acc_a, "b": acc_b})
        for r in results:
            where = r["where"]
            assert r["status"] == 200 and r["text"] == "served-by-B", (where, r["status"], r["raw"][:300])
            assert r["served"] == ["A", "B"], (where, r["served"])
            assert r["a"]["ok"] is False and AUTH_401 in r["a"]["note"], (
                f"{where}: 切到 B 成功后 A 应保持 ok=False 与 401 原因，实际 {_state_text(r['a'])}")
            assert r["b"]["ok"] is True and r["b"]["note"] == "", (where, r["b"])
            print(f"PASS: H2-EXP {where} A 401 后由 B 完成：A 保持 ok=False、备注 {r['a']['note']!r}；"
                  f"B ok=True、备注清空")
    finally:
        hm.close_app(app)


# ------------------------- H2-ERR -------------------------
def test_h2_err_auth_after_output():
    """H2-ERR(a)（1.4 → 2.4、3.16）：流式吐出分片后实际执行的账号认证失效：已有输出不切号重试，
    只把该账号记为 ok=False 并写明原因，另一个账号不动，SSE 以错误事件结束。"""
    app = hm.load_app(H1_ENV, "g1-h2-err-auth")
    try:
        a = _add_account(app, "good-A", "A")
        b = _add_account(app, "good-B", "B")
        names = {a: "A", b: "B"}
        headers = hm.auth_headers(app)
        chat = ScriptedChat(["partial ", "reply"], raise_exc=app.MuseAuthError(AUTH_401))
        hm.fake_engine(app, chat_stream=chat)
        _set_state(app, a, True, "")
        _set_state(app, b, None, H2_STALE)
        _make_lru(app, a, b)
        state_b = _acc_state(app, b)
        with hm.serve_app(app) as server:
            status, raw = server.request("POST", CHAT, headers=headers, timeout=30,
                                         body=_body(CHAT, "h2 auth after output", stream=True))
        out = _chat_stream_outcome(raw) if status == 200 else {}
        served = _named([c["account_id"] for c in chat.calls], names)
        acc_a, acc_b = _acc_state(app, a), _acc_state(app, b)
        print(f"OBSERVED: H2-ERR(a) chat 流式吐出 {out.get('text')!r} 后 A 认证失效：status={status}，"
              f"chat_stream 依次用 {served}，结尾 error={out.get('error')} [DONE]={out.get('done')}；"
              f"A {_state_text(acc_a)}，B {_state_text(acc_b)}")
        assert status == 200 and out["text"] == "partial reply", (status, raw[:300])
        assert out["error"] and AUTH_401 in out["error"]["message"] and not out["done"], out
        assert served == ["A"], served
        assert acc_a["ok"] is False and AUTH_401 in acc_a["note"], acc_a
        assert acc_b == state_b, ("另一个账号不能被改写", state_b, acc_b)
        print(f"PASS: H2-ERR(a) 吐出分片后认证失效：不切号，A 记为 ok=False、备注 {acc_a['note']!r}，B 不变，"
              f"SSE 以错误事件结束")
    finally:
        hm.close_app(app)


def test_h2_err_generation_after_output():
    """H2-ERR(b)（1.4 → 2.4、2.20、3.35）：流式吐出分片后实际执行的账号生成失败（偶发）：只给该账号记
    "任务异常: …"备注与 checked_at，ok 保持原值（原值 True、None 各一次），不切号，另一个账号不动。"""
    app = hm.load_app(H1_ENV, "g1-h2-err-gen")
    try:
        a = _add_account(app, "good-A", "A")
        b = _add_account(app, "good-B", "B")
        names = {a: "A", b: "B"}
        headers = hm.auth_headers(app)
        results = []
        with hm.serve_app(app) as server:
            for ok0 in (True, None):
                chat = ScriptedChat(["partial ", "reply"], raise_exc=app.MuseGenerationError(H2_GEN_ERR))
                hm.fake_engine(app, chat_stream=chat)
                _set_state(app, a, ok0, "")
                _set_state(app, b, None, H2_STALE)
                _make_lru(app, a, b)
                state_b = _acc_state(app, b)
                t0 = int(time.time())
                status, raw = server.request("POST", CHAT, headers=headers, timeout=30,
                                             body=_body(CHAT, "h2 generation after output", stream=True))
                out = _chat_stream_outcome(raw) if status == 200 else {}
                served = _named([c["account_id"] for c in chat.calls], names)
                acc_a, acc_b = _acc_state(app, a), _acc_state(app, b)
                print(f"OBSERVED: H2-ERR(b) A 原 ok={ok0}，chat 流式吐出 {out.get('text')!r} 后 A 生成失败："
                      f"status={status}，chat_stream 依次用 {served}，结尾 error={out.get('error')}；"
                      f"A {_state_text(acc_a)} checked_at={acc_a['checked_at']}，B {_state_text(acc_b)}")
                results.append({"ok0": ok0, "t0": t0, "status": status, "raw": raw, "out": out,
                                "served": served, "a": acc_a, "b": acc_b, "b0": state_b})
        for r in results:
            out, acc_a = r["out"], r["a"]
            assert r["status"] == 200 and out["text"] == "partial reply", (r["status"], r["raw"][:300])
            assert out["error"] and H2_GEN_ERR in out["error"]["message"] and not out["done"], out
            assert r["served"] == ["A"], r["served"]
            assert acc_a["note"].startswith("任务异常") and H2_GEN_ERR in acc_a["note"], (
                f"A 原 ok={r['ok0']}：生成失败应只给 A 记\"任务异常: …\"备注，实际 {_state_text(acc_a)}")
            assert acc_a["ok"] is r["ok0"], f"偶发生成失败不应改 ok：原 {r['ok0']}，现 {acc_a['ok']}"
            assert (acc_a["checked_at"] or 0) >= r["t0"], acc_a
            assert r["b"] == r["b0"], ("另一个账号不能被改写", r["b0"], r["b"])
            print(f"PASS: H2-ERR(b) A 原 ok={r['ok0']}：吐出分片后生成失败，A 只记备注 {acc_a['note']!r} 与 "
                  f"checked_at、ok 仍为 {acc_a['ok']}，不切号，B 不变")
    finally:
        hm.close_app(app)


def test_h2_err_generation_retry():
    """H2-ERR(c)（1.4 → 2.4、2.20、3.35）：首字前 A 生成失败（偶发），worker 切到 B 完成：A 只记
    "任务异常: …"备注、ok 不变（原值 None，误记成 mark(…, True) 也看得出来），B 记为 ok=True、备注清空。"""
    app = hm.load_app(H1_ENV, "g1-h2-err-retry")
    try:
        a = _add_account(app, "good-A", "A")
        b = _add_account(app, "good-B", "B")
        names = {a: "A", b: "B"}
        headers = hm.auth_headers(app)
        chat = FailFor(a, "served-by-B", app.MuseGenerationError, H2_GEN_ERR)
        hm.fake_engine(app, chat_stream=chat)
        _set_state(app, a, None, "")
        _set_state(app, b, None, H2_STALE)
        _make_lru(app, a, b)
        with hm.serve_app(app) as server:
            status, raw = server.request("POST", CHAT, headers=headers, timeout=30,
                                         body=_body(CHAT, "h2 generation retry", stream=True))
        text = _safe_reply(CHAT, True, status, raw)
        served = _named([c["account_id"] for c in chat.calls], names)
        acc_a, acc_b = _acc_state(app, a), _acc_state(app, b)
        print(f"OBSERVED: H2-ERR(c) chat 流式首字前 A 生成失败：status={status}，chat_stream 依次用 {served}，"
              f"回复 {text!r}；A {_state_text(acc_a)}，B {_state_text(acc_b)}")
        assert status == 200 and text == "served-by-B", (status, raw[:300])
        assert served == ["A", "B"], served
        assert acc_a["note"].startswith("任务异常") and H2_GEN_ERR in acc_a["note"], (
            f"首字前生成失败应给 A 记\"任务异常: …\"备注，实际 {_state_text(acc_a)}")
        assert acc_a["ok"] is None, f"偶发生成失败不应改 A 的 ok：原 None，现 {acc_a['ok']}"
        assert acc_b["ok"] is True and acc_b["note"] == "", acc_b
        print(f"PASS: H2-ERR(c) 首字前 A 生成失败、B 完成：A 只记备注 {acc_a['note']!r}、ok 仍为 None；"
              f"B ok=True、备注清空")
    finally:
        hm.close_app(app)


def test_h2_err_preleased_error_not_recorded():
    """H2-ERR(d)（2.4）：账号状态只写本次尝试实际租到的账号。A 首字前 401 之后，第二次尝试没租到账号，
    请求以错误结束；不能有别的错误写到 A 上覆盖 401 原因：
      (d1) A 抛错前把全部账号禁用 → 没有其它可切换的账号：acquire 立即抛 NoAlternativeAccount，worker
           不重试，返回第一次的 401 错误（L6 之前是 acquire 抛 MuseAuthError「没有可用账号…」）；
      (d2) A 抛错前只禁用 A，B 被生成任务占着，请求 timeout=2s → acquire 等号超时抛 MuseGenerationError。"""
    app = hm.load_app(H1_ENV, "g1-h2-err-prelease")
    holds = []
    try:
        a = _add_account(app, "good-A", "A")
        b = _add_account(app, "good-B", "B")
        names = {a: "A", b: "B"}
        headers = hm.auth_headers(app)
        results = []

        def disable(*ids):
            def hook():
                for aid in ids:
                    app.store.update_account(aid, enabled=False)
            return hook

        with hm.serve_app(app) as server:
            for case, off, busy_b in (("d1", (a, b), False), ("d2", (a,), True)):
                for aid in (a, b):
                    app.store.update_account(aid, enabled=True)
                chat = AuthFailFor(app, a, "never-served", before_raise=disable(*off))
                gen = HoldGenerate(30)
                hm.fake_engine(app, chat_stream=chat, generate=gen)
                _set_state(app, a, True, "")
                _set_state(app, b, None, H2_STALE)
                _make_lru(app, a, b)
                if busy_b:
                    holds.append((gen, _occupy(app, b, gen)))  # B 被占着：第二次尝试只能等它，直到超时
                state_b = _acc_state(app, b)
                t0 = time.monotonic()
                status, raw = server.request("POST", CHAT, headers=headers, timeout=30,
                                             body=_body(CHAT, f"h2 pre-lease {case}", stream=True,
                                                        timeout=H2_BUSY_TIMEOUT if busy_b else None))
                took = time.monotonic() - t0
                out = _chat_stream_outcome(raw) if status == 200 else {}
                served = _named([c["account_id"] for c in chat.calls], names)
                acc_a, acc_b = _acc_state(app, a), _acc_state(app, b)  # 先取 B 的状态，再放行占着它的生成任务
                gen.release.set()
                err = (out.get("error") or {}).get("message")
                print(f"OBSERVED: H2-ERR({case}) A 首字前 401 后第二次尝试没租到账号：status={status}，"
                      f"{took:.2f}s，chat_stream 依次用 {served}，结尾 error={err!r}；"
                      f"A {_state_text(acc_a)}，B {_state_text(acc_b)}")
                results.append({"case": case, "status": status, "raw": raw, "out": out, "served": served,
                                "a": acc_a, "b": acc_b, "b0": state_b})
        for r in results:
            case, acc_a = r["case"], r["a"]
            assert r["status"] == 200 and r["out"]["error"] and not r["out"]["done"], (case, r["raw"][:300])
            assert r["served"] == ["A"], (case, r["served"])
            assert acc_a["ok"] is False and AUTH_401 in acc_a["note"], (
                f"H2-ERR({case}): 第二次尝试在租到账号之前的错误不应写到 A 上，A 的备注应仍是 401 原因，"
                f"实际 {_state_text(acc_a)}")
            assert H2_DENIED not in acc_a["note"] and "任务异常" not in acc_a["note"], (case, acc_a)
            assert r["b"] == r["b0"], (case, "另一个账号不能被改写", r["b0"], r["b"])
            print(f"PASS: H2-ERR({case}) 第二次尝试没租到账号，请求以错误 {r['out']['error']['message']!r} 结束，"
                  f"没有错误写到别的账号上：A 仍为 ok=False、备注 {acc_a['note']!r}，B 不变")
    finally:
        _release_all(holds)
        hm.close_app(app)


# ------------------------- PRES-H2 -------------------------
def test_pres_h2_direct_success():
    """PRES-H2（3.15、3.34）：流式对话由首选账号直接成功完成：该账号记 ok=True、备注清空，另一个账号不动。
    chat 与 responses 的流式各一次。"""
    app = hm.load_app(H1_ENV, "g1-pres-h2")
    try:
        a = _add_account(app, "good-A", "A")
        b = _add_account(app, "good-B", "B")
        names = {a: "A", b: "B"}
        headers = hm.auth_headers(app)
        chat = ScriptedChat(["direct ", "reply"])
        hm.fake_engine(app, chat_stream=chat)
        with hm.serve_app(app) as server:
            for path in (CHAT, RESPONSES):
                _set_state(app, a, None, "任务异常: 上一次的偶发失败")
                _set_state(app, b, None, H2_STALE)
                _make_lru(app, a, b)
                state_b = _acc_state(app, b)
                n = len(chat.calls)
                status, raw = server.request("POST", path, headers=headers, timeout=30,
                                             body=_body(path, "pres h2", stream=True))
                served = _named([c["account_id"] for c in chat.calls[n:]], names)
                text = _safe_reply(path, True, status, raw)
                acc_a, acc_b = _acc_state(app, a), _acc_state(app, b)
                print(f"OBSERVED: PRES-H2 {path} 流式由 {served} 直接完成，回复 {text!r}；"
                      f"A {_state_text(acc_a)}，B {_state_text(acc_b)}")
                assert status == 200 and text == "direct reply", (path, status, raw[:300])
                assert served == ["A"], (path, served)
                assert acc_a["ok"] is True and acc_a["note"] == "", (path, acc_a)
                assert acc_b == state_b, (path, "另一个账号不能被改写", state_b, acc_b)
                print(f"PASS: PRES-H2 {path} 流式由首选账号 A 直接完成：A 记 ok=True、备注清空，B 不变")
    finally:
        hm.close_app(app)


# ------------------------- L6 小工具 -------------------------
L6_DELAY = 1.0   # 假 chat_stream 首字前停这么久再抛错：以切号名义重试同一账号会让总耗时翻倍
L6_FAST = 1.8    # 不重试时请求的总耗时上限（秒）
L6_DEAD = "会话已失效（之前的检测）"   # L6-RETRY(b) 里 B 的 ok=False 备注
L6_PLAIN_ERR = "页面脚本异常（模拟）"  # L6-FAILED 里普通 RuntimeError 的消息
L6_ERRORS = (("MuseGenerationError", H2_GEN_ERR), ("MuseAuthError", AUTH_401))


class SlowFailFor(FailFor):
    """FailFor 的慢速版：bad 账号首字前先停 delay 秒再抛错，消息末尾带"第 n 次调用"，
    据此区分返回的是第一次还是重试的错误，并量出重试是否让总耗时翻倍。"""

    def __init__(self, bad: str, reply: str, exc_type, message: str, delay: float = L6_DELAY):
        super().__init__(bad, reply, exc_type, message, before_raise=lambda: time.sleep(delay))

    def __call__(self, cookies, prompt, expires=None, timeout=None, account_id=None, stop_event=None):
        self.calls.append({"account_id": account_id})
        if account_id == self.bad:
            self.before_raise()
            raise self.exc_type(f"{self.message}（第 {len(self.calls)} 次调用）")
        yield self.reply


def _error_message(path: str, stream: bool, status: int | None, raw: bytes):
    """请求以错误结束时返回错误消息：非流式取响应体的 error，chat 流式取最后的错误事件，
    responses 流式取 response.failed 的 response.error；正常完成或取不到时返回 None。"""
    try:
        if not stream:
            return None if status == 200 else (json.loads(raw).get("error") or {}).get("message")
        if path == CHAT:
            return (_chat_stream_outcome(raw)["error"] or {}).get("message")
        failed = [json.loads(d) for name, d in _sse_events(raw) if name == "response.failed"]
        return (failed[-1]["response"].get("error") or {}).get("message") if failed else None
    except (ValueError, KeyError, TypeError, AttributeError):
        return None


def _wait_waiter(app, req: _Request, timeout: float = 10.0) -> bool:
    """等到有线程停在 AccountPoolManager.acquire 里等账号（或请求已返回、超时），返回是否看到了等待。"""
    deadline = time.monotonic() + timeout
    while req.is_alive() and time.monotonic() < deadline:
        if _account_waiters(app):
            return True
        time.sleep(0.05)
    return bool(_account_waiters(app))


# ------------------------- L6-RETRY -------------------------
def test_l6_retry_no_same_account():
    """L6-RETRY（1.53 → 2.53）：首字前失败后，没有其它可切换的健康账号就不重试，直接返回第一次的错误。
      (a) 只有一个账号 A：MuseGenerationError 与 MuseAuthError 各在 chat / responses 的流式、非流式下跑一次；
      (b) 另一个账号 B 存在但 ok=False：同样不重试（chat 流式、responses 非流式）；
      (a)(b) 断言 chat_stream 只调用 1 次、返回的是第一次的错误、总耗时 < L6_FAST（假 chat_stream 抛错前
      停 L6_DELAY 秒，重试同一账号会让总耗时翻倍）；(b) 另外断言 B 的状态没被改写。
      (c) 另一个健康账号 B 正被生成任务占着、A 是唯一空闲的账号：第二次尝试不再选回 A，排队等 B 释放后由 B 完成。"""
    app = hm.load_app(H1_ENV, "g1-l6-retry")
    holds = []
    try:
        a = _add_account(app, "good-A", "A")
        names = {a: "A"}
        headers = hm.auth_headers(app)
        quick, busy = [], []
        b = None
        with hm.serve_app(app) as server:
            for case, combos in (("a", H2_COMBOS), ("b", ((CHAT, True), (RESPONSES, False)))):
                if case == "b":
                    b = _add_account(app, "good-B", "B")
                    names[b] = "B"
                for exc_name, msg in L6_ERRORS:
                    for path, stream in combos:
                        _set_state(app, a, True, "")       # A 健康：首次尝试落在 A
                        if b:
                            _set_state(app, b, False, L6_DEAD)
                        state_b = _acc_state(app, b) if b else None
                        chat = SlowFailFor(a, "served-by-B", getattr(app, exc_name), msg)
                        hm.fake_engine(app, chat_stream=chat)
                        t0 = time.monotonic()
                        status, raw = server.request("POST", path, headers=headers, timeout=30,
                                                     body=_body(path, f"l6 retry {case}", stream=stream))
                        took = time.monotonic() - t0
                        served = _named([c["account_id"] for c in chat.calls], names)
                        err = _error_message(path, stream, status, raw)
                        text = None if err is not None else _safe_reply(path, stream, status, raw)
                        acc_b = _acc_state(app, b) if b else None
                        where = f"({case}) {exc_name} {path} stream={stream}"
                        print(f"OBSERVED: L6-RETRY{where} 首字前 A 失败：{took:.2f}s 返回 status={status}，"
                              f"chat_stream 依次用 {served}，错误 {err!r}"
                              + (f"，回复 {text!r}" if text is not None else "")
                              + (f"；B {_state_text(acc_b)}" if b else ""))
                        quick.append({"where": where, "first": f"{msg}（第 1 次调用）", "status": status,
                                      "took": took, "served": served, "err": err, "b": acc_b, "b0": state_b})
            for exc_name, msg in L6_ERRORS:
                _set_state(app, a, True, "")
                _set_state(app, b, True, "")             # (c) B 健康，但正被生成任务占着
                gen = HoldGenerate(30)
                chat = FailFor(a, "served-by-B", getattr(app, exc_name), msg)
                hm.fake_engine(app, chat_stream=chat, generate=gen)
                holds.append((gen, _occupy(app, b, gen)))
                req = _Request(app, server, CHAT, _body(CHAT, "l6 retry busy", stream=True, timeout=20), headers)
                waited = _wait_waiter(app, req)
                t_release = time.monotonic()
                gen.release.set()
                req.join(15)
                after = None if req.done_at is None else req.done_at - t_release
                served = _named([c["account_id"] for c in chat.calls], names)
                raw = req.raw or b""
                text = _safe_reply(CHAT, True, req.status, raw)
                err = _error_message(CHAT, True, req.status, raw)
                where = f"(c) {exc_name}"
                print(f"OBSERVED: L6-RETRY{where} B 被占着、A 首字前失败：chat_stream 依次用 {served}，"
                      f"第二次尝试{'排队等账号' if waited else '没有排队等账号'}；放行 B 后 "
                      f"{'-' if after is None else f'{after:.2f}s'} 返回 status={req.status}，"
                      f"回复 {text!r}，错误 {err!r}")
                busy.append({"where": where, "status": req.status, "text": text, "err": err,
                             "served": served, "waited": waited, "after": after})
        for r in quick:
            where = r["where"]
            assert r["served"] == ["A"], (f"L6-RETRY{where}: 没有其它可切换的健康账号时不应重试，"
                                          f"chat_stream 实际依次用 {r['served']}")
            assert r["err"] is not None and r["first"] in r["err"], (
                f"L6-RETRY{where}: 应返回第一次的错误 {r['first']!r}，实际 {r['err']!r}（status={r['status']}）")
            assert r["took"] < L6_FAST, f"L6-RETRY{where}: 总耗时 {r['took']:.2f}s，应 < {L6_FAST}s"
            if r["b0"] is not None:
                assert r["b"] == r["b0"], (where, "B 的状态不能被改写", r["b0"], r["b"])
            print(f"PASS: L6-RETRY{where} chat_stream 只用 A 调用 1 次，{r['took']:.2f}s 返回第一次的错误 "
                  f"{r['err']!r}" + ("，B 不变" if r["b0"] is not None else ""))
        for r in busy:
            where = r["where"]
            assert r["served"] == ["A", "B"], (f"L6-RETRY{where}: 第二次尝试应排队等忙碌的 B，不应再选回 A："
                                               f"chat_stream 实际依次用 {r['served']}")
            assert r["status"] == 200 and r["text"] == "served-by-B", (where, r["status"], r["text"], r["err"])
            assert r["waited"], f"L6-RETRY{where}: 第二次尝试应排队等 B 释放"
            assert r["after"] is not None and r["after"] < 1.0, (where, r["after"])
            print(f"PASS: L6-RETRY{where} B 被占着时第二次尝试不选回 A，排队等 B，放行后 {r['after']:.2f}s 由 B 完成")
    finally:
        _release_all(holds)
        hm.close_app(app)


# ------------------------- L6-FAILED -------------------------
L6_FAILED_CASES = (("MuseAuthError", AUTH_401, "upstream_auth_error"),
                   ("MuseGenerationError", H2_GEN_ERR, "upstream_error"),
                   ("RuntimeError", L6_PLAIN_ERR, "server_error"))
L6_FAILED_EVENTS = (["response.created", "response.output_item.added", "response.content_part.added"]
                    + ["response.output_text.delta"] * 2 + ["response.failed"])


def test_l6_failed_event_carries_error():
    """L6-FAILED（1.54 → 2.54、3.61）：/v1/responses 流式吐出两个分片后分别以 MuseAuthError、MuseGenerationError、
    普通 RuntimeError 失败：response.failed 的 response.error 为 {code, message}，code 依次为 upstream_auth_error、
    upstream_error、server_error，message 为异常消息；其余不变：事件序列为 created → output_item.added →
    content_part.added → 2 个 delta → failed，failed 的 response 除 error 外与 created 时相同（status 为 failed）。"""
    app = hm.load_app(H1_ENV, "g1-l6-failed")
    try:
        a = _add_account(app, "good-A", "A")
        headers = hm.auth_headers(app)
        results = []
        with hm.serve_app(app) as server:
            for exc_name, msg, code in L6_FAILED_CASES:
                exc_type = RuntimeError if exc_name == "RuntimeError" else getattr(app, exc_name)
                _set_state(app, a, True, "")
                hm.fake_engine(app, chat_stream=ScriptedChat(["partial ", "reply"], raise_exc=exc_type(msg)))
                status, raw = server.request("POST", RESPONSES, headers=headers, timeout=30,
                                             body=_body(RESPONSES, f"l6 failed {exc_name}", stream=True))
                events = [(name, json.loads(d)) for name, d in _sse_events(raw)] if status == 200 else []
                failed = (events[-1][1].get("response") or {}) if events else {}
                print(f"OBSERVED: L6-FAILED {exc_name}: status={status}，事件 {[name for name, _ in events]}；"
                      f"response.failed 的 response.error={failed.get('error')!r}")
                results.append({"exc": exc_name, "msg": msg, "code": code, "status": status,
                                "raw": raw, "events": events})
        for r in results:
            exc_name, events = r["exc"], r["events"]
            assert r["status"] == 200, (exc_name, r["status"], r["raw"][:300])
            assert [name for name, _ in events] == L6_FAILED_EVENTS, (exc_name, [name for name, _ in events])
            assert all(obj["type"] == name for name, obj in events), (exc_name, events)
            deltas = [obj["delta"] for name, obj in events if name == "response.output_text.delta"]
            assert deltas == ["partial ", "reply"], (exc_name, deltas)
            created = events[0][1]["response"]
            failed = dict(events[-1][1]["response"])
            err = failed.pop("error", None)
            assert isinstance(err, dict) and set(err) == {"code", "message"}, (
                f"L6-FAILED {exc_name}: response.failed 的 response 应带 error={{code, message}}，实际 {err!r}")
            assert err["code"] == r["code"] and r["msg"] in err["message"], (exc_name, err)
            assert failed == {**created, "status": "failed"}, (exc_name, created, failed)
            print(f"PASS: L6-FAILED {exc_name}: response.failed 带 error={err}；事件序列与 response 其它字段不变")
    finally:
        hm.close_app(app)


# ------------------------- PRES-SWITCH -------------------------
def test_pres_switch_to_other_account():
    """PRES-SWITCH（3.60、3.7、3.12）：有其它健康账号时，首字前失败照常切到另一个账号完成。
      (a) A、B 都健康：A 首字前抛 MuseAuthError / MuseGenerationError，worker 切到 B 完成
          （chat 流式与 responses 非流式各一次）；
      (b) B 正被生成任务占着：A 首字前 401，抛错前把 A 禁用（修复前的第二次尝试不排除失败的账号，A 仍可选时
          会再选回 A，见 L6-RETRY(c)；禁用后修复前后都只能选 B），第二次尝试排队等 B，B 释放后由 B 完成。"""
    app = hm.load_app(H1_ENV, "g1-pres-switch")
    holds = []
    try:
        a = _add_account(app, "good-A", "A")
        b = _add_account(app, "good-B", "B")
        names = {a: "A", b: "B"}
        headers = hm.auth_headers(app)
        results = []
        with hm.serve_app(app) as server:
            for exc_name, msg in (("MuseAuthError", AUTH_401), ("MuseGenerationError", H2_GEN_ERR)):
                for path, stream in ((CHAT, True), (RESPONSES, False)):
                    _set_state(app, a, True, "")
                    _set_state(app, b, True, "")
                    _make_lru(app, a, b)  # A 是 LRU 首选：首次尝试落在 A
                    chat = FailFor(a, "served-by-B", getattr(app, exc_name), msg)
                    hm.fake_engine(app, chat_stream=chat)
                    t0 = time.monotonic()
                    status, raw = server.request("POST", path, headers=headers, timeout=30,
                                                 body=_body(path, "pres switch", stream=stream))
                    took = time.monotonic() - t0
                    served = _named([c["account_id"] for c in chat.calls], names)
                    text = _safe_reply(path, stream, status, raw)
                    where = f"(a) {exc_name} {path} stream={stream}"
                    print(f"OBSERVED: PRES-SWITCH{where} 首字前 A 失败：{took:.2f}s 返回 status={status}，"
                          f"chat_stream 依次用 {served}，回复 {text!r}")
                    results.append({"where": where, "status": status, "text": text, "served": served,
                                    "took": took, "waited": None, "after": None})
            _set_state(app, a, True, "")
            _set_state(app, b, True, "")
            gen = HoldGenerate(30)
            chat = AuthFailFor(app, a, "served-by-B",
                               before_raise=lambda: app.store.update_account(a, enabled=False))
            hm.fake_engine(app, chat_stream=chat, generate=gen)
            holds.append((gen, _occupy(app, b, gen)))
            req = _Request(app, server, CHAT, _body(CHAT, "pres switch busy", stream=True, timeout=20), headers)
            waited = _wait_waiter(app, req)
            t_release = time.monotonic()
            gen.release.set()
            req.join(15)
            after = None if req.done_at is None else req.done_at - t_release
            served = _named([c["account_id"] for c in chat.calls], names)
            text = _safe_reply(CHAT, True, req.status, req.raw or b"")
            print(f"OBSERVED: PRES-SWITCH(b) B 被占着、A 首字前 401（抛错前禁用 A）：chat_stream 依次用 {served}，"
                  f"第二次尝试{'排队等账号' if waited else '没有排队等账号'}；放行 B 后 "
                  f"{'-' if after is None else f'{after:.2f}s'} 返回 status={req.status}，回复 {text!r}")
            results.append({"where": "(b) MuseAuthError chat stream=True", "status": req.status, "text": text,
                            "served": served, "took": req.took, "waited": waited, "after": after})
        for r in results:
            where = r["where"]
            assert r["status"] == 200 and r["text"] == "served-by-B", (where, r["status"], r["text"])
            assert r["served"] == ["A", "B"], (where, r["served"])
            if r["waited"] is None:
                assert r["took"] < 3.0, (where, r["took"])
                print(f"PASS: PRES-SWITCH{where} 首字前 A 失败，{r['took']:.2f}s 内切到 B 完成")
            else:
                assert r["waited"], f"PRES-SWITCH{where}: 第二次尝试应排队等 B 释放"
                assert r["after"] is not None and r["after"] < 1.0, (where, r["after"])
                print(f"PASS: PRES-SWITCH{where} 第二次尝试排队等被占着的 B，放行后 {r['after']:.2f}s 由 B 完成")
    finally:
        _release_all(holds)
        hm.close_app(app)


# ------------------------- C1-EXP（真实链路） -------------------------
def test_c1_exp_real_chain_contexts():
    """FakeMuse 慢速回复 + Chromium + 真实 engine：断开后槽位、账号、浏览器上下文都不残留。"""
    with hm.FakeMuse() as muse:
        wrapper = hm.make_chromium_wrapper(muse.port, muse.workdir)
        app = hm.load_app({"MUSE2API_CHROMIUM": wrapper, "MUSE2API_CONCURRENCY": "1",
                           "MUSE2API_CHAT_TIMEOUT": "60"}, "g1-c1-real")
        try:
            aid = _add_account(app)
            headers = hm.auth_headers(app)
            with hm.serve_app(app) as server:
                cases = ((CHAT, b'"content"', "please be slow, chat"),
                         (RESPONSES, b"event: response.output_text.delta", "please be slow, responses"))
                for path, marker, prompt in cases:
                    state0 = _acc_state(app, aid)
                    with hm.StreamClient(server) as cli:
                        cli.post_stream(path, _body(path, prompt, stream=True))
                        cli.read_until(marker, timeout=90)
                        assert cli.status() == 200, (path, cli.buf[:200])
                        before = hm.snapshot(app)
                        assert before["gen_lock_active"] == 1 and before["busy_accounts"] == {aid: 1}, before
                        assert (before["contexts"] or 0) >= 1, before
                        at = cli.abort()
                    released = hm.wait_released(app, timeout=RELEASE_WITHIN)
                    stacks = _settled_stacks(app)
                    snap = hm.snapshot(app)
                    print(f"OBSERVED: C1-EXP 真实链路 {path} 首个分片 {cli.first_chunk_seconds:.2f}s，"
                          f"{at:.2f}s 断开；{_released_text(released)}；断开前 contexts={before['contexts']}，"
                          f"之后 gen_lock_active={snap['gen_lock_active']} busy_accounts={snap['busy_accounts']} "
                          f"contexts={snap['contexts']}")
                    _show_stacks(stacks)
                    _check_disconnect_released(app, path, aid, state0, released, stacks)
                    assert snap["contexts"] == 0, f"{path}: 断开后仍残留 {snap['contexts']} 个浏览器上下文"

                    follow = f"hello again {path[4:8]}"
                    t0 = time.monotonic()
                    status, raw = server.request("POST", path, headers=headers, timeout=90,
                                                 body=_body(path, follow, stream=False, timeout=45))
                    took = time.monotonic() - t0
                    assert status == 200, (path, status, raw[:300])
                    assert _reply_text(path, json.loads(raw)) == muse.expected_reply(follow), raw[:300]
                    print(f"PASS: C1-EXP 真实链路 {path} 断开后 {released:.2f}s 释放槽位、账号与浏览器上下文；"
                          f"下一个请求 {took:.2f}s 正常完成")
        finally:
            hm.close_app(app)


if __name__ == "__main__":
    hm.run_main(test_c1_exp_disconnect_releases, test_c1_slow_reader_gets_everything,
                test_c1_wd_stall_watchdog, test_pres_stream_sequences,
                test_h1_exp_skips_busy_account, test_pres_rotate_idle, test_pres_rotate_all_busy,
                test_pres_rotate_auth_switch, test_h2_exp_switched_entry_keeps_error,
                test_h2_err_auth_after_output, test_h2_err_generation_after_output,
                test_h2_err_generation_retry, test_h2_err_preleased_error_not_recorded,
                test_pres_h2_direct_success, test_l6_retry_no_same_account,
                test_l6_failed_event_carries_error, test_pres_switch_to_other_account,
                test_c1_exp_real_chain_contexts)
    print("PASS: test_g1_dispatch")
