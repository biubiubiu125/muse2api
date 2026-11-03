"""Offline regressions for MUSE page readiness and video delivery waits."""
import ast
import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

# The composer-readiness checks below evaluate engine.py's JS with node; where node
# is not installed (e.g. the runtime image) skip instead of failing.
if shutil.which("node") is None:
    print("SKIP: node not found (needed to evaluate engine.py's composer-readiness JS)")
    sys.exit(0)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location("muse_engine_candidate", ROOT / "engine.py")
engine_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(engine_module)

tree = ast.parse((ROOT / "engine.py").read_text(encoding="utf-8"))
method = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_wait_ws_ready")
expression = next(n.args[0].value for n in ast.walk(method)
                  if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "js")

for name, connecting, hydrated, disabled, expected, nav_stale in (
        ("empty_connected", False, True, False, "ready", False),
        ("connecting", True, True, False, "connecting", False),
        ("hydrating", False, False, False, "hydrating", False),
        ("disabled_input", False, True, True, "input-disabled", False),
        # 侧栏里其它会话残留的 Connecting... 不算本页卡住
        ("stale_sidebar_status", False, True, False, "ready", True)):
    script = """const c=%s;const ta={disabled:c.disabled};const document={readyState:'complete',body:{innerText:(c.connecting?'Connecting...':'')+(c.navStale?'Old chat Connecting...':'')},querySelectorAll:s=>c.navStale&&s.indexOf('nav')!==-1?[{innerText:'Old chat Connecting...'}]:[],querySelector:s=>s==='textarea'?ta:s==='[data-hatch-shell-hydration-state]'?{getAttribute:()=>c.hydrated?'hydrated':'server'}:null};console.log(%s);""" % (
        json.dumps(dict(connecting=connecting, hydrated=hydrated, disabled=disabled, navStale=nav_stale)), expression)
    result = subprocess.check_output(["node", "-e", script], text=True).strip()
    assert result == expected, (name, result, expected)

with tempfile.TemporaryDirectory() as profile:
    engine = engine_module.MuseEngine(SimpleNamespace(profile_dir=profile, login_wait=2))
    clock = [0.0]
    engine_module.time = SimpleNamespace(time=lambda: clock[0], sleep=lambda n: clock.__setitem__(0, clock[0] + n))
    engine.page = SimpleNamespace(js=lambda _: json.dumps({
        # 「Still sending」是站点自身的状态条（不在聊天气泡里），同时出现在 tail 与 sys_tail
        "cnt": 0, "txt": "", "stop": True, "tail": "Still sending", "sys_tail": "Still sending"}))
    engine.attachments = lambda: []
    try:
        engine._wait_attachment("", 60, "video")
    except engine_module.MuseGenerationError as exc:
        assert "未确认送达" in str(exc)
        assert isinstance(exc, engine_module.MuseSubmittedError), type(exc)
        assert clock[0] < 32, clock[0]
    else:
        raise AssertionError("undelivered video request waited for the full timeout")

print("PASS: composer readiness and undelivered video fast-fail")


class FakeClock:
    def __init__(self):
        self.now = 0.0
    def time(self):
        return self.now
    def sleep(self, seconds):
        self.now += seconds


def wait_case(states, timeout=600, expect="video"):
    clock = FakeClock()
    calls = {"n": 0}
    def js(_script):
        index = min(calls["n"], len(states) - 1)
        calls["n"] += 1
        state = dict(states[index])
        state.setdefault("cnt", 0)
        state.setdefault("txt", "")
        state.setdefault("stop", False)
        state.setdefault("tail", "")
        state.setdefault("path", "/thread/new")
        return json.dumps(state)
    page = SimpleNamespace(js=js)
    e = engine_module.MuseEngine(SimpleNamespace(profile_dir=tempfile.gettempdir()))
    e.page = page
    e.attachments = lambda: []
    e._scroll_bottom = lambda: None
    with patch.object(engine_module, "time", clock):
        return e._wait_attachment("", timeout, expect)


refusal = "视频没生成成功：你这个账号所在的国家/地区目前不支持视频生成，这是持续限制。"
try:
    wait_case([
        {"stop": True, "tail": "正在处理", "path": "/thread/new"},
        {"stop": False, "tail": refusal, "path": "/thread/new"},
    ])
except engine_module.MuseModelRefusalError as exc:
    assert refusal in str(exc), str(exc)
else:
    raise AssertionError("explicit region refusal was not returned promptly")

# Generic page chrome containing region/video words is not sufficient evidence.
assert engine_module.MuseEngine._media_refusal_text(
    "Settings\\nRegion: US\\nVideo", "video") == ""

# A completed submission that remains idle on /thread/new stops before 600s,
# but only after the Stop lifecycle and a long quiet period.
try:
    wait_case([
        {"stop": True, "tail": "", "path": "/thread/new"},
        {"stop": False, "tail": "", "path": "/thread/new"},
    ])
except engine_module.MuseGenerationError as exc:
    assert "未建立视频任务线程" in str(exc), str(exc)
    assert isinstance(exc, engine_module.MuseSubmittedError), type(exc)
else:
    raise AssertionError("idle /thread/new task waited for full timeout")

# No observed submission lifecycle must not trigger the early no-thread error.
assert wait_case([{"stop": False, "tail": "", "path": "/thread/new"}], timeout=20) is None
print("PASS: explicit refusal propagated; idle no-thread stopped; normal wait preserved")
